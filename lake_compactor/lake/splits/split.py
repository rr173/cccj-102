"""按 row_id 成组抽样的训练集切分：train / validation / test 三个 Parquet 子集。

业务约束（为什么必须成组）：

  - 一个业务对象可能对应多条训练观测（同一个 row_id 出现多次）。如果把
    这些观测拆散，同一对象会同时落在训练集与评测集，离线指标虚高。
  - 因此抽样单位是 **row_id** 而不是行：相同 row_id 的所有可见行必然
    落在同一个子集里。

分配算法（稳定、可重放、与文件顺序无关）：

  分组 -> 每组计算 h = SHA-256(seed 的十进制字符串 + b"\\x00" + row_id
  的 UTF-8 字节) -> 取前 8 字节作为无符号整数 u -> 在区间
  [0, 10000) 上按权重依次放置：train 占 [0, w_train)，
  validation 占 [w_train, w_train + w_validation)，test 占其余。
  这个 10000 就是权重向量的分母，权重实际是“万分份”。

  散列只承诺 **长期比例** 接近权重；小数据下实际数量可以偏斜，
  清单里显式记录 `proportionality_notice`。分配只用到 row_id 与 seed，
  不依赖文件排列、物理行号或压实顺序，所以重排文件或先压实都不改变
  归属。

交付目录（一次目录改名发布）：

  <root>/splits/<split_id>/
    train.parquet
    validation.parquet
    test.parquet
    manifest.json

  split_id = SHA-256(规范化的参数身份)[:16]，参数身份 = 算法版本 +
  输入 snapshot + train/validation/test 权重 + seed。完全相同参数 ->
  同一个 split_id -> 直接返回原切分，不重写；参数变化 -> 新身份 ->
  新目录，旧数据不被替换。

崩溃语义：全部文件先写进 `splits/.staging-<split_id>-<uuid>/`，逐项核对
（行数、校验和、分组约束）后，`os.rename(staging, splits/<split_id>)`
一次发布。目录改名在同一文件系统内是原子的：发布前进程退出，目标路径
根本不存在（只剩 `.staging-*` 残骸）；重新调用在锁内确认目标缺失，
清掉残骸并重新产出。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from .. import manifest as mf
from .. import storage
from ..errors import SplitConflictError, SplitError
from ..table import LakeTable

SPLITS_DIRNAME = "splits"
MANIFEST_NAME = "manifest.json"
SUBSETS = ("train", "validation", "test")
# 分配算法版本写进清单；算法实现变化时必须升版本，旧交付身份因此不再
# 与新参数命中，不会被误认为“完全相同的参数”。
ALGORITHM_VERSION = 1
TOTAL_WEIGHT = 10000
DEFAULT_BATCH_SIZE = 4096
LOCK_NAME = "LOCK"

PROPORTIONALITY_NOTICE = (
    "散列抽样只承诺长期比例接近权重；小数据下各子集的实际行数/对象数"
    "可以偏斜，本清单记录的是这次实际分配的结果。"
)

# 测试钩子：命中后在该位置以 os._exit 硬退出（真正退出的子进程）
#   staged:  暂存目录已全部写完并 fsync、核对完成后，目录改名发布之前退出
#   published: 目录已改名发布之后退出（交付已完成，重新调用应返回原切分）
CRASH_ENV = "LAKE_SPLIT_CRASH_POINT"

_LOCKS: dict[str, dict] = {}


def _maybe_crash(point: str) -> None:
    if os.environ.get(CRASH_ENV) == point:
        os._exit(23)


def _process_lock(table: LakeTable):
    """进程内可重入锁 + 跨进程 flock，保护“发布目录存在性”的检查与改名。

    fcntl.flock 只保证跨进程互斥（同一进程内重复加锁会立即成功），
    所以进程内再用 threading.RLock 兜住同进程的并发/嵌套调用。
    """
    import threading

    path_key = os.path.abspath(table.root)
    state = _LOCKS.get(path_key)
    if state is None:
        state = {
            "rlock": threading.RLock(),
            "depth": 0,
            "path": os.path.join(_split_root(table), LOCK_NAME),
        }
        _LOCKS[path_key] = state
    return state


class _SplitLock:
    def __init__(self, table: LakeTable):
        self._state = _process_lock(table)
        self._fh = None
        self._path = self._state["path"]

    def __enter__(self):
        self._state["rlock"].acquire()
        try:
            if self._state["depth"] == 0:
                os.makedirs(os.path.dirname(self._path), exist_ok=True)
                self._fh = open(self._path, "a+b")
                import fcntl

                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
            self._state["depth"] += 1
        except BaseException:
            self._state["rlock"].release()
            raise
        return self

    def __exit__(self, *exc):
        try:
            self._state["depth"] -= 1
            if self._state["depth"] == 0 and self._fh is not None:
                import fcntl

                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
                self._fh.close()
                self._fh = None
        finally:
            self._state["rlock"].release()




# ------------------------------------------------------------------- 参数校验
def normalize_weights(train, validation, test) -> dict[str, int]:
    """规范化并校验整数权重：非负、总和恰为 10000。

    布尔值被拒绝（Python 里 bool 是 int 的子类，True/False 容易是配置
    笔误）。校验失败整次拒绝，不做任何 IO。
    """
    names = ("train", "validation", "test")
    values = {"train": train, "validation": validation, "test": test}
    out: dict[str, int] = {}
    for name in names:
        v = values[name]
        if isinstance(v, bool) or not isinstance(v, int):
            raise SplitError(f"weight {name} must be an integer, got {v!r}")
        if v < 0:
            raise SplitError(f"weight {name} must be non-negative, got {v}")
        out[name] = int(v)
    total = sum(out.values())
    if total != TOTAL_WEIGHT:
        raise SplitError(
            f"weights must sum to {TOTAL_WEIGHT}, got {total} "
            f"({out['train']}/{out['validation']}/{out['test']})"
        )
    return out


def normalize_seed(seed) -> int:
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise SplitError(f"seed must be an integer, got {seed!r}")
    return int(seed)


def split_identity(
    weights: dict[str, int], seed: int, version: int
) -> str:
    """参数身份 -> split_id（16 个十六进制字符）。

    身份只包含真正影响结果的参数：算法版本、已解析的输入 snapshot、
    三个权重、seed。文件排列、物理行号、清单键序都不在身份里。
    """
    payload = {
        "algorithm_version": ALGORITHM_VERSION,
        "input_version": version,
        "weights": dict(weights),
        "seed": seed,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


# ------------------------------------------------------------------- 对象散列
def _digest_prefix(seed: int, row_id: str) -> bytes:
    h = hashlib.sha256()
    h.update(str(seed).encode("utf-8"))
    h.update(b"\x00")
    h.update(row_id.encode("utf-8"))
    return h.digest()[:8]


def group_hash(seed: int, row_id: str) -> int:
    """row_id 与 seed 的稳定散列值（无符号 64 位整数）。

    公开它是为了让独立校验脚本/调用方在交付之外复算归属；分配用的是它
    对 10000 取模后的桶号（见 assign_subset）。
    """
    return int.from_bytes(_digest_prefix(seed, row_id), "big")


def assign_subset(seed: int, row_id: str, weights: dict[str, int]) -> str:
    """把对象分配到子集：只在 [0, 10000) 的散列桶上做区间切分。"""
    u = int.from_bytes(_digest_prefix(seed, row_id), "big") % TOTAL_WEIGHT
    boundary = weights["train"]
    if u < boundary:
        return "train"
    boundary += weights["validation"]
    if u < boundary:
        return "validation"
    return "test"


# ------------------------------------------------------------------- 路径
def _split_root(table: LakeTable) -> str:
    return os.path.join(table.root, SPLITS_DIRNAME)


def split_dir(table: LakeTable, split_id: str) -> str:
    return os.path.join(_split_root(table), split_id)


def subset_path(directory: str, subset: str) -> str:
    return os.path.join(directory, f"{subset}.parquet")


def read_manifest(directory: str) -> dict:
    return storage.read_json(os.path.join(directory, MANIFEST_NAME))


# ------------------------------------------------------------- 扫描与分组
def _resolve_input_version(table: LakeTable, version: int | None) -> int:
    """把调用参数解析成一个固定的输入版本号。

    version=None 表示“调用时 checkout 当前 HEAD“——在提交锁下读一次 HEAD，
    之后运行期间表再产生新 snapshot，本次也只用这个版本号。
    """
    if version is None:
        with table.lock():
            return table.current_version()
    if isinstance(version, bool) or not isinstance(version, int):
        raise SplitError(f"input snapshot must be an integer, got {version!r}")
    v = int(version)
    if not os.path.exists(mf.version_path(table.meta_root, v)):
        raise SplitError(f"input snapshot {v} does not exist")
    return v


def _iter_group_batches(table: LakeTable, manifest: dict, batch_size: int):
    """逐批产出 (row_id 列表, value 列表) 的可见行，全表不进 Python 列表。

    可见行 = 该 snapshot 的记录经位置删除过滤后的行（与 table.scan 语义
    一致）。这里按物理行号过滤，流式读取，只保留当前一个 RecordBatch。
    """
    deletes = table._all_deleted_positions(manifest)
    for fid in sorted(manifest["data_files"]):
        meta = manifest["data_files"][fid]
        path = os.path.join(table.root, meta["path"])
        dead = deletes.get(fid)
        pf = pq.ParquetFile(path)
        row_offset = 0
        for batch in pf.iter_batches(batch_size=batch_size):
            n = batch.num_rows
            if dead:
                keep = [i for i in range(n) if (row_offset + i) not in dead]
                if len(keep) != n:
                    batch = batch.take(pa.array(keep, pa.int64()))
            if batch.num_rows:
                yield (
                    batch.column("row_id").to_pylist(),
                    batch.column("value").to_pylist(),
                )
            row_offset += n


def collect_groups(
    table: LakeTable, manifest: dict, batch_size: int = DEFAULT_BATCH_SIZE
) -> dict:
    """扫描固定 snapshot 的全部可见行，按 row_id 聚成对象组。

    返回 {"groups": {row_id: [value, ...]}, "num_rows": int}。任何可见行
    row_id 为空 -> SplitError（空标识无法用于对象分组，整次产出拒绝，
    不写任何文件）。

    groups 的插入顺序是 (data_file_id, 物理行号) 的扫描顺序，同一 row_id
    的多条观测保持它们在输入快照中的相对次序；随后 _partition_groups
    按 row_id 排序输出，因此最终交付内容不依赖文件排列。
    """
    groups: dict[str, list] = {}
    seen = 0
    for ids, values in _iter_group_batches(table, manifest, batch_size):
        for rid, val in zip(ids, values):
            if rid is None or rid == "":
                raise SplitError(
                    "empty row_id cannot be used for object grouping; "
                    f"refusing the whole split (row #{seen} in input snapshot "
                    f"v{manifest['version']})"
                )
            groups.setdefault(rid, []).append(val)
            seen += 1
    return {"groups": groups, "num_rows": seen}


# --------------------------------------------------------------- 写 Parquet
def _write_subset(
    path: str, row_ids: list[str], values: list, split_id: str, subset: str
) -> dict:
    """写一个子集 Parquet，返回文件名、行数与对象数。"""
    table = pa.table(
        {
            "row_id": pa.array(row_ids, pa.string()),
            "value": pa.array(values, pa.int64()),
            "split": pa.array([subset] * len(row_ids), pa.string()),
            "split_id": pa.array([split_id] * len(row_ids), pa.string()),
        },
        schema=pa.schema(
            [
                ("row_id", pa.string()),
                ("value", pa.int64()),
                ("split", pa.string()),
                ("split_id", pa.string()),
            ]
        ),
    )
    tmp = path + ".tmp"
    pq.write_table(table, tmp)
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return {
        "path": os.path.basename(path),
        "num_rows": len(row_ids),
        "num_objects": len(set(row_ids)),
    }


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _cleanup_stale_staging(root: str) -> list[str]:
    """清理发布前崩溃留下的 `.staging-*` 残骸（只在持有 flock 时调用）。"""
    removed = []
    for name in sorted(os.listdir(root)):
        if not name.startswith(".staging-"):
            continue
        p = os.path.join(root, name)
        if os.path.isdir(p):
            shutil.rmtree(p, ignore_errors=True)
            removed.append(name)
    return removed


# ----------------------------------------------------------------- 计划分配
def _partition_groups(
    groups: dict[str, list], weights: dict[str, int], seed: int
) -> tuple[dict[str, list[str]], dict[str, list]]:
    """按散列分配对象组，返回 (每子集 row_id 列表, 每子集 value 列表)。

    对象的**归属**只由 assign_subset(seed, row_id) 决定，与遍历顺序无关；
    输出按 row_id 排序、组内按输入观测次序，保证内容稳定可核对。
    """
    per_subset: dict[str, list[tuple[str, list]]] = {s: [] for s in SUBSETS}
    for rid in sorted(groups):
        per_subset[assign_subset(seed, rid, weights)].append((rid, groups[rid]))

    row_ids: dict[str, list[str]] = {}
    values: dict[str, list] = {}
    for s in SUBSETS:
        rids: list[str] = []
        vals: list = []
        for rid, vlist in per_subset[s]:
            rids.extend([rid] * len(vlist))
            vals.extend(vlist)
        row_ids[s] = rids
        values[s] = vals
    return row_ids, values


# ------------------------------------------------------------------ 交付核对
def _verify_staged(
    staging: str,
    row_ids: dict[str, list[str]],
    values: dict[str, list],
    weights: dict[str, int],
    seed: int,
    input_rows: int,
) -> None:
    """逐项核对暂存目录里刚写出的三个文件。

    - 文件真实存在、能被 Parquet 读取；
    - 每个文件的行序与写入时计划的完全一致（含 value）；
    - 每个 row_id 按散列规则复算后仍落在它所在的子集；
    - 三个子集的对象集合两两不相交（对象不跨集合）；
    - 三个子集合起来恰好是输入的全部可见行（行数一致）。
    """
    all_ids: list[str] = []
    all_values: list = []
    for s in SUBSETS:
        p = subset_path(staging, s)
        if not os.path.exists(p):
            raise SplitError(f"staged subset {s} is missing")
        tab = pq.read_table(p)
        got_ids = tab.column("row_id").to_pylist()
        got_values = tab.column("value").to_pylist()
        if got_ids != row_ids[s]:
            raise SplitError(
                f"staged subset {s} rows differ from what was planned"
            )
        misplaced = [
            rid for rid in got_ids if assign_subset(seed, rid, weights) != s
        ]
        if misplaced:
            raise SplitError(
                f"staged subset {s} contains objects assigned elsewhere: "
                f"{misplaced[:3]}"
            )
        all_ids.extend(got_ids)
        all_values.extend(got_values)
    if len(all_ids) != input_rows:
        raise SplitError(
            f"subsets total {len(all_ids)} rows != input visible rows {input_rows}"
        )
    # 对象不跨集合（按 row_id 集合两两相交检查；同一子集内一个对象的多
    # 条观测本来就会重复 row_id，这是合法且有意的）。
    per_subset_objects = {
        s: set(row_ids[s]) for s in SUBSETS
    }
    for i, s in enumerate(SUBSETS):
        for s2 in SUBSETS[i + 1:]:
            overlap = per_subset_objects[s] & per_subset_objects[s2]
            if overlap:
                raise SplitError(
                    f"object {next(iter(overlap))!r} appears in both {s} and {s2}"
                )
    planned_ids = sum((row_ids[s] for s in SUBSETS), [])
    planned_values = sum((values[s] for s in SUBSETS), [])
    if sorted(all_ids) != sorted(planned_ids):
        raise SplitError("staged totals disagree with planned totals")
    if all_values != planned_values:
        raise SplitError("staged values disagree with planned values")


# ------------------------------------------------------------------- 主入口
def export_subset(
    directory: str, subset: str, dest: str | None = None
) -> str:
    """把一个已交付子集复制成用户可直接读取的独立 Parquet 文件。

    交付目录里的三个 Parquet 本身就是标准 Parquet（带 split/split_id 标签
    列），可直接读取；这个辅助函数用于把其中一份复制到调用方指定的位置
    （先写 .tmp 再 rename，不暴露半成品）。默认落点是交付目录的**同级**
    文件，不往已交付目录里加东西。
    """
    if subset not in SUBSETS:
        raise SplitError(f"unknown subset {subset!r}; expected one of {SUBSETS}")
    src = subset_path(directory, subset)
    if not os.path.exists(src):
        raise SplitError(f"subset {subset} not found in {directory}")
    if dest is None:
        split_id = os.path.basename(os.path.abspath(directory))
        dest = os.path.join(
            os.path.dirname(os.path.abspath(directory)),
            f"{split_id}-{subset}.export.parquet",
        )
    tmp = dest + ".tmp"
    shutil.copyfile(src, tmp)
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    os.replace(tmp, dest)
    return dest


def create_split(
    table: LakeTable,
    version: int | None,
    train: int,
    validation: int,
    test: int,
    seed: int,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict:
    """产出（或取回）一个按 row_id 成组抽样的交付目录。

    参数：湖表、输入 snapshot（None = 调用时固定当前 HEAD）、三个整数
    权重（非负、总和 10000）、seed。

    重复调用语义：
      - 完全相同的参数（相同 snapshot + 相同权重 + 相同 seed）-> 同一个
        split_id -> 直接返回原清单（`reused=true`），不重写文件；
      - 参数变化 -> 新的 split_id -> 新目录，旧交付原样保留。
    """
    weights = normalize_weights(train, validation, test)
    seed = normalize_seed(seed)
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise SplitError(f"batch_size must be an integer, got {batch_size!r}")
    if batch_size <= 0:
        raise SplitError("batch_size must be positive")

    root = _split_root(table)
    os.makedirs(root, exist_ok=True)

    # 先固定输入版本号：运行期间 HEAD 再推进也不改变本次输入。
    v = _resolve_input_version(table, version)
    split_id = split_identity(weights, seed, v)
    final = split_dir(table, split_id)

    if os.path.isdir(final):
        # 已交付的完全相同参数：原样返回原清单（不重写、不替换旧数据）。
        manifest = read_manifest(final)
        if manifest.get("split_id") != split_id:
            raise SplitConflictError(
                f"split directory {final} holds a different identity"
            )
        _verify_delivered(final, weights, seed)
        manifest["reused"] = True
        return manifest

    # 持读者租约读取固定版本：读取期间 GC 不会删除本次输入文件，
    # 表产生的新 snapshot 与本次无关。
    with table.lease(v):
        base = table.manifest(v)
        with _SplitLock(table):
            # 锁内再查一次：两个进程同时产出同一身份时，后到者看到已发布
            # 目录后直接复用原交付，不会产出第二份或替换第一份。
            if os.path.isdir(final):
                manifest = read_manifest(final)
                _verify_delivered(final, weights, seed)
                manifest["reused"] = True
                return manifest

            # 目标确认不存在：本目录里的 .staging-* 只能是此前进程在发布
            # 边界崩溃留下的残骸（flock 保证没有其他在途发布），清掉后重做。
            _cleanup_stale_staging(root)

            groups_info = collect_groups(table, base, batch_size=batch_size)
            row_ids, values = _partition_groups(
                groups_info["groups"], weights, seed
            )
            input_rows = groups_info["num_rows"]
            input_objects = len(groups_info["groups"])

            manifest = _build_manifest(
                table, split_id, v, version is not None, weights, seed,
                input_rows, input_objects,
            )

            staging = os.path.join(
                root, f".staging-{split_id}-{uuid.uuid4().hex}"
            )
            os.makedirs(staging)
            try:
                for s in SUBSETS:
                    manifest["subsets"][s] = _write_subset(
                        subset_path(staging, s), row_ids[s], values[s],
                        split_id, s,
                    )
                for s in SUBSETS:
                    manifest["subsets"][s]["sha256"] = sha256_file(
                        subset_path(staging, s)
                    )
                _verify_staged(
                    staging, row_ids, values, weights, seed, input_rows
                )
                storage.atomic_write_json(
                    os.path.join(staging, MANIFEST_NAME), manifest
                )
                storage.fsync_dir(staging)
                _maybe_crash("staged")
                # 一次目录改名发布：此前目标路径不可见，此后整个交付可见。
                os.rename(staging, final)
                storage.fsync_dir(root)
                _maybe_crash("published")
            except BaseException:
                shutil.rmtree(staging, ignore_errors=True)
                raise
    manifest["reused"] = False
    return manifest


def _build_manifest(
    table, split_id, version, explicit, weights, seed,
    input_rows, input_objects,
) -> dict:
    return {
        "split_id": split_id,
        "algorithm_version": ALGORITHM_VERSION,
        "algorithm": "sha256(seed,row_id)[:8] -> uint64 % 10000 -> weighted bands",
        "weights": dict(weights),
        "weight_total": TOTAL_WEIGHT,
        "seed": seed,
        "input_version": version,
        "input_version_explicit": explicit,
        "input_snapshot": {
            "version": version,
            "version_explicit": explicit,
            "num_visible_rows": input_rows,
            "num_objects": input_objects,
        },
        "proportionality_notice": PROPORTIONALITY_NOTICE,
        "grouping": {
            "key": "row_id",
            "rule": "all visible rows sharing a row_id land in exactly one subset",
        },
        "subsets": {},
        "created_at": time.time(),
        "generator": "lake.split",
    }


def _verify_delivered(directory: str, weights: dict[str, int], seed: int) -> dict:
    """核对一个已交付目录：清单、文件、校验和、分组与归属全部一致。

    这是 `check_split`（独立校验脚本）的核心，也用于“相同参数再次调用”
    时确认返回的确实是完好的原切分。
    """
    manifest_path = os.path.join(directory, MANIFEST_NAME)
    if not os.path.exists(manifest_path):
        raise SplitError(f"delivery {directory} has no manifest")
    manifest = read_manifest(directory)
    if manifest.get("algorithm_version") != ALGORITHM_VERSION:
        raise SplitError("manifest algorithm_version is not supported")
    m_weights = manifest.get("weights") or {}
    if m_weights != dict(weights):
        raise SplitError("manifest weights disagree with requested weights")
    if manifest.get("seed") != seed:
        raise SplitError("manifest seed disagrees with requested seed")

    total_rows = 0
    per_subset: dict[str, dict] = {}
    object_sets: dict[str, set] = {}
    for s in SUBSETS:
        meta = (manifest.get("subsets") or {}).get(s)
        if meta is None:
            raise SplitError(f"manifest is missing subset {s}")
        p = subset_path(directory, s)
        if not os.path.exists(p):
            raise SplitError(f"delivery is missing {s}.parquet")
        got_sha = sha256_file(p)
        if got_sha != meta.get("sha256"):
            raise SplitError(f"{s}.parquet checksum mismatch")
        tab = pq.read_table(p)
        ids = tab.column("row_id").to_pylist()
        if tab.num_rows != meta.get("num_rows"):
            raise SplitError(f"{s}.parquet row count disagrees with manifest")
        if "split" in tab.column_names:
            bad = [x for x in tab.column("split").to_pylist() if x != s]
            if bad:
                raise SplitError(
                    f"{s}.parquet contains rows tagged for another subset"
                )
        misplaced = [i for i in ids if assign_subset(seed, i, m_weights) != s]
        if misplaced:
            raise SplitError(
                f"{s}.parquet contains objects assigned elsewhere: "
                f"{misplaced[:3]}"
            )
        objects = set(ids)
        if len(objects) != meta.get("num_objects"):
            raise SplitError(f"{s}.parquet object count disagrees with manifest")
        object_sets[s] = objects
        total_rows += tab.num_rows
        per_subset[s] = {"num_rows": tab.num_rows, "num_objects": len(objects)}

    # 对象不跨集合：三个文件的 row_id 集合两两不相交（row_id 是对象组）。
    for i, s in enumerate(SUBSETS):
        for s2 in SUBSETS[i + 1:]:
            overlap = object_sets[s] & object_sets[s2]
            if overlap:
                raise SplitError(
                    f"object {next(iter(overlap))!r} appears in both "
                    f"{s} and {s2}"
                )

    snap = manifest.get("input_snapshot") or {}
    if total_rows != snap.get("num_visible_rows"):
        raise SplitError(
            f"subsets total {total_rows} rows != manifest input rows "
            f"{snap.get('num_visible_rows')}"
        )
    if sum(per_subset[s]["num_objects"] for s in SUBSETS) != snap.get(
        "num_objects"
    ):
        raise SplitError("subsets object count does not add up to input objects")

    return {
        "split_id": manifest.get("split_id"),
        "input_version": manifest.get("input_version"),
        "seed": manifest.get("seed"),
        "weights": m_weights,
        "subsets": per_subset,
        "num_rows": total_rows,
        "num_objects": snap.get("num_objects"),
        "ok": True,
    }


def check_split(directory: str) -> dict:
    """独立分组校验入口：只读交付目录，不看湖表。

    校验内容：
      - manifest / 三个 Parquet 都在，文件名与清单一致；
      - 每个文件的 SHA-256、行数、对象数与清单一致；
      - 每个 row_id 按清单参数复算的归属与文件所在子集一致；
      - 三个子集的 row_id 集合两两不相交（对象不跨集合）；
      - 三个子集合起来恰好等于清单记录的全部输入可见行，且没有重复。
    """
    manifest = read_manifest(directory)
    weights = manifest.get("weights") or {}
    seed = manifest.get("seed")
    if not isinstance(seed, int):
        raise SplitError("manifest seed is missing or not an integer")
    if manifest.get("algorithm_version") != ALGORITHM_VERSION:
        raise SplitError("manifest algorithm_version is not supported")
    return _verify_delivered(directory, weights, seed)


def load_split(directory: str) -> dict:
    return read_manifest(directory)
