"""LakeTable：建表、追加、发布位置删除、扫描、读者租约、分支绑定。

数据文件布局（全部在单机本地文件系统上）：

    <root>/
      data/                  已发布的数据文件与位置删除文件（不可变）
      staging/               未发布的压实候选（不在读路径上）
      metadata/
        HEAD                 正式数据（main）的尖端版本号（原子替换）
        branches.json        实验分支尖端 {name: tip}（无此文件 = 只有 main）
        LOCK                 flock 提交锁
        versions/v*.json     不可变 manifest 版本（全库唯一编号）
        leases/lease-*.json  读者租约

分支语义：

  - main 表示正式数据，`HEAD` 就是它的尖端；旧格式目录没有
    branches.json，第一次使用时自然对应 main，旧调用无需传分支参数。
  - 实验分支从某个已有提交建立，起初与来源共享全部底层内容文件
    （manifest 版本之间没有数据拷贝，只有引用关系）。
  - 每个 LakeTable 对象绑定一条分支（默认 main）。append /
    publish_deletes / ingest_batch / commit_plan 只推进绑定分支的尖端；
    版本号是全库唯一坐标，父提交指向本线尖端。
  - 读取（scan / row_ids / ingest_cursors / batch_record）默认解析绑定
    分支的尖端；显式 version 则是全库中的那个提交（可能来自任何分支）。

数据文件带稳定 row_id 列，但删除一律按物理行号（0 起）记录与执行，
绝不从业务字段（包括 row_id）反推行号。
"""

from __future__ import annotations

import copy
import os
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from . import manifest as mf
from . import storage
from .errors import BranchNotFoundError, ConflictError, LeaseError


DATA_SCHEMA = pa.schema([("row_id", pa.string()), ("value", pa.int64())])
DELETE_SCHEMA = pa.schema([("data_file_id", pa.string()), ("pos", pa.int64())])


class LakeTable:
    def __init__(self, root: str, branch: str = mf.MAIN_BRANCH):
        from .branches import validate_name

        validate_name(branch)
        self.root = os.path.abspath(root)
        self.branch = branch
        self.meta_root = os.path.join(self.root, "metadata")
        self.data_dir = os.path.join(self.root, "data")
        self.staging_dir = os.path.join(self.root, "staging")
        self.leases_dir = os.path.join(self.meta_root, "leases")

    # ------------------------------------------------------------------ 建表
    @classmethod
    def create(cls, root: str) -> "LakeTable":
        t = cls(root)
        for d in (
            t.meta_root,
            t.data_dir,
            t.staging_dir,
            t.leases_dir,
            mf.versions_dir(t.meta_root),
        ):
            os.makedirs(d, exist_ok=True)
        m0 = {
            "version": 0,
            "parent": None,
            "branch": mf.MAIN_BRANCH,
            "created_at": time.time(),
            "data_files": {},
            "delete_files": {},
            "ingest": {"cursors": {}, "batches": {}},
        }
        mf.publish_manifest(t.meta_root, m0)
        return t

    @classmethod
    def open(cls, root: str, branch: str = mf.MAIN_BRANCH) -> "LakeTable":
        """打开湖表并绑定一条分支；branch 默认 main（旧调用自然兼容）。"""
        from .branches import validate_name

        validate_name(branch)
        t = cls(root, branch=branch)
        if not os.path.exists(mf.head_path(t.meta_root)):
            raise FileNotFoundError(f"no lake table at {root}")
        if branch != mf.MAIN_BRANCH and branch not in mf.load_branches(t.meta_root):
            raise BranchNotFoundError(f"branch {branch!r} does not exist")
        return t

    # ------------------------------------------------------------------ 锁
    def lock(self) -> storage.FileLock:
        return storage.FileLock(os.path.join(self.meta_root, "LOCK"))

    # ------------------------------------------------------------- manifest
    def current_version(self) -> int:
        """绑定分支的尖端版本号（main 即 HEAD）。"""
        return mf.branch_tip(self.meta_root, self.branch)

    def manifest(self, version: int | None = None) -> dict:
        """version=None 取绑定分支尖端；显式版本则是全库坐标中的那个提交。"""
        v = self.current_version() if version is None else version
        return mf.load_manifest(self.meta_root, v)

    def _current_manifest(self) -> dict:
        return mf.load_manifest(self.meta_root, self.current_version())

    def _publish(self, manifest: dict) -> None:
        """把新提交发布到**绑定分支**的尖端（manifest 已不可变落盘）。

        提交前核验父提交正是绑定分支当前的尖端：另一条线推进版本不构成
        本线冲突，但本对象若基于旧尖端构造提交，则必须明确失败而不是
        悄悄把本线尖端倒退或旁路。
        """
        tip = self.current_version()
        if manifest.get("parent") != tip:
            raise ConflictError(
                f"stale commit on branch {self.branch!r}: parent "
                f"v{manifest.get('parent')} but branch tip is v{tip}"
            )
        mf.write_manifest(self.meta_root, manifest)
        if self.branch == mf.MAIN_BRANCH:
            mf.set_head(self.meta_root, manifest["version"])
        else:
            mf.set_branch_tip(self.meta_root, self.branch, manifest["version"])

    def _next(self, cur: dict) -> dict:
        """以本线尖端 cur 为父构造下一版本（版本号取全库最大值 + 1）。

        ingest 段（接入游标 + 批次索引）必须随每个新版本原样携带，
        这样追加 / 删除 / 压实 / 摄取任何一条提交路径都不会丢进度。
        旧格式 manifest 没有该段，按空状态补齐。
        """
        return {
            "version": mf.next_version(self.meta_root),
            "parent": cur["version"],
            "branch": self.branch,
            "created_at": time.time(),
            "data_files": dict(cur["data_files"]),
            "delete_files": dict(cur["delete_files"]),
            "ingest": copy.deepcopy(
                cur.get("ingest") or {"cursors": {}, "batches": {}}
            ),
        }

    # ---------------------------------------------------------------- 分支
    def checkout(self, name: str) -> "LakeTable":
        """切换访问对象：返回绑定到 name 分支的新 LakeTable（共享同一目录）。"""
        return LakeTable.open(self.root, branch=name)

    def create_branch(self, name: str, from_version: int | None = None) -> dict:
        """从某个已有提交建立命名实验分支；返回 {"name","tip","base_version"}。"""
        from .branches import create_branch

        return create_branch(self, name, from_version=from_version)

    def delete_branch(self, name: str) -> dict:
        """撤销实验分支（main 受保护）；其独占内容由之后的 GC 回收。"""
        from .branches import delete_branch

        return delete_branch(self, name)

    def list_branches(self) -> list[dict]:
        """列举全部存活分支（含 main）的尖端、父提交与提交信息。"""
        from .branches import list_branches

        return list_branches(self)

    def publish(self, expected_main_version: int) -> dict:
        """把当前绑定分支的尖端快进交给 main；仅当 main 是其祖先时允许。"""
        from .branches import publish

        return publish(self, expected_main_version)

    # ---------------------------------------------------------------- 追加
    def append(self, rows) -> str:
        """追加一个数据文件，返回新的 data_file_id。rows: [(row_id, value), ...]"""
        rows = list(rows)
        fid = uuid.uuid4().hex
        table = pa.table(
            {
                "row_id": pa.array([r[0] for r in rows], pa.string()),
                "value": pa.array([r[1] for r in rows], pa.int64()),
            },
            schema=DATA_SCHEMA,
        )
        rel = f"data/data-{fid}.parquet"
        tmp = os.path.join(self.data_dir, f".tmp-{fid}")
        pq.write_table(table, tmp)
        with self.lock():
            # 先把数据文件就位，再发布引用它的 manifest，最后翻转本线尖端。
            os.replace(tmp, os.path.join(self.root, rel))
            storage.fsync_dir(self.data_dir)
            new = self._next(self._current_manifest())
            new["data_files"][fid] = {"path": rel, "num_rows": len(rows)}
            self._publish(new)
        return fid

    # ---------------------------------------------------------- 位置删除
    def publish_deletes(self, data_file_id: str, positions) -> int:
        """针对当前分支可见的一个数据文件发布位置删除（0 起物理行号）。

        删除文件内每行显式记录 (data_file_id, pos)；一个删除文件只针对
        一个数据文件，便于压实时按文件整体吸收或翻译。只推进本线尖端。
        """
        positions = sorted({int(p) for p in positions})
        if not positions:
            raise ValueError("positions must be non-empty")
        with self.lock():
            cur = self._current_manifest()
            meta = cur["data_files"].get(data_file_id)
            if meta is None:
                raise KeyError(f"unknown data file {data_file_id}")
            already = self._deleted_positions(cur, data_file_id)
            for p in positions:
                if not 0 <= p < meta["num_rows"]:
                    raise ValueError(
                        f"position {p} out of range [0, {meta['num_rows']})"
                    )
                if p in already:
                    raise ValueError(f"position {p} already deleted")
            did = uuid.uuid4().hex
            rel = f"data/deletes-{did}.parquet"
            deletes = pa.table(
                {
                    "data_file_id": pa.array(
                        [data_file_id] * len(positions), pa.string()
                    ),
                    "pos": pa.array(positions, pa.int64()),
                },
                schema=DELETE_SCHEMA,
            )
            tmp = os.path.join(self.data_dir, f".tmp-{did}")
            pq.write_table(deletes, tmp)
            os.replace(tmp, os.path.join(self.root, rel))
            storage.fsync_dir(self.data_dir)
            new = self._next(cur)
            new["delete_files"][did] = {
                "path": rel,
                "target": data_file_id,
                "num_deletes": len(positions),
            }
            self._publish(new)
            return new["version"]

    # ---------------------------------------------------------------- 扫描
    def scan(self, version: int | None = None) -> pa.Table:
        """固定一个 manifest 版本，按其中的位置删除过滤后返回活行。

        version=None 取绑定分支的尖端；显式版本是全库坐标中的提交。
        位置过滤用 PyArrow 完成：pyarrow.parquet 按文件物理顺序读出行，
        与删除项的 0 起物理行号严格对应（不依赖任何业务字段）。
        """
        m = self.manifest(version)
        deletes = self._all_deleted_positions(m)
        out = []
        for fid in sorted(m["data_files"]):
            meta = m["data_files"][fid]
            t = pq.read_table(os.path.join(self.root, meta["path"]))
            dead = deletes.get(fid)
            if dead:
                keep = [i for i in range(t.num_rows) if i not in dead]
                t = t.take(pa.array(keep, pa.int64()))
            out.append(t)
        if not out:
            return DATA_SCHEMA.empty_table()
        return pa.concat_tables(out)

    def row_ids(self, version: int | None = None) -> list[str]:
        return self.scan(version).column("row_id").to_pylist()

    # ---------------------------------------------------------------- 租约
    @contextmanager
    def lease(self, version: int | None = None, require_files: bool = False):
        """读者租约：保护指定版本（默认绑定分支尖端）引用的全部文件不被 GC。

        租约的创建与 GC 都在同一把提交锁下执行，因此 GC 不会漏看
        一个刚刚创建好的租约；持租约期间可安全扫描旧版本。

        版本 manifest 也在同一临界区内读取并随租约一起产出
        （``lease.manifest``）：这样“定位版本 -> 建立保护”之间没有窗口，
        GC 不可能在两者之间删掉该版本引用的文件。

        ``require_files=True`` 时还会在同一临界区内确认该版本引用的数据/
        删除文件都在磁盘上：如果 GC 在租约建立之前已经回收了某个历史版本
        的文件，这里会以 LeaseError 明确拒绝，而不是让读取在途中失败。
        """
        with self.lock():
            v = self.current_version() if version is None else version
            if not os.path.exists(mf.version_path(self.meta_root, v)):
                raise LeaseError(f"version {v} no longer exists")
            m = mf.load_manifest(self.meta_root, v)
            if require_files:
                missing = [
                    meta["path"]
                    for group in ("data_files", "delete_files")
                    for meta in m[group].values()
                    if not os.path.exists(os.path.join(self.root, meta["path"]))
                ]
                if missing:
                    raise LeaseError(
                        f"version {v} files have been collected: "
                        f"{missing[:3]}"
                        + (" ..." if len(missing) > 3 else "")
                    )
            lid = uuid.uuid4().hex
            lpath = os.path.join(self.leases_dir, f"lease-{lid}.json")
            storage.atomic_write_json(
                lpath,
                {
                    "lease_id": lid,
                    "version": v,
                    "branch": self.branch,
                    "pid": os.getpid(),
                    "created_at": time.time(),
                },
            )
        try:
            yield SimpleNamespace(lease_id=lid, version=v, manifest=m)
        finally:
            try:
                os.unlink(lpath)
            except FileNotFoundError:
                pass

    # ------------------------------------------------------------ 微批摄取
    def ingest_batch(self, batch_key: str, messages) -> dict:
        """可重放的微批摄取事务（只推进绑定分支的尖端），见 lake/ingest.py。"""
        from .ingest import ingest_batch

        return ingest_batch(self, batch_key, messages)

    def ingest_cursors(self, version: int | None = None) -> dict:
        """查询绑定分支（或显式版本）各来源分片的消费游标。"""
        from .ingest import ingest_cursors

        return ingest_cursors(self, version)

    def batch_record(self, batch_key: str) -> dict | None:
        """查询绑定分支尖端已提交批次的记录；未提交返回 None。"""
        from .ingest import batch_record

        return batch_record(self, batch_key)

    # ------------------------------------------------------- 成组抽样切分
    def create_split(
        self,
        train: int,
        validation: int,
        test: int,
        seed: int,
        version: int | None = None,
        batch_size: int | None = None,
    ) -> dict:
        """按 row_id 成组抽样产出 train/validation/test 三个 Parquet 子集。

        详见 lake/splits/split.py：同一 row_id 的全部可见行必然落在同一个
        子集；分配只依赖 seed 与 row_id，与 Parquet 文件顺序、压实无关。
        """
        from .splits.split import DEFAULT_BATCH_SIZE, create_split

        return create_split(
            self,
            version,
            train,
            validation,
            test,
            seed,
            batch_size=DEFAULT_BATCH_SIZE if batch_size is None else batch_size,
        )

    # ------------------------------------------------------------ 内部工具
    def _all_deleted_positions(self, manifest: dict) -> dict[str, set[int]]:
        """读取该 manifest 引用的全部删除文件，按 data_file_id 归组。"""
        out: dict[str, set[int]] = defaultdict(set)
        for dm in manifest["delete_files"].values():
            t = pq.read_table(os.path.join(self.root, dm["path"]))
            for fid, pos in zip(
                t.column("data_file_id").to_pylist(), t.column("pos").to_pylist()
            ):
                out[fid].add(pos)
        return out

    def _deleted_positions(self, manifest: dict, data_file_id: str) -> set[int]:
        return self._all_deleted_positions(manifest).get(data_file_id, set())
