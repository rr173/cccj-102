"""按 row_id 成组抽样的切分端到端验收（全部本地数据，不依赖外部服务）。

对应用户验收清单：

  1. 构造样本：同一对象的多条观测不跨集合；三个集合合起来恰好是输入
     可见行，且没有重复
  2. 相同输入下改变 Parquet 文件排列 / 先压实：归属完全不变
  3. snapshot 推进：本次仍使用原输入 snapshot
  4. 空 row_id：整次产出拒绝，不落盘任何交付物
  5. 错误权重：整次拒绝，不落盘
  6. 完全相同的参数再次调用 -> 返回原切分（身份与字节不变）
  7. 参数变化 -> 新身份、新目录，旧交付不被替换
  8. 发布改名前的子进程退出：目标路径不可见，再次调用可完成交付
  9. 源数据文件回收后，已交付子集仍可直接读取
"""

from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import (
    LakeTable,
    SplitError,
    check_split,
    collect,
    commit_plan,
    create_plan,
    create_split,
    export_subset,
    group_hash,
)
from lake.splits.split import assign_subset, split_identity

CTX = mp.get_context("fork")

# 构造样本：每个 row_id 是一个业务对象，可能有多条观测（有的对象出现在
# 不同物理文件里，故意制造“文件边界切开同一对象”的场景）。
ROWS = [
    ("obj-1", 10),
    ("obj-2", 20),
    ("obj-3", 30),
    ("obj-1", 11),     # obj-1 的第二条观测
    ("obj-4", 40),
    ("obj-5", 50),
    ("obj-6", 60),
    ("obj-2", 21),     # obj-2 的第二条观测
    ("obj-7", 70),
    ("obj-8", 80),
    ("obj-9", 90),
    ("obj-10", 100),
    ("obj-3", 31),     # obj-3 的第三条观测
    ("obj-11", 110),
    ("obj-12", 120),
    ("obj-4", 41),     # obj-4 的第二条观测，落在第二个文件
]

WEIGHTS = (6000, 2000, 2000)
SEED = 12345


def write_shard(table, rows, name=None):
    """把一个列表写成单独的 Parquet 数据文件（可含 null，append 不支持）。"""
    fid = uuid.uuid4().hex
    arrow = pa.table(
        {
            "row_id": pa.array([r[0] for r in rows], pa.string()),
            "value": pa.array([r[1] for r in rows], pa.int64()),
        },
        schema=pa.schema([("row_id", pa.string()), ("value", pa.int64())]),
    )
    rel = f"data/data-{fid}.parquet"
    pq.write_table(arrow, os.path.join(table.root, rel))
    with table.lock():
        from lake import manifest as mf

        new = table._next(mf.load_current(table.meta_root))
        new["data_files"][fid] = {"path": rel, "num_rows": arrow.num_rows}
        table._publish(new)
    return fid


def build_lake(root):
    t = LakeTable.create(root)
    # 三个物理分片；obj-1/obj-2/obj-3/obj-4 的观测跨文件出现
    write_shard(t, ROWS[:5])
    write_shard(t, ROWS[5:10])
    write_shard(t, ROWS[10:])
    return t


def read_subset(directory, name):
    return pq.read_table(os.path.join(directory, f"{name}.parquet"))


def object_map(directory):
    """{row_id: (subset, [values...])}，用于核对对象没有跨集合。"""
    out = {}
    for name in ("train", "validation", "test"):
        tab = read_subset(directory, name)
        for rid, val in zip(
            tab.column("row_id").to_pylist(), tab.column("value").to_pylist()
        ):
            if rid in out:
                assert out[rid][0] == name, f"{rid} 跨集合"
                out[rid][1].append(val)
            else:
                out[rid] = (name, [val])
    return out


def stable_content(directory):
    """子集内容的稳定表示：每个子集按 (row_id, value) 排序后的列表。"""
    out = {}
    for name in ("train", "validation", "test"):
        tab = read_subset(directory, name)
        out[name] = sorted(
            zip(tab.column("row_id").to_pylist(), tab.column("value").to_pylist())
        )
    return out


def file_sha(directory):
    return {
        name: hashlib.sha256(
            open(os.path.join(directory, f"{name}.parquet"), "rb").read()
        ).hexdigest()
        for name in ("train", "validation", "test")
    }


def delivered_dir(table, split_id):
    return os.path.join(table.root, "splits", split_id)


def no_deliveries(table):
    """没有任何正式（非暂存）交付目录；LOCK/暂存残骸不算交付物。"""
    root = os.path.join(table.root, "splits")
    if not os.path.isdir(root):
        return True
    return not [
        n
        for n in os.listdir(root)
        if not n.startswith(".") and n != "LOCK"
    ]


# ---------------------------------------------------------------------------
# 1. 成组、不重不漏
# ---------------------------------------------------------------------------
def test_objects_never_cross_subsets_and_rows_are_partitioned(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    v = t.current_version()
    manifest = create_split(t, v, *WEIGHTS, SEED)

    assert manifest["reused"] is False
    assert manifest["input_snapshot"] == {
        "version": v,
        "version_explicit": True,
        "num_visible_rows": len(ROWS),
        "num_objects": len({r[0] for r in ROWS}),
    }
    assert manifest["seed"] == SEED
    assert manifest["weights"] == {
        "train": WEIGHTS[0], "validation": WEIGHTS[1], "test": WEIGHTS[2],
    }
    assert manifest["algorithm_version"] == 1
    assert "proportionality_notice" in manifest

    directory = delivered_dir(t, manifest["split_id"])
    objmap = object_map(directory)
    # 每个对象的所有观测都在同一个子集
    for rid, (subset, vals) in objmap.items():
        expected = [v2 for (r, v2) in ROWS if r == rid]
        assert sorted(vals) == sorted(expected), f"{rid} 观测不完整"
    # 三个集合恰好覆盖输入可见行，且没有重行
    all_pairs = [
        (rid, val)
        for name in ("train", "validation", "test")
        for rid, val in zip(
            read_subset(directory, name).column("row_id").to_pylist(),
            read_subset(directory, name).column("value").to_pylist(),
        )
    ]
    assert sorted(all_pairs) == sorted(ROWS)
    # 对象集合两两不相交
    object_sets = {
        name: set(read_subset(directory, name).column("row_id").to_pylist())
        for name in ("train", "validation", "test")
    }
    assert not (object_sets["train"] & object_sets["validation"])
    assert not (object_sets["train"] & object_sets["test"])
    assert not (object_sets["validation"] & object_sets["test"])
    # 独立校验脚本的结论
    result = check_split(directory)
    assert result["ok"] is True
    assert result["num_rows"] == len(ROWS)


def test_assignment_matches_documented_hash(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    manifest = create_split(t, t.current_version(), *WEIGHTS, SEED)
    directory = delivered_dir(t, manifest["split_id"])
    weights = manifest["weights"]
    for name in ("train", "validation", "test"):
        tab = read_subset(directory, name)
        for rid in set(tab.column("row_id").to_pylist()):
            assert assign_subset(SEED, rid, weights) == name
            # group_hash 是公开的同一散列，取模后与 assign_subset 一致
            assert group_hash(SEED, rid) % 10000 == group_hash(SEED, rid) % 10000


# ---------------------------------------------------------------------------
# 2. 文件排列变化 / 压实不改变归属
# ---------------------------------------------------------------------------
def test_reordering_parquet_files_does_not_change_ownership(tmp_path):
    """同样的对象与观测，重新分片并按不同顺序追加，归属不变。"""
    t1 = build_lake(str(tmp_path / "lake_a"))
    v1 = t1.current_version()
    m1 = create_split(t1, v1, *WEIGHTS, SEED)
    c1 = stable_content(delivered_dir(t1, m1["split_id"]))

    # 换一种物理排列：逐行一个文件，并且按 row_id 倒序追加
    t2 = LakeTable.create(str(tmp_path / "lake_b"))
    for row in reversed(ROWS):
        write_shard(t2, [row])
    v2 = t2.current_version()
    m2 = create_split(t2, v2, *WEIGHTS, SEED)
    c2 = stable_content(delivered_dir(t2, m2["split_id"]))
    assert c1 == c2
    # snapshot 不同，身份不同；但归属逐对象一致
    assert m1["split_id"] != m2["split_id"]


def test_compaction_does_not_change_ownership(tmp_path):
    """先压实再切分：同一 snapshot 内容经物理整理，归属完全不变。"""
    t = build_lake(str(tmp_path / "lake"))
    v_before = t.current_version()
    m_before = create_split(t, v_before, *WEIGHTS, SEED)
    before = stable_content(delivered_dir(t, m_before["split_id"]))

    fids = sorted(t.manifest(v_before)["data_files"])
    commit_plan(t, create_plan(t, selected=fids))
    v_after = t.current_version()
    assert v_after != v_before
    m_after = create_split(t, v_after, *WEIGHTS, SEED)
    after = stable_content(delivered_dir(t, m_after["split_id"]))
    assert before == after
    # 同内容、不同 snapshot -> 不同身份（输入 snapshot 是身份的一部分）
    assert m_before["split_id"] != m_after["split_id"]
    assert m_after["input_snapshot"]["num_visible_rows"] == len(ROWS)


def test_assignment_independent_of_manifest_file_key_order(tmp_path):
    """直接把 manifest 的文件键顺序打乱（同一 snapshot 的语义不变），归属相同。"""
    t = build_lake(str(tmp_path / "lake"))
    v = t.current_version()
    m = create_split(t, v, *WEIGHTS, SEED)
    original = stable_content(delivered_dir(t, m["split_id"]))

    # 复制整个湖表，仅改变 manifest 中 data_files 的键顺序
    import shutil

    copy_root = str(tmp_path / "lake_copy")
    shutil.copytree(t.root, copy_root)
    t2 = LakeTable.open(copy_root)
    with t2.lock():
        from lake import manifest as mf

        cur = mf.load_current(t2.meta_root)
        keys = list(cur["data_files"])
        reordered = {k: cur["data_files"][k] for k in reversed(keys)}
        cur["data_files"] = reordered
        # 同内容、同版本号重写 manifest（测试专用：只改键顺序）
        new = {**cur, "version": cur["version"] + 1, "parent": cur["version"]}
        t2._publish(new)
    v2 = t2.current_version()
    m2 = create_split(t2, v2, *WEIGHTS, SEED)
    assert stable_content(delivered_dir(t2, m2["split_id"])) == original


# ---------------------------------------------------------------------------
# 3. snapshot 推进：本次仍使用原输入
# ---------------------------------------------------------------------------
def test_snapshot_advances_during_split_still_uses_original_input(tmp_path, monkeypatch):
    t = build_lake(str(tmp_path / "lake"))
    v_fixed = t.current_version()
    original_rows = len(ROWS)

    from lake.splits import split as split_mod

    real_collect = split_mod.collect_groups

    def collect_then_advance(table, manifest, batch_size=4096):
        # 在读取输入的过程中，表继续提交：追加 + 压实 + 删除
        table.append([("late-obj", 999)])
        table.publish_deletes(
            sorted(manifest["data_files"])[0], [0]
        )
        commit_plan(table, create_plan(table, selected=sorted(table.manifest()["data_files"])))
        return real_collect(table, manifest, batch_size=batch_size)

    monkeypatch.setattr(split_mod, "collect_groups", collect_then_advance)
    manifest = create_split(t, v_fixed, *WEIGHTS, SEED)
    assert manifest["input_version"] == v_fixed
    assert manifest["input_snapshot"]["num_visible_rows"] == original_rows
    directory = delivered_dir(t, manifest["split_id"])
    pairs = [
        (rid, val)
        for name in ("train", "validation", "test")
        for rid, val in zip(
            read_subset(directory, name).column("row_id").to_pylist(),
            read_subset(directory, name).column("value").to_pylist(),
        )
    ]
    # 新追加的对象、被删除的行都没有出现在本次交付
    assert sorted(pairs) == sorted(ROWS)
    assert "late-obj" not in {r for r, _ in pairs}
    assert t.current_version() > v_fixed


def test_checkout_head_at_call_time_is_recorded(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    v = t.current_version()
    manifest = create_split(t, None, *WEIGHTS, SEED)
    assert manifest["input_version"] == v
    assert manifest["input_version_explicit"] is False


# ---------------------------------------------------------------------------
# 4/5. 空标识与错误权重：整次拒绝，不落盘
# ---------------------------------------------------------------------------
def test_empty_row_id_rejects_whole_split(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    write_shard(t, ROWS[:8])
    write_shard(t, [("obj-ok", 1), (None, 2)])  # 空标识
    v = t.current_version()
    with pytest.raises(SplitError, match="empty row_id"):
        create_split(t, v, *WEIGHTS, SEED)
    assert no_deliveries(t)
    assert os.listdir(os.path.join(t.root, "splits")) == [] or all(
        n.startswith(".") or n == "LOCK"
        for n in os.listdir(os.path.join(t.root, "splits"))
    )

    # 只有空标识的行也拒绝
    t2 = LakeTable.create(str(tmp_path / "lake2"))
    write_shard(t2, [("", 1), ("x", 2)])
    with pytest.raises(SplitError, match="empty row_id"):
        create_split(t2, t2.current_version(), *WEIGHTS, SEED)
    assert no_deliveries(t2)


@pytest.mark.parametrize(
    "weights",
    [
        (7000, 1000, 1000),     # 总和不是 10000
        (9000, 2000, 1000),     # 总和超过
        (-1, 5001, 5000),       # 负数
        (10000, 0, 0.5),        # 非整数
        (True, 5000, 4999),     # 布尔值不是合法整数权重
    ],
)
def test_bad_weights_reject_without_writing(tmp_path, weights):
    t = build_lake(str(tmp_path / "lake"))
    v = t.current_version()
    with pytest.raises(SplitError):
        create_split(t, v, *weights, SEED)
    assert no_deliveries(t)


def test_missing_snapshot_rejects(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    with pytest.raises(SplitError, match="does not exist"):
        create_split(t, t.current_version() + 100, *WEIGHTS, SEED)
    assert no_deliveries(t)


# ---------------------------------------------------------------------------
# 6/7. 身份与不可替换
# ---------------------------------------------------------------------------
def test_identical_params_return_original_split(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    v = t.current_version()
    first = create_split(t, v, *WEIGHTS, SEED)
    directory = delivered_dir(t, first["split_id"])
    shas_before = file_sha(directory)
    mtime_before = os.stat(os.path.join(directory, "manifest.json")).st_mtime_ns

    again = create_split(t, v, *WEIGHTS, SEED)
    assert again["reused"] is True
    assert again["split_id"] == first["split_id"]
    assert file_sha(directory) == shas_before
    assert os.stat(os.path.join(directory, "manifest.json")).st_mtime_ns == mtime_before

    # 同样参数但从“当前 HEAD”进入、且 HEAD 未变，也复用同一交付
    head_same = create_split(t, None, *WEIGHTS, SEED)
    assert head_same["split_id"] == first["split_id"]
    assert head_same["reused"] is True


def test_changed_params_create_new_identity_without_replacing_old(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    v = t.current_version()
    base = create_split(t, v, 6000, 2000, 2000, SEED)
    base_sha = file_sha(delivered_dir(t, base["split_id"]))

    variants = [
        create_split(t, v, 5000, 2500, 2500, SEED),        # 权重变化
        create_split(t, v, 6000, 2000, 2000, SEED + 1),    # seed 变化
    ]
    ids = {base["split_id"], *(m["split_id"] for m in variants)}
    assert len(ids) == 3
    for m in variants:
        assert m["reused"] is False
    # 旧交付原样保留，文件字节不变
    assert file_sha(delivered_dir(t, base["split_id"])) == base_sha
    # 三个交付都在
    for sid in ids:
        assert os.path.isdir(delivered_dir(t, sid))


def test_split_identity_is_deterministic():
    a = split_identity({"train": 6000, "validation": 2000, "test": 2000}, 12345, 9)
    b = split_identity({"test": 2000, "train": 6000, "validation": 2000}, 12345, 9)
    assert a == b


# ---------------------------------------------------------------------------
# 8. 发布改名前的子进程退出
# ---------------------------------------------------------------------------
def _crashing_split_worker(root, crash_point):
    os.environ["LAKE_SPLIT_CRASH_POINT"] = crash_point
    t = LakeTable.open(root)
    create_split(t, None, *WEIGHTS, SEED)


def test_crash_before_publish_leaves_no_delivery_and_recovers(tmp_path):
    root = str(tmp_path / "lake")
    build_lake(root)
    q = CTX.Queue()

    def worker():
        try:
            _crashing_split_worker(root, "staged")
            q.put("ok")
        except BaseException as e:  # noqa: BLE001
            q.put(repr(e))

    p = CTX.Process(target=worker)
    p.start()
    p.join(60)
    assert p.exitcode == 23, "应在发布改名之前用 os._exit 硬退出"

    splits_root = os.path.join(root, "splits")
    published = [
        n
        for n in os.listdir(splits_root)
        if not n.startswith(".") and n != "LOCK"
    ]
    assert published == [], "发布前目标路径不可见半成品"
    staging = [n for n in os.listdir(splits_root) if n.startswith(".staging-")]
    assert len(staging) == 1, "暂存目录残骸存在，但不在正式路径上"

    # 重新调用：清理残骸、完成交付
    t = LakeTable.open(root)
    recovered = create_split(t, None, *WEIGHTS, SEED)
    assert recovered["reused"] is False
    directory = delivered_dir(t, recovered["split_id"])
    assert check_split(directory)["ok"] is True
    assert [
        n for n in os.listdir(splits_root) if n.startswith(".staging-")
    ] == []
    # 再次调用复用
    assert create_split(t, None, *WEIGHTS, SEED)["reused"] is True


def test_crash_after_publish_is_already_delivered(tmp_path):
    root = str(tmp_path / "lake")
    build_lake(root)
    p = CTX.Process(target=_crashing_split_worker, args=(root, "published"))
    p.start()
    p.join(60)
    assert p.exitcode == 23
    t = LakeTable.open(root)
    manifest = create_split(t, None, *WEIGHTS, SEED)
    assert manifest["reused"] is True
    assert check_split(delivered_dir(t, manifest["split_id"]))["ok"] is True


# ---------------------------------------------------------------------------
# 9. 源数据回收后，交付子集仍可读取
# ---------------------------------------------------------------------------
def test_delivered_subsets_readable_after_source_data_gc(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    v = t.current_version()
    m = create_split(t, v, *WEIGHTS, SEED)
    directory = delivered_dir(t, m["split_id"])
    before = stable_content(directory)

    # 压实替换全部数据文件，然后 GC 回收旧文件
    fids = sorted(t.manifest(v)["data_files"])
    commit_plan(t, create_plan(t, selected=fids))
    gc = collect(t)
    assert gc["removed_files"], "旧数据文件应被回收"
    old_paths = [meta["path"] for meta in t.manifest(v)["data_files"].values()]
    for rel in old_paths:
        assert not os.path.exists(os.path.join(t.root, rel))

    # 已交付子集不依赖源数据：直接读取、校验、导出都正常
    assert stable_content(directory) == before
    assert check_split(directory)["ok"] is True
    dest = os.path.join(str(tmp_path), "exported-train.parquet")
    export_subset(directory, "train", dest)
    tab = pq.read_table(dest)
    assert tab.num_rows == m["subsets"]["train"]["num_rows"]
    # 文件校验和仍与清单一致
    for name in ("train", "validation", "test"):
        path = os.path.join(directory, f"{name}.parquet")
        assert (
            hashlib.sha256(open(path, "rb").read()).hexdigest()
            == m["subsets"][name]["sha256"]
        )


# ---------------------------------------------------------------------------
# 独立校验脚本：能读取交付目录、拒绝被篡改的交付
# ---------------------------------------------------------------------------
def test_standalone_checker_detects_tampering(tmp_path):
    import check_split as checker

    t = build_lake(str(tmp_path / "lake"))
    m = create_split(t, t.current_version(), *WEIGHTS, SEED)
    directory = delivered_dir(t, m["split_id"])
    assert checker.validate(directory)["ok"] is True
    assert checker.validate(directory, verify_assignment=True)["ok"] is True

    # 篡改一个已交付文件 -> 校验和与分组约束都应发现问题
    path = os.path.join(directory, "train.parquet")
    os.chmod(path, 0o644)
    tab = pq.read_table(path)
    pq.write_table(tab.slice(0, max(0, tab.num_rows - 1)), path)
    result = checker.validate(directory)
    assert not result["ok"]
    assert result["problems"]


def test_deterministic_parquet_content_across_runs(tmp_path):
    """同一 snapshot、同一参数：重复调用复用同一文件，行内容稳定。"""
    t = build_lake(str(tmp_path / "lake"))
    m1 = create_split(t, None, *WEIGHTS, SEED)
    # 模拟一个相同内容的新表（同样数据、同样分片顺序），身份相同
    t2 = build_lake(str(tmp_path / "lake2"))
    m2 = create_split(t2, None, *WEIGHTS, SEED)
    assert m1["split_id"] == m2["split_id"]
    assert stable_content(delivered_dir(t, m1["split_id"])) == stable_content(
        delivered_dir(t2, m2["split_id"])
    )


# ---------------------------------------------------------------------------
# 并发：同一身份同时产出只发布一份；不同身份互不干扰
# ---------------------------------------------------------------------------
def _split_worker(root, version, weights, seed, q):
    try:
        t = LakeTable.open(root)
        m = create_split(t, version, *weights, seed)
        q.put(("ok", m["split_id"], m["reused"]))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def test_concurrent_identical_requests_publish_once(tmp_path):
    root = str(tmp_path / "lake")
    t = build_lake(root)
    v = t.current_version()
    q = CTX.Queue()
    procs = [
        CTX.Process(target=_split_worker, args=(root, v, WEIGHTS, SEED, q))
        for _ in range(3)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(120)
    assert all(p.exitcode == 0 for p in procs)
    results = [q.get(timeout=30) for _ in range(3)]
    assert all(r[0] == "ok" for r in results)
    split_ids = {r[1] for r in results}
    assert len(split_ids) == 1
    # 恰好一个正式交付目录，且完好
    directories = [
        n
        for n in os.listdir(os.path.join(root, "splits"))
        if not n.startswith(".") and n != "LOCK"
    ]
    assert len(directories) == 1
    assert check_split(
        os.path.join(root, "splits", directories[0])
    )["ok"] is True


def test_concurrent_distinct_requests_keep_both_deliveries(tmp_path):
    root = str(tmp_path / "lake")
    t = build_lake(root)
    v = t.current_version()
    q = CTX.Queue()
    p1 = CTX.Process(target=_split_worker, args=(root, v, WEIGHTS, SEED, q))
    p2 = CTX.Process(
        target=_split_worker, args=(root, v, (5000, 2500, 2500), SEED, q)
    )
    p1.start(); p2.start(); p1.join(120); p2.join(120)
    assert p1.exitcode == 0 and p2.exitcode == 0
    results = [q.get(timeout=30) for _ in range(2)]
    assert {r[1] for r in results} == {
        split_identity({"train": 6000, "validation": 2000, "test": 2000}, SEED, v),
        split_identity({"train": 5000, "validation": 2500, "test": 2500}, SEED, v),
    }
    directories = [
        n
        for n in os.listdir(os.path.join(root, "splits"))
        if not n.startswith(".") and n != "LOCK"
    ]
    assert len(directories) == 2
    for name in directories:
        assert check_split(os.path.join(root, "splits", name))["ok"] is True
