"""端到端自动化校验。所有用例都真实读写 Parquet，并实际 fork 子进程
来构造跨进程交错。运行：python3 -m pytest tests/ -v
"""

from __future__ import annotations

import multiprocessing as mp
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import (
    ConflictError,
    LakeTable,
    collect,
    commit_plan,
    create_plan,
    load_plan,
    verify,
)

CTX = mp.get_context("fork")
BARRIER_TIMEOUT = 60


def make_rows(prefix: str, n: int, start: int = 0):
    return [(f"{prefix}{i:04d}", i) for i in range(start, start + n)]


def drain(q, n):
    results = [q.get(timeout=30) for _ in range(n)]
    errors = [r for r in results if r[0] != "ok"]
    assert not errors, f"child process errors: {errors}"
    return results


# ---------------------------------------------------------------------------
# 1. 扫描：位置删除（含空文件、全删文件、同文件多条删除）
# ---------------------------------------------------------------------------
def test_scan_applies_position_deletes(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    # row_id 顺序故意与物理位置错开，证明删除按位置而非业务字段执行
    f1 = t.append([("z9", 90), ("a1", 11), ("m5", 55), ("b2", 22), ("k3", 33), ("q7", 77)])
    f2 = t.append(make_rows("b", 4))
    t.append([])  # 空文件
    f_dead = t.append(make_rows("d", 3))
    v_before_deletes = t.current_version()

    # 同一文件多条删除，分两个删除文件发布
    t.publish_deletes(f1, [1, 4])
    t.publish_deletes(f1, [0])
    t.publish_deletes(f2, [2])
    t.publish_deletes(f_dead, [0, 1, 2])  # 全删文件

    got = t.scan().column("row_id").to_pylist()
    # f1 物理位置 0,1,4 被删 -> 剩下 m5,b2,q7；f2 位置 2 被删；d* 全删
    assert sorted(got) == sorted(["m5", "b2", "q7", "b0000", "b0001", "b0003"])

    # 固定旧 manifest 的读者仍看到删除前的内容
    old = t.scan(v_before_deletes).column("row_id").to_pylist()
    assert len(old) == 6 + 4 + 3


# ---------------------------------------------------------------------------
# 2. 同步屏障交错：计划生成 -> 额外删除 -> 提交（删除被翻译）
# ---------------------------------------------------------------------------
def _compacter_child(root, fids, barrier, q):
    try:
        t = LakeTable.open(root)
        plan_id = create_plan(t, selected=fids)   # 1) 生成计划（基版本 V）
        barrier.wait(BARRIER_TIMEOUT)             # 放行删除写者
        barrier.wait(BARRIER_TIMEOUT)             # 等额外删除发布完毕
        v = commit_plan(t, plan_id)               # 3) 提交（应翻译新删除）
        q.put(("ok", v))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def _deleter_child(root, fid, positions, barrier, q):
    try:
        t = LakeTable.open(root)
        barrier.wait(BARRIER_TIMEOUT)             # 等计划生成
        t.publish_deletes(fid, positions)         # 2) 针对旧文件追加删除
        barrier.wait(BARRIER_TIMEOUT)             # 放行提交
        q.put(("ok", None))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def test_commit_translates_concurrent_deletes(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    f1 = t.append(make_rows("a", 10))
    f2 = t.append(make_rows("b", 10))
    t.publish_deletes(f1, [1])  # 基版本已有删除 -> 应被压实吸收
    before = t.scan()

    barrier = CTX.Barrier(2)
    q = CTX.Queue()
    pc = CTX.Process(target=_compacter_child, args=(root, [f1, f2], barrier, q))
    pd = CTX.Process(target=_deleter_child, args=(root, f1, [3], barrier, q))
    pc.start()
    pd.start()
    pc.join(90)
    pd.join(90)
    assert pc.exitcode == 0 and pd.exitcode == 0
    drain(q, 2)

    # 内容校验：before 基础上再删掉 a0003（f1 物理位置 3）
    expected = before.filter(
        pa.compute.not_equal(before.column("row_id"), pa.scalar("a0003"))
    )
    verify.assert_same_multiset(t.scan(), expected)

    # 结构校验：两个旧文件合并成一个；旧删除被吸收；
    # 新到的删除被翻译到新文件位置（旧位置 3 -> 新位置 2，因位置 1 的行已被剔除）
    m = t.manifest()
    assert len(m["data_files"]) == 1
    (merged_id, merged_meta), = m["data_files"].items()
    assert merged_meta["num_rows"] == 19
    assert len(m["delete_files"]) == 1
    (did, dmeta), = m["delete_files"].items()
    assert dmeta["target"] == merged_id
    dt = pq.read_table(os.path.join(root, dmeta["path"]))
    assert dt.column("data_file_id").to_pylist() == [merged_id]
    assert dt.column("pos").to_pylist() == [2]


# ---------------------------------------------------------------------------
# 3. 追加合并：计划生成后另一写者追加文件，提交必须保留
# ---------------------------------------------------------------------------
def test_commit_preserves_concurrent_appends(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    f1 = t.append(make_rows("a", 5))
    f2 = t.append(make_rows("b", 5))
    plan_id = create_plan(t, selected=[f1, f2])
    f3 = t.append(make_rows("c", 3))  # 计划生成之后的无关追加
    commit_plan(t, plan_id)

    m = t.manifest()
    assert f3 in m["data_files"], "并发追加的文件必须保留"
    assert len(m["data_files"]) == 2  # 合并文件 + 追加文件
    assert len(t.row_ids()) == 13
    assert sorted(t.row_ids()) == sorted(
        [r[0] for r in make_rows("a", 5) + make_rows("b", 5) + make_rows("c", 3)]
    )


# ---------------------------------------------------------------------------
# 4. 两个压实者选中重叠文件：后到者冲突失败，且不能覆盖对方
# ---------------------------------------------------------------------------
def _compacter_a(root, fids, barrier, q):
    try:
        t = LakeTable.open(root)
        plan_id = create_plan(t, selected=fids)
        barrier.wait(BARRIER_TIMEOUT)   # 双方都生成完计划
        v = commit_plan(t, plan_id)     # A 先提交
        barrier.wait(BARRIER_TIMEOUT)   # 放行 B
        q.put(("ok", v))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def _compacter_b(root, fids, barrier, q):
    try:
        t = LakeTable.open(root)
        plan_id = create_plan(t, selected=fids)
        barrier.wait(BARRIER_TIMEOUT)   # 双方都生成完计划
        barrier.wait(BARRIER_TIMEOUT)   # 等 A 先提交
        try:
            commit_plan(t, plan_id)
        except ConflictError as e:
            q.put(("conflict", str(e)))
            return
        q.put(("err", "expected ConflictError but commit succeeded"))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def test_overlapping_compactors_conflict(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    f1 = t.append(make_rows("a", 4))
    f2 = t.append(make_rows("b", 4))
    f3 = t.append(make_rows("c", 4))
    expected = t.scan()

    barrier = CTX.Barrier(2)
    q = CTX.Queue()
    pa_ = CTX.Process(target=_compacter_a, args=(root, [f1, f2], barrier, q))
    pb = CTX.Process(target=_compacter_b, args=(root, [f2, f3], barrier, q))
    pa_.start()
    pb.start()
    pa_.join(90)
    pb.join(90)
    assert pa_.exitcode == 0 and pb.exitcode == 0
    results = [q.get(timeout=30) for _ in range(2)]  # "conflict" 是 B 的预期结果
    statuses = sorted(r[0] for r in results)
    assert statuses == ["conflict", "ok"], results

    # B 的失败不能覆盖 A 的成果：f1,f2 已合并，f3 原样保留
    m = t.manifest()
    assert f3 in m["data_files"]
    assert f1 not in m["data_files"] and f2 not in m["data_files"]
    assert len(m["data_files"]) == 2
    verify.assert_same_multiset(t.scan(), expected)
    # 失败计划留在 staging 供排查，标记为 failed
    staged = os.listdir(t.staging_dir)
    assert len(staged) == 1
    plan = load_plan(t, staged[0].removeprefix("compact-"))
    assert plan["state"] == "failed"


# ---------------------------------------------------------------------------
# 5. 发布前崩溃：候选留在 staging，不进入读路径，GC 负责清理
# ---------------------------------------------------------------------------
def _crashing_compacter(root, fids):
    t = LakeTable.open(root)
    create_plan(t, selected=fids)  # 候选已写入 staging
    os._exit(1)                    # 模拟崩溃：不提交、不清理


def test_crash_before_publish_leaves_no_trace_in_read_path(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    f1 = t.append(make_rows("a", 5))
    f2 = t.append(make_rows("b", 5))
    before = t.scan()

    p = CTX.Process(target=_crashing_compacter, args=(root, [f1, f2]))
    p.start()
    p.join(90)
    assert p.exitcode == 1  # 真的崩溃了

    # 候选确实存在，但只在 staging/，不被任何 manifest 引用
    staged = os.listdir(t.staging_dir)
    assert len(staged) == 1
    staged_files = os.listdir(os.path.join(t.staging_dir, staged[0]))
    assert any(name.startswith("data-") for name in staged_files)
    m = t.manifest()
    assert set(m["data_files"]) == {f1, f2}
    verify.assert_same_multiset(t.scan(), before)  # 读路径完全不受影响

    # GC 清理崩溃残留
    report = collect(t, staging_max_age_seconds=0)
    assert report["removed_staging"] == staged
    assert os.listdir(t.staging_dir) == []
    verify.assert_same_multiset(t.scan(), before)


# ---------------------------------------------------------------------------
# 6 & 7. 租约保护旧版本；释放租约后 GC 才真正清理
# ---------------------------------------------------------------------------
def test_lease_protects_old_version_then_release_allows_gc(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    f1 = t.append(make_rows("a", 5))
    f2 = t.append(make_rows("b", 5))
    t.publish_deletes(f1, [0])
    v1 = t.current_version()
    snapshot_v1 = t.scan(v1)
    m1 = t.manifest(v1)
    old_paths = [m["path"] for m in m1["data_files"].values()]
    old_paths += [m["path"] for m in m1["delete_files"].values()]

    with t.lease(v1) as lease:
        assert lease.version == v1
        # 另一写者完成压实，HEAD 前进，旧文件对新版本不再可见
        plan_id = create_plan(t, selected=[f1, f2])
        commit_plan(t, plan_id)
        assert t.current_version() > v1

        report = collect(t)
        assert v1 in report["protected_versions"]
        assert report["removed_files"] == [], "租约保护期间不得删除任何文件"
        for rel in old_paths:
            assert os.path.exists(os.path.join(root, rel)), f"{rel} 被误删"
        # 持租约的读者仍能读取旧快照
        verify.assert_same_multiset(t.scan(v1), snapshot_v1)

    # 租约释放后：旧文件被回收，当前版本文件完好
    report = collect(t)
    assert set(report["removed_files"]) == set(old_paths)
    for rel in old_paths:
        assert not os.path.exists(os.path.join(root, rel))
    cur = t.manifest()
    for meta in cur["data_files"].values():
        assert os.path.exists(os.path.join(root, meta["path"]))
    # 当前版本内容 = v1 快照（a0000 被删）压实后的结果
    verify.assert_same_multiset(t.scan(), snapshot_v1)


# ---------------------------------------------------------------------------
# 8. 压实吸收空文件与全删文件
# ---------------------------------------------------------------------------
def test_compaction_absorbs_empty_and_fully_deleted_files(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    f_empty = t.append([])
    f_dead = t.append(make_rows("d", 4))
    t.publish_deletes(f_dead, [0, 1, 2, 3])
    f_live = t.append(make_rows("e", 3))
    t.publish_deletes(f_live, [1])

    plan_id = create_plan(t, selected=[f_empty, f_dead, f_live])
    commit_plan(t, plan_id)

    m = t.manifest()
    assert len(m["data_files"]) == 1  # 只剩一个合并文件
    (_, merged), = m["data_files"].items()
    assert merged["num_rows"] == 2
    assert m["delete_files"] == {}  # 删除全部被吸收
    assert sorted(t.row_ids()) == ["e0000", "e0002"]


# ---------------------------------------------------------------------------
# 9. 跨进程原子发布点：写者持续提交，读者永不见撕裂状态
# ---------------------------------------------------------------------------
def _writer_child(root, n, q):
    try:
        t = LakeTable.open(root)
        for i in range(n):
            t.append([(f"w{i:03d}", i)])
        q.put(("ok", None))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def _reader_child(root, n, universe, q):
    try:
        t = LakeTable.open(root)
        last_version = -1
        for _ in range(n):
            v = t.current_version()
            assert v >= last_version, "HEAD 版本回退"
            last_version = v
            ids = t.scan(v).column("row_id").to_pylist()
            assert len(ids) == len(set(ids)), "同一快照内 row_id 重复"
            assert set(ids) <= universe, "读到不属于任何已提交版本的行"
        q.put(("ok", None))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def test_atomic_publish_point_cross_process(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    t.append([("seed", 0)])
    n_appends = 40
    universe = {"seed"} | {f"w{i:03d}" for i in range(n_appends)}

    q = CTX.Queue()
    procs = [CTX.Process(target=_writer_child, args=(root, n_appends, q))]
    procs += [
        CTX.Process(target=_reader_child, args=(root, 60, universe, q))
        for _ in range(2)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join(180)
    assert all(p.exitcode == 0 for p in procs)
    drain(q, len(procs))

    assert set(t.row_ids()) == universe
