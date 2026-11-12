"""实验分支端到端验收：分支创建/列举/切换/撤销、全库唯一提交号、
分支隔离的写入与摄取游标、快进 publish、引用感知回收、重开持久化。

所有断言都核对**完整记录内容与状态变化**（行多重集、manifest 文件集合、
提交号/父提交、游标、receipt、磁盘文件存在性），不只核对分支名字。
运行：python3 -m pytest tests/test_branches.py -v
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os

import pyarrow as pa
import pytest

from lake import (
    BatchRejectedError,
    BranchBaseError,
    BranchExistsError,
    BranchMismatchError,
    BranchNameError,
    BranchNotFoundError,
    DivergedError,
    IdempotencyConflictError,
    LakeTable,
    ProtectedBranchError,
    PublishError,
    SourceIsMainError,
    StaleMainError,
    collect,
    commit_plan,
    create_plan,
    verify,
)
from lake import manifest as mf
from lake.workbench import query_arrow

CTX = mp.get_context("fork")


# ---------------------------------------------------------------- 工具
def table_of(rows, prefix="o"):
    return pa.table(
        {
            "row_id": pa.array([r[0] for r in rows], pa.string()),
            "value": pa.array([r[1] for r in rows], pa.int64()),
        }
    )


def rows_by_id(table):
    """有序 [(row_id, value)]，便于逐条核对完整记录内容。"""
    return sorted(
        zip(
            table.column("row_id").to_pylist(),
            table.column("value").to_pylist(),
        )
    )


def file_paths(manifest_obj):
    return sorted(
        [m["path"] for m in manifest_obj["data_files"].values()]
        + [m["path"] for m in manifest_obj["delete_files"].values()]
    )


def msgs(source, partition, offsets, prefix, base=0):
    return [
        {
            "source": source,
            "partition": partition,
            "offset": o,
            "row_id": f"{prefix}{o:04d}",
            "value": base + o,
        }
        for o in offsets
    ]


# ---------------------------------------------------------------------------
# 1. 旧格式目录第一次使用自然对应 main；已有调用无需传分支参数
# ---------------------------------------------------------------------------
def _strip_branch_metadata(root):
    """删除 branches.json 并把 manifest 改回旧格式（无 branch/ingest 扩展），
    模拟“此前创建的表”。"""
    meta = os.path.join(root, "metadata")
    bpath = os.path.join(meta, "branches.json")
    if os.path.exists(bpath):
        os.unlink(bpath)
    for name in os.listdir(mf.versions_dir(meta)):
        path = os.path.join(mf.versions_dir(meta), name)
        with open(path, "r", encoding="utf-8") as f:
            m = json.load(f)
        m.pop("branch", None)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(m, f)


def test_legacy_directory_maps_to_main_and_old_calls_work(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    f1 = t.append([("legacy-1", 1), ("legacy-2", 2)])
    t.publish_deletes(f1, [0])
    _strip_branch_metadata(root)

    # 旧入口 LakeTable.open(root) 不传分支：就是 main，读写与压实全部可用
    t = LakeTable.open(root)
    assert t.branch == "main"
    assert t.row_ids() == ["legacy-2"]
    assert t.current_version() == mf.current_version(t.meta_root)
    assert [b["name"] for b in t.list_branches()] == ["main"]

    f2 = t.append([("legacy-3", 3)])
    t.publish_deletes(f2, [0])
    r = t.ingest_batch("legacy-batch", msgs("s", 0, [0, 1], "n"))
    assert r["cursors"] == {"s": {"0": 2}}
    plan = create_plan(t, max_rows=10_000)
    commit_plan(t, plan)
    assert sorted(t.row_ids()) == ["legacy-2", "n0000", "n0001"]
    assert t.ingest_cursors() == {"s": {"0": 2}}

    # 第一个实验分支从 main 尖端建立，起初共享全部底层内容
    info = t.create_branch("exp-legacy")
    assert info["base_version"] == t.current_version()
    assert info["copied_versions"] == 0
    assert info["shared_files"] == len(t.manifest()["data_files"]) + len(
        t.manifest()["delete_files"]
    )
    e = t.checkout("exp-legacy")
    assert rows_by_id(e.scan()) == rows_by_id(t.scan())
    assert e.current_version() == t.current_version()


# ---------------------------------------------------------------------------
# 2. 分支名字校验：非法名字与冲突明确报错，且不改变任何状态
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "bad",
    ["", " main", "main ", "1bad name", "with/slash", ".hidden", "..", "x" * 65,
     "-leading", "bad:name", "metadata"],
)
def test_invalid_branch_names_are_rejected(tmp_path, bad):
    t = LakeTable.create(str(tmp_path / "lake"))
    t.append([("a", 1)])
    before = t.current_version()
    before_data = sorted(t.row_ids())
    with pytest.raises((BranchNameError, BranchExistsError)):
        t.create_branch(bad)
    assert t.current_version() == before
    assert sorted(t.row_ids()) == before_data
    assert mf.load_branches(t.meta_root) == {}
    assert not os.path.exists(mf.branches_path(t.meta_root))


def test_duplicate_and_reserved_branch_names_are_rejected(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    t.append([("a", 1)])
    v = t.current_version()
    t.create_branch("exp")
    with pytest.raises(BranchExistsError, match="already exists"):
        t.create_branch("exp")
    with pytest.raises(BranchExistsError, match="reserved"):
        t.create_branch("main")
    with pytest.raises(BranchExistsError, match="reserved"):
        t.create_branch("MAIN")
    # 失败不改变任何分支尖端
    assert mf.load_branches(t.meta_root) == {"exp": v}
    assert mf.current_version(t.meta_root) == v

    with pytest.raises(ProtectedBranchError, match="cannot be deleted"):
        t.delete_branch("main")
    with pytest.raises(BranchNotFoundError, match="does not exist"):
        t.delete_branch("no-such-branch")
    with pytest.raises(BranchBaseError, match="does not exist"):
        t.create_branch("exp-base", from_version=999)


def test_checkout_unknown_branch_is_rejected(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    with pytest.raises(BranchNotFoundError):
        t.checkout("nope")
    with pytest.raises(BranchNotFoundError):
        LakeTable.open(t.root, branch="nope")


# ---------------------------------------------------------------------------
# 3. 分支操作：创建 / 列举 / 切换 / 撤销，共享底层内容而不复制
# ---------------------------------------------------------------------------
def test_branch_create_list_checkout_delete(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    t.append([("base-1", 1), ("base-2", 2)])
    base_v = t.current_version()
    base_manifest = t.manifest()
    base_paths = file_paths(base_manifest)

    created = t.create_branch("exp-a", from_version=base_v)
    assert created["tip"] == base_v and created["base_version"] == base_v
    e = t.checkout("exp-a")
    assert e.branch == "exp-a"
    assert e.current_version() == base_v
    # 起初共享：同一组底层文件、同一 manifest，不产生任何新文件
    assert file_paths(e.manifest()) == base_paths
    assert sorted(os.listdir(t.data_dir)) == sorted(
        os.path.basename(p) for p in base_paths
    )
    listed = {b["name"]: b for b in t.list_branches()}
    assert set(listed) == {"main", "exp-a"}
    assert listed["main"]["official"] is True
    assert listed["exp-a"]["official"] is False
    assert listed["exp-a"]["tip"] == base_v

    # 切换到 main / 实验分支：同一物理目录的不同访问对象
    assert t.branch == "main"
    assert rows_by_id(t.scan()) == rows_by_id(e.scan())

    deleted = t.delete_branch("exp-a")
    assert deleted["deleted_tip"] == base_v
    assert mf.load_branches(t.meta_root) == {}
    assert [b["name"] for b in t.list_branches()] == ["main"]
    with pytest.raises(BranchNotFoundError):
        t.checkout("exp-a")
    # main 完全不受影响
    assert rows_by_id(t.scan()) == [("base-1", 1), ("base-2", 2)]


def test_branch_from_older_commit_shares_that_snapshot(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    t.append([("a", 1)])
    v1 = t.current_version()
    t.append([("b", 2)])
    v2 = t.current_version()
    t.append([("c", 3)])
    info = t.create_branch("from-v1", from_version=v1)
    e = t.checkout("from-v1")
    assert info["tip"] == v1
    assert sorted(e.row_ids()) == ["a"]        # 从历史提交建立，不是最新内容
    assert sorted(t.row_ids()) == ["a", "b", "c"]
    # 实验线从 v1 继续发展；main 保持自己的 v2/v3
    e.append([("exp", 9)])
    assert e.current_version() != v2
    assert sorted(t.row_ids()) == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# 4. 全库提交编号唯一；父子关系与各分支尖端独立记录
# ---------------------------------------------------------------------------
def test_version_numbers_unique_across_branches_with_parent_links(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    t.append([("m1", 1)])                    # v1 main
    t.create_branch("exp", from_version=t.current_version())  # exp tip v1
    e = t.checkout("exp")
    e.append([("e1", 10)])                   # v2 exp
    t.append([("m2", 2)])                    # v3 main
    e.append([("e2", 20)])                   # v4 exp
    t.create_branch("exp2", from_version=e.current_version())
    e2 = t.checkout("exp2")
    e2.append([("x", 30)])                   # v5 exp2

    tips = mf.all_tips(t.meta_root)
    assert tips == {"main": 3, "exp": 4, "exp2": 5}
    assert set(tips.values()) == {3, 4, 5}

    # 每个提交文件的 version 与文件名一致，parent 是**本线**前驱，branch 是作者线
    tree = mf.ancestors(t.meta_root, 5)          # 5 -> 4 -> 2 -> 1 -> 0
    for v, expect_parent, expect_branch in [
        (1, 0, "main"),
        (2, 1, "exp"),
        (4, 2, "exp"),
        (5, 4, "exp2"),
    ]:
        m = mf.load_manifest(t.meta_root, v)
        assert m["version"] == v
        assert m["parent"] == expect_parent
        assert m["branch"] == expect_branch
    # main 线在图中另有一条提交（v3, parent=v1），不进入实验链
    m3 = mf.load_manifest(t.meta_root, 3)
    assert m3["parent"] == 1 and m3["branch"] == "main"
    assert 3 not in tree

    # main 线只看得到自己的链：m1/m2 的行，绝无实验行
    assert rows_by_id(t.scan()) == [("m1", 1), ("m2", 2)]
    assert rows_by_id(e.scan()) == [("e1", 10), ("e2", 20), ("m1", 1)]
    assert rows_by_id(e2.scan()) == [
        ("e1", 10), ("e2", 20), ("m1", 1), ("x", 30)
    ]
    # 每个提交都在数据字典里**新增**自己那一行；共享的只有共同祖先的引用。
    # 「当前尖端 vs 父提交」的差集应恰好是本次提交写入的那个文件。
    for v, parent in ((1, 0), (2, 1), (3, 1), (4, 2), (5, 4)):
        cur = mf.load_manifest(t.meta_root, v)
        prev = mf.load_manifest(t.meta_root, parent)
        added = set(cur["data_files"]) - set(prev["data_files"])
        assert len(added) == 1, f"v{v} 应恰好新增一个数据文件引用"
        removed = set(prev["data_files"]) - set(cur["data_files"])
        assert removed == set(), "分支提交不得移除其他提交的文件引用"


def test_same_snapshot_appends_on_two_branches_do_not_collide(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    t.append([("seed", 0)])
    t.create_branch("a", from_version=t.current_version())
    t.create_branch("b", from_version=t.current_version())
    ta, tb = t.checkout("a"), t.checkout("b")
    fa = ta.append([("a-row", 1)])
    fb = tb.append([("b-row", 2)])
    mgr = t.append([("main-row", 3)])
    versions = {
        mf.load_manifest(t.meta_root, v)["version"]
        for v in (ta.current_version(), tb.current_version(), t.current_version())
    }
    assert len(versions) == 3
    assert len({fa, fb, mgr}) == 3
    assert sorted(ta.row_ids()) == ["a-row", "seed"]
    assert sorted(tb.row_ids()) == ["b-row", "seed"]
    assert sorted(t.row_ids()) == ["main-row", "seed"]


# ---------------------------------------------------------------------------
# 5. 分支上的写入（追加 / 删行 / 微批 / 压实）只推进本线
# ---------------------------------------------------------------------------
def test_writes_on_branch_do_not_leak_to_main(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    seed = t.append([("k1", 1), ("k2", 2), ("k3", 3)])
    t.publish_deletes(seed, [1])                     # main: k2 删除
    main_before = t.scan()
    main_version = t.current_version()
    t.create_branch("exp", from_version=main_version)
    e = t.checkout("exp")

    # 1) 追加 + 删行（针对本线可见文件）
    ef = e.append([("e1", 10), ("e2", 20)])
    e.publish_deletes(ef, [1])                       # 删 e2
    assert sorted(e.row_ids()) == ["e1", "k1", "k3"]

    # 2) 微批接入
    r = e.ingest_batch("exp-batch", msgs("s", 0, [0, 1], "i"))
    assert r["cursors"] == {"s": {"0": 2}}
    assert sorted(e.row_ids()) == ["e1", "i0000", "i0001", "k1", "k3"]

    # 3) 文件整理（压实本线所有文件）
    plan = create_plan(e, selected=sorted(e.manifest()["data_files"]))
    commit_plan(e, plan)
    assert sorted(e.row_ids()) == ["e1", "i0000", "i0001", "k1", "k3"]

    # main 完全未动：版本、行多重集、文件引用、游标都保持
    assert t.current_version() == main_version
    assert rows_by_id(t.scan()) == rows_by_id(main_before)
    assert t.manifest()["data_files"] == mf.load_manifest(
        t.meta_root, main_version
    )["data_files"]
    assert t.ingest_cursors() == {}
    assert t.batch_record("exp-batch") is None
    # 实验线的尖端文件引用与 main 的完全不同
    assert set(e.manifest()["data_files"]) != set(t.manifest()["data_files"])


def test_compaction_plan_cannot_cross_branches(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    f = t.append([("a", 1)])
    t.create_branch("exp", from_version=t.current_version())
    e = t.checkout("exp")
    plan_main = create_plan(t, selected=[f])         # 在 main 生成
    with pytest.raises(BranchMismatchError, match="created on branch"):
        commit_plan(e, plan_main)                    # 在 exp 提交
    # 反过来也一样
    e.append([("b", 2)])
    ef = sorted(set(e.manifest()["data_files"]) - set(t.manifest()["data_files"]))[0]
    plan_exp = create_plan(e, selected=[ef])
    with pytest.raises(BranchMismatchError):
        commit_plan(t, plan_exp)
    # 各自原状：版本未推进
    assert t.current_version() == 1
    assert e.current_version() == 2


# ---------------------------------------------------------------------------
# 6. 相同接入请求键在两条分支上各自独立进展（并发、真实子进程）
# ---------------------------------------------------------------------------
def _branch_ingest_child(root, branch, batch_key, batch, barrier, q):
    try:
        t = LakeTable.open(root, branch=branch)
        barrier.wait(60)
        r = t.ingest_batch(batch_key, batch)
        q.put(("ok", r))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def test_same_batch_key_on_two_branches_is_isolated(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    t.append([("seed", 0)])
    t.create_branch("exp-a", from_version=t.current_version())
    t.create_branch("exp-b", from_version=t.current_version())

    key = "shared-request-key"
    batch = msgs("train", 0, [0, 1, 2], "p")

    barrier = CTX.Barrier(2)
    q = CTX.Queue()
    pa_ = CTX.Process(
        target=_branch_ingest_child,
        args=(root, "exp-a", key, batch, barrier, q),
    )
    pb = CTX.Process(
        target=_branch_ingest_child,
        args=(root, "exp-b", key, batch, barrier, q),
    )
    pa_.start(); pb.start()
    pa_.join(90); pb.join(90)
    assert pa_.exitcode == 0 and pb.exitcode == 0
    first = q.get(timeout=60)
    second = q.get(timeout=60)
    assert first[0] == "ok" and second[0] == "ok", (first, second)
    results = {"exp-a": None, "exp-b": None}
    for status, value in (first, second):
        results[value["branch"]] = value
    a_res, b_res = results["exp-a"], results["exp-b"]

    # 两条分支各自成功提交，receipt 不同（回执不跨线借用）
    assert a_res["replayed"] is False and b_res["replayed"] is False
    assert a_res["receipt"] != b_res["receipt"]
    assert a_res["version"] != b_res["version"]
    assert sorted((a_res["version"], b_res["version"])) == [2, 3]

    ta, tb = t.checkout("exp-a"), t.checkout("exp-b")
    # 各自游标都从 0 推进到 3，互不借用
    assert ta.ingest_cursors() == {"train": {"0": 3}}
    assert tb.ingest_cursors() == {"train": {"0": 3}}
    assert rows_by_id(ta.scan()) == [
        ("p0000", 0), ("p0001", 1), ("p0002", 2), ("seed", 0)
    ]
    assert rows_by_id(tb.scan()) == rows_by_id(ta.scan())
    assert t.ingest_cursors() == {}, "main 不受实验线摄取影响"
    assert t.batch_record(key) is None

    # 各自重放本线：拿到本线自己的原 receipt，不新增行、不推进版本
    a2 = ta.ingest_batch(key, list(reversed(batch)))
    b2 = tb.ingest_batch(key, list(reversed(batch)))
    assert a2["replayed"] and b2["replayed"]
    assert a2["receipt"] == a_res["receipt"]
    assert b2["receipt"] == b_res["receipt"]
    assert a2["receipt"] != b2["receipt"]
    assert ta.current_version() == a_res["version"]
    assert tb.current_version() == b_res["version"]

    # 每条线的批次记录文件命名带分支，审计内容不互相覆盖
    rec_a = ta.batch_record(key)
    rec_b = tb.batch_record(key)
    assert rec_a["branch"] == "exp-a" and rec_b["branch"] == "exp-b"
    assert rec_a["receipt"] == a_res["receipt"]
    assert rec_b["receipt"] == b_res["receipt"]
    assert rec_a["version"] == a_res["version"]
    assert rec_b["version"] == b_res["version"]
    assert rec_a["messages"] == rec_b["messages"]
    # 批次数目相同：每线整批 3 行，各推进自己的游标
    assert rows_by_id(ta.scan()).count(("p0002", 2)) == 1
    assert rows_by_id(tb.scan()).count(("p0002", 2)) == 1


def test_branch_cursor_progress_differs_from_inherited_ancestor(tmp_path):
    """同一来源在 main 已推进到某位置；从该提交拉分支后可以各自继续。"""
    t = LakeTable.create(str(tmp_path / "lake"))
    r_main = t.ingest_batch("main-1", msgs("s", 0, [0, 1], "m"))
    assert r_main["cursors"] == {"s": {"0": 2}}
    t.create_branch("exp", from_version=t.current_version())
    e = t.checkout("exp")
    # 实验线继承父提交游标 2，只接 [2,3]；main 同时接 [2,3] 也可
    re_ = e.ingest_batch("exp-1", msgs("s", 0, [2, 3], "e"))
    rm = t.ingest_batch("main-2", msgs("s", 0, [2, 3], "x"))
    assert re_["cursors"] == {"s": {"0": 4}}
    assert rm["cursors"] == {"s": {"0": 4}}
    assert re_["receipt"] != rm["receipt"]
    assert t.ingest_cursors() == {"s": {"0": 4}}
    assert e.ingest_cursors() == {"s": {"0": 4}}
    # 历史进度查询使用全库坐标，能看到各自的提交
    assert t.ingest_cursors(t.current_version()) == {"s": {"0": 4}}
    assert e.ingest_cursors(re_["version"]) == {"s": {"0": 4}}

    # 实验线上用 main 已消费的区间重发（同 key 不同内容也按本线判定）
    with pytest.raises(BatchRejectedError, match="overlap"):
        e.ingest_batch("exp-2", msgs("s", 0, [0, 1], "z"))
    assert e.ingest_cursors() == {"s": {"0": 4}}
    # main 用实验线的 key 查看：不命中，因为不属于本线
    assert t.batch_record("exp-1") is None
    assert t.batch_record("main-2")["receipt"] == rm["receipt"]


def test_main_receipt_stays_compatible_with_legacy_formula(tmp_path):
    """main 分支的 receipt 必须与旧公式逐字节相同，旧表重放行为不变。"""
    import hashlib

    from lake.ingest import make_receipt, messages_digest, normalize_messages

    t = LakeTable.create(str(tmp_path / "lake"))
    batch = msgs("s", 0, [0, 1], "a")
    t.create_branch("exp", from_version=t.current_version())    # 分支在摄取之前
    r = t.ingest_batch("legacy-key", batch)
    digest = messages_digest(normalize_messages(batch))
    legacy = "rcpt-" + hashlib.sha256(
        f"legacy-key\x00{digest}".encode("utf-8")
    ).hexdigest()[:32]
    assert r["receipt"] == legacy == make_receipt("legacy-key", digest)
    # 在分叉点之后两条线各自提交同名同内容的批次：回执必然不同
    e = t.checkout("exp")
    r_exp = e.ingest_batch("legacy-key", batch)
    assert r_exp["receipt"] != legacy
    assert r_exp["branch"] == "exp"
    # 从**已含该批次**的父提交拉出的新分支继承父状态（不是借用，是共享祖先）
    t.create_branch("exp-inherit", from_version=t.current_version())
    i = t.checkout("exp-inherit")
    assert i.ingest_batch("legacy-key", batch)["receipt"] == legacy
    assert i.ingest_batch("legacy-key", batch)["replayed"] is True


def test_idempotency_conflict_is_per_branch(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    t.create_branch("exp", from_version=t.current_version())
    e = t.checkout("exp")
    batch = msgs("s", 0, [0], "a")
    rm = t.ingest_batch("k", batch)
    re_ = e.ingest_batch("k", batch)
    assert rm["receipt"] != re_["receipt"]          # 不同线不同回执
    tampered = [dict(m) for m in batch]
    tampered[0]["value"] = 99
    # 本线同 key 改内容 → 幂等冲突，携带**本线**原回执
    with pytest.raises(IdempotencyConflictError) as ei:
        e.ingest_batch("k", tampered)
    assert ei.value.receipt == re_["receipt"]
    assert ei.value.version == re_["version"]
    # main 自己的 key 内容未变，仍然可以重放原回执
    assert t.ingest_batch("k", batch)["receipt"] == rm["receipt"]


# ---------------------------------------------------------------------------
# 7. 版本号在全库唯一；写者只核验本线尖端（另一线推进不算冲突）
# ---------------------------------------------------------------------------
def test_writer_checks_only_own_tip(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    t.append([("m1", 1)])                       # v1
    t.create_branch("exp", from_version=t.current_version())
    e = t.checkout("exp")
    stale_e = t.checkout("exp")                 # 另一个句柄，仍以为尖端是 v1

    e.append([("e1", 2)])                       # exp -> v2
    t.append([("m2", 3)])                       # 别的线推进 main -> v3
    # 本线句柄继续提交：父提交是本线尖端 v2，而不是 v1
    stale_e.append([("e2", 4)])                 # exp -> v4, parent=2
    m = mf.load_manifest(t.meta_root, e.current_version())
    assert m["version"] == 4 and m["parent"] == 2
    assert m["branch"] == "exp"
    assert sorted(e.row_ids()) == ["e1", "e2", "m1"]
    # main 的三条记录不受影响
    assert t.current_version() == 3
    assert sorted(t.row_ids()) == ["m1", "m2"]


def test_stale_branch_tip_commit_is_rejected_not_silently_rebased(tmp_path):
    """同一分支内真正的竞争：后到者基于旧父提交时必须明确冲突。"""
    from lake import ConflictError

    t = LakeTable.create(str(tmp_path / "lake"))
    t.create_branch("exp", from_version=t.current_version())
    e = t.checkout("exp")
    a = t.checkout("exp")
    b = t.checkout("exp")
    a.append([("a", 1)])                        # exp tip -> v1
    tip = a.current_version()
    # b 仍以 v0 为父构造提交（模拟基于旧尖端），发布时必须明确冲突
    cur = mf.load_manifest(t.meta_root, 0)
    new = b._next(cur)
    new["data_files"]["fake"] = {"path": "data/fake.parquet", "num_rows": 1}
    with pytest.raises(ConflictError, match="stale commit"):
        b._publish(new)
    assert e.current_version() == tip
    assert mf.load_manifest(t.meta_root, tip)["version"] == tip
    assert sorted(e.row_ids()) == ["a"]
    assert t.current_version() == 0


def _branch_writer_child(root, branch, tag, barrier, q):
    try:
        t = LakeTable.open(root, branch=branch)
        barrier.wait(60)
        t.append([(tag, 1)])
        t.append([(tag + "-2", 2)])
        q.put(("ok", tag, t.current_version()))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", tag, repr(e)))


def test_concurrent_writers_on_one_branch_serialize_correctly(tmp_path):
    """本线竞争：两个写者同线并发追加，都得正确仲裁（串行化、父链线性）。"""
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    t.append([("seed", 0)])
    t.create_branch("exp", from_version=t.current_version())
    barrier = CTX.Barrier(2)
    q = CTX.Queue()
    p1 = CTX.Process(target=_branch_writer_child, args=(root, "exp", "w1", barrier, q))
    p2 = CTX.Process(target=_branch_writer_child, args=(root, "exp", "w2", barrier, q))
    p1.start(); p2.start(); p1.join(90); p2.join(90)
    assert p1.exitcode == 0 and p2.exitcode == 0
    events = [q.get(timeout=60) for _ in range(2)]
    assert all(e[0] == "ok" for e in events), events
    e = t.checkout("exp")
    tips = mf.all_tips(t.meta_root)
    assert tips["main"] == 1
    assert tips["exp"] == 5, "本线两次提交各写两个文件：v2..v5"
    # 本线父链线性：5 -> 4 -> 3 -> 2 -> 1
    assert mf.ancestors(t.meta_root, tips["exp"]) == [5, 4, 3, 2, 1, 0]
    assert sorted(e.row_ids()) == ["seed", "w1", "w1-2", "w2", "w2-2"]
    # main 不受影响
    assert t.row_ids() == ["seed"]
    # 两个写者各自最后一次提交拿到不同的版本号；中间允许按提交粒度交错，
    # 但每条提交都从锁内读到的本线尖端取父、取全库下一个号
    versions = [ev[2] for ev in events]
    assert versions[0] != versions[1]
    assert all(2 <= v <= 5 for v in versions)
    # 本线四条提交的父链在图上线性（不存在分叉的 exp 尖端）
    assert len(mf.ancestors(t.meta_root, tips["exp"])) == 6


# ---------------------------------------------------------------------------
# 8. publish：仅当 main 是实验尖端祖先时快进；分叉拒绝；不复制样本
# ---------------------------------------------------------------------------
def test_publish_fast_forwards_main_to_existing_commit(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    t.append([("base", 0)])
    base = t.current_version()
    t.create_branch("exp", from_version=base)
    e = t.checkout("exp")
    ef = e.append([("exp-1", 1), ("exp-2", 2)])
    e.publish_deletes(ef, [1])                  # 实验线删掉 exp-2
    exp_tip = e.current_version()
    exp_manifest = e.manifest()
    exp_paths = file_paths(exp_manifest)
    pre_publish_files = sorted(os.listdir(t.data_dir))
    assert mf.current_version(t.meta_root) == base  # 发布前 main 看不到实验改动

    # 拒绝过期预期：expected 不等于当前 main 尖端
    with pytest.raises(StaleMainError) as ei:
        e.publish(base + 7)
    assert ei.value.expected_main_version == base + 7
    assert ei.value.actual_main_version == base
    assert mf.current_version(t.meta_root) == base
    assert sorted(os.listdir(t.data_dir)) == pre_publish_files

    # 拒绝 source-is-main
    with pytest.raises(SourceIsMainError, match="main"):
        t.publish(base)
    # 参数形状错误
    with pytest.raises(PublishError, match="must be an int"):
        e.publish("not-an-int")  # type: ignore[arg-type]

    result = e.publish(base)
    assert result["source_version"] == exp_tip
    assert result["main_before"] == base
    assert result["main_after"] == exp_tip
    assert result["reused_commit"] is True
    assert result["copied_rows"] == 0
    # base 之后实验线有两个提交（追加 + 删除），都是新交付给 main 的
    assert result["commits"] == [exp_tip - 1, exp_tip]
    # main 快进前完全看不到实验行，快进后才看到
    assert rows_by_id(t.scan()) == [("base", 0), ("exp-1", 1)]

    # main 指向**已有实验提交**：manifest 文件字节未被重写
    assert mf.current_version(t.meta_root) == exp_tip
    m = mf.load_manifest(t.meta_root, exp_tip)
    assert m == exp_manifest
    assert m["branch"] == "exp"                 # 提交的身份没有变
    # 不复制样本：data/ 没有新增文件，也没有新版本文件
    assert sorted(os.listdir(t.data_dir)) == pre_publish_files
    assert set(os.listdir(mf.versions_dir(t.meta_root))) == {
        f"v{v:06d}.json" for v in range(exp_tip + 1)
    }
    assert file_paths(t.manifest()) == exp_paths
    assert rows_by_id(t.scan()) == [("base", 0), ("exp-1", 1)]
    # 实验分支引用仍在，还可以继续推进
    e.append([("exp-3", 3)])
    assert e.current_version() > exp_tip
    assert t.current_version() == exp_tip, "main 不会自动跟随实验线的新提交"


def test_publish_rejected_when_lines_diverged(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    t.append([("base", 0)])
    fork = t.current_version()
    t.create_branch("exp", from_version=fork)
    e = t.checkout("exp")
    e.append([("exp-only", 1)])                 # exp tip v2
    t.append([("main-only", 2)])                # main tip v3 —— 已分叉
    exp_tip, main_tip = e.current_version(), t.current_version()
    assert exp_tip != main_tip
    snapshot_main = t.scan()
    snapshot_exp = e.scan()
    versions_before = sorted(os.listdir(mf.versions_dir(t.meta_root)))
    files_before = sorted(os.listdir(t.data_dir))

    with pytest.raises(DivergedError) as ei:
        e.publish(main_tip)
    assert ei.value.reason == "diverged"
    assert ei.value.details["main_version"] == main_tip
    assert ei.value.details["source_version"] == exp_tip
    # 拒绝保留双方原状
    assert mf.current_version(t.meta_root) == main_tip
    assert mf.load_branches(t.meta_root)["exp"] == exp_tip
    assert sorted(os.listdir(mf.versions_dir(t.meta_root))) == versions_before
    assert sorted(os.listdir(t.data_dir)) == files_before
    assert rows_by_id(t.scan()) == rows_by_id(snapshot_main)
    assert rows_by_id(e.scan()) == rows_by_id(snapshot_exp)
    # 过期预期与分叉是两类明确拒绝，先被 checks 区分
    with pytest.raises(StaleMainError):
        e.publish(exp_tip)


def test_workbench_reads_branch_tip_of_checked_out_table(tmp_path):
    """工作台按精确版本取数：登记实验尖端号即可读取实验内容，而 main 的

    正式尖端号仍然只返回正式行。同为表达“分支上的读取”，不改变工作台
    以全库提交号定位数据的契约。
    """
    t = LakeTable.create(str(tmp_path / "lake"))
    t.append([("prod-1", 1), ("prod-2", 2)])
    main_tip = t.current_version()
    t.create_branch("exp", from_version=main_tip)
    e = t.checkout("exp")
    e.append([("exp-1", 10)])
    exp_tip = e.current_version()

    main_rows = query_arrow(
        {"lake": (t.root, main_tip)}, "SELECT row_id, value FROM lake ORDER BY row_id"
    ).to_pylist()
    exp_rows = query_arrow(
        {"lake": (t.root, exp_tip)}, "SELECT row_id, value FROM lake ORDER BY row_id"
    ).to_pylist()
    assert [(r["row_id"], r["value"]) for r in main_rows] == [
        ("prod-1", 1), ("prod-2", 2)
    ]
    assert [(r["row_id"], r["value"]) for r in exp_rows] == [
        ("exp-1", 10), ("prod-1", 1), ("prod-2", 2)
    ]


def test_publish_when_main_already_equals_exp_tip_is_idempotent(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    t.append([("a", 1)])
    t.create_branch("exp", from_version=t.current_version())
    e = t.checkout("exp")
    e.append([("b", 2)])
    tip = e.current_version()
    first = e.publish(t.current_version())
    assert first["main_after"] == tip
    second = e.publish(tip)                     # main 已是尖端：祖先关系成立
    assert second["main_before"] == tip == second["main_after"]
    assert second["commits"] == []
    assert second["copied_rows"] == 0


# ---------------------------------------------------------------------------
# 9. 整理一条线 + 修改另一条线：彼此数据都保留（真实子进程 + 屏障）
# ---------------------------------------------------------------------------
def _compacter_on_branch(root, branch, tag, barrier, q):
    try:
        t = LakeTable.open(root, branch=branch)
        plan = create_plan(t, max_rows=10_000)   # 刚生成的计划基于本线尖端
        barrier.wait(60)
        v = commit_plan(t, plan)
        q.put((tag, v))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def _writer_on_branch(root, branch, tag, rows, dropped_pos, barrier, q):
    try:
        t = LakeTable.open(root, branch=branch)
        barrier.wait(60)                          # 与压实计划对齐后并发写
        fid = t.append(rows)
        t.publish_deletes(fid, [dropped_pos])
        q.put((tag, t.current_version()))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def test_compact_one_line_while_writing_the_other(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    f1 = t.append([("base-1", 1), ("base-2", 2), ("base-3", 3)])
    f2 = t.append([("base-4", 4)])
    t.publish_deletes(f1, [1])                    # base-2 在 main 被删
    t.create_branch("exp", from_version=t.current_version())
    e = t.checkout("exp")

    # 实验线先有自己的改动，再从它的尖端继续整理
    ef = e.append([("exp-a", 10), ("exp-b", 20)])
    e.publish_deletes(ef, [1])
    main_before = rows_by_id(t.scan())
    exp_before = rows_by_id(e.scan())

    barrier = CTX.Barrier(2)
    q = CTX.Queue()
    pc = CTX.Process(
        target=_compacter_on_branch, args=(root, "exp", "compact", barrier, q)
    )
    pw = CTX.Process(
        target=_writer_on_branch,
        args=(
            root,
            "main",
            "write",
            [("m-added", 30), ("m-dropped", 40)],
            1,
            barrier,
            q,
        ),
    )
    pc.start(); pw.start()
    pc.join(90); pw.join(90)
    assert pc.exitcode == 0 and pw.exitcode == 0
    events = dict(q.get(timeout=60) for _ in range(2))
    assert "compact" in events and "write" in events, events
    compact_v, write_v = events["compact"], events["write"]
    assert compact_v != write_v, "两条线各自取号，编号不得相同"

    main_manifest = t.manifest()
    exp_manifest = e.manifest()
    # 两条线各自的数据都保留：main 有新写入、实验线有整理结果
    assert rows_by_id(t.scan()) == [
        ("base-1", 1), ("base-3", 3), ("base-4", 4), ("m-added", 30)
    ]
    assert rows_by_id(e.scan()) == [
        ("base-1", 1), ("base-3", 3), ("base-4", 4), ("exp-a", 10)
    ]
    assert "m-added" in t.row_ids() and "m-added" not in e.row_ids()
    assert "exp-a" in e.row_ids() and "exp-a" not in t.row_ids()
    # 并发写入的删除也正确：m-dropped 不在任何线上
    assert "m-dropped" not in t.row_ids() and "m-dropped" not in e.row_ids()
    # 实验线的基删除（base-2）与自身删除（exp-b）都不可见
    assert "base-2" not in e.row_ids() and "exp-b" not in e.row_ids()

    # 实验线整理后文件数下降；两条线的文件引用互不覆盖
    assert len(exp_manifest["data_files"]) <= 2
    assert set(main_manifest["data_files"]) != set(exp_manifest["data_files"])
    for m in (main_manifest, exp_manifest):
        for meta in m["data_files"].values():
            assert os.path.exists(os.path.join(root, meta["path"]))
    # main 的提交父链仍是 main 自己的线（不指向实验提交）
    main_ids = set(mf.ancestors(t.meta_root, t.current_version()))
    assert all(
        mf.load_manifest(t.meta_root, v)["branch"] == "main" for v in main_ids
    )
    assert rows_by_id(t.scan()) != main_before   # 写入确实生效
    assert rows_by_id(e.scan()) == exp_before   # 整理后内容不变


# ---------------------------------------------------------------------------
# 10. 撤销后独占内容可回收；撤销前已被读者保护的内容要等读完
# ---------------------------------------------------------------------------
def test_deleted_branch_content_is_collected_but_lease_protects_readers(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    seed = t.append([("common", 0)])               # 共享内容
    base_v = t.current_version()
    t.create_branch("exp", from_version=base_v)
    e = t.checkout("exp")
    ef = e.append([("exp-1", 10), ("exp-2", 20)])  # 实验独占数据文件
    exp_tip = e.current_version()
    e.publish_deletes(ef, [0])                     # 实验独占删除文件
    del_v = e.current_version()
    exp_files = file_paths(e.manifest())
    exclusive = [p for p in exp_files if p not in file_paths(t.manifest())]
    assert len(exclusive) == 2                     # 数据 + 删除各一
    common_files = [p for p in file_paths(t.manifest())]
    all_paths = {p: os.path.join(root, p) for p in exp_files}

    # 撤销实验分支：引用消失，但文件此刻仍在磁盘上（尚未回收）
    e.delete_branch("exp")
    assert mf.load_branches(t.meta_root) == {}
    for p in exp_files:
        assert os.path.exists(all_paths[p]), "撤销本身不删文件"

    # 应用正在读取即将撤销的独占内容：先建租约
    with e.lease(del_v, require_files=True) as lease:
        assert lease.version == del_v
        # 租约内可以完整读到撤销前的快照（exp-1 已删，剩 exp-2 + common）
        snap = e.scan(lease.version)
        assert rows_by_id(snap) == [("common", 0), ("exp-2", 20)]
        report = collect(e, staging_max_age_seconds=0)
        assert del_v in report["protected_versions"], "租约版本必须受保护"
        assert "exp" not in report["branches"]
        assert report["removed_files"] == [], "读取期间不得删除独占内容"
        for p in exp_files:
            assert os.path.exists(all_paths[p]), f"租约保护期间 {p} 被误删"
        # 租约内还能读完整个实验链的历史提交
        assert rows_by_id(e.scan(exp_tip)) == [
            ("common", 0), ("exp-1", 10), ("exp-2", 20)
        ]

    # 结束读取：下一次回收才清除实验独占内容（未被 main/任何尖端引用）
    report = collect(e, staging_max_age_seconds=0)
    assert del_v not in report["protected_versions"]
    for p in exclusive:
        assert p in report["removed_files"], f"独占内容 {p} 应被回收"
        assert not os.path.exists(all_paths[p])
    # 共享内容仍被 main 引用，保留
    for p in common_files:
        assert p not in report["removed_files"]
        assert os.path.exists(all_paths[p])
    # main 内容逐条完好
    assert rows_by_id(t.scan()) == [("common", 0)]
    assert file_paths(t.manifest()) == common_files
    # 实验线的批次记录（若有）也不留孤儿
    assert report["removed_ingest_records"] == []


def test_gc_keeps_branch_tips_that_main_would_have_deleted(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    f1 = t.append([("base-1", 1), ("base-2", 2)])
    base = t.current_version()
    t.create_branch("exp", from_version=base)
    e = t.checkout("exp")
    ef = e.append([("exp-1", 10)])                 # 实验独占文件
    exp_tip = e.current_version()
    exp_files = file_paths(e.manifest())
    exp_only = [p for p in exp_files if p not in file_paths(t.manifest())]
    assert len(exp_only) == 1

    # 回收：实验尖端仍然存活，其独占文件必须保留（即使 main 看不到）
    report = collect(e, staging_max_age_seconds=0)
    assert report["branches"] == {"exp": exp_tip, "main": base}
    assert report["removed_files"] == []
    for p in exp_files:
        assert os.path.exists(os.path.join(root, p))
    assert rows_by_id(e.scan()) == [("base-1", 1), ("base-2", 2), ("exp-1", 10)]

    # 撤销后才允许回收；先不回收，确认 main 仍完好
    t.delete_branch("exp")
    report = collect(t, staging_max_age_seconds=0)
    for p in exp_only:
        assert p in report["removed_files"]
    assert rows_by_id(t.scan()) == [("base-1", 1), ("base-2", 2)]
    for p in file_paths(t.manifest()):
        assert os.path.exists(os.path.join(root, p))


def test_gc_keeps_published_main_after_branch_deleted(tmp_path):
    """快进发布后再撤销实验分支：main 现在引用那些提交，内容不能回收。"""
    t = LakeTable.create(str(tmp_path / "lake"))
    t.append([("base", 0)])
    t.create_branch("exp", from_version=t.current_version())
    e = t.checkout("exp")
    published_tip = e.append([("exp", 1)])
    exp_files = file_paths(e.manifest())
    e.publish(t.current_version())
    e.delete_branch("exp")
    assert mf.load_branches(t.meta_root) == {}

    report = collect(t, staging_max_age_seconds=0)
    assert report["branches"] == {"main": t.current_version()}
    for p in exp_files:
        assert p not in report["removed_files"]
        assert os.path.exists(os.path.join(t.root, p))
    assert rows_by_id(t.scan()) == [("base", 0), ("exp", 1)]
    assert published_tip is not None


# ---------------------------------------------------------------------------
# 11. 撤销分支不删版本文件；尖端目录/关系在重开后仍然正确
# ---------------------------------------------------------------------------
def test_reopen_preserves_branches_tips_parents_and_reads(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    t.append([("m-base", 0)])
    m1 = t.current_version()
    t.create_branch("exp-a", from_version=m1)
    t.create_branch("exp-b", from_version=m1)
    a, b = t.checkout("exp-a"), t.checkout("exp-b")
    a.append([("a1", 1)])
    a_plan = create_plan(a, max_rows=10_000)
    commit_plan(a, a_plan)                       # 实验线整理
    b.ingest_batch("b-batch", msgs("src", 0, [0, 1], "b"))
    t.append([("m-later", 2)])
    t.create_branch("exp-c", from_version=t.current_version())  # 从最新 main 尖端
    c = t.checkout("exp-c")
    c.append([("c1", 3)])

    tips_before = mf.all_tips(t.meta_root)
    manifests_before = {
        v: mf.load_manifest(t.meta_root, v)
        for v in range(max(tips_before.values()) + 1)
    }
    scan_before = {
        "main": rows_by_id(t.scan()),
        "exp-a": rows_by_id(a.scan()),
        "exp-b": rows_by_id(b.scan()),
    }
    cursors_before = {
        "main": t.ingest_cursors(),
        "exp-a": a.ingest_cursors(),
        "exp-b": b.ingest_cursors(),
    }
    receipts_before = {
        "exp-b": b.batch_record("b-batch")["receipt"],
    }
    files_before = sorted(os.listdir(t.data_dir))

    # 关闭（丢弃全部对象）并重开产品
    del t, a, b
    import gc as _pygc

    _pygc.collect()
    t2 = LakeTable.open(root)
    a2 = LakeTable.open(root, branch="exp-a")
    b2 = LakeTable.open(root, branch="exp-b")

    assert t2.branch == "main"
    assert mf.all_tips(t2.meta_root) == tips_before
    assert {v: mf.load_manifest(t2.meta_root, v) for v in manifests_before} == (
        manifests_before
    )
    assert rows_by_id(t2.scan()) == scan_before["main"]
    assert rows_by_id(a2.scan()) == scan_before["exp-a"]
    assert rows_by_id(b2.scan()) == scan_before["exp-b"]
    assert t2.ingest_cursors() == cursors_before["main"]
    assert a2.ingest_cursors() == cursors_before["exp-a"]
    assert b2.ingest_cursors() == cursors_before["exp-b"]
    assert b2.batch_record("b-batch")["receipt"] == receipts_before["exp-b"]
    assert sorted(os.listdir(t2.data_dir)) == files_before

    # 重开后继续在两条线上写，编号仍全库唯一，父提交接续原尖端
    a2.append([("a2", 9)])
    b2.append([("b2", 8)])
    tips = mf.all_tips(t2.meta_root)
    assert len(set(tips.values())) == len(tips)
    assert mf.load_manifest(t2.meta_root, tips["exp-a"])["parent"] == tips_before["exp-a"]
    assert mf.load_manifest(t2.meta_root, tips["exp-b"])["parent"] == tips_before["exp-b"]

    # 重开后 publish 依然工作：c 基于最新 main 尖端，可快进交付
    c2 = LakeTable.open(root, branch="exp-c")
    assert rows_by_id(c2.scan()) == sorted(scan_before["main"] + [("c1", 3)])
    c2.publish(t2.current_version())
    assert mf.current_version(t2.meta_root) == c2.current_version()
    assert rows_by_id(t2.scan()) == rows_by_id(c2.scan())
    assert ("c1", 3) in rows_by_id(t2.scan())
    # 撤销已交付分支后，main 引用的内容仍然全部保留；其他分支引用不变
    expected_after_publish = rows_by_id(t2.scan())
    t2.delete_branch("exp-c")
    assert set(mf.load_branches(t2.meta_root)) == {"exp-a", "exp-b"}
    report = collect(t2, staging_max_age_seconds=0)
    # main 与存活实验尖端引用的文件一个都不能删
    live = set(file_paths(t2.manifest())) | set(file_paths(a2.manifest())) | set(
        file_paths(b2.manifest())
    )
    assert not (set(report["removed_files"]) & live), report["removed_files"]
    for p in live:
        assert os.path.exists(os.path.join(t2.root, p))
    assert rows_by_id(t2.scan()) == expected_after_publish
