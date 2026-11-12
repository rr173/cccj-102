"""微批摄取事务的端到端校验。

所有并发 / 崩溃用例都真实 fork 子进程，用同步屏障或测试钩子
（LAKE_INGEST_CRASH_POINT）安排确定性交错；每个场景都核验
源游标、receipt 与落地数据三者的一致性。运行：python3 -m pytest tests/ -v
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os

import pyarrow as pa
import pytest

from lake import (
    BatchRejectedError,
    IdempotencyConflictError,
    LakeTable,
    collect,
    commit_plan,
    create_plan,
    verify,
)
from lake import manifest as mf

CTX = mp.get_context("fork")
BARRIER_TIMEOUT = 60


def msgs(source, partition, offsets, prefix, base=0):
    """构造一批消息：offset 列表即分片内编号，row_id 全表唯一便于多重集核对。"""
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


def expected_table(message_groups):
    """独立重算：直接从消息构造期望的 Arrow 表（不经过引擎）。"""
    flat = [m for group in message_groups for m in group]
    return pa.table(
        {
            "row_id": pa.array([m["row_id"] for m in flat], pa.string()),
            "value": pa.array([m["value"] for m in flat], pa.int64()),
        }
    )


def drain(q, n, ok_statuses=("ok",)):
    results = [q.get(timeout=60) for _ in range(n)]
    bad = [r for r in results if r[0] not in ok_statuses]
    assert not bad, f"child process errors: {bad}"
    return results


# ---------------------------------------------------------------------------
# 1. 多分片正常摄取 + 独立重算；未出现的分片维持原游标
# ---------------------------------------------------------------------------
def test_ingest_multi_shard_and_independent_recompute(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    v0 = t.current_version()

    # 三个分片同一批；批内故意乱序
    g1 = msgs("s1", 0, [2, 0, 1], "a")
    g2 = msgs("s2", "p1", [1, 0], "b", base=100)
    g3 = msgs("s1", 1, [0], "c", base=200)
    batch = g1 + g2 + g3
    r = t.ingest_batch("batch-1", batch)

    # 独立重算：游标 = 各分片 max(offset)+1；行多重集 = 全部消息
    assert r["receipt"].startswith("rcpt-")
    assert r["replayed"] is False
    assert r["version"] == v0 + 1
    assert r["cursors"] == {"s1": {"0": 3, "1": 1}, "s2": {"p1": 2}}
    assert r["num_rows"] == 6
    assert t.ingest_cursors() == r["cursors"]
    verify.assert_same_multiset(t.scan(), expected_table([g1, g2, g3]))

    # 历史版本的进度查询：v0 时还没有任何游标
    assert t.ingest_cursors(v0) == {}

    # 批次记录：规范化消息已排序、与输入顺序无关
    rec = t.batch_record("batch-1")
    assert rec["receipt"] == r["receipt"] and rec["version"] == r["version"]
    offsets_s10 = [m[2] for m in rec["messages"] if m[0] == "s1" and m[1] == "0"]
    assert offsets_s10 == [0, 1, 2]
    assert t.batch_record("no-such-batch") is None

    # 第二批只动 s1/0：s2 维持原游标
    g4 = msgs("s1", 0, [4, 3], "a")
    r2 = t.ingest_batch("batch-2", g4)
    assert r2["cursors"] == {"s1": {"0": 5}}
    assert t.ingest_cursors() == {"s1": {"0": 5, "1": 1}, "s2": {"p1": 2}}
    verify.assert_same_multiset(t.scan(), expected_table([g1, g2, g3, g4]))


# ---------------------------------------------------------------------------
# 2. 乱序重发：接入端子进程确认超时后整批重发（顺序不同），返回原 receipt
# ---------------------------------------------------------------------------
def _sender_child(root, batch_key, batch, q):
    try:
        t = LakeTable.open(root)
        r = t.ingest_batch(batch_key, batch)
        q.put(("ok", r))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def test_out_of_order_redelivery_returns_original_receipt(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    batch = msgs("s1", 0, [0, 1, 2, 3], "a") + msgs("s2", 0, [0, 1], "b")

    q = CTX.Queue()
    p = CTX.Process(target=_sender_child, args=(root, "batch-1", batch, q))
    p.start()
    p.join(90)
    assert p.exitcode == 0
    (status, r1), = drain(q, 1)
    assert status == "ok"
    version_after_commit = t.current_version()

    # 同一 batch_key、相同消息、输入顺序完全打乱的重发
    shuffled = list(reversed(batch))
    r2 = t.ingest_batch("batch-1", shuffled)
    assert r2["replayed"] is True
    assert r2["receipt"] == r1["receipt"], "重发必须返回原 receipt"
    assert r2["version"] == r1["version"]
    assert t.current_version() == version_after_commit, "重放不得推进版本"
    assert len(t.scan()) == len(batch), "重放不得新增行"
    assert t.ingest_cursors() == {"s1": {"0": 4}, "s2": {"0": 2}}


# ---------------------------------------------------------------------------
# 3. 改内容的重发：相同 batch_key 不同内容 -> 明确的幂等冲突
# ---------------------------------------------------------------------------
def test_changed_content_redelivery_raises_idempotency_conflict(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    batch = msgs("s1", 0, [0, 1], "a")
    r1 = t.ingest_batch("batch-1", batch)
    version = t.current_version()

    # 改一个 value
    tampered = [dict(m) for m in batch]
    tampered[1]["value"] = 9999
    with pytest.raises(IdempotencyConflictError) as ei:
        t.ingest_batch("batch-1", tampered)
    assert ei.value.receipt == r1["receipt"]
    assert ei.value.version == r1["version"]
    assert ei.value.expected_digest != ei.value.got_digest

    # 改 offset 集合（行数都变了）同样是冲突而不是静默接受
    with pytest.raises(IdempotencyConflictError):
        t.ingest_batch("batch-1", msgs("s1", 0, [0, 1, 2], "a"))

    # 冲突不影响已生效内容
    assert t.current_version() == version
    verify.assert_same_multiset(t.scan(), expected_table([batch]))
    assert t.ingest_cursors() == {"s1": {"0": 2}}


# ---------------------------------------------------------------------------
# 4. 缺号 / 批内重复 / 与既往区间交叠 -> 整批拒绝；失败后可用合法消息再摄取
# ---------------------------------------------------------------------------
def test_invalid_batches_rejected_atomically_then_legal_retry(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    good_a = msgs("sa", 0, [0, 1], "a")
    t.ingest_batch("seed", good_a)  # sa/0 游标推进到 2
    version = t.current_version()

    # 缺号：sa/0 从 2 接续合法，但 sb/0 从 1 开始（游标 0 之后缺 0）-> 整批拒绝
    with pytest.raises(BatchRejectedError) as ei:
        t.ingest_batch(
            "bad-gap", msgs("sa", 0, [2, 3], "a") + msgs("sb", 0, [1], "b")
        )
    assert any("gap" in r for r in ei.value.reasons)
    # 原子性：合法的 sa/0 也不许推进
    assert t.ingest_cursors() == {"sa": {"0": 2}}
    assert t.current_version() == version
    verify.assert_same_multiset(t.scan(), expected_table([good_a]))

    # 批内重复编号
    with pytest.raises(BatchRejectedError) as ei:
        t.ingest_batch("bad-dup", msgs("sa", 0, [2, 2], "a"))
    assert any("duplicate" in r for r in ei.value.reasons)

    # 与既往区间交叠
    with pytest.raises(BatchRejectedError) as ei:
        t.ingest_batch("bad-overlap", msgs("sa", 0, [1, 2], "a"))
    assert any("overlap" in r for r in ei.value.reasons)

    # 批内断档（缺 3）
    with pytest.raises(BatchRejectedError) as ei:
        t.ingest_batch("bad-hole", msgs("sa", 0, [2, 4], "a"))
    assert any("missing" in r for r in ei.value.reasons)

    assert t.current_version() == version
    assert t.ingest_cursors() == {"sa": {"0": 2}}

    # 失败后：合法消息可以再次摄取，游标正常接续
    retry = msgs("sa", 0, [2, 3], "a") + msgs("sb", 0, [0], "b")
    r = t.ingest_batch("retry-ok", retry)
    assert r["cursors"] == {"sa": {"0": 4}, "sb": {"0": 1}}
    verify.assert_same_multiset(t.scan(), expected_table([good_a, retry]))


# ---------------------------------------------------------------------------
# 5. 失败响应契约：请求形状不合法 -> ValueError（与表状态无关）
# ---------------------------------------------------------------------------
def test_malformed_requests_raise_value_error(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    with pytest.raises(ValueError):
        t.ingest_batch("", msgs("s", 0, [0], "a"))          # 空 batch_key
    with pytest.raises(ValueError):
        t.ingest_batch("k", [])                              # 空批
    with pytest.raises(ValueError):
        t.ingest_batch("k", [{"source": "s"}])               # 缺字段
    with pytest.raises(ValueError):
        t.ingest_batch("k", msgs("s", 0, [-1], "a"))         # 负 offset
    bad = msgs("s", 0, [0], "a")
    bad[0]["value"] = 1.5                                    # 非整数 value
    with pytest.raises(ValueError):
        t.ingest_batch("k", bad)
    bad2 = msgs("s", 0, [0], "a")
    bad2[0]["offset"] = True                                 # bool 不是合法 offset
    with pytest.raises(ValueError):
        t.ingest_batch("k", bad2)
    assert t.current_version() == 0, "形状错误不得产生任何副作用"
    assert t.ingest_cursors() == {}


# ---------------------------------------------------------------------------
# 6. 两个请求（不同 batch_key）争抢同一来源区间：最多一个成功
# ---------------------------------------------------------------------------
def _competing_child(root, batch_key, batch, barrier, q):
    try:
        t = LakeTable.open(root)
        barrier.wait(BARRIER_TIMEOUT)  # 对齐后同时冲向提交锁
        r = t.ingest_batch(batch_key, batch)
        q.put(("ok", r))
    except BatchRejectedError as e:
        q.put(("rejected", e.reasons))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def test_competing_batches_for_one_range_at_most_one_wins(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    batch_a = msgs("s1", 0, [0, 1, 2], "a")
    batch_b = msgs("s1", 0, [0, 1, 2], "b")  # 同一区间，不同内容、不同 key

    barrier = CTX.Barrier(2)
    q = CTX.Queue()
    pa_ = CTX.Process(target=_competing_child, args=(root, "key-a", batch_a, barrier, q))
    pb = CTX.Process(target=_competing_child, args=(root, "key-b", batch_b, barrier, q))
    pa_.start(); pb.start()
    pa_.join(90); pb.join(90)
    assert pa_.exitcode == 0 and pb.exitcode == 0
    results = drain(q, 2, ok_statuses=("ok", "rejected"))
    statuses = sorted(r[0] for r in results)
    assert statuses == ["ok", "rejected"], f"必须恰好一个成功: {results}"

    winner = next(r[1] for r in results if r[0] == "ok")
    loser_reasons = next(r[1] for r in results if r[0] == "rejected")
    assert any("overlap" in r for r in loser_reasons)

    # 区间只被摄取一次：游标、数据、版本都与“只成功一批”一致
    assert t.ingest_cursors() == {"s1": {"0": 3}}
    assert t.current_version() == winner["version"]
    assert len(t.scan()) == 3
    landed = set(t.row_ids())
    assert landed == {m["row_id"] for m in batch_a} or landed == {
        m["row_id"] for m in batch_b
    }


def test_identical_competing_batches_follow_key_rules(tmp_path):
    """争抢者完全相同（同 key 同内容）：按请求键规则处理——一个提交、
    一个幂等重放，两个都拿到原 receipt，谁也不能被默默吞掉。"""
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    batch = msgs("s1", 0, [0, 1, 2], "a")

    barrier = CTX.Barrier(2)
    q = CTX.Queue()
    # 两边输入顺序故意不同，证明跨进程也以规范化内容为准
    p1 = CTX.Process(target=_competing_child, args=(root, "key-x", batch, barrier, q))
    p2 = CTX.Process(
        target=_competing_child, args=(root, "key-x", list(reversed(batch)), barrier, q)
    )
    p1.start(); p2.start()
    p1.join(90); p2.join(90)
    assert p1.exitcode == 0 and p2.exitcode == 0
    results = drain(q, 2)

    receipts = {r[1]["receipt"] for r in results}
    assert len(receipts) == 1, "两个进程必须拿到同一个 receipt"
    replayed_flags = sorted(r[1]["replayed"] for r in results)
    assert replayed_flags == [False, True], "恰好一个真实提交、一个重放"
    assert t.current_version() == 1, "版本只推进一次"
    assert len(t.scan()) == 3
    assert t.ingest_cursors() == {"s1": {"0": 3}}


# ---------------------------------------------------------------------------
# 7. 两个互不相关来源同时写入：两个接入进程都成功，互不覆盖
# ---------------------------------------------------------------------------
def _source_worker(root, source, n_batches, barrier, q):
    try:
        t = LakeTable.open(root)
        barrier.wait(BARRIER_TIMEOUT)
        receipts = []
        for i in range(n_batches):
            r = t.ingest_batch(f"{source}-batch-{i}", msgs(source, 0, [2 * i, 2 * i + 1], source))
            receipts.append(r["receipt"])
        q.put(("ok", receipts))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def test_unrelated_sources_ingest_concurrently(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    n = 8
    barrier = CTX.Barrier(2)
    q = CTX.Queue()
    p1 = CTX.Process(target=_source_worker, args=(root, "s1", n, barrier, q))
    p2 = CTX.Process(target=_source_worker, args=(root, "s2", n, barrier, q))
    p1.start(); p2.start()
    p1.join(120); p2.join(120)
    assert p1.exitcode == 0 and p2.exitcode == 0
    results = drain(q, 2)
    all_receipts = [rc for _, receipts in results for rc in receipts]
    assert len(set(all_receipts)) == 2 * n, "每批 receipt 唯一，无丢失无重复"

    # 两个来源的游标都完整推进；版本恰好推进 2n 次（无丢失更新）
    assert t.ingest_cursors() == {"s1": {"0": 2 * n}, "s2": {"0": 2 * n}}
    assert t.current_version() == 2 * n
    expected = expected_table(
        [msgs("s1", 0, list(range(2 * n)), "s1"), msgs("s2", 0, list(range(2 * n)), "s2")]
    )
    verify.assert_same_multiset(t.scan(), expected)


# ---------------------------------------------------------------------------
# 8. 摄取与原有整理作业（压实）同时工作：互不覆盖
# ---------------------------------------------------------------------------
def _compacter_with_barrier(root, fids, barrier, q):
    try:
        t = LakeTable.open(root)
        plan_id = create_plan(t, selected=fids)   # 1) 计划生成（基版本 V）
        q.put(("plan", plan_id))
        barrier.wait(BARRIER_TIMEOUT)             # 等摄取提交完成
        v = commit_plan(t, plan_id)               # 3) 提交压实
        q.put(("ok", v))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def test_ingest_interleaved_with_compaction(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    f1 = t.append([("old-a", 1), ("old-b", 2)])
    f2 = t.append([("old-c", 3)])
    base_version = t.current_version()

    barrier = CTX.Barrier(2)
    q = CTX.Queue()
    pc = CTX.Process(target=_compacter_with_barrier, args=(root, [f1, f2], barrier, q))
    pc.start()
    assert q.get(timeout=60)[0] == "plan"         # 压实计划已基于旧版本生成
    g = msgs("s1", 0, [0, 1], "n")                # 2) 摄取在计划与提交之间落地
    r = t.ingest_batch("ingest-during-compact", g)
    barrier.wait(BARRIER_TIMEOUT)
    pc.join(90)
    assert pc.exitcode == 0
    results = drain(q, 1)
    compact_version = results[0][1]

    # 压实成果（旧文件合并）与摄取成果（新数据 + 游标）都必须保留
    m = t.manifest()
    assert compact_version == base_version + 2
    assert f1 not in m["data_files"] and f2 not in m["data_files"]
    assert len(m["data_files"]) == 2, "合并文件 + 摄取文件"
    assert t.ingest_cursors() == {"s1": {"0": 2}}, "压实不得吃掉接入游标"
    assert t.batch_record("ingest-during-compact")["receipt"] == r["receipt"]
    expected = expected_table([g])
    old = pa.table(
        {"row_id": pa.array(["old-a", "old-b", "old-c"], pa.string()),
         "value": pa.array([1, 2, 3], pa.int64())}
    )
    verify.assert_same_multiset(t.scan(), pa.concat_tables([old, expected]))


# ---------------------------------------------------------------------------
# 9. 准备阶段硬退出（prepared / recorded）：不吃掉消费位置，可重新摄取
# ---------------------------------------------------------------------------
def _crashing_sender(root, batch_key, batch, crash_point):
    os.environ["LAKE_INGEST_CRASH_POINT"] = crash_point
    t = LakeTable.open(root)
    t.ingest_batch(batch_key, batch)  # 在指定位置 os._exit(42)


@pytest.mark.parametrize("point", ["prepared", "recorded"])
def test_crash_before_commit_does_not_consume_positions(tmp_path, point):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    batch = msgs("s1", 0, [0, 1, 2], "a")

    p = CTX.Process(target=_crashing_sender, args=(root, "batch-1", batch, point))
    p.start()
    p.join(90)
    assert p.exitcode == 42, f"{point}: 子进程应在指定位置硬退出"

    # 消费位置、版本、数据都没有被吃掉
    assert t.ingest_cursors() == {}, f"{point}: 游标不得推进"
    assert t.current_version() == 0, f"{point}: 版本不得推进"
    assert t.batch_record("batch-1") is None, f"{point}: 批次不可见"
    assert len(t.scan()) == 0

    # 残骸（临时文件 / 未引用的数据文件 / 孤儿批次记录）由 GC 清理
    report = collect(t, staging_max_age_seconds=0)
    assert report["removed_files"], f"{point}: 未就位的数据文件应被回收"
    if point == "recorded":
        assert len(report["removed_ingest_records"]) == 1, "孤儿批次记录被回收"
    assert t.batch_record("batch-1") is None

    # 同一接入端重发：全量重做并成功，游标正常接续
    r = t.ingest_batch("batch-1", batch)
    assert r["replayed"] is False
    assert r["cursors"] == {"s1": {"0": 3}}
    assert t.ingest_cursors() == {"s1": {"0": 3}}
    verify.assert_same_multiset(t.scan(), expected_table([batch]))


# ---------------------------------------------------------------------------
# 10. 生效后、确认前硬退出（published）：重启重放找到原 receipt
# ---------------------------------------------------------------------------
def _replay_child(root, batch_key, batch, q):
    try:
        t = LakeTable.open(root)
        r = t.ingest_batch(batch_key, batch)
        q.put(("ok", r))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def test_crash_after_publish_replay_finds_original_receipt(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    batch = msgs("s1", 0, [0, 1, 2], "a") + msgs("s2", 0, [0], "b")

    # 接入进程在 HEAD 翻转后、返回确认前硬退出（确认丢失 -> 触发重发）
    p = CTX.Process(target=_crashing_sender, args=(root, "batch-1", batch, "published"))
    p.start()
    p.join(90)
    assert p.exitcode == 42

    # 事务已完整生效：游标、receipt、数据都在同一个可见性边界内
    committed = t.batch_record("batch-1")
    assert committed is not None
    original_receipt = committed["receipt"]
    assert t.ingest_cursors() == {"s1": {"0": 3}, "s2": {"0": 1}}
    version = t.current_version()
    verify.assert_same_multiset(t.scan(), expected_table([batch]))

    # 重启后的新进程乱序重发同一批：找到原 receipt，不再加行、不推版本
    q = CTX.Queue()
    p2 = CTX.Process(
        target=_replay_child, args=(root, "batch-1", list(reversed(batch)), q)
    )
    p2.start()
    p2.join(90)
    assert p2.exitcode == 0
    (status, r), = drain(q, 1)
    assert status == "ok"
    assert r["receipt"] == original_receipt
    assert r["replayed"] is True
    assert t.current_version() == version
    assert len(t.scan()) == len(batch)


# ---------------------------------------------------------------------------
# 11. 兼容：打开此前创建的表（manifest 无 ingest 段），旧入口继续可用
# ---------------------------------------------------------------------------
def _strip_ingest_section(root):
    """把表里所有 manifest 改写成旧格式（无 ingest 段），模拟历史表。"""
    meta = os.path.join(root, "metadata")
    versions_dir = mf.versions_dir(meta)
    for name in os.listdir(versions_dir):
        path = os.path.join(versions_dir, name)
        with open(path, "r", encoding="utf-8") as f:
            m = json.load(f)
        m.pop("ingest", None)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(m, f)


def test_legacy_table_without_ingest_section(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)
    f1 = t.append([("old-1", 1), ("old-2", 2)])
    t.publish_deletes(f1, [0])
    _strip_ingest_section(root)

    # 旧表重新打开：旧业务入口全部可用
    t = LakeTable.open(root)
    assert t.ingest_cursors() == {}, "旧格式按空游标处理"
    assert t.row_ids() == ["old-2"]
    f2 = t.append([("old-3", 3)])                    # 旧入口：追加
    t.publish_deletes(f2, [0])                       # 旧入口：位置删除

    # 新入口在旧表上正常工作：游标从 0 起
    g = msgs("s1", 0, [0, 1], "n")
    r = t.ingest_batch("batch-on-legacy", g)
    assert r["cursors"] == {"s1": {"0": 2}}

    # 旧入口：压实，且不得吃掉摄取成果
    plan_id = create_plan(t, max_rows=10_000)
    commit_plan(t, plan_id)
    assert t.ingest_cursors() == {"s1": {"0": 2}}
    assert sorted(t.row_ids()) == ["n0000", "n0001", "old-2"]


# ---------------------------------------------------------------------------
# 12. 摄取数据与旧读路径 / 删除 / 压实 / 历史进度查询的协同
# ---------------------------------------------------------------------------
def test_ingest_rows_work_with_existing_read_and_compact_paths(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    g1 = msgs("s1", 0, [0, 1, 2], "a")
    r1 = t.ingest_batch("b1", g1)
    v1 = t.current_version()
    fid = t.batch_record("b1")["data_file_id"]

    # 旧读路径：scan / 位置删除直接作用于摄取产生的数据文件
    t.publish_deletes(fid, [0])
    assert sorted(t.row_ids()) == ["a0001", "a0002"]

    # 压实选中摄取文件：游标与批次记录原样携带
    plan_id = create_plan(t, selected=[fid])
    commit_plan(t, plan_id)
    assert t.ingest_cursors() == {"s1": {"0": 3}}
    assert t.batch_record("b1")["receipt"] == r1["receipt"]
    assert sorted(t.row_ids()) == ["a0001", "a0002"]

    # 历史进度查询：v1 时游标已是 3，但删除尚未发布
    assert t.ingest_cursors(v1) == {"s1": {"0": 3}}
    assert sorted(t.row_ids(v1)) == ["a0000", "a0001", "a0002"]

    # 压实后继续摄取：游标正常接续
    g2 = msgs("s1", 0, [3], "a")
    t.ingest_batch("b2", g2)
    assert t.ingest_cursors() == {"s1": {"0": 4}}
    assert sorted(t.row_ids()) == ["a0001", "a0002", "a0003"]
