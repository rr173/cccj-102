"""数据质量画像端到端验收。

对应用户验收清单（全部使用本地生成样本，不连接外部系统）：

  1. 可手算小样本：流式画像与独立参考实现逐项一致
  2. 仅物理整理（压实）、可见数据不变的两版画像完全一致
  3. 删行后的版本重新统计，指标同步变化，比较差异正确
  4. 数据触犯规则：完整画像照常落盘，失败规则逐条列出
  5. 错误配置被拒，且没有任何报告落盘
  6. 同配置重复请求读原报告（文件身份/内容不变），改配置不覆盖
  7. 持旧版读者租约触发 GC：输入仍可用；释放后允许清理
  8. 发布边界用真正退出的子进程制造中断，再请求恢复
  9. 原始数据清理后，已保存报告仍可打开和比较
 10. 计数来自查询结果而非物理分片统计相加；全表不进 Python 列表
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import stat

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import LakeTable, collect, commit_plan, create_plan
from lake.errors import RuleSpecError
from lake.quality import (
    ProfileStore,
    compare_reports,
    compute_profile,
    cross_check,
    normalize_rules,
    reference_profile,
    rule_summary,
    rules_digest,
)
from lake.quality.profile import iter_visible_batches
from lake.quality.reference import reference_visible_rows

CTX = mp.get_context("fork")

# 可手算样本（故意含：空 row_id、空 value、重复 id、越界值、分段外值）
ROWS_A = [
    ("a", 0),
    ("b", 9),
    ("b", 10),     # b 重复
    ("c", 50),
    ("d", 99),
    (None, 7),     # 空主标识
    ("e", None),   # 空数值
    ("f", 120),    # 高于上界、分段外
    ("g", -3),     # 低于下界、分段外
]

RULES = {
    "min_samples": 8,
    "max_duplicate_ids": 0,
    "value_bounds": {"min": 0, "max": 100},
    "buckets": [
        {"name": "low", "lower": 0, "upper": 10},
        {"name": "mid", "lower": 10, "upper": 90},
        {"name": "high", "lower": 90, "upper": 101},
    ],
}


def write_rows(table, rows):
    """直接写一个含 null 的数据文件（append 不支持 None，测试专用）。"""
    import uuid

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
        # num_rows 是该文件的物理行数（删除位置按 0 起物理行号校验）
        new["data_files"][fid] = {"path": rel, "num_rows": arrow.num_rows}
        table._publish(new)
    return fid


def rows_of_file(table, version: int, fid: str):
    """读取单个物理文件的全部行（不做删除过滤），用于按内容定位分片。"""
    import pyarrow.parquet as pq

    meta = table.manifest(version)["data_files"][fid]
    tab = pq.read_table(os.path.join(table.root, meta["path"]))
    return list(zip(tab.column("row_id").to_pylist(),
                    tab.column("value").to_pylist()))


def build_lake(root):
    t = LakeTable.create(root)
    # 拆成两个物理分片，证明统计基于查询结果而非分片 num_rows 相加
    write_rows(t, ROWS_A[:5])
    write_rows(t, ROWS_A[5:])
    return t


# ---------------------------------------------------------------------------
# 1. 可手算小样本：手算指标、流式实现、参考实现三方一致
# ---------------------------------------------------------------------------
def test_hand_computable_metrics_match_reference(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    v = t.current_version()
    store = ProfileStore(t)
    report = store.create_or_get(RULES, version=v, batch_size=3)

    m = report["metrics"]
    # ---- 手算期望（9 行，2 个物理分片，batch_size=3 强制多批次）----
    assert m["sample_count"] == 9
    assert m["null_row_id_count"] == 1          # (None, 7)
    assert m["null_value_count"] == 1           # ("e", None)
    assert m["null_fields"] == 2
    assert m["distinct_id_count"] == 7          # a,b,c,d,e,f,g（None 不计，b 只算一个）
    assert m["duplicate_id_count"] == 1         # 仅 b
    assert m["duplicated_row_count"] == 2       # b 的两行
    vals = [r[1] for r in ROWS_A if r[1] is not None]
    assert m["value_count"] == 8
    assert m["value_min"] == min(vals) == -3
    assert m["value_max"] == max(vals) == 120
    assert m["value_sum"] == sum(vals) == 292   # 0+9+10+50+99+7+120-3
    # 左闭右开：low=[0,10): 0,9,7 -> 3；mid=[10,90): 10,50 -> 2；
    # high=[90,101): 99 -> 1；分段外: 120,-3 -> 2
    assert m["bucket_counts"] == {"low": 3, "mid": 2, "high": 1}
    assert m["out_of_buckets"] == 2
    assert m["below_min"] == 1                  # -3
    assert m["above_max"] == 1                  # 120

    # 生产实现与独立参考实现逐项一致
    ref = reference_profile(t, v, RULES)
    assert ref["metrics"] == m
    assert cross_check(report, t) == []

    # 参考实现自己读出来的可见行与样本一致
    assert sorted(map(str, reference_visible_rows(t, v))) == sorted(map(str, ROWS_A))


def test_scan_is_batched_and_never_materializes_whole_table(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    v = t.current_version()
    total = 0
    for batch in iter_visible_batches(t, v, batch_size=2):
        assert batch.num_rows <= 2  # 任何时刻内存里只有一个小批次
        total += batch.num_rows
    assert total == 9


# ---------------------------------------------------------------------------
# 2. 仅物理整理（压实）、可见数据不变 -> 画像逐项一致
# ---------------------------------------------------------------------------
def test_compaction_with_same_visible_data_gives_identical_profile(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    v_before = t.current_version()
    store = ProfileStore(t)
    r1 = store.create_or_get(RULES, version=v_before)

    fids = list(t.manifest(v_before)["data_files"])
    plan_id = create_plan(t, selected=fids)
    v_after = commit_plan(t, plan_id)
    assert v_after > v_before
    assert len(t.manifest(v_after)["data_files"]) == 1  # 物理布局已变

    r2 = store.create_or_get(RULES, version=v_after)
    assert r1["revision"] != r2["revision"]
    assert r1["rule_digest"] == r2["rule_digest"]
    assert r1["metrics"] == r2["metrics"]
    assert r1["verdicts"] == r2["verdicts"]
    assert r1["passed"] == r2["passed"]
    # 参考实现交叉核对两版
    assert cross_check(r1, t) == []
    assert cross_check(r2, t) == []


# ---------------------------------------------------------------------------
# 3. 删行后重新统计 + 比较：指标差值与异常迁移
# ---------------------------------------------------------------------------
def test_delete_changes_metrics_and_comparison(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    v1 = t.current_version()
    store = ProfileStore(t)
    r1 = store.create_or_get(RULES, version=v1)
    assert r1["passed"] is False  # 重复 id + 两个越界值

    # 删掉越界的两行（120 在第二分片位置 2，-3 在位置 3）与重复行之一。
    # 分片 id 是随机 uuid，按单文件内容定位目标文件，不能假设 sorted() 顺序。
    per_file = {fid: rows_of_file(t, v1, fid)
                for fid in t.manifest(v1)["data_files"]}
    f_with_b = next(f for f, rows in per_file.items() if ("b", 9) in rows)
    f_with_outliers = next(f for f, rows in per_file.items() if ("f", 120) in rows)
    t.publish_deletes(f_with_b, [1])            # 删掉 ("b", 9)，b 不再重复
    t.publish_deletes(f_with_outliers, [2, 3])  # 删掉 ("f",120)、("g",-3)
    v2 = t.current_version()

    r2 = store.create_or_get(RULES, version=v2)
    m1, m2 = r1["metrics"], r2["metrics"]
    assert m2["sample_count"] == m1["sample_count"] - 3
    assert m2["duplicate_id_count"] == 0
    assert m2["below_min"] == 0 and m2["above_max"] == 0
    assert m2["value_sum"] == m1["value_sum"] - (9 + 120 - 3)
    assert m2["out_of_buckets"] == 0
    # 重复与越界消失，但样本 6 < min_samples 8，min_samples 新失败
    failed_v2 = {v["rule"] for v in r2["verdicts"] if not v["passed"]}
    assert failed_v2 == {"min_samples"}

    diff = compare_reports(r1, r2)
    assert diff["metric_diffs"]["sample_count"]["delta"] == -3
    assert diff["metric_diffs"]["value_sum"]["delta"] == -(9 + 120 - 3)
    assert diff["metric_diffs"]["duplicate_id_count"]["delta"] == -1
    anom = diff["anomalies"]
    failed_rules_v1 = {v["rule"] for v in r1["verdicts"] if not v["passed"]}
    assert failed_rules_v1 == {"max_duplicate_ids", "value_bounds"}
    assert {v["rule"] for v in anom["appeared"]} == {"min_samples"}
    assert {v["rule"] for v in anom["disappeared"]} == failed_rules_v1
    assert anom["persistent"] == []


def test_comparison_detects_new_and_persistent_anomalies(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    v1 = t.current_version()
    store = ProfileStore(t)
    # 放宽数值上下界到 [-100, 200]：value_bounds 在两个版本都通过，
    # 只让重复 id 与样本量规则参与异常迁移的验证。
    strict = {
        "min_samples": 9,          # v1 样本 9 恰好满足；删一行后新违规
        "max_duplicate_ids": 0,    # v1 因 b 重复而失败，删除后通过
        "value_bounds": {"min": -100, "max": 200},
    }
    r1 = store.create_or_get(strict, version=v1)
    assert {v["rule"] for v in r1["verdicts"] if not v["passed"]} == {
        "max_duplicate_ids"
    }
    fids = list(t.manifest(v1)["data_files"])
    f_with_b = next(
        fid for fid in fids if ("b", 9) in rows_of_file(t, v1, fid)
    )
    t.publish_deletes(f_with_b, [1])
    v2 = t.current_version()
    r2 = store.create_or_get(strict, version=v2)

    diff = compare_reports(r1, r2)
    appeared = {v["rule"] for v in diff["anomalies"]["appeared"]}
    persistent = {v["rule"] for v in diff["anomalies"]["persistent"]}
    disappeared = {v["rule"] for v in diff["anomalies"]["disappeared"]}
    assert appeared == {"min_samples"}
    assert persistent == set()
    assert disappeared == {"max_duplicate_ids"}

    # 再构造一个“持续”异常：保留重复行，只删一个不重复行
    t2 = build_lake(str(tmp_path / "lake2"))
    w1 = t2.current_version()
    store2 = ProfileStore(t2)
    rr1 = store2.create_or_get(strict, version=w1)
    assert "max_duplicate_ids" in {
        v["rule"] for v in rr1["verdicts"] if not v["passed"]
    }
    f_a = next(
        fid for fid in t2.manifest(w1)["data_files"]
        if ("a", 0) in rows_of_file(t2, w1, fid)
    )
    t2.publish_deletes(f_a, [0])  # 删 a，b 仍重复
    rr2 = store2.create_or_get(strict, version=t2.current_version())
    d2 = compare_reports(rr1, rr2)
    assert "max_duplicate_ids" in {
        v["rule"] for v in d2["anomalies"]["persistent"]
    }
    assert "min_samples" in {v["rule"] for v in d2["anomalies"]["appeared"]}


# ---------------------------------------------------------------------------
# 4. 数据触犯规则：画像完整保存，失败逐条列出
# ---------------------------------------------------------------------------
def test_rule_violations_keep_full_report_with_failures(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    store = ProfileStore(t)
    report = store.create_or_get(RULES)
    assert report["passed"] is False
    failed = {v["rule"]: v for v in report["verdicts"] if not v["passed"]}
    # 样本 9 >= min_samples 8 通过；重复 id 与数值越界两条失败
    assert set(failed) == {"max_duplicate_ids", "value_bounds"}
    passed = {v["rule"] for v in report["verdicts"] if v["passed"]}
    assert passed == {"min_samples"}
    # 全部指标无论违规与否都完整存在
    for key in (
        "sample_count", "null_fields", "duplicate_id_count", "value_min",
        "value_max", "value_sum", "bucket_counts", "out_of_buckets",
    ):
        assert key in report["metrics"]
    # 报告确实落盘且为只读
    path = store.report_path(report["revision"], report["rule_digest"])
    assert os.path.exists(path)
    assert not os.access(path, os.W_OK)


# ---------------------------------------------------------------------------
# 5. 错误配置被拒，且没有任何报告落盘
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "bad",
    [
        {"min_samples": -1},
        {"min_samples": "8"},
        {"max_duplicate_ids": True},
        {"value_bounds": {"min": 10, "max": 0}},
        {"value_bounds": {"min": "x", "max": 1}},
        {"value_bounds": {"min": 1}},
        {"buckets": [{"name": "x", "lower": 5, "upper": 5}]},
        {"buckets": [
            {"name": "a", "lower": 0, "upper": 10},
            {"name": "b", "lower": 5, "upper": 20},
        ]},
        {"buckets": [{"name": "", "lower": 0, "upper": 1}]},
        {"buckets": [
            {"name": "a", "lower": 0, "upper": 1},
            {"name": "a", "lower": 1, "upper": 2},
        ]},
        {"unknown_key": 1},
        "not-a-dict",
    ],
)
def test_malformed_rules_rejected_and_nothing_persisted(tmp_path, bad):
    t = build_lake(str(tmp_path / "lake"))
    store = ProfileStore(t)
    with pytest.raises(RuleSpecError):
        store.create_or_get(bad)
    assert store.list_reports() == []
    # 连临时残骸都不应留下
    leftovers = [
        n for n in os.listdir(store.profiles_root) if n.startswith(".tmp-")
    ] if os.path.isdir(store.profiles_root) else []
    assert leftovers == []


def test_rule_digest_is_order_insensitive_but_content_sensitive():
    r1 = normalize_rules({"buckets": [
        {"name": "b", "lower": 10, "upper": 20},
        {"name": "a", "lower": 0, "upper": 10},
    ]})
    r2 = normalize_rules({"buckets": [
        {"name": "a", "lower": 0, "upper": 10},
        {"name": "b", "lower": 10, "upper": 20},
    ]})
    assert rules_digest(r1) == rules_digest(r2)
    r3 = normalize_rules({"min_samples": 1})
    assert rules_digest(r3) != rules_digest(r1)


# ---------------------------------------------------------------------------
# 6. 同配置重复请求读原报告；不同规则各存一份、互不覆盖
# ---------------------------------------------------------------------------
def test_duplicate_request_returns_saved_report_and_new_rules_coexist(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    v = t.current_version()
    store = ProfileStore(t)
    r1 = store.create_or_get(RULES, version=v)
    path1 = store.report_path(v, r1["rule_digest"])
    before = os.stat(path1)

    r2 = store.create_or_get(dict(RULES), version=v)  # 内容相同、键顺序无关
    assert r2["reused"] is True
    assert r1["reused"] is False
    assert r2["report_id"] == r1["report_id"]
    after = os.stat(path1)
    assert (before.st_ino, before.st_mtime_ns, before.st_size) == (
        after.st_ino, after.st_mtime_ns, after.st_size,
    )  # 原报告一个字节都没被改写

    other_rules = {"min_samples": 99}
    r3 = store.create_or_get(other_rules, version=v)
    assert r3["reused"] is False
    assert r3["rule_digest"] != r1["rule_digest"]
    path3 = store.report_path(v, r3["rule_digest"])
    assert path3 != path1
    assert os.path.exists(path1) and os.path.exists(path3)
    assert len(store.list_reports()) == 2
    # 再用另一组规则请求，绝不能覆盖前两份
    store.create_or_get({"min_samples": 1}, version=v)
    assert len(store.list_reports()) == 3
    assert os.path.exists(path1)


def test_frozen_report_is_read_only_and_immutable(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    store = ProfileStore(t)
    r = store.create_or_get(RULES)
    path = store.report_path(r["revision"], r["rule_digest"])
    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode == 0o444
    with pytest.raises(PermissionError):
        with open(path, "w") as f:
            f.write("tamper")


# ---------------------------------------------------------------------------
# 7. 租约保护：持旧版租约时 GC 不能删输入；释放后可以
# ---------------------------------------------------------------------------
def test_lease_protects_profile_inputs_then_release_allows_gc(tmp_path):
    root = str(tmp_path / "lake")
    t = build_lake(root)
    v1 = t.current_version()
    m1 = t.manifest(v1)
    old_paths = [m["path"] for m in m1["data_files"].values()]

    # 模拟画像计算期间持租约：在租约内让表发生压实，旧文件成为 GC 目标
    with t.lease(v1):
        fids = list(m1["data_files"])
        plan_id = create_plan(t, selected=fids)
        commit_plan(t, plan_id)
        gc = collect(t)
        assert v1 in gc["protected_versions"]
        assert gc["removed_files"] == []
        for rel in old_paths:
            assert os.path.exists(os.path.join(root, rel))
        # 受保护期间仍可对旧修订号计算画像（输入齐全）
        rep = compute_profile(t, v1, RULES, batch_size=4)
        assert rep["metrics"]["sample_count"] == 9

    gc = collect(t)
    assert set(gc["removed_files"]) == set(old_paths)
    for rel in old_paths:
        assert not os.path.exists(os.path.join(root, rel))


def test_profile_create_holds_lease_through_scan(tmp_path, monkeypatch):
    """create_or_get 扫描期间必须持有租约：钩子在扫描中断言租约文件存在。"""
    t = build_lake(str(tmp_path / "lake"))
    v = t.current_version()
    seen = {}
    orig = iter_visible_batches

    def spying(table, version, batch_size):
        leases = [
            n for n in os.listdir(table.leases_dir) if n.startswith("lease-")
        ]
        seen["leases_during_scan"] = leases
        assert leases, "扫描期间必须存在读者租约"
        yield from orig(table, version, batch_size)

    monkeypatch.setattr(
        "lake.quality.profile.iter_visible_batches", spying
    )
    store = ProfileStore(t)
    store.create_or_get(RULES, version=v, batch_size=2)
    assert seen["leases_during_scan"]
    # 扫描结束租约已释放
    assert os.listdir(t.leases_dir) == []


# ---------------------------------------------------------------------------
# 8. 发布边界：真正退出的子进程制造中断，再请求恢复
# ---------------------------------------------------------------------------
def _crashing_profile_worker(root, rules, crash_point, q):
    try:
        os.environ["LAKE_QUALITY_CRASH_POINT"] = crash_point
        t = LakeTable.open(root)
        ProfileStore(t).create_or_get(rules)
        q.put(("unexpected-ok", None))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def _run_crash_recovery(tmp_path, crash_point):
    root = str(tmp_path / "lake")
    t = build_lake(root)
    v = t.current_version()
    store = ProfileStore(t)
    q = CTX.Queue()
    p = CTX.Process(
        target=_crashing_profile_worker, args=(root, RULES, crash_point, q)
    )
    p.start()
    p.join(60)
    assert p.exitcode == 23  # os._exit 真退出，没有 finally 兜底

    # 没有任何正式报告暴露
    assert store.list_reports() == []
    published = os.listdir(store.reports_dir)
    assert published == []
    # prepared/computed 在创建临时文件之前退出，无残骸；staged 在临时文件
    # 写完、rename 之前退出，留下一个 .tmp 残骸——但 reports/ 下绝不会有
    # 写了一半的报告。
    tmps = [n for n in os.listdir(store.profiles_root) if n.startswith(".tmp-")]
    if crash_point == "staged":
        assert len(tmps) == 1
    else:
        assert tmps == []

    # 相同请求恢复：清理残骸并成功发布
    report = store.create_or_get(RULES, version=v)
    assert report["reused"] is False
    assert report["metrics"]["sample_count"] == 9
    assert store.list_reports() and not [
        n for n in os.listdir(store.profiles_root) if n.startswith(".tmp-")
    ]
    # 再请求一次走复用路径
    assert store.create_or_get(RULES, version=v)["reused"] is True


def test_crash_before_compute_then_recover(tmp_path):
    _run_crash_recovery(tmp_path, "prepared")


def test_crash_after_compute_before_publish_then_recover(tmp_path):
    _run_crash_recovery(tmp_path, "computed")


def test_crash_with_staged_tmp_file_then_recover(tmp_path):
    _run_crash_recovery(tmp_path, "staged")


# ---------------------------------------------------------------------------
# 9. 原始数据清理后，冻结报告仍可打开、比较（报告自包含）
# ---------------------------------------------------------------------------
def test_reports_survive_data_gc_and_remain_comparable(tmp_path):
    root = str(tmp_path / "lake")
    t = build_lake(root)
    v1 = t.current_version()
    store = ProfileStore(t)
    r1 = store.create_or_get(RULES, version=v1)

    fids = sorted(t.manifest(v1)["data_files"])
    t.publish_deletes(fids[0], [1])
    v2 = t.current_version()
    r2 = store.create_or_get(RULES, version=v2)

    # 压实 + 释放全部租约 + GC：旧数据文件被回收
    plan_id = create_plan(t, selected=list(t.manifest(v2)["data_files"]))
    commit_plan(t, plan_id)
    gc = collect(t)
    assert gc["removed_files"]  # 确实有数据文件被删除

    # 画像目录独立于 data/，报告一个不少
    reports = store.list_reports()
    assert len(reports) == 2
    got1 = store.get(r1["revision"], r1["rule_digest"])
    got2 = store.get(r2["revision"], r2["rule_digest"])
    assert got1["metrics"] == r1["metrics"]
    diff = compare_reports(got1, got2)
    assert diff["metric_diffs"]["sample_count"]["delta"] == -1
    # 即使旧修订号的数据文件已不存在，报告仍可打开（不触达数据文件）
    with pytest.raises(FileNotFoundError):
        t.scan(r1["revision"])  # 原始数据确实没了
    # 报告文件本身完好且只读
    for r in (r1, r2):
        p = store.report_path(r["revision"], r["rule_digest"])
        assert os.path.exists(p) and not os.access(p, os.W_OK)


# ---------------------------------------------------------------------------
# 10. 计数来自查询结果：物理分片 num_rows 之和 != 可见行数
# ---------------------------------------------------------------------------
def test_counts_come_from_query_not_shard_stats(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    v1 = t.current_version()
    physical_rows = sum(
        m["num_rows"] for m in t.manifest(v1)["data_files"].values()
    )
    fids = list(t.manifest(v1)["data_files"])
    f_first = next(
        fid for fid in fids if ("a", 0) in rows_of_file(t, v1, fid)
    )
    t.publish_deletes(f_first, [0, 2])  # 物理文件仍在，只是两行对查询不可见
    v2 = t.current_version()

    # 分片统计相加仍是 9，但查询结果只有 7
    assert sum(m["num_rows"] for m in t.manifest(v2)["data_files"].values()) == physical_rows
    store = ProfileStore(t)
    report = store.create_or_get(RULES, version=v2)
    assert report["metrics"]["sample_count"] == physical_rows - 2
    ref_rows = reference_visible_rows(t, v2)
    assert len(ref_rows) == physical_rows - 2
    assert report["metrics"] == reference_profile(t, v2, RULES)["metrics"]


def test_empty_table_profile(tmp_path):
    t = LakeTable.create(str(tmp_path / "lake"))
    store = ProfileStore(t)
    r = store.create_or_get(RULES)
    m = r["metrics"]
    assert m["sample_count"] == 0
    assert m["value_min"] is None and m["value_max"] is None
    assert m["value_sum"] == 0
    assert m["bucket_counts"] == {"low": 0, "mid": 0, "high": 0}
    failed = {v["rule"] for v in r["verdicts"] if not v["passed"]}
    assert "min_samples" in failed
    assert "value_bounds" not in failed  # 空表不触犯上下界
    assert "max_duplicate_ids" not in failed


def test_report_identity_and_content_fields(tmp_path):
    t = build_lake(str(tmp_path / "lake"))
    store = ProfileStore(t)
    r = store.create_or_get(RULES)
    for key in (
        "report_id", "revision", "rules", "rule_digest", "rule_summary",
        "metrics", "verdicts", "passed", "frozen_at", "schema_version",
    ):
        assert key in r
    # 报告内保存的是规范化后的规则（分段已排序）
    names = [b["name"] for b in r["rules"]["buckets"]]
    assert names == sorted(names, key=lambda n: {"low": 0, "mid": 1, "high": 2}[n])
    assert r["report_id"] == f"rev{r['revision']:06d}-{r['rule_digest'][:12]}"
    # JSON 可被外部工具独立打开
    with open(store.report_path(r["revision"], r["rule_digest"])) as f:
        again = json.load(f)
    assert again["metrics"] == r["metrics"]
    assert rule_summary(r["rules"]) == r["rule_summary"]
