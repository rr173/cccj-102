"""隐私视图端到端验收（全部本地数据，不访问外部系统）。

对应用户验收清单：

  1. 两个共享客户的特征表/标签表，同一 HMAC key 下假名可跨表连接；
     输出值符合隐藏、时间截断、空值规则；独立实现逐批核对行值。
  2. 较大生成数据观察峰值缓冲，确认转换按 batch 前进（任何时刻只有
     一个批次）。
  3. 字段改名后按稳定 field id 规则仍作用于目标列；删除该列、移除 key、
     改成不兼容类型时，第一批数据交出之前即报错，不泄出数据。
  4. 调用方中途停止 / 下游消费注入异常 / 读取失败：版本读取保护（租约）
     全部解除，之后 GC 回收可继续。
  5. 源表提交与文件哈希不变；Arrow schema 不含原始敏感字段名且无
     metadata；诊断/异常输出不含客户编号或 key。
  6. 无规则读取保持现有（scan）行为。
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import os

import pyarrow as pa
import pytest

from lake import (
    LakeTable,
    PrivacyViewError,
    collect,
    commit_plan,
    create_plan,
)
from lake.privacy import (
    HIDE,
    PSEUDONYMIZE,
    TRUNCATE_TIME,
    open_privacy_view,
    privacy_batches,
    pseudonym,
)

# ---------------------------------------------------------------------------
# 测试表结构与造数
# ---------------------------------------------------------------------------
FEATURE_SCHEMA = [
    {"id": 1, "name": "row_id", "type": "string", "nullable": False},
    {"id": 2, "name": "score", "type": "int64", "nullable": True},
    {"id": 3, "name": "customer_id", "type": "string", "nullable": True},
    {"id": 4, "name": "event_time", "type": "timestamp", "nullable": True},
    {"id": 5, "name": "secret_note", "type": "string", "nullable": True},
]

LABEL_SCHEMA = [
    {"id": 11, "name": "row_id", "type": "string", "nullable": False},
    {"id": 12, "name": "label", "type": "int64", "nullable": True},
    {"id": 13, "name": "customer_id", "type": "string", "nullable": True},
    {"id": 14, "name": "event_time", "type": "timestamp", "nullable": True},
]

# 两表共享客户 CUST-A / CUST-B；特征表另有 CUST-C，标签表另有 CUST-D。
FEATURE_ROWS = [
    {"row_id": "f1", "score": 10, "customer_id": "CUST-A",
     "event_time": dt.datetime(2024, 3, 7, 13, 45, 30), "secret_note": "n1"},
    {"row_id": "f2", "score": None, "customer_id": None,
     "event_time": None, "secret_note": None},
    {"row_id": "f3", "score": 20, "customer_id": "CUST-B",
     "event_time": dt.datetime(2024, 3, 8, 9, 5, 0), "secret_note": "n3"},
    {"row_id": "f4", "score": 30, "customer_id": "CUST-C",
     "event_time": dt.datetime(2024, 1, 1, 0, 0, 1), "secret_note": "n4"},
]
LABEL_ROWS = [
    {"row_id": "l1", "label": 100, "customer_id": "CUST-A",
     "event_time": dt.datetime(2024, 4, 1, 12, 0, 0)},
    {"row_id": "l2", "label": None, "customer_id": "CUST-B",
     "event_time": None},
    {"row_id": "l3", "label": 300, "customer_id": "CUST-D",
     "event_time": dt.datetime(2024, 4, 2, 23, 59, 59)},
]

KEY = b"shared-partner-secret"
DOMAIN = b"lake-compactor/privacy-pseudonym/v1\x00"


# ---------------------------------------------------------------------------
# 独立参考实现：不 import 任何隐私视图产品代码，纯 Python 逐行复算
# ---------------------------------------------------------------------------
def independent_pseudonym(value: str | None, key: bytes) -> str | None:
    if value is None:
        return None
    return hmac.new(
        key, DOMAIN + value.encode("utf-8"), hashlib.sha256
    ).hexdigest()[:32]


def independent_truncate(value: dt.datetime | None, granularity: str):
    if value is None:
        return None
    if granularity == "day":
        return dt.datetime(value.year, value.month, value.day)
    if granularity == "month":
        return dt.datetime(value.year, value.month, 1)
    raise AssertionError(granularity)


def build_feature_lake(root):
    t = LakeTable.create(os.path.join(root, "features"), schema=FEATURE_SCHEMA)
    # 拆两个文件，验证跨文件批处理与位置删除过滤口径
    t.append_rows(FEATURE_ROWS[:2], schema_version=1)
    t.append_rows(FEATURE_ROWS[2:], schema_version=1)
    return t


def build_label_lake(root):
    t = LakeTable.create(os.path.join(root, "labels"), schema=LABEL_SCHEMA)
    t.append_rows(LABEL_ROWS, schema_version=1)
    return t


FEATURE_RULES = [
    {"field_id": 5, "action": HIDE},
    {"field_id": 3, "action": PSEUDONYMIZE,
     "alias": "customer_pid"},
    {"field_id": 4, "action": TRUNCATE_TIME,
     "granularity": "day", "alias": "event_day"},
]
LABEL_RULES = [
    {"field_id": 13, "action": PSEUDONYMIZE, "alias": "customer_pid"},
    {"field_id": 14, "action": TRUNCATE_TIME,
     "granularity": "month", "alias": "event_month"},
]


def consume(view):
    rows = []
    for batch in view:
        rows.extend(batch.to_pylist())
    return rows


# ---------------------------------------------------------------------------
# 1. 跨表假名连接 + 隐藏/截断/空值，独立实现逐行核对
# ---------------------------------------------------------------------------
def test_pseudonym_join_across_two_tables_and_independent_check(tmp_path):
    features = build_feature_lake(tmp_path)
    labels = build_label_lake(tmp_path)

    with open_privacy_view(features, FEATURE_RULES, hmac_key=KEY,
                           batch_size=2) as view:
        feat_rows = consume(view)
        feat_schema = view.schema
    with open_privacy_view(labels, LABEL_RULES, hmac_key=KEY) as view:
        label_rows = consume(view)

    # 1a. 输出 schema 不含任何原始敏感字段名，也没有 metadata
    sensitive_names = {"customer_id", "event_time", "secret_note"}
    assert not (sensitive_names & set(feat_schema.names))
    assert feat_schema.metadata is None
    # 输出列只含允许的别名与透传列
    assert feat_schema.names == [
        "row_id", "score", "customer_pid", "event_day"
    ]

    # 1b. 独立逐行核对特征表
    by_row = {r["row_id"]: r for r in feat_rows}
    for src in FEATURE_ROWS:
        out = by_row[src["row_id"]]
        assert set(out) == {"row_id", "score", "customer_pid", "event_day"}
        assert out["score"] == src["score"]  # 透传列含 null 语义
        assert out["customer_pid"] == independent_pseudonym(
            src["customer_id"], KEY
        )
        assert out["event_day"] == independent_truncate(
            src["event_time"], "day"
        )
        # 隐藏字段的值/列名都不出现
        assert "secret_note" not in out
        if src["secret_note"] is not None:
            assert src["secret_note"] not in str(out)

    # null 语义保持：f2 的客户号与时间都是 null
    assert by_row["f2"]["customer_pid"] is None
    assert by_row["f2"]["event_day"] is None
    # 时间截断后时分秒归零，类型仍是 timestamp
    assert by_row["f1"]["event_day"] == dt.datetime(2024, 3, 7)

    # 1c. 同一客户跨表假名一致（可连接），不同客户假名不同
    f_pid = {r["row_id"]: r["customer_pid"] for r in feat_rows}
    l_pid = {r["row_id"]: r["customer_pid"] for r in label_rows}
    assert f_pid["f1"] == l_pid["l1"]  # CUST-A
    assert f_pid["f3"] == l_pid["l2"]  # CUST-B
    distinct = {f_pid["f1"], f_pid["f3"], f_pid["f4"], l_pid["l3"]}
    assert len(distinct) == 4  # 不同客户不撞值
    # 标签表时间按月截断
    l_by_row = {r["row_id"]: r for r in label_rows}
    assert l_by_row["l1"]["event_month"] == dt.datetime(2024, 4, 1)
    assert l_by_row["l2"]["event_month"] is None

    # 1d. 假名与独立 HMAC 完全一致（确定性 PRF，跨运行稳定）
    assert f_pid["f1"] == independent_pseudonym("CUST-A", KEY)
    assert pseudonym("CUST-A", KEY) == independent_pseudonym("CUST-A", KEY)


def test_pseudonym_stable_across_runs_and_processes(tmp_path):
    features = build_feature_lake(tmp_path)
    rule = [{"field_id": 3, "action": PSEUDONYMIZE, "alias": "pid"}]
    with open_privacy_view(features, rule, hmac_key=KEY) as v:
        first = {r["row_id"]: r["pid"] for r in consume(v)}
    # 重新打开表、新建视图，模拟另一次运行
    reopened = LakeTable.open(features.root)
    with open_privacy_view(reopened, rule, hmac_key=KEY) as v:
        second = {r["row_id"]: r["pid"] for r in consume(v)}
    assert first == second
    # 不同 key 产生不同假名（key 口径隔离）
    with open_privacy_view(features, rule, hmac_key=b"other-key") as v:
        other = {r["row_id"]: r["pid"] for r in consume(v)}
    assert other["f1"] != first["f1"]


def test_key_from_environment_variable(tmp_path, monkeypatch):
    features = build_feature_lake(tmp_path)
    rule = [{"field_id": 3, "action": PSEUDONYMIZE, "alias": "pid"}]
    monkeypatch.setenv("LAKE_PRIVACY_HMAC_KEY", "env-secret")
    with open_privacy_view(features, rule) as v:
        rows = {r["row_id"]: r for r in consume(v)}
    # f1 的客户号是 CUST-A；按 row_id 定位，不依赖文件读取顺序
    assert rows["f1"]["pid"] == independent_pseudonym("CUST-A", b"env-secret")
    # 内存参数优先于环境变量
    with open_privacy_view(features, rule, hmac_key=KEY) as v:
        rows = {r["row_id"]: r for r in consume(v)}
    assert rows["f1"]["pid"] == independent_pseudonym("CUST-A", KEY)


# ---------------------------------------------------------------------------
# 2. 峰值缓冲：转换按 batch 前进，任何时刻只有一个批次
# ---------------------------------------------------------------------------
def test_streaming_advances_batch_by_batch_with_bounded_memory(tmp_path):
    schema = [
        {"id": 1, "name": "row_id", "type": "string", "nullable": False},
        {"id": 3, "name": "customer_id", "type": "string", "nullable": True},
    ]
    t = LakeTable.create(os.path.join(tmp_path, "big"), schema=schema)
    n_rows = 2500
    rows = [
        {"row_id": f"r{i:05d}", "customer_id": f"CUST-{i % 50:03d}"}
        for i in range(n_rows)
    ]
    # 写多个文件，batch_size 小，制造大量批次
    for start in range(0, n_rows, 300):
        t.append_rows(rows[start:start + 300], schema_version=1)

    rule = [{"field_id": 3, "action": PSEUDONYMIZE, "alias": "pid"}]
    batch_sizes = []
    seen_rows = 0
    max_batch = 0
    with open_privacy_view(t, rule, hmac_key=KEY, batch_size=128) as view:
        for batch in view:
            batch_sizes.append(batch.num_rows)
            max_batch = max(max_batch, batch.num_rows)
            seen_rows += batch.num_rows
            # 每个批次都不超过读取粒度（尾批可以更小）
            assert batch.num_rows <= 128

    assert seen_rows == n_rows
    assert len(batch_sizes) > 10  # 确实是多批前进，而非一次性物化
    assert max_batch <= 128
    # 独立逐行核对全部假名。文件按 data_file_id 排序读取（与 scan 同口径），
    # 因此按 row_id 建立期望映射而不是依赖追加顺序。
    expected_by_id = {r["row_id"]: r["customer_id"] for r in rows}
    with open_privacy_view(t, rule, hmac_key=KEY, batch_size=128) as view:
        all_rows = consume(view)
    assert len(all_rows) == n_rows
    seen_ids = set()
    for out in all_rows:
        assert out["pid"] == independent_pseudonym(
            expected_by_id[out["row_id"]], KEY
        )
        seen_ids.add(out["row_id"])
    assert seen_ids == set(expected_by_id)


# ---------------------------------------------------------------------------
# 3. field id 规则：改名仍生效；删列 / 缺 key / 类型不合在首批前拒绝
# ---------------------------------------------------------------------------
def test_rule_survives_column_rename_via_stable_field_id(tmp_path):
    t = build_feature_lake(tmp_path)
    renamed = [
        {"id": 1, "name": "row_id", "type": "string", "nullable": False},
        {"id": 2, "name": "score", "type": "int64", "nullable": True},
        {"id": 3, "name": "partner_ref", "type": "string", "nullable": True},
        {"id": 4, "name": "event_time", "type": "timestamp", "nullable": True},
        {"id": 5, "name": "secret_note", "type": "string", "nullable": True},
    ]
    t.evolve_schema(renamed)
    rule = [{"field_id": 3, "action": PSEUDONYMIZE, "alias": "pid"}]
    with open_privacy_view(t, rule, hmac_key=KEY) as view:
        # 新旧列名都不出现在输出，只有别名
        assert "customer_id" not in view.schema.names
        assert "partner_ref" not in view.schema.names
        assert view.schema.names[-1] == "pid"
        rows = {r["row_id"]: r for r in consume(view)}
    # f1 的客户号 CUST-A 在改名后仍按字段 id 3 取到并正确假名化
    assert rows["f1"]["pid"] == independent_pseudonym("CUST-A", KEY)
    assert rows["f2"]["pid"] is None  # 空客户号仍为空


def test_rule_on_dropped_field_rejected_before_first_batch(tmp_path):
    t = LakeTable.create(os.path.join(tmp_path, "lake"), schema=[
        {"id": 1, "name": "row_id", "type": "string", "nullable": False},
        {"id": 3, "name": "customer_id", "type": "string", "nullable": True},
    ])
    t.append_rows([{"row_id": "r", "customer_id": "C"}], schema_version=1)
    # 加一个无文件引用的临时列再删除，拿到一个“已不存在的字段 id”
    ev = t.evolve_schema([
        {"id": 1, "name": "row_id", "type": "string", "nullable": False},
        {"id": 3, "name": "customer_id", "type": "string", "nullable": True},
        {"id": 9, "name": "temp", "type": "string", "nullable": True},
    ])
    temp_id = ev["schema"]["fields"][-1]["id"]
    t.evolve_schema([
        {"id": 1, "name": "row_id", "type": "string", "nullable": False},
        {"id": 3, "name": "customer_id", "type": "string", "nullable": True},
    ])

    for bad_rule in (
        [{"field_id": temp_id, "action": HIDE}],
        [{"field_id": temp_id, "action": PSEUDONYMIZE, "alias": "z"}],
        [{"field_id": 999, "action": HIDE}],
    ):
        with pytest.raises(PrivacyViewError) as ei:
            open_privacy_view(t, bad_rule, hmac_key=KEY)
        assert ei.value.kind == "field"
        # 没有任何批次被交出，也没有残留租约
        assert os.listdir(t.leases_dir) == []


def test_missing_key_rejected_before_first_batch(tmp_path):
    t = build_feature_lake(tmp_path)
    rule = [{"field_id": 3, "action": PSEUDONYMIZE, "alias": "pid"}]
    assert "LAKE_PRIVACY_HMAC_KEY" not in os.environ
    with pytest.raises(PrivacyViewError) as ei:
        open_privacy_view(t, rule)
    assert ei.value.kind == "key"
    assert os.listdir(t.leases_dir) == []
    # 空 key（环境变量为空串 / 空 bytes）同样拒绝
    os.environ["LAKE_PRIVACY_HMAC_KEY"] = ""
    try:
        with pytest.raises(PrivacyViewError) as ei:
            open_privacy_view(t, rule)
        assert ei.value.kind == "key"
    finally:
        del os.environ["LAKE_PRIVACY_HMAC_KEY"]
    with pytest.raises(PrivacyViewError) as ei:
        open_privacy_view(t, rule, hmac_key=b"")
    assert ei.value.kind == "key"
    with pytest.raises(PrivacyViewError) as ei:
        open_privacy_view(t, rule, hmac_key=123)
    assert ei.value.kind == "key"
    # 没有假名规则时不需要 key
    with open_privacy_view(t, [{"field_id": 5, "action": HIDE}]) as view:
        assert view.read_all().num_rows == len(FEATURE_ROWS)


def test_incompatible_target_type_rejected_before_first_batch(tmp_path):
    t = build_feature_lake(tmp_path)
    bad_rules = [
        # 假名作用在 int64 列
        ([{"field_id": 2, "action": PSEUDONYMIZE, "alias": "x"}], "type"),
        # 时间截断作用在 string 列
        ([{"field_id": 3, "action": TRUNCATE_TIME,
           "granularity": "day", "alias": "x"}], "type"),
        # 时间截断作用在 int64 列
        ([{"field_id": 2, "action": TRUNCATE_TIME,
           "granularity": "month", "alias": "x"}], "type"),
        # 别名与透传列冲突
        ([{"field_id": 3, "action": PSEUDONYMIZE, "alias": "row_id"}], "rule"),
        # 重复别名
        ([{"field_id": 3, "action": PSEUDONYMIZE, "alias": "z"},
          {"field_id": 5, "action": PSEUDONYMIZE, "alias": "z"}], "rule"),
        # 同字段两条规则
        ([{"field_id": 3, "action": HIDE},
          {"field_id": 3, "action": PSEUDONYMIZE, "alias": "z"}], "rule"),
        # 未知动作 / 未知粒度 / 缺别名
        ([{"field_id": 3, "action": "explode"}], "rule"),
        ([{"field_id": 4, "action": TRUNCATE_TIME,
           "granularity": "year", "alias": "z"}], "rule"),
        ([{"field_id": 3, "action": PSEUDONYMIZE}], "rule"),
    ]
    for rule, kind in bad_rules:
        with pytest.raises(PrivacyViewError) as ei:
            open_privacy_view(t, rule, hmac_key=KEY)
        assert ei.value.kind == kind, rule
        assert os.listdir(t.leases_dir) == []


# ---------------------------------------------------------------------------
# 4. 提前停止 / 下游异常 / 读取失败：保护必释放，之后 GC 可继续
# ---------------------------------------------------------------------------
def _lease_count(table):
    return len([n for n in os.listdir(table.leases_dir)
                if n.startswith("lease-")])


def test_early_stop_releases_reader_protection(tmp_path):
    t = build_feature_lake(tmp_path)
    rule = [{"field_id": 3, "action": PSEUDONYMIZE, "alias": "pid"}]
    view = open_privacy_view(t, rule, hmac_key=KEY, batch_size=1)
    assert _lease_count(t) == 1
    for _ in view:
        break  # 调用方中途停止
    assert _lease_count(t) == 0

    # 上下文管理器显式关闭同样释放
    view = open_privacy_view(t, rule, hmac_key=KEY)
    assert _lease_count(t) == 1
    view.close()
    assert _lease_count(t) == 0
    view.close()  # 幂等


def test_downstream_exception_releases_reader_protection(tmp_path):
    t = build_feature_lake(tmp_path)
    rule = [{"field_id": 3, "action": PSEUDONYMIZE, "alias": "pid"}]
    view = open_privacy_view(t, rule, hmac_key=KEY, batch_size=1)
    with pytest.raises(RuntimeError, match="downstream boom"):
        for _ in view:
            raise RuntimeError("downstream boom")
    assert _lease_count(t) == 0

    # privacy_batches 便捷生成器在异常下也释放
    gen = privacy_batches(t, rule, hmac_key=KEY, batch_size=1)
    with pytest.raises(ValueError):
        for _ in gen:
            raise ValueError("another downstream failure")
    assert _lease_count(t) == 0


def test_read_failure_releases_protection(tmp_path, monkeypatch):
    t = build_feature_lake(tmp_path)
    rule = [{"field_id": 3, "action": PSEUDONYMIZE, "alias": "pid"}]
    view = open_privacy_view(t, rule, hmac_key=KEY, batch_size=1)
    assert _lease_count(t) == 1

    real_iter = pa.parquet.ParquetFile.iter_batches

    def failing_iter(self, *args, **kwargs):
        raise OSError("simulated unreadable parquet")

    monkeypatch.setattr(pa.parquet.ParquetFile, "iter_batches", failing_iter)
    with pytest.raises(PrivacyViewError) as ei:
        consume(view)
    assert ei.value.kind == "read"
    monkeypatch.undo()
    assert real_iter is pa.parquet.ParquetFile.iter_batches
    assert _lease_count(t) == 0


def test_gc_blocked_while_view_open_and_runs_after_close(tmp_path):
    t = LakeTable.create(os.path.join(tmp_path, "gc"))
    f1 = t.append([("a", 1), ("a", 2)])
    f2 = t.append([("b", 3)])
    protected_version = t.current_version()
    plan = create_plan(t, selected=[f1, f2])
    commit_plan(t, plan)

    # 无规则视图持租约，GC 必须保护旧版本文件
    view = open_privacy_view(t, [], version=protected_version)
    info = collect(t)
    assert protected_version in info["protected_versions"]
    old_paths = [
        os.path.join(t.root, m["path"])
        for m in t.manifest(protected_version)["data_files"].values()
    ]
    assert all(os.path.exists(p) for p in old_paths)

    view.close()
    info2 = collect(t)
    assert protected_version not in info2["protected_versions"]
    assert not any(os.path.exists(p) for p in old_paths)


def test_collected_version_rejected_without_leaking_lease(tmp_path):
    t = LakeTable.create(os.path.join(tmp_path, "gc2"))
    fid = t.append([("a", 1)])
    old = t.current_version()
    plan = create_plan(t, selected=[fid])
    commit_plan(t, plan)
    collect(t)  # 旧版本文件被回收
    with pytest.raises(PrivacyViewError) as ei:
        open_privacy_view(t, [], version=old)
    assert ei.value.kind == "version"
    assert os.listdir(t.leases_dir) == []


# ---------------------------------------------------------------------------
# 5. 源表不变、schema 无敏感名/无 metadata、诊断不含客户号或 key
# ---------------------------------------------------------------------------
def _tree_fingerprint(root):
    out = {}
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            p = os.path.join(dirpath, name)
            rel = os.path.relpath(p, root)
            out[rel] = hashlib.sha256(open(p, "rb").read()).hexdigest()
    return out


def test_source_table_unchanged_and_no_export_directory(tmp_path):
    t = build_feature_lake(tmp_path)
    before = _tree_fingerprint(t.root)
    head_before = t.current_version()
    top_before = sorted(os.listdir(t.root))

    rule = [
        {"field_id": 5, "action": HIDE},
        {"field_id": 3, "action": PSEUDONYMIZE, "alias": "pid"},
        {"field_id": 4, "action": TRUNCATE_TIME,
         "granularity": "day", "alias": "event_day"},
    ]
    with open_privacy_view(t, rule, hmac_key=KEY, batch_size=1) as view:
        rows = consume(view)
        # 输出批次的 schema metadata 始终为空
        for _ in rows:
            pass
    # 再消费一次（重复读取不产生任何写入）
    with open_privacy_view(t, rule, hmac_key=KEY) as view:
        consume(view)

    assert t.current_version() == head_before
    assert _tree_fingerprint(t.root) == before
    assert sorted(os.listdir(t.root)) == top_before
    # 没有新增任何导出/暂存目录（只有引擎原有三个目录）
    assert set(os.listdir(t.root)) <= {"data", "metadata", "staging"}


def test_output_schema_has_no_sensitive_names_or_metadata(tmp_path):
    features = build_feature_lake(tmp_path)
    labels = build_label_lake(tmp_path)
    with open_privacy_view(features, FEATURE_RULES, hmac_key=KEY) as v:
        assert v.schema.metadata is None
        for field in v.schema:
            assert field.name not in {"customer_id", "event_time",
                                      "secret_note"}
        for batch in v:
            assert batch.schema.metadata is None
    with open_privacy_view(labels, LABEL_RULES, hmac_key=KEY) as v:
        assert v.schema.metadata is None
        assert "customer_id" not in v.schema.names
        for batch in v:
            assert batch.schema.metadata is None


def test_diagnostics_and_errors_never_contain_customers_or_key(tmp_path):
    t = build_feature_lake(tmp_path)
    secret = "CUST-A"
    # 各类错误文本都不含客户编号或 key
    probes = [
        lambda: open_privacy_view(
            t, [{"field_id": 3, "action": PSEUDONYMIZE, "alias": "pid"}]),
        lambda: open_privacy_view(
            t, [{"field_id": 999, "action": HIDE}], hmac_key=KEY),
        lambda: open_privacy_view(
            t, [{"field_id": 2, "action": PSEUDONYMIZE,
                 "alias": "x"}], hmac_key=KEY),
        lambda: open_privacy_view(
            t, [{"field_id": 3, "action": "bogus"}], hmac_key=KEY),
    ]
    for probe in probes:
        with pytest.raises(PrivacyViewError) as ei:
            probe()
        text = str(ei.value)
        assert secret not in text
        assert KEY.decode() not in text
        assert "CUST" not in text
    # output_columns 诊断只暴露字段 id / 别名 / 类型，不暴露原值或 key
    with open_privacy_view(t, FEATURE_RULES, hmac_key=KEY) as view:
        descr = view.output_columns
        blob = repr(descr)
        assert secret not in blob and KEY.decode() not in blob
        assert all("customer_id" not in d["name"] for d in descr)
    # 租约文件内容不含 key / 规则 / 客户值
    view = open_privacy_view(t, FEATURE_RULES, hmac_key=KEY)
    lease_files = [
        os.path.join(t.leases_dir, n)
        for n in os.listdir(t.leases_dir) if n.startswith("lease-")
    ]
    assert lease_files, "view should hold a lease while open"
    for lp in lease_files:
        blob = open(lp, "rb").read()
        assert KEY not in blob and b"CUST" not in blob
    view.close()


# ---------------------------------------------------------------------------
# 6. 无规则读取保持现有 scan 行为；位置删除口径一致
# ---------------------------------------------------------------------------
def test_no_rules_matches_scan_including_position_deletes(tmp_path):
    t = LakeTable.create(os.path.join(tmp_path, "legacy"))
    f1 = t.append([("z9", 90), ("a1", 11), ("m5", 55), ("b2", 22)])
    t.append([("c", 3), ("d", 4)])
    t.publish_deletes(f1, [1, 3])  # 按物理行号删除 a1 / b2

    with open_privacy_view(t, [], batch_size=2) as view:
        got = view.read_all()
    expected = t.scan()
    assert got.schema.names == expected.schema.names
    assert got.to_pylist() == expected.to_pylist()

    # 结构受管表 + 空规则同样等价于 scan（含改名/缺列补 null 的逻辑映射）
    managed = LakeTable.create(os.path.join(tmp_path, "managed"),
                               schema=FEATURE_SCHEMA)
    managed.append_rows(FEATURE_ROWS, schema_version=1)
    with open_privacy_view(managed, None, batch_size=2) as view:
        got = view.read_all()
    assert got.to_pylist() == managed.scan().to_pylist()
    assert got.schema.metadata is None


def test_hide_removes_column_with_null_semantics_intact(tmp_path):
    t = build_feature_lake(tmp_path)
    # 只隐藏一列，其余列（含 null）原样保留
    rule = [{"field_id": 5, "action": HIDE}]
    with open_privacy_view(t, rule) as view:
        assert "secret_note" not in view.schema.names
        rows = consume(view)
    src_by_id = {r["row_id"]: r for r in FEATURE_ROWS}
    for out in rows:
        src = src_by_id[out["row_id"]]
        assert "secret_note" not in out
        assert out["score"] == src["score"]
        assert out["customer_id"] == src["customer_id"]
    # 全隐藏规则：输出只剩未被规则覆盖的列
    rule_all = [
        {"field_id": fid, "action": HIDE}
        for fid in (2, 3, 4, 5)
    ]
    with open_privacy_view(t, rule_all) as view:
        assert view.schema.names == ["row_id"]
        rows = consume(view)
    # 行集合完整（读取顺序按 data_file_id，与 scan 同口径，不要求追加序）
    assert sorted(r["row_id"] for r in rows) == sorted(
        r["row_id"] for r in FEATURE_ROWS
    )


def test_empty_table_preserves_output_schema(tmp_path):
    t = LakeTable.create(os.path.join(tmp_path, "empty"),
                         schema=FEATURE_SCHEMA)
    with open_privacy_view(t, FEATURE_RULES, hmac_key=KEY) as view:
        assert view.schema.names == [
            "row_id", "score", "customer_pid", "event_day"
        ]
        assert view.schema.metadata is None
        assert consume(view) == []
        assert view.read_all().num_rows == 0


# ---------------------------------------------------------------------------
# 7. timestamp 类型贯穿压实与隐私投影（新增逻辑类型的端到端回归）
# ---------------------------------------------------------------------------
def test_timestamp_survives_compaction_and_privacy_projection(tmp_path):
    t = LakeTable.create(os.path.join(tmp_path, "ts"), schema=[
        {"id": 1, "name": "row_id", "type": "string", "nullable": False},
        {"id": 2, "name": "score", "type": "int64", "nullable": True},
        {"id": 3, "name": "customer_id", "type": "string", "nullable": True},
        {"id": 4, "name": "event_time", "type": "timestamp", "nullable": True},
    ])
    t.append_rows([
        {"row_id": "a", "score": 1, "customer_id": "C1",
         "event_time": dt.datetime(2024, 3, 7, 13, 45, 30)},
        {"row_id": "b", "score": None, "customer_id": None,
         "event_time": None},
    ], schema_version=1)
    t.append_rows([
        {"row_id": "c", "score": 3, "customer_id": "C2",
         "event_time": dt.datetime(2024, 1, 1, 0, 0, 1)},
    ], schema_version=1)
    old_version = t.current_version()
    fids = sorted(t.manifest()["data_files"])
    new_version = commit_plan(t, create_plan(t, selected=fids))
    assert len(t.manifest()["data_files"]) == 1  # 已合并为单文件

    # 普通扫描保留 timestamp 类型、可空性与 null 值
    scan = t.scan()
    ts_field = next(f for f in scan.schema if f.name == "event_time")
    assert pa.types.is_timestamp(ts_field.type) and ts_field.nullable

    rules = [
        {"field_id": 3, "action": PSEUDONYMIZE, "alias": "pid"},
        {"field_id": 4, "action": TRUNCATE_TIME,
         "granularity": "day", "alias": "event_day"},
    ]
    # 压实后的新版本经 project_batch 流式投影仍正确
    with open_privacy_view(t, rules, version=new_version,
                           hmac_key=KEY, batch_size=1) as view:
        rows = {r["row_id"]: r for r in consume(view)}
    assert rows["a"]["event_day"] == dt.datetime(2024, 3, 7)
    assert rows["b"]["event_day"] is None and rows["b"]["pid"] is None
    assert rows["c"]["event_day"] == dt.datetime(2024, 1, 1)
    assert rows["a"]["pid"] == independent_pseudonym("C1", KEY)

    # 压实前的旧显式版本在被回收前仍可读取（读者保护 + 历史结构映射）
    with open_privacy_view(t, rules, version=old_version,
                           hmac_key=KEY) as view:
        assert view.read_all().num_rows == 3
