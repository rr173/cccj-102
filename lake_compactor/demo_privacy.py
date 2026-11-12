"""隐私视图端到端叙述式演示（本地样本，可手算核对，不访问外部系统）。

合作方在隔离环境联调模型，但不能拿到生产客户编号或精确事件时间。本演示
用两张共享客户的湖表（特征表 / 标签表）走查隐私视图的验收主线：

  1. 同一 HMAC key 下，两张表的同一客户得到同一假名，可按假名连接；
     输出值符合隐藏、时间截断（日/月）与空值规则——用一份独立于产品
     代码的纯 Python 参考实现逐行核对。
  2. 较大生成数据按 batch 前进：任何时刻内存里只有一个批次。
  3. 字段改名后，绑定稳定 field id 的规则仍作用于目标列；删除该列、
     移除 key、换成不兼容类型时，第一批数据交出之前即报错，不泄出数据。
  4. 调用方中途停止 / 下游消费注入异常：版本读取保护（读者租约）立即
     解除，之后垃圾回收可以继续。
  5. 源表提交与文件哈希不变；Arrow schema 不含原始敏感字段名且无
     metadata；诊断与异常文本不含客户编号或 key。
  6. 无规则读取保持与普通扫描一致的行为。

运行：python3 demo_privacy.py
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lake import LakeTable, PrivacyViewError, collect, commit_plan, create_plan  # noqa: E402
from lake.privacy import (  # noqa: E402
    HIDE,
    PSEUDONYMIZE,
    TRUNCATE_TIME,
    open_privacy_view,
    privacy_batches,
)

DOMAIN = b"lake-compactor/privacy-pseudonym/v1\x00"
KEY = b"partner-shared-hmac-key"

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

FEATURE_RULES = [
    {"field_id": 5, "action": HIDE},
    {"field_id": 3, "action": PSEUDONYMIZE, "alias": "customer_pid"},
    {"field_id": 4, "action": TRUNCATE_TIME,
     "granularity": "day", "alias": "event_day"},
]
LABEL_RULES = [
    {"field_id": 13, "action": PSEUDONYMIZE, "alias": "customer_pid"},
    {"field_id": 14, "action": TRUNCATE_TIME,
     "granularity": "month", "alias": "event_month"},
]


def ref_pid(value):
    """独立纯 Python 参考：不 import 隐私视图产品代码。"""
    if value is None:
        return None
    return hmac.new(KEY, DOMAIN + value.encode("utf-8"),
                    hashlib.sha256).hexdigest()[:32]


def ref_trunc(value, granularity):
    if value is None:
        return None
    if granularity == "day":
        return dt.datetime(value.year, value.month, value.day)
    return dt.datetime(value.year, value.month, 1)


def tree_fingerprint(root):
    out = {}
    for dp, _d, files in os.walk(root):
        for name in files:
            p = os.path.join(dp, name)
            out[os.path.relpath(p, root)] = hashlib.sha256(
                open(p, "rb").read()).hexdigest()
    return out


def lease_count(table):
    return len([n for n in os.listdir(table.leases_dir)
                if n.startswith("lease-")])


def heading(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def check(cond, message):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {message}")
    if not cond:
        raise AssertionError(message)


def main():
    workdir = tempfile.mkdtemp(prefix="demo_privacy_")
    print(f"临时工作目录（演示结束清理）: {workdir}")
    try:
        features = LakeTable.create(
            os.path.join(workdir, "features"), schema=FEATURE_SCHEMA)
        labels = LakeTable.create(
            os.path.join(workdir, "labels"), schema=LABEL_SCHEMA)
        features.append_rows(FEATURE_ROWS[:2], schema_version=1)
        features.append_rows(FEATURE_ROWS[2:], schema_version=1)
        labels.append_rows(LABEL_ROWS, schema_version=1)

        # ------------------------------------------------------------- 1
        heading("1. 跨表假名连接 + 隐藏/截断/空值，独立实现逐行核对")
        with open_privacy_view(features, FEATURE_RULES, hmac_key=KEY,
                               batch_size=2) as view:
            print("  输出 schema:", view.schema.names,
                  "| metadata:", view.schema.metadata)
            feat_rows = [r for b in view for r in b.to_pylist()]
        with open_privacy_view(labels, LABEL_RULES, hmac_key=KEY) as view:
            label_rows = [r for b in view for r in b.to_pylist()]

        check("secret_note/customer_id/event_time 均不在输出列",
              not ({"secret_note", "customer_id", "event_time"}
                   & set(view.schema.names)))
        f_by_id = {r["row_id"]: r for r in feat_rows}
        ok = all(
            f_by_id[r["row_id"]]["customer_pid"] == ref_pid(r["customer_id"])
            and f_by_id[r["row_id"]]["event_day"]
            == ref_trunc(r["event_time"], "day")
            for r in FEATURE_ROWS
        )
        check(ok, "特征表每行假名/日截断与独立 HMAC/截断参考一致")
        check(f_by_id["f2"]["customer_pid"] is None
              and f_by_id["f2"]["event_day"] is None,
              "空客户号/空时间仍为 null（空值语义保持）")
        check(f_by_id["f1"]["event_day"] == dt.datetime(2024, 3, 7),
              "事件时间截到日，时分秒归零且仍是 timestamp")

        f_pid = {r["row_id"]: r["customer_pid"] for r in feat_rows}
        l_pid = {r["row_id"]: r["customer_pid"] for r in label_rows}
        check(f_pid["f1"] == l_pid["l1"] and f_pid["f3"] == l_pid["l2"],
              "两表共享客户 CUST-A/CUST-B 假名一致，可按假名连接")
        check(len({f_pid["f1"], f_pid["f3"], f_pid["f4"], l_pid["l3"]}) == 4,
              "不同客户映射为不同假名（无碰撞）")
        l_by_id = {r["row_id"]: r for r in label_rows}
        check(l_by_id["l1"]["event_month"] == dt.datetime(2024, 4, 1),
              "标签表事件时间截到月")
        print("  特征表样例:", f_by_id["f1"])
        print("  标签表样例:", l_by_id["l1"])

        # ------------------------------------------------------------- 2
        heading("2. 较大生成数据按 batch 前进（峰值缓冲 = 单批次）")
        big = LakeTable.create(os.path.join(workdir, "big"), schema=[
            {"id": 1, "name": "row_id", "type": "string", "nullable": False},
            {"id": 3, "name": "customer_id", "type": "string",
             "nullable": True},
        ])
        n_rows = 2500
        rows = [{"row_id": f"r{i:05d}",
                 "customer_id": f"CUST-{i % 50:03d}"} for i in range(n_rows)]
        for s in range(0, n_rows, 300):
            big.append_rows(rows[s:s + 300], schema_version=1)
        rule = [{"field_id": 3, "action": PSEUDONYMIZE, "alias": "pid"}]
        sizes, total = [], 0
        with open_privacy_view(big, rule, hmac_key=KEY,
                               batch_size=128) as view:
            for batch in view:
                sizes.append(batch.num_rows)
                total += batch.num_rows
        check(total == n_rows, f"全部 {n_rows} 行都被产出")
        check(max(sizes) <= 128 and len(sizes) > 10,
              f"共 {len(sizes)} 批，单批不超过 128 行（转换按 batch 前进）")

        # ------------------------------------------------------------- 3
        heading("3. field id 规则抗改名；删列/缺 key/类型不合在首批前拒绝")
        features.evolve_schema([
            {"id": 1, "name": "row_id", "type": "string", "nullable": False},
            {"id": 2, "name": "score", "type": "int64", "nullable": True},
            {"id": 3, "name": "partner_ref", "type": "string",
             "nullable": True},
            {"id": 4, "name": "event_time", "type": "timestamp",
             "nullable": True},
            {"id": 5, "name": "secret_note", "type": "string",
             "nullable": True},
        ])
        with open_privacy_view(features, rule, hmac_key=KEY) as view:
            check("partner_ref" not in view.schema.names
                  and "customer_id" not in view.schema.names
                  and view.schema.names[-1] == "pid",
                  "改名后规则仍按 field id 3 作用，新旧列名均不出现")
            renamed = {r["row_id"]: r for r in
                       (x for b in view for x in b.to_pylist())}
        check(renamed["f1"]["pid"] == ref_pid("CUST-A"),
              "改名后假名结果与改名前一致")

        def expect_kind(bad_rules, key_arg=KEY):
            try:
                open_privacy_view(features, bad_rules, hmac_key=key_arg)
                return None
            except PrivacyViewError as e:
                return e.kind

        saved_env = os.environ.pop("LAKE_PRIVACY_HMAC_KEY", None)
        try:
            check(expect_kind([{"field_id": 3, "action": PSEUDONYMIZE,
                                "alias": "pid"}], key_arg=None) == "key",
                  "内存与环境都无 key -> kind=key，首批数据前拒绝")
        finally:
            if saved_env is not None:
                os.environ["LAKE_PRIVACY_HMAC_KEY"] = saved_env
        check(expect_kind([{"field_id": 999, "action": HIDE}]) == "field",
              "规则指向已不存在的字段 id -> kind=field")
        check(expect_kind([{"field_id": 2, "action": PSEUDONYMIZE,
                            "alias": "x"}]) == "type",
              "假名作用于 int64 列 -> kind=type")
        check(expect_kind([{"field_id": 3, "action": TRUNCATE_TIME,
                            "granularity": "day", "alias": "x"}]) == "type",
              "时间截断作用于 string 列 -> kind=type")
        check(lease_count(features) == 0,
              "全部拒绝路径都没有残留读者租约，也没有交出任何批次")

        # ------------------------------------------------------------- 4
        heading("4. 中途停止 / 下游异常：读取保护立即解除，GC 可继续")
        view = open_privacy_view(features, rule, hmac_key=KEY, batch_size=1)
        check(lease_count(features) == 1, "迭代中持有 1 个读者租约")
        for _ in view:
            break
        check(lease_count(features) == 0, "调用方 break 后租约立即解除")
        try:
            for _ in privacy_batches(features, rule, hmac_key=KEY,
                                     batch_size=1):
                raise RuntimeError("downstream boom")
        except RuntimeError:
            pass
        check(lease_count(features) == 0, "下游抛错后租约立即解除")

        gc_table = LakeTable.create(os.path.join(workdir, "gc"))
        f1 = gc_table.append([("a", 1), ("a", 2)])
        f2 = gc_table.append([("b", 3)])
        protected = gc_table.current_version()
        commit_plan(gc_table, create_plan(gc_table, selected=[f1, f2]))
        view = open_privacy_view(gc_table, [], version=protected)
        info = collect(gc_table)
        check(protected in info["protected_versions"],
              "视图打开期间 GC 保护旧版本文件")
        view.close()
        info = collect(gc_table)
        check(protected not in info["protected_versions"],
              "视图关闭后旧版本不再受保护，GC 回收可继续")

        # ------------------------------------------------------------- 5
        heading("5. 源表不变；schema 无敏感名/无 metadata；诊断不含秘密")
        before = tree_fingerprint(features.root)
        head_before = features.current_version()
        with open_privacy_view(features, FEATURE_RULES, hmac_key=KEY,
                               batch_size=1) as view:
            for _ in view:
                pass
        check(features.current_version() == head_before,
              "隐私读取不产生新提交")
        check(tree_fingerprint(features.root) == before,
              "源表全部文件字节哈希不变")
        check(set(os.listdir(features.root)) <= {"data", "metadata",
                                                  "staging"},
              "不生成任何可单独打开的导出目录")
        with open_privacy_view(features, FEATURE_RULES, hmac_key=KEY) as view:
            blob = repr(view.output_columns)
            check("customer_id" not in blob
                  and KEY.decode() not in blob
                  and "CUST-A" not in blob,
                  "诊断输出不含原始敏感字段名、客户编号或 key")
        try:
            open_privacy_view(features, [{"field_id": 999, "action": HIDE}],
                              hmac_key=KEY)
        except PrivacyViewError as e:
            check(KEY.decode() not in str(e) and "CUST" not in str(e),
                  "异常文本不含 key 或客户编号")

        # ------------------------------------------------------------- 6
        heading("6. 无规则读取保持现有 scan 行为")
        legacy = LakeTable.create(os.path.join(workdir, "legacy"))
        g1 = legacy.append([("z9", 90), ("a1", 11), ("m5", 55)])
        legacy.append([("c", 3)])
        legacy.publish_deletes(g1, [1])  # 位置删除
        with open_privacy_view(legacy, [], batch_size=2) as view:
            check(view.read_all().to_pylist() == legacy.scan().to_pylist(),
                  "空规则 + 位置删除过滤与 LakeTable.scan 完全一致")
        with open_privacy_view(features, None, batch_size=2) as view:
            check(view.read_all().to_pylist() == features.scan().to_pylist(),
                  "结构受管表空规则（含改名逻辑映射）与 scan 一致")

        print("\n全部演示场景通过。")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
