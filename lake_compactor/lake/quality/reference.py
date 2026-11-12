"""参考计算程序：与生产流式画像完全分开的独立实现。

用途：给小样本提供一份可以逐行手算核对的参照系。它不 import
lake.quality.profile 里的任何扫描/聚合代码，只复用：
  - rules.py 的规则规范化（保证两边对同一份规则的理解一致）；
  - LakeTable 的 manifest/删除文件读取原语（自己实现行过滤）。

与生产实现的差异（刻意分开，避免同源 bug 互相背书）：

  - 生产实现 profile.py：RecordBatch 分批 + pyarrow.compute 向量化聚合，
    保证全表不进 Python 列表；
  - 参考实现：自己逐文件读取、自己按物理行号做删除过滤、用普通
    Python 循环和 dict/set 逐行累加，逻辑直白、便于手算对照。

参考程序面向“可手算的小样本”，因此允许把小表物化成普通列表；
它不是生产扫描路径，质量画像的生成永远走 profile.py。
"""

from __future__ import annotations

import pyarrow.parquet as pq

from .rules import normalize_rules, rule_summary, rules_digest


def reference_visible_rows(table, version: int):
    """独立实现的可见行读取：逐文件、逐物理行号过滤删除位置。

    返回 list[(row_id, value)]。仅用于小样本参考计算。
    """
    m = table.manifest(version)
    deletes = table._all_deleted_positions(m)
    rows = []
    for fid in sorted(m["data_files"]):
        meta = m["data_files"][fid]
        dead = deletes.get(fid, set())
        arrow_table = pq.read_table(table.root + "/" + meta["path"])
        ids = arrow_table.column("row_id").to_pylist()
        vals = arrow_table.column("value").to_pylist()
        for pos, (rid, val) in enumerate(zip(ids, vals)):
            if pos not in dead:  # 0 起物理行号，与生产过滤同一语义、独立实现
                rows.append((rid, val))
    return rows


def reference_profile(table, version: int, raw_rules) -> dict:
    """用独立的纯 Python 路径计算指标与判定，返回与生产报告同形的内容。"""
    rules = normalize_rules(raw_rules)
    rows = reference_visible_rows(table, version)

    sample_count = len(rows)
    null_row_id = sum(1 for rid, _ in rows if rid is None)
    null_value = sum(1 for _, val in rows if val is None)

    id_counts: dict[str, int] = {}
    for rid, _ in rows:
        if rid is not None:
            id_counts[rid] = id_counts.get(rid, 0) + 1
    duplicate_id_count = sum(1 for c in id_counts.values() if c > 1)
    duplicated_row_count = sum(c for c in id_counts.values() if c > 1)

    non_null_values = [val for _, val in rows if val is not None]
    value_count = len(non_null_values)
    value_sum = sum(non_null_values)
    value_min = min(non_null_values) if non_null_values else None
    value_max = max(non_null_values) if non_null_values else None

    bucket_counts = {b["name"]: 0 for b in rules.get("buckets", [])}
    out_of_buckets = 0
    for val in non_null_values:
        hit = False
        for b in rules.get("buckets", []):  # 左闭右开 [lower, upper)
            if b["lower"] <= val < b["upper"]:
                bucket_counts[b["name"]] += 1
                hit = True
                break
        if not hit:
            out_of_buckets += 1

    bounds = rules.get("value_bounds")
    if bounds:
        below_min = sum(1 for v in non_null_values if v < bounds["min"])
        above_max = sum(1 for v in non_null_values if v > bounds["max"])
    else:
        below_min = above_max = None

    metrics = {
        "sample_count": sample_count,
        "null_fields": null_row_id + null_value,
        "null_row_id_count": null_row_id,
        "null_value_count": null_value,
        "distinct_id_count": len(id_counts),
        "duplicate_id_count": duplicate_id_count,
        "duplicated_row_count": duplicated_row_count,
        "value_count": value_count,
        "value_min": value_min,
        "value_max": value_max,
        "value_sum": value_sum,
        "bucket_counts": bucket_counts,
        "out_of_buckets": out_of_buckets,
        "below_min": below_min,
        "above_max": above_max,
    }

    # 判定逻辑也独立写一遍，只产出与生产相同形状的结构
    verdicts = []
    if "min_samples" in rules:
        exp = rules["min_samples"]
        verdicts.append(
            {
                "rule": "min_samples",
                "passed": metrics["sample_count"] >= exp,
                "expected": f"sample_count >= {exp}",
                "actual": metrics["sample_count"],
                "detail": f"样本数 {metrics['sample_count']}，要求至少 {exp}",
            }
        )
    if "max_duplicate_ids" in rules:
        exp = rules["max_duplicate_ids"]
        verdicts.append(
            {
                "rule": "max_duplicate_ids",
                "passed": metrics["duplicate_id_count"] <= exp,
                "expected": f"duplicate_id_count <= {exp}",
                "actual": metrics["duplicate_id_count"],
                "detail": f"重复主标识 {metrics['duplicate_id_count']} 个，允许至多 {exp} 个",
            }
        )
    if bounds:
        verdicts.append(
            {
                "rule": "value_bounds",
                "passed": (below_min or 0) == 0 and (above_max or 0) == 0,
                "expected": f"all values in [{bounds['min']}, {bounds['max']}]",
                "actual": {"below_min": below_min, "above_max": above_max},
                "detail": (
                    f"区间 [{bounds['min']}, {bounds['max']}] 外："
                    f"低于下界 {below_min} 条，高于上界 {above_max} 条"
                ),
            }
        )

    return {
        "revision": version,
        "rules": rules,
        "rule_digest": rules_digest(rules),
        "rule_summary": rule_summary(rules),
        "metrics": metrics,
        "passed": all(v["passed"] for v in verdicts),
        "verdicts": verdicts,
        "scan": {"generator": "lake.quality.reference"},
    }


def cross_check(production_report: dict, table) -> list[str]:
    """用参考实现复核一份已生成的生产报告，返回不一致项说明（空列表=一致）。"""
    ref = reference_profile(table, production_report["revision"], production_report["rules"])
    problems = []
    if ref["metrics"] != production_report["metrics"]:
        for key in sorted(set(ref["metrics"]) | set(production_report["metrics"])):
            if ref["metrics"].get(key) != production_report["metrics"].get(key):
                problems.append(
                    f"指标 {key} 不一致: 生产={production_report['metrics'].get(key)!r} "
                    f"参考={ref['metrics'].get(key)!r}"
                )
    prod_pass = {v["rule"]: v["passed"] for v in production_report["verdicts"]}
    ref_pass = {v["rule"]: v["passed"] for v in ref["verdicts"]}
    if prod_pass != ref_pass:
        problems.append(f"规则判定不一致: 生产={prod_pass} 参考={ref_pass}")
    if ref["rule_digest"] != production_report["rule_digest"]:
        problems.append("规则摘要不一致")
    return problems
