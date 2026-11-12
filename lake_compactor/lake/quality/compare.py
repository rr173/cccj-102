"""两份已冻结画像报告的差异比较。

输入是两个独立于原始数据的冻结报告文件（JSON），因此即使底层数据
已被整理或回收，比较仍然可用。输出包含：

  - 两份报告的身份（修订号、规则摘要）；
  - 全部标量指标的差值（新 - 旧）与分段计数差值（按分段名对齐）；
  - 异常（未通过规则）的迁移：新出现 / 消失 / 持续存在。

异常身份按规则名（rule）匹配；同一规则在两边都失败即“持续”，
只在新报告失败即“新出现”，只在旧报告失败即“消失”。
"""

from __future__ import annotations

# 参与差值计算的标量指标（value_min/max 也给出差值，空表为 None 时跳过）
_SCALAR_METRICS = [
    "sample_count",
    "null_fields",
    "null_row_id_count",
    "null_value_count",
    "distinct_id_count",
    "duplicate_id_count",
    "duplicated_row_count",
    "value_count",
    "value_min",
    "value_max",
    "value_sum",
    "out_of_buckets",
    "below_min",
    "above_max",
]


def _failed_map(report: dict) -> dict[str, dict]:
    return {v["rule"]: v for v in report.get("verdicts", []) if not v["passed"]}


def compare_reports(old: dict, new: dict) -> dict:
    """比较两份报告，返回结构化差异。"""
    old_m, new_m = old["metrics"], new["metrics"]

    metric_diffs = {}
    for key in _SCALAR_METRICS:
        a, b = old_m.get(key), new_m.get(key)
        if a is None or b is None:
            metric_diffs[key] = {"old": a, "new": b, "delta": None}
        else:
            metric_diffs[key] = {"old": a, "new": b, "delta": b - a}

    # 分段计数按分段名对齐（两份报告的规则通常一致；规则不同也能对齐）
    bucket_names = sorted(
        set(old_m.get("bucket_counts", {})) | set(new_m.get("bucket_counts", {}))
    )
    bucket_diffs = {}
    for name in bucket_names:
        a = old_m.get("bucket_counts", {}).get(name, 0)
        b = new_m.get("bucket_counts", {}).get(name, 0)
        bucket_diffs[name] = {"old": a, "new": b, "delta": b - a}

    old_fail, new_fail = _failed_map(old), _failed_map(new)
    appeared = sorted(set(new_fail) - set(old_fail))
    disappeared = sorted(set(old_fail) - set(new_fail))
    persistent = sorted(set(old_fail) & set(new_fail))

    return {
        "old": {
            "report_id": old["report_id"],
            "revision": old["revision"],
            "rule_digest": old["rule_digest"],
            "rule_summary": old["rule_summary"],
            "passed": old["passed"],
        },
        "new": {
            "report_id": new["report_id"],
            "revision": new["revision"],
            "rule_digest": new["rule_digest"],
            "rule_summary": new["rule_summary"],
            "passed": new["passed"],
        },
        "same_rules": old["rule_digest"] == new["rule_digest"],
        "metric_diffs": metric_diffs,
        "bucket_diffs": bucket_diffs,
        "anomalies": {
            "appeared": [new_fail[r] for r in appeared],
            "disappeared": [old_fail[r] for r in disappeared],
            "persistent": [
                {"rule": r, "old": old_fail[r], "new": new_fail[r]}
                for r in persistent
            ],
        },
    }


def format_comparison(diff: dict) -> str:
    """把比较结果渲染成可读文本（CLI 与演示共用）。"""
    o, n = diff["old"], diff["new"]
    lines = [
        f"比较报告 {o['report_id']} (rev{o['revision']}) -> "
        f"{n['report_id']} (rev{n['revision']})",
        f"规则一致: {'是' if diff['same_rules'] else '否'} "
        f"({o['rule_summary']} vs {n['rule_summary']})",
        "",
        "指标差值（新 - 旧）:",
    ]
    for key, d in diff["metric_diffs"].items():
        if d["delta"] is None:
            lines.append(f"  {key:>22}: {d['old']} -> {d['new']}（无可比差值）")
        else:
            lines.append(f"  {key:>22}: {d['old']} -> {d['new']}  Δ={d['delta']:+d}")

    lines.append("")
    lines.append("分段计数差值:")
    for name, d in diff["bucket_diffs"].items():
        lines.append(
            f"  {name:>22}: {d['old']} -> {d['new']}  Δ={d['delta']:+d}"
        )

    a = diff["anomalies"]
    lines.append("")
    lines.append(f"异常新出现 ({len(a['appeared'])}):")
    lines += [f"  - {v['rule']}: {v['detail']}" for v in a["appeared"]] or ["  （无）"]
    lines.append(f"异常消失 ({len(a['disappeared'])}):")
    lines += [f"  - {v['rule']}: {v['detail']}" for v in a["disappeared"]] or ["  （无）"]
    lines.append(f"异常持续 ({len(a['persistent'])}):")
    lines += [f"  - {v['rule']}" for v in a["persistent"]] or ["  （无）"]
    return "\n".join(lines)


def format_report(report: dict) -> str:
    """渲染单份报告的可读文本。"""
    m = report["metrics"]
    title = report.get(
        "report_id",
        f"rev{report['revision']:06d}-{report['rule_digest'][:12]}（参考计算）",
    )
    lines = [
        f"报告 {title}",
        f"数据修订号: {report['revision']}",
        f"规则摘要: {report['rule_summary']}",
        f"规则摘要哈希: {report['rule_digest']}",
        f"总体判定: {'通过' if report['passed'] else '未通过'}",
        "",
        "指标:",
        f"  样本数            : {m['sample_count']}",
        f"  空字段数          : {m['null_fields']} "
        f"(row_id 空 {m['null_row_id_count']}, value 空 {m['null_value_count']})",
        f"  不同主标识数      : {m['distinct_id_count']}",
        f"  重复主标识数      : {m['duplicate_id_count']} "
        f"(涉及 {m['duplicated_row_count']} 行)",
        f"  非空数值数        : {m['value_count']}",
        f"  数值最小值        : {m['value_min']}",
        f"  数值最大值        : {m['value_max']}",
        f"  数值合计          : {m['value_sum']}",
        "  分段计数（左闭右开）:",
    ]
    for name, c in m["bucket_counts"].items():
        lines.append(f"    {name:<16}: {c}")
    lines.append(f"  分段外计数        : {m['out_of_buckets']}")
    if m["below_min"] is not None:
        lines.append(
            f"  区间外计数        : 低于下界 {m['below_min']}，"
            f"高于上界 {m['above_max']}"
        )
    lines.append("")
    lines.append("逐条规则判定:")
    if not report["verdicts"]:
        lines.append("  （未配置任何约束规则）")
    for v in report["verdicts"]:
        lines.append(
            f"  [{'PASS' if v['passed'] else 'FAIL'}] {v['rule']}: {v['detail']}"
        )
    return "\n".join(lines)
