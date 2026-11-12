"""画像规则：规范化、格式校验与确定性摘要。

规则以普通 dict 描述（可从 JSON 读入），支持四类约束：

    {
      "min_samples": 10,                 # 可见样本量下限（含边界）
      "max_duplicate_ids": 0,            # 重复主标识数（出现次数 > 1 的 id 个数）上限
      "value_bounds": {"min": 0, "max": 100},   # 整数数值闭区间 [min, max]
      "buckets": [                       # 整数分段，左闭右开 [lower, upper)
        {"name": "low",  "lower": 0,  "upper": 10},
        {"name": "med",  "lower": 10, "upper": 90},
        {"name": "high", "lower": 90, "upper": 101}
      ]
    }

四个键都可省略；空规则（{}）合法，表示只出指标、不做判定。
规则只描述“形状”，与任何修订号无关；规范化结果按确定顺序序列化后
取 SHA-256 作为规则摘要——同一份规则无论键的输入顺序如何，摘要恒定。

区分两类失败：
  - 规则本身格式错误（类型错、边界反了、分段重叠等）-> RuleSpecError，
    调用方不得据此生成或落盘任何报告；
  - 数据触犯合法规则 -> 报告照常生成并完整保存，仅在 verdicts 中列出未通过项。
"""

from __future__ import annotations

import copy
import hashlib
import json

from ..errors import RuleSpecError

RULE_KEYS = {"min_samples", "max_duplicate_ids", "value_bounds", "buckets"}


def _is_int(x) -> bool:
    # bool 是 int 的子类，但规则里的整数边界不接受布尔值
    return isinstance(x, int) and not isinstance(x, bool)


def normalize_rules(raw) -> dict:
    """校验并规范化规则，返回形状稳定的新 dict。

    规范化内容：只保留已知键；分段按 lower 升序排列；分段名补成稳定字符串。
    任何格式问题抛 RuleSpecError（不产生部分结果）。
    """
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise RuleSpecError(f"规则必须是对象(dict)，实际为 {type(raw).__name__}")
    unknown = set(raw) - RULE_KEYS
    if unknown:
        raise RuleSpecError(f"存在未知规则键: {sorted(unknown)}")

    out: dict = {}

    if "min_samples" in raw:
        v = raw["min_samples"]
        if not _is_int(v) or v < 0:
            raise RuleSpecError("min_samples 必须是非负整数")
        out["min_samples"] = v

    if "max_duplicate_ids" in raw:
        v = raw["max_duplicate_ids"]
        if not _is_int(v) or v < 0:
            raise RuleSpecError("max_duplicate_ids 必须是非负整数")
        out["max_duplicate_ids"] = v

    if "value_bounds" in raw:
        bounds = raw["value_bounds"]
        if bounds is None:
            raise RuleSpecError("value_bounds 不能为 null")
        if not isinstance(bounds, dict):
            raise RuleSpecError("value_bounds 必须是对象 {min, max}")
        extra = set(bounds) - {"min", "max"}
        if extra:
            raise RuleSpecError(f"value_bounds 存在未知键: {sorted(extra)}")
        if "min" not in bounds or "max" not in bounds:
            raise RuleSpecError("value_bounds 必须同时包含 min 和 max")
        lo, hi = bounds["min"], bounds["max"]
        if not (_is_int(lo) and _is_int(hi)):
            raise RuleSpecError("value_bounds 的 min/max 必须是整数")
        if lo > hi:
            raise RuleSpecError(f"value_bounds 下界 {lo} 大于上界 {hi}")
        out["value_bounds"] = {"min": lo, "max": hi}

    if "buckets" in raw:
        buckets_raw = raw["buckets"]
        if not isinstance(buckets_raw, list):
            raise RuleSpecError("buckets 必须是数组")
        norm_buckets = []
        seen_names: set[str] = set()
        for i, b in enumerate(buckets_raw):
            if not isinstance(b, dict):
                raise RuleSpecError(f"buckets[{i}] 必须是对象 {{name, lower, upper}}")
            extra = set(b) - {"name", "lower", "upper"}
            if extra:
                raise RuleSpecError(f"buckets[{i}] 存在未知键: {sorted(extra)}")
            if not all(k in b for k in ("name", "lower", "upper")):
                raise RuleSpecError(
                    f"buckets[{i}] 必须同时包含 name/lower/upper 三个键"
                )
            name = b["name"]
            if not isinstance(name, str) or not name:
                raise RuleSpecError(f"buckets[{i}].name 必须是非空字符串")
            if name in seen_names:
                raise RuleSpecError(f"分段名重复: {name!r}")
            lower, upper = b["lower"], b["upper"]
            if not (_is_int(lower) and _is_int(upper)):
                raise RuleSpecError(f"buckets[{i}] 的 lower/upper 必须是整数")
            if lower >= upper:
                raise RuleSpecError(
                    f"buckets[{i}]({name!r}) 边界非法: "
                    f"[{lower}, {upper}) 要求 lower < upper"
                )
            seen_names.add(name)
            norm_buckets.append({"name": name, "lower": lower, "upper": upper})

        norm_buckets.sort(key=lambda b: (b["lower"], b["upper"], b["name"]))
        for prev, cur in zip(norm_buckets, norm_buckets[1:]):
            if cur["lower"] < prev["upper"]:
                raise RuleSpecError(
                    f"分段重叠: {prev['name']!r}[{prev['lower']},{prev['upper']}) "
                    f"与 {cur['name']!r}[{cur['lower']},{cur['upper']})"
                )
        out["buckets"] = norm_buckets

    return out


def rules_digest(normalized_rules: dict) -> str:
    """对规范化规则计算确定性 SHA-256 摘要（16 进制字符串）。

    用 sort_keys 的紧凑 JSON 作为规范序列化，键输入顺序不影响摘要。
    """
    blob = json.dumps(
        normalized_rules, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def rule_summary(normalized_rules: dict) -> str:
    """人类可读的规则摘要（同时作为报告身份的一部分）。"""
    parts = []
    if "min_samples" in normalized_rules:
        parts.append(f"min_samples={normalized_rules['min_samples']}")
    if "max_duplicate_ids" in normalized_rules:
        parts.append(f"max_duplicate_ids={normalized_rules['max_duplicate_ids']}")
    if "value_bounds" in normalized_rules:
        b = normalized_rules["value_bounds"]
        parts.append(f"bounds=[{b['min']},{b['max']}]")
    if normalized_rules.get("buckets"):
        seg = ";".join(
            f"{b['name']}[{b['lower']},{b['upper']})"
            for b in normalized_rules["buckets"]
        )
        parts.append(f"buckets=({seg})")
    return ",".join(parts) if parts else "no-rules"


def load_rules_from_file(path: str) -> dict:
    """从 JSON 文件读取并规范化规则；文件/JSON 错误统一转成 RuleSpecError。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except RuleSpecError:
        raise
    except (OSError, ValueError) as e:
        raise RuleSpecError(f"无法读取规则文件 {path}: {e}") from e
    return normalize_rules(raw)


def canonical_json(normalized_rules: dict) -> str:
    """规范化规则的规范 JSON 文本（报告内原样保存，便于追溯）。"""
    return json.dumps(
        copy.deepcopy(normalized_rules),
        sort_keys=True,
        indent=2,
        ensure_ascii=False,
    )
