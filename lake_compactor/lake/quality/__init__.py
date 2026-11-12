"""数据质量画像子包：规则、流式画像、冻结存储、报告比较与参考实现。

模块划分：
  rules.py     画像规则的规范化、格式校验与确定性摘要
  profile.py   基于表查询结果的分批流式扫描与指标聚合（不把全表装入列表）
  store.py     画像报告的冻结存储（修订号 + 规则摘要唯一身份、原子发布）
  compare.py   两份已冻结报告的差异比较
  reference.py 与生产扫描逻辑分开的纯 Python 参考计算程序
  cli.py       命令行入口
"""

from .compare import compare_reports, format_comparison, format_report
from .profile import (
    ProfileAccumulator,
    compute_profile,
    evaluate_rules,
    iter_visible_batches,
)
from .reference import cross_check, reference_profile, reference_visible_rows
from .rules import (
    canonical_json,
    load_rules_from_file,
    normalize_rules,
    rule_summary,
    rules_digest,
)
from .store import ProfileStore

__all__ = [
    "ProfileAccumulator",
    "ProfileStore",
    "compute_profile",
    "iter_visible_batches",
    "evaluate_rules",
    "compare_reports",
    "format_comparison",
    "format_report",
    "reference_profile",
    "reference_visible_rows",
    "cross_check",
    "normalize_rules",
    "rules_digest",
    "rule_summary",
    "canonical_json",
    "load_rules_from_file",
]
