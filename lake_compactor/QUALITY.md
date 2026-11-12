# 数据质量画像（Quality Profile）使用说明

为湖表产品增加独立的数据质量画像与差异查询能力。每份报告指向明确的
**数据修订号（manifest 版本）**，发布后只读冻结，任何后续写入、压实、
清理都不能改写报告内容；报告自包含，原始数据文件被回收后仍可打开和比较。

## 1. 规则配置

规则是一个 JSON 对象，四个键全部可选，空对象 `{}` 表示只出指标、不做判定：

| 键 | 类型 | 含义 |
|---|---|---|
| `min_samples` | 非负整数 | 可见样本量下限，`sample_count >= min_samples` 才通过 |
| `max_duplicate_ids` | 非负整数 | 重复主标识（出现 >1 次的不同 id 个数）上限 |
| `value_bounds` | `{"min": int, "max": int}` | 整数数值闭区间 `[min, max]`，要求 `min <= max` |
| `buckets` | `[{"name": str, "lower": int, "upper": int}]` | 整数分段，**左闭右开** `[lower, upper)`；分段名不可重复，分段之间不允许重叠（允许留空隙，落入空隙计入分段外） |

示例（`examples/rules.example.json`，分段故意乱序，规范化后自动排序）：

```json
{
  "min_samples": 8,
  "max_duplicate_ids": 0,
  "value_bounds": {"min": 0, "max": 100},
  "buckets": [
    {"name": "high", "lower": 90, "upper": 101},
    {"name": "low",  "lower": 0,  "upper": 10},
    {"name": "mid",  "lower": 10, "upper": 90}
  ]
}
```

两类失败严格区分：

- **规则格式错误**（类型错、边界反向、分段重叠、未知键等）→ 抛
  `RuleSpecError`，**不生成、不落盘任何报告**；
- **数据触犯合法规则** → 完整画像照常保存，仅在 `verdicts` 中逐条列出
  未通过规则，报告顶层 `passed=false`。

规则规范化后按紧凑 JSON 计算 SHA-256 摘要；键顺序、分段输入顺序都不影响
摘要。报告唯一身份 = `revision + rule_digest`，文件名
`reports/revNNNNNN-<digest12>.json`。

## 2. 报告内容

```json
{
  "report_id": "rev000002-be3cfa556f6a",
  "revision": 2,
  "rules": { ... 规范化后的规则 ... },
  "rule_digest": "be3cfa55...",
  "rule_summary": "min_samples=8,max_duplicate_ids=0,bounds=[0,100],buckets=(...)",
  "metrics": {
    "sample_count": 9,
    "null_fields": 2,
    "null_row_id_count": 1,
    "null_value_count": 1,
    "distinct_id_count": 7,
    "duplicate_id_count": 1,
    "duplicated_row_count": 2,
    "value_count": 8,
    "value_min": -3,
    "value_max": 120,
    "value_sum": 292,
    "bucket_counts": {"low": 3, "mid": 2, "high": 1},
    "out_of_buckets": 2,
    "below_min": 1,
    "above_max": 1
  },
  "passed": false,
  "verdicts": [ {"rule": "...", "passed": false, "detail": "..."}, ... ],
  "frozen_at": 1730000000.0
}
```

指标口径：

- 计数基于**表查询结果**（固定修订号 manifest 经位置删除过滤后的可见行），
  不是各物理分片 `num_rows` 相加；
- `null_fields = null_row_id_count + null_value_count`；空值不进任何分段，
  也不计入分段外与上下界违规；
- `duplicate_id_count` 是重复的不同标识个数；`duplicated_row_count` 是这些
  标识涉及的行数；
- 空表：min/max 为 `null`，sum 为 0，不触犯上下界与重复标识规则。

## 3. Python 接口

```python
from lake import LakeTable
from lake.quality import ProfileStore, compare_reports
from lake.quality.reference import reference_profile, cross_check

table = LakeTable.open("./mylake")
store = ProfileStore(table)

# 创建或取回（重复请求返回已冻结报告，reused=True）
report = store.create_or_get(rules, version=None, batch_size=1000)

# 查看 / 列举
report = store.get(revision=2, digest="be3cfa556f6a")
store.list_reports()

# 比较两份冻结报告（不触达任何数据文件）
diff = compare_reports(store.get(2, digest_a), store.get(4, digest_a))
diff["metric_diffs"]["sample_count"]      # {"old": 9, "new": 6, "delta": -3}
diff["anomalies"]["appeared"]             # 新出现的异常
diff["anomalies"]["disappeared"]          # 消失的异常
diff["anomalies"]["persistent"]           # 持续存在的异常

# 独立参考实现（与生产扫描逻辑分开，小样本手算核对）
ref = reference_profile(table, 2, rules)
cross_check(report, table)                # 返回 [] 表示逐项一致
```

## 4. 命令行入口

```bash
python3 -m lake.quality.cli create  LAKE [--rules r.json] [--revision N] [--json]
python3 -m lake.quality.cli show    LAKE --revision N --digest HASH [--json]
python3 -m lake.quality.cli list    LAKE [--json]
python3 -m lake.quality.cli compare LAKE --old-rev N --new-rev M \
                                    [--rules r.json | --old-digest .. --new-digest ..]
python3 -m lake.quality.cli reference LAKE [--rules r.json] [--revision N]
```

退出码：`0` 成功；`2` 规则配置错误（不落盘）；`3` 报告/修订号不存在。

## 5. 存储与安全语义

```
<root>/quality_profiles/
  LOCK                               跨进程互斥（flock）
  reports/revNNNNNN-<digest12>.json  冻结报告（0444 只读，永不改写）
  .tmp-<random>                      发布前临时文件（崩溃残骸，恢复时清理）
```

- 统计期间持有 `table.lease(version)` 读者租约，GC 不会提前删除输入文件；
- 发布走“临时文件 + fsync + 原子 rename + 目录 fsync”，临时文件与正式
  报告分目录存放，进程在发布前退出不会暴露不完整内容；
- 画像目录独立于 `data/`，垃圾回收完全不触碰；报告保存后即与原始数据
  文件解耦；
- 分批 `RecordBatch` 扫描，内存中任何时刻只有一个批次，绝不把全表收进
  Python 列表；
- 查询目标（修订号）在扫描开始前固定，扫描期间表继续提交不影响本次统计；
- 仅物理整理（压实）而可见数据不变时，两份画像指标逐项一致；行被标记
  删除后，新版本画像同步变化。

测试钩子（仅测试使用）：`LAKE_QUALITY_CRASH_POINT=prepared|computed|staged`
在计算前、发布前、临时文件落盘后 rename 前三个位置硬退出。
