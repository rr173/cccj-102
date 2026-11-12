"""流式数据质量画像：基于表查询结果分批扫描，聚合全部指标。

关键约束：

  - 计数基于表的“查询结果”——即固定修订号 manifest 经位置删除过滤后的
    可见行，而不是把各物理分片 manifest 里的 num_rows 直接相加；
  - 分批（RecordBatch）扫描，任何时刻内存里只有一个批次，绝不把全表
    收集进 Python 列表；主标识去重计数用 dict 增量聚合（体量与不同
    标识数成正比，而非与行数成正比）；
  - 查询目标（修订号）在扫描开始前固定，扫描期间表继续提交新版本也
    不影响本次统计——读者始终读同一个不可变 manifest；
  - 数值分段左闭右开 [lower, upper)；分段外计数只统计非空数值，
    空值（null）不计入任何分段，也不计入分段外。

数据触犯规则不影响画像生成：指标始终完整计算，未通过的规则在
verdicts 中逐条列出。规则格式错误在 rules.normalize_rules 阶段就被
拒绝，不会进入本模块。
"""

from __future__ import annotations

import bisect
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .rules import normalize_rules, rule_summary, rules_digest

SCHEMA_VERSION = 1
DEFAULT_BATCH_SIZE = 1000


class ProfileAccumulator:
    """逐批吸收可见行的聚合器（有界内存：单批次 + 主标识字典）。"""

    def __init__(self, rules: dict, batch_size: int = DEFAULT_BATCH_SIZE):
        self.rules = rules
        self.batch_size = batch_size
        self.sample_count = 0
        self.null_row_id_count = 0
        self.null_value_count = 0
        # row_id -> 出现次数；null 主标识不进字典，单独计数
        self._id_counts: dict[str, int] = {}
        self.value_count = 0
        self.value_sum: int = 0
        self.value_min: int | None = None
        self.value_max: int | None = None
        buckets = rules.get("buckets", [])
        self._bucket_names = [b["name"] for b in buckets]
        # 边界按 lower 升序（规范化时已排序），供二分定位
        self._bucket_lowers = [b["lower"] for b in buckets]
        self._bucket_uppers = [b["upper"] for b in buckets]
        self.bucket_counts = {name: 0 for name in self._bucket_names}
        self.out_of_buckets = 0
        bounds = rules.get("value_bounds")
        self._bounds = bounds
        self.below_min = 0 if bounds else None
        self.above_max = 0 if bounds else None

    def update(self, batch: pa.RecordBatch) -> None:
        """吸收一个已按位置删除过滤的批次。"""
        n = batch.num_rows
        if n == 0:
            return
        self.sample_count += n

        id_col = batch.column("row_id")
        val_col = batch.column("value")

        null_id = pc.is_null(id_col)
        null_val = pc.is_null(val_col)
        self.null_row_id_count += pc.sum(null_id).as_py() or 0
        self.null_value_count += pc.sum(null_val).as_py() or 0

        # 主标识频次：每批转一次 Python 列表（长度 <= batch_size），增量进字典
        for rid in id_col.to_pylist():
            if rid is not None:
                self._id_counts[rid] = self._id_counts.get(rid, 0) + 1

        # 数值极值/合计：交给 Arrow 聚合，null 自动忽略
        self.value_count += pc.count(val_col).as_py() or 0
        s = pc.sum(val_col).as_py()
        if s is not None:
            self.value_sum += int(s)
        mn = pc.min(val_col).as_py()
        if mn is not None:
            self.value_min = mn if self.value_min is None else min(self.value_min, mn)
        mx = pc.max(val_col).as_py()
        if mx is not None:
            self.value_max = mx if self.value_max is None else max(self.value_max, mx)

        # 分段与区间外：只看非空数值，每批列表长度 <= batch_size
        lo_bound = self._bounds["min"] if self._bounds else None
        hi_bound = self._bounds["max"] if self._bounds else None
        for v in val_col.drop_null().to_pylist():
            idx = bisect.bisect_right(self._bucket_lowers, v) - 1
            if idx >= 0 and v < self._bucket_uppers[idx]:
                self.bucket_counts[self._bucket_names[idx]] += 1
            else:
                self.out_of_buckets += 1
            if self._bounds is not None:
                if v < lo_bound:
                    self.below_min += 1
                elif v > hi_bound:
                    self.above_max += 1

    # ------------------------------------------------------------- 结果
    def metrics(self) -> dict:
        duplicate_id_count = sum(1 for c in self._id_counts.values() if c > 1)
        duplicated_row_count = sum(c for c in self._id_counts.values() if c > 1)
        return {
            "sample_count": self.sample_count,
            "null_fields": self.null_row_id_count + self.null_value_count,
            "null_row_id_count": self.null_row_id_count,
            "null_value_count": self.null_value_count,
            "distinct_id_count": len(self._id_counts),
            "duplicate_id_count": duplicate_id_count,
            "duplicated_row_count": duplicated_row_count,
            "value_count": self.value_count,
            "value_min": self.value_min,
            "value_max": self.value_max,
            "value_sum": self.value_sum,
            "bucket_counts": dict(self.bucket_counts),
            "out_of_buckets": self.out_of_buckets,
            "below_min": self.below_min,
            "above_max": self.above_max,
        }


def iter_visible_batches(table, version: int, batch_size: int = DEFAULT_BATCH_SIZE):
    """对指定修订号的可见记录分批产出 RecordBatch。

    与 LakeTable.scan 的查询语义一致（固定 manifest → 归组位置删除 →
    按物理行号过滤），但逐文件、逐批产出，调用方永远拿不到全表列表。
    删除位置过滤在每个批次内按“文件内物理行号”完成。
    """
    m = table.manifest(version)
    deletes = table._all_deleted_positions(m)
    for fid in sorted(m["data_files"]):
        meta = m["data_files"][fid]
        path = os.path.join(table.root, meta["path"])
        dead = deletes.get(fid)
        pf = pq.ParquetFile(path)
        row_offset = 0
        for batch in pf.iter_batches(batch_size=batch_size):
            orig_len = batch.num_rows
            if dead:
                keep = [
                    i
                    for i in range(orig_len)
                    if (row_offset + i) not in dead
                ]
                if len(keep) != orig_len:
                    batch = batch.take(pa.array(keep, pa.int64()))
            yield batch
            row_offset += orig_len


def evaluate_rules(rules: dict, metrics: dict) -> list[dict]:
    """对聚合后的指标逐条判定已配置规则，返回稳定顺序的判定列表。

    分段（buckets）只是计数分区，本身不产生通过/失败；样本量、重复
    主标识数、数值上下界各自产生一条判定。
    """
    verdicts: list[dict] = []

    if "min_samples" in rules:
        expected = rules["min_samples"]
        actual = metrics["sample_count"]
        verdicts.append(
            {
                "rule": "min_samples",
                "passed": actual >= expected,
                "expected": f"sample_count >= {expected}",
                "actual": actual,
                "detail": f"样本数 {actual}，要求至少 {expected}",
            }
        )

    if "max_duplicate_ids" in rules:
        expected = rules["max_duplicate_ids"]
        actual = metrics["duplicate_id_count"]
        verdicts.append(
            {
                "rule": "max_duplicate_ids",
                "passed": actual <= expected,
                "expected": f"duplicate_id_count <= {expected}",
                "actual": actual,
                "detail": f"重复主标识 {actual} 个，允许至多 {expected} 个",
            }
        )

    if "value_bounds" in rules:
        b = rules["value_bounds"]
        below = metrics["below_min"] or 0
        above = metrics["above_max"] or 0
        verdicts.append(
            {
                "rule": "value_bounds",
                "passed": below == 0 and above == 0,
                "expected": f"all values in [{b['min']}, {b['max']}]",
                "actual": {"below_min": below, "above_max": above},
                "detail": (
                    f"区间 [{b['min']}, {b['max']}] 外："
                    f"低于下界 {below} 条，高于上界 {above} 条"
                ),
            }
        )

    return verdicts


def compute_profile(
    table,
    version: int,
    raw_rules,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict:
    """计算指定修订号下的完整画像内容（尚未落盘）。

    规则先规范化：格式错误抛 RuleSpecError，不产生报告。统计期间由
    调用方（store）持有读者租约，保证输入文件不被 GC 提前回收。
    """
    rules = normalize_rules(raw_rules)
    digest = rules_digest(rules)
    acc = ProfileAccumulator(rules, batch_size=batch_size)
    m = table.manifest(version)
    scanned_files = sorted(m["data_files"].keys())
    for batch in iter_visible_batches(table, version, batch_size=batch_size):
        acc.update(batch)

    metrics = acc.metrics()
    verdicts = evaluate_rules(rules, metrics)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_id": f"rev{version:06d}-{digest[:12]}",
        "revision": version,
        "created_at": time.time(),
        "rules": rules,
        "rule_digest": digest,
        "rule_summary": rule_summary(rules),
        "metrics": metrics,
        "passed": all(v["passed"] for v in verdicts),
        "verdicts": verdicts,
        "scan": {
            "batch_size": batch_size,
            "data_files": scanned_files,
            "generator": "lake.quality.profile",
        },
    }
