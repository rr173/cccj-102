"""数据质量画像端到端叙述式演示（本地样本，不连接任何外部系统）。

覆盖验收主线：
  1. 可手算小样本建表（两个物理分片，含 null / 重复 id / 越界值 / 分段外值）
  2. 创建画像：逐项对照手算结果，并用独立参考实现交叉核对
  3. 仅物理整理（压实）：两版画像逐项一致，比较差值全 0
  4. 删行后重新统计：指标同步变化，异常新出现 / 消失
  5. 数据触犯规则：完整画像保留，失败规则逐条列出
  6. 错误配置：直接拒绝，没有任何报告落盘
  7. 重复请求复用原报告；另一组规则并存、不覆盖
  8. 读者租约保护：持旧版租约触发 GC 输入仍可用，释放后允许清理
  9. 发布边界用真正退出的子进程制造中断，再请求恢复
 10. 原始数据被 GC 后，冻结报告仍可打开与比较

运行：python3 demo_quality.py
"""

from __future__ import annotations

import multiprocessing as mp
import os
import shutil
import sys
import tempfile
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lake import LakeTable, collect, commit_plan, create_plan  # noqa: E402
from lake.errors import RuleSpecError  # noqa: E402
from lake.quality import (  # noqa: E402
    ProfileStore,
    compare_reports,
    cross_check,
    format_comparison,
    format_report,
)

CTX = mp.get_context("fork")

# 9 行可手算样本
ROWS = [
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


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def write_shard(table, rows):
    """写一个可含 null 的数据文件（LakeTable.append 不接受 None）。"""
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
        new["data_files"][fid] = {"path": rel, "num_rows": arrow.num_rows}
        table._publish(new)
    return fid


def rows_of_file(table, version, fid):
    meta = table.manifest(version)["data_files"][fid]
    tab = pq.read_table(os.path.join(table.root, meta["path"]))
    return list(zip(tab.column("row_id").to_pylist(),
                    tab.column("value").to_pylist()))


def crash_worker(root, point, q):
    try:
        os.environ["LAKE_QUALITY_CRASH_POINT"] = point
        ProfileStore(LakeTable.open(root)).create_or_get(RULES)
        q.put(("unexpected-ok", None))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def main() -> None:
    workdir = tempfile.mkdtemp(prefix="quality-demo-")
    root = os.path.join(workdir, "lake")
    print(f"临时工作目录: {workdir}")

    # ---------------------------------------------------------- 1. 建表
    banner("1. 建表：9 行样本拆成两个物理分片（统计基于查询结果，非分片相加）")
    t = LakeTable.create(root)
    write_shard(t, ROWS[:5])
    write_shard(t, ROWS[5:])
    v1 = t.current_version()
    print(f"修订号 v{v1}，物理分片 {len(t.manifest(v1)['data_files'])} 个，"
          f"分片 num_rows 之和 = "
          f"{sum(m['num_rows'] for m in t.manifest(v1)['data_files'].values())}")

    store = ProfileStore(t)

    # ------------------------------------------------------- 2. 画像+手算
    banner("2. 创建画像并逐项对照手算（batch_size=3 强制分批，全表不入列表）")
    r1 = store.create_or_get(RULES, version=v1, batch_size=3)
    print(format_report(r1))
    m = r1["metrics"]
    print("\n手算核对:")
    print("  样本 9；空字段 2（1 空 id + 1 空 value）；不同 id 7；重复 id 1（b）")
    print("  非空数值 8 个；min=-3 max=120 sum=292")
    print("  low[0,10)={0,9,7}=3  mid[10,90)={10,50}=2  "
          "high[90,101)={99}=1  分段外=2")
    assert (m["sample_count"], m["null_fields"], m["duplicate_id_count"],
            m["value_min"], m["value_max"], m["value_sum"],
            m["out_of_buckets"]) == (9, 2, 1, -3, 120, 292, 2)
    problems = cross_check(r1, t)
    print(f"\n独立参考实现交叉核对: {'一致' if not problems else problems}")
    assert not problems

    # ------------------------------------------------- 3. 物理整理一致性
    banner("3. 仅物理整理（压实）：可见数据不变 -> 两份画像逐项一致")
    plan = create_plan(t, selected=list(t.manifest(v1)["data_files"]))
    v_compacted = commit_plan(t, plan)
    r_comp = store.create_or_get(RULES, version=v_compacted)
    print(f"v{v1} -> v{v_compacted}，物理分片数 "
          f"{len(t.manifest(v1)['data_files'])} -> "
          f"{len(t.manifest(v_compacted)['data_files'])}")
    print(f"指标完全一致: {r1['metrics'] == r_comp['metrics']}")
    assert r1["metrics"] == r_comp["metrics"]
    diff_zero = compare_reports(r1, r_comp)
    assert all(d["delta"] == 0 for d in diff_zero["metric_diffs"].values())
    assert all(d["delta"] == 0 for d in diff_zero["bucket_diffs"].values())
    print("比较结果：全部指标差值 Δ=0，无异常迁移")

    # ------------------------------------------------------- 4. 删行统计
    banner("4. 删行后重新统计：指标同步变化，异常消失 / 新出现")
    t2 = LakeTable.create(os.path.join(workdir, "lake2"))
    write_shard(t2, ROWS[:5])
    write_shard(t2, ROWS[5:])
    w1 = t2.current_version()
    s2 = ProfileStore(t2)
    before = s2.create_or_get(RULES, version=w1)
    per_file = {fid: rows_of_file(t2, w1, fid)
                for fid in t2.manifest(w1)["data_files"]}
    f_b = next(f for f, rows in per_file.items() if ("b", 9) in rows)
    f_out = next(f for f, rows in per_file.items() if ("f", 120) in rows)
    t2.publish_deletes(f_b, [1])            # 去掉重复 id
    t2.publish_deletes(f_out, [2, 3])       # 去掉两个越界值
    w2 = t2.current_version()
    after = s2.create_or_get(RULES, version=w2)
    print(format_comparison(compare_reports(before, after)))

    # --------------------------------------------- 5. 违规保留完整画像
    banner("5. 数据触犯规则：画像完整保存，失败规则逐条列出")
    print(f"报告总体 passed={r1['passed']}，但全部指标均已落盘；"
          f"未通过规则: "
          f"{[v['rule'] for v in r1['verdicts'] if not v['passed']]}")
    path = store.report_path(r1["revision"], r1["rule_digest"])
    print(f"报告文件: {path}（权限 {oct(os.stat(path).st_mode & 0o777)}，只读冻结）")

    # ------------------------------------------------- 6. 错误配置拒绝
    banner("6. 错误配置被拒，且没有任何报告落盘")
    for bad in (
        {"min_samples": -1},
        {"value_bounds": {"min": 10, "max": 0}},
        {"buckets": [{"name": "x", "lower": 5, "upper": 5}]},
        {"buckets": [
            {"name": "a", "lower": 0, "upper": 10},
            {"name": "b", "lower": 5, "upper": 20},
        ]},
    ):
        try:
            store.create_or_get(bad)
        except RuleSpecError as e:
            print(f"  拒绝 {bad} -> {e}")
    print(f"已冻结报告数仍为: {len(store.list_reports())}（错误配置不落盘）")

    # ------------------------------------------- 7. 复用与多规则并存
    banner("7. 同配置重复请求读原报告；另一组规则各存一份、互不覆盖")
    again = store.create_or_get(dict(RULES), version=v1)
    print(f"重复请求 reused={again['reused']}，report_id={again['report_id']}")
    other = store.create_or_get({"min_samples": 99}, version=v1)
    third = store.create_or_get({"min_samples": 1}, version=v1)
    print(f"不同规则报告: {other['report_id']} / {third['report_id']}")
    print(f"当前冻结报告总数: {len(store.list_reports())}（原报告未被覆盖）")

    # ------------------------------------------------- 8. 租约与 GC
    banner("8. 读者租约：持旧版租约时 GC 不能删输入，释放后可以")
    t3 = LakeTable.create(os.path.join(workdir, "lake3"))
    f1 = write_shard(t3, ROWS[:5])
    f2 = write_shard(t3, ROWS[5:])
    base_v = t3.current_version()
    old_paths = [m["path"] for m in t3.manifest(base_v)["data_files"].values()]
    with t3.lease(base_v):
        commit_plan(t3, create_plan(t3, selected=[f1, f2]))
        gc = collect(t3)
        print(f"租约内 GC: protected_versions={gc['protected_versions']}, "
              f"removed_files={gc['removed_files']}")
        assert gc["removed_files"] == []
    gc = collect(t3)
    print(f"释放后 GC: 删除 {len(gc['removed_files'])} 个旧文件")
    assert set(gc["removed_files"]) == set(old_paths)

    # ------------------------------------------- 9. 发布边界崩溃恢复
    banner("9. 真正退出的子进程在发布边界中断，再请求恢复")
    t4 = LakeTable.create(os.path.join(workdir, "lake4"))
    write_shard(t4, ROWS[:5])
    write_shard(t4, ROWS[5:])
    s4 = ProfileStore(t4)
    for point in ("prepared", "computed", "staged"):
        q = CTX.Queue()
        p = CTX.Process(target=crash_worker,
                        args=(t4.root, point, q))
        p.start()
        p.join(60)
        print(f"  崩溃点 {point:>8}: 子进程 exitcode={p.exitcode}，"
              f"正式报告数={len(s4.list_reports())}")
        assert p.exitcode == 23 and s4.list_reports() == []
    recovered = s4.create_or_get(RULES)
    print(f"  相同请求恢复成功: reused={recovered['reused']}，"
          f"样本数={recovered['metrics']['sample_count']}，"
          f"临时残骸已清理: "
          f"{not [n for n in os.listdir(s4.profiles_root) if n.startswith('.tmp-')]}")

    # ------------------------------------------- 10. GC 后报告仍可用
    banner("10. 原始数据被回收后，冻结报告仍可打开与比较（报告自包含）")
    t5 = LakeTable.create(os.path.join(workdir, "lake5"))
    write_shard(t5, ROWS[:5])
    write_shard(t5, ROWS[5:])
    z1 = t5.current_version()
    s5 = ProfileStore(t5)
    rep1 = s5.create_or_get(RULES, version=z1)
    pf = next(iter(t5.manifest(z1)["data_files"]))
    t5.publish_deletes(pf, [0])
    z2 = t5.current_version()
    rep2 = s5.create_or_get(RULES, version=z2)
    commit_plan(t5, create_plan(t5, selected=list(t5.manifest(z2)["data_files"])))
    removed = collect(t5)["removed_files"]
    print(f"GC 回收 {len(removed)} 个原始数据/删除文件")
    reopened1 = s5.get(rep1["revision"], rep1["rule_digest"])
    reopened2 = s5.get(rep2["revision"], rep2["rule_digest"])
    d = compare_reports(reopened1, reopened2)
    print(f"报告仍可打开；比较样本数差值: "
          f"{d['metric_diffs']['sample_count']['delta']}")
    print("（画像目录独立于 data/，GC 完全不触碰）")

    shutil.rmtree(workdir, ignore_errors=True)
    print("\n全部演示完成。")


if __name__ == "__main__":
    main()
