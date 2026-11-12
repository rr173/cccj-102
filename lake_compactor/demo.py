"""端到端演示：碎片 Parquet 湖表压实引擎。

依次演示：
  1. 建表、追加碎片文件（含空文件）、发布位置删除（同文件多条、全删文件）
  2. 固定 manifest 的扫描
  3. 同步屏障交错：计划生成 -> 额外删除 -> 提交（删除翻译）
  4. 追加合并
  5. 两个压实者重叠选文件 -> 冲突
  6. 发布前崩溃留下候选 -> GC 清理
  7. 租约保护旧版本 -> 释放后 GC 清理

运行：python3 demo.py
"""

from __future__ import annotations

import multiprocessing as mp
import os
import shutil
import sys

import pyarrow as pa

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lake import (  # noqa: E402
    ConflictError,
    LakeTable,
    collect,
    commit_plan,
    create_plan,
    verify,
)

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo_lake")
CTX = mp.get_context("fork")


def banner(msg: str) -> None:
    print(f"\n{'=' * 64}\n■ {msg}\n{'=' * 64}")


def ok(msg: str) -> None:
    print(f"  ✔ {msg}")


def rows(prefix, n):
    return [(f"{prefix}{i:04d}", i) for i in range(n)]


# ---- 场景 3 的子进程：压实者 / 删除写者，用屏障精确交错 ---------------------
def compacter_child(barrier, q):
    t = LakeTable.open(ROOT)
    m = t.manifest()
    fids = sorted(m["data_files"])
    plan_id = create_plan(t, selected=fids)          # 1) 计划生成（基版本 V）
    q.put(("plan_base", m["version"]))
    barrier.wait(60)                                  # 放行删除写者
    barrier.wait(60)                                  # 等额外删除发布
    v = commit_plan(t, plan_id)                       # 3) 提交
    q.put(("committed", v))


def deleter_child(fid, barrier, q):
    t = LakeTable.open(ROOT)
    barrier.wait(60)                                  # 等计划生成
    t.publish_deletes(fid, [2])                       # 2) 针对旧文件追加删除
    q.put(("deleted", t.current_version()))
    barrier.wait(60)                                  # 放行提交


# ---- 场景 6 的子进程：生成候选后崩溃 ---------------------------------------
def crashing_child():
    t = LakeTable.open(ROOT)
    create_plan(t, max_rows=10_000)
    os._exit(1)


def main() -> None:
    shutil.rmtree(ROOT, ignore_errors=True)

    banner("1. 建表 + 碎片文件 + 位置删除（空文件 / 全删文件 / 同文件多条删除）")
    t = LakeTable.create(ROOT)
    f1 = t.append(rows("a", 8))
    f2 = t.append(rows("b", 6))
    t.append([])                                       # 空文件
    f_dead = t.append(rows("d", 3))
    t.publish_deletes(f1, [1, 5])                      # 同文件多条删除（两个删除文件）
    t.publish_deletes(f1, [7])
    t.publish_deletes(f_dead, [0, 1, 2])               # 全删文件
    ok(f"4 个数据文件（含 1 空文件），3 个删除文件，当前版本 v{t.current_version()}")
    ok(f"扫描结果: {sorted(t.row_ids())}")

    banner("2. 读者固定一个 manifest 再扫描")
    with t.lease() as lease:
        ids = t.scan(lease.version).column("row_id").to_pylist()
        ok(f"租约固定在 v{lease.version}，读到 {len(ids)} 行: {sorted(ids)}")

    banner("3. 屏障交错：计划生成 -> 额外删除 -> 提交（删除翻译到新位置）")
    before = t.scan()
    target_f2_pos2_id = f"b{2:04d}"
    barrier = CTX.Barrier(2)
    q = CTX.Queue()
    pc = CTX.Process(target=compacter_child, args=(barrier, q))
    pd = CTX.Process(target=deleter_child, args=(f2, barrier, q))
    pc.start(); pd.start(); pc.join(120); pd.join(120)
    assert pc.exitcode == 0 and pd.exitcode == 0
    events = {}
    while not q.empty():
        k, v = q.get()
        events[k] = v
    ok(f"计划基于 v{events['plan_base']}，额外删除发布于 v{events['deleted']}，"
       f"压实提交于 v{events['committed']}")
    m = t.manifest()
    assert len(m["data_files"]) == 1, "全部碎片合并为一个文件"
    (merged_id, _), = m["data_files"].items()
    translated = [
        dm for dm in m["delete_files"].values() if dm["target"] == merged_id
    ]
    ok(f"基版本删除被吸收；新到删除被翻译到合并文件 {merged_id[:8]}…: "
       f"{[d['num_deletes'] for d in translated]} 条")
    expected = before.filter(
        pa.compute.not_equal(before.column("row_id"), pa.scalar(target_f2_pos2_id))
    )
    verify.assert_same_multiset(t.scan(), expected)
    ok(f"row_id 多重集核对通过：{sorted(t.row_ids())}")

    banner("4. 追加合并：计划生成后的并发追加被保留")
    small = sorted(m["data_files"])
    plan_id = create_plan(t, selected=small)
    f_new = t.append(rows("z", 4))                     # 计划之后的无关追加
    commit_plan(t, plan_id)
    m = t.manifest()
    assert f_new in m["data_files"]
    ok(f"追加文件 {f_new[:8]}… 保留，当前数据文件数 = {len(m['data_files'])}")

    banner("5. 两个压实者重叠选文件：后到者冲突失败")
    f_x = t.append(rows("x", 5))
    f_y = t.append(rows("y", 5))
    plan_a = create_plan(t, selected=[f_x, f_y])
    plan_b = create_plan(t, selected=[f_y])            # 与 A 重叠
    commit_plan(t, plan_a)
    try:
        commit_plan(t, plan_b)
        raise SystemExit("应当冲突却成功了")
    except ConflictError as e:
        ok(f"B 按计划冲突失败：{e}")
    ok(f"表内容未被 B 覆盖，行数 = {len(t.row_ids())}")

    banner("6. 发布前崩溃：候选留在 staging，不进读路径，GC 清理")
    before = t.scan()
    p = CTX.Process(target=crashing_child)
    p.start(); p.join(60)
    assert p.exitcode == 1
    staged = os.listdir(t.staging_dir)
    ok(f"崩溃进程退出码 1，staging 残留候选: {staged}")
    verify.assert_same_multiset(t.scan(), before)
    ok("读路径不受未发布候选影响（row_id 多重集一致）")
    report = collect(t, staging_max_age_seconds=0)
    ok(f"GC 清除 staging 残骸: {report['removed_staging']}")

    banner("7. 租约保护旧版本；释放后 GC 才真正删除文件")
    v_old = t.current_version()
    snap = t.scan(v_old)
    old_files = [m_["path"] for m_ in t.manifest(v_old)["data_files"].values()]
    with t.lease(v_old):
        plan_id = create_plan(t, max_rows=10_000)      # 压实所有小文件
        commit_plan(t, plan_id)
        report = collect(t)
        assert report["removed_files"] == []
        ok(f"持租约期间 GC 未删任何文件（保护版本: {report['protected_versions']}）")
        verify.assert_same_multiset(t.scan(v_old), snap)
        ok(f"旧版本 v{v_old} 仍可扫描")
    report = collect(t)
    ok(f"租约释放后 GC 回收 {len(report['removed_files'])} 个文件: "
       f"{report['removed_files']}")
    assert all(not os.path.exists(os.path.join(ROOT, p_)) for p_ in old_files)
    verify.assert_same_multiset(t.scan(), snap)
    ok("当前版本内容不变，旧文件已清理")

    banner("全部演示通过")


if __name__ == "__main__":
    main()
