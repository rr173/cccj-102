"""端到端演示：碎片 Parquet 湖表压实引擎 + 可重放微批摄取。

依次演示：
  1. 建表、追加碎片文件（含空文件）、发布位置删除（同文件多条、全删文件）
  2. 固定 manifest 的扫描
  3. 同步屏障交错：计划生成 -> 额外删除 -> 提交（删除翻译）
  4. 追加合并
  5. 两个压实者重叠选文件 -> 冲突
  6. 发布前崩溃留下候选 -> GC 清理
  7. 租约保护旧版本 -> 释放后 GC 清理
  8. 微批摄取：多分片一批、批内乱序、游标与 receipt 一起生效
  9. 确认超时重发：乱序重发返回原 receipt；改内容则幂等冲突
 10. 缺号 / 重复 / 交叠：整批拒绝，合法消息可再次摄取
 11. 确认前崩溃：重启重放找到原 receipt（真实子进程）
 12. 两个接入进程与压实同时工作：互不覆盖

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
    BatchRejectedError,
    ConflictError,
    IdempotencyConflictError,
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


# ---- 场景 11/12 的子进程：摄取确认前崩溃 / 并发接入 ------------------------
def crashing_sender_child(batch_key, batch, crash_point):
    os.environ["LAKE_INGEST_CRASH_POINT"] = crash_point  # 测试钩子：定点硬退出
    t = LakeTable.open(ROOT)
    t.ingest_batch(batch_key, batch)


def ingest_worker_child(source, n, barrier, q):
    t = LakeTable.open(ROOT)
    barrier.wait(60)
    for i in range(n):
        t.ingest_batch(
            f"{source}-{i}",
            [
                {"source": source, "partition": 0, "offset": 2 * i,
                 "row_id": f"{source}-{2 * i:04d}", "value": 2 * i},
                {"source": source, "partition": 0, "offset": 2 * i + 1,
                 "row_id": f"{source}-{2 * i + 1:04d}", "value": 2 * i + 1},
            ],
        )
    q.put(("done", source))


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

    banner("8. 微批摄取：多分片一批、批内乱序、游标与数据同一事务生效")
    batch = [
        {"source": "train", "partition": 0, "offset": 1, "row_id": "t0-0001", "value": 101},
        {"source": "train", "partition": 0, "offset": 0, "row_id": "t0-0000", "value": 100},
        {"source": "train", "partition": 1, "offset": 0, "row_id": "t1-0000", "value": 110},
        {"source": "eval", "partition": 0, "offset": 0, "row_id": "e0-0000", "value": 200},
    ]
    r = t.ingest_batch("demo-batch-1", batch)
    ok(f"receipt={r['receipt']}  生效版本 v{r['version']}  行数 {r['num_rows']}")
    ok(f"各分片新游标: {r['cursors']}")
    ok(f"进度查询 ingest_cursors(): {t.ingest_cursors()}")

    banner("9. 确认超时重发：乱序重发返回原 receipt；改内容则幂等冲突")
    r2 = t.ingest_batch("demo-batch-1", list(reversed(batch)))  # 顺序打乱的重发
    assert r2["receipt"] == r["receipt"] and r2["replayed"]
    ok(f"乱序重发 -> 原 receipt（replayed=True），版本仍 v{t.current_version()}，未加行")
    tampered = [dict(m) for m in batch]
    tampered[0]["value"] = 999
    try:
        t.ingest_batch("demo-batch-1", tampered)
        raise SystemExit("改内容重发应当冲突")
    except IdempotencyConflictError as e:
        ok(f"改内容重发 -> 幂等冲突: {e}")

    banner("10. 缺号 / 重复 / 交叠：整批拒绝；合法消息可再次摄取")
    try:
        t.ingest_batch("demo-bad", [
            {"source": "train", "partition": 0, "offset": 5, "row_id": "x", "value": 1},
            {"source": "eval", "partition": 0, "offset": 1, "row_id": "y", "value": 2},
        ])
        raise SystemExit("缺号应当整批拒绝")
    except BatchRejectedError as e:
        ok(f"train/0 游标在 2 却发来 5 -> 整批拒绝: {e.reasons[0]}")
    ok(f"合法的 eval/0 也未被推进: {t.ingest_cursors()}")
    r3 = t.ingest_batch("demo-batch-2", [
        {"source": "train", "partition": 0, "offset": 2, "row_id": "t0-0002", "value": 102},
        {"source": "eval", "partition": 0, "offset": 1, "row_id": "e0-0001", "value": 201},
    ])
    ok(f"修正后重发成功: 游标 {r3['cursors']}")

    banner("11. 确认前崩溃：重启重放找到原 receipt（真实子进程硬退出）")
    crash_batch = [
        {"source": "train", "partition": 1, "offset": 1, "row_id": "t1-0001", "value": 111},
    ]
    p = CTX.Process(
        target=crashing_sender_child, args=("demo-crash", crash_batch, "published")
    )
    p.start(); p.join(60)
    assert p.exitcode == 42
    rec = t.batch_record("demo-crash")
    ok(f"子进程在确认前硬退出（exit 42），但事务已生效: receipt={rec['receipt'][:20]}…")
    rows_before = len(t.scan())
    r4 = t.ingest_batch("demo-crash", crash_batch)  # 接入端重发
    assert r4["receipt"] == rec["receipt"] and r4["replayed"]
    assert len(t.scan()) == rows_before
    ok(f"重放返回原 receipt，未新增行（{rows_before} 行）、未推进版本")

    banner("12. 两个接入进程与压实同时工作：互不覆盖")
    base_version = t.current_version()
    before = t.scan()
    n = 5
    barrier = CTX.Barrier(3)
    q = CTX.Queue()
    p1 = CTX.Process(target=ingest_worker_child, args=("srcA", n, barrier, q))
    p2 = CTX.Process(target=ingest_worker_child, args=("srcB", n, barrier, q))
    p1.start(); p2.start()
    small = sorted(t.manifest()["data_files"])
    plan_id = create_plan(t, selected=small)       # 压实计划先行
    barrier.wait(60)                                # 三方对齐：摄取并发、压实提交
    commit_plan(t, plan_id)
    p1.join(120); p2.join(120)
    assert p1.exitcode == 0 and p2.exitcode == 0
    done = sorted(q.get(timeout=30)[1] for _ in range(2))
    assert done == ["srcA", "srcB"]
    expected_rows = before.num_rows + 2 * 2 * n
    assert t.scan().num_rows == expected_rows
    assert t.ingest_cursors()["srcA"] == {"0": 2 * n}
    assert t.ingest_cursors()["srcB"] == {"0": 2 * n}
    assert t.current_version() == base_version + 2 * n + 1
    ok(f"2 个接入进程各 {n} 批 + 1 次压实全部生效: "
       f"版本 v{base_version} -> v{t.current_version()}，行数 {expected_rows}")
    ok(f"游标完整: srcA={t.ingest_cursors()['srcA']} srcB={t.ingest_cursors()['srcB']}")

    banner("全部演示通过")


if __name__ == "__main__":
    main()
