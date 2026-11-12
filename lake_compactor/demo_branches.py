"""实验分支端到端演示：分叉后内容确实不同的小样本，逐步核对完整状态。

流程（严格按验收顺序）：

  1. 建表并写入正式基线；证明旧调用（不传分支）= main
  2. 从已有提交建立实验分支，起初共享底层内容（不复制文件）
  3. 实验线追加/删行；证明实验改动对正式读者不可见
  4. 快进发布：main 指向已有实验提交，不复制样本；断言版本/文件不变
  5. main 先产生独立变更 -> 再发布被拒（分叉），双方原状保留
  6. 两条实验线并发接入**相同请求键**，各自重放核对消费进展与回执
  7. 一条线文件整理、另一条线修改：断言两条线数据都在
  8. 应用读取即将撤销的实验内容时运行回收，读完后再次回收
  9. 关闭并重开产品，核对全部引用与读取结果

每一段都打印并断言**完整记录内容**（row_id/value 多重集、manifest 文件
集合、提交号与父提交、游标、receipt、磁盘文件存在性），而不只是分支名。

运行：python3 demo_branches.py
"""

from __future__ import annotations

import multiprocessing as mp
import os
import shutil
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lake import (  # noqa: E402
    BatchRejectedError,
    BranchExistsError,
    BranchNameError,
    DivergedError,
    LakeTable,
    ProtectedBranchError,
    StaleMainError,
    collect,
    commit_plan,
    create_plan,
)
from lake import manifest as mf  # noqa: E402
from lake.workbench import query_arrow  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo_branch_lake")
CTX = mp.get_context("fork")


def banner(msg: str) -> None:
    print(f"\n{'=' * 72}\n■ {msg}\n{'=' * 72}")


def ok(msg: str) -> None:
    print(f"  ✔ {msg}")


def rows(table):
    """完整记录内容（有序 row_id, value 对），演示中所有核对都用它。"""
    return sorted(
        zip(
            table.column("row_id").to_pylist(),
            table.column("value").to_pylist(),
        )
    )


def file_refs(manifest_obj):
    return sorted(
        [meta["path"] for meta in manifest_obj["data_files"].values()]
        + [meta["path"] for meta in manifest_obj["delete_files"].values()]
    )


def show_rows(label, table):
    print(f"    {label}: {rows(table)}")


def msgs(source, offsets, prefix, base=0, partition=0):
    return [
        {
            "source": source,
            "partition": partition,
            "offset": o,
            "row_id": f"{prefix}{o:04d}",
            "value": base + o,
        }
        for o in offsets
    ]


# ---------------------------------------------------------------- 子进程
def isolated_ingest_child(root, branch, batch_key, batch, barrier, q):
    """同一个请求键在两个分支上并发提交（真实子进程 + 屏障对齐）。"""
    try:
        t = LakeTable.open(root, branch=branch)
        barrier.wait(60)
        result = t.ingest_batch(batch_key, batch)
        q.put(("ok", branch, result))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", branch, repr(e)))


def compact_child(root, branch, q, barrier):
    try:
        t = LakeTable.open(root, branch=branch)
        plan = create_plan(t, max_rows=10_000)
        barrier.wait(60)
        q.put(("ok", commit_plan(t, plan)))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def write_child(root, branch, q, barrier):
    try:
        t = LakeTable.open(root, branch=branch)
        barrier.wait(60)
        fid = t.append([("line-b-added", 41), ("line-b-dropped", 42)])
        t.publish_deletes(fid, [1])   # 删掉本批第二个追加文件中的第二行
        q.put(("ok", t.current_version()))
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def main() -> None:  # noqa: C901 - 演示是顺序脚本，刻意保持线性
    shutil.rmtree(ROOT, ignore_errors=True)

    # ------------------------------------------------------------------ 1
    banner("1. 正式基线：旧调用（不传分支）= main；名字冲突与非法名明确报错")
    t = LakeTable.create(ROOT)
    base_file = t.append([("prod-1", 100), ("prod-2", 200), ("prod-3", 300)])
    t.publish_deletes(base_file, [1])          # 正式数据删掉 prod-2
    main_tip = t.current_version()
    main_manifest = t.manifest()
    main_files = file_refs(main_manifest)
    show_rows("main 可见行", t.scan())
    ok(f"main 尖端 v{main_tip}，数据文件 {list(main_manifest['data_files'])}，"
       f"删除文件 {list(main_manifest['delete_files'])}")
    assert rows(t.scan()) == [("prod-1", 100), ("prod-3", 300)]
    assert t.branch == "main"

    for bad in ["", "exp/alpha", "1 two", ".hidden", "main"]:
        try:
            t.create_branch(bad)
            raise SystemExit(f"非法/保留名字 {bad!r} 应当被拒绝")
        except (BranchNameError, BranchExistsError) as e:
            ok(f"名字 {bad!r} 被拒绝：{e}")
    assert mf.load_branches(t.meta_root) == {}, "拒绝不得留下分支引用"
    assert mf.load_manifest(t.meta_root, main_tip) == main_manifest

    # 工作台以精确提交号取数：正式读者登记 main 尖端，看不到实验行
    prod_query = query_arrow(
        {"lake": (ROOT, main_tip)},
        "SELECT row_id, value FROM lake ORDER BY row_id",
    ).to_pylist()
    ok(f"正式读者工作台查询 v{main_tip}: {[(r['row_id'], r['value']) for r in prod_query]}")
    assert [(r["row_id"], r["value"]) for r in prod_query] == [
        ("prod-1", 100), ("prod-3", 300)
    ]

    # ------------------------------------------------------------------ 2
    banner("2. 从已有提交建立实验分支：起初共享底层内容，不复制样本")
    created = t.create_branch("exp-alpha", from_version=main_tip)
    exp = t.checkout("exp-alpha")
    ok(f"分支 {created['name']} 起于 v{created['base_version']}，"
       f"共享 {created['shared_files']} 个底层文件，复制版本 {created['copied_versions']} 个")
    assert exp.current_version() == main_tip
    assert file_refs(exp.manifest()) == main_files, "起初共享同一组文件"
    assert sorted(os.listdir(t.data_dir)) == sorted(
        os.path.basename(p) for p in main_files
    )
    show_rows("实验分支初始可见行（与 main 相同）", exp.scan())
    listed = t.list_branches()
    ok(f"列举分支: " + ", ".join(
        f"{b['name']}@v{b['tip']}" for b in listed
    ))
    assert [b["name"] for b in listed] == ["exp-alpha", "main"]

    # ------------------------------------------------------------------ 3
    banner("3. 实验改动（追加 / 删行 / 微批接入 / 文件整理）：只推进本线")
    exp_added = exp.append([("exp-x", 401), ("exp-y", 402), ("exp-z", 403)])
    exp.publish_deletes(exp_added, [1])                 # 删 exp-y
    r = exp.ingest_batch("experiment-sample-batch", msgs("train", [0, 1, 2], "exp-i", 500))
    exp_plan = create_plan(exp, selected=sorted(exp.manifest()["data_files"]))
    commit_plan(exp, exp_plan)                          # 实验线整理
    exp_tip = exp.current_version()
    exp_manifest = exp.manifest()
    exp_files = file_refs(exp_manifest)
    show_rows("实验分支可见行", exp.scan())
    ok(f"实验尖端 v{exp_tip}（父 v{exp_manifest['parent']}，作者分支 "
       f"{exp_manifest['branch']}），文件引用 {exp_files}")
    assert rows(exp.scan()) == sorted(
        [("prod-1", 100), ("prod-3", 300), ("exp-x", 401), ("exp-z", 403)]
        + [("exp-i0000", 500), ("exp-i0001", 501), ("exp-i0002", 502)]
    )
    assert exp.ingest_cursors() == {"train": {"0": 3}}

    # 正式读者看不到任何实验改动：版本、行、文件、游标全部未动
    show_rows("main 可见行（实验进行中）", t.scan())
    assert t.current_version() == main_tip
    assert rows(t.scan()) == [("prod-1", 100), ("prod-3", 300)]
    assert file_refs(t.manifest()) == main_files
    assert t.ingest_cursors() == {}
    assert t.batch_record("experiment-sample-batch") is None
    ok("实验行 exp-x/exp-i* 对正式读者不可见；main 的版本、文件引用、游标均未变")

    # ------------------------------------------------------------------ 4
    banner("4. publish 快进：main 指向已有实验提交，不复制样本、不另造提交")
    files_before = sorted(os.listdir(t.data_dir))
    versions_before = sorted(os.listdir(mf.versions_dir(t.meta_root)))
    exp_manifest_bytes = mf.version_path(t.meta_root, exp_tip)
    with open(exp_manifest_bytes, "rb") as f:
        exp_manifest_blob = f.read()
    published = exp.publish(expected_main_version=main_tip)
    ok(f"publish 结果: main v{published['main_before']} -> v{published['main_after']}；"
       f"交付提交 {published['commits']}；复用={published['reused_commit']}，"
       f"复制行数={published['copied_rows']}")
    assert published["main_after"] == exp_tip
    assert mf.current_version(t.meta_root) == exp_tip
    with open(exp_manifest_bytes, "rb") as f:
        assert f.read() == exp_manifest_blob, "实验提交文件字节未被重写"
    assert mf.load_manifest(t.meta_root, exp_tip)["branch"] == "exp-alpha"
    assert sorted(os.listdir(t.data_dir)) == files_before, "未复制任何数据文件"
    assert sorted(os.listdir(mf.versions_dir(t.meta_root))) == versions_before
    assert file_refs(t.manifest()) == exp_files
    show_rows("main 快进后可见行", t.scan())
    assert rows(t.scan()) == rows(exp.scan())
    assert t.ingest_cursors() == {"train": {"0": 3}}
    exp_query = query_arrow(
        {"lake": (ROOT, exp_tip)},
        "SELECT row_id, value FROM lake ORDER BY row_id",
    ).to_pylist()
    assert [(r["row_id"], r["value"]) for r in exp_query] == rows(t.scan())
    ok(f"工作台登记实验提交 v{exp_tip} 读到 {len(exp_query)} 行，"
       "与 main 快进后的可见行逐条一致")
    ok("main 现在读取实验样本；版本文件集合与 data/ 文件集合均无新增")

    # ------------------------------------------------------------------ 5
    banner("5. main 先产生独立变更 -> publish 被拒（已分叉），双方原状保留")
    fork_point = mf.current_version(t.meta_root)
    t.append([("prod-4", 400)])                     # 正式写者推进 main
    main_diverged = t.current_version()
    m = t.manifest()
    ok(f"main 在 v{fork_point} 上新增正式行 -> v{main_diverged}（父 v{m['parent']}）")

    exp.append([("exp-new", 404)])                  # 实验线也继续走
    exp_after_diverged = exp.current_version()
    ok(f"实验线继续提交 -> v{exp_after_diverged}（父 "
       f"v{exp.manifest()['parent']}）")

    before_main = rows(t.scan())
    before_exp = rows(exp.scan())
    versions_before_reject = sorted(os.listdir(mf.versions_dir(t.meta_root)))
    files_before_reject = sorted(os.listdir(t.data_dir))
    try:
        exp.publish(expected_main_version=main_diverged)
        raise SystemExit("已分叉，publish 应被拒绝")
    except DivergedError as e:
        ok(f"分叉拒绝: {e}")
        assert e.main_version == main_diverged
        assert e.source_version == exp_after_diverged
    # 预期的 main 尖端过期也会被拒绝（防止检查后被抢跑）
    try:
        exp.publish(expected_main_version=main_diverged - 1)
        raise SystemExit("过期 expected main 应被拒绝")
    except StaleMainError as e:
        ok(f"过期预期拒绝: {e}")
        assert e.actual_main_version == main_diverged
    # 拒绝保留双方原状
    assert mf.current_version(t.meta_root) == main_diverged
    assert mf.load_branches(t.meta_root)["exp-alpha"] == exp_after_diverged
    assert sorted(os.listdir(mf.versions_dir(t.meta_root))) == versions_before_reject
    assert sorted(os.listdir(t.data_dir)) == files_before_reject
    assert rows(t.scan()) == before_main
    assert rows(exp.scan()) == before_exp
    show_rows("拒绝后 main", t.scan())
    show_rows("拒绝后 exp-alpha", exp.scan())
    ok("拒绝路径未产生新版本、未移动任何尖端、未删除/改写任何文件")

    # main 不能撤销
    try:
        t.delete_branch("main")
        raise SystemExit("main 应受保护")
    except ProtectedBranchError as e:
        ok(f"撤销 main 被拒绝: {e}")

    # ------------------------------------------------------------------ 6
    banner("6. 两条实验线并发接入相同请求键：各自进展，不借用回执/游标")
    t.create_branch("line-a", from_version=main_diverged)
    t.create_branch("line-b", from_version=main_diverged)
    a, b = t.checkout("line-a"), t.checkout("line-b")
    key = "shared-key-001"
    # 用一条此前未消费的来源，证明两条线都从同一游标（0）出发
    same_batch = msgs("branch-src", [0, 1, 2], "shared", 600)
    barrier = CTX.Barrier(2)
    q = CTX.Queue()
    pa_ = CTX.Process(
        target=isolated_ingest_child, args=(ROOT, "line-a", key, same_batch, barrier, q)
    )
    pb = CTX.Process(
        target=isolated_ingest_child, args=(ROOT, "line-b", key, same_batch, barrier, q)
    )
    pa_.start(); pb.start(); pa_.join(120); pb.join(120)
    assert pa_.exitcode == 0 and pb.exitcode == 0
    results = {}
    for _ in range(2):
        status, branch, payload = q.get(timeout=60)
        assert status == "ok", (branch, payload)
        results[branch] = payload
    ra, rb = results["line-a"], results["line-b"]
    ok(f"line-a: receipt={ra['receipt']} v{ra['version']} 游标 {ra['cursors']}")
    ok(f"line-b: receipt={rb['receipt']} v{rb['version']} 游标 {rb['cursors']}")
    assert ra["receipt"] != rb["receipt"], "相同键在两条线回执必须不同"
    assert ra["version"] != rb["version"], "两条线各自取全库唯一号"
    assert ra["replayed"] is rb["replayed"] is False
    assert a.ingest_cursors()["branch-src"] == {"0": 3}
    assert b.ingest_cursors()["branch-src"] == {"0": 3}
    # 父提交继承的 train 游标各自保留，未互相借用
    assert a.ingest_cursors()["train"] == {"0": 3}
    assert b.ingest_cursors()["train"] == {"0": 3}

    # 各自重放本线：拿本线自己的原 receipt；两条线数据都在
    a2 = a.ingest_batch(key, list(reversed(same_batch)))
    b2 = b.ingest_batch(key, list(reversed(same_batch)))
    assert a2["replayed"] and b2["replayed"]
    assert a2["receipt"] == ra["receipt"] != rb["receipt"] == b2["receipt"]
    assert a.current_version() == ra["version"]
    assert b.current_version() == rb["version"]
    rec_a, rec_b = a.batch_record(key), b.batch_record(key)
    idx_a = a.manifest()["ingest"]["batches"][key]["record"]
    idx_b = b.manifest()["ingest"]["batches"][key]["record"]
    ok(f"重放核对: line-a 回执保持 {a2['receipt']}，line-b 回执保持 {b2['receipt']}")
    ok(f"记录文件: {idx_a} / {idx_b}")
    assert idx_a != idx_b, "同键的两条线使用不同的审计记录文件"
    assert a.manifest()["ingest"]["batches"][key]["receipt"] == ra["receipt"]
    assert b.manifest()["ingest"]["batches"][key]["receipt"] == rb["receipt"]
    assert rec_a["branch"] == "line-a" and rec_b["branch"] == "line-b"
    assert rec_a["messages"] == rec_b["messages"]
    assert a2["num_rows"] == b2["num_rows"] == 3
    show_rows("line-a", a.scan())
    show_rows("line-b", b.scan())
    assert rows(a.scan()) == rows(b.scan())
    assert ("shared0000", 600) in rows(a.scan())
    assert ("shared0000", 600) not in rows(t.scan())
    # 两条线共享 main 的正式行，但共享行在各自快照里都存在
    assert ("prod-4", 400) in rows(a.scan()) and ("prod-4", 400) in rows(b.scan())

    # ------------------------------------------------------------------ 7
    banner("7. 一条线文件整理、另一条线修改：两条线的数据都保留")
    # line-a 先有自己的写入，然后整理；line-b 在屏障后并发写
    a.append([("line-a-note", 700)])
    a_before = rows(a.scan())
    barrier = CTX.Barrier(2)
    q = CTX.Queue()
    pc = CTX.Process(target=compact_child, args=(ROOT, "line-a", q, barrier))
    pw = CTX.Process(target=write_child, args=(ROOT, "line-b", q, barrier))
    pc.start(); pw.start(); pc.join(120); pw.join(120)
    assert pc.exitcode == 0 and pw.exitcode == 0
    events = [q.get(timeout=60) for _ in range(2)]
    assert all(e[0] == "ok" for e in events), events
    compact_v = events[0][1]
    ok(f"子进程结果: {events}")
    assert a.current_version() != b.current_version()

    a_after, b_after = rows(a.scan()), rows(b.scan())
    show_rows("line-a（已整理）", a.scan())
    show_rows("line-b（并发追加/删行）", b.scan())
    # line-a：整理不改变内容，且保留自己的 note
    assert a_after == a_before
    assert ("line-a-note", 700) in a_after
    # line-b：并发写入生效，删除生效
    assert ("line-b-added", 41) in b_after
    assert ("line-b-dropped", 42) not in b_after
    # 彼此数据都保留：两条线都完整保留 main 的共享行 + 各自独有行
    assert ("shared0002", 602) in a_after and ("shared0002", 602) in b_after
    assert ("line-a-note", 700) not in b_after
    assert ("line-b-added", 41) not in a_after
    ok("整理线内容不变、修改线写入生效；两条线的独有行互不丢失")
    # 磁盘上每条线引用到的文件都存在
    for label, tbl in (("line-a", a), ("line-b", b)):
        for rel in file_refs(tbl.manifest()):
            assert os.path.exists(os.path.join(ROOT, rel)), (label, rel)
    ok("两条线的 manifest 引用文件全部在磁盘上")
    # 正式 main 仍不受两条实验线影响（除了它自己的正式行）
    assert rows(t.scan()) == before_main
    ok("main 未受实验线整理/写入影响")

    # ------------------------------------------------------------------ 8
    banner("8. 应用读取即将撤销的实验内容时回收；读完后再次回收才清除")
    # 另起一条短命实验线，它有独占内容，随后撤销
    temp_base = mf.current_version(t.meta_root)
    t.create_branch("exp-temp", from_version=temp_base)
    tmp = t.checkout("exp-temp")
    tmp_file = tmp.append([("temp-a", 800), ("temp-b", 801)])   # temp_tip - 1
    tmp.publish_deletes(tmp_file, [0])                          # temp_tip
    temp_tip = tmp.current_version()
    temp_manifest = tmp.manifest()
    temp_files = file_refs(temp_manifest)
    exclusive = [
        p for p in temp_files if p not in set(file_refs(t.manifest()))
    ]
    assert len(exclusive) == 2, "实验独占：一个数据文件 + 一个删除文件"
    ok(f"exp-temp 尖端 v{temp_tip}，独占内容 {exclusive}")

    temp_deleted = tmp.delete_branch("exp-temp")
    ok(f"撤销 exp-temp：引用已移除，独占提交 {temp_deleted['exclusive_versions']}")
    assert "exp-temp" not in mf.load_branches(t.meta_root)
    for rel in temp_files:
        assert os.path.exists(os.path.join(ROOT, rel)), "撤销本身不删文件"

    # 应用正在读取即将撤销的实验内容：持租约
    with tmp.lease(temp_tip, require_files=True) as lease:
        snapshot = tmp.scan(lease.version)
        show_rows(f"读取中的撤销快照 (v{lease.version})", snapshot)
        # 精确核对：撤销线是 main 的快照 + temp 的两行，再删掉 temp-a
        expected = sorted(rows(t.scan()) + [("temp-b", 801)])
        assert rows(snapshot) == expected
        report = collect(tmp, staging_max_age_seconds=0)
        ok(f"读取期间回收：受保护版本 {report['protected_versions']}；"
           f"本轮可回收的其他历史残骸 {len(report['removed_files'])} 个文件")
        assert temp_tip in report["protected_versions"]
        # 读取中的撤销快照独占内容不得出现在回收清单里，且文件仍在
        assert not (set(report["removed_files"]) & set(exclusive))
        for rel in exclusive:
            assert os.path.exists(os.path.join(ROOT, rel)), "租约期内不得清除"
        # 读取期间还能读完整的历史提交：删除前一行不少，删除后只少 temp-a
        base_rows = rows(tmp.scan(temp_base))
        appended = sorted(base_rows + [("temp-a", 800), ("temp-b", 801)])
        assert rows(tmp.scan(temp_tip - 1)) == appended
        assert rows(tmp.scan(temp_tip)) == sorted(base_rows + [("temp-b", 801)])
        ok("应用在租约内读到了完整撤销前快照，独占文件未被清除")

    # 读取结束：下一次回收清除不被任何存活尖端引用的独占内容
    report = collect(tmp, staging_max_age_seconds=0)
    ok(f"读完后回收：删除 {report['removed_files']}，"
       f"受保护 {report['protected_versions']}")
    assert set(report["removed_files"]) >= set(exclusive)
    for rel in exclusive:
        assert not os.path.exists(os.path.join(ROOT, rel))
    assert ("temp-a", 800) not in rows(t.scan())
    assert ("temp-b", 801) not in rows(t.scan())
    ok("撤销线独占内容已清除；main 内容逐条完好")
    show_rows("回收后 main", t.scan())

    # ------------------------------------------------------------------ 9
    banner("9. 关闭并重开产品：核对全部引用、父提交、游标与读取结果")
    tips_before = mf.all_tips(t.meta_root)
    manifests_before = {
        v: mf.load_manifest(t.meta_root, v) for v in range(max(tips_before.values()) + 1)
    }
    rows_before = {
        "main": rows(t.scan()),
        "line-a": rows(a.scan()),
        "line-b": rows(b.scan()),
        "exp-alpha": rows(exp.scan()),
    }
    cursors_before = {
        "line-a": a.ingest_cursors(),
        "line-b": b.ingest_cursors(),
        "exp-alpha": exp.ingest_cursors(),
    }
    receipts_before = {
        "line-a": a2["receipt"],
        "line-b": b2["receipt"],
    }
    files_before = sorted(os.listdir(t.data_dir))
    ok(f"关闭前尖端 {tips_before}")

    del t, a, b, exp, tmp
    import gc as _pygc

    _pygc.collect()

    t = LakeTable.open(ROOT)
    a = LakeTable.open(ROOT, branch="line-a")
    b = LakeTable.open(ROOT, branch="line-b")
    exp = LakeTable.open(ROOT, branch="exp-alpha")

    assert mf.all_tips(t.meta_root) == tips_before
    assert {
        v: mf.load_manifest(t.meta_root, v) for v in manifests_before
    } == manifests_before
    assert {name: rows(tbl.scan()) for name, tbl in
            (("main", t), ("line-a", a), ("line-b", b), ("exp-alpha", exp))} == rows_before
    assert {
        "line-a": a.ingest_cursors(),
        "line-b": b.ingest_cursors(),
        "exp-alpha": exp.ingest_cursors(),
    } == cursors_before
    assert a.batch_record(key)["receipt"] == receipts_before["line-a"]
    assert b.batch_record(key)["receipt"] == receipts_before["line-b"]
    assert sorted(os.listdir(t.data_dir)) == files_before
    ok("重开后分支尖端、提交文件、父链、游标、回执、数据行、磁盘文件全部一致")
    for b_ in t.list_branches():
        print(f"    分支 {b_['name']:>10} @ v{b_['tip']:>2} 父 "
              f"v{b_['parent']}  数据文件 {b_['num_data_files']}  "
              f"删除文件 {b_['num_delete_files']}  批次 {b_['ingest_batches']}")

    banner("全部演示通过：实验改动隔离、快进发布、拒绝分叉、并发接入隔离、"
           "整理与修改并存、撤销后租约感知回收、重开持久化")


if __name__ == "__main__":
    main()
