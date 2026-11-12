"""按 row_id 成组抽样的切分演示（本地样本，不连接任何外部系统）。

依次演示：
  1. 构造样本：一个业务对象对应多条观测，且观测跨物理分片
  2. 创建切分：对象成组、三个子集不重不漏（对照手算与独立校验脚本）
  3. 改变 Parquet 文件排列：归属逐对象不变
  4. 先压实再切分：归属不变（输入 snapshot 变化 -> 新身份）
  5. snapshot 推进：切分过程中新提交的数据不进入本次结果
  6. 空 row_id：整次拒绝，不留下任何交付物
  7. 错误权重：整次拒绝
  8. 相同参数再次调用 -> 返回原切分，文件字节不变
  9. 参数变化 -> 新身份、新目录，旧交付不被替换
 10. 发布改名前的子进程退出：目标路径不可见半成品，重新调用完成交付
 11. 源数据文件被回收后，已交付子集仍可直接读取

运行：python3 demo_split.py
"""

from __future__ import annotations

import hashlib
import multiprocessing as mp
import os
import shutil
import subprocess
import sys
import tempfile
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lake import (  # noqa: E402
    LakeTable,
    SplitError,
    check_split,
    collect,
    commit_plan,
    create_plan,
    create_split,
    export_subset,
)
from lake.splits.split import assign_subset  # noqa: E402

CTX = mp.get_context("fork")
HERE = os.path.dirname(os.path.abspath(__file__))

# 一个对象 -> 多条观测；obj-1/obj-2/obj-3/obj-4 的观测故意跨分片出现
ROWS = [
    ("obj-1", 10), ("obj-2", 20), ("obj-3", 30), ("obj-1", 11),
    ("obj-4", 40), ("obj-5", 50), ("obj-6", 60), ("obj-2", 21),
    ("obj-7", 70), ("obj-8", 80), ("obj-9", 90), ("obj-10", 100),
    ("obj-3", 31), ("obj-11", 110), ("obj-12", 120), ("obj-4", 41),
]
WEIGHTS = (6000, 2000, 2000)
SEED = 12345


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def write_shard(table, rows):
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


def build_lake(root):
    t = LakeTable.create(root)
    write_shard(t, ROWS[:5])
    write_shard(t, ROWS[5:10])
    write_shard(t, ROWS[10:])
    return t


def read_all(directory):
    out = []
    for name in ("train", "validation", "test"):
        tab = pq.read_table(os.path.join(directory, f"{name}.parquet"))
        out.extend(
            (rid, val)
            for rid, val in zip(
                tab.column("row_id").to_pylist(), tab.column("value").to_pylist()
            )
        )
    return out


def objects_at(directory):
    """{row_id: subset}，并断言同一对象只出现在一个子集。"""
    mapping = {}
    for name in ("train", "validation", "test"):
        tab = pq.read_table(os.path.join(directory, f"{name}.parquet"))
        for rid in set(tab.column("row_id").to_pylist()):
            assert rid not in mapping or mapping[rid] == name, "对象跨集合"
            mapping[rid] = name
    return mapping


def crash_worker(root):
    os.environ["LAKE_SPLIT_CRASH_POINT"] = "staged"
    create_split(LakeTable.open(root), None, *WEIGHTS, SEED)


def main() -> None:
    workdir = tempfile.mkdtemp(prefix="split-demo-")
    root = os.path.join(workdir, "lake")
    print(f"临时工作目录: {workdir}")

    # ---------------------------------------------------------- 1. 构样本
    banner("1. 构造样本：一个对象多条观测，且观测跨物理分片")
    t = build_lake(root)
    v1 = t.current_version()
    objects = {r[0] for r in ROWS}
    print(f"输入 snapshot v{v1}：{len(ROWS)} 行、{len(objects)} 个对象、"
          f"{len(t.manifest(v1)['data_files'])} 个物理分片")
    for rid in ("obj-1", "obj-2", "obj-4"):
        vals = sorted(v for r, v in ROWS if r == rid)
        print(f"  对象 {rid} 的观测: {vals}")

    # ---------------------------------------------------------- 2. 建切分
    banner("2. 创建切分：对象成组；三个子集恰好覆盖输入、且无重复")
    m1 = create_split(t, v1, *WEIGHTS, SEED)
    d1 = os.path.join(t.root, "splits", m1["split_id"])
    print(f"split_id={m1['split_id']}  seed={SEED}  权重={m1['weights']}")
    for name in ("train", "validation", "test"):
        meta = m1["subsets"][name]
        print(f"  {name:<10} {meta['num_rows']:>3} 行  "
              f"{meta['num_objects']:>3} 对象  sha256={meta['sha256'][:12]}…")
    print(f"输入可见行 {m1['input_snapshot']['num_visible_rows']} 行 / "
          f"{m1['input_snapshot']['num_objects']} 对象；")
    print(f"注意: {m1['proportionality_notice']}")

    objmap = objects_at(d1)
    assert set(objmap) == objects
    assert sorted(read_all(d1)) == sorted(ROWS)
    print(f"对象归属（按文档散列复算）: "
          f"{ {o: assign_subset(SEED, o, m1['weights']) for o in sorted(objects)} }")
    for o in objects:
        assert objmap[o] == assign_subset(SEED, o, m1["weights"])
    print("成组核对通过：每个对象的多条观测同属一个子集；"
          "三集合总计 16 行，无重复、无遗漏")

    r = subprocess.run(
        [sys.executable, os.path.join(HERE, "check_split.py"), d1,
         "--verify-assignment"],
        capture_output=True, text=True,
    )
    print(f"\n独立校验脚本 check_split.py（exit {r.returncode}）：")
    print(r.stdout.rstrip())

    # ---------------------------------------------- 3. 文件排列不变
    banner("3. 改变 Parquet 文件排列：逐对象归属不变")
    t_reorder = LakeTable.create(os.path.join(workdir, "lake_reorder"))
    for row in reversed(ROWS):           # 每行一个文件，且倒序追加
        write_shard(t_reorder, [row])
    m2 = create_split(t_reorder, t_reorder.current_version(), *WEIGHTS, SEED)
    d2 = os.path.join(t_reorder.root, "splits", m2["split_id"])
    assert objects_at(d1) == objects_at(d2)
    print(f"原排列 {len(t.manifest(v1)['data_files'])} 个分片 / "
          f"新排列 {len(t_reorder.manifest()['data_files'])} 个分片，"
          f"归属表逐对象相同: {objects_at(d1) == objects_at(d2)}")
    print(f"（物理排列不同 -> 输入 snapshot 不同 -> split_id 不同: "
          f"{m1['split_id']} vs {m2['split_id']}）")

    # ---------------------------------------------- 4. 先压实再切分
    banner("4. 先压实再切分：可见内容不变 -> 归属不变")
    v_pre = t.current_version()
    m_pre = create_split(t, v_pre, *WEIGHTS, SEED)
    d_pre = os.path.join(t.root, "splits", m_pre["split_id"])
    pre_map = objects_at(d_pre)

    commit_plan(t, create_plan(t, selected=sorted(t.manifest(v_pre)["data_files"])))
    v_post = t.current_version()
    m_post = create_split(t, v_post, *WEIGHTS, SEED)
    d_post = os.path.join(t.root, "splits", m_post["split_id"])
    post_map = objects_at(d_post)
    assert pre_map == post_map
    print(f"压实 v{v_pre} -> v{v_post}，数据文件 "
          f"{len(t.manifest(v_pre)['data_files'])} -> "
          f"{len(t.manifest(v_post)['data_files'])}；归属逐对象一致: "
          f"{pre_map == post_map}")

    # ---------------------------------------------- 5. snapshot 推进
    banner("5. snapshot 推进：切分中途提交的新数据不进入本次结果")
    from lake.splits import split as split_mod

    t_snap = build_lake(os.path.join(workdir, "lake_snap"))
    v_fixed = t_snap.current_version()
    real_collect = split_mod.collect_groups

    def collect_then_commit(table, manifest, batch_size=4096):
        table.append([("late-obj", 999)])            # 读取途中的新 snapshot
        return real_collect(table, manifest, batch_size=batch_size)

    split_mod.collect_groups = collect_then_commit
    try:
        m_late = create_split(t_snap, v_fixed, *WEIGHTS, SEED)
    finally:
        split_mod.collect_groups = real_collect
    d_late = os.path.join(t_snap.root, "splits", m_late["split_id"])
    late_pairs = read_all(d_late)
    assert "late-obj" not in {rid for rid, _ in late_pairs}
    assert t_snap.current_version() > v_fixed
    print(f"输入固定 v{v_fixed}，期间 HEAD 推进到 v{t_snap.current_version()}；"
          f"本次交付仍是 {len(late_pairs)} 行，late-obj 未出现: "
          f"{'late-obj' not in {rid for rid, _ in late_pairs}}")
    print(f"清单如实记录 input snapshot: v{m_late['input_version']}")

    # ---------------------------------------------- 6. 空 row_id
    banner("6. 空 row_id 无法分组：整次产出拒绝，不留下交付物")
    t_bad = LakeTable.create(os.path.join(workdir, "lake_bad"))
    write_shard(t_bad, ROWS[:8])
    write_shard(t_bad, [("obj-ok", 1), (None, 2)])
    try:
        create_split(t_bad, None, *WEIGHTS, SEED)
        raise SystemExit("空 row_id 应当被拒绝")
    except SplitError as e:
        print(f"  拒绝: {e}")
    splits_root = os.path.join(t_bad.root, "splits")
    leftovers = [
        n for n in os.listdir(splits_root)
        if not n.startswith(".") and n != "LOCK"
    ]
    print(f"  正式交付目录: {leftovers}（空）")

    # ---------------------------------------------- 7. 错误权重
    banner("7. 错误权重：整次拒绝")
    for label, w in (
        ("总和 9000", (7000, 1000, 1000)),
        ("含负数", (8000, -1, 2001)),
    ):
        try:
            create_split(t_bad, None, *w, SEED)
            raise SystemExit("应当拒绝")
        except SplitError as e:
            print(f"  {label}: {e}")

    # ---------------------------------------------- 8/9. 身份
    banner("8. 相同参数再次调用：返回原切分，文件字节不变")
    before = {
        n: hashlib.sha256(
            open(os.path.join(d1, f"{n}.parquet"), "rb").read()
        ).hexdigest()
        for n in ("train", "validation", "test")
    }
    again = create_split(t, v1, *WEIGHTS, SEED)
    after = {
        n: hashlib.sha256(
            open(os.path.join(d1, f"{n}.parquet"), "rb").read()
        ).hexdigest()
        for n in ("train", "validation", "test")
    }
    print(f"  reused={again['reused']}  split_id 相同: "
          f"{again['split_id'] == m1['split_id']}  文件字节不变: "
          f"{before == after}")

    banner("9. 参数变化 -> 新身份、新目录；旧交付原样保留")
    other = create_split(t, v1, 5000, 2500, 2500, SEED + 1)
    print(f"  原: {m1['split_id']}（保留）  新: {other['split_id']}"
          f"（reused={other['reused']}）")
    assert os.path.isdir(d1) and os.path.isdir(
        os.path.join(t.root, "splits", other["split_id"])
    )

    # ---------------------------------------------- 10. 发布边界崩溃
    banner("10. 发布改名前的子进程退出：目标路径不可见半成品，再次调用完成")
    t_crash = build_lake(os.path.join(workdir, "lake_crash"))
    v_crash = t_crash.current_version()
    p = CTX.Process(target=crash_worker, args=(t_crash.root,))
    p.start()
    p.join(60)
    print(f"  子进程 exitcode={p.exitcode}（发布前硬退出）")
    splits_root = os.path.join(t_crash.root, "splits")
    published = [
        n for n in os.listdir(splits_root)
        if not n.startswith(".") and n != "LOCK"
    ]
    staging = [n for n in os.listdir(splits_root) if n.startswith(".staging-")]
    print(f"  正式交付目录: {published}（空）  暂存残骸: {len(staging)} 个")
    assert published == [] and len(staging) == 1
    recovered = create_split(t_crash, v_crash, *WEIGHTS, SEED)
    d_crash = os.path.join(t_crash.root, "splits", recovered["split_id"])
    print(f"  重新调用完成交付: reused={recovered['reused']}，"
          f"校验通过={check_split(d_crash)['ok']}，"
          f"暂存残骸已清理={[n for n in os.listdir(splits_root) if n.startswith('.staging-')] == []}")

    # ---------------------------------------------- 11. GC 后仍可读
    banner("11. 源数据文件被回收后，已交付子集仍可直接读取")
    t_gc = build_lake(os.path.join(workdir, "lake_gc"))
    v_gc = t_gc.current_version()
    m_gc = create_split(t_gc, v_gc, *WEIGHTS, SEED)
    d_gc = os.path.join(t_gc.root, "splits", m_gc["split_id"])
    before_gc = sorted(read_all(d_gc))
    commit_plan(t_gc, create_plan(
        t_gc, selected=sorted(t_gc.manifest(v_gc)["data_files"])
    ))
    removed = collect(t_gc)["removed_files"]
    old_paths = [meta["path"] for meta in t_gc.manifest(v_gc)["data_files"].values()]
    assert all(not os.path.exists(os.path.join(t_gc.root, p_)) for p_ in old_paths)
    assert sorted(read_all(d_gc)) == before_gc
    assert check_split(d_gc)["ok"] is True
    dest = export_subset(d_gc, "train")
    print(f"  GC 回收 {len(removed)} 个旧数据/删除文件")
    print(f"  交付子集仍可读取: {len(read_all(d_gc))} 行；独立校验通过: "
          f"{check_split(d_gc)['ok']}；导出的 train 文件: "
          f"{os.path.basename(dest)}（{pq.read_table(dest).num_rows} 行）")

    shutil.rmtree(workdir, ignore_errors=True)
    print("\n全部演示完成。")


if __name__ == "__main__":
    main()
