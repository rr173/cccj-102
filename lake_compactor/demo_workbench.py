"""湖表查询工作台端到端叙述式演示（本地样本，可手算核对）。

覆盖验收主线：
  1. 特征表 + 标签表按样本键对齐：内连接/左连接/过滤/分组逐一对照
     一份独立于产品扫描代码的期望行集（纯 Python 逐行展开）
  2. 同一目录登记新旧两个版本：同一条查询里对照修订前后的标签
  3. 查询取数途中子进程追加、压实并启动回收：读者保护下结果仍来自
     登记版本；结束后没有残留租约，版本与文件未被查询改动
  4. 空结果保留列名与类型；空版本也能返回空表 + 完整 schema
  5. 写语句 / 直接读文件 / 联网 / 未知名称 / 不存在的版本 / 语法错误
     逐项拒绝，并核对湖表文件与版本没有变化

运行：python3 demo_workbench.py
"""

from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import shutil
import sys
import tempfile
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lake import LakeTable, collect, commit_plan, create_plan  # noqa: E402
from lake.errors import WorkbenchError  # noqa: E402
from lake.workbench import query, query_arrow  # noqa: E402

CTX = mp.get_context("fork")

FEATURE_ROWS = [
    ("s-01", 10),
    ("s-02", 20),
    ("s-03", 30),
    ("s-01", 11),
    ("s-04", 40),
    ("s-05", 50),
    ("s-06", 60),
    ("s-02", 21),
]
LABEL_ROWS = [
    ("s-01", 100),
    ("s-02", 200),
    ("s-03", 300),
    ("s-07", 700),
]
NEW_LABELS = [
    ("s-01", 101),
    ("s-02", 201),
    ("s-04", 401),
]


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def write_shard(table: LakeTable, rows) -> str:
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


# ---------------------------------------------------------------- 独立参考实现
def independent_visible_rows(lake_dir: str, version: int) -> list[tuple]:
    """纯 Python 逐行展开：读 manifest + 删除文件 + Parquet 原始行。

    刻意不复用 lake.table.scan 或工作台的扫描代码，用来交叉核对查询结果。
    """
    with open(
        os.path.join(lake_dir, "metadata", "versions", f"v{version:06d}.json")
    ) as f:
        m = json.load(f)
    deleted: dict[str, set[int]] = {}
    for meta in m["delete_files"].values():
        t = pq.read_table(os.path.join(lake_dir, meta["path"]))
        for fid, pos in zip(
            t.column("data_file_id").to_pylist(), t.column("pos").to_pylist()
        ):
            deleted.setdefault(fid, set()).add(pos)
    rows = []
    for fid in sorted(m["data_files"]):
        meta = m["data_files"][fid]
        t = pq.read_table(os.path.join(lake_dir, meta["path"]))
        dead = deleted.get(fid, set())
        for i in range(t.num_rows):
            if i not in dead:
                rows.append(
                    (t.column("row_id")[i].as_py(), t.column("value")[i].as_py())
                )
    return rows


def independently_join(features, labels, left=False):
    out = []
    for fid, fval in features:
        matched = [(lid, lval) for lid, lval in labels if lid == fid]
        if matched:
            out.extend((fid, fval, lval) for _lid, lval in matched)
        elif left:
            out.append((fid, fval, None))
    return out


def independently_group(features):
    counts, totals = {}, {}
    for rid, val in features:
        counts[rid] = counts.get(rid, 0) + 1
        totals[rid] = totals.get(rid, 0) + val
    return counts, totals


def lake_fingerprint(lake_dir: str) -> dict:
    out = {}
    for base, _dirs, files in os.walk(lake_dir):
        for name in sorted(files):
            p = os.path.join(base, name)
            with open(p, "rb") as f:
                out[os.path.relpath(p, lake_dir)] = hashlib.sha256(
                    f.read()
                ).hexdigest()
    return out


def build_lakes(root: str):
    features = LakeTable.create(os.path.join(root, "features"))
    write_shard(features, FEATURE_ROWS[:5])
    write_shard(features, FEATURE_ROWS[5:])
    labels = LakeTable.create(os.path.join(root, "labels"))
    write_shard(labels, LABEL_ROWS[:2])
    write_shard(labels, LABEL_ROWS[2:])
    return features, labels


# ------------------------------------------------------------------ 子进程
def mutate_child(root: str, started, q):
    t = LakeTable.open(root)
    try:
        before = t.current_version()
        q.put(("ready", before))
        started.wait(120)
        for i in range(4):
            t.append([(f"late-{i}", 1000 + i)])
        commit_plan(t, create_plan(t, selected=sorted(t.manifest()["data_files"])))
        info = collect(t, staging_max_age_seconds=0.0)
        q.put(
            (
                "ok",
                {
                    "version_before": before,
                    "version_after": t.current_version(),
                    "num_files": len(t.manifest()["data_files"]),
                    "protected": info["protected_versions"],
                    "removed_files": len(info["removed_files"]),
                },
            )
        )
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def main() -> int:
    root = tempfile.mkdtemp(prefix="lake_workbench_demo_")
    print(f"工作目录: {root}")
    features, labels = build_lakes(root)
    fv = features.current_version()
    lv = labels.current_version()
    bindings = {"features": (features.root, fv), "labels": (labels.root, lv)}

    # ------------------------------------------------------------------ 1
    banner("1. 特征表与标签表按样本键对齐（对照独立期望行集）")
    f_rows = independent_visible_rows(features.root, fv)
    l_rows = independent_visible_rows(labels.root, lv)
    print(f"特征可见行 {len(f_rows)} 行，标签可见行 {len(l_rows)} 行")
    print("特征:", f_rows)
    print("标签:", l_rows)

    inner = query_arrow(
        bindings,
        "SELECT f.row_id, f.value, l.value AS label "
        "FROM features f JOIN labels l ON l.row_id = f.row_id "
        "ORDER BY f.row_id, f.value",
    )
    expected_inner = sorted(independently_join(f_rows, l_rows))
    got_inner = sorted(
        (r["row_id"], r["value"], r["label"]) for r in inner.to_pylist()
    )
    print("内连接结果:", got_inner)
    print("期望行集  :", expected_inner)
    assert got_inner == expected_inner
    print("内连接：与独立期望行集一致 ✓")

    left = query_arrow(
        bindings,
        "SELECT f.row_id, f.value, l.value AS label "
        "FROM features f LEFT JOIN labels l ON l.row_id = f.row_id",
    )
    got_left = sorted(
        (r["row_id"], r["value"], r["label"]) for r in left.to_pylist()
    )
    expected_left = sorted(
        independently_join(f_rows, l_rows, left=True),
        key=lambda r: (r[0], r[1], -1 if r[2] is None else r[2]),
    )
    got_left_sorted = sorted(
        got_left, key=lambda r: (r[0], r[1], -1 if r[2] is None else r[2])
    )
    print(f"左连接 {len(got_left)} 行，其中无标签 {sum(1 for r in got_left if r[2] is None)} 行")
    assert got_left_sorted == expected_left
    print("左连接：保留无标签特征行，与独立期望行集一致 ✓")

    grouped = query_arrow(
        bindings,
        "SELECT row_id, COUNT(*) AS c, SUM(value) AS s FROM features "
        "WHERE value >= 20 GROUP BY row_id ORDER BY row_id",
    )
    counts, totals = independently_group(r for r in f_rows if r[1] >= 20)
    expected_group = sorted((rid, c, totals[rid]) for rid, c in counts.items())
    got_group = sorted(
        (r["row_id"], r["c"], int(r["s"])) for r in grouped.to_pylist()
    )
    print("分组结果:", got_group)
    print("期望分组:", expected_group)
    assert got_group == expected_group
    print("过滤 + 分组统计：与独立期望行集一致 ✓")

    # ------------------------------------------------------------------ 2
    banner("2. 同一物理湖表登记新旧两个版本，对照修订前后标签")
    old_label_v = labels.current_version()
    write_shard(labels, NEW_LABELS)
    new_label_v = labels.current_version()
    print(f"标签表：修订前 v{old_label_v}，修订后 v{new_label_v}（追加 {len(NEW_LABELS)} 行）")
    old_rows = independent_visible_rows(labels.root, old_label_v)
    new_rows = independent_visible_rows(labels.root, new_label_v)

    contrast = query(
        {
            "features": (features.root, features.current_version()),
            "labels_before": (labels.root, old_label_v),
            "labels_after": (labels.root, new_label_v),
        },
        "SELECT f.row_id, f.value, old.value AS before, new.value AS after_ "
        "FROM features f "
        "LEFT JOIN labels_before old ON old.row_id = f.row_id "
        "LEFT JOIN labels_after new ON new.row_id = f.row_id "
        "ORDER BY f.row_id, f.value",
    )
    print("本次实际读取版本:")
    for s in contrast.sources:
        print(f"  {s['name']:>14} -> {s['lake']} @ v{s['version']}")
    print("对照结果（row_id, feature, 修订前标签, 修订后标签）:")
    for row in contrast.to_pylist():
        print("  ", (row["row_id"], row["value"], row["before"], row["after_"]))

    # 独立核对：s-04 旧版本没有标签、新版本有；s-01 旧值仍在、新值追加
    s04 = [r for r in contrast.to_pylist() if r["row_id"] == "s-04"]
    assert {r["before"] for r in s04} == {None}
    assert {r["after_"] for r in s04} == {401}
    s01 = [r for r in contrast.to_pylist() if r["row_id"] == "s-01"]
    assert {r["before"] for r in s01} == {100}
    assert {r["after_"] for r in s01} == {100, 101}
    # 与独立可见行集一致
    for row in contrast.to_pylist():
        assert row["before"] in ([v for (i, v) in old_rows if i == row["row_id"]] or [None])
        assert row["after_"] in ([v for (i, v) in new_rows if i == row["row_id"]] or [None])
    assert contrast.sources[1]["lake"] == contrast.sources[2]["lake"]
    print("同一目录两个版本并存、对照修订前后标签 ✓")

    # ------------------------------------------------------------------ 3
    banner("3. 取数途中子进程追加 + 压实 + 回收：读者保护与结果稳定性")
    # 用一个新的特征湖表，多分片，便于制造交错
    feats = LakeTable.create(os.path.join(root, "features_concurrent"))
    for chunk in (FEATURE_ROWS[:3], FEATURE_ROWS[3:6], FEATURE_ROWS[6:]):
        write_shard(feats, chunk)
    cv = feats.current_version()
    expected_rows = independent_visible_rows(feats.root, cv)
    print(f"特征湖表 v{cv}，{len(feats.manifest()['data_files'])} 个数据文件，"
          f"{len(expected_rows)} 可见行")

    started = CTX.Event()
    q = CTX.Queue()
    child = CTX.Process(target=mutate_child, args=(feats.root, started, q))
    child.start()
    ready = q.get(timeout=120)
    assert ready[0] == "ready", ready

    from lake.workbench import engine as engine_mod

    real = engine_mod._iter_visible_batches
    first_batch = {"done": False}

    def slow_scan(table, manifest, batch_size):
        for batch in real(table, manifest, batch_size):
            if not first_batch["done"]:
                first_batch["done"] = True
                started.set()          # 通知子进程开始追加/压实/回收
            time.sleep(0.05)           # 拉长取数窗口
            yield batch

    engine_mod._iter_visible_batches = slow_scan
    try:
        result = query(
            {"features": (feats.root, cv)},
            "SELECT row_id, value FROM features ORDER BY row_id, value",
        )
    finally:
        engine_mod._iter_visible_batches = real

    state = q.get(timeout=120)
    child.join(timeout=120)
    assert state[0] == "ok", state
    info = state[1]
    print(f"子进程：HEAD v{info['version_before']} -> v{info['version_after']}，"
          f"压实后 {info['num_files']} 个文件，GC 删除 {info['removed_files']} 个旧文件")
    print(f"GC 看到的受保护版本: {info['protected']}")
    assert info["version_before"] == cv and info["version_after"] > cv
    assert cv in info["protected"]
    assert info["num_files"] <= 2 and info["removed_files"] > 0

    got = sorted((r["row_id"], r["value"]) for r in result.to_pylist())
    print(f"查询返回 {len(got)} 行；来源版本 {[s['version'] for s in result.sources]}")
    assert got == sorted(expected_rows)
    assert all(not rid.startswith("late-") for rid, _ in got)
    assert os.listdir(feats.leases_dir) == []
    print("结果仍来自登记的旧版本；查询结束无残留租约 ✓")

    # ------------------------------------------------------------------ 4
    banner("4. 空结果保留列名与类型")
    empty = query_arrow(
        bindings,
        "SELECT f.row_id, f.value, l.value AS label FROM features f "
        "JOIN labels l ON l.row_id = f.row_id WHERE f.value > 1000",
    )
    print(f"空结果: {empty.num_rows} 行，列名 {empty.column_names}")
    print(f"schema: {empty.schema}")
    assert empty.num_rows == 0
    assert empty.column_names == ["row_id", "value", "label"]
    assert empty.schema.field("value").type == pa.int64()
    csv_text = query(
        bindings, "SELECT row_id FROM features WHERE value > 1000"
    ).to_csv()
    print(f"CSV（空结果仍带表头）: {csv_text.strip()}")
    assert '"row_id"' in csv_text.splitlines()[0]

    empty_lake = LakeTable.create(os.path.join(root, "empty_lake"))
    zero = query_arrow(
        {"empty": (empty_lake.root, 0)},
        "SELECT row_id, SUM(value) AS s FROM empty GROUP BY row_id",
    )
    print(f"版本 0（无任何数据文件）: {zero.num_rows} 行，schema {zero.schema}")
    assert zero.column_names == ["row_id", "s"]
    print("空结果/空版本都保留列名与类型 ✓")

    # ------------------------------------------------------------------ 5
    banner("5. 逐项拒绝，并核对湖表未被改动")
    before_fp = lake_fingerprint(features.root), lake_fingerprint(labels.root)
    before_versions = (features.current_version(), labels.current_version())
    cases = [
        ("写语句（DELETE）", "DELETE FROM features"),
        ("写语句（INSERT）", "INSERT INTO features VALUES ('x', 1)"),
        ("写语句（建表）", "CREATE TABLE t AS SELECT * FROM features"),
        ("直接读本地文件", "SELECT * FROM read_parquet('/etc/passwd')"),
        ("直接读数据文件", "SELECT * FROM 'data/data-x.parquet'"),
        ("联网", "SELECT * FROM read_csv_auto('http://127.0.0.1:9/x.csv')"),
        ("外部数据源", "SELECT * FROM read_json_auto('s3://bucket/x.json')"),
        ("目录函数", "SELECT * FROM duckdb_settings()"),
        ("未知名称", "SELECT * FROM orders"),
        ("未知列", "SELECT nope FROM features"),
        ("多语句", "SELECT 1; SELECT 2"),
        ("语法错误", "SELECT FROM"),
    ]
    for label, sql in cases:
        try:
            query(bindings, sql)
        except WorkbenchError as e:
            print(f"  拒绝[{e.kind}] {label}: {str(e)[:72]}")
            assert e.kind in ("permission", "query", "syntax")
        else:
            raise AssertionError(f"应拒绝: {label}")

    bad_version = dict(bindings, features=(features.root, fv + 100))
    try:
        query(bad_version, "SELECT 1")
    except WorkbenchError as e:
        print(f"  拒绝[{e.kind}] 不存在的版本: {str(e)[:72]}")
        assert e.kind == "version"
    else:
        raise AssertionError("应拒绝不存在的版本")

    after_fp = lake_fingerprint(features.root), lake_fingerprint(labels.root)
    after_versions = (features.current_version(), labels.current_version())
    print(f"湖表版本: {before_versions} -> {after_versions}")
    print(f"全部文件内容指纹未变化: {before_fp == after_fp}")
    assert after_fp == before_fp
    assert after_versions == before_versions
    assert os.listdir(features.leases_dir) == []
    assert os.listdir(labels.leases_dir) == []
    print("拒绝不修改湖表文件/版本/清单，也不留租约 ✓")

    banner("全部验收场景通过")
    shutil.rmtree(root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
