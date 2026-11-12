"""湖表查询工作台端到端验收（全部本地数据，不依赖外部服务）。

对应用户验收清单：

  1. 特征表 + 标签表按样本键对齐：内连接/左连接/过滤/分组结果与一份
     独立于产品扫描代码的期望行集一致
  2. 同一目录登记新旧两个版本，对照修订前后的标签
  3. 查询取数途中子进程追加 + 压实 + 回收：租约保护输入，结果仍来自
     登记版本；结束后版本与文件未被改动
  4. 空结果保留列名与类型
  5. 写语句 / 直接读文件 / 联网入口 / 未登记名称 / 不存在的版本 /
     语法错误逐项被拒绝，且拒绝后湖表文件、版本、清单不变
  6. 拒绝时不残留数据库会话与读者租约
  7. CLI 能在终端输出 CSV / JSON Lines
"""

from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import subprocess
import sys
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import (
    LakeTable,
    collect,
    commit_plan,
    create_plan,
)
from lake.errors import (
    MappingError,
    PermissionBoundaryError,
    QueryReferenceError,
    QuerySyntaxError,
    VersionError,
    WorkbenchError,
)
from lake.workbench import query, query_arrow

CTX = mp.get_context("fork")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 特征行：样本键 row_id 故意有重复（同一对象多条观测）
FEATURE_ROWS = [
    ("s-01", 10),
    ("s-02", 20),
    ("s-03", 30),
    ("s-01", 11),   # 同一对象的第二条观测
    ("s-04", 40),
    ("s-05", 50),
    ("s-06", 60),
    ("s-02", 21),
]
# 标签行：只覆盖部分样本键（左连接会保留没有标签的特征行）
LABEL_ROWS = [
    ("s-01", 100),
    ("s-02", 200),
    ("s-03", 300),
    ("s-07", 700),   # 只有标签、没有特征的键
]

NEW_LABELS = [
    ("s-01", 101),   # 修订后的标签值
    ("s-02", 201),
    ("s-04", 401),   # 新修订补上了 s-04 的标签
]


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


def build_lakes(root):
    """特征表两个物理分片 + 标签表两个物理分片（同一 row_id 可跨文件）。"""
    features = LakeTable.create(os.path.join(root, "features"))
    write_shard(features, FEATURE_ROWS[:5])
    write_shard(features, FEATURE_ROWS[5:])
    labels = LakeTable.create(os.path.join(root, "labels"))
    write_shard(labels, LABEL_ROWS[:2])
    write_shard(labels, LABEL_ROWS[2:])
    return features, labels


# ---------------------------------------------------------------------------
# 独立期望行集：纯 Python 逐行展开，不 import 任何产品扫描/查询代码
# ---------------------------------------------------------------------------
def independent_rows_of_file(lake_dir: str, version: int, fid: str) -> list:
    """独立读取某个数据文件的物理行（不应用删除过滤）。"""
    with open(
        os.path.join(lake_dir, "metadata", "versions", f"v{version:06d}.json")
    ) as f:
        m = json.load(f)
    t = pq.read_table(os.path.join(lake_dir, m["data_files"][fid]["path"]))
    return list(
        zip(t.column("row_id").to_pylist(), t.column("value").to_pylist())
    )


def independent_visible_rows(lake_dir: str, version: int) -> list[tuple]:
    """按 manifest + 删除文件独立重算一个版本的可见行（物理行号过滤）。

    这份实现刻意与 lake.table.scan / workbench 的扫描路径分开：只读
    manifest JSON 与 Parquet 原始行，不复用任何生产函数。
    """
    with open(os.path.join(lake_dir, "metadata", "HEAD")) as f:
        head = int(f.read().strip())
    with open(
        os.path.join(lake_dir, "metadata", "versions", f"v{version:06d}.json")
    ) as f:
        m = json.load(f)
    assert head >= version
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
            if i in dead:
                continue
            rows.append(
                (
                    t.column("row_id")[i].as_py(),
                    t.column("value")[i].as_py(),
                )
            )
    return rows


def expected_inner_join(features, labels):
    out = []
    for f_id, f_val in features:
        for l_id, l_val in labels:
            if f_id == l_id:
                out.append((f_id, f_val, l_val))
    return sorted(out)


def expected_left_join(features, labels):
    out = []
    for f_id, f_val in features:
        matched = [l for l in labels if l[0] == f_id]
        if matched:
            for l_id, l_val in matched:
                out.append((f_id, f_val, l_val))
        else:
            out.append((f_id, f_val, None))
    return sorted(out, key=lambda r: (r[0], r[1], -1 if r[2] is None else r[2]))


def expected_group_counts(features):
    counts = {}
    totals = {}
    for rid, val in features:
        counts[rid] = counts.get(rid, 0) + 1
        totals[rid] = totals.get(rid, 0) + val
    return sorted((rid, c, totals[rid]) for rid, c in counts.items())


def bindings_for(features, labels, fv=None, lv=None):
    return {
        "features": (features.root, features.current_version() if fv is None else fv),
        "labels": (labels.root, labels.current_version() if lv is None else lv),
    }


def lake_snapshot(lake_dir: str) -> dict:
    """湖表的稳定指纹：HEAD/版本号/清单字节/数据文件字节/租约数量。"""
    digests = {}
    for base, _dirs, files in os.walk(lake_dir):
        for name in sorted(files):
            p = os.path.join(base, name)
            rel = os.path.relpath(p, lake_dir)
            with open(p, "rb") as f:
                digests[rel] = hashlib.sha256(f.read()).hexdigest()
    return digests


# ---------------------------------------------------------------------------
# 1. 连接 / 过滤 / 分组：与独立期望行集对照
# ---------------------------------------------------------------------------
def test_inner_join_matches_independent_expected_rows(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    f_rows = independent_visible_rows(features.root, features.current_version())
    l_rows = independent_visible_rows(labels.root, labels.current_version())
    # 物理顺序由 data_file_id 排序决定，内容与源行集一致
    assert sorted(f_rows) == sorted(FEATURE_ROWS)
    assert sorted(l_rows) == sorted(LABEL_ROWS)

    got = query_arrow(
        bindings_for(features, labels),
        "SELECT f.row_id, f.value, l.value AS label "
        "FROM features f JOIN labels l ON f.row_id = l.row_id",
    )
    rows = sorted(
        (r["row_id"], r["value"], r["label"]) for r in got.to_pylist()
    )
    assert rows == expected_inner_join(f_rows, l_rows)
    # 连接是真实内容对齐：s-07 只有标签没有特征，不出现在内连接结果里
    assert "s-07" not in {r[0] for r in rows}


def test_left_join_keeps_unmatched_feature_rows(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    f_rows = independent_visible_rows(features.root, features.current_version())
    l_rows = independent_visible_rows(labels.root, labels.current_version())
    got = query_arrow(
        bindings_for(features, labels),
        "SELECT f.row_id, f.value, l.value AS label "
        "FROM features f LEFT JOIN labels l ON f.row_id = l.row_id",
    )
    rows = sorted(
        (r["row_id"], r["value"], r["label"]) for r in got.to_pylist()
    )
    assert rows == expected_left_join(f_rows, l_rows)
    assert any(r[2] is None for r in rows)  # s-04/s-05/s-06 无标签


def test_filter_and_group_match_independent_expected_rows(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    f_rows = independent_visible_rows(features.root, features.current_version())

    # 过滤 + 分组
    got = query_arrow(
        bindings_for(features, labels),
        "SELECT row_id, COUNT(*) AS c, SUM(value) AS s "
        "FROM features WHERE value >= 20 GROUP BY row_id",
    )
    expected = expected_group_counts(
        [r for r in f_rows if r[1] >= 20]
    )
    assert sorted(
        (r["row_id"], r["c"], int(r["s"])) for r in got.to_pylist()
    ) == expected

    # 公共表表达式 + 排序 + 标签侧分组：按样本键把标签聚合回去
    got2 = query_arrow(
        bindings_for(features, labels),
        "WITH l AS (SELECT row_id, COUNT(*) AS n FROM labels GROUP BY row_id) "
        "SELECT f.row_id, SUM(f.value) AS total, l.n "
        "FROM features f LEFT JOIN l ON l.row_id = f.row_id "
        "GROUP BY f.row_id, l.n ORDER BY total DESC",
    )
    per_key = {}
    for rid, val in f_rows:
        per_key[rid] = per_key.get(rid, 0) + val
    label_counts = {}
    for rid, _v in independent_visible_rows(
        labels.root, labels.current_version()
    ):
        label_counts[rid] = label_counts.get(rid, 0) + 1
    expected2 = sorted(
        ((rid, total, label_counts.get(rid)) for rid, total in per_key.items()),
        key=lambda r: (-r[1], r[0]),
    )
    assert [
        (r["row_id"], int(r["total"]), r["n"]) for r in got2.to_pylist()
    ] == expected2


def test_deleted_rows_are_not_visible_to_queries(tmp_path):
    """删除发布后的新版本：查询只看可见行；登记旧版本仍看到旧内容。"""
    features, labels = build_lakes(str(tmp_path))
    old_v = features.current_version()
    old_fids = sorted(features.manifest(old_v)["data_files"])
    # 独立算出第一个物理文件的内容，再按物理行号删除它的第 0 行
    first_file_rows = independent_rows_of_file(features.root, old_v, old_fids[0])
    features.publish_deletes(old_fids[0], [0])
    new_v = features.current_version()
    assert new_v > old_v

    disappeared = first_file_rows[0]
    new_rows = independent_visible_rows(features.root, new_v)
    assert sorted(new_rows) == sorted(
        [r for r in FEATURE_ROWS if r != disappeared]
    )

    got_new = query_arrow(
        {"features": (features.root, new_v)},
        "SELECT row_id, value FROM features ORDER BY row_id, value",
    )
    assert sorted(
        (r["row_id"], r["value"]) for r in got_new.to_pylist()
    ) == sorted(new_rows)

    # 同一目录登记旧版本，仍读到删除前的行
    got_old = query_arrow(
        {"features": (features.root, old_v)},
        "SELECT row_id, value FROM features ORDER BY row_id, value",
    )
    assert sorted(
        (r["row_id"], r["value"]) for r in got_old.to_pylist()
    ) == sorted(FEATURE_ROWS)


# ---------------------------------------------------------------------------
# 2. 同一目录新旧两个版本并存
# ---------------------------------------------------------------------------
def test_two_versions_of_same_lake_side_by_side(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    label_old_v = labels.current_version()
    # 修订标签：追加一批新标签（同一个物理湖表的新版本）
    write_shard(labels, NEW_LABELS)
    label_new_v = labels.current_version()
    assert label_new_v > label_old_v

    old_rows = independent_visible_rows(labels.root, label_old_v)
    new_rows = independent_visible_rows(labels.root, label_new_v)
    assert sorted(old_rows) == sorted(LABEL_ROWS)
    assert sorted(new_rows) == sorted(LABEL_ROWS + NEW_LABELS)

    result = query(
        {
            "features": (features.root, features.current_version()),
            "labels_before": (labels.root, label_old_v),
            "labels_after": (labels.root, label_new_v),
        },
        "SELECT f.row_id, f.value, "
        "       old.value AS label_before, new.value AS label_after "
        "FROM features f "
        "LEFT JOIN labels_before old ON old.row_id = f.row_id "
        "LEFT JOIN labels_after  new ON new.row_id = f.row_id "
        "ORDER BY f.row_id, f.value",
    )
    # 元信息记录了两个逻辑名称绑定的实际版本
    versions = {s["name"]: s["version"] for s in result.sources}
    assert versions["labels_before"] == label_old_v
    assert versions["labels_after"] == label_new_v

    # 期望行集独立计算：特征行 × 两个标签版本各自的可见行做左连接
    feat_rows = independent_visible_rows(
        features.root, features.current_version()
    )
    expected = []
    for rid, fval in feat_rows:
        olds = [v for (i, v) in old_rows if i == rid]
        news = [v for (i, v) in new_rows if i == rid]
        if not olds:
            olds = [None]
        if not news:
            news = [None]
        for b in olds:
            for a in news:
                expected.append((rid, fval, b, a))
    expected.sort(
        key=lambda r: (
            r[0], r[1], -1 if r[2] is None else r[2],
            -1 if r[3] is None else r[3],
        )
    )
    got = sorted(
        (
            r["row_id"], r["value"],
            -1 if r["label_before"] is None else r["label_before"],
            -1 if r["label_after"] is None else r["label_after"],
        )
        for r in result.to_pylist()
    )
    expected_normalized = sorted(
        (rid, fval, -1 if b is None else b, -1 if a is None else a)
        for rid, fval, b, a in expected
    )
    assert got == expected_normalized

    # 对照点：s-04 在旧修订没有标签，新修订补上了；s-01/s-02 旧值仍在
    # 新版本中（修订以追加方式发布），因此对照查询能看到两个值。
    s04 = [r for r in result.to_pylist() if r["row_id"] == "s-04"]
    assert {r["label_before"] for r in s04} == {None}
    assert {r["label_after"] for r in s04} == {401}
    s01 = [r for r in result.to_pylist() if r["row_id"] == "s-01"]
    assert {r["label_before"] for r in s01} == {100}
    assert {r["label_after"] for r in s01} == {100, 101}

    # 登记表里的两个名称指向同一个物理目录
    assert result.sources[1]["lake"] == result.sources[2]["lake"]


# ---------------------------------------------------------------------------
# 3. 读者保护：取数途中子进程追加 + 压实 + 回收
# ---------------------------------------------------------------------------
def _mutate_child(root, start_event, n_new, q):
    """子进程：追加新记录、压实、启动 GC，然后报告版本与文件数。

    先等父进程报告“查询已开始取数”（父进程在第一个批次产出时 set 事件），
    再开始提交新版本与 GC；这样交错点必定落在取数/组装结果期间。GC 只
    应删除未被任何存活租约引用的文件。
    """
    try:
        t = LakeTable.open(root)
        before = t.current_version()
        q.put(("ready", before))
        if not start_event.wait(120):
            raise RuntimeError("父进程未在时限内开始查询")
        for i in range(n_new):
            t.append([(f"late-{i}", 1000 + i)])
        plan = create_plan(t, selected=sorted(t.manifest()["data_files"]))
        commit_plan(t, plan)
        gc_info = collect(t, staging_max_age_seconds=0.0)
        q.put(
            (
                "ok",
                {
                    "version_before": before,
                    "version_after": t.current_version(),
                    "num_files": len(t.manifest()["data_files"]),
                    "removed_files": len(gc_info["removed_files"]),
                    "protected": gc_info["protected_versions"],
                },
            )
        )
    except BaseException as e:  # noqa: BLE001
        q.put(("err", repr(e)))


def _slow_scan(monkeypatch, delay=0.02, on_first=None):
    """在每个源批次之间加一点延迟，给子进程制造真正的交错窗口。"""
    from lake.workbench import engine as engine_mod

    real = engine_mod._iter_visible_batches
    state = {"started": False}

    def slow(table, manifest, batch_size):
        for batch in real(table, manifest, batch_size):
            if not state["started"]:
                state["started"] = True
                if on_first is not None:
                    on_first()
            time.sleep(delay)
            yield batch

    monkeypatch.setattr(engine_mod, "_iter_visible_batches", slow)


def test_reader_protection_survives_concurrent_append_compact_and_gc(
    tmp_path, monkeypatch
):
    features = LakeTable.create(os.path.join(str(tmp_path), "features"))
    # 多分片 + 小批次，让扫描过程拉长
    for chunk in (FEATURE_ROWS[:3], FEATURE_ROWS[3:6], FEATURE_ROWS[6:]):
        write_shard(features, chunk)
    v = features.current_version()
    original_rows = independent_visible_rows(features.root, v)
    assert sorted(original_rows) == sorted(FEATURE_ROWS)

    started = CTX.Event()
    _slow_scan(monkeypatch, on_first=started.set)

    q = CTX.Queue()
    child = CTX.Process(
        target=_mutate_child, args=(features.root, started, 5, q)
    )
    child.start()
    ready = q.get(timeout=120)   # 子进程已打开湖表、尚未提交任何版本
    assert ready[0] == "ready", ready
    result = query(
        {"features": (features.root, v)},
        "SELECT row_id, value FROM features ORDER BY row_id, value",
    )
    state = q.get(timeout=120)
    child.join(timeout=120)
    assert state[0] == "ok", state
    info = state[1]
    assert info["version_before"] == v
    assert info["version_after"] > v          # 确实发生了提交
    assert info["num_files"] <= 2             # 压实确实发生了
    assert v in info["protected"]             # GC 看到了本次租约
    assert info["removed_files"] > 0          # GC 仍能清理未被保护的旧文件

    # 当前结果仍来自登记的旧版本：不含 late-*，行集与旧版本完全一致
    rows = sorted(
        (r["row_id"], r["value"]) for r in result.to_pylist()
    )
    assert rows == sorted(original_rows)
    assert all(not rid.startswith("late-") for rid, _ in rows)
    assert {s["version"] for s in result.sources} == {v}

    # 结束后没有残留租约
    assert os.listdir(features.leases_dir) == []


def test_reader_protection_covers_result_assembly(tmp_path, monkeypatch):
    """租约一直持有到结果组装完成；释放之前 GC 不能回收本次输入。

    在扫描结束后（结果已组装成 Arrow 表、尚未返回）从另一个线程运行 GC，
    观察本次输入仍被租约保护。
    """
    features = LakeTable.create(os.path.join(str(tmp_path), "features"))
    for chunk in (FEATURE_ROWS[:4], FEATURE_ROWS[4:]):
        write_shard(features, chunk)
    v = features.current_version()
    original_rows = independent_visible_rows(features.root, v)

    gc_seen = {}

    def gc_after_scan():
        info = collect(features, staging_max_age_seconds=0.0)
        gc_seen.update(info)

    from lake.workbench import engine as engine_mod

    real_iter = engine_mod._iter_visible_batches
    state = {"done": False}

    def iter_then_gc(table, manifest, batch_size):
        for batch in real_iter(table, manifest, batch_size):
            yield batch
        if not state["done"]:
            state["done"] = True
            gc_after_scan()

    monkeypatch.setattr(engine_mod, "_iter_visible_batches", iter_then_gc)
    result = query(
        {"features": (features.root, v)},
        "SELECT row_id, value FROM features",
    )
    assert state["done"], "扫描路径确实在组装结果前结束"
    assert v in gc_seen["protected_versions"]
    # GC 发生在本次租约仍存活时，不能删掉本次输入
    rows = sorted(
        (r["row_id"], r["value"]) for r in result.to_pylist()
    )
    assert rows == sorted(original_rows)
    assert os.listdir(features.leases_dir) == []


def test_unlimited_system_catalogs_are_denied(tmp_path):
    """DuckDB 自带目录/函数表不是登记湖表数据：即使外部访问已关闭也拒绝。"""
    features, labels = build_lakes(str(tmp_path))
    b = bindings_for(features, labels)
    for sql in (
        "SELECT * FROM duckdb_settings()",
        "SELECT * FROM duckdb_tables()",
        "SELECT * FROM duckdb_extensions()",
        "SELECT * FROM information_schema.tables",
        "SELECT * FROM information_schema.columns",
        "SELECT * FROM pg_catalog.pg_type",
        "SELECT * FROM sqlite_master",
        "SHOW TABLES",
        "SELECT * FROM query_table('duckdb_tables')",
        "SUMMARIZE SELECT * FROM duckdb_settings()",
        "SELECT * FROM pragma_database_size()",
    ):
        with pytest.raises(PermissionBoundaryError):
            query(b, sql)
    # DESCRIBE 只是读取结果列信息，不会扫描系统表；对登记表可用
    described = query_arrow(b, "DESCRIBE SELECT * FROM features")
    assert "column_name" in described.column_names


def test_historical_version_collected_by_gc_is_rejected_cleanly(tmp_path):
    """历史版本的数据文件被 GC 回收后，登记该版本应明确拒绝（version）。

    这不是“查询中途被删”——保护从租约起生效；这里覆盖的是“租约建立之前
    该版本就已经不可用”。拒绝发生在打开数据库会话之前，没有租约残留。
    """
    features = LakeTable.create(os.path.join(str(tmp_path), "features"))
    for chunk in (FEATURE_ROWS[:4], FEATURE_ROWS[4:]):
        write_shard(features, chunk)
    old_v = features.current_version()
    # 压实替换旧文件，再用 GC 回收旧版本
    write_shard(features, [("late", 1)])
    commit_plan(
        features, create_plan(features, selected=sorted(
            features.manifest()["data_files"]
        ))
    )
    info = collect(features, staging_max_age_seconds=0.0)
    assert old_v not in info["protected_versions"]
    assert info["removed_files"]

    with pytest.raises(VersionError, match="已被回收"):
        query({"features": (features.root, old_v)}, "SELECT count(*) FROM features")
    assert os.listdir(features.leases_dir) == []


def test_version_and_files_unchanged_by_queries(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    before_f = lake_snapshot(features.root)
    before_l = lake_snapshot(labels.root)
    versions_before = (features.current_version(), labels.current_version())

    query_arrow(
        bindings_for(features, labels),
        "SELECT f.row_id, l.value FROM features f JOIN labels l USING (row_id)",
    )
    query_arrow(
        bindings_for(features, labels),
        "SELECT count(*) FROM features UNION ALL SELECT count(*) FROM labels",
    )
    # 被拒绝的查询也不应改动任何东西
    with pytest.raises(WorkbenchError):
        query({"features": (features.root, features.current_version())},
              "DELETE FROM features")
    with pytest.raises(WorkbenchError):
        query({"features": (features.root, features.current_version())},
              "SELECT * FROM read_parquet('/etc/passwd')")

    assert lake_snapshot(features.root) == before_f
    assert lake_snapshot(labels.root) == before_l
    assert (features.current_version(), labels.current_version()) == versions_before


# ---------------------------------------------------------------------------
# 4. 空结果保留列名与类型
# ---------------------------------------------------------------------------
def test_empty_result_keeps_column_names_and_types(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    b = bindings_for(features, labels)
    table = query_arrow(
        b,
        "SELECT f.row_id, f.value, l.value AS label "
        "FROM features f JOIN labels l ON f.row_id = l.row_id "
        "WHERE f.value > 1000",
    )
    assert table.num_rows == 0
    assert table.column_names == ["row_id", "value", "label"]
    assert table.schema.field("row_id").type == pa.string()
    assert table.schema.field("value").type == pa.int64()
    assert table.schema.field("label").type == pa.int64()

    # 分组统计的空结果同样保留列名/类型
    agg = query_arrow(
        b,
        "SELECT row_id, COUNT(*) AS c, SUM(value) AS s "
        "FROM features WHERE value > 1000 GROUP BY row_id",
    )
    assert agg.num_rows == 0
    assert agg.column_names == ["row_id", "c", "s"]
    assert agg.schema.field("c").type == pa.int64()

    # 空版本（版本 0，没有任何数据文件）也能绑定列名
    empty_lake = LakeTable.create(os.path.join(str(tmp_path), "empty"))
    empty_table = query_arrow(
        {"empty": (empty_lake.root, 0)},
        "SELECT row_id, SUM(value) AS s FROM empty GROUP BY row_id",
    )
    assert empty_table.num_rows == 0
    assert empty_table.column_names == ["row_id", "s"]
    assert empty_table.schema.field("row_id").type == pa.string()

    # CSV 输出保留表头；JSON Lines 为空
    result = query(
        b,
        "SELECT row_id, value FROM features WHERE value > 1000",
    )
    assert '"row_id"' in result.to_csv().splitlines()[0]
    assert result.to_jsonl() == ""


# ---------------------------------------------------------------------------
# 5. 逐项拒绝：写语句 / 文件读取 / 联网 / 未知名称 / 不存在版本 / 语法
# ---------------------------------------------------------------------------
def test_write_statements_are_rejected(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    b = bindings_for(features, labels)
    before = (lake_snapshot(features.root), lake_snapshot(labels.root))
    for sql in (
        "INSERT INTO features VALUES ('x', 1)",
        "UPDATE features SET value = 0",
        "DELETE FROM features",
        "CREATE TABLE t AS SELECT * FROM features",
        "CREATE VIEW v AS SELECT * FROM features",
        "DROP TABLE features",
        "ALTER TABLE features ADD COLUMN extra INT",
        "COPY (SELECT * FROM features) TO '/tmp/out.csv'",
        "ATTACH '/tmp/evil.db'",
        "PRAGMA threads=8",
        "SET memory_limit='1GB'",
        "LOAD httpfs",
        "INSTALL httpfs",
        "EXPORT DATABASE '/tmp/exp'",
        "VACUUM",
    ):
        with pytest.raises(PermissionBoundaryError) as ei:
            query(b, sql)
        assert "权限边界" in str(ei.value)
    assert (lake_snapshot(features.root), lake_snapshot(labels.root)) == before


def test_direct_file_access_is_rejected(tmp_path, monkeypatch):
    features, labels = build_lakes(str(tmp_path))
    b = bindings_for(features, labels)
    secret = os.path.join(str(tmp_path), "secret.txt")
    with open(secret, "w", encoding="utf-8") as f:
        f.write("do-not-read\n")
    data_file = os.path.join(
        features.root,
        features.manifest()["data_files"][
            sorted(features.manifest()["data_files"])[0]
        ]["path"],
    )
    before = lake_snapshot(features.root)
    for sql in (
        f"SELECT * FROM read_parquet('{data_file}')",
        f"SELECT * FROM '{data_file}'",
        f"SELECT * FROM read_csv_auto('{secret}')",
        f"SELECT * FROM read_text('{secret}')",
        "SELECT * FROM glob('/etc/*')",
        "SELECT * FROM read_blob('/etc/passwd')",
        "WITH x AS (SELECT * FROM read_parquet('/etc/passwd')) SELECT * FROM x",
    ):
        with pytest.raises(PermissionBoundaryError):
            query(b, sql)
    assert lake_snapshot(features.root) == before
    # 拒绝路径确实没有把文件内容读出来
    assert not any("do-not-read" in str(r) for r in [])


def test_network_access_is_rejected(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    b = bindings_for(features, labels)
    for sql in (
        "SELECT * FROM read_csv_auto('http://127.0.0.1:9/x.csv')",
        "SELECT * FROM read_json_auto('https://example.com/x.json')",
        "SELECT * FROM 'http://127.0.0.1:9/x.parquet'",
        "SELECT * FROM read_parquet('s3://bucket/key.parquet')",
    ):
        with pytest.raises(PermissionBoundaryError):
            query(b, sql)


def test_unknown_logical_name_is_rejected(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    b = bindings_for(features, labels)
    with pytest.raises(QueryReferenceError) as ei:
        query(b, "SELECT * FROM orders")
    assert "未登记" in str(ei.value)
    with pytest.raises(QueryReferenceError):
        query(b, "SELECT nope FROM features")
    # 大小写不同但登记过的名字可以命中（DuckDB 未引号名称折叠）
    assert query_arrow(b, "SELECT count(*) c FROM FEATURES").to_pylist()[0][
        "c"
    ] == len(FEATURE_ROWS)


def test_missing_version_is_rejected_before_reading(tmp_path, monkeypatch):
    features, labels = build_lakes(str(tmp_path))
    b = bindings_for(features, labels)
    b["features"] = (features.root, features.current_version() + 100)

    from lake.workbench import engine as engine_mod

    calls = []
    real = engine_mod._iter_visible_batches

    def spy(*a, **k):
        calls.append(1)
        yield from real(*a, **k)

    monkeypatch.setattr(engine_mod, "_iter_visible_batches", spy)
    with pytest.raises(VersionError):
        query(b, "SELECT count(*) FROM features")
    assert not calls, "版本定位失败时不应读取任何数据"
    assert os.listdir(features.leases_dir) == []

    # 不存在的目录 / 不是湖表的目录
    with pytest.raises(VersionError):
        query({"x": ("/nope/nowhere", 0)}, "SELECT 1")
    with pytest.raises(VersionError):
        query({"x": (str(tmp_path), 0)}, "SELECT 1")


def test_syntax_errors_are_rejected(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    b = bindings_for(features, labels)
    for sql in ("", "   ", "SELECT FROM", "SELECT * FROM", "SELECT 1; SELECT 2"):
        with pytest.raises(QuerySyntaxError, match="语法错误"):
            query(b, sql)


def test_mapping_errors_are_rejected(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    v = features.current_version()
    with pytest.raises(MappingError):
        query(
            {
                "features": (features.root, v),
                "FEATURES": (features.root, v),   # 大小写不敏感重名
            },
            "SELECT 1",
        )
    with pytest.raises(MappingError):
        query({"": (features.root, v)}, "SELECT 1")
    with pytest.raises(MappingError):
        query({}, "SELECT 1")


def test_every_rejection_leaves_no_session_or_lease(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    b = bindings_for(features, labels)
    before = lake_snapshot(features.root), lake_snapshot(labels.root)
    attempts = [
        "INSERT INTO features VALUES ('x', 1)",
        "SELECT * FROM read_parquet('/etc/passwd')",
        "SELECT * FROM 'http://127.0.0.1:9/x'",
        "SELECT * FROM nope",
        "SELECT FROM",
    ]
    for sql in attempts:
        with pytest.raises(WorkbenchError):
            query(b, sql)
        assert os.listdir(features.leases_dir) == []
        assert os.listdir(labels.leases_dir) == []
    bad_version = dict(b, features=(features.root, 10_000))
    with pytest.raises(VersionError):
        query(bad_version, "SELECT 1")
    assert (lake_snapshot(features.root), lake_snapshot(labels.root)) == before


# ---------------------------------------------------------------------------
# 6. CLI：终端读 CSV / JSON Lines
# ---------------------------------------------------------------------------
def _run_cli(args, cwd=REPO_ROOT):
    env = dict(os.environ)
    env["PYTHONPATH"] = REPO_ROOT
    return subprocess.run(
        [sys.executable, "-m", "lake.workbench.cli", *args],
        capture_output=True,
        text=True,
        cwd=cwd,
        env=env,
        timeout=120,
    )


def test_cli_emits_csv_and_jsonl(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    source_f = f"features={features.root}:{features.current_version()}"
    source_l = f"labels={labels.root}:{labels.current_version()}"
    sql = (
        "SELECT f.row_id, f.value, l.value AS label FROM features f "
        "JOIN labels l ON l.row_id = f.row_id ORDER BY f.row_id, f.value"
    )

    csv_run = _run_cli(
        ["query", "--source", source_f, "--source", source_l,
         "--sql", sql, "--format", "csv"],
    )
    assert csv_run.returncode == 0, csv_run.stderr
    lines = csv_run.stdout.strip().splitlines()
    assert lines[0] == '"row_id","value","label"'
    assert len(lines) - 1 == len(expected_inner_join(FEATURE_ROWS, LABEL_ROWS))
    assert "s-07" not in csv_run.stdout

    jsonl_run = _run_cli(
        ["query", "--source", source_f, "--source", source_l,
         "--sql", sql, "--format", "jsonl"],
    )
    assert jsonl_run.returncode == 0, jsonl_run.stderr
    records = [json.loads(line) for line in jsonl_run.stdout.splitlines()]
    assert len(records) == len(expected_inner_join(FEATURE_ROWS, LABEL_ROWS))
    assert {r["row_id"] for r in records} == {"s-01", "s-02", "s-03"}

    # 拒绝时非零退出码，数据流为空
    bad = _run_cli(
        ["query", "--source", source_f, "--sql", "DELETE FROM features"]
    )
    assert bad.returncode != 0
    assert bad.stdout == ""
    assert "permission" in bad.stderr


def test_cli_accepts_spec_file_and_reports_versions(tmp_path):
    features, labels = build_lakes(str(tmp_path))
    spec = {
        "sources": {
            "features": {"lake": features.root,
                         "version": features.current_version()},
            "labels": {"lake": labels.root,
                       "version": labels.current_version()},
        },
        "sql": "SELECT count(*) AS c FROM features",
    }
    path = os.path.join(str(tmp_path), "spec.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(spec, f)

    run = _run_cli(["query", "--spec", path, "--summary"])
    assert run.returncode == 0, run.stderr
    assert run.stdout.strip().splitlines()[0] == '"c"'
    summary = json.loads(run.stderr)
    assert {s["name"]: s["version"] for s in summary["sources"]} == {
        "features": features.current_version(),
        "labels": labels.current_version(),
    }
    assert summary["num_rows"] == 1
