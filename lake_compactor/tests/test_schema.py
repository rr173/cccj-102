"""字段身份与结构演进的端到端校验。

全部用例真实读写 Parquet：模拟生产者滚动升级（新增可空列 → 改名 →
int32 无损扩宽 int64），新旧物理格式的文件并存，逐版本核对列名、
Arrow 类型、null 位置与全部行值；压实混合格式文件后物理列符合当前
定义；不兼容变更、错误类型输入、结构发布与旧结构写者竞争、发布期间
中断、无结构元数据的旧目录等场景逐一核验，每次拒绝都检查结构、版本
与数据没有变化。运行：python3 -m pytest tests/test_schema.py -v
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lake import (
    LakeTable,
    SchemaConflictError,
    SchemaError,
    SchemaEvolutionError,
    SchemaValidationError,
    commit_plan,
    create_plan,
)

CTX = mp.get_context("fork")

# 滚动升级全程使用的字段定义（id 稳定，名字/类型随版本演进）
ROW_ID = {"id": 1, "name": "row_id", "type": "string", "nullable": False}
SCORE32 = {"id": 2, "name": "score", "type": "int32", "nullable": True}
TAG = {"id": 3, "name": "tag", "type": "string", "nullable": True}
CONF32 = {"id": 2, "name": "confidence", "type": "int32", "nullable": True}
CONF64 = {"id": 2, "name": "confidence", "type": "int64", "nullable": True}


def scan_rows(scan: pa.Table) -> list[str]:
    """扫描结果的规范化行集合（与物理顺序无关，逐值精确比较）。"""
    return sorted(json.dumps(r, sort_keys=True) for r in scan.to_pylist())


def snapshot(t: LakeTable) -> dict:
    """表状态快照：版本、当前结构、结构历史、文件条目、全部可见行、游标。

    每个被拒绝的操作之后都要核对快照逐字节不变。
    """
    m = t.manifest()
    return {
        "version": t.current_version(),
        "schema": t.current_schema(),
        "history": t.schema_history(),
        "files": {k: dict(v) for k, v in m["data_files"].items()},
        "deletes": {k: dict(v) for k, v in m["delete_files"].items()},
        "rows": scan_rows(t.scan()),
        "cursors": t.ingest_cursors(),
    }


def physical_schema(t: LakeTable, data_file_id: str) -> pa.Schema:
    meta = t.manifest()["data_files"][data_file_id]
    return pq.read_schema(os.path.join(t.root, meta["path"]))


def msg(source, partition, offset, row_id, value, fields=None):
    m = {
        "source": source,
        "partition": partition,
        "offset": offset,
        "row_id": row_id,
        "value": value,
    }
    if fields is not None:
        m["fields"] = fields
    return m


# ---------------------------------------------------------------------------
# 1. 生产者滚动升级：新增列 -> 改名 -> 无损扩宽，逐版本核对
# ---------------------------------------------------------------------------
def test_rolling_upgrade_add_rename_widen(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root, schema=[ROW_ID, SCORE32])
    assert t.current_schema()["version"] == 1
    assert t.current_schema()["fields"] == [
        {"id": 1, "name": "row_id", "type": "string", "nullable": False},
        {"id": 2, "name": "score", "type": "int32", "nullable": True},
    ]

    # —— 旧格式数据开始写入（结构 v1：row_id / score int32）——
    f1 = t.append_rows(
        [
            {"row_id": "a", "score": 10},
            {"row_id": "b", "score": None},   # 可空列显式 null
            {"row_id": "c", "score": 30},
        ],
        schema_version=1,
    )
    v1 = t.current_version()
    r = t.ingest_batch(
        "ing-1",
        [
            msg("s", 0, 0, "d", 40),
            msg("s", 0, 1, "e", 50),
        ],
    )
    assert r["num_rows"] == 2
    v2 = t.current_version()

    s = t.scan()
    assert s.column_names == ["row_id", "score"]
    assert s.schema.field("row_id").type == pa.string()
    assert s.schema.field("score").type == pa.int32()
    assert scan_rows(s) == scan_rows(
        pa.table(
            {
                "row_id": ["a", "b", "c", "d", "e"],
                "score": pa.array([10, None, 30, 40, 50], pa.int32()),
            }
        )
    )

    # —— 滚动升级 1：新增可空列 tag（结构 v2），新旧文件并存 ——
    ev = t.evolve_schema([ROW_ID, SCORE32, TAG])
    assert ev["changed"] is True and ev["schema_version"] == 2
    v_add = ev["version"]
    f2 = t.append_rows(
        [
            {"row_id": "f", "score": 60, "tag": "new"},
            {"row_id": "g", "score": 70},                  # tag 缺省 -> null
        ],
        schema_version=2,
    )
    v3 = t.current_version()

    s = t.scan()
    assert s.column_names == ["row_id", "score", "tag"]
    by_id = {r["row_id"]: r for r in s.to_pylist()}
    # null 位置精确核对：旧文件（v1 写入）的 5 行 tag 全为 null
    assert all(by_id[k]["tag"] is None for k in ["a", "b", "c", "d", "e"])
    assert by_id["f"]["tag"] == "new" and by_id["g"]["tag"] is None
    assert by_id["b"]["score"] is None  # 原本就是 null 的值保持 null

    # 历史版本读取：当时生效的列定义（v1 还没有 tag）
    s1 = t.scan(v1)
    assert s1.column_names == ["row_id", "score"]
    assert s1.schema.field("score").type == pa.int32()
    assert scan_rows(s1) == scan_rows(
        pa.table({"row_id": ["a", "b", "c"], "score": pa.array([10, None, 30], pa.int32())})
    )
    # 加列提交当时：结构已是 v2（有 tag），但还没有 v2 文件 -> 全 null
    sadd = t.scan(v_add)
    assert sadd.column_names == ["row_id", "score", "tag"]
    assert all(r["tag"] is None for r in sadd.to_pylist())

    # 物理格式核对：旧文件仍是 (row_id, score:int32)，新文件带 tag；
    # 文件条目如实记录各自的结构版本
    m = t.manifest()
    assert m["data_files"][f1]["schema_version"] == 1
    assert m["data_files"][f2]["schema_version"] == 2
    phys1 = physical_schema(t, f1)
    assert phys1.names == ["row_id", "score"]
    assert phys1.field("score").type == pa.int32()
    assert physical_schema(t, f2).names == ["row_id", "score", "tag"]

    # —— 滚动升级 2：改名 score -> confidence（id 2 不变）——
    ev = t.evolve_schema([ROW_ID, CONF32, TAG])
    assert ev["schema_version"] == 3
    v_rename = ev["version"]
    s = t.scan()
    assert s.column_names == ["row_id", "confidence", "tag"]
    assert s.schema.field("confidence").type == pa.int32()  # 尚未扩宽
    # 全部行值逐值不变（改名只是逻辑映射变化）
    assert {r["row_id"]: r["confidence"] for r in s.to_pylist()} == {
        "a": 10, "b": None, "c": 30, "d": 40, "e": 50, "f": 60, "g": 70,
    }
    # 旧文件物理列仍叫 score（历史数据不重写）
    assert physical_schema(t, f1).names == ["row_id", "score"]
    # 改名后摄取仍按字段 id 2 落列（消息里的 value -> confidence）
    t.ingest_batch("ing-2", [msg("s", 0, 2, "h", 80)])
    assert {r["row_id"]: r["confidence"] for r in t.scan().to_pylist()}["h"] == 80

    # —— 滚动升级 3：int32 无损扩宽 int64 ——
    ev = t.evolve_schema([ROW_ID, CONF64, TAG])
    assert ev["schema_version"] == 4
    s = t.scan()
    assert s.column_names == ["row_id", "confidence", "tag"]
    assert s.schema.field("confidence").type == pa.int64()
    # 旧物理 int32 文件的值无损放大，逐值不变
    assert {r["row_id"]: r["confidence"] for r in s.to_pylist()} == {
        "a": 10, "b": None, "c": 30, "d": 40, "e": 50, "f": 60, "g": 70, "h": 80,
    }
    # 历史版本仍是当时的类型：v3 时是 score:int32，改名提交时是 confidence:int32
    assert t.scan(v3).schema.field("score").type == pa.int32()
    assert t.scan(v_rename).schema.field("confidence").type == pa.int32()
    assert t.scan(v3).column_names == ["row_id", "score", "tag"]
    # 新追加按当前结构（int64）校验与落盘：2**40 超出 int32，符合 int64
    f3 = t.append_rows(
        [{"row_id": "i", "confidence": 2**40, "tag": "big"}], schema_version=4
    )
    assert physical_schema(t, f3).field("confidence").type == pa.int64()
    assert {r["row_id"]: r["confidence"] for r in t.scan().to_pylist()}["i"] == 2**40


# ---------------------------------------------------------------------------
# 2. 压实混合格式文件：每条可见记录保留，物理列符合当前定义
# ---------------------------------------------------------------------------
def test_compaction_mixed_physical_formats(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root, schema=[ROW_ID, SCORE32])
    f1 = t.append_rows([{"row_id": "a", "score": 1}, {"row_id": "b", "score": 2}],
                       schema_version=1)
    t.evolve_schema([ROW_ID, SCORE32, TAG])                    # v2: +tag
    f2 = t.append_rows([{"row_id": "c", "score": 3, "tag": "x"}], schema_version=2)
    t.evolve_schema([ROW_ID, CONF32, TAG])                     # v3: 改名
    f3 = t.append_rows([{"row_id": "d", "confidence": 4, "tag": "y"}], schema_version=3)
    t.evolve_schema([ROW_ID, CONF64, TAG])                     # v4: 扩宽
    f4 = t.append_rows([{"row_id": "e", "confidence": 2**40, "tag": None}],
                       schema_version=4)
    # 一个位置删除：旧格式文件 f1 的第 0 行（row_id=a）
    t.publish_deletes(f1, [0])

    # 四个文件四种物理格式；当前结构是 v4
    m = t.manifest()
    versions = {m["data_files"][f]["schema_version"] for f in (f1, f2, f3, f4)}
    assert versions == {1, 2, 3, 4}
    before = t.scan()
    before_rows = scan_rows(before)
    v_before = t.current_version()

    plan_id = create_plan(t, selected=[f1, f2, f3, f4])
    commit_plan(t, plan_id)

    # 每条可见记录保留（f1 的 a 行已被删除，其余全在）
    after = t.scan()
    assert scan_rows(after) == before_rows
    assert {r["row_id"] for r in after.to_pylist()} == {"b", "c", "d", "e"}
    assert {r["row_id"]: r["confidence"] for r in after.to_pylist()} == {
        "b": 2, "c": 3, "d": 4, "e": 2**40
    }
    # 合并文件的物理列符合当前定义：名字与 Arrow 类型都与当前结构一致
    m = t.manifest()
    (merged_id, merged_meta), = m["data_files"].items()
    assert merged_meta["schema_version"] == 4
    phys = pq.read_schema(os.path.join(t.root, merged_meta["path"]))
    assert phys.names == ["row_id", "confidence", "tag"]
    assert phys.field("confidence").type == pa.int64()
    assert phys.field("tag").type == pa.string()
    # 压实前的历史提交仍按当时结构读取（旧文件还在磁盘上）
    assert scan_rows(t.scan(v_before)) == before_rows


# ---------------------------------------------------------------------------
# 3. 不兼容结构变更逐项拒绝：结构、版本、数据都不变
# ---------------------------------------------------------------------------
def test_incompatible_evolutions_rejected(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root, schema=[ROW_ID, SCORE32, TAG])
    t.append_rows([{"row_id": "a", "score": 1, "tag": "x"}], schema_version=1)
    snap = snapshot(t)

    def rejected(fields, **kw):
        with pytest.raises(SchemaEvolutionError):
            t.evolve_schema(fields, **kw)
        assert snapshot(t) == snap

    # 删除仍被文件引用的列（score id2 被 v1 文件引用）
    with pytest.raises(SchemaEvolutionError) as ei:
        t.evolve_schema([ROW_ID, TAG])
    assert "cannot drop" in str(ei.value) and "score" in str(ei.value)
    assert snapshot(t) == snap

    # 重复 ID
    rejected([ROW_ID, SCORE32, {"id": 2, "name": "other", "type": "int64"}])
    # 重复名字
    rejected([ROW_ID, SCORE32, {"name": "score", "type": "int64"}])
    # 缩窄类型：int32 没有更窄的去处，先合法扩宽再尝试缩回来
    ev = t.evolve_schema([ROW_ID, CONF64, TAG])
    assert ev["schema_version"] == 2
    snap = snapshot(t)
    rejected([ROW_ID, CONF32, TAG])                       # int64 -> int32 缩窄
    rejected([ROW_ID, {"id": 2, "name": "confidence", "type": "double",
                      "nullable": True}, TAG])            # int64 -> double 有损
    # 可空收紧为必填
    rejected([ROW_ID, {"id": 2, "name": "confidence", "type": "int64",
                      "nullable": False}, TAG])
    # 新增必填列没有默认值
    rejected([ROW_ID, CONF64, TAG,
              {"name": "must", "type": "string", "nullable": False}])
    # 未知类型 / 坏字段形状
    rejected([ROW_ID, CONF64, TAG, {"name": "x", "type": "varchar"}])
    rejected([ROW_ID, CONF64, TAG, {"name": "x"}])
    # 默认值类型不符
    rejected([ROW_ID, CONF64, TAG,
              {"name": "x", "type": "int32", "default": "not-an-int"}])

    # 过期预期版本：基于 v1 的提议在 v2 上提交 -> SchemaConflictError
    with pytest.raises(SchemaConflictError) as ei:
        t.evolve_schema([ROW_ID, CONF64, TAG], expected_schema_version=1)
    assert (ei.value.expected, ei.value.actual) == (1, 2)
    assert snapshot(t) == snap

    # 复用已删除字段的 id：加一个不被任何文件引用的临时列 -> 删除 -> 重用其 id
    ev = t.evolve_schema([ROW_ID, CONF64, TAG,
                          {"name": "tmp", "type": "string", "nullable": True}])
    tmp_id = ev["schema"]["fields"][-1]["id"]
    ev = t.evolve_schema([ROW_ID, CONF64, TAG])           # 无文件引用 tmp，可删
    snap = snapshot(t)
    with pytest.raises(SchemaEvolutionError) as ei:
        t.evolve_schema([ROW_ID, CONF64, TAG,
                         {"id": tmp_id, "name": "recycled", "type": "string"}])
    assert "never reused" in str(ei.value)
    assert snapshot(t) == snap

    # 与当前完全一致的提议是空操作：不产生新提交
    cur = t.current_schema()
    r = t.evolve_schema(cur["fields"])
    assert r["changed"] is False and r["version"] is None
    assert snapshot(t) == snap


# ---------------------------------------------------------------------------
# 4. 错误类型输入逐项拒绝：结构、版本、数据都不变
# ---------------------------------------------------------------------------
def test_wrong_type_inputs_rejected(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root, schema=[ROW_ID, SCORE32, TAG])
    t.append_rows([{"row_id": "ok", "score": 1, "tag": "x"}], schema_version=1)
    snap = snapshot(t)

    bad_rows = [
        [{"row_id": "x", "score": "not-an-int"}],            # int 列给字符串
        [{"row_id": "x", "score": 2**31}],                   # int32 越界
        [{"row_id": "x", "score": True}],                    # bool 不是 int
        [{"row_id": None, "score": 1}],                      # 必填列给 null
        [{"score": 1}],                                      # 缺必填列
        [{"row_id": "x", "score": 1, "unknown": 1}],         # 未知列
        [{"row_id": "x", "score": 1, "tag": 5}],             # string 列给 int
        ["not-a-mapping"],                                   # 行形状错误
    ]
    for rows in bad_rows:
        with pytest.raises(SchemaValidationError):
            t.append_rows(rows, schema_version=1)
    assert snapshot(t) == snap

    # 不存在的结构版本
    with pytest.raises(SchemaError):
        t.append_rows([{"row_id": "x"}], schema_version=99)
    assert snapshot(t) == snap

    # 摄取的 fields 扩展列同样按当前结构校验
    with pytest.raises(SchemaValidationError):
        t.ingest_batch("bad-1", [msg("s", 0, 0, "r1", 1, fields={"tag": 7})])
    with pytest.raises(SchemaValidationError):
        t.ingest_batch("bad-2", [msg("s", 0, 0, "r1", 1, fields={"nope": "v"})])
    with pytest.raises(SchemaValidationError):
        t.ingest_batch("bad-3", [msg("s", 0, 0, "r1", 2**31)])  # value 越 int32 界
    assert snapshot(t) == snap


# ---------------------------------------------------------------------------
# 5. 结构发布与旧结构写者竞争：旧写入明确失败，重读后重试成功
# ---------------------------------------------------------------------------
def test_schema_publish_races_stale_writer(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root, schema=[ROW_ID, SCORE32])
    t.append_rows([{"row_id": "a", "score": 1}], schema_version=1)

    prepared = threading.Event()
    proceed = threading.Event()
    outcome = {}

    def hook(point):
        # 写者已按 v1 准备好数据文件、尚未进入提交锁：卡住，让结构先演进
        if point == "append_rows":
            prepared.set()
            assert proceed.wait(30)

    t._test_pre_commit_hook = hook

    def writer():
        try:
            t.append_rows([{"row_id": "w1", "score": 9}], schema_version=1)
            outcome["result"] = "committed"
        except SchemaConflictError as e:
            outcome["result"] = (e.expected, e.actual)

    th = threading.Thread(target=writer)
    th.start()
    assert prepared.wait(30)
    t.evolve_schema([ROW_ID, SCORE32, TAG])   # 结构 v1 -> v2 抢先发布
    snap = snapshot(t)
    proceed.set()
    th.join(30)

    # 旧结构写者明确失败：结构、版本、数据与演进后完全一致
    assert outcome["result"] == (1, 2)
    assert snapshot(t) == snap
    assert t.row_ids() == ["a"]
    # 未留下按旧定义物化的临时文件
    assert not [n for n in os.listdir(t.data_dir) if n.startswith(".tmp-")]

    # 写者重新读取当前结构后重试：成功，新列按 null 补齐
    t._test_pre_commit_hook = None
    v = t.current_schema()["version"]
    assert v == 2
    t.append_rows([{"row_id": "w1", "score": 9}], schema_version=v)
    by_id = {r["row_id"]: r for r in t.scan().to_pylist()}
    assert by_id["w1"] == {"row_id": "w1", "score": 9, "tag": None}


# ---------------------------------------------------------------------------
# 6. 结构发布期间中断：重开后结构、版本、数据不变，重试成功
# ---------------------------------------------------------------------------
def _evolve_crash_child(root, fields):
    os.environ["LAKE_PUBLISH_CRASH_POINT"] = "manifest-written"
    t = LakeTable.open(root)
    t.evolve_schema(fields)  # manifest 落盘后、HEAD 翻转前硬退出


def test_crash_during_evolve_publish_leaves_no_half_definition(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root, schema=[ROW_ID, SCORE32])
    t.append_rows([{"row_id": "a", "score": 1}], schema_version=1)
    snap = snapshot(t)

    p = CTX.Process(target=_evolve_crash_child, args=(root, [ROW_ID, SCORE32, TAG]))
    p.start()
    p.join(60)
    assert p.exitcode == 42  # 在“manifest 已落盘、指针未翻转”处硬退出

    # 重开：结构、版本、数据与崩溃前完全一致（没有只改了一半的定义）
    t2 = LakeTable.open(root)
    assert snapshot(t2) == snap
    assert t2.current_schema()["version"] == 1

    # 同一变更可原样重试并成功（孤儿 manifest 只是占过一个版本号）
    r = t2.evolve_schema([ROW_ID, SCORE32, TAG])
    assert r["changed"] is True and r["schema_version"] == 2
    assert t2.current_schema()["fields"][-1]["name"] == "tag"
    assert all(r["tag"] is None for r in t2.scan().to_pylist())


# ---------------------------------------------------------------------------
# 7. 无结构元数据的旧湖表：旧调用无需迁移；演进后旧文件按 null 解释
# ---------------------------------------------------------------------------
def test_legacy_table_without_schema_metadata(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root)  # 旧式建表：无结构元数据
    assert t.current_schema()["version"] == 0
    f1 = t.append([("a", 1), ("b", None)])          # 旧式追加（value 可为 null）
    t.ingest_batch("k1", [msg("s", 0, 0, "c", 3)])  # 旧式摄取
    v1 = t.current_version()

    # 重开：仍按已有 row_id/value 格式工作
    t2 = LakeTable.open(root)
    assert t2.current_schema()["version"] == 0
    s = t2.scan()
    assert s.column_names == ["row_id", "value"]
    assert s.schema.field("value").type == pa.int64()
    assert t2.row_ids() == t.row_ids()
    assert {r["row_id"]: r["value"] for r in s.to_pylist()} == {
        "a": 1, "b": None, "c": 3,
    }

    # 旧表直接演进：新增可空列（隐式 v0 结构随历史一起物化）
    LEG_ROW_ID = {"id": 1, "name": "row_id", "type": "string", "nullable": True}
    LEG_VALUE = {"id": 2, "name": "value", "type": "int64", "nullable": True}
    ev = t2.evolve_schema([LEG_ROW_ID, LEG_VALUE, TAG])
    assert ev["schema_version"] == 1
    assert [s["version"] for s in t2.schema_history()] == [0, 1]

    # 旧文件缺少 tag -> null；旧式追加/摄取继续可用（兼容路径，新列补 null）
    f2 = t2.append([("d", 4)])
    t2.ingest_batch("k2", [msg("s", 0, 1, "e", 5)])
    s = t2.scan()
    assert s.column_names == ["row_id", "value", "tag"]
    assert all(r["tag"] is None for r in s.to_pylist())
    assert {r["row_id"]: r["value"] for r in s.to_pylist()} == {
        "a": 1, "b": None, "c": 3, "d": 4, "e": 5,
    }
    # 兼容路径写出的文件按当前结构落盘并记录结构版本
    m = t2.manifest()
    assert m["data_files"][f2]["schema_version"] == 1
    assert "schema_version" not in m["data_files"][f1]  # 旧文件缺省为 0
    # 历史版本仍是当时的两列
    assert t2.scan(v1).column_names == ["row_id", "value"]


# ---------------------------------------------------------------------------
# 8. 新增带默认值的必填列：旧文件按默认值解释
# ---------------------------------------------------------------------------
def test_add_required_column_with_default(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root, schema=[ROW_ID, SCORE32])
    t.append_rows([{"row_id": "a", "score": 1}], schema_version=1)

    LABEL = {"id": 3, "name": "label", "type": "string",
             "nullable": False, "default": "unk"}
    ev = t.evolve_schema([ROW_ID, SCORE32, LABEL])
    assert ev["schema_version"] == 2
    # 旧文件缺少该列 -> 按默认值解释
    assert [r["label"] for r in t.scan().to_pylist()] == ["unk"]

    # 新行缺省补默认值；显式给值（含显式 null -> 拒绝）按规则校验
    t.append_rows([{"row_id": "b", "score": 2}], schema_version=2)
    t.append_rows([{"row_id": "c", "score": 3, "label": "y"}], schema_version=2)
    with pytest.raises(SchemaValidationError):
        t.append_rows([{"row_id": "d", "score": 4, "label": None}], schema_version=2)
    assert {r["row_id"]: r["label"] for r in t.scan().to_pylist()} == {
        "a": "unk", "b": "unk", "c": "y",
    }


# ---------------------------------------------------------------------------
# 9. 摄取 fields 扩展列与幂等重放
# ---------------------------------------------------------------------------
def test_ingest_with_fields_and_replay(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root, schema=[ROW_ID, SCORE32, TAG])
    batch = [
        msg("s", 0, 0, "r1", 5, fields={"tag": "x"}),
        msg("s", 0, 1, "r2", 6),                    # 未给 fields -> tag 为 null
    ]
    r = t.ingest_batch("b1", batch)
    s = t.scan()
    assert s.column_names == ["row_id", "score", "tag"]
    assert {row["row_id"]: (row["score"], row["tag"]) for row in s.to_pylist()} == {
        "r1": (5, "x"), "r2": (6, None),
    }
    # 相同 batch_key + 相同内容（乱序）-> 原 receipt，不新增行
    again = t.ingest_batch("b1", list(reversed(batch)))
    assert again["replayed"] is True and again["receipt"] == r["receipt"]
    assert t.scan().num_rows == 2


# ---------------------------------------------------------------------------
# 10. 旧式写入与当前结构不兼容时明确拒绝
# ---------------------------------------------------------------------------
def test_legacy_write_rejected_on_incompatible_schema(tmp_path):
    root = str(tmp_path / "lake")
    # 显式 id 从 10 开始：字段 id 1/2 不存在，旧式 (row_id, value) 写入无的放矢
    t = LakeTable.create(
        root,
        schema=[
            {"id": 10, "name": "key", "type": "string", "nullable": False},
            {"id": 11, "name": "x", "type": "int64", "nullable": True},
        ],
    )
    snap = snapshot(t)
    with pytest.raises(SchemaError):
        t.append([("a", 1)])
    with pytest.raises(SchemaError):
        t.ingest_batch("k", [msg("s", 0, 0, "a", 1)])
    assert snapshot(t) == snap
    # 结构感知写入正常工作
    t.append_rows([{"key": "a", "x": 1}], schema_version=1)
    assert t.scan().column_names == ["key", "x"]


# ---------------------------------------------------------------------------
# 11. 重开湖表：结构历史与数据提交一致，读取/微批/整理同一套映射
# ---------------------------------------------------------------------------
def test_reopen_keeps_schema_history_consistent(tmp_path):
    root = str(tmp_path / "lake")
    t = LakeTable.create(root, schema=[ROW_ID, SCORE32])
    t.append_rows([{"row_id": "a", "score": 1}], schema_version=1)
    t.ingest_batch("k1", [msg("s", 0, 0, "b", 2)])
    t.evolve_schema([ROW_ID, SCORE32, TAG])
    t.append_rows([{"row_id": "c", "score": 3, "tag": "t"}], schema_version=2)
    t.evolve_schema([ROW_ID, CONF64, TAG])
    snap = snapshot(t)

    t2 = LakeTable.open(root)
    assert snapshot(t2) == snap
    assert [s["version"] for s in t2.schema_history()] == [1, 2, 3]
    # 微批校验用当前结构：value 越 int32 界（当前已是 int64，合法）与坏 fields
    t2.ingest_batch("k2", [msg("s", 0, 1, "d", 2**40)])
    assert {r["row_id"]: r["confidence"] for r in t2.scan().to_pylist()}["d"] == 2**40
    with pytest.raises(SchemaValidationError):
        t2.ingest_batch("k3", [msg("s", 0, 2, "e", 1, fields={"tag": 9})])
    # 整理用同一套映射：压实全部混合格式文件，行集合不变
    m = t2.manifest()
    plan_id = create_plan(t2, selected=sorted(m["data_files"]))
    commit_plan(t2, plan_id)
    assert scan_rows(t2.scan()) == scan_rows(t.scan())
    (merged_meta,) = t2.manifest()["data_files"].values()
    assert merged_meta["schema_version"] == 3
    assert pq.read_schema(
        os.path.join(t2.root, merged_meta["path"])
    ).field("confidence").type == pa.int64()
