"""端到端演示：字段身份与逻辑结构演进（生产者滚动升级）。

依次演示：
  1. 建表（显式结构 v1）+ 旧格式数据写入（追加 + 微批摄取）
  2. 滚动升级①：新增可空列 tag —— 新旧物理格式文件并存，旧文件按 null 解释
  3. 滚动升级②：改名 score -> confidence（字段 id 不变，历史数据不重写）
  4. 滚动升级③：int32 无损扩宽 int64 —— 逐版本核对列名 / Arrow 类型 /
     null 位置 / 全部行值
  5. 压实四种历史格式的混合文件：每条可见记录保留，物理列符合当前定义
  6. 不兼容结构变更逐项拒绝（删引用列 / 重复 ID / 缩窄 / 必填无默认 /
     过期预期版本），每次拒绝后结构、版本、数据不变
  7. 错误类型输入拒绝（SchemaValidationError），表状态不变
  8. 结构发布与旧结构写者竞争：旧写入明确失败，重读结构后重试成功
  9. 结构发布期间崩溃（真实子进程）：重开后没有只改了一半的定义
 10. 无结构元数据的旧湖表：旧调用无需迁移；演进后旧文件自动补 null
 11. 重开湖表：结构历史与数据提交一致，读取 / 微批 / 整理同一套映射

运行：python3 demo_schema.py
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import shutil
import sys
import threading

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lake import (  # noqa: E402
    LakeTable,
    SchemaConflictError,
    SchemaError,
    SchemaEvolutionError,
    SchemaValidationError,
    commit_plan,
    create_plan,
)

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo_schema_lake")
CTX = mp.get_context("fork")

ROW_ID = {"id": 1, "name": "row_id", "type": "string", "nullable": False}
SCORE32 = {"id": 2, "name": "score", "type": "int32", "nullable": True}
TAG = {"id": 3, "name": "tag", "type": "string", "nullable": True}
CONF32 = {"id": 2, "name": "confidence", "type": "int32", "nullable": True}
CONF64 = {"id": 2, "name": "confidence", "type": "int64", "nullable": True}


def banner(msg: str) -> None:
    print(f"\n{'=' * 64}\n■ {msg}\n{'=' * 64}")


def ok(msg: str) -> None:
    print(f"  ✔ {msg}")


def scan_rows(scan: pa.Table) -> list[str]:
    return sorted(json.dumps(r, sort_keys=True) for r in scan.to_pylist())


def snapshot(t: LakeTable) -> dict:
    m = t.manifest()
    return {
        "version": t.current_version(),
        "schema": t.current_schema(),
        "history": t.schema_history(),
        "files": {k: dict(v) for k, v in m["data_files"].items()},
        "rows": scan_rows(t.scan()),
        "cursors": t.ingest_cursors(),
    }


def show_scan(t: LakeTable, version=None) -> pa.Table:
    s = t.scan(version)
    types = ", ".join(f"{f.name}: {f.type}" for f in s.schema)
    print(f"    scan(v{version if version is not None else t.current_version()}): "
          f"[{types}]")
    for r in sorted(s.to_pylist(), key=lambda r: r["row_id"]):
        print(f"      {r}")
    return s


def physical_of(t: LakeTable, fid: str) -> pa.Schema:
    meta = t.manifest()["data_files"][fid]
    return pq.read_schema(os.path.join(t.root, meta["path"]))


def msg(source, partition, offset, row_id, value, fields=None):
    m = {"source": source, "partition": partition, "offset": offset,
         "row_id": row_id, "value": value}
    if fields is not None:
        m["fields"] = fields
    return m


def main() -> None:
    shutil.rmtree(ROOT, ignore_errors=True)

    # ---------------------------------------------------------------- 场景 1
    banner("场景 1：建表（显式结构 v1）+ 旧格式数据开始写入")
    t = LakeTable.create(ROOT, schema=[ROW_ID, SCORE32])
    print(f"  当前结构: v{t.current_schema()['version']} "
          f"{[(f['id'], f['name'], f['type']) for f in t.current_schema()['fields']]}")
    f1 = t.append_rows(
        [{"row_id": "a", "score": 10}, {"row_id": "b", "score": None},
         {"row_id": "c", "score": 30}],
        schema_version=1,
    )
    t.ingest_batch("ing-1", [msg("s", 0, 0, "d", 40), msg("s", 0, 1, "e", 50)])
    v1 = t.current_version()
    show_scan(t)
    ok("旧格式文件物理列为 (row_id, score:int32)，摄取消息的 value 落到字段 id 2")
    assert physical_of(t, f1).names == ["row_id", "score"]

    # ---------------------------------------------------------------- 场景 2
    banner("场景 2：滚动升级① 新增可空列 tag —— 新旧文件并存")
    ev = t.evolve_schema([ROW_ID, SCORE32, TAG])
    print(f"  结构 v{ev['schema_version']} 随提交 v{ev['version']} 原子生效")
    f2 = t.append_rows(
        [{"row_id": "f", "score": 60, "tag": "new"}, {"row_id": "g", "score": 70}],
        schema_version=2,
    )
    s = show_scan(t)
    by_id = {r["row_id"]: r for r in s.to_pylist()}
    assert all(by_id[k]["tag"] is None for k in "abcde")
    assert by_id["f"]["tag"] == "new" and by_id["g"]["tag"] is None
    ok("旧文件缺少 tag 的 5 行全部按 null 解释，新文件正常取值")
    assert physical_of(t, f1).names == ["row_id", "score"]
    assert physical_of(t, f2).names == ["row_id", "score", "tag"]
    ok(f"新旧物理格式并存：旧文件 {physical_of(t, f1).names}，"
       f"新文件 {physical_of(t, f2).names}")
    assert t.scan(v1).column_names == ["row_id", "score"]
    ok("读取旧提交 v{} 仍用当时生效的两列定义".format(v1))

    # ---------------------------------------------------------------- 场景 3
    banner("场景 3：滚动升级② 改名 score -> confidence（字段 id 不变）")
    before = {r["row_id"]: r["score"] for r in t.scan().to_pylist()}
    ev = t.evolve_schema([ROW_ID, CONF32, TAG])
    print(f"  结构 v{ev['schema_version']}：id 2 现在叫 confidence")
    s = show_scan(t)
    assert s.column_names == ["row_id", "confidence", "tag"]
    assert {r["row_id"]: r["confidence"] for r in s.to_pylist()} == before
    ok("全部行值逐值不变；改名只是逻辑映射，历史文件一个字节都没改")
    assert physical_of(t, f1).names == ["row_id", "score"]
    ok("旧文件物理列仍叫 score，读取时按字段 id 映射到 confidence")
    t.ingest_batch("ing-2", [msg("s", 0, 2, "h", 80)])
    assert {r["row_id"]: r["confidence"] for r in t.scan().to_pylist()}["h"] == 80
    ok("改名后旧式摄取照常工作：value 仍按 id 2 落到 confidence")

    # ---------------------------------------------------------------- 场景 4
    banner("场景 4：滚动升级③ int32 无损扩宽 int64，逐版本核对")
    ev = t.evolve_schema([ROW_ID, CONF64, TAG])
    print(f"  结构 v{ev['schema_version']}：confidence int32 -> int64")
    f3 = t.append_rows([{"row_id": "i", "confidence": 2**40, "tag": "big"}],
                       schema_version=4)
    s = show_scan(t)
    assert s.schema.field("confidence").type == pa.int64()
    vals = {r["row_id"]: r["confidence"] for r in s.to_pylist()}
    assert vals == {"a": 10, "b": None, "c": 30, "d": 40, "e": 50,
                    "f": 60, "g": 70, "h": 80, "i": 2**40}
    ok("旧物理 int32 文件的值无损放大为 int64，逐值不变；2**40 的新值合法入湖")
    assert physical_of(t, f1).field("score").type == pa.int32()
    assert physical_of(t, f3).field("confidence").type == pa.int64()
    assert t.scan(v1).schema.field("score").type == pa.int32()
    ok("历史提交的 Arrow 类型保持当时定义（v{} 仍是 int32）".format(v1))

    # ---------------------------------------------------------------- 场景 5
    banner("场景 5：压实四种历史格式的混合文件")
    t.publish_deletes(f1, [0])   # 删除旧格式文件 f1 的第 0 行（row_id=a）
    before_rows = scan_rows(t.scan())
    versions = {m.get("schema_version", 0)
                for m in t.manifest()["data_files"].values()}
    print(f"  待压实文件的结构版本: {sorted(versions)}")
    plan_id = create_plan(t, max_rows=10**9)  # 阈值选中全部小文件
    commit_plan(t, plan_id)
    after = t.scan()
    assert scan_rows(after) == before_rows
    ok("每条可见记录保留（含已删除行的剔除），行集合与压实前逐值一致")
    (merged_id, merged_meta), = t.manifest()["data_files"].items()
    phys = physical_of(t, merged_id)
    assert phys.names == ["row_id", "confidence", "tag"]
    assert phys.field("confidence").type == pa.int64()
    assert merged_meta["schema_version"] == t.current_schema()["version"]
    ok(f"合并文件物理列符合当前定义: {phys.names} "
       f"(confidence: {phys.field('confidence').type})")

    # ---------------------------------------------------------------- 场景 6
    banner("场景 6：不兼容结构变更逐项拒绝，状态不变")
    snap = snapshot(t)

    def expect_reject(title, fields, **kw):
        try:
            t.evolve_schema(fields, **kw)
        except SchemaEvolutionError as e:
            assert snapshot(t) == snap
            print(f"  ✔ {title}\n      规则: {e.reasons[0]}")
            return
        raise AssertionError(f"{title}: 未被拒绝")

    expect_reject("删除仍被文件引用的列", [ROW_ID, TAG])
    expect_reject("重复字段 ID", [ROW_ID, CONF64,
                                {"id": 2, "name": "x", "type": "int64"}])
    expect_reject("缩窄类型 int64 -> int32", [ROW_ID, CONF32, TAG])
    expect_reject("新增必填列没有默认值",
                  [ROW_ID, CONF64, TAG,
                   {"name": "must", "type": "string", "nullable": False}])
    try:
        t.evolve_schema([ROW_ID, CONF64, TAG], expected_schema_version=1)
        raise AssertionError("过期预期版本未被拒绝")
    except SchemaConflictError as e:
        assert snapshot(t) == snap
        ok(f"过期预期版本被拒绝（expected v{e.expected}, current v{e.actual}）")

    # ---------------------------------------------------------------- 场景 7
    banner("场景 7：错误类型输入拒绝，表状态不变")
    v = t.current_schema()["version"]
    for title, rows in [
        ("int 列给字符串", [{"row_id": "x", "confidence": "bad"}]),
        ("必填列缺省", [{"confidence": 1}]),
        ("未知列", [{"row_id": "x", "confidence": 1, "nope": 2}]),
    ]:
        try:
            t.append_rows(rows, schema_version=v)
            raise AssertionError(f"{title}: 未被拒绝")
        except SchemaValidationError as e:
            assert snapshot(t) == snap
            print(f"  ✔ {title}\n      规则: {e.reasons[0]}")

    # ---------------------------------------------------------------- 场景 8
    banner("场景 8：结构发布与旧结构写者竞争（真实线程交错）")
    prepared, proceed = threading.Event(), threading.Event()
    outcome = {}

    def hook(point):
        if point == "append_rows":
            prepared.set()
            assert proceed.wait(30)

    t._test_pre_commit_hook = hook

    def writer():
        try:
            t.append_rows([{"row_id": "w1", "confidence": 9}], schema_version=4)
            outcome["r"] = "committed"
        except SchemaConflictError as e:
            outcome["r"] = (e.expected, e.actual)

    th = threading.Thread(target=writer)
    th.start()
    assert prepared.wait(30)
    print("  写者已按 v4 备好数据文件，结构演进抢先发布 v5 ...")
    t.evolve_schema([ROW_ID, CONF64, TAG,
                     {"name": "note", "type": "string", "nullable": True}])
    snap = snapshot(t)
    proceed.set()
    th.join(30)
    assert outcome["r"] == (4, 5)
    assert snapshot(t) == snap
    ok("旧结构写者明确失败（SchemaConflictError），结构、版本、数据不变")
    t._test_pre_commit_hook = None
    t.append_rows([{"row_id": "w1", "confidence": 9}],
                  schema_version=t.current_schema()["version"])
    assert {r["row_id"] for r in t.scan().to_pylist()} >= {"w1"}
    ok("写者重读当前结构后重试成功，新列 note 按 null 补齐")

    # ---------------------------------------------------------------- 场景 9
    banner("场景 9：结构发布期间崩溃（真实子进程），重开后无半成品定义")
    snap = snapshot(t)

    def crash_child():
        os.environ["LAKE_PUBLISH_CRASH_POINT"] = "manifest-written"
        LakeTable.open(ROOT).evolve_schema(
            [ROW_ID, CONF64, TAG,
             {"id": 4, "name": "note", "type": "string", "nullable": True},
             {"name": "extra", "type": "double", "nullable": True}])

    p = CTX.Process(target=crash_child)
    p.start()
    p.join(60)
    assert p.exitcode == 42
    t2 = LakeTable.open(ROOT)
    assert snapshot(t2) == snap
    ok("子进程在“manifest 落盘、HEAD 未翻转”处崩溃：重开后结构/版本/数据不变")
    r = t2.evolve_schema([ROW_ID, CONF64, TAG,
                          {"id": 4, "name": "note", "type": "string",
                           "nullable": True},
                          {"name": "extra", "type": "double", "nullable": True}])
    assert r["changed"] is True
    ok(f"同一变更重试成功：结构 v{r['schema_version']} 随提交 v{r['version']} 生效")
    t = t2

    # ---------------------------------------------------------------- 场景 10
    banner("场景 10：无结构元数据的旧湖表 —— 旧调用无需迁移")
    legacy_root = ROOT + "_legacy"
    shutil.rmtree(legacy_root, ignore_errors=True)
    lt = LakeTable.create(legacy_root)          # 旧式建表：无结构元数据
    lt.append([("a", 1), ("b", None)])
    lt.ingest_batch("k1", [msg("s", 0, 0, "c", 3)])
    lt2 = LakeTable.open(legacy_root)           # 重开仍按 row_id/value 工作
    s = lt2.scan()
    assert s.column_names == ["row_id", "value"]
    ok(f"旧目录直接打开：{s.column_names}，结构版本 v{lt2.current_schema()['version']}")
    LEG = [{"id": 1, "name": "row_id", "type": "string", "nullable": True},
           {"id": 2, "name": "value", "type": "int64", "nullable": True}]
    lt2.evolve_schema(LEG + [TAG])
    lt2.append([("d", 4)])                       # 旧式追加：兼容路径，新列补 null
    lt2.ingest_batch("k2", [msg("s", 0, 1, "e", 5)])
    s = lt2.scan()
    assert s.column_names == ["row_id", "value", "tag"]
    assert all(r["tag"] is None for r in s.to_pylist())
    ok("演进后旧式追加/摄取继续可用；旧文件与新写入的 tag 全按 null 解释")

    # ---------------------------------------------------------------- 场景 11
    banner("场景 11：重开湖表 —— 结构历史与数据提交一致")
    t3 = LakeTable.open(ROOT)
    hist = [(s["version"], [(f["name"], f["type"]) for f in s["fields"]])
            for s in t3.schema_history()]
    for ver, fields in hist:
        print(f"    结构 v{ver}: {fields}")
    assert [h[0] for h in hist] == [1, 2, 3, 4, 5, 6]
    assert snapshot(t3) == snapshot(t)
    ok("重开后结构历史、数据提交、游标与全部可见行完全一致")
    plan_id = create_plan(t3, max_rows=10**9)
    commit_plan(t3, plan_id)
    assert scan_rows(t3.scan()) == scan_rows(t.scan())
    ok("整理混合格式文件用同一套字段映射：压实后行集合不变")

    banner("全部 11 个场景演示完成")
    shutil.rmtree(ROOT, ignore_errors=True)
    shutil.rmtree(legacy_root, ignore_errors=True)


if __name__ == "__main__":
    main()
