"""字段身份与逻辑结构演进（schema evolution）。

训练特征在生产者滚动升级时会逐步增加或调整列，湖表里新旧物理格式的
Parquet 文件同时存在。本模块提供**一套**字段映射规则，供扫描、追加、
微批摄取与压实共同使用，任何入口都不自行猜测字段含义。

字段身份
--------
每个字段有稳定 ``id``（整数，永不复用）、当前 ``name``、逻辑 ``type``
与 ``nullable`` 标记（可选 ``default``）。改名不改 id；类型只允许无损
扩宽（int32→int64、int32→double、float32→double）；删除仍被文件引用
的列、重复 id、缩窄类型、没有默认值的必填新增列一律拒绝。

结构版本与历史
--------------
结构版本是表级单调递增整数；**0 保留给旧式 row_id/value 隐式结构**
（没有结构元数据的旧湖表）。每个 manifest 携带完整结构历史
``"schemas": [schema, ...]``（最后一个即当前定义），结构变更就是一个
普通的 manifest 提交：与数据提交共用同一条原子发布路径，拒绝或中断
不会留下只改了一半的定义；重开后结构历史与数据提交天然一致。

每个数据文件记录写入时的结构版本（``data_files[fid]["schema_version"]``，
缺省为 0）。读取提交 V 时：

    物理列名 --(文件自己的结构版本)--> 字段 id --(V 的结构定义)--> 当前名称与类型

因此改名/扩宽后旧文件无需重写；旧提交用当时生效的列定义读取，新提交
把各历史格式的文件统一映射到该提交要求的名称和类型。文件缺少的列按
``default`` 或 null 补齐。
"""

from __future__ import annotations

import math
from collections.abc import Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .errors import SchemaError, SchemaEvolutionError, SchemaValidationError

# --------------------------------------------------------------------------
# 逻辑类型注册表与无损扩宽关系
# --------------------------------------------------------------------------
_TYPE_REGISTRY = {
    "bool": pa.bool_(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float32": pa.float32(),
    "double": pa.float64(),
    "string": pa.string(),
}
_INT_RANGES = {
    "int32": (-(2**31), 2**31 - 1),
    "int64": (-(2**63), 2**63 - 1),
}
# 无损扩宽：target 能精确表示 source 的全部取值。int64→double 不是无损
# （>2^53 的整数会丢精度），因此不在表里。
_WIDENING = {
    "int32": {"int64", "double"},
    "float32": {"double"},
}

LEGACY_SCHEMA_VERSION = 0
LEGACY_ROW_ID_FIELD = 1
LEGACY_VALUE_FIELD = 2

_FIELD_KEYS = {"id", "name", "type", "nullable", "default"}


def legacy_schema() -> dict:
    """旧式湖表的隐式结构（v0）：row_id/value，两列历史上都允许 null。"""
    return {
        "version": LEGACY_SCHEMA_VERSION,
        "fields": [
            {
                "id": LEGACY_ROW_ID_FIELD,
                "name": "row_id",
                "type": "string",
                "nullable": True,
            },
            {
                "id": LEGACY_VALUE_FIELD,
                "name": "value",
                "type": "int64",
                "nullable": True,
            },
        ],
    }


# --------------------------------------------------------------------------
# 结构历史的读取
# --------------------------------------------------------------------------
def schema_chain(manifest: dict) -> list[dict]:
    """manifest 的完整结构历史（按版本升序，最后一个为当前定义）。

    旧格式 manifest 没有 "schemas" 段，返回隐式旧式结构（v0）——
    旧湖表无需迁移即可继续工作。
    """
    chain = manifest.get("schemas")
    if not chain:
        return [legacy_schema()]
    return chain


def current_schema(manifest: dict) -> dict:
    """该提交生效的字段定义（结构历史的最后一项）。"""
    return schema_chain(manifest)[-1]


def schema_by_version(chain: list[dict], version: int) -> dict:
    for s in chain:
        if s["version"] == version:
            return s
    raise SchemaError(f"schema version {version} not found in table history")


def arrow_type(type_name: str) -> pa.DataType:
    try:
        return _TYPE_REGISTRY[type_name]
    except KeyError:
        raise SchemaError(f"unknown logical type {type_name!r}") from None


def arrow_schema(schema: dict) -> pa.Schema:
    """字段定义对应的 Arrow schema（列顺序 = 字段定义顺序）。"""
    return pa.schema(
        [
            pa.field(f["name"], arrow_type(f["type"]), nullable=f["nullable"])
            for f in schema["fields"]
        ]
    )


def field_by_id(schema: dict, field_id: int) -> dict | None:
    for f in schema["fields"]:
        if f["id"] == field_id:
            return f
    return None


# --------------------------------------------------------------------------
# 值与字段定义的校验
# --------------------------------------------------------------------------
def value_matches(type_name: str, value) -> bool:
    """Python 值是否符合逻辑类型（用于行数据与默认值校验）。"""
    if type_name == "bool":
        return isinstance(value, bool)
    if type_name in _INT_RANGES:
        if isinstance(value, bool) or not isinstance(value, int):
            return False
        lo, hi = _INT_RANGES[type_name]
        return lo <= value <= hi
    if type_name in ("float32", "double"):
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if type_name == "string":
        return isinstance(value, str)
    return False


def _normalize_field(raw, index: int):
    """把调用方给出的字段定义规范化为内部形状；返回 (field, errors)。"""
    where = f"field #{index}"
    if not isinstance(raw, Mapping):
        return None, [f"{where}: field definition must be a mapping"]
    errors = []
    unknown = sorted(set(raw) - _FIELD_KEYS)
    if unknown:
        errors.append(f"{where}: unknown key(s) {unknown}")
    name = raw.get("name")
    if not isinstance(name, str) or not name:
        errors.append(f"{where}: 'name' must be a non-empty str")
    type_name = raw.get("type")
    if not isinstance(type_name, str) or type_name not in _TYPE_REGISTRY:
        errors.append(
            f"{where} ({name!r}): unknown type {type_name!r}; "
            f"supported: {sorted(_TYPE_REGISTRY)}"
        )
    nullable = raw.get("nullable", True)
    if not isinstance(nullable, bool):
        errors.append(f"{where} ({name!r}): 'nullable' must be a bool")
        nullable = True
    field_id = raw.get("id")
    if field_id is not None and (
        isinstance(field_id, bool) or not isinstance(field_id, int) or field_id < 1
    ):
        errors.append(f"{where} ({name!r}): 'id' must be an int >= 1")
        field_id = None
    field = {}
    if errors:
        return None, errors
    field = {"name": name, "type": type_name, "nullable": nullable}
    if field_id is not None:
        field["id"] = field_id
    if "default" in raw:
        default = raw["default"]
        if default is None:
            errors.append(
                f"{where} ({name!r}): default must be a non-null value "
                f"(a nullable field needs no default)"
            )
        elif not value_matches(type_name, default):
            errors.append(
                f"{where} ({name!r}): default {default!r} does not match "
                f"type {type_name!r}"
            )
        elif isinstance(default, float) and not math.isfinite(default):
            errors.append(f"{where} ({name!r}): default must be a finite number")
        else:
            field["default"] = default
    if errors:
        return None, errors
    return field, []


def validate_evolution(
    chain: list[dict],
    proposed_fields,
    live_file_schema_versions: list[int],
    *,
    allow_required_add: bool = False,
) -> dict:
    """校验“当前结构 -> 提议结构”的变更，返回新的结构定义。

    chain                    当前结构历史（最后一项是当前定义；建表时为空）
    proposed_fields          调用方给出的完整新字段列表
    live_file_schema_versions 当前 manifest 全部数据文件的结构版本
    allow_required_add       建表时为 True（还没有任何旧文件，必填列无需默认值）

    任何一条规则不满足都抛 SchemaEvolutionError，reasons 列出全部违规；
    通过则返回 {"version": ..., "fields": [...]}（id 已补全）。
    """
    reasons: list[str] = []
    if not isinstance(proposed_fields, (list, tuple)) or not proposed_fields:
        raise SchemaEvolutionError(["schema must contain at least one field"])
    current = chain[-1] if chain else None
    cur_by_id = {f["id"]: f for f in current["fields"]} if current else {}
    cur_by_name = {f["name"]: f for f in current["fields"]} if current else {}
    # 历史链上出现过的全部 id（含已删除字段）：id 永不复用，否则旧文件
    # 的物理列会被错误地映射到复用该 id 的新字段。
    historical_ids = {f["id"] for s in chain for f in s["fields"]}

    # 1) 结构规范化
    fields = []
    for i, raw in enumerate(proposed_fields):
        field, errs = _normalize_field(raw, i)
        if errs:
            reasons.extend(errs)
        else:
            fields.append(field)
    if reasons:
        raise SchemaEvolutionError(reasons)

    # 2) 未显式给 id 的字段：名字与当前字段相同则继承其 id（按名延续身份），
    #    否则视为新增列，稍后分配新 id。改名必须显式携带 id。
    for f in fields:
        if "id" not in f and f["name"] in cur_by_name:
            f["id"] = cur_by_name[f["name"]]["id"]

    # 3) 重复 id / 重复名字
    seen_ids: dict[int, str] = {}
    for f in fields:
        if "id" in f:
            if f["id"] in seen_ids:
                reasons.append(
                    f"duplicate field id {f['id']} "
                    f"({seen_ids[f['id']]!r} and {f['name']!r})"
                )
            else:
                seen_ids[f["id"]] = f["name"]
    seen_names: set[str] = set()
    for f in fields:
        if f["name"] in seen_names:
            reasons.append(f"duplicate field name {f['name']!r}")
        seen_names.add(f["name"])

    proposed_ids = {f["id"] for f in fields if "id" in f}

    # 4) 保留字段：类型只能无损扩宽；可空不可收紧为必填
    for f in fields:
        fid = f.get("id")
        if fid is None or fid not in cur_by_id:
            continue
        old = cur_by_id[fid]
        if f["type"] != old["type"] and f["type"] not in _WIDENING.get(
            old["type"], ()
        ):
            reasons.append(
                f"field {old['name']!r} (id {fid}): type change "
                f"{old['type']} -> {f['type']} is not a lossless widening "
                f"(allowed: int32->int64, int32->double, float32->double)"
            )
        if old["nullable"] and not f["nullable"]:
            reasons.append(
                f"field {old['name']!r} (id {fid}): cannot tighten nullable "
                f"to required (existing files may contain nulls)"
            )

    # 5) 删除字段：仍被数据文件引用的列拒绝删除
    chain_by_version = {s["version"]: s for s in chain}
    for old_id, old in cur_by_id.items():
        if old_id in proposed_ids:
            continue
        referencing = sorted(
            {
                v
                for v in live_file_schema_versions
                if v in chain_by_version
                and field_by_id(chain_by_version[v], old_id) is not None
            }
        )
        if referencing:
            reasons.append(
                f"field {old['name']!r} (id {old_id}): cannot drop, still "
                f"referenced by data files written under schema version(s) "
                f"{referencing} (compact or remove those files first)"
            )

    # 6) 新增字段：必填列必须带默认值（旧文件缺少该列，读取时按默认值/
    #    null 解释；没有默认值的必填列无法解释，拒绝）
    for f in fields:
        fid = f.get("id")
        is_new = fid is None or fid not in cur_by_id
        if not is_new:
            continue
        if fid is not None and fid in historical_ids:
            reasons.append(
                f"field {f['name']!r}: id {fid} was used by a previously "
                f"dropped field; field ids are never reused"
            )
        if (
            not f["nullable"]
            and "default" not in f
            and not allow_required_add
        ):
            reasons.append(
                f"added field {f['name']!r} is required (nullable=false) but "
                f"has no default: old files lack this column"
            )

    if reasons:
        raise SchemaEvolutionError(reasons)

    # 7) 为新增字段分配从未使用过的 id
    next_id = max(historical_ids, default=0) + 1
    for f in fields:
        if "id" not in f:
            while next_id in proposed_ids:
                next_id += 1
            f["id"] = next_id
            proposed_ids.add(next_id)
            next_id += 1

    version = (current["version"] + 1) if current else (LEGACY_SCHEMA_VERSION + 1)
    return {"version": version, "fields": fields}


def initial_schema(proposed_fields) -> dict:
    """建表时的首份显式结构（版本 1；版本 0 保留给旧式隐式结构）。"""
    return validate_evolution([], proposed_fields, [], allow_required_add=True)


# --------------------------------------------------------------------------
# 行数据校验与物理表构造（追加 / 微批共用）
# --------------------------------------------------------------------------
def validate_rows(schema: dict, rows) -> list[dict]:
    """按结构定义校验行（字段名 -> 值的映射列表），返回补齐后的规范化行。

    未知列、缺必填列、必填列给 null、值类型不符都会记入 reasons，整批抛
    SchemaValidationError；可空列缺省补 null，带默认值的列缺省补默认值。
    """
    fields = schema["fields"]
    by_name = {f["name"]: f for f in fields}
    reasons: list[str] = []
    out = []
    for i, row in enumerate(rows):
        if not isinstance(row, Mapping):
            reasons.append(f"row {i}: must be a mapping of field name -> value")
            continue
        unknown = sorted(str(k) for k in row if k not in by_name)
        if unknown:
            reasons.append(
                f"row {i}: unknown field(s) {unknown} "
                f"(known: {[f['name'] for f in fields]})"
            )
        rec = {}
        for f in fields:
            name, type_name = f["name"], f["type"]
            if name in row:
                value = row[name]
                if value is None:
                    if not f["nullable"]:
                        reasons.append(
                            f"row {i}: field {name!r} is required "
                            f"(null not allowed)"
                        )
                    rec[name] = None
                elif not value_matches(type_name, value):
                    reasons.append(
                        f"row {i}: field {name!r} expects {type_name}, got "
                        f"{type(value).__name__} value {value!r}"
                    )
                    rec[name] = None
                else:
                    rec[name] = value
            elif "default" in f:
                rec[name] = f["default"]
            elif f["nullable"]:
                rec[name] = None
            else:
                reasons.append(f"row {i}: missing required field {name!r}")
                rec[name] = None
        out.append(rec)
    if reasons:
        shown, rest = reasons[:20], reasons[20:]
        if rest:
            shown.append(f"... and {len(rest)} more violation(s)")
        raise SchemaValidationError(shown)
    return out


def build_table(schema: dict, normalized_rows: list[dict]) -> pa.Table:
    """把 validate_rows 的产物写成指定结构的 Arrow 表（列序 = 字段定义序）。"""
    arrays = [
        pa.array([r[f["name"]] for r in normalized_rows], type=arrow_type(f["type"]))
        for f in schema["fields"]
    ]
    return pa.Table.from_arrays(arrays, schema=arrow_schema(schema))


# --------------------------------------------------------------------------
# 读取映射：物理文件 -> 指定结构版本的逻辑表（扫描 / 压实共用）
# --------------------------------------------------------------------------
def project_file(path: str, file_schema: dict, read_schema: dict) -> pa.Table:
    """把一个物理 Parquet 文件映射为 read_schema 要求的逻辑表。

    物理列按**文件写入时的结构版本**（file_schema）解析出字段 id，再按
    read_schema 落到当前名称与类型：

      - 文件缺少的列：按 read_schema 的 default 或 null 补齐；
      - 类型已无损扩宽的列：cast 到目标类型（扩宽是精确的，不会失败）；
      - 文件里多出的、read_schema 已不包含的列：忽略（不进入逻辑表）。
    """
    file_by_id = {f["id"]: f for f in file_schema["fields"]}
    physical_names = set(pq.read_schema(path).names)
    plan = []  # (read_field, physical_name or None)
    for rf in read_schema["fields"]:
        ff = file_by_id.get(rf["id"])
        if ff is None or ff["name"] not in physical_names:
            plan.append((rf, None))
        else:
            plan.append((rf, ff["name"]))

    to_read = [pname for _, pname in plan if pname is not None]
    if to_read:
        physical = pq.read_table(path, columns=to_read)
        num_rows = physical.num_rows
    else:
        physical = None
        num_rows = pq.read_metadata(path).num_rows

    arrays = []
    for rf, pname in plan:
        target = arrow_type(rf["type"])
        if pname is None:
            if "default" in rf:
                fill = rf["default"]
            elif rf["nullable"]:
                fill = None
            else:
                raise SchemaError(
                    f"field {rf['name']!r} (id {rf['id']}): column missing in "
                    f"{path} and the field has no default"
                )
            arrays.append(pa.array([fill] * num_rows, type=target))
            continue
        col = physical.column(pname)
        if col.type != target:
            col = col.cast(target)  # 演进规则保证这里只会是无损扩宽
        arrays.append(col)
    return pa.Table.from_arrays(arrays, schema=arrow_schema(read_schema))


def file_schema_of(chain: list[dict], file_meta: dict) -> dict:
    """数据文件写入时的结构定义（缺省 schema_version=0，即旧式隐式结构）。"""
    return schema_by_version(chain, int(file_meta.get("schema_version", 0)))


# --------------------------------------------------------------------------
# 旧式写入（row_id/value 二元组、摄取消息）与当前结构的兼容性
# --------------------------------------------------------------------------
def legacy_write_mapping(schema: dict) -> tuple[str, str] | None:
    """旧式写入能否落到这份结构上：返回 (id1 的当前名, id2 的当前名) 或 None。

    旧式追加/摄取只提供 (row_id, value) 两个值，按**字段 id** 1/2 映射到
    当前列名（改名不影响）：id 1 必须是 string 列（row_id 恒为字符串），
    id 2 必须是能接受整数值的列（具体取值范围由 validate_rows 按当前
    类型逐值校验）；其余列必须可空或带默认值，否则旧式写入无法诚实地
    填满当前结构，调用方必须改用结构感知的写入入口。
    """
    f1 = field_by_id(schema, LEGACY_ROW_ID_FIELD)
    f2 = field_by_id(schema, LEGACY_VALUE_FIELD)
    if f1 is None or f1["type"] != "string":
        return None
    if f2 is None or f2["type"] not in ("int32", "int64", "float32", "double"):
        return None
    for f in schema["fields"]:
        if f["id"] in (LEGACY_ROW_ID_FIELD, LEGACY_VALUE_FIELD):
            continue
        if f["nullable"] or "default" in f:
            continue
        return None
    return f1["name"], f2["name"]
