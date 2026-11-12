"""隐私变换原语与输出列规划（纯函数，不做任何 IO）。

两类转换：

  - **稳定假名**：``HMAC-SHA256(key, domain || value)`` 的前 16 字节十六
    进制（128 bit，32 个十六进制字符）。HMAC 是确定性 PRF：同一 key 下
    同一客户编号在任意进程、任意表、任意运行中都得到同一假名，因此两张
    表可以按假名连接；不同客户编号在 128 bit 空间发生碰撞的概率可忽略
    （2^-128 量级），满足“不同客户不能映射成同一值”。固定域分隔前缀让
    假名与引擎内其它散列用途互不串扰。
  - **时间截断**：timestamp 列截到日或月（``pc.floor_temporal``），输出
    仍是同精度的 timestamp，可空性不变，null 仍为 null。

密钥纪律：本模块只接收已经解析好的 key 字节，**不**读取环境、不打印、
不写入任何结构；key 不进异常文本（异常只引用字段 id/别名/动作）。
"""

from __future__ import annotations

import hashlib
import hmac

import pyarrow as pa
import pyarrow.compute as pc

from .. import schema_evo
from ..errors import PrivacyViewError
from .rules import (
    HIDE,
    PSEUDONYMIZE,
    ACTION_REQUIRED_TYPE,
)

# 域分隔前缀：固定、公开、与输入值用长度前缀风格隔开，避免“拼接歧义”
# （例如不同字符串在简单 concat 下撞车）。前缀本身不含任何敏感信息。
_PSEUDONYM_DOMAIN = b"lake-compactor/privacy-pseudonym/v1\x00"
_PSEUDONYM_HEX_LEN = 32  # 16 字节 -> 32 个十六进制字符

_ARROW_TIME_UNIT = {"day": "day", "month": "month"}


def pseudonym(value: str | None, key: bytes) -> str | None:
    """单个客户编号 -> 稳定假名（None 原样透传）。"""
    if value is None:
        return None
    mac = hmac.new(key, _PSEUDONYM_DOMAIN + value.encode("utf-8"), hashlib.sha256)
    return mac.hexdigest()[:_PSEUDONYM_HEX_LEN]


def pseudonym_array(array: pa.Array, key: bytes) -> pa.Array:
    """对整个 string 批次计算假名；null 位置保持 null，不参与 HMAC。

    按批转换（批次大小由读取侧控制），任何时刻内存里只有一个批次的
    明文值与一个批次的假名。
    """
    if pa.types.is_null(array.type):
        return pa.nulls(array.length, type=pa.string())
    values = array.to_pylist()
    return pa.array(
        [pseudonym(v, key) if v is not None else None for v in values],
        type=pa.string(),
    )


def truncate_array(array: pa.Array, granularity: str) -> pa.Array:
    """把 timestamp 批次截到 day/month；null 保持 null，类型/可空性不变。"""
    unit = _ARROW_TIME_UNIT.get(granularity)
    if unit is None:  # 规范化阶段已拦截，这里只是防御
        raise PrivacyViewError(
            f"unsupported truncation granularity {granularity!r}", kind="rule"
        )
    if pa.types.is_null(array.type):
        return pa.nulls(array.length, type=schema_evo.arrow_type("timestamp"))
    return pc.floor_temporal(array, unit=unit)


# --------------------------------------------------------------------------
# 输出列规划：把“所选提交的字段定义 + 规则”编译成确定的输出结构
# --------------------------------------------------------------------------
class OutputColumn:
    """输出中的一列。

    kind:
      - "passthrough"     原样透传：source_id 是字段 id，列名/类型取当前定义；
      - "pseudonym"       假名列：source_id 的 string 值经 HMAC 替换，列名 alias；
      - "truncate"        时间截断列：source_id 的 timestamp 截断，列名 alias；
    hide 的字段根本不产生 OutputColumn。
    """

    __slots__ = ("kind", "source_id", "name", "arrow_type", "nullable",
                 "granularity")

    def __init__(self, kind, source_id, name, arrow_type, nullable,
                 granularity=None):
        self.kind = kind
        self.source_id = source_id
        self.name = name
        self.arrow_type = arrow_type
        self.nullable = nullable
        self.granularity = granularity


def plan_output_columns(read_schema: dict, rules: list[dict]) -> list[OutputColumn]:
    """编译输出列规划，并在交出第一批数据前完成全部静态校验。

    校验（全部在视图建立阶段、读取数据行之前）：
      - 每条规则绑定的字段 id 必须存在于所选提交的结构，否则 kind="field"；
      - 假名目标必须是 string、时间截断目标必须是 timestamp，
        否则 kind="type"；
      - 转换别名彼此不同，且不与任何透传列的当前列名冲突（否则输出 schema
        会出现重名列，或让敏感原列名借别名重新出现），kind="rule"；
      - hide 只要求字段存在，对类型无要求。

    输出列顺序：先按结构定义顺序输出全部透传列与 hide 之外的字段，再按
    规则给出的顺序追加转换列。这样透传列的相对顺序与表结构一致，转换列
    的位置由调用方规则决定且确定可复现。
    """
    fields_by_id = {f["id"]: f for f in read_schema["fields"]}
    rules_by_id = {r["field_id"]: r for r in rules}

    errors: list[str] = []
    columns: list[OutputColumn] = []
    used_names: set[str] = set()

    # 1) 结构驱动：透传列 + hide 检查
    for field in read_schema["fields"]:
        fid = field["id"]
        rule = rules_by_id.get(fid)
        if rule is None:
            if field["name"] in used_names:  # 结构本身重名（演进规则已禁止，防御）
                errors.append(
                    f"output column name {field['name']!r} would be duplicated"
                )
                continue
            used_names.add(field["name"])
            columns.append(
                OutputColumn(
                    "passthrough", fid, field["name"],
                    schema_evo.arrow_type(field["type"]), field["nullable"],
                )
            )
            continue
        if rule["action"] == HIDE:
            continue  # 不进入输出
        # 转换动作：原列不直接输出，位置留给后面的转换列
        _check_transform_target(field, rule, errors)

    # 2) 规则驱动：确认所有规则都能解析到字段，并追加转换列
    for rule in rules:
        fid = rule["field_id"]
        field = fields_by_id.get(fid)
        if field is None:
            errors.append(
                f"rule references field_id {fid} which does not exist in the "
                "selected table state"
            )
            continue
        if rule["action"] == HIDE:
            continue
        alias = rule["alias"]
        if alias in used_names:
            errors.append(
                f"output alias {alias!r} (field_id {fid}) collides with a "
                "passthrough column name or another alias"
            )
            continue
        used_names.add(alias)
        if rule["action"] == PSEUDONYMIZE:
            columns.append(
                OutputColumn(
                    "pseudonym", fid, alias, pa.string(), field["nullable"]
                )
            )
        else:
            columns.append(
                OutputColumn(
                    "truncate", fid, alias,
                    schema_evo.arrow_type("timestamp"), field["nullable"],
                    granularity=rule["granularity"],
                )
            )

    if errors:
        # 一次性给出全部静态问题；文本只含字段 id/别名/动作，不含数据值。
        raise PrivacyViewError(
            "privacy view rejected before any batch was produced: "
            + "; ".join(errors),
            kind=_classify_errors(errors),
        )
    return columns


def _check_transform_target(field: dict, rule: dict, errors: list[str]) -> None:
    required = ACTION_REQUIRED_TYPE[rule["action"]]
    if field["type"] != required:
        errors.append(
            f"field_id {field['id']} ({rule['action']}): target type "
            f"{field['type']!r} is not suitable, required {required!r}"
        )


def _classify_errors(messages: list[str]) -> str:
    joined = " ".join(messages).lower()
    if "does not exist" in joined:
        return "field"
    if "not suitable" in joined or "required" in joined:
        return "type"
    return "rule"


def output_arrow_schema(columns: list[OutputColumn]) -> pa.Schema:
    """输出 Arrow schema：只含允许的别名/透传列名，且不带任何 metadata。

    显式构造空 metadata：即使底层 Parquet 或读取批次带 metadata，输出
    批次也不继承，避免敏感字段名等借 metadata 泄漏。
    """
    return pa.schema(
        [
            pa.field(col.name, col.arrow_type, nullable=col.nullable)
            for col in columns
        ],
        metadata=None,
    )


def transform_batch(
    logical_batch: pa.RecordBatch,
    read_schema: dict,
    columns: list[OutputColumn],
    key: bytes,
) -> pa.RecordBatch:
    """把一个“逻辑结构批次”变换为输出批次。

    logical_batch 的列与 ``read_schema["fields"]`` 同序（读取侧按字段 id
    投影后的逻辑批次），因此字段 id -> 列位置的映射直接由 read_schema
    决定，列随后如何改名都不影响。本函数按输出规划逐列取数：透传列原样
    引用，假名列做 HMAC，截断列做 floor。null 语义与可空性由各 Arrow
    算子保持。
    """
    id_to_index = {
        field["id"]: i for i, field in enumerate(read_schema["fields"])
    }
    arrays = []
    for col in columns:
        data = logical_batch.column(id_to_index[col.source_id])
        if col.kind == "passthrough":
            arrays.append(data)
        elif col.kind == "pseudonym":
            arrays.append(pseudonym_array(data, key))
        elif col.kind == "truncate":
            arrays.append(truncate_array(data, col.granularity))
        else:  # pragma: no cover - 规划阶段已保证不会出现
            raise PrivacyViewError(
                f"internal error: unknown output column kind {col.kind!r}"
            )
    return pa.RecordBatch.from_arrays(
        arrays, schema=output_arrow_schema(columns)
    )
