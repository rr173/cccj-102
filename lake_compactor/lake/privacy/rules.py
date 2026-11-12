"""隐私视图字段规则的规范化与校验。

规则绑定**稳定字段 id**而不是列名：字段改名不改变 id，因此同一份规则
在列改名后继续作用于目标列；列被删除（id 从所选提交的结构里消失）后，
规则在视图建立阶段即以 field 错误拒绝，绝不先交出数据。

三种动作（每条规则只作用于一个字段 id）：

  - ``hide``          该字段不进入输出（原始列名与值都不出现）；
  - ``pseudonymize``  string 列替换为稳定假名，输出列名取 ``alias``；
  - ``truncate_time`` timestamp 列截到 ``day`` 或 ``month``，输出列名
                      取 ``alias``（截断列仍为 timestamp 类型，null 语义
                      与可空性保持不变）。

没有规则覆盖的字段按原列名、原类型原样透传。规则只描述“怎么变换”，
不包含任何密钥材料：HMAC key 只经进程环境或内存参数提供（见 view.py），
永远不进入规则对象、配置、manifest、Arrow metadata、异常文本或日志。
"""

from __future__ import annotations

from collections.abc import Mapping

from ..errors import PrivacyViewError

HIDE = "hide"
PSEUDONYMIZE = "pseudonymize"
TRUNCATE_TIME = "truncate_time"

ACTIONS = (HIDE, PSEUDONYMIZE, TRUNCATE_TIME)
TRANSFORM_ACTIONS = (PSEUDONYMIZE, TRUNCATE_TIME)

GRANULARITIES = ("day", "month")

# 各动作允许/要求的键。alias 对转换动作是必填（输出里不允许保留原列名），
# hide 不接受 alias（该字段根本不输出）。
_ACTION_KEYS = {
    HIDE: {"field_id", "action"},
    PSEUDONYMIZE: {"field_id", "action", "alias"},
    TRUNCATE_TIME: {"field_id", "action", "alias", "granularity"},
}

# 目标列的逻辑类型要求（与 schema_evo 类型注册表一致）。
ACTION_REQUIRED_TYPE = {
    PSEUDONYMIZE: "string",
    TRUNCATE_TIME: "timestamp",
}


def _rule_error(message: str) -> PrivacyViewError:
    return PrivacyViewError(message, kind="rule")


def _normalize_field_id(raw) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 1:
        raise _rule_error(
            f"field_id must be a positive integer, got {raw!r}"
        )
    return int(raw)


def normalize_rules(raw_rules) -> list[dict]:
    """把调用方给出的规则列表规范化为内部形状。

    接受列表/元组，每项是含 ``field_id`` 与 ``action`` 的映射。规范化在
    视图建立阶段、读取任何数据行之前完成：任何形状错误都抛
    PrivacyViewError(kind="rule")，不产生批次、不建立租约。

    返回的规则保持调用方给出的顺序（输出列规划也按这个顺序处理转换列），
    每条规则形如::

        {"field_id": int, "action": str, "alias": str|None,
         "granularity": str|None}
    """
    if raw_rules is None:
        return []
    if not isinstance(raw_rules, (list, tuple)):
        raise _rule_error(
            "privacy rules must be a list of field rule mappings, got "
            f"{type(raw_rules).__name__}"
        )

    normalized: list[dict] = []
    seen_field_ids: set[int] = set()
    # 转换列的别名必须彼此不同，也不能与任何透传列的当前列名冲突
    # （后者在 view 建立时结合结构定义复核，这里先查规则内部冲突）。
    seen_aliases: set[str] = set()

    for index, raw in enumerate(raw_rules):
        where = f"privacy rule #{index}"
        if not isinstance(raw, Mapping):
            raise _rule_error(f"{where}: rule must be a mapping")
        action = raw.get("action")
        if not isinstance(action, str) or action not in ACTIONS:
            raise _rule_error(
                f"{where}: action must be one of {list(ACTIONS)}, got {action!r}"
            )
        field_id = _normalize_field_id(raw.get("field_id"))
        if field_id in seen_field_ids:
            raise _rule_error(
                f"{where}: duplicate rule for field_id {field_id} (each field "
                "may carry at most one rule)"
            )

        unknown = sorted(set(raw) - _ACTION_KEYS[action])
        if unknown:
            raise _rule_error(
                f"{where}: unsupported key(s) {unknown} for action {action!r}"
            )

        rule = {"field_id": field_id, "action": action,
                "alias": None, "granularity": None}

        if action == HIDE:
            pass
        else:
            alias = raw.get("alias")
            if not isinstance(alias, str) or not alias:
                raise _rule_error(
                    f"{where}: action {action!r} requires a non-empty "
                    "string 'alias' for the output column"
                )
            if not _is_simple_alias(alias):
                raise _rule_error(
                    f"{where}: alias {alias!r} must be a non-empty string "
                    "without NUL/newline characters"
                )
            if alias in seen_aliases:
                raise _rule_error(
                    f"{where}: output alias {alias!r} is used by more than "
                    "one rule"
                )
            seen_aliases.add(alias)
            rule["alias"] = alias
            if action == TRUNCATE_TIME:
                granularity = raw.get("granularity")
                if granularity not in GRANULARITIES:
                    raise _rule_error(
                        f"{where}: truncate_time granularity must be one of "
                        f"{list(GRANULARITIES)}, got {granularity!r}"
                    )
                rule["granularity"] = granularity

        seen_field_ids.add(field_id)
        normalized.append(rule)

    return normalized


def _is_simple_alias(alias: str) -> bool:
    return (
        isinstance(alias, str)
        and bool(alias)
        and "\x00" not in alias
        and "\n" not in alias
        and "\r" not in alias
    )


def rules_by_field(rules: list[dict]) -> dict[int, dict]:
    return {rule["field_id"]: rule for rule in rules}
