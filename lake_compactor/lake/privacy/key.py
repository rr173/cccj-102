"""HMAC key 的解析与校验（key 只存在于进程内存）。

key 的两个来源，按优先级：

  1. 调用方在内存里显式传入的 ``hmac_key`` 参数（bytes 或 str）；
  2. 进程环境变量（默认 ``LAKE_PRIVACY_HMAC_KEY``，可用 ``env_var``
     改名）。

明确**不做**的事：

  - 不读配置文件、不读 manifest、不接受落盘路径；
  - 不把 key 写进任何对象的可持久化字段、Arrow metadata、日志或异常文本
    （异常只说“缺少/形状不合法”，绝不回显 key 内容）；
  - 不做缓存到磁盘的动作。解析结果只是函数内的 bytes，视图关闭后随
    Python 对象回收。

只要规则里存在 pseudonymize 动作就必须有 key；没有该动作时无需 key。
"""

from __future__ import annotations

import os

from ..errors import PrivacyViewError

DEFAULT_ENV_VAR = "LAKE_PRIVACY_HMAC_KEY"


def resolve_key(
    hmac_key=None,
    *,
    env_var: str = DEFAULT_ENV_VAR,
    require: bool = True,
) -> bytes | None:
    """解析 HMAC key，返回规范化后的 bytes；不需要且未提供时返回 None。

    内存参数优先于环境变量，避免同一进程里环境残留悄悄改变假名口径。
    缺 key（且 require）抛 PrivacyViewError(kind="key")，错误文本不含
    key 值本身。
    """
    if hmac_key is not None:
        return _coerce_key(hmac_key)

    if not isinstance(env_var, str) or not env_var:
        raise PrivacyViewError(
            "env_var must be a non-empty environment variable name",
            kind="key",
        )

    env_value = os.environ.get(env_var)
    if env_value is not None:
        if env_value == "":
            raise PrivacyViewError(
                f"environment variable {env_var} is set but empty; an empty "
                "HMAC key is not accepted",
                kind="key",
            )
        return _coerce_key(env_value)

    if require:
        raise PrivacyViewError(
            "missing HMAC key for pseudonymization: provide it via the "
            f"hmac_key argument or the {env_var} environment variable; the "
            "key is never read from config files or manifests",
            kind="key",
        )
    return None


def _coerce_key(raw) -> bytes:
    """把调用方给的 key 规范为非空 bytes；类型/空值明确拒绝。"""
    if isinstance(raw, bytes):
        key = raw
    elif isinstance(raw, str):
        if raw == "":
            raise PrivacyViewError(
                "HMAC key must be a non-empty value, got an empty string",
                kind="key",
            )
        key = raw.encode("utf-8")
    else:
        raise PrivacyViewError(
            "HMAC key must be bytes or a UTF-8 string, got "
            f"{type(raw).__name__}",
            kind="key",
        )
    if not key:
        raise PrivacyViewError(
            "HMAC key must be a non-empty value, got empty bytes",
            kind="key",
        )
    return key


def rules_require_key(rules: list[dict]) -> bool:
    from .rules import PSEUDONYMIZE

    return any(rule["action"] == PSEUDONYMIZE for rule in rules)
