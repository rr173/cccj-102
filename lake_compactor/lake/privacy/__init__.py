"""隐私视图（privacy view）：合作方隔离联调用的临时、只读、逐批脱敏入口。

面向“合作方需要在隔离环境里联调模型，但不能拿到生产客户编号或精确事件
时间”的场景。调用方选择一个**已提交表状态**与一组绑定稳定字段 id 的
规则，迭代取得处理后的 Arrow 批次：

  - 隐藏字段（``hide``）：整列不进入输出；
  - 客户编号稳定假名（``pseudonymize``）：HMAC-SHA256 派生，同一 key 下
    两张表的同一客户在不同运行中仍能匹配，不同客户不会撞值；
  - 事件时间截断（``truncate_time``）：截到日或月，仍是 timestamp。

结果仅供当前进程消费：

  - 不写回湖表、不改版本/manifest、不生成可单独打开的导出目录；
  - 不保存转换后的数据，按 batch 前进，消费者取完即释放；
  - 持读者租约保护所选版本，提前关闭 / 下游抛错 / 读取失败都会释放保护；
  - key 只经进程环境或内存参数提供，不进配置、manifest、Arrow metadata、
    异常文本或日志；
  - 不运行 SQL、不接受 Python 表达式。

典型用法
--------

    from lake.privacy import open_privacy_view

    rules = [
        {"field_id": 3, "action": "hide"},
        {"field_id": 4, "action": "pseudonymize", "alias": "customer_pid"},
        {"field_id": 5, "action": "truncate_time",
         "granularity": "day", "alias": "event_day"},
    ]
    with open_privacy_view(table, rules, version=12,
                           hmac_key=b"shared-secret") as view:
        for batch in view:
            ...  # batch 只含允许的别名与转换值

缺 key、规则指向已不存在的字段、目标列类型不合适等都在交出第一批结果前
抛 :class:`~lake.errors.PrivacyViewError`（``kind`` 区分 key/rule/field/
type/version/read）。
"""

from .key import DEFAULT_ENV_VAR, resolve_key
from .rules import (
    ACTIONS,
    GRANULARITIES,
    HIDE,
    PSEUDONYMIZE,
    TRUNCATE_TIME,
    normalize_rules,
)
from .transform import (
    OutputColumn,
    output_arrow_schema,
    plan_output_columns,
    pseudonym,
    truncate_array,
)
from .view import (
    DEFAULT_BATCH_SIZE,
    PrivacyView,
    open_privacy_view,
    privacy_batches,
)

__all__ = [
    "open_privacy_view",
    "privacy_batches",
    "PrivacyView",
    "normalize_rules",
    "resolve_key",
    "plan_output_columns",
    "output_arrow_schema",
    "pseudonym",
    "truncate_array",
    "OutputColumn",
    "HIDE",
    "PSEUDONYMIZE",
    "TRUNCATE_TIME",
    "ACTIONS",
    "GRANULARITIES",
    "DEFAULT_ENV_VAR",
    "DEFAULT_BATCH_SIZE",
]
