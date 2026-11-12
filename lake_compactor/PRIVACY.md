# 隐私视图（privacy view）

合作方在隔离环境里联调模型，但不应拿到生产客户编号或精确事件时间。隐私
视图是一个**临时、只读、逐批**的脱敏入口：调用方选择一个**已提交表状态**
与一组绑定**稳定字段 id** 的规则，迭代取得处理后的 Arrow 批次。结果仅供
当前进程消费——不写回湖表、不生成可在之后单独打开的导出目录、不保存转换
后的数据。

```python
from lake.privacy import open_privacy_view

rules = [
    {"field_id": 5, "action": "hide"},                       # 整列隐藏
    {"field_id": 3, "action": "pseudonymize",
     "alias": "customer_pid"},                               # 客户编号 -> 稳定假名
    {"field_id": 4, "action": "truncate_time",
     "granularity": "day", "alias": "event_day"},            # 事件时间截到日
]
with open_privacy_view(table, rules, version=12,
                       hmac_key=b"shared-secret") as view:
    print(view.schema)          # 只含允许的别名/透传列，无 metadata
    for batch in view:          # pyarrow.RecordBatch，逐批转换
        ...
```

## 规则（绑定稳定 field ID）

每条规则是一个映射，作用于一个字段 id；字段改名不改变 id，规则因此在
改名后继续作用于目标列。列被删除（id 从所选提交的结构里消失）后，规则在
**第一批数据交出之前**以 `field` 错误拒绝。

| action | 必填键 | 目标列类型 | 输出 |
|---|---|---|---|
| `hide` | `field_id`, `action` | 任意 | 该列不进入输出（原列名与值都不出现） |
| `pseudonymize` | 加 `alias` | `string` | HMAC 稳定假名，列名取 `alias`，仍是 string |
| `truncate_time` | 加 `alias`, `granularity`（`day`/`month`） | `timestamp` | 截断后的 timestamp，列名取 `alias` |

没有规则覆盖的字段按**原列名、原类型原样透传**。转换列的 `alias` 必须
彼此不同，也不能与任何透传列的当前列名冲突，否则按 `rule` 错误拒绝。

输出列顺序：先按结构定义顺序给出透传列，再按规则顺序追加转换列。输出
Arrow schema 显式不带 metadata（底层 Parquet 的 metadata 也不会继承）。

## 稳定假名（HMAC）

```
假名 = HMAC-SHA256(key, "lake-compactor/privacy-pseudonym/v1\x00" || 客户编号)
       的前 16 字节 -> 32 个十六进制字符
```

- 确定性 PRF：同一 key 下同一客户编号在**任意进程、任意表、任意运行**中
  都得到同一假名，因此特征表与标签表可按假名连接；
- 不同客户编号在 128 bit 空间碰撞概率约 2^-128，满足“不同客户不能映射成
  同一值”；
- 固定域分隔前缀，避免与引擎内其它散列用途串扰；
- 输入为 `null` 时输出仍为 `null`，不参与 HMAC；假名输出列的可空性与
  原列一致。

## 时间截断与空值

`truncate_time` 用 Arrow 的 `floor_temporal` 截到日或月：时分秒（截到日）
或日（截到月）归零，输出仍是 `timestamp[us]`，可空性不变，`null` 仍为
`null`。事件时间列使用结构类型 `timestamp`（Arrow 微秒精度无时区时间戳）。

## HMAC key 的来源与纪律

key 只从两个来源解析，内存参数优先：

1. `open_privacy_view(..., hmac_key=...)`：`bytes` 或 UTF-8 `str`；
2. 进程环境变量 `LAKE_PRIVACY_HMAC_KEY`（可用 `env_var=` 改名）。

只有规则里存在 `pseudonymize` 时才必须提供 key；缺 key、空 key、类型
不合法在第一批数据前以 `key` 错误拒绝。key **绝不**进入：

- 配置文件、manifest、任何落盘结构；
- Arrow schema / RecordBatch metadata；
- 异常文本与日志（错误只引用字段 id、别名、动作，不回显 key 或数据值）；
- 读者租约文件（租约只记录版本、分支、pid、时间）。

视图关闭时主动丢弃持有的 key 引用，随 Python 对象回收。

## 读者保护与资源释放

建立视图时在提交锁内取该版本的读者租约（`lease(..., require_files=True)`），
与 GC 共用同一把锁，因此“定位版本 → 建立保护”之间没有窗口；历史版本文件
若已被回收，以 `version` 错误明确拒绝。

租约在以下所有路径都保证解除（迭代由生成器实现，清理在其 `finally`）：

- 正常读完；
- 消费者提前 `break`（for 循环对生成器 `close()`，触发 `GeneratorExit`）；
- 下游在循环体内抛错（异常在挂起的 `yield` 处重抛，栈展开运行 finally）；
- 读取途中 I/O / Arrow 失败（包装成 `PrivacyViewError(kind="read")`）；
- 显式 `view.close()` 或 `with` 退出（`close` 幂等）。

保护解除后，`collect()` 即可按既有规则回收该历史版本。

## 边界

- 不运行 SQL、不接受 Python 表达式，只执行内置三类规则；
- 不写回湖表、不推进版本、不改 manifest、不产生导出/暂存目录；
- 不保存转换结果：按 `batch_size` 逐批读取与转换，任何时刻内存里只有
  一个批次，消费者取下一批时上一批即可释放；
- 一个视图对应一条受保护扫描，不支持并发重复迭代；需要多次扫描请分别
  打开视图（各自独立取租约）。

## 错误分类

`PrivacyViewError.kind`：

| kind | 触发时机 |
|---|---|
| `key` | 缺 HMAC key、空 key、key 类型不合法 |
| `rule` | 规则形状/动作/粒度/别名非法、同字段多条规则、别名冲突 |
| `field` | 规则绑定的字段 id 在所选提交结构中不存在 |
| `type` | 假名目标不是 string、时间截断目标不是 timestamp |
| `version` | 版本不存在或其文件已被回收 |
| `read` | 读取途中数据文件不可用（I/O / Arrow 错误） |

除 `read` 外，全部在**交出第一批结果之前**抛出，且不残留租约。

## 便捷入口

```python
from lake.privacy import privacy_batches

for batch in privacy_batches(table, rules, hmac_key=key, batch_size=2048):
    ...                       # 结束/异常/break 自动释放租约
```

`privacy_batches` 返回视图对象本身（类生成器迭代器）；需要检查输出
schema、显式关闭或作为上下文管理器时用 `open_privacy_view`。

## 无规则即透传

`rules` 为空列表或 `None` 时，视图按当前结构原样透传，可见行口径（固定
manifest、位置删除过滤、字段 id 读取映射、缺列补 null、无损扩宽）与
`LakeTable.scan` 完全一致，仅交付方式是逐批而非整表。
