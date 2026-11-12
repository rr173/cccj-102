# 字段身份与逻辑结构演进（SCHEMA）

训练特征在生产者滚动升级时会逐步增加或调整列，湖表里新旧物理格式的
Parquet 文件同时存在。本模块给每张表一套**字段身份**机制：所有读写入口
（扫描 / 追加 / 微批摄取 / 压实）共用同一套字段映射规则，任何入口都不
自行猜测字段含义；生产者可以分批切换格式，历史数据无需立即重写。

## 1. 字段定义与结构版本

一张表的字段定义（`lake/schema_evo.py`）：

```json
{"version": 4,
 "fields": [
   {"id": 1, "name": "row_id",     "type": "string", "nullable": false},
   {"id": 2, "name": "confidence", "type": "int64",  "nullable": true},
   {"id": 3, "name": "tag",        "type": "string", "nullable": true}
 ]}
```

- **稳定 id**：字段身份。改名不改 id；id 永不复用（已删除字段的 id 也不
  许回收，否则旧文件的物理列会被错映射到新字段）。
- **name**：当前逻辑名称。扫描输出、行校验都按当前名称。
- **type**：逻辑类型，取值为 `bool / int32 / int64 / float32 / double /
  string`。
- **nullable**：是否允许空值。旧文件缺少该列时按 `default`（如有）或
  null 解释。
- **default**（可选）：缺列时的填充值；新增**必填**列必须带默认值。

**结构版本**是表级单调递增整数：**0 保留给旧式 `row_id/value` 隐式结构**
（没有结构元数据的旧湖表），建表时的首份显式结构是 v1，每次演进 +1。

## 2. 演进规则（`LakeTable.evolve_schema`）

```python
table.evolve_schema(fields, expected_schema_version=None)
# fields: 完整的新字段定义列表（不是增量）
# 返回 {"changed": bool, "schema_version": int, "version": 提交号|None, "schema": {...}}
```

| 变更 | 规则 |
|---|---|
| 新增可空列 | 允许；旧文件缺少的值按 null 解释 |
| 新增必填列 | 必须带 `default`，否则拒绝（旧文件缺少该列无法解释） |
| 改列名 | 允许；id 不变，物理文件不重写 |
| 类型变更 | 只允许无损扩宽：`int32→int64`、`int32→double`、`float32→double`；`int64→double`（>2^53 丢精度）与其余变更一律拒绝 |
| 可空 → 必填 | 拒绝（既有文件可能含 null） |
| 删除列 | 仍被数据文件引用的列拒绝删除（文件按其写入时的结构版本引用字段） |
| 重复 id / 重复名字 / 未知类型 / 复用已删除字段的 id | 拒绝 |

未带 `id` 的字段按**名字**继承当前同名字段的 id（按名延续身份）；全新
名字分配从未使用过的新 id。**改名必须显式携带 id**，否则与“删旧列 +
加新列”无法区分。提议与当前完全一致时不产生新提交（`changed=False`）。

所有违规一次性收进 `SchemaEvolutionError.reasons`（指出哪条规则不满足）；
拒绝路径只读：结构、版本与数据文件都不变。

## 3. 原子生效与历史一致

结构变更就是一个普通的 manifest 提交：新结构追加进 manifest 的
`"schemas"` 历史（最后一个即当前定义），与数据提交共用同一条原子发布
路径（manifest 落盘 → HEAD 翻转）。因此：

- **拒绝或中断不会留下只改了一半的定义**：崩溃最多在 `versions/` 留下
  不被引用的孤儿 manifest，HEAD 不动，重开后结构原样；
- **重开湖表后结构历史与数据提交天然一致**：两者在同一份历史里；
- 每个数据文件条目记录写入时的结构版本
  （`data_files[fid]["schema_version"]`，缺省 0）。

## 4. 读取映射（扫描 / 压实 / 微批校验共用）

读取提交 V 时，每个数据文件独立映射：

```
物理列名 --(文件自己的结构版本)--> 字段 id --(V 的结构定义)--> 当前名称与类型
```

- 读取**旧提交**：用当时生效的列定义（列名、Arrow 类型都是当时的）；
- 读取**新提交**：各历史格式的文件统一映射到该提交要求的名称和类型；
- 文件缺少的列按 `default` 或 null 补齐；已扩宽的列无损 cast；文件里
  多出的、当前定义已不包含的列忽略。

`scan(version)` 与 `create_plan` 都走 `schema_evo.project_file` 这同一个
函数；压实结果按当前逻辑结构落盘并记录其结构版本，每条可见记录保留。

## 5. 写入契约

**结构感知追加**：

```python
s = table.current_schema()                 # 开始处理时读取结构版本
fid = table.append_rows(rows, schema_version=s["version"])
# rows: [{"row_id": ..., "confidence": ..., ...}, ...]  按字段名给值
```

行先按该版本定义校验（未知列 / 缺必填列 / 必填列给 null / 类型不符 →
`SchemaValidationError`，整批拒绝），再按该版本的物理布局写盘；发布时
在提交锁内**重新确认** `schema_version` 仍是当前定义，结构已变更则抛
`SchemaConflictError`——已准备的文件被清理，**绝不按新定义静默解释**。
调用方重新读取 `current_schema()` 后重试即可。这就是“结构发布与旧结构
写者竞争”的裁决方式：旧写入明确失败，而不是被误读。

**微批摄取**：`ingest_batch` 消息的 `row_id`/`value` 按字段 id 1/2 映射
进当前结构，其余列可用消息的 `fields` 映射按名字给值；校验、落盘、
结构版本复核与追加同规则。不带 `fields` 的旧消息在旧式表上逐字节兼容
（摘要、receipt 不变）。

**旧式调用**（`append((row_id, value))`、不带 `fields` 的摄取）：

- 无结构元数据的旧湖表上：与旧实现完全一致，**无需先做迁移**；
- 已演进的表上：id 1/2 必须仍在且类型兼容（string / 可接受整数），其余
  列必须可空或带默认值（缺省补 null/默认值）——满足则按当前结构落盘，
  不满足则抛 `SchemaError`，请改用 `append_rows`。

## 6. 磁盘格式

manifest 增加一个字段，数据文件条目增加一个字段，其余不变：

```json
{
  "version": 9, "parent": 8, "...": "...",
  "schemas": [{"version": 1, "fields": [...]}, {"version": 2, "fields": [...]}],
  "data_files": {"<id>": {"path": "...", "num_rows": 19, "schema_version": 2}}
}
```

旧格式 manifest 没有 `schemas` 段 → 隐式 v0 结构；旧文件条目没有
`schema_version` → 缺省 0。首次演进时隐式 v0 结构随历史一起物化进
manifest，旧文件一个字节都不改。

## 7. 限制（本题范围）

- 结构历史随 manifest 完整携带（只增不减），演进次数按生产变更频率设计；
- 删除列的“仍被引用”检查覆盖当前 manifest 的文件；进行中的压实计划
  提交的旧结构候选文件由读取映射兜底（历史完整，正确性不依赖该检查）；
- 质量画像 / 切分 / 查询工作台等下游工具按物理列读取，面向旧式表；
  结构受管表请经 `scan()` 消费逻辑视图。

## 8. 运行

```bash
python3 -m pytest tests/test_schema.py -v   # 11 个端到端用例
python3 demo_schema.py                      # 11 步叙述式演示（真实 Parquet）
```
