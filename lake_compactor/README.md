# lake-compactor

小型湖表压实引擎：为离线训练集整理合并碎片 Parquet、物化位置删除，并为
训练数据接入端提供可重放的微批摄取事务。
Python + PyArrow + DuckDB，元数据用本机文件与 flock 实现，单机多进程并发。

- 不可变 manifest 版本 + HEAD 原子翻转 = 跨进程原子发布点
- 位置删除显式引用 `(data_file_id, 0 起物理行号)`，不从业务字段猜行号
- 压实提交时比对版本：保留并发追加、翻译新到删除、重叠选文件则冲突失败
- 微批摄取事务：源端消费游标、请求摘要、receipt 与本批数据在同一个
  manifest 版本里原子生效，确认超时重发不会重复入仓
- 微批摄取事务：源端消费游标、请求摘要、receipt 与本批数据在同一个
  manifest 版本里原子生效，确认超时重发不会重复入仓
- 读者租约保护旧版本，GC 不清租约引用的文件
- **数据质量画像**：按修订号冻结完整质量报告（样本量/空字段/重复标识/
  数值极值合计/左闭右开分段计数/规则逐条判定），报告只读、自包含，
  支持两份报告的指标差值与异常新出现/消失/持续比较（见 [QUALITY.md](QUALITY.md)）

## 运行

```bash
./run.sh                 # 自动化校验 + 压实/摄取演示 + 质量画像演示
# 或分别运行：
python3 -m pytest -v     # 52 个端到端用例（真实读写 Parquet、真实多进程交错）
python3 demo.py          # 叙述式演示 12 个场景
python3 demo_quality.py  # 质量画像 10 个验收场景（本地样本、可手算核对）
```

依赖：`pyarrow`、`duckdb`、`pytest`（可用 `PYTHON=/path/to/python` 指定解释器）。

## 数据质量画像（quality profile）

```python
from lake import LakeTable
from lake.quality import ProfileStore, compare_reports

store = ProfileStore(LakeTable.open("./mylake"))
report = store.create_or_get(rules)                 # 规则见 QUALITY.md/examples/
again  = store.create_or_get(rules)                 # 同（修订号,规则摘要）-> 复用原报告
other  = store.create_or_get(other_rules)           # 不同规则 -> 另存一份，不覆盖
diff   = compare_reports(old_report, new_report)    # 指标差值 + 异常迁移
```

- 唯一身份 = 数据修订号 + 规则摘要（规范化规则的 SHA-256）；报告发布后
  置 0444 只读，任何写入/整理/回收都不能改写；
- 计数基于固定修订号的**查询结果**（位置删除过滤后的可见行），分批
  RecordBatch 扫描，全表不进 Python 列表；扫描期间持读者租约，GC 不会
  提前删除输入；
- 数据触犯规则仍保存完整画像并逐条列出失败项；规则格式错误直接拒绝、
  不落盘；
- 临时文件 + fsync + 原子 rename 发布；发布前崩溃只留临时残骸，重复
  请求自动恢复；报告独立于原始数据，GC 后仍可打开和比较；
- `lake/quality/reference.py` 是与生产扫描逻辑分开的纯 Python 参考
  计算程序，供小样本手算交叉核对。

CLI：`python3 -m lake.quality.cli {create,show,list,compare,reference} ...`

## 微批摄取契约（ingest_batch）

接入端按批调用，一批可含多个来源分片的消息：

```python
result = table.ingest_batch(batch_key, messages)
# messages: [{"source": str, "partition": str|int, "offset": int,
#             "row_id": str, "value": int}, ...]   批内允许乱序
# result: {"receipt": str, "batch_key": str, "version": int,
#          "cursors": {source: {partition: next_offset}},   # 本批涉及的分片
#          "num_rows": int, "replayed": bool}
```

- 每个分片 `(source, partition)` 的 offset 从 0 起连续编号；一批必须恰好
  覆盖各分片当前游标之后的连续区间。缺号、批内重复编号、与既往区间
  交叠 → 抛 `BatchRejectedError`（`reasons` 含逐分片原因），**整批**拒绝，
  任何分片游标都不推进；未出现在批内的分片维持原游标。
- 相同 `batch_key` + 相同规范化内容（与输入顺序无关）→ 返回原
  receipt（`replayed=True`），不新增行、不推进版本；相同 key 不同内容 →
  抛 `IdempotencyConflictError`（携带原 receipt / version）。
- 不同 `batch_key` 争抢同一区间：由游标规则裁决，最多一个成功。
- 请求形状不合法（缺字段、负 offset、空批等）→ `ValueError`，与表状态无关。
- 进度查询：`table.ingest_cursors(version=None)`（可指定历史版本）、
  `table.batch_record(batch_key)`（已提交批次的完整记录，未提交为 None）。
- 读取契约不变：摄取的行就是普通数据文件，`scan(version)` 直接可见；
  删除、压实、租约、GC 对摄取文件一视同仁。

崩溃语义：准备阶段硬退出不消费任何位置（重发 = 全量重做）；HEAD 翻转后、
确认返回前硬退出，重启后用同一 `batch_key` 重放即可拿到原 receipt。

## 代码结构

| 文件 | 职责 |
|---|---|
| `lake/storage.py` | 原子写（tmp+fsync+rename）、目录 fsync、flock 锁 |
| `lake/manifest.py` | manifest 版本读写、HEAD 原子发布点 |
| `lake/table.py` | 建表、追加、发布位置删除、扫描、读者租约、摄取入口 |
| `lake/ingest.py` | 微批摄取事务：消息规范化、游标校验、幂等重放、崩溃钩子 |
| `lake/compact.py` | 生成压实计划（staging 候选+位置映射）、提交计划（冲突检测/删除翻译） |
| `lake/gc.py` | 租约感知的垃圾回收（含孤儿批次记录、在途临时文件年龄保护） |
| `lake/verify.py` | DuckDB 多重集校验 |
| `lake/quality/rules.py` | 画像规则规范化、格式校验、确定性 SHA-256 摘要 |
| `lake/quality/profile.py` | 分批流式扫描与指标聚合（全表不入 Python 列表） |
| `lake/quality/store.py` | 报告冻结存储：唯一身份、租约保护、原子发布、崩溃钩子 |
| `lake/quality/compare.py` | 报告比较：指标差值、异常新出现/消失/持续 |
| `lake/quality/reference.py` | 独立纯 Python 参考计算程序（与生产路径分开） |
| `lake/quality/cli.py` | 质量画像命令行入口（create/show/list/compare/reference） |
| `tests/test_engine.py` | 压实引擎校验（屏障交错、冲突、崩溃、租约、原子发布压力） |
| `tests/test_ingest.py` | 摄取事务校验（多分片、重发、争抢、并发、崩溃恢复、旧表兼容） |
| `tests/test_quality.py` | 质量画像验收（手算样本、整理一致、删行、违规、错配、复用、租约、崩溃、GC 后可用） |
| `demo.py` | 叙述式端到端演示 |
| `demo_quality.py` | 质量画像叙述式演示（10 个验收场景） |
| `QUALITY.md` | 画像规则配置、报告字段、接口与存储语义说明 |
| `ARCHITECTURE.md` | 架构说明 |
