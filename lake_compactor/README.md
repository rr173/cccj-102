# lake-compactor

小型湖表压实引擎：为离线训练集整理合并碎片 Parquet、物化位置删除，并为
训练数据接入端提供可重放的微批摄取事务。
Python + PyArrow + DuckDB，元数据用本机文件与 flock 实现，单机多进程并发。

- 不可变 manifest 版本 + HEAD 原子翻转 = 跨进程原子发布点
- 位置删除显式引用 `(data_file_id, 0 起物理行号)`，不从业务字段猜行号
- 压实提交时比对版本：保留并发追加、翻译新到删除、重叠选文件则冲突失败
- 微批摄取事务：源端消费游标、请求摘要、receipt 与本批数据在同一个
  manifest 版本里原子生效，确认超时重发不会重复入仓
- 读者租约保护旧版本，GC 不清租约引用的文件

## 运行

```bash
./run.sh                 # 自动化校验 + 端到端演示
# 或分别运行：
python3 -m pytest -v     # 22 个端到端用例（真实读写 Parquet、真实多进程交错）
python3 demo.py          # 叙述式演示 12 个场景
```

依赖：`pyarrow`、`duckdb`、`pytest`（可用 `PYTHON=/path/to/python` 指定解释器）。

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
| `tests/test_engine.py` | 压实引擎校验（屏障交错、冲突、崩溃、租约、原子发布压力） |
| `tests/test_ingest.py` | 摄取事务校验（多分片、重发、争抢、并发、崩溃恢复、旧表兼容） |
| `demo.py` | 叙述式端到端演示 |
| `ARCHITECTURE.md` | 架构说明 |
