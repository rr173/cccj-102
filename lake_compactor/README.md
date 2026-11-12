# lake-compactor

小型湖表压实引擎：为离线训练集整理合并碎片 Parquet、物化位置删除。
Python + PyArrow + DuckDB，元数据用本机文件与 flock 实现，单机多进程并发。

- 不可变 manifest 版本 + HEAD 原子翻转 = 跨进程原子发布点
- 位置删除显式引用 `(data_file_id, 0 起物理行号)`，不从业务字段猜行号
- 压实提交时比对版本：保留并发追加、翻译新到删除、重叠选文件则冲突失败
- 读者租约保护旧版本，GC 不清租约引用的文件

## 运行

```bash
./run.sh                 # 自动化校验 + 端到端演示
# 或分别运行：
python3 -m pytest -v     # 8 个端到端用例（真实读写 Parquet、真实多进程交错）
python3 demo.py          # 叙述式演示 7 个场景
```

依赖：`pyarrow`、`duckdb`、`pytest`（可用 `PYTHON=/path/to/python` 指定解释器）。

## 代码结构

| 文件 | 职责 |
|---|---|
| `lake/storage.py` | 原子写（tmp+fsync+rename）、目录 fsync、flock 锁 |
| `lake/manifest.py` | manifest 版本读写、HEAD 原子发布点 |
| `lake/table.py` | 建表、追加、发布位置删除、扫描、读者租约 |
| `lake/compact.py` | 生成压实计划（staging 候选+位置映射）、提交计划（冲突检测/删除翻译） |
| `lake/gc.py` | 租约感知的垃圾回收 |
| `lake/verify.py` | DuckDB 多重集校验 |
| `tests/test_engine.py` | 自动化校验（屏障交错、冲突、崩溃、租约、原子发布压力） |
| `demo.py` | 叙述式端到端演示 |
| `ARCHITECTURE.md` | 架构说明 |
