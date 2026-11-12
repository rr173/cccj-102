# 按 row_id 成组抽样的数据切分（Split）

为离线训练集产出 train / validation / test 三个可直接读取的 Parquet
子集。抽样单位是**业务对象** `row_id`，而不是行：同一对象的多条训练
观测必然落在同一个子集，评测指标不会因为对象跨集合而虚高。

## 1. 调用参数

| 参数 | 类型 | 含义 |
|---|---|---|
| 湖表 | `LakeTable` | 已打开的本地湖表 |
| 输入 snapshot | `int \| None` | manifest 版本号；`None` = 调用时固定当前 HEAD |
| train / validation / test | 非负整数 | 权重，分母固定 10000，总和必须恰为 10000 |
| seed | 整数 | 散列种子 |

权重是“万分份”：`8000 / 1000 / 1000` 表示长期约 80% / 10% / 10%。
任意权重可以为 0（对应子集产出空 Parquet，schema 完整）。

非法参数一律 `SplitError`，且**整次拒绝、不落盘任何交付物**：

- 任一权重为负、非整数（含 `bool`）、总和 ≠ 10000；
- 输入 snapshot 不存在；
- 输入快照的可见行中存在空 `row_id`（`None` 或 `""`）——空标识无法
  用于对象分组，因此拒绝整次产出，而不是把空标识行当作独立对象。

```python
from lake import LakeTable
table = LakeTable.open("./mylake")
manifest = table.create_split(
    train=8000, validation=1000, test=1000, seed=42,
    version=None,            # 省略 = 调用时 checkout 当前 HEAD
)
```

CLI：

```bash
python3 -m lake.splits.cli create LAKE --train 8000 --validation 1000 \
                                      --test 1000 --seed 42 \
                                      [--snapshot V] [--batch-size N] [--json]
python3 -m lake.splits.cli check  SPLIT_DIR [--json]     # 走同一份校验逻辑
python3 -m lake.splits.cli list   LAKE [--json]
```

## 2. 分配算法（版本 1）

```
分组：同一 row_id 的全部可见行是一个对象组
散列：h = SHA-256(str(seed) + b"\x00" + row_id.encode("utf-8"))[:8] -> uint64
落桶：bucket = h % 10000
区间：train [0, w_train)
      validation [w_train, w_train + w_validation)
      test  [w_train + w_validation, 10000)
```

- 分配只使用 `seed` 与 `row_id`：与 Parquet 文件的物理排列、manifest 键
  序、物理行号、是否先压实全都无关。重排文件或先压实后，逐对象归属不变；
- 对象组在子集内按 `row_id` 排序写出，组内观测保持输入快照中的相对次序，
  子集行的内容因此稳定可核对；
- 子集行带 `split` 与 `split_id` 标签列，便于下游核对来源；原始
  `row_id` / `value` 列保持原样；
- 散列抽样只承诺**长期比例**接近权重。小数据下实际行数/对象数可以
  偏斜（例如 5000/2500/2500 的权重在 10 个对象上可能得到 7/1/2）；清单
  的 `proportionality_notice` 与 `input_snapshot.num_visible_rows` /
  `num_objects` 记录了本次实际分配，下游不应把“数量接近权重”当作硬保证。

公开的复算助手（与生产分配同一份实现）：

```python
from lake.splits import assign_subset, group_hash
assign_subset(seed, "obj-1", {"train": 8000, "validation": 1000, "test": 1000})
group_hash(seed, "obj-1")          # 未取模的 64 位散列值
```

## 3. 交付目录与清单

```
<root>/splits/<split_id>/
  train.parquet
  validation.parquet
  test.parquet
  manifest.json
```

`manifest.json`（`sort_keys` 写出）字段：

```json
{
  "split_id": "…16 hex…",
  "algorithm_version": 1,
  "algorithm": "sha256(seed,row_id)[:8] -> uint64 % 10000 -> weighted bands",
  "weights": {"train": 8000, "validation": 1000, "test": 1000},
  "weight_total": 10000,
  "seed": 42,
  "input_version": 3,
  "input_version_explicit": true,
  "input_snapshot": {
    "version": 3, "version_explicit": true,
    "num_visible_rows": 16, "num_objects": 12
  },
  "grouping": {"key": "row_id", "rule": "…"},
  "proportionality_notice": "…",
  "subsets": {
    "train":      {"path": "train.parquet",
                   "num_rows": 9, "num_objects": 7, "sha256": "…"},
    "validation": {"path": "validation.parquet", "…": "…"},
    "test":       {"path": "test.parquet", "…": "…"}
  },
  "created_at": …,
  "generator": "lake.split"
}
```

`num_rows` / `num_objects` 是**查询结果**口径：固定输入 snapshot 经位置
删除过滤后的可见行，而不是各物理分片 `num_rows` 相加。

## 4. 身份、复用与不可替换

`split_id = SHA-256(算法版本 + 已解析的输入 snapshot + 权重 + seed)[:16]`

- 完全相同参数再次调用：直接返回原清单（`reused=true`），不重写任何
  文件，子集字节与清单 mtime 都不变；
- 参数变化（权重、seed、输入 snapshot 任一不同）：新的 `split_id`，
  新目录，**旧交付绝不替换**；
- 参数相同但最初用 `version=None`（按 HEAD 解析）而 HEAD 已推进：解析出
  的 snapshot 不同 → 新身份，不会悄悄沿用旧身份。

## 5. 发布语义与崩溃恢复

1. 在提交锁下读一次 HEAD（`version=None` 时）→ 固定输入版本号；
2. 持该版本的读者租约，逐文件、逐 `RecordBatch` 流式读取可见行
   （全表不入 Python 列表），按 `row_id` 分组；
3. 三个子集与清单写入 `splits/.staging-<split_id>-<uuid>/`，每个文件
   刷盘后计算 SHA-256 写入清单；
4. 逐项核对：行数、对象数、校验和、**对象不跨集合**、三集合恰好等于
   输入可见行；
5. `os.rename(staging, splits/<split_id>)` **一次目录改名**发布，随后
   fsync 目录。

- 发布前任何时刻进程退出：正式路径根本不存在（只剩 `.staging-*`），
  `list`/检查脚本看不到半成品；
- 下次相同调用在 `splits/LOCK`（flock，进程内可重入）内确认目标缺失，
  清掉残骸并重新产出；
- 发布后退出：交付已完整可见，再次调用返回 `reused=true`。
- 测试钩子（仅测试使用）：`LAKE_SPLIT_CRASH_POINT=staged|published`。

## 6. 输入 snapshot 固定

调用方显式给出 snapshot 时，本次全程使用它；即便运行期间 HEAD 推进、
压实替换了物理文件，读者租约保证输入文件不被 GC 回收，结果也不包含新
快照的数据。`version=None` 时只读一次 HEAD，同样固定。

## 7. 独立校验脚本

`check_split.py` 不 import 湖表的扫描代码，只读交付目录：

```bash
python3 check_split.py SPLIT_DIR [--json] [--verify-assignment]
```

- 清单与三个文件都在，文件名与清单一致；
- 每个文件 SHA-256、行数、对象数与清单一致；
- 三个子集 `row_id` 集合两两不相交（对象不跨集合）；
- 三集合合计恰好等于清单记录的输入可见行/对象数（不重不漏）；
- `--verify-assignment` 按清单声明的 seed/权重独立复算每个 `row_id`
  的归属，要求与文件所在子集一致。

退出码：`0` 通过；`1` 发现问题；`2` 用法错误。

包内也有同一份校验逻辑：`from lake import check_split`。

## 8. 与源数据解耦

交付目录位于 `<root>/splits/`，不在 GC 的 `data/` 扫描范围内。子集
文件与清单是自包含的：源数据文件被压实替换、被 GC 回收后，直接
`pyarrow.parquet.read_table(<目录>/train.parquet)`、`check_split` 与
`export_subset` 全部照常工作。

## 9. 验收覆盖（tests/test_split.py、demo_split.py）

- 构造样本中同一对象的多条观测跨物理分片：对象不跨集合，三集合合计
  恰好包含输入可见行且无重复；
- 每行一个文件 + 倒序追加、以及先压实再切分：归属逐对象不变；
- 切分进行中追加/删除/压实：本次仍使用原输入 snapshot；
- 空 `row_id`、负数/非整数/总和错误权重、不存在的 snapshot：整次拒绝
  且无正式交付；
- 相同参数复用原切分（字节不变）；参数变化新目录且旧交付保留；
- 发布改名前的真实子进程硬退出（`os._exit(23)`）：正式路径不可见，
  重新调用完成交付；
- 源数据文件 GC 后，已交付子集仍可直接读取与校验。
