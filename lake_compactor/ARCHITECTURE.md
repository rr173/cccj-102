# 小型湖表压实引擎 — 架构说明

面向离线训练集整理场景：大量碎片 Parquet 需要合并、位置删除需要物化；
训练数据接入端需要可重放的微批摄取（确认超时会整批重发）。
纯本机文件系统 + 进程锁实现，不依赖对象存储，也不兼容 Iceberg。

## 1. 磁盘布局

```
<root>/
  data/                        已发布文件（不可变）
    data-<data_file_id>.parquet     数据文件，含稳定 row_id 列
    deletes-<delete_file_id>.parquet 位置删除文件
  staging/                     未发布的压实候选（不在读路径上）
    compact-<plan_id>/
      data-<new_id>.parquet         合并后的候选数据文件
      plan.json                     计划：基版本、选中文件、位置映射
  metadata/
    versions/vNNNNNN.json      不可变 manifest，一版一个文件（全库唯一编号）
    HEAD                       main（正式数据）尖端版本号（原子替换）
    branches.json              实验分支尖端 {"name": 提交号}（原子替换）
    LOCK                       flock 跨进程提交锁
    leases/lease-<id>.json     读者租约
    ingest/batches/batch-<hash>.json  已提交批次的记录（审计载体）
```

**Manifest** 完整描述一个提交（不描述“某个分支的表状态”，分支状态由
尖端指针 + parent 链表达）：

```json
{
  "version": 9, "parent": 8, "branch": "main", "created_at": ...,
  "data_files":  {"<id>": {"path": "data/data-<id>.parquet", "num_rows": 19}},
  "delete_files": {"<id>": {"path": "data/deletes-<id>.parquet",
                            "target": "<data_file_id>", "num_deletes": 1}},
  "ingest": {
    "cursors": {"<source>": {"<partition>": 128}},
    "batches": {"<batch_key>": {"record": "metadata/ingest/batches/batch-<h>.json",
                                "receipt": "rcpt-...", "version": 9,
                                "digest": "...", "num_rows": 20,
                                "cursors_after": {"<source>": {"<partition>": 128}}}}
  }
}
```

`ingest` 段是摄取事务的状态：`cursors` 是各来源分片下一个期望的 offset，
`batches` 是已提交批次索引（只增不减，随版本原样携带）。旧格式 manifest
没有该段，一律按空状态处理，因此新代码可以直接打开此前创建的表。

**位置删除文件**内每行显式记录 `(data_file_id, pos)`，`pos` 是该数据文件内
**从零开始的物理行号**。约束：一个删除文件只针对一个数据文件，压实时才能
按文件整体“吸收”或“翻译”。行号只来自读取时的物理位置，**绝不**从
`row_id` 等业务字段反推。

## 2. 原子发布点

所有推进版本的操作（追加、发布删除、提交压实、摄取批次）走同一条路径：

1. 新数据/删除文件先以临时名写入 `data/` 并 `os.replace` 就位，fsync 目录；
2. 新 manifest 写入 `versions/vNNNNNN.json`（临时文件 + fsync + rename）；
3. 最后原子替换 `HEAD`（rename 在同一文件系统内是原子的）。

读者先读 `HEAD` 固定版本号，再读对应 manifest。manifest 先于 HEAD 就位，
因此读者要么看到旧版本、要么看到完整的新版本——这就是跨进程原子发布点。
`staging/` 里的候选不被任何 manifest 引用，天然不在读路径上。
所有写者共用 `metadata/LOCK` 上的 `flock` 排他锁。版本号是**全库坐标**：
每次提交在锁内取“所有已落盘版本号的最大值 + 1”，所以 main 与任意多条
实验分支并发提交都不会生成同号记录；commit 的 `parent` 指向**本线**
前驱，提交图因此不依赖分支名。

**尖端指针**：main 的尖端就是 `HEAD`（与旧格式完全一致）；实验分支尖端
存在 `metadata/branches.json`。两者都用“临时文件 + fsync + rename”原子
替换，且 manifest 先于指针就位。旧目录没有 branches.json，第一次使用
自然只有 main，所有旧调用无需传分支参数。

## 3. 扫描（读路径）

`scan(version)`：固定 manifest → 读出其中全部删除文件，按 `data_file_id`
归组成位置集合 → 用 PyArrow 按物理顺序读每个数据文件，按位置过滤
（`pyarrow.parquet` 保证按文件物理行序返回，与删除项的 0 起行号严格对应）。

位置过滤刻意用 PyArrow 而非 SQL：物理行序是格式层保证，不该交给查询引擎
的扫描顺序假设。DuckDB 用于内容级校验（`lake/verify.py` 把 Arrow 表注册
进去做 `GROUP BY row_id` 计数，比对多重集）。

## 4. 压实

**生成计划（`create_plan`）**：在基版本 V 选中小文件（显式列表或
`max_rows` 阈值），剔除 V 已记录的删除行，把活行合并成一个候选 Parquet
写入 `staging/compact-<plan_id>/`，并记录每个旧文件的
`旧物理行号 -> 新物理行号` 映射与被吸收的删除位置。计划落盘为 `plan.json`。

**提交计划（`commit_plan`）**：在提交锁下把当前版本 W 与基版本 V 比较——

| 情况 | 处理 |
|---|---|
| 选中文件在 W 中已不存在（被别的压实替换） | `ConflictError`，计划标记 `failed`，不覆盖对方 |
| W 相对 V 的新增数据文件（并发追加） | 原样保留（随 W 的 manifest 自然带入） |
| W 相对 V 新到的、针对选中文件的删除 | 按映射翻译到新文件位置，写成新删除文件随新版本发布 |
| V 已有的针对选中文件的删除 | 行已被物理剔除，删除文件被“吸收”丢弃 |
| 针对未选中文件的删除 | 原样保留 |

随后组装新 manifest 并翻转 HEAD，staging 目录即清理。

## 5. 微批摄取事务

训练数据接入端会因确认超时重发整批消息，因此摄取必须可重放：
源端消费进度（游标）与入湖记录要么一起生效、要么一起不生效。

**规范化与摘要**：每条消息含 `source / partition / offset / row_id / value`。
`partition` 规范为字符串；整批按 `(source, partition, offset)` 排序后计算
SHA-256 摘要——同一批消息无论输入顺序如何都得到相同摘要，这是
“乱序重试视为相同内容”的基础。receipt 由 `batch_key + 摘要 + 分支名`
确定性导出（main 分支与旧实现逐字节兼容），因此同一请求键在两条实验线
上的回执不同，回执只在本线有效。

**提交序列**（与追加同路，全部在 `metadata/LOCK` 的临界区内完成校验与发布）：

1. 锁外准备：规范化、算摘要，数据先写 `data/.tmp-<id>`（不在读路径上）；
2. 锁内复核：当前 manifest 的批次索引里已有该 `batch_key` → 走重放
   （摘要一致返回原 receipt，不一致抛 `IdempotencyConflictError`）；
3. 锁内校验：每个分片的 offset 必须恰好接续当前游标——缺号、批内
   重复、与既往区间交叠，任一发生则**整批**抛 `BatchRejectedError`，
   任何分片游标都不推进；
4. 数据文件 rename 就位 → 批次记录（含规范化消息、游标变迁）原子落盘
   → 组装新 manifest（数据文件引用 + 新游标 + 批次索引条目）→ 翻转 HEAD。

游标、请求摘要、receipt、本批数据文件引用都在同一个 manifest 里随
**本线尖端**一次翻转生效：这就是该线的唯一可见性边界，不存在“先写进度
再补数据”的窗口，也不需要补偿修正。未出现在批内的分片游标自然不变（新
manifest 从本线尖端整体派生）。并发安全性与压实一致：`_next` 基于锁内
读到的本线尖端，因此同一线上的摄取、追加、压实两两不会互相覆盖成果；
另一条分支推进版本不构成本线冲突。

**游标与批次索引按分支独立**：两者都随 manifest 携带，两条实验线从各自
父提交继承进度后各自推进。批次记录文件名带分支摘要，同 key 的两条线
不会覆盖彼此的审计载体。

**重放判定以本线 manifest 为准**：批次记录文件本身不构成提交证据（可能
是崩溃残留的孤儿），只有本线尖端 manifest 的批次索引引用的记录才算已
生效；另一条线的同名 key 不会被本线借用。
不同 `batch_key` 争抢同一区间时，锁内后到者看到游标已推进，按交叠
整批拒绝——最多一个成功；相同 `batch_key` 的并发者则一个提交、一个
按重放拿到原 receipt，谁也不会被默默吞掉。

**测试钩子**：`LAKE_INGEST_CRASH_POINT=prepared|recorded|published` 在
三个关键位置硬退出（仅测试使用），分别覆盖“锁外准备”“记录已写未发布”
“已发布未确认”三条崩溃时间线。

## 6. 读者租约与垃圾回收

- `table.lease(version)`：在提交锁下写入 `leases/lease-<id>.json`。
  租约与 GC 共用同一把锁，GC 不会漏看刚创建的租约。
- `collect()`：受保护版本 = 所有存活分支尖端 ∪ 所有存活租约的版本。
  只看 main 不够：实验尖端引用的独占内容在 main 上不可见但必须保留；
  撤销分支只移除引用，其尖端随即不受保护，若没有租约则其数据/删除文件
  允许回收，正在读取的租约继续保护到读完为止。只有受保护版本引用的
  数据/删除文件允许保留，其余删除；
  `staging/` 下超过 `max_age` 的候选目录（崩溃残留、失败计划）一并清除；
  `data/` 下的 `.tmp` 在途临时文件按同样的年龄阈值保护，只清理确定是
  残骸的；`metadata/ingest/batches/` 下不被任一受保护版本批次索引引用的
  孤儿记录（摄取提交中途崩溃的残留）一并删除——记录写入与尖端翻转在同
  一把锁内，GC 不会与在途提交交错。
- manifest JSON 本身不裁剪（体量小，且进行中的压实计划需要读基版本）。

## 7. 崩溃恢复

- 追加/发布删除中途崩溃：`data/` 里留下不被任何 manifest 引用的孤儿文件，GC 回收。
- 压实提交前崩溃：候选留在 `staging/`，不进读路径，GC 按年龄清理。
- 提交在尖端翻转前崩溃：版本未发布，与从未发生无异；翻转后崩溃：
  版本已完整发布，staging 残骸由 GC 清理。`branches.json` 与 `HEAD`
  都是原子 rename，不会出现半个分支表。
- 摄取在锁外准备阶段崩溃：只留下 `.tmp` 临时文件，游标未动，重发即
  全量重做；批次记录已写但 HEAD 未翻转时崩溃：记录与数据文件都是
  孤儿，批次不可见、游标未动，GC 回收后重发重做；HEAD 翻转后、确认
  返回前崩溃：事务已完整生效，接入端重发同一 `batch_key` 即重放到原
  receipt，不会再加行、不会再推进版本。

## 7b. 实验分支（lake/branches.py）

- **引用而不是复制**：`create_branch(name, from_version)` 只往
  `metadata/branches.json` 写 `name -> 已有提交号`；数据文件与 manifest
  全部原处共享。
- **提交号全库唯一**：`manifest.next_version()` 在提交锁内取所有已落盘
  版本号的最大值 +1；每条提交的 `parent` 是本线前驱，`branch` 是作者线。
- **写入只推进本线**：`append` / `publish_deletes` / `ingest_batch` /
  `commit_plan` 以本线尖端为父，`_publish` 再核验父提交等于本线尖端
  （同一分支内的竞争明确失败，另一条线推进不算冲突）。压实计划记录
  `base_branch`，不能跨线提交。
- **分支绑定对象**：`LakeTable.open(root, branch=...)` / `table.checkout(name)`
  返回同一物理目录上的另一个访问对象，原有读写接口直接可用；默认分支
  是 main，旧调用无需改动。
- **publish 只做快进**：要求 main 尖端是实验尖端祖先，且调用方给出预期
  的 main 尖端（防止检查后被抢跑）。成功只把 HEAD 原子指向已存在的实验
  提交，不写新 manifest、不复制样本；分叉 → `DivergedError`，不自动内容
  合并；预期过期 → `StaleMainError`；拒绝保留双方原状。
- **撤销与回收**：撤销只删 `branches.json` 里的引用；其独占提交在无
  尖端祖先链、无租约引用时由 `collect()` 回收，读者租约保护到读完。
- **持久化**：`HEAD` / `branches.json` / `versions/` 全部落盘在
  `metadata/`，重开后分支尖端、父链、游标、回执与数据读取结果一致。

## 7c. 字段身份与结构演进（lake/schema_evo.py）

训练特征在生产者滚动升级时逐步增调列，新旧物理格式的 Parquet 并存。
机制要点：

- **字段身份**：稳定 id + 当前名称 + 逻辑类型 + 可空性（可选默认值）。
  改名不改 id；id 永不复用；类型只允许无损扩宽（int32→int64、
  int32→double、float32→double）。
- **结构版本**：表级单调递增；0 保留给旧式 row_id/value 隐式结构。
  每个 manifest 携带完整结构历史 `"schemas": [...]`（最后一项即当前
  定义），结构变更就是一个普通 manifest 提交——与数据提交共用同一条
  原子发布路径，拒绝或崩溃不会留下半成品定义，重开后结构历史与数据
  提交天然一致。旧格式 manifest 没有该段 → 隐式 v0，旧湖表免迁移。
- **读取映射**：每个数据文件条目记录写入时的结构版本
  （`schema_version`，缺省 0）。读取提交 V 时：物理列名 →（文件的
  结构版本）→ 字段 id →（V 的结构定义）→ 当前名称与类型；缺列按
  default/null 补齐，扩宽列无损 cast。`scan` 与 `create_plan` 共用
  `schema_evo.project_file`；压实结果按当前逻辑结构落盘。
- **写入契约**：`append_rows(rows, schema_version)` 先按该版本校验行
  （`SchemaValidationError`），发布时在提交锁内复核版本仍是当前定义，
  否则 `SchemaConflictError`——旧写入明确失败，绝不按新定义静默解释。
  摄取消息的 row_id/value 按字段 id 1/2 映射，其余列走 `fields` 扩展；
  旧式 `append` 在无元数据表上逐字节兼容，在已演进表上按兼容规则
  （id 1/2 在且类型兼容、其余列可填充）落当前结构，否则明确拒绝。
- **演进校验**：`evolve_schema` 在锁内校验完整新定义——删除仍被文件
  引用的列、重复 id/名字、缩窄或有损类型变更、可空收紧为必填、无默认
  值的必填新增列、复用已删除字段 id，全部收进
  `SchemaEvolutionError.reasons` 一次性拒绝；与当前一致的提议是空操作。

## 8. 限制（本题范围）

- 仅支持同一台机器上的多进程并发（flock 语义决定）。
- 分支只支持快进发布；分叉后的合并需要人工作业，引擎不做内容合并。
- `branches.json` 是单个小 JSON，全库分支数按小规模实验团队的量级设计。
- staging 清理依赖年龄阈值，进行中的压实须在阈值内完成（生产上可换计划心跳）。
- manifest 只增不减；如需裁剪历史，需保证无进行中计划引用基版本。
- 已提交批次索引随 manifest 累积（每批一条小 JSON 摘要），微批规模下
  体量可控；超大规模接入需要把索引外移并保留同样的发布语义。
- 结构历史随 manifest 完整携带（只增不减），演进次数按生产变更频率
  设计；质量画像/切分/查询工作台等下游工具按物理列读取，面向旧式表，
  结构受管表请经 `scan()` 消费逻辑视图。

## 9. 运行

```bash
./run.sh        # 自动化校验（pytest）+ 叙述式演示（demo*.py）
```

测试实际读写 Parquet、实际 fork 子进程，用同步屏障精确安排
“计划生成—额外删除—提交”与“摄取—压实—提交”的交错，用测试钩子
在摄取提交的关键位置硬退出，并用 DuckDB 核对 row_id 多重集。

## 10. 数据质量画像（lake/quality/）

画像为交付前冻结的质量检查结果，与数据修订号一一对应：

```
<root>/quality_profiles/
  LOCK                                跨进程互斥（flock，独立于 metadata/LOCK）
  reports/revNNNNNN-<digest12>.json   冻结报告，0444 只读，永不改写
  .tmp-<random>                       发布前临时文件（与正式目录分离）
```

- **身份与不可变**：唯一身份 = 修订号 + 规则摘要（规范化规则的
  SHA-256）。重复请求直接返回原报告（`reused=true`）；另一组规则摘要
  不同，写另一个文件，绝不覆盖。规则格式错误在任何 IO 之前被
  `RuleSpecError` 拒绝，不落盘任何报告；数据触犯规则仍保存完整画像，
  失败项进 `verdicts`。
- **计数口径**：固定修订号 manifest → 归组位置删除 → 逐文件
  `ParquetFile.iter_batches` 分批产出可见行（`profile.iter_visible_batches`），
  聚合器任何时刻只持有一个 RecordBatch 与一个主标识频次字典，全表不进
  Python 列表。统计的是查询结果而非分片 `num_rows` 相加；修订号在扫描
  开始前固定，扫描期间表继续提交不影响结果。分段左闭右开，null 不计入
  分段与上下界。
- **读者保护**：`ProfileStore.create_or_get` 在 `table.lease(version)`
  租约内完成扫描，租约与 GC 共用表的提交锁，输入文件不会在统计途中被
  回收。
- **原子发布与崩溃恢复**：临时文件写在 `quality_profiles/` 根目录，
  fsync 后 `os.replace` 到 `reports/` 并 fsync 目录，再 chmod 0444。
  发布前任何时刻退出，`reports/` 下都不会出现半成品；下次相同请求在
  锁内确认目标缺失后清掉 `.tmp` 残骸并重新计算（测试钩子
  `LAKE_QUALITY_CRASH_POINT=prepared|computed|staged` 覆盖三个中断点，
  用真实 `os._exit` 的子进程验证）。
- **与数据解耦**：画像目录不在 GC 的扫描范围内，报告内容自包含；
  原始数据文件被整理/回收后，`show`/`compare` 只依赖报告 JSON。
- **比较**：`compare_reports` 对两份报告输出全部标量/分段指标差值
  （新 - 旧），并按规则名匹配异常，分为新出现、消失、持续三类。
- **参考程序**：`quality/reference.py` 不 import 生产扫描/聚合代码，
  用独立的纯 Python 逐行路径重算同一批指标，供小样本手算交叉核对
  （`cross_check`）。

## 11. 按 row_id 成组抽样的数据切分（lake/splits/）

```
<root>/splits/
  LOCK                              跨进程 flock（进程内 RLock 可重入）
  <split_id>/                       已交付：三个子集 + 清单，一次改名发布
    train.parquet
    validation.parquet
    test.parquet
    manifest.json
  .staging-<split_id>-<uuid>/       发布前的暂存目录（不在正式路径上）
```

- **抽样单位是对象组**：`row_id` 相同的全部可见行（固定快照经位置删除
  过滤后）必须落在同一个子集。这也是“不能按行随机”的原因：同一对象的
  观测若拆散，训练集与评测集会共享对象，离线指标虚高。
- **稳定散列**：`SHA-256(str(seed) + \x00 + row_id)[:8] % 10000`，按权重
  在 `[0, 10000)` 上切成 train / validation / test 三段。只依赖 seed 与
  row_id，因此重排 Parquet 文件、改变 manifest 键序、先压实都不改变
  归属；同一 `row_id` 的多条观测天然同桶。散列只承诺长期比例，小数据
  数量可偏斜，清单显式记录。
- **固定输入**：`version=None` 时在提交锁下只读一次 HEAD；显式
  `version` 时校验存在性。读取全程持 `table.lease(version)`，GC 不会
  回收输入，运行期间的新 snapshot/压实不改变本次输入。清单记录输入
  snapshot 与可见行/对象数。
- **身份与不可替换**：`split_id = SHA-256(算法版本 + 输入 snapshot +
  权重 + seed)[:16]`。相同参数命中同一目录 → 原样复用（`reused=true`，
  文件不改写）；参数变化 → 新目录，旧交付保留。算法实现变化时提升
  `algorithm_version`，旧交付不会被误认为相同参数。
- **原子交付**：三个文件与清单先写进 `.staging-<split_id>-<uuid>/`，
  刷盘后写入每个文件的 SHA-256，再逐项核对（行数/对象数/校验和/对象
  不跨集合/三集合合计等于输入）。全部通过后 `os.rename` 到正式目录，
  一次发布。发布前退出只有 `.staging-*` 残骸，正式路径不可见；下次调用
  在 `splits/LOCK` 内确认目标缺失后清残骸重做。测试钩子
  `LAKE_SPLIT_CRASH_POINT=staged|published` 用真实 `os._exit` 子进程验证。
- **独立校验**：`check_split.py` 只读交付目录、不 import 扫描代码，可
  `--verify-assignment` 按清单参数复算归属；包内 `lake.splits.check_split`
  是同一份逻辑，用于发布前与“相同参数复用”时的完好性核对。
- **与源数据解耦**：交付目录不在 GC 扫描范围内，子集文件自包含；源数据
  被压实替换或被 GC 回收后，子集仍可直接读取与校验。

## 12. DuckDB 查询工作台（lake/workbench/）

```
<进程内，不落盘>
  LogicalBinding  逻辑名称 -> (本机湖表目录, 精确 manifest 版本号)
  ResolvedSource  已定位到版本的来源（table + manifest，尚未读数据行）
  QueryResult     Arrow 表 + 每个来源的实际版本 + 耗时
```

- **数据边界**：SQL 里只能出现登记过的逻辑名称，底层 Parquet 路径不进入
  SQL。每个名称装载 `LakeTable.scan` 同语义的可见行（固定 manifest →
  归组位置删除 → 按物理行号过滤），但按批次流式产出并以 Arrow C 流注册
  给 DuckDB（每次扫描重新构造流，支持自连接/子查询重复扫描）。
- **版本固定**：一次请求的所有绑定在读取前调用 `resolve_sources`，逐个
  读 `versions/vNNNNNN.json` 校验版本存在；任何失败整次拒绝，不打开
  DuckDB 会话。
- **读者保护**：每个来源在读取前持 `table.lease(version, require_files=True)`；
  `lease` 在同一临界区内读 `HEAD` 与 manifest、确认版本引用的文件仍在、
  写入租约文件，因此“定位版本 → 建立保护”之间没有窗口。租约一直持有到
  结果 Arrow 表组装完成、会话关闭之后。另一个进程的追加/压实/`collect()`
  因此在取数途中无法删除本次输入；若历史版本在租约之前已被回收，请求
  以 `VersionError` 明确拒绝。
- **只读边界（两层）**：
  1. 工作台侧：`duckdb.extract_statements` 只允许 SELECT 类语句（SELECT /
     WITH / FROM-first / VALUES / DESCRIBE / SUMMARIZE）；写语句与
     CALL/LOAD/INSTALL/SET/PRAGMA/COPY/ATTACH 等立即拒绝。执行前再用
     `EXPLAIN (FORMAT JSON)` 遍历物理计划，拒绝 `DUCKDB_*` 目录/函数表
     节点与任何文件/外部数据源扫描节点（`READ_PARQUET`、`READ_CSV`、
     `GLOB` 等）。
  2. DuckDB 侧：`enable_external_access=false` +
     `autoload/autoinstall_known_extensions=false` +
     `lock_configuration=true`，从引擎层关闭文件系统、扩展加载与网络；
     查询无法用 SET/PRAGMA 重新打开。
- **清理**：成功返回、语法错误、版本定位失败、查询执行失败都走同一
  finally：关闭 DuckDB 会话、按栈顺序释放全部租约；拒绝路径不写湖表
  文件/版本/清单。
- **结果上限**：按批次拉取结果并累计行数，超过 `max_rows` 立即拒绝整次
  查询；空结果从 reader.schema 构造 0 行 Arrow 表，列名/类型不丢。
- **错误分类**：`WorkbenchError.kind` ∈ mapping / version / syntax /
  permission / query，分别对应登记表、版本定位、查询文本、权限边界、
  绑定与执行。
