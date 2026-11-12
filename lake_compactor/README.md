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
- **数据质量画像**：按修订号冻结完整质量报告（样本量/空字段/重复标识/
  数值极值合计/左闭右开分段计数/规则逐条判定），报告只读、自包含，
  支持两份报告的指标差值与异常新出现/消失/持续比较（见 [QUALITY.md](QUALITY.md)）
- **按对象成组抽样**：以 `row_id` 为抽样单位产出 train/validation/test
  三个 Parquet 子集，同一对象的多条观测必然同属一个子集；分配只依赖
  seed 与 row_id，与文件排列、压实无关（见 [SPLIT.md](SPLIT.md)）
- **DuckDB 查询工作台**：一次请求登记若干逻辑名称（各自绑定本机湖表目录
  与精确版本号），提交只读 SELECT（含 CTE/连接/过滤/分组/排序），直接拿
  Arrow 表或终端 CSV / JSON Lines；取数期间持读者租约，写语句、直接读
  文件、外部数据源与联网一律拒绝（见 [WORKBENCH.md](WORKBENCH.md)）
- **实验分支**：`main` 是正式数据，实验者从任意已有提交建立命名分支，
  起初共享底层内容，随后各自发展；提交号全库唯一并记录父提交与各线
  尖端，写入只推进本线，`publish` 只允许快进交付，GC 按全部存活分支与
  租约保护内容（见 [BRANCHES.md](BRANCHES.md)）
- **字段身份与结构演进**：字段有稳定 id、当前名称、逻辑类型与可空性；
  新增可空列旧文件按 null 解释、改名不改 id、int32→int64 等无损扩宽，
  删除仍被引用的列 / 重复 id / 缩窄类型 / 无默认值的必填列一律拒绝；
  结构随 manifest 原子生效，新旧物理格式并存由同一套字段映射读取，
  旧湖表无需迁移（见 [SCHEMA.md](SCHEMA.md)）
- **临时隐私视图**：合作方隔离联调时按已提交版本与绑定稳定字段 id 的
  规则逐批取得脱敏 Arrow 批次——隐藏字段、客户编号 HMAC 稳定假名（同 key
  跨表跨运行可连接、不同客户不撞值）、事件时间截到日/月；结果仅供当前
  进程，不写回湖表、不生成导出目录、不保存转换数据，key 只经环境/内存
  提供且不进配置/manifest/metadata/异常/日志，读者租约在提前关闭、下游
  抛错或读取失败时必释放（见 [PRIVACY.md](PRIVACY.md)）

## 运行

```bash
./run.sh                 # 自动化校验 + 压实/摄取/质量画像/切分/查询工作台/分支/结构演进/隐私视图演示
# 或分别运行：
python3 -m pytest -v     # 164 个端到端用例（真实读写 Parquet、真实多进程交错）
python3 demo.py          # 叙述式演示 12 个场景
python3 demo_quality.py  # 质量画像 10 个验收场景（本地样本、可手算核对）
python3 demo_split.py    # 成组抽样切分 11 个验收场景
python3 demo_workbench.py  # 查询工作台 5 组验收场景
python3 demo_branches.py   # 实验分支 9 步验收（隔离/快进/拒绝分叉/并发接入/回收/重开）
python3 demo_schema.py     # 结构演进 11 步验收（滚动升级/混合压实/逐项拒绝/竞争/崩溃/旧目录）
python3 demo_privacy.py    # 隐私视图 6 组验收（假名连接/截断空值/抗改名/保护释放/源表不变/透传）
python3 check_split.py <交付目录> --verify-assignment   # 独立分组校验脚本
```

依赖：`pyarrow`、`duckdb`、`pytest`（可用 `PYTHON=/path/to/python` 指定解释器）。

## 查询工作台（workbench）

特征湖表与标签湖表按样本键对齐时，不用分别导出再由临时脚本合并：

```python
from lake.workbench import query_arrow

result = query_arrow(
    {
        "features":   ("./lake_feature", 12),
        "labels_old": ("./lake_label", 9),    # 同一个物理湖表
        "labels_new": ("./lake_label", 12),   # 两个不同历史版本
    },
    """
    SELECT f.row_id, f.value,
           old.value AS label_before,
           new.value AS label_after
    FROM features f
    LEFT JOIN labels_old old ON old.row_id = f.row_id
    LEFT JOIN labels_new new ON new.row_id = f.row_id
    ORDER BY f.row_id
    """,
)
result       # pyarrow.Table
```

- 每个逻辑名称装载它登记的**精确**版本经位置删除过滤后的可见行；底层
  Parquet 路径不进入 SQL。同一物理表可登记成多个名称绑定不同版本。
- 一次请求的全部输入版本在执行前确定；取数与组装结果期间每个来源都持
  `table.lease(version)`，另一进程追加/压实/GC 不会改变本次结果。
- 只读：写语句、DuckDB 直接读本地路径、加载外部数据源、联网、目录函数
  一律拒绝；拒绝时不动湖表文件/版本/清单，也不留数据库会话与租约。
- 错误分类：`mapping` / `version` / `syntax` / `permission` / `query`。
- 空结果保留列名与类型；CSV 保留表头，JSON Lines 为空。

CLI：`python3 -m lake.workbench.cli query --source NAME=DIR:VERSION ...`；
详见 [WORKBENCH.md](WORKBENCH.md) 与 `examples/workbench.example.json`。

## 临时隐私视图（privacy view）

合作方在隔离环境联调模型，但不能拿到生产客户编号或精确事件时间。选定一个
已提交版本与一组绑定稳定字段 id 的规则，逐批取得脱敏 Arrow 批次：

```python
from lake.privacy import open_privacy_view

rules = [
    {"field_id": 5, "action": "hide"},
    {"field_id": 3, "action": "pseudonymize", "alias": "customer_pid"},
    {"field_id": 4, "action": "truncate_time",
     "granularity": "day", "alias": "event_day"},
]
with open_privacy_view(table, rules, version=12,
                       hmac_key=b"shared-secret") as view:
    for batch in view:        # pyarrow.RecordBatch，按 batch 转换、取完即释放
        ...
```

- 规则绑定**稳定 field id**：列改名后规则仍作用于目标列；列被删除、缺 key、
  目标列类型不合适都在**第一批结果前**报错（`PrivacyViewError.kind` 为
  field/key/type/rule/version/read），不泄出数据、不残留租约；
- 假名 = `HMAC-SHA256(key, 域前缀||客户编号)` 前 16 字节十六进制：同 key 下
  两张表的同一客户跨运行可按假名连接，不同客户不撞值；`null` 仍为 `null`；
- key 只经内存参数（`hmac_key=`）或环境变量（`LAKE_PRIVACY_HMAC_KEY`）提供，
  不进配置、manifest、Arrow metadata、异常文本或日志，视图关闭即丢弃；
- 只读、不落盘：不写回湖表、不推进版本、不生成导出目录、不保存转换结果；
  不运行 SQL、不接受 Python 表达式；
- 读者租约覆盖整个迭代，正常读完 / 提前 `break` / 下游抛错 / 读取失败都在
  生成器 finally 中释放，之后 GC 可回收该版本；
- 空规则（`[]` 或 `None`）即按字段 id 映射原样透传，可见行口径与 `scan()`
  一致（含位置删除、缺列补 null、无损扩宽）。

详见 [PRIVACY.md](PRIVACY.md)；验收演示 `python3 demo_privacy.py`。

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

## 按对象成组抽样的数据切分（split）

一个业务对象可能对应多条训练观测。若按行抽样，同一对象会同时出现在
训练集和评测集，离线指标虚高。切分以 `row_id` 为抽样单位成组分配：

```python
from lake import LakeTable
from lake.splits import check_split

table = LakeTable.open("./mylake")
manifest = table.create_split(
    train=8000, validation=1000, test=1000,
    seed=42, version=None,          # version=None：调用时固定当前 HEAD
)
# manifest: split_id / input_version / weights / seed / algorithm_version /
#           各子集行数、对象数、SHA-256 / proportionality_notice
directory = f"{table.root}/splits/{manifest['split_id']}"
# 直接读取： <directory>/{train,validation,test}.parquet
check_split(directory)              # 独立校验：对象不跨集合、不重不漏
```

- 权重是非负整数“万分份”，总和必须为 10000；任何可见行 `row_id` 为空
  → `SplitError`，整次拒绝且不落盘；
- 归属 = `SHA-256(seed, row_id)` 对 10000 取模后落权重区间，只依赖
  `seed` 与 `row_id`：重排 Parquet 文件、先压实都不改变归属；
- 散列只承诺**长期比例**接近权重，小数据实际数量可以偏斜，清单里显式
  记录该口径；
- 相同参数（相同 snapshot + 权重 + seed）再次调用返回原切分（文件字节
  不变）；参数变化生成新 `split_id`，旧交付原样保留；
- 三个文件 + `manifest.json` 先写进 `.staging-<id>-<uuid>/`，逐项核对
  后一次目录改名发布：发布前退出，目标路径不可见半成品，再次调用完成
  交付；读取期间持读者租约，源数据被 GC 回收后交付子集仍可直接读取。

CLI：`python3 -m lake.splits.cli {create,check,list} ...`；
独立校验：`python3 check_split.py <交付目录> [--verify-assignment]`。
详见 [SPLIT.md](SPLIT.md)。

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

## 字段身份与结构演进（schema evolution）

生产者滚动升级时新旧物理格式的 Parquet 文件并存。字段有稳定 id、当前
名称、逻辑类型与可空性；所有读写入口共用同一套字段映射：

```python
from lake import LakeTable

t = LakeTable.create("./feat", schema=[
    {"id": 1, "name": "row_id", "type": "string", "nullable": False},
    {"id": 2, "name": "score",  "type": "int32",  "nullable": True},
])
t.append_rows([{"row_id": "a", "score": 10}], schema_version=1)

t.evolve_schema([                                  # 结构 v2：新增可空列
    {"id": 1, "name": "row_id", "type": "string", "nullable": False},
    {"id": 2, "name": "score",  "type": "int32",  "nullable": True},
    {"id": 3, "name": "tag",    "type": "string", "nullable": True},
])
t.evolve_schema([...])                             # v3：{"id": 2, "name": "confidence", ...} 改名
t.evolve_schema([...])                             # v4："type": "int64" 无损扩宽

s = t.current_schema()                             # 写者开始处理时读取结构版本
t.append_rows(rows, schema_version=s["version"])   # 发布时复核，过期明确失败
t.scan()                                           # 各历史格式统一映射到当前定义
t.scan(old_version)                                # 旧提交按当时生效的列定义读取
```

- 旧文件缺少的列按 default/null 解释；改名/扩宽不重写历史文件；
- 删除仍被引用的列、重复 id、缩窄类型、无默认值的必填列 →
  `SchemaEvolutionError`（逐条列出规则）；错误类型输入 →
  `SchemaValidationError`；结构发布与旧结构写者竞争 →
  `SchemaConflictError`（旧写入明确失败，不按新定义静默解释）；
- 结构随 manifest 原子生效，中断不留半成品定义；重开后结构历史与数据
  提交一致；无结构元数据的旧湖表照旧工作，无需迁移。

详见 [SCHEMA.md](SCHEMA.md)。

## 代码结构

| 文件 | 职责 |
|---|---|
| `lake/storage.py` | 原子写（tmp+fsync+rename）、目录 fsync、flock 锁 |
| `lake/manifest.py` | manifest 版本读写、HEAD / 分支尖端原子发布点、全库唯一取号、祖先判定 |
| `lake/schema_evo.py` | 字段身份与结构演进：类型注册表与无损扩宽、演进规则校验、行校验、物理文件 → 逻辑结构的读取映射 |
| `lake/branches.py` | 命名实验分支：名字校验、创建/列举/撤销、快进 publish（拒绝分叉与过期预期） |
| `lake/table.py` | 分支绑定的表对象：建表（可选显式结构）、追加（旧式/结构感知）、发布位置删除、扫描、结构演进、读者租约、摄取入口、checkout |
| `lake/ingest.py` | 微批摄取事务：消息规范化、分支独立游标、按线 receipt/记录命名、幂等重放、崩溃钩子 |
| `lake/compact.py` | 生成压实计划（staging 候选+位置映射+分支归属）、提交计划（冲突检测/删除翻译/跨线拒绝） |
| `lake/gc.py` | 引用感知的垃圾回收：保护全部存活分支尖端 + 读者租约（含孤儿批次记录、在途临时文件年龄保护） |
| `lake/verify.py` | DuckDB 多重集校验 |
| `lake/quality/rules.py` | 画像规则规范化、格式校验、确定性 SHA-256 摘要 |
| `lake/quality/profile.py` | 分批流式扫描与指标聚合（全表不入 Python 列表） |
| `lake/quality/store.py` | 报告冻结存储：唯一身份、租约保护、原子发布、崩溃钩子 |
| `lake/quality/compare.py` | 报告比较：指标差值、异常新出现/消失/持续 |
| `lake/quality/reference.py` | 独立纯 Python 参考计算程序（与生产路径分开） |
| `lake/quality/cli.py` | 质量画像命令行入口（create/show/list/compare/reference） |
| `lake/splits/split.py` | 按 row_id 成组抽样：稳定散列分配、固定 snapshot 流式读取、暂存目录 + 一次改名发布、身份复用 |
| `lake/splits/cli.py` | 切分命令行入口（create/check/list） |
| `lake/workbench/registry.py` | 逻辑名称登记表：名称 -> (湖表目录, 精确版本)，读取前统一定位 manifest |
| `lake/workbench/engine.py` | DuckDB 查询工作台：只读语句校验、计划边界检查、可见行流式装载、读者租约、结果组装 |
| `lake/workbench/cli.py` | 查询工作台命令行入口（query/spec，CSV / JSON Lines / Arrow） |
| `lake/privacy/rules.py` | 隐私字段规则规范化与校验（绑定稳定 field id：hide/pseudonymize/truncate_time） |
| `lake/privacy/key.py` | HMAC key 解析（内存参数优先，其次环境变量；不进配置/manifest/异常/日志） |
| `lake/privacy/transform.py` | HMAC 稳定假名、时间截断原语与输出列规划（只含允许别名，schema 无 metadata） |
| `lake/privacy/view.py` | 隐私视图：版本定位 + 读者租约、逐批字段 id 投影与转换、全路径租约释放 |
| `check_split.py` | 独立分组校验脚本（只读交付目录，可复算归属） |
| `tests/test_engine.py` | 压实引擎校验（屏障交错、冲突、崩溃、租约、原子发布压力） |
| `tests/test_ingest.py` | 摄取事务校验（多分片、重发、争抢、并发、崩溃恢复、旧表兼容） |
| `tests/test_quality.py` | 质量画像验收（手算样本、整理一致、删行、违规、错配、复用、租约、崩溃、GC 后可用） |
| `tests/test_split.py` | 切分验收（成组不重不漏、文件排列/压实不变、snapshot 推进、空标识、坏权重、身份复用、发布边界崩溃、GC 后读取） |
| `tests/test_workbench.py` | 查询工作台验收（独立期望行集、连接/分组、双版本对照、取数途中追加+压实+GC 的读者保护、空结果 schema、逐项拒绝与湖表不变） |
| `tests/test_branches.py` | 实验分支验收（旧目录对应 main、名字/冲突、共享建立、全库唯一号与父链、写入隔离、同键并发接入隔离、跨线压实拒绝、快进/分叉/过期拒绝、整理与修改并存、撤销后租约保护再回收、重开持久化） |
| `tests/test_schema.py` | 结构演进验收（滚动升级逐版本核对、混合格式压实、逐项拒绝、错误输入、发布竞争、发布中断重开、旧目录免迁移、重开一致性） |
| `tests/test_privacy.py` | 隐私视图验收（跨表假名连接与独立逐行核对、逐批峰值缓冲、抗改名、删列/缺 key/坏类型首批前拒绝、停止/异常/读取失败租约必释放、GC 衔接、源表不变与无 metadata、无规则透传） |
| `demo.py` | 叙述式端到端演示 |
| `demo_quality.py` | 质量画像叙述式演示（10 个验收场景） |
| `demo_split.py` | 成组抽样切分叙述式演示（11 个验收场景） |
| `demo_workbench.py` | 查询工作台叙述式演示（5 组验收场景） |
| `demo_branches.py` | 实验分支叙述式演示（9 步验收，逐步断言完整内容与状态） |
| `demo_schema.py` | 结构演进叙述式演示（11 步验收：滚动升级/混合压实/逐项拒绝/竞争/崩溃/旧目录） |
| `demo_privacy.py` | 隐私视图叙述式演示（6 组验收：假名连接/截断空值/抗改名/保护释放/源表不变/透传） |
| `QUALITY.md` | 画像规则配置、报告字段、接口与存储语义说明 |
| `SPLIT.md` | 成组抽样切分的参数、算法、清单字段与交付语义 |
| `WORKBENCH.md` | 查询工作台的登记表、查询语法、读者保护、安全边界与 CLI 说明 |
| `BRANCHES.md` | 实验分支的名字规则、接口、隔离语义、publish 快进规则与回收口径 |
| `SCHEMA.md` | 字段身份、结构版本、演进规则、读取映射与写入契约说明 |
| `PRIVACY.md` | 临时隐私视图的字段规则、HMAC 假名与 key 纪律、读者保护、错误分类与边界 |
| `ARCHITECTURE.md` | 架构说明 |
