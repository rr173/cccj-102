# 湖表查询工作台（Workbench）

训练分析人员要把特征湖表和标签湖表按样本键对齐时，不必再分别导出文件
再用临时脚本合并：一次请求登记若干**逻辑名称**（各自绑定一个本机湖表
目录和**精确版本号**），提交一条 SQL，直接拿到 Arrow 表；终端里可以读
CSV 或 JSON Lines。

```python
from lake import LakeTable
from lake.workbench import query_arrow, query

table = LakeTable.open("./mylake")
result = query_arrow(
    {
        "features":    ("./lake_feature", 12),
        "labels_old":  ("./lake_label", 9),    # 同一物理表
        "labels_new":  ("./lake_label", 12),   # 同一物理表的另一个版本
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
result           # pyarrow.Table
```

`query(...)` 返回 `QueryResult`：`.table`（Arrow 表）、`.sources`（本次
实际读取的逻辑名称 + 目录 + 版本 + 租约 id）、`.num_rows`、`.to_csv()`、
`.to_jsonl()`、`.summary()`。

## 1. 登记表

三种等价形状，任选其一：

```python
{"features": ("./lake_feature", 12), "labels": ("./lake_label", 12)}
{"features": {"lake": "./lake_feature", "version": 12}, ...}
[{"name": "features", "lake": "./lake_feature", "version": 12}, ...]
```

- 名称是 SQL 里唯一能出现的表名；未引号名称大小写不敏感（与 DuckDB
  的标识符折叠一致），因此重名（含大小写差异）直接拒绝。
- 版本号是**精确**的 manifest 版本，不是 `HEAD`/`latest`。省略版本没有
  默认值：分析结果必须可复现。想在“调用时取当前 HEAD”，先读
  `table.current_version()` 再把数字登记进来。
- 同一物理湖表可以登记成多个名称、绑定不同版本（对照修订前后）。
- 登记阶段不读数据；所有来源在查询开始前统一定位 manifest，任何一个
  版本不存在都整次拒绝，不启动数据库会话。

## 2. 查询契约

支持 SELECT、公共表表达式（`WITH`）、内连接/左连接、过滤、分组统计和
排序；也可以用 DuckDB 的 FROM-first 写法与 `VALUES`。每个逻辑名称装载
的是该版本**经过位置删除过滤后的可见行**，列就是数据文件里的
`row_id` / `value`（标签表通常另有一列，例如 `label`）。

```sql
-- 内连接 + 分组
SELECT f.row_id, COUNT(*) AS n, SUM(f.value) AS total, MAX(l.value) AS label
FROM features f
JOIN labels l ON l.row_id = f.row_id
GROUP BY f.row_id
ORDER BY total DESC;

-- 过滤 + 左连接（保留没有标签的特征行）
SELECT f.row_id, f.value, l.value AS label
FROM features f LEFT JOIN labels l ON l.row_id = f.row_id
WHERE f.value >= 20;
```

底层 Parquet 文件路径不出现在 SQL 里，工作台也不把它交给 DuckDB：
数据按批次读到内存后以 Arrow 流注册，DuckDB 只看到内存视图。

## 3. 读者保护与版本固定

1. **输入版本在执行前确定**：`resolve_sources` 依次定位 manifest 并校验
   版本存在；之后才开始读数据。
2. **租约覆盖取数与结果组装的全过程**：每个来源在读取前持
   `table.lease(version, require_files=True)`，manifest 也在租约的临界区
   内读取，因此“定位版本 → 建立保护”之间没有窗口。租约建立时还会在同一
   临界区内确认该版本引用的文件仍在；如果历史版本已在租约建立前被 GC
   回收，请求以 `VersionError` 明确拒绝，而不是读一半再失败。
3. 因此即使另一个进程在查询途中追加记录、重新整理文件并启动
   `collect()`，本次结果仍来自登记的版本；结果组装完成、数据库会话
   关闭后才释放租约。
4. 成功返回、语法错误、来源打不开、运行期错误：都走同一条清理路径
   （关闭 DuckDB 会话、释放全部已取得的租约），不留下半成品结果，也
   不修改湖表的文件、版本或清单。

## 4. 安全边界

工作台只服务登记过的湖表数据。拒绝以下入口（均抛
`PermissionBoundaryError`，`kind="permission"`）：

| 入口 | 例子 |
|---|---|
| 更新/删除/建表等写语句 | `INSERT / UPDATE / DELETE / CREATE / DROP / ALTER / COPY / ATTACH` |
| DuckDB 直接读本地路径 | `read_parquet('/tmp/x')`、`FROM 'data/data-x.parquet'`、`glob('/etc/*')` |
| 加载外部数据源 | `read_csv_auto(...)`、`read_json(...)`、`read_text(...)`、`read_blob(...)`、`LOAD/INSTALL httpfs` |
| 联网 | `http://`、`https://`、`s3://` 等 URL |
| 目录/函数表与配置 | `duckdb_settings()`、`information_schema.*`、`PRAGMA`、`SET`、`SHOW` |

两层实现：工作台先用 DuckDB 语句类型白名单拒绝非 SELECT 语句，再检查
物理计划中的扫描节点；DuckDB 侧另以
`enable_external_access=false` + `lock_configuration=true` 关闭文件系统、
扩展加载与网络，查询无法通过 `SET`/`PRAGMA` 重新打开。两层都执行时，
拒绝发生在任何湖表写入之前。

## 5. 拒绝分类

`WorkbenchError.kind`（及其子类）明确指出问题所在：

| kind | 子类 | 含义 |
|---|---|---|
| `mapping` | `MappingError` | 登记表不合法：重名、空名、绑定形状错误 |
| `version` | `VersionError` | 湖表目录不存在/不是湖表、版本号不存在、manifest 损坏 |
| `syntax` | `QuerySyntaxError` | 空查询、多语句、无法解析 |
| `permission` | `PermissionBoundaryError` | 写语句、文件读写、外部数据源、联网、目录表 |
| `query` | `QueryReferenceError` / `QueryExecutionError` | 未登记名称、列名/绑定错误、运行期错误 |

## 6. 命令行

```bash
python3 -m lake.workbench.cli query \
    --source features=./lake_feature:12 \
    --source labels=./lake_label:12 \
    --sql "SELECT f.row_id, f.value, l.value AS label
           FROM features f JOIN labels l ON l.row_id = f.row_id
           ORDER BY f.row_id" \
    --format csv

python3 -m lake.workbench.cli query --spec examples/workbench.example.json \
    --format jsonl

python3 -m lake.workbench.cli spec examples/workbench.example.json
```

- `--source NAME=DIR:VERSION` 可重复；`--spec` 接受
  `{"sources": {...}, "sql": "..."}`。
- `--format csv`（默认）输出含表头的 CSV，空结果也保留表头行；
  `--format jsonl` 每行一个 JSON 对象，空结果无行（列名见 schema）；
  `--format arrow` 打印带类型的 Arrow 表。
- 元信息（实际读取的版本、行数、耗时）写 stderr，不污染管道数据；
  `--summary` 以 JSON 输出。
- 退出码：`0` 成功；`4` 登记表；`5` 版本；`6` 语法；`7` 权限边界；
  `8` 查询绑定/执行。

## 7. 其他

- 结果行数默认上限 1,000,000（`DuckDBWorkbench(max_rows=...)` 或
  `query(..., max_rows=N)`）；超限整次拒绝。分析训练集应先用过滤/聚合。
- `batch_size` 控制源文件读取批次；结果组装也按批次拉取。
- 空结果、零行版本：列名与类型来自来源 schema / DuckDB 结果 schema，
  CSV 仍有表头，Arrow 表仍带完整 schema。
