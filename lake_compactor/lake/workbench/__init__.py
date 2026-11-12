"""湖表查询工作台（DuckDB 查询工作台）。

面向训练分析人员的只读查询入口：一次请求登记若干逻辑名称（各自绑定
一个本机湖表目录和精确版本号），提交一条 SELECT，拿到实际结果（Arrow
表），再导出为 CSV 或 JSON Lines。

    python3 -m lake.workbench.cli query LAKE:VERSION ... --sql-file q.sql
    python3 -m lake.workbench.cli spec  examples/workbench.example.json

Python：

    from lake.workbench import query_arrow
    table = query_arrow(
        {"features": ("/path/to/lake", 3), "labels": ("/path/to/lake", 3)},
        "SELECT f.row_id, l.label FROM features f JOIN labels l USING (row_id)",
    )

同一个物理湖表可以同时登记成两个名字、绑定不同历史版本（对照修订前后
的标签），因为每个名称的版本在执行前独立定位。

安全边界：只执行针对登记湖表数据的只读 SELECT；写语句、DuckDB 直接读
本地路径、加载外部数据源、联网访问一律拒绝，且拒绝时不改动湖表的任何
文件、版本或清单。
"""

from .engine import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_ROWS,
    DuckDBWorkbench,
    QueryResult,
    query,
    query_arrow,
    validate_statement,
)
from .registry import (
    LogicalBinding,
    ResolvedSource,
    normalize_bindings,
    resolve_source,
    resolve_sources,
)

__all__ = [
    "DuckDBWorkbench",
    "QueryResult",
    "LogicalBinding",
    "ResolvedSource",
    "normalize_bindings",
    "resolve_source",
    "resolve_sources",
    "validate_statement",
    "query",
    "query_arrow",
    "DEFAULT_MAX_ROWS",
    "DEFAULT_BATCH_SIZE",
]
