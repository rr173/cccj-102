"""DuckDB 查询工作台：按精确版本读取多张湖表并执行只读 SQL。

一次请求 = 一份逻辑名称登记表 + 一条查询。登记表把每个逻辑名称绑定到
一个本机湖表目录和一个**精确版本号**；查询里的表名只能是这些登记过的
名称，底层 Parquet 文件路径不进入 SQL。

核心保证：

  1. **输入版本在执行前确定**：所有绑定先定位到 manifest 并校验版本
     存在，之后才开始读取数据；查询执行途中表再提交新版本也与本次无关。
  2. **读者保护覆盖取数与组装结果的全过程**：每个来源在读取期间持
     ``table.lease(version)``（与 GC 共用提交锁），因此另一个进程追加、
     压实并启动 ``collect()`` 也不会删掉本次输入文件；结果组装完成、
     连接关闭之后才释放租约。
  3. **只读边界**：只接受单条 SELECT 类语句；写语句、文件读写、外部
     数据源、联网、扩展与配置入口都被拒绝（DuckDB 侧
     ``enable_external_access=false`` 且配置锁定，工作台侧先做语句白名单
     与计划检查）。
  4. **失败即清理**：语法错误、来源打不开、查询失败都结束数据库会话并
     释放已取得的租约，不留下半成品结果，也不改动湖表文件/版本/清单。

可见行语义与 ``LakeTable.scan`` 一致：每个来源装载它登记的版本经过位置
删除过滤后的行，列就是数据文件本身的 ``row_id`` / ``value``。
"""

from __future__ import annotations

import contextlib
import json
import os
import time

import duckdb
import pyarrow as pa
import pyarrow.csv as pa_csv

from ..errors import (
    LakeError,
    LeaseError,
    MappingError,
    PermissionBoundaryError,
    QueryExecutionError,
    QueryReferenceError,
    QuerySyntaxError,
    VersionError,
)
from .registry import (
    ResolvedSource,
    normalize_bindings,
    resolve_sources,
)

DEFAULT_MAX_ROWS = 1_000_000
DEFAULT_BATCH_SIZE = 4096
DEFAULT_FETCH_BATCH_SIZE = 64_000

# DuckDB 侧的最后一道闸：完全禁止文件系统与网络访问，并锁定配置，
# 使查询本身无法用 SET/PRAGMA 重新打开。工作台的白名单与计划检查在
# 这之前执行，两层都为正拒绝给出可读的错误信息。
_SECURE_CONFIG = {
    "enable_external_access": "false",
    "autoinstall_known_extensions": "false",
    "autoload_known_extensions": "false",
    "allow_unsigned_extensions": "false",
    "lock_configuration": "true",
}

# 允许的语句类型（DuckDB StatementType 名称）。SELECT 覆盖 SELECT、
# 公共表表达式、FROM-first 语法与 VALUES；DESCRIBE/SUMMARIZE 只是读
# 计划的别名，也按只读放行。
_ALLOWED_STATEMENT_TYPES = {"SELECT"}

# 计划里出现这些节点说明查询绕开了登记表：扫描 DuckDB 自带的目录/函数
# 表、或任何文件/扩展表函数。它们不属于“登记过的湖表数据”。
_DENIED_PLAN_NODES = {
    "DUCKDB_TABLES",
    "DUCKDB_VIEWS",
    "DUCKDB_COLUMNS",
    "DUCKDB_SCHEMAS",
    "DUCKDB_FUNCTIONS",
    "DUCKDB_SETTINGS",
    "DUCKDB_EXTENSIONS",
    "DUCKDB_KEYWORDS",
    "DUCKDB_TYPES",
    "DUCKDB_DATABASES",
    "DUCKDB_INDEXES",
    "DUCKDB_CONSTRAINTS",
    "DUCKDB_MEMORY",
    "DUCKDB_TEMP",
    "DUCKDB_VARIABLES",
    "PRAGMA_VERSION",
    "PRAGMA_TABLE_INFO",
    "PRAGMA_SHOW",
    "PRAGMA_DATABASE_SIZE",
    "PRAGMA_STORAGE_INFO",
    "PRAGMA_COLLATIONS",
    "PRAGMA_FUNCTIONS",
    "PRAGMA_SETTINGS",
    "PRAGMA_ENABLE_EXTENSION",
    "PRAGMA_DISABLE_EXTENSION",
}

# 计划里出现这些节点说明查询在访问本机路径、外部数据源或网络。
_DENIED_PLAN_NODE_TOKENS = (
    "READ_PARQUET",
    "PARQUET_SCAN",
    "READ_CSV",
    "CSV_SCAN",
    "READ_JSON",
    "JSON_SCAN",
    "READ_NDJSON",
    "READ_BLOB",
    "READ_TEXT",
    "READ_FUNCTIONS",
    "GLOB",
    "HTTP",
    "S3",
    "DELTA",
    "ICEBERG",
    "SQLITE",
    "POSTGRES",
    "MYSQL",
    "EXCEL",
    "ARROW_SCAN_FILE",
)

# 必须被拒绝的语句类型（给出“越过权限边界”而不是语法错误）。其余不在
# 白名单里的类型（PREPARE/EXECUTE 等）一律同样按权限边界拒绝。
_WRITE_STATEMENT_TYPES = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "DROP",
    "ALTER",
    "COPY",
    "ATTACH",
    "DETACH",
    "SET",
    "PRAGMA",
    "CALL",
    "LOAD",
    "INSTALL",
    "EXPORT",
    "IMPORT",
    "VACUUM",
    "TRANSACTION",
    "CHECKPOINT",
    "USE",
}

# 名字里带这些词的标识符可能是“想读外部数据”的意图（用于把 Catalog
# 错误翻译成权限边界错误，例如 SELECT * FROM '/tmp/x.parquet'）。
_PATH_LIKE = ("/", "\\", ".parquet", ".csv", ".json", ".ndjson", ".gz", ".db")


class QueryResult:
    """一次查询的返回值：Arrow 表 + 元信息（版本、耗时、行数）。"""

    __slots__ = ("table", "sources", "elapsed_seconds", "sql", "truncated")

    def __init__(
        self,
        table: pa.Table,
        sources: list[dict],
        elapsed_seconds: float,
        sql: str,
        truncated: bool = False,
    ):
        self.table = table
        self.sources = sources
        self.elapsed_seconds = elapsed_seconds
        self.sql = sql
        self.truncated = truncated

    @property
    def num_rows(self) -> int:
        return self.table.num_rows

    @property
    def num_columns(self) -> int:
        return self.table.num_columns

    @property
    def column_names(self) -> list[str]:
        return list(self.table.column_names)

    @property
    def schema(self) -> pa.Schema:
        return self.table.schema

    def to_pydict(self) -> dict:
        return self.table.to_pydict()

    def to_pylist(self) -> list[dict]:
        return self.table.to_pylist()

    def to_csv(self) -> str:
        """RFC 4180 CSV 文本（含表头）。空结果也保留列名。"""
        import io

        buf = io.BytesIO()
        pa_csv.write_csv(self.table, buf)  # 0 行时只写表头行
        return buf.getvalue().decode("utf-8")

    def to_jsonl(self) -> str:
        """JSON Lines 文本；空结果为无行（列名见 schema）。"""
        cols = self.table.column_names
        arrays = [self.table.column(c).to_pylist() for c in cols]
        lines = []
        for i in range(self.table.num_rows):
            row = {cols[j]: arrays[j][i] for j in range(len(cols))}
            lines.append(json.dumps(row, ensure_ascii=False, sort_keys=False))
        return "".join(line + "\n" for line in lines)

    def summary(self) -> dict:
        return {
            "num_rows": self.num_rows,
            "num_columns": self.num_columns,
            "columns": [
                {"name": f.name, "type": str(f.type)} for f in self.table.schema
            ],
            "sources": self.sources,
            "elapsed_seconds": self.elapsed_seconds,
            "sql": self.sql,
            "truncated": self.truncated,
        }


# ---------------------------------------------------------------------------
# 语句校验
# ---------------------------------------------------------------------------
def _boundary_error(message: str) -> PermissionBoundaryError:
    return PermissionBoundaryError(
        "权限边界: " + message,
        detail=(
            "工作台只执行针对已登记湖表数据的只读 SELECT 查询；"
            "不提供写语句、文件读写、外部数据源与联网入口"
        ),
    )


def validate_statement(sql) -> str:
    """校验查询文本本身，返回规范化后的 SQL。

    先判空，再判多语句，最后用 DuckDB 的语句类型白名单。这一步不需要
    连接数据库，因此即使来源定位失败也能先给出语法/权限层面的错误。
    """
    if not isinstance(sql, str):
        raise QuerySyntaxError(f"查询必须是字符串，实际为 {type(sql).__name__}")
    text = sql.strip()
    if not text:
        raise QuerySyntaxError("语法错误: 查询为空")
    try:
        statements = duckdb.extract_statements(text)
    except duckdb.ParserException as e:  # pragma: no cover - 版本差异兜底
        raise QuerySyntaxError(f"语法错误: {_first_line(e)}") from e
    except duckdb.Error as e:
        raise QuerySyntaxError(f"语法错误: {_first_line(e)}") from e
    if not statements:
        raise QuerySyntaxError("语法错误: 查询为空（只有注释或空白）")
    if len(statements) > 1:
        raise QuerySyntaxError(
            "语法错误: 一次请求只接受单条查询，实际包含 "
            f"{len(statements)} 条语句"
        )
    stmt = statements[0]
    if getattr(stmt, "query", None) is None:
        raise QuerySyntaxError("语法错误: 无法解析查询文本")
    type_name = str(stmt.type).rsplit(".", 1)[-1]
    if type_name in _WRITE_STATEMENT_TYPES:
        raise _boundary_error(
            f"语句类型 {type_name} 不在只读白名单内（写语句或有副作用的语句）"
        )
    if type_name not in _ALLOWED_STATEMENT_TYPES:
        raise _boundary_error(
            f"语句类型 {type_name} 不在只读白名单内"
        )
    return text


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip()
    return text.splitlines()[0] if text else exc.__class__.__name__


def _mentions_path_like(msg: str) -> bool:
    lowered = msg.lower()
    return any(token in lowered for token in _PATH_LIKE)


def _plan_node_names(plan) -> list[str]:
    names: list[str] = []

    def walk(node):
        if isinstance(node, dict):
            name = node.get("name")
            if isinstance(name, str):
                names.append(name)
            for child in node.get("children") or []:
                walk(child)

    for root in plan:
        walk(root)
    return names


def _check_plan_boundary(con, sql: str) -> None:
    """在执行前检查 DuckDB 的物理计划，拒绝绕过登记表的扫描节点。

    这一段用于覆盖“查询语法层面看不出意图”的入口：例如
    ``SELECT * FROM duckdb_settings()``、``read_text('/etc/passwd')``，
    或 ``FROM query_table('duckdb_tables')``。文件系统本身已被配置禁用，
    这里再给出明确的工作台边界错误。
    """
    try:
        rows = con.sql("EXPLAIN (FORMAT JSON) " + sql).fetchall()
    except duckdb.PermissionException as e:
        raise _boundary_error(
            f"查询试图访问本机文件或外部数据源: {_first_line(e)}"
        ) from e
    except duckdb.ParserException as e:
        raise QuerySyntaxError(f"语法错误: {_first_line(e)}") from e
    except duckdb.CatalogException as e:
        raise _catalog_error(e, sql) from e
    except duckdb.BinderException as e:
        raise QueryReferenceError(f"查询绑定失败: {_first_line(e)}") from e
    except duckdb.Error:
        # 计划生成失败但错误分类不明确：留给真正的执行阶段给出明确错误。
        return

    if not rows:
        return
    raw = rows[0][1] if rows[0] and len(rows[0]) > 1 else None
    if not isinstance(raw, str):
        return
    try:
        plan = json.loads(raw)
    except ValueError:
        return
    nodes = _plan_node_names(plan)
    denied = sorted({n for n in nodes if n in _DENIED_PLAN_NODES})
    if denied:
        raise _boundary_error(
            "查询扫描了 DuckDB 自带的目录/函数表，不属于登记过的湖表数据: "
            + ", ".join(denied)
        )
    external = sorted(
        n
        for n in nodes
        if any(token in n.upper() for token in _DENIED_PLAN_NODE_TOKENS)
        and "PYTHON_ARROW_SCAN" not in n
    )
    if external:
        raise _boundary_error(
            "查询计划包含文件/外部数据源扫描: " + ", ".join(external)
        )


def _catalog_error(exc: duckdb.CatalogException, sql: str) -> LakeError:
    msg = _first_line(exc)
    if _mentions_path_like(msg) or _mentions_path_like(sql):
        return _boundary_error(f"查询引用了本机路径或未登记的数据源: {msg}")
    return QueryReferenceError(
        f"查询引用了未登记的表名: {msg}",
        detail="表名只能是本次登记过的逻辑名称（未引号名称大小写不敏感）",
    )


# ---------------------------------------------------------------------------
# 来源装载
# ---------------------------------------------------------------------------
def _iter_visible_batches(table, manifest: dict, batch_size: int):
    """按登记的 manifest 逐批产出可见行（位置删除过滤后的 RecordBatch）。

    与 ``LakeTable.scan`` 的可见行语义一致，但分批流式读取：DuckDB 从
    Python 侧拉取批次，任何时刻只有一个批次在内存里。位置删除按文件内
    物理行号（0 起）执行，绝不从 row_id 等业务字段反推。
    """
    import pyarrow.parquet as pq

    deletes = table._all_deleted_positions(manifest)
    schema = None
    for fid in sorted(manifest["data_files"]):
        meta = manifest["data_files"][fid]
        path = os.path.join(table.root, meta["path"])
        dead = deletes.get(fid)
        pf = pq.ParquetFile(path)
        row_offset = 0
        for batch in pf.iter_batches(batch_size=batch_size):
            orig_len = batch.num_rows
            if dead:
                keep = [
                    i for i in range(orig_len) if (row_offset + i) not in dead
                ]
                if len(keep) != orig_len:
                    batch = batch.take(pa.array(keep, pa.int64()))
            row_offset += orig_len
            if schema is None:
                schema = batch.schema
            if batch.num_rows:
                yield batch
    if schema is None:
        # 该版本没有任何数据文件：用表的标准可见列给一个空 schema，
        # 使查询仍能绑定列名并返回“空结果 + 列名与类型”。
        from ..table import DATA_SCHEMA

        schema = DATA_SCHEMA


class _VisibleSourceProvider:
    """可重复扫描的 Arrow C 流提供者，每次扫描都按登记的版本/删除过滤产出。

    DuckDB 可能对同一个注册名发起多次扫描（自连接、子查询复用等），因此
    这里不能只给一个一次性的 RecordBatchReader；每次要不是重新构造流。
    所有扫描都发生在同一份读者租约保护期内，读到的始终是登记版本。
    """

    __slots__ = ("table", "manifest", "batch_size", "scans")

    def __init__(self, table, manifest: dict, batch_size: int):
        self.table = table
        self.manifest = manifest
        self.batch_size = batch_size
        self.scans = 0

    def __arrow_c_stream__(self, requested_schema=None):
        self.scans += 1
        reader = _visible_reader(self.table, self.manifest, self.batch_size)
        if requested_schema is not None:
            return reader.__arrow_c_stream__(requested_schema)
        return reader.__arrow_c_stream__()


def _visible_reader(table, manifest: dict, batch_size: int):
    """把可见行包装成 RecordBatchReader，交给 DuckDB 注册为内存视图。

    空版本也要有正确 schema：先探测一次 schema（不读行），再用它构造
    空 reader。这样 ``SELECT row_id, COUNT(*) ... GROUP BY`` 在零行时依然
    保留列名与类型。
    """
    from ..table import DATA_SCHEMA
    import pyarrow.parquet as pq

    schema = None
    for fid in sorted(manifest["data_files"]):
        meta = manifest["data_files"][fid]
        try:
            schema = pq.ParquetFile(
                os.path.join(table.root, meta["path"])
            ).schema_arrow
        except (OSError, pa.ArrowException):
            schema = None
        if schema is not None:
            break
    if schema is None:
        schema = DATA_SCHEMA

    def gen():
        for batch in _iter_visible_batches(table, manifest, batch_size):
            yield batch

    return pa.RecordBatchReader.from_batches(schema, gen())


class DuckDBWorkbench:
    """可复用的查询工作台（本身无状态；每次 query 开独立会话）。

    ``max_rows`` 是结果行数上限：超过即拒绝整次查询，避免把训练集全量
    拉进内存。``batch_size`` 是源文件读取批次大小。
    """

    def __init__(
        self,
        max_rows: int = DEFAULT_MAX_ROWS,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows <= 0:
            raise ValueError("max_rows must be a positive integer")
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or batch_size <= 0
        ):
            raise ValueError("batch_size must be a positive integer")
        self.max_rows = int(max_rows)
        self.batch_size = int(batch_size)

    # ------------------------------------------------------------------
    def query(
        self,
        bindings,
        sql: str,
        *,
        max_rows: int | None = None,
    ) -> QueryResult:
        """登记逻辑名称并执行查询，返回 Arrow 表与元信息。

        失败契约（全部是 ``WorkbenchError`` 子类，``kind`` 指出类别）：
          - ``mapping``    登记表不合法（重名、形状错误）
          - ``version``    湖表目录/精确版本无法定位
          - ``syntax``     查询为空、多语句、无法解析
          - ``permission`` 写语句、文件读写、外部数据源、联网、目录函数
          - ``query``      未登记名称、列名错误、运行期错误
        """
        # 1) 查询文本先校验：语法/权限问题不依赖任何来源是否可打开。
        text = validate_statement(sql)
        # 2) 登记表规范化 + 所有来源在读取前定位到精确版本。
        registered = normalize_bindings(bindings)
        sources = resolve_sources(registered)
        limit = self.max_rows if max_rows is None else int(max_rows)
        if limit <= 0:
            raise ValueError("max_rows must be a positive integer")
        return self._execute(text, sources, limit)

    def query_arrow(self, bindings, sql: str, *, max_rows: int | None = None):
        """只返回 Arrow 表（调用端最常用的形态）。"""
        return self.query(bindings, sql, max_rows=max_rows).table

    # ------------------------------------------------------------------
    def _execute(
        self, sql: str, sources: list[ResolvedSource], limit: int
    ) -> QueryResult:
        start = time.time()
        leases = contextlib.ExitStack()
        con = None
        consumed: list[dict] = []
        try:
            # 读者保护：先对所有来源按登记顺序建立租约，再开始读取。
            # manifest 在租约的临界区内读出（lease.manifest），因此
            # “定位版本 -> 建立保护”没有窗口，GC 不可能在中间删文件。
            # require_files=True 在同一临界区内确认该版本引用的文件仍在：
            # 若 GC 在定位之后、租约之前回收了这个历史版本，这里明确拒绝
            # （归入 version 类别），而不是让读取在途中失败。
            # 任何一个租约失败都整次拒绝；已取得的租约由 ExitStack 释放。
            protected: list[tuple[ResolvedSource, dict, str]] = []
            for src in sources:
                try:
                    lease = leases.enter_context(
                        src.table.lease(src.version, require_files=True)
                    )
                except LeaseError as e:
                    raise VersionError(
                        f"版本定位错误: 逻辑名称 {src.name!r} 的版本 "
                        f"v{src.version} 数据文件已被回收: {e}"
                    ) from e
                protected.append((src, lease.manifest, lease.lease_id))
            for src, manifest, lease_id in protected:
                consumed.append({**src.describe(), "lease_id": lease_id})

            con = duckdb.connect(":memory:", config=dict(_SECURE_CONFIG))

            seen_names: dict[str, str] = {}
            for src, manifest, _lease_id in protected:
                folded = src.name.casefold()
                if folded in seen_names:
                    raise MappingError(
                        f"逻辑名称 {src.name!r} 重复登记"
                    )
                seen_names[folded] = src.name
                reader = _VisibleSourceProvider(
                    src.table, manifest, self.batch_size
                )
                con.register(src.name, reader)

            _check_plan_boundary(con, sql)

            arrow = self._fetch_arrow(con.sql(sql), sql, limit)
            elapsed = time.time() - start
            return QueryResult(
                table=arrow,
                sources=consumed,
                elapsed_seconds=elapsed,
                sql=sql,
                truncated=False,
            )
        finally:
            # 成功、语法/权限/查询错误、来源打不开：都结束会话并释放保护。
            if con is not None:
                try:
                    con.close()
                except duckdb.Error:
                    pass
            leases.close()

    # ------------------------------------------------------------------
    def _fetch_arrow(self, rel, sql: str, limit: int) -> pa.Table:
        """按批次拉取结果并组装 Arrow 表；超过行数上限立即拒绝。

        结果组装同样发生在读者租约的保护期内（调用方在 finally 里才释放
        租约），所以取数途中发生的追加/压实/GC 都不会改变本次结果。
        空结果用 reader.schema 构造 0 行表，列名与类型完整保留。
        """
        try:
            reader = rel.to_arrow_reader(DEFAULT_FETCH_BATCH_SIZE)
            batches: list[pa.RecordBatch] = []
            total = 0
            for batch in reader:
                total += batch.num_rows
                if total > limit:
                    raise QueryExecutionError(
                        f"查询结果超过上限 {limit} 行（已读到 {total} 行）；"
                        "请增加聚合/过滤，或用 max_rows 显式提高上限"
                    )
                batches.append(batch)
            schema = reader.schema
            return pa.Table.from_batches(batches, schema=schema)
        except (QueryExecutionError, PermissionBoundaryError):
            raise
        except duckdb.PermissionException as e:
            raise _boundary_error(
                f"查询试图访问本机文件或外部数据源: {_first_line(e)}"
            ) from e
        except duckdb.ParserException as e:
            raise QuerySyntaxError(f"语法错误: {_first_line(e)}") from e
        except duckdb.CatalogException as e:
            raise _catalog_error(e, sql) from e
        except duckdb.BinderException as e:
            raise QueryReferenceError(
                f"查询绑定失败: {_first_line(e)}"
            ) from e
        except duckdb.InvalidInputException as e:
            raise _input_error(e) from e
        except duckdb.OutOfMemoryException as e:
            raise QueryExecutionError(
                f"查询执行失败（内存不足）: {_first_line(e)}"
            ) from e
        except duckdb.Error as e:
            raise QueryExecutionError(
                f"查询执行失败: {_first_line(e)}"
            ) from e
        except pa.ArrowException as e:
            raise QueryExecutionError(
                f"读取湖表数据失败: {_first_line(e)}"
            ) from e

    # ------------------------------------------------------------------
    def query_csv(self, bindings, sql: str, *, max_rows: int | None = None) -> str:
        return self.query(bindings, sql, max_rows=max_rows).to_csv()

    def query_jsonl(
        self, bindings, sql: str, *, max_rows: int | None = None
    ) -> str:
        return self.query(bindings, sql, max_rows=max_rows).to_jsonl()


def _input_error(exc: duckdb.InvalidInputException) -> LakeError:
    """DuckDB 的 InvalidInput 常是“流式注册的 reader 里抛了异常”。

    把其中携带的 Python 异常文本提升为“读取湖表数据失败”，并保留原始
    原因；这类失败说明来源文件在读取途中不可用（本不该发生，因为租约
    保护了它），归为工作台状态异常。
    """
    msg = _first_line(exc)
    lowered = msg.lower()
    if "permission" in lowered or "file system" in lowered:
        return _boundary_error(f"查询试图访问本机文件或外部数据源: {msg}")
    return QueryExecutionError(f"查询执行失败: {msg}")


# ---------------------------------------------------------------------------
# 便捷函数
# ---------------------------------------------------------------------------
_DEFAULT = DuckDBWorkbench()


def query_arrow(bindings, sql: str, *, max_rows: int | None = None):
    """一次查询，返回 Arrow 表。"""
    return _DEFAULT.query_arrow(bindings, sql, max_rows=max_rows)


def query(bindings, sql: str, *, max_rows: int | None = None) -> QueryResult:
    """一次查询，返回 QueryResult（表 + 来源版本 + 耗时）。"""
    return _DEFAULT.query(bindings, sql, max_rows=max_rows)
