"""基于 DuckDB 的校验工具：row_id 多重集比较。

扫描结果的物理位置过滤由 PyArrow 完成（严格对应文件物理行号）；
内容级校验交给 DuckDB：把 Arrow 表注册进来做 GROUP BY 计数，
比较两个快照的 row_id 多重集是否一致。
"""

from __future__ import annotations

import duckdb
import pyarrow as pa


def row_id_multiset(table: pa.Table) -> dict[str, int]:
    con = duckdb.connect()
    try:
        con.register("t", table)
        rows = con.execute("SELECT row_id, COUNT(*) FROM t GROUP BY row_id").fetchall()
        return {r[0]: r[1] for r in rows}
    finally:
        con.close()


def count(table: pa.Table) -> int:
    con = duckdb.connect()
    try:
        con.register("t", table)
        return con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    finally:
        con.close()


def assert_same_multiset(left: pa.Table, right: pa.Table) -> None:
    lm, rm = row_id_multiset(left), row_id_multiset(right)
    if lm != rm:
        diff_l = {k: v for k, v in lm.items() if rm.get(k) != v}
        diff_r = {k: v for k, v in rm.items() if lm.get(k) != v}
        raise AssertionError(
            f"row_id multiset mismatch:\n  left diff:  {diff_l}\n  right diff: {diff_r}"
        )
