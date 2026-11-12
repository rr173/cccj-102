"""湖表查询工作台命令行入口（终端里读 CSV / JSON Lines）。

用法：

    # 逻辑名称 = 目录:精确版本号（多次 --source 登记多个名称）
    python3 -m lake.workbench.cli query \\
        --source features=./lake_feature:12 \\
        --source labels=./lake_label:12 \\
        --sql "SELECT f.row_id, f.value, l.value AS label \\
               FROM features f JOIN labels l USING (row_id) \\
               ORDER BY 1" \\
        --format csv

    python3 -m lake.workbench.cli query --spec spec.json --sql-file q.sql \\
        --format jsonl

    python3 -m lake.workbench.cli spec examples/workbench.example.json

spec 文件（逻辑名称登记表 + 查询）示例：

    {
      "sources": {
        "features": {"lake": "./lake_feature", "version": 12},
        "labels_old": {"lake": "./lake_label", "version": 9},
        "labels_new": {"lake": "./lake_label", "version": 12}
      },
      "sql": "SELECT ..."
    }

输出：
  - `--format csv`（默认）stdout 是 CSV，表头始终保留（空结果也保留列名）；
  - `--format jsonl` 每行一个 JSON 对象；
  - `--format arrow` 打印 Arrow 表（类型清晰，便于人工核对）；
  - 元信息（实际读取的版本、行数、耗时）写到 stderr（`--summary` 可
    额外输出为 JSON），避免污染管道里的数据。
"""

from __future__ import annotations

import argparse
import json
import sys

from ..errors import WorkbenchError
from .engine import DuckDBWorkbench
from .registry import normalize_bindings

EXIT_MAPPING = 4
EXIT_VERSION = 5
EXIT_SYNTAX = 6
EXIT_PERMISSION = 7
EXIT_QUERY = 8

_KIND_EXIT = {
    "mapping": EXIT_MAPPING,
    "version": EXIT_VERSION,
    "syntax": EXIT_SYNTAX,
    "permission": EXIT_PERMISSION,
    "query": EXIT_QUERY,
    "internal": 1,
}


def _parse_source(text: str) -> tuple[str, tuple[str, int]]:
    """解析 NAME=LIBDIR:VERSION（目录里可以含冒号之外的任意字符）。"""
    if "=" not in text:
        raise argparse.ArgumentTypeError(
            "--source 必须是 NAME=湖表目录:版本号，例如 features=./lake:12"
        )
    name, rest = text.split("=", 1)
    if ":" not in rest:
        raise argparse.ArgumentTypeError(
            "--source 缺少版本号，例如 features=./lake:12"
        )
    lake, version = rest.rsplit(":", 1)
    name = name.strip()
    if not name or not lake.strip():
        raise argparse.ArgumentTypeError("--source 的名称与目录都不能为空")
    try:
        version = int(version)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"--source 的版本号必须是整数，实际为 {version!r}"
        )
    return name, (lake.strip(), version)


def _load_spec(path: str) -> tuple[dict, str]:
    with open(path, "r", encoding="utf-8") as f:
        spec = json.load(f)
    if not isinstance(spec, dict):
        raise ValueError("spec 文件必须是 JSON 对象")
    sources = spec.get("sources")
    if sources is None:
        raise ValueError("spec 文件缺少 sources（逻辑名称登记表）")
    sql = spec.get("sql")
    if sql is None:
        sql = ""
    if not isinstance(sql, str):
        raise ValueError("spec 文件的 sql 必须是字符串")
    return sources, sql


def _emit_error(err: WorkbenchError) -> int:
    print(f"错误[{err.kind}]: {err}", file=sys.stderr)
    if err.detail:
        print(f"  {err.detail}", file=sys.stderr)
    return _KIND_EXIT.get(err.kind, 1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lake.workbench",
        description="湖表查询工作台：登记逻辑名称（目录+精确版本），执行只读 SELECT",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_query = sub.add_parser("query", help="执行一次只读查询")
    p_query.add_argument(
        "--source", action="append", type=_parse_source, default=[],
        metavar="NAME=LAKE:VERSION",
        help="逻辑名称绑定（可重复）；同一物理表可登记成两个名字绑定不同版本",
    )
    p_query.add_argument("--spec", help="spec 文件（sources + sql）")
    p_query.add_argument("--sql", default=None, help="查询文本")
    p_query.add_argument("--sql-file", default=None, help="从文件读取查询文本")
    p_query.add_argument(
        "--format", choices=("csv", "jsonl", "arrow"), default="csv"
    )
    p_query.add_argument(
        "--max-rows", type=int, default=None, help="结果行数上限（超过即拒绝）"
    )
    p_query.add_argument(
        "--batch-size", type=int, default=None, help="源文件读取批次大小"
    )
    p_query.add_argument(
        "--summary", action="store_true", help="把来源版本/行数/耗时以 JSON 写到 stderr"
    )

    p_spec = sub.add_parser("spec", help="校验并回显一个 spec 文件的登记表")
    p_spec.add_argument("spec_file")
    p_spec.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)

    if args.cmd == "spec":
        try:
            sources, sql = _load_spec(args.spec_file)
            bindings = normalize_bindings(sources, check_exists=False)
        except (OSError, ValueError) as e:
            print(f"错误[mapping]: {e}", file=sys.stderr)
            return EXIT_MAPPING
        except WorkbenchError as e:
            return _emit_error(e)
        out = {
            "sources": [
                {
                    "name": b.name,
                    "lake": b.lake_dir,
                    "version": b.version,
                }
                for b in bindings
            ],
            "sql": sql,
        }
        if args.json:
            print(json.dumps(out, indent=2, ensure_ascii=False))
        else:
            for item in out["sources"]:
                print(
                    f"{item['name']} -> {item['lake']} @ v{item['version']}"
                )
        return 0

    # ---------------------------------------------------------------- query
    sql = args.sql
    file_sql = None
    if args.sql_file:
        try:
            with open(args.sql_file, "r", encoding="utf-8") as f:
                file_sql = f.read()
        except OSError as e:
            print(f"错误[syntax]: 无法读取 SQL 文件: {e}", file=sys.stderr)
            return EXIT_SYNTAX
        if sql is not None and sql.strip() != file_sql.strip():
            print(
                "错误[syntax]: --sql 与 --sql-file 同时给出且内容不一致",
                file=sys.stderr,
            )
            return EXIT_SYNTAX
        sql = file_sql

    sources = None
    if args.spec:
        try:
            sources, spec_sql = _load_spec(args.spec)
        except OSError as e:
            print(f"错误[mapping]: 无法读取 spec 文件: {e}", file=sys.stderr)
            return EXIT_MAPPING
        except ValueError as e:
            print(f"错误[mapping]: {e}", file=sys.stderr)
            return EXIT_MAPPING
        if sql is None and spec_sql:
            sql = spec_sql
    if args.source:
        if sources is None:
            sources = {}
        elif isinstance(sources, dict):
            sources = dict(sources)
        for name, binding in args.source:
            if isinstance(sources, dict):
                if name in sources:
                    print(
                        f"错误[mapping]: 逻辑名称 {name!r} 重复登记",
                        file=sys.stderr,
                    )
                    return EXIT_MAPPING
                sources[name] = binding
            else:  # pragma: no cover - spec 已校验为 dict
                sources.append({"name": name, "lake": binding[0],
                                "version": binding[1]})

    if sources is None:
        print("错误[mapping]: 至少需要用 --source 或 --spec 登记一个来源",
              file=sys.stderr)
        return EXIT_MAPPING
    if sql is None:
        print("错误[syntax]: 缺少查询文本（--sql / --sql-file / spec 的 sql）",
              file=sys.stderr)
        return EXIT_SYNTAX

    workbench = DuckDBWorkbench(
        batch_size=args.batch_size or 4096
    )
    try:
        result = workbench.query(sources, sql, max_rows=args.max_rows)
    except WorkbenchError as e:
        return _emit_error(e)
    except ValueError as e:
        print(f"错误[syntax]: {e}", file=sys.stderr)
        return EXIT_SYNTAX
    except OSError as e:
        print(f"错误[version]: {e}", file=sys.stderr)
        return EXIT_VERSION

    if args.format == "csv":
        sys.stdout.write(result.to_csv())
    elif args.format == "jsonl":
        sys.stdout.write(result.to_jsonl())
    else:
        print(result.table.to_string())

    if args.summary:
        print(json.dumps(result.summary(), indent=2, ensure_ascii=False),
              file=sys.stderr)
    else:
        versions = ", ".join(
            f"{s['name']}=v{s['version']}" for s in result.sources
        )
        print(
            f"# {result.num_rows} 行 × {result.num_columns} 列；"
            f"来源: {versions}；耗时 {result.elapsed_seconds:.3f}s",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
