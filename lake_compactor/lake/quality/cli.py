"""数据质量画像命令行入口。

用法：

    python -m lake.quality.cli create  LAKE [--rules RULES.json] [--revision N]
                                       [--batch-size 1000] [--json]
    python -m lake.quality.cli show    LAKE --revision N --digest HASH [--json]
    python -m lake.quality.cli list    LAKE [--json]
    python -m lake.quality.cli compare LAKE --old-rev N --new-rev N
                                       [--old-digest HASH --new-digest HASH]
                                       [--rules RULES.json] [--json]
    python -m lake.quality.cli reference LAKE [--rules RULES.json] [--revision N]

规则文件为 JSON，全部键可选：

    {"min_samples": 10, "max_duplicate_ids": 0,
     "value_bounds": {"min": 0, "max": 100},
     "buckets": [{"name": "low", "lower": 0, "upper": 10}]}

compare 省略 digest 时，可用 --rules 指定规则摘要；两者都省略则取
各修订号下唯一的报告（有多份时会报错并列出候选）。
"""

from __future__ import annotations

import argparse
import json
import sys

from ..errors import LakeError
from ..table import LakeTable
from .compare import compare_reports, format_comparison, format_report
from .reference import reference_profile
from .rules import load_rules_from_file
from .store import ProfileStore

EXIT_RULE_ERROR = 2
EXIT_NOT_FOUND = 3


def _load_rules_arg(args) -> dict:
    if args.rules:
        return load_rules_from_file(args.rules)
    return {}


def _resolve_report(store: ProfileStore, revision: int, digest: str | None,
                    rules_path: str | None) -> dict:
    if digest is not None:
        return store.get(revision, digest)
    if rules_path is not None:
        return store.get_by_rules(revision, load_rules_from_file(rules_path))
    candidates = store.find(revision)
    if len(candidates) == 1:
        return store.get(revision, candidates[0]["rule_digest"])
    if not candidates:
        raise SystemExit(f"修订号 {revision} 下没有已冻结报告")
    names = "\n".join(f"  {c['rule_digest'][:12]}  {c['rule_summary']}" for c in candidates)
    raise SystemExit(f"修订号 {revision} 下有多份报告，请用 --digest 指定:\n{names}")


def _print_report(report: dict, as_json: bool) -> None:
    if as_json:
        print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
    else:
        print(format_report(report))
        print(f"\n复用既有报告: {'是' if report.get('reused') else '否（新发布）'}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lake.quality", description="湖表数据质量画像：创建、查看、比较"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_create = sub.add_parser("create", help="创建（或取回）冻结画像报告")
    p_create.add_argument("lake")
    p_create.add_argument("--rules", help="规则 JSON 文件；省略表示空规则")
    p_create.add_argument("--revision", type=int, default=None,
                          help="数据修订号；省略为当前 HEAD")
    p_create.add_argument("--batch-size", type=int, default=1000)
    p_create.add_argument("--json", action="store_true")

    p_show = sub.add_parser("show", help="查看已冻结报告")
    p_show.add_argument("lake")
    p_show.add_argument("--revision", type=int, required=True)
    p_show.add_argument("--digest", required=True, help="规则摘要哈希（前缀即可，至少 12 位）")
    p_show.add_argument("--json", action="store_true")

    p_list = sub.add_parser("list", help="列出全部冻结报告")
    p_list.add_argument("lake")
    p_list.add_argument("--json", action="store_true")

    p_cmp = sub.add_parser("compare", help="比较两份冻结报告")
    p_cmp.add_argument("lake")
    p_cmp.add_argument("--old-rev", type=int, required=True)
    p_cmp.add_argument("--new-rev", type=int, required=True)
    p_cmp.add_argument("--old-digest", default=None)
    p_cmp.add_argument("--new-digest", default=None)
    p_cmp.add_argument("--rules", default=None,
                       help="规则文件；两个修订号按同一规则摘要定位报告")
    p_cmp.add_argument("--json", action="store_true")

    p_ref = sub.add_parser("reference", help="参考计算程序（独立路径，小样本核对）")
    p_ref.add_argument("lake")
    p_ref.add_argument("--rules", help="规则 JSON 文件；省略表示空规则")
    p_ref.add_argument("--revision", type=int, default=None)
    p_ref.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)

    try:
        if args.cmd == "create":
            table = LakeTable.open(args.lake)
            store = ProfileStore(table)
            rules = _load_rules_arg(args)
            report = store.create_or_get(
                rules, version=args.revision, batch_size=args.batch_size
            )
            _print_report(report, args.json)
            return 0

        if args.cmd == "show":
            table = LakeTable.open(args.lake)
            store = ProfileStore(table)
            report = store.get(args.revision, args.digest)
            if args.json:
                print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
            else:
                print(format_report(report))
            return 0

        if args.cmd == "list":
            table = LakeTable.open(args.lake)
            store = ProfileStore(table)
            reports = store.list_reports()
            if args.json:
                print(json.dumps(reports, indent=2, sort_keys=True, ensure_ascii=False))
            else:
                if not reports:
                    print("（无冻结报告）")
                for r in reports:
                    print(
                        f"{r['report_id']}  rev={r['revision']}  "
                        f"{'PASS' if r['passed'] else 'FAIL'}  {r['rule_summary']}"
                    )
            return 0

        if args.cmd == "compare":
            table = LakeTable.open(args.lake)
            store = ProfileStore(table)
            old = _resolve_report(store, args.old_rev, args.old_digest, args.rules)
            new = _resolve_report(store, args.new_rev, args.new_digest, args.rules)
            diff = compare_reports(old, new)
            if args.json:
                print(json.dumps(diff, indent=2, sort_keys=True, ensure_ascii=False))
            else:
                print(format_comparison(diff))
            return 0

        if args.cmd == "reference":
            table = LakeTable.open(args.lake)
            rules = _load_rules_arg(args)
            version = args.revision
            if version is None:
                version = table.current_version()
            report = reference_profile(table, version, rules)
            if args.json:
                print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False))
            else:
                print(format_report(report))
                print("\n（参考实现 lake.quality.reference 的结果，非冻结报告）")
            return 0

    except (OSError, ValueError, LakeError) as e:
        # 错误配置（RuleSpecError）不落盘 -> 2；找不到报告 -> 3；其余 -> 1
        from ..errors import ProfileNotFoundError, RuleSpecError

        if isinstance(e, RuleSpecError):
            code = EXIT_RULE_ERROR
        elif isinstance(e, (FileNotFoundError, ProfileNotFoundError)):
            code = EXIT_NOT_FOUND
        else:
            code = 1
        print(f"错误: {e}", file=sys.stderr)
        return code

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
