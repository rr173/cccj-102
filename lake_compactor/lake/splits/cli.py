"""分组抽样切分的命令行入口。

    python3 -m lake.splits.cli create LAKE --train N --validation N --test N
                                         --seed N [--snapshot V]
                                         [--batch-size N] [--json]
    python3 -m lake.splits.cli check  SPLIT_DIR [--json]
    python3 -m lake.splits.cli list   LAKE [--json]

权重是整数“万分份”，非负且总和必须为 10000。create 打印交付目录与清单
摘要；check 校验一个已交付目录（清单、校验和、分组不重不漏）。
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from ..errors import LakeError, SplitError
from ..table import LakeTable
from .split import (
    SPLITS_DIRNAME,
    check_split,
    create_split,
    read_manifest,
)

EXIT_SPLIT_ERROR = 2
EXIT_NOT_FOUND = 3


def _emit(obj, as_json: bool, text_fn) -> None:
    if as_json:
        print(json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False))
    else:
        text_fn(obj)


def _format_created(m: dict) -> None:
    print(f"交付目录: splits/{m['split_id']}")
    print(
        f"输入 snapshot: v{m['input_version']}"
        f"（{'显式指定' if m.get('input_version_explicit') else '调用时 HEAD'}）"
    )
    print(f"seed={m['seed']}  权重={m['weights']}  算法版本={m['algorithm_version']}")
    print(
        f"输入可见行 {m['input_snapshot']['num_visible_rows']} 行 / "
        f"{m['input_snapshot']['num_objects']} 个对象"
    )
    print("子集        行数      对象数    文件校验和")
    for s in ("train", "validation", "test"):
        meta = m["subsets"].get(s, {})
        print(
            f"  {s:<10} {meta.get('num_rows', 0):>7} "
            f"{meta.get('num_objects', 0):>9}  {meta.get('sha256', '')[:12]}…"
        )
    print(f"复用原切分: {'是' if m.get('reused') else '否（本次新发布）'}")
    print(f"注意: {m.get('proportionality_notice', '')}")


def _format_check(result: dict) -> None:
    print(f"split_id: {result['split_id']}  输入 snapshot: v{result['input_version']}")
    print(f"seed={result['seed']}  权重={result['weights']}")
    print("子集        行数      对象数")
    for s in ("train", "validation", "test"):
        meta = result["subsets"][s]
        print(f"  {s:<10} {meta['num_rows']:>7} {meta['num_objects']:>9}")
    print(
        f"合计 {result['num_rows']} 行 / {result['num_objects']} 个对象；"
        "对象不跨集合、不重不漏：通过"
    )


def _format_list(entries: list[dict]) -> None:
    if not entries:
        print("（无已交付切分）")
        return
    for e in entries:
        print(
            f"{e['split_id']}  snapshot=v{e['input_version']}  "
            f"seed={e['seed']}  weights={e['weights']}  "
            f"rows={e.get('num_rows', '?')}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lake.splits", description="按 row_id 成组抽样切分 train/validation/test"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_create = sub.add_parser("create", help="产出（或取回）切分交付目录")
    p_create.add_argument("lake")
    p_create.add_argument("--train", type=int, required=True)
    p_create.add_argument("--validation", type=int, required=True)
    p_create.add_argument("--test", type=int, required=True)
    p_create.add_argument("--seed", type=int, required=True)
    p_create.add_argument(
        "--snapshot", type=int, default=None, help="输入 manifest 版本号；省略为当前 HEAD"
    )
    p_create.add_argument("--batch-size", type=int, default=None)
    p_create.add_argument("--json", action="store_true")

    p_check = sub.add_parser("check", help="校验一个已交付切分目录")
    p_check.add_argument("split_dir")
    p_check.add_argument("--json", action="store_true")

    p_list = sub.add_parser("list", help="列出湖表下的全部已交付切分")
    p_list.add_argument("lake")
    p_list.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)

    try:
        if args.cmd == "create":
            table = LakeTable.open(args.lake)
            batch_size = (
                4096 if args.batch_size is None else args.batch_size
            )
            if batch_size <= 0:
                raise SplitError("batch size must be positive")
            manifest = create_split(
                table,
                args.snapshot,
                args.train,
                args.validation,
                args.test,
                args.seed,
                batch_size=batch_size,
            )
            manifest["directory"] = os.path.join(
                SPLITS_DIRNAME, manifest["split_id"]
            )
            _emit(manifest, args.json, _format_created)
            return 0

        if args.cmd == "check":
            result = check_split(args.split_dir)
            _emit(result, args.json, _format_check)
            return 0

        if args.cmd == "list":
            root = os.path.join(os.path.abspath(args.lake), SPLITS_DIRNAME)
            entries = []
            if os.path.isdir(root):
                for name in sorted(os.listdir(root)):
                    d = os.path.join(root, name)
                    if not os.path.isdir(d) or name.startswith("."):
                        continue
                    try:
                        m = read_manifest(d)
                    except (OSError, ValueError):
                        continue
                    entries.append(
                        {
                            "split_id": m.get("split_id"),
                            "input_version": m.get("input_version"),
                            "seed": m.get("seed"),
                            "weights": m.get("weights"),
                            "num_rows": (m.get("input_snapshot") or {}).get(
                                "num_visible_rows"
                            ),
                        }
                    )
            _emit(entries, args.json, _format_list)
            return 0
    except LakeError as e:
        print(f"错误: {e}", file=sys.stderr)
        return EXIT_SPLIT_ERROR
    except FileNotFoundError as e:
        print(f"错误: {e}", file=sys.stderr)
        return EXIT_NOT_FOUND

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
