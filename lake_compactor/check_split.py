"""独立的分组校验脚本（不依赖 lake 引擎的读路径）。

用法：

    python3 check_split.py SPLIT_DIR [--json]

只读一个已交付切分目录：train.parquet / validation.parquet /
test.parquet / manifest.json，逐项核对：

  1. 三个文件与清单都在，文件名与清单一致；
  2. 每个文件的 SHA-256 与清单记录一致；
  3. 每个文件的行数、对象数与清单一致；
  4. 三个子集的 row_id 集合两两不相交（对象没有跨集合）；
  5. 三个子集合起来恰好是清单记录的全部输入可见行，行数不重不漏；
  6. 可选：用 --verify-assignment 按 manifest 里的 seed/权重复算每个
     row_id 的归属，要求与文件所在子集一致。

脚本刻意不 import lake 的数据扫描代码；它可以直接对交付目录运行，
用于发布前的人工/CI 核对。退出码：0 全部通过；1 发现问题；2 用法错误。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys

SPLIT_NAMES = ("train", "validation", "test")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_parquet_ids_values(path: str):
    """用 pyarrow 读 Parquet（不经过任何湖表扫描逻辑）。"""
    import pyarrow.parquet as pq

    tab = pq.read_table(path)
    ids = tab.column("row_id").to_pylist()
    vals = tab.column("value").to_pylist()
    return ids, vals


def assignment_of(manifest: dict, row_id: str) -> str:
    """按 manifest 声明的算法复算归属。

    与 lake.splits.split 使用同一份规范：SHA-256(seed 十进制 + \\x00 +
    row_id)，取前 8 字节，对 10000 取模，再按权重区间落桶。
    """
    h = hashlib.sha256()
    h.update(str(manifest["seed"]).encode("utf-8"))
    h.update(b"\x00")
    h.update(row_id.encode("utf-8"))
    bucket = int.from_bytes(h.digest()[:8], "big") % manifest["weight_total"]
    weights = manifest["weights"]
    if bucket < weights["train"]:
        return "train"
    if bucket < weights["train"] + weights["validation"]:
        return "validation"
    return "test"


def validate(directory: str, verify_assignment: bool = False) -> dict:
    problems: list[str] = []

    manifest_path = os.path.join(directory, "manifest.json")
    if not os.path.exists(manifest_path):
        return {"ok": False, "problems": [f"missing manifest: {manifest_path}"]}
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    subsets = manifest.get("subsets") or {}
    seen_objects: dict[str, str] = {}
    total_rows = 0
    total_objects = 0
    per_subset: dict[str, dict] = {}
    for name in SPLIT_NAMES:
        meta = subsets.get(name)
        if meta is None:
            problems.append(f"manifest missing subset {name}")
            continue
        path = os.path.join(directory, meta.get("path", f"{name}.parquet"))
        if not os.path.exists(path):
            problems.append(f"missing file {name}.parquet")
            continue

        got_sha = sha256_file(path)
        if got_sha != meta.get("sha256"):
            problems.append(
                f"{name}.parquet checksum mismatch: "
                f"{got_sha[:12]} != manifest {str(meta.get('sha256'))[:12]}"
            )

        ids, _vals = read_parquet_ids_values(path)
        if len(ids) != meta.get("num_rows"):
            problems.append(
                f"{name}.parquet has {len(ids)} rows, manifest says "
                f"{meta.get('num_rows')}"
            )
        objects = set(ids)
        if len(objects) != meta.get("num_objects"):
            problems.append(
                f"{name}.parquet has {len(objects)} objects, manifest says "
                f"{meta.get('num_objects')}"
            )
        for rid in ids:
            if rid is None or rid == "":
                problems.append(f"{name}.parquet contains an empty row_id")
                break
            prior = seen_objects.get(rid)
            if prior is not None and prior != name:
                problems.append(
                    f"object {rid!r} appears in both {prior} and {name}"
                )
                break
            seen_objects[rid] = name

        if verify_assignment:
            bad = [
                rid
                for rid in objects
                if assignment_of(manifest, rid) != name
            ]
            if bad:
                problems.append(
                    f"{name}.parquet has objects assigned elsewhere: {bad[:5]}"
                )

        total_rows += len(ids)
        total_objects += len(objects)
        per_subset[name] = {"num_rows": len(ids), "num_objects": len(objects)}

    snapshot = manifest.get("input_snapshot") or {}
    if total_rows != snapshot.get("num_visible_rows"):
        problems.append(
            f"three subsets total {total_rows} rows, manifest input rows "
            f"{snapshot.get('num_visible_rows')}"
        )
    if total_objects != snapshot.get("num_objects"):
        problems.append(
            f"three subsets total {total_objects} objects, manifest input "
            f"objects {snapshot.get('num_objects')}"
        )

    return {
        "ok": not problems,
        "split_id": manifest.get("split_id"),
        "input_version": manifest.get("input_version"),
        "seed": manifest.get("seed"),
        "weights": manifest.get("weights"),
        "subsets": per_subset,
        "num_rows": total_rows,
        "num_objects": total_objects,
        "problems": problems,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="check_split", description="校验一个已交付的分组抽样切分目录"
    )
    parser.add_argument("split_dir")
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--verify-assignment",
        action="store_true",
        help="按 manifest 的 seed/权重复算归属并与文件所在子集比对",
    )
    args = parser.parse_args(argv)

    if not os.path.isdir(args.split_dir):
        print(f"错误: 目录不存在: {args.split_dir}", file=sys.stderr)
        return 2

    result = validate(args.split_dir, verify_assignment=args.verify_assignment)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False))
    else:
        if result["ok"]:
            print(
                f"通过: {result['split_id']}  行数合计 {result['num_rows']}  "
                f"对象合计 {result['num_objects']}"
            )
            for name in SPLIT_NAMES:
                meta = result["subsets"].get(name, {})
                print(
                    f"  {name:<10} {meta.get('num_rows', 0):>7} 行  "
                    f"{meta.get('num_objects', 0):>7} 对象"
                )
        else:
            print("未通过:")
            for p in result["problems"]:
                print(f"  - {p}")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
