"""压实：生成计划（staging 候选）与提交计划（原子发布）。

create_plan  在绑定分支的尖端 V 选中若干小文件，剔除已删除行，把活行
             合并成一个新候选 Parquet 写入 staging/，并记录
             “旧物理行号 -> 新物理行号”的映射与生成时所在的分支。
             候选不进入读路径。

commit_plan  在提交锁下把**本线**当前尖端 W 与基版本 V 比较：
             - 计划绑定的分支必须与提交者当前绑定分支一致，否则拒绝；
             - 选中文件若在 W 中已不存在（被别的压实替换）→ 冲突失败；
             - W 相对 V 的新增追加文件原样保留；
             - W 相对 V 新到的、针对选中文件的删除，按映射翻译到
               新文件的物理位置上，随新版本一起发布；
             - 基版本已有的删除已被物理剔除，随之吸收（不再保留）。
             另一条分支上的推进完全不影响本线；本线内部的竞争仍按上述
             规则仲裁。
"""

from __future__ import annotations

import os
import shutil
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from . import manifest as mf
from . import storage
from .errors import BranchMismatchError, ConflictError
from .table import DELETE_SCHEMA, LakeTable


def _plan_dir(table: LakeTable, plan_id: str) -> str:
    return os.path.join(table.staging_dir, f"compact-{plan_id}")


def load_plan(table: LakeTable, plan_id: str) -> dict:
    return storage.read_json(os.path.join(_plan_dir(table, plan_id), "plan.json"))


def create_plan(
    table: LakeTable,
    selected: list[str] | None = None,
    max_rows: int | None = None,
) -> str:
    """生成压实计划并把候选写入 staging，返回 plan_id。

    selected 为 None 时按 max_rows 阈值自动挑选小文件；显式传入时保留
    给定顺序——该顺序就是活行在合并文件中的拼接顺序，位置映射据此生成。
    计划记录生成时的分支名与基版本，提交时据此核验归属。
    """
    base = table.manifest()
    base_version = base["version"]
    if selected is None:
        if max_rows is None:
            raise ValueError("either selected or max_rows is required")
        selected = sorted(
            fid
            for fid, meta in base["data_files"].items()
            if meta["num_rows"] <= max_rows
        )
    else:
        selected = list(selected)  # 保留调用方顺序，即合并文件内的拼接顺序
    for fid in selected:
        if fid not in base["data_files"]:
            raise KeyError(f"unknown data file {fid}")

    plan_id = uuid.uuid4().hex
    pdir = _plan_dir(table, plan_id)
    os.makedirs(pdir)

    deletes = table._all_deleted_positions(base)
    merged_id = uuid.uuid4().hex
    mapping: dict[str, list[list[int]]] = {}  # old_fid -> [[old_pos, new_pos], ...]
    absorbed: dict[str, list[int]] = {}       # old_fid -> [被吸收的删除位置]
    kept_tables = []
    new_pos = 0
    for fid in selected:
        meta = base["data_files"][fid]
        t = pq.read_table(os.path.join(table.root, meta["path"]))
        dead = deletes.get(fid, set())
        keep = [i for i in range(t.num_rows) if i not in dead]
        mapping[fid] = [[p, new_pos + i] for i, p in enumerate(keep)]
        absorbed[fid] = sorted(dead)
        if keep:
            kept_tables.append(t.take(pa.array(keep, pa.int64())))
        new_pos += len(keep)

    total = new_pos
    merged_file = None
    if total > 0:
        merged = pa.concat_tables(kept_tables)
        pq.write_table(merged, os.path.join(pdir, f"data-{merged_id}.parquet"))
        merged_file = merged_id

    plan = {
        "plan_id": plan_id,
        "base_version": base_version,
        "base_branch": table.branch,
        "parent": base.get("parent"),
        "selected": selected,
        "merged_file": merged_file,
        "num_rows": total,
        "mapping": mapping,
        "absorbed": absorbed,
        "state": "executed",
        "created_at": time.time(),
    }
    storage.atomic_write_json(os.path.join(pdir, "plan.json"), plan)
    return plan_id


def commit_plan(table: LakeTable, plan_id: str) -> int:
    """提交压实计划，原子发布到本线尖端；冲突时抛 ConflictError。"""
    pdir = _plan_dir(table, plan_id)
    plan = load_plan(table, plan_id)
    if plan["state"] != "executed":
        raise ValueError(f"plan {plan_id} is in state {plan['state']}")

    with table.lock():
        # 计划属于生成它的那条分支：不能拿一条线的成果去推进另一条线。
        if plan.get("base_branch", mf.MAIN_BRANCH) != table.branch:
            raise BranchMismatchError(
                f"plan {plan_id} was created on branch "
                f"{plan.get('base_branch')!r}, cannot commit it on "
                f"{table.branch!r}"
            )
        cur = table._current_manifest()  # 本线尖端，不是全库 HEAD
        base = mf.load_manifest(table.meta_root, plan["base_version"])

        # 冲突检测：选中文件已被其他压实替换（在当前版本中消失）。
        missing = [f for f in plan["selected"] if f not in cur["data_files"]]
        if missing:
            plan["state"] = "failed"
            plan["error"] = f"selected files already replaced: {missing}"
            storage.atomic_write_json(os.path.join(pdir, "plan.json"), plan)
            raise ConflictError(plan["error"])

        selected = set(plan["selected"])
        pos_maps = {
            fid: {int(o): int(n) for o, n in pairs}
            for fid, pairs in plan["mapping"].items()
        }
        merged_file = plan["merged_file"]

        # 1) 候选数据文件从 staging 移入 data/（先就位，后发布）。
        new_data_files: dict[str, dict] = {}
        if merged_file is not None:
            rel = f"data/data-{merged_file}.parquet"
            os.replace(
                os.path.join(pdir, f"data-{merged_file}.parquet"),
                os.path.join(table.root, rel),
            )
            storage.fsync_dir(table.data_dir)
            new_data_files[merged_file] = {"path": rel, "num_rows": plan["num_rows"]}

        # 2) 删除文件分类：无关的带走、基版本已有的吸收、新到的翻译。
        carried: dict[str, dict] = {}
        translated: list[int] = []
        for did, dm in cur["delete_files"].items():
            tgt = dm["target"]
            if tgt not in selected:
                carried[did] = dm  # 与本次压实无关，原样保留
                continue
            if did in base["delete_files"]:
                continue  # 基版本已有的删除：行已被物理剔除，吸收
            if merged_file is None:
                continue  # 选中文件已无任何活行，删除无的放矢
            # 计划生成之后新到的删除：翻译到新文件位置
            t = pq.read_table(os.path.join(table.root, dm["path"]))
            for pos in t.column("pos").to_pylist():
                new_pos = pos_maps[tgt].get(pos)
                if new_pos is not None:
                    translated.append(new_pos)

        new_delete_files: dict[str, dict] = {}
        if translated:
            translated = sorted(set(translated))
            did = uuid.uuid4().hex
            rel = f"data/deletes-{did}.parquet"
            deletes = pa.table(
                {
                    "data_file_id": pa.array(
                        [merged_file] * len(translated), pa.string()
                    ),
                    "pos": pa.array(translated, pa.int64()),
                },
                schema=DELETE_SCHEMA,
            )
            tmp = os.path.join(table.data_dir, f".tmp-{did}")
            pq.write_table(deletes, tmp)
            os.replace(tmp, os.path.join(table.root, rel))
            storage.fsync_dir(table.data_dir)
            new_delete_files[did] = {
                "path": rel,
                "target": merged_file,
                "num_deletes": len(translated),
            }

        # 3) 组装并原子发布新版本：无关追加随 cur 自然保留。
        new = table._next(cur)
        for fid in selected:
            new["data_files"].pop(fid, None)
        new["data_files"].update(new_data_files)
        new["delete_files"] = {**carried, **new_delete_files}
        table._publish(new)

        shutil.rmtree(pdir, ignore_errors=True)
        return new["version"]
