"""垃圾回收：清理不被任何受保护版本引用的文件与 staging 残骸。

受保护版本 = 当前 HEAD 版本 ∪ 所有存活读者租约指向的版本。
只有这些版本引用的数据/删除文件允许保留；其余一律删除。
staging 下超过 max_age 的候选目录（崩溃残留、失败计划）一并清除。
摄取批次记录只保留当前 manifest 批次索引引用的；孤儿记录（提交中途
崩溃的残留）删除。data/ 下的 .tmp 临时文件按 max_age 年龄保护，避免
误删其他写者在途的写入。

GC 与租约创建、版本提交（含摄取提交）共用同一把 flock，因此“读租约
集合”与“删文件”之间不会插入新的租约或新的版本；摄取的批次记录
写入与 manifest 发布在同一临界区内，GC 不会误删即将被引用的记录。
"""

from __future__ import annotations

import os
import shutil
import time

from . import manifest as mf
from . import storage
from .table import LakeTable


def collect(
    table: LakeTable,
    staging_max_age_seconds: float = 0.0,
    now: float | None = None,
) -> dict:
    now = time.time() if now is None else now
    with table.lock():
        cur_v = mf.current_version(table.meta_root)
        protected_versions = {cur_v}
        leases = []
        for name in sorted(os.listdir(table.leases_dir)):
            if not name.startswith("lease-"):
                continue
            try:
                info = storage.read_json(os.path.join(table.leases_dir, name))
            except (OSError, ValueError):
                continue  # 租约文件写了一半：下一轮再处理
            leases.append(info)
            protected_versions.add(info["version"])

        protected_paths: set[str] = set()
        for v in protected_versions:
            m = mf.load_manifest(table.meta_root, v)
            for meta in m["data_files"].values():
                protected_paths.add(meta["path"])
            for meta in m["delete_files"].values():
                protected_paths.add(meta["path"])

        removed_files = []
        for name in sorted(os.listdir(table.data_dir)):
            rel = f"data/{name}"
            abspath = os.path.join(table.data_dir, name)
            if name.startswith(".tmp-"):
                # 在途写入的临时文件按年龄保护，只清理确定是残骸的
                if now - os.path.getmtime(abspath) >= staging_max_age_seconds:
                    os.unlink(abspath)
                    removed_files.append(rel)
            elif rel not in protected_paths:
                os.unlink(abspath)
                removed_files.append(rel)

        removed_staging = []
        for name in sorted(os.listdir(table.staging_dir)):
            d = os.path.join(table.staging_dir, name)
            if not os.path.isdir(d):
                continue
            if now - os.path.getmtime(d) >= staging_max_age_seconds:
                shutil.rmtree(d, ignore_errors=True)
                removed_staging.append(name)

        # 摄取批次记录：批次索引只增不减地随 manifest 携带，当前 manifest
        # 未引用的记录只可能是提交中途崩溃的孤儿（记录写入与 HEAD 翻转在
        # 同一把锁内，GC 不会与在途提交交错）。
        from .ingest import INGEST_REL_DIR, ingest_state

        referenced_records = {
            b["record"]
            for b in ingest_state(mf.load_manifest(table.meta_root, cur_v))[
                "batches"
            ].values()
        }
        removed_ingest_records = []
        batches_dir = os.path.join(table.root, INGEST_REL_DIR)
        if os.path.isdir(batches_dir):
            for name in sorted(os.listdir(batches_dir)):
                rel = os.path.join(INGEST_REL_DIR, name)
                if rel not in referenced_records:
                    os.unlink(os.path.join(batches_dir, name))
                    removed_ingest_records.append(rel)

        return {
            "current_version": cur_v,
            "protected_versions": sorted(protected_versions),
            "active_leases": leases,
            "removed_files": removed_files,
            "removed_staging": removed_staging,
            "removed_ingest_records": removed_ingest_records,
        }
