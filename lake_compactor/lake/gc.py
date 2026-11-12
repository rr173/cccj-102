"""垃圾回收：清理不被任何受保护版本引用的文件与 staging 残骸。

受保护版本 = 当前 HEAD 版本 ∪ 所有存活读者租约指向的版本。
只有这些版本引用的数据/删除文件允许保留；其余一律删除。
staging 下超过 max_age 的候选目录（崩溃残留、失败计划）一并清除。

GC 与租约创建、版本提交共用同一把 flock，因此“读租约集合”
与“删文件”之间不会插入新的租约或新的版本。
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
            if name.startswith(".tmp-") or rel not in protected_paths:
                os.unlink(os.path.join(table.data_dir, name))
                removed_files.append(rel)

        removed_staging = []
        for name in sorted(os.listdir(table.staging_dir)):
            d = os.path.join(table.staging_dir, name)
            if not os.path.isdir(d):
                continue
            if now - os.path.getmtime(d) >= staging_max_age_seconds:
                shutil.rmtree(d, ignore_errors=True)
                removed_staging.append(name)

        return {
            "current_version": cur_v,
            "protected_versions": sorted(protected_versions),
            "active_leases": leases,
            "removed_files": removed_files,
            "removed_staging": removed_staging,
        }
