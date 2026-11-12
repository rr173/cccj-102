"""垃圾回收：清理不被任何受保护提交引用的文件与 staging 残骸。

受保护提交 = 所有存活分支尖端的 manifest ∪ 所有存活读者租约指向的版本。

只看 main（HEAD）是不够的：实验分支的尖端引用着与 main 不同的底层内容，
这些文件在 main 上不可见但必须保留。撤销分支只是移除引用，其独占尖端
随即不再出现在任何存活尖端里——若没有存活租约，它们引用的文件才允许
被删除；正在读取该内容的读者持有租约，其版本仍在保护集合中，读完之后
下一次回收才会清除。历史提交（已被本线后续提交取代）与 main 上的旧版本
同语义：只由租约保护，否则可回收。

staging 下超过 max_age 的候选目录（崩溃残留、失败计划）一并清除；
data/ 下的 .tmp 在途临时文件按同样的年龄阈值保护，只清理确定是残骸的。
摄取批次记录保留所有受保护版本批次索引引用的；孤儿记录（提交中途
崩溃的残留）删除。GC 与租约创建、版本提交（含摄取提交）共用同一把
flock，因此“读保护集合”与“删文件”之间不会插入新的租约或提交。
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
        tips = mf.all_tips(table.meta_root)
        protected_versions: set[int] = set(tips.values())

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
        referenced_records: set[str] = set()
        from .ingest import ingest_state

        for v in protected_versions:
            m = mf.load_manifest(table.meta_root, v)
            for meta in m["data_files"].values():
                protected_paths.add(meta["path"])
            for meta in m["delete_files"].values():
                protected_paths.add(meta["path"])
            for b in ingest_state(m)["batches"].values():
                # 批次记录写入与 manifest 发布在同一临界区，受保护版本
                # 引用的记录一定还在；记录缺失时不要凭空补一个路径。
                if os.path.exists(os.path.join(table.root, b["record"])):
                    referenced_records.add(b["record"])

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

        # 摄取批次记录：只保留**任一受保护版本**引用的；撤销实验分支后
        # 其独有批次的记录不再被任何存活链引用，随之回收。
        from .ingest import INGEST_REL_DIR

        removed_ingest_records = []
        batches_dir = os.path.join(table.root, INGEST_REL_DIR)
        if os.path.isdir(batches_dir):
            for name in sorted(os.listdir(batches_dir)):
                rel = os.path.join(INGEST_REL_DIR, name)
                if rel not in referenced_records:
                    os.unlink(os.path.join(batches_dir, name))
                    removed_ingest_records.append(rel)

        return {
            "current_version": tips[mf.MAIN_BRANCH],
            "branches": tips,
            "protected_versions": sorted(protected_versions),
            "active_leases": leases,
            "removed_files": removed_files,
            "removed_staging": removed_staging,
            "removed_ingest_records": removed_ingest_records,
        }
