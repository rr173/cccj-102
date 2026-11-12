"""命名实验分支：建立、列举、撤销，以及把实验尖端快进交付给 main。

设计要点：

  - **main 是正式数据**，尖端存在旧有的 `metadata/HEAD` 里。实验分支尖端
    存在 `metadata/branches.json`。旧格式目录没有该文件，自然只有 main，
    所有旧调用（不传 branch）行为不变。
  - **不复制底层内容**：建分支只是在 branches.json 里写一条
    `name -> 某个已有提交号` 的引用；数据/删除文件与 manifest 版本都在
    原处共享。
  - **版本号全库唯一**：新提交的号由 `manifest.next_version()` 在提交锁内
    取“全部已落盘版本号最大值 + 1”，父子关系写进 manifest 的 `parent`，
    因此两条分支先后提交也不会生成同号记录。
  - **撤销不删历史**：撤销只移除分支引用；该分支独有的版本 manifest 与
    数据文件在没有任何分支/租约/后代引用时由 GC 回收。正在读取的应用
    持有租约，回收器看得到租约指向的版本并继续保护其文件。
  - **publish 只做快进**：要求 main 尖端是实验尖端的祖先（含同提交），
    调用方必须传入预期的 main 尖端；成功时只把 HEAD 指向**已存在的**
    实验提交号，不新建版本、不复制样本。分叉则明确拒绝，由人工处理，
    绝不自动内容合并。
"""

from __future__ import annotations

import os
import re

from . import manifest as mf
from .errors import (
    BranchBaseError,
    BranchExistsError,
    BranchNameError,
    BranchNotFoundError,
    DivergedError,
    PublishError,
    ProtectedBranchError,
    SourceIsMainError,
    StaleMainError,
)

# 保留名：main 是正式数据的固定分支，实验分支不得占用或撤销。
RESERVED_NAMES = {mf.MAIN_BRANCH}
# 引擎自身目录/入口名字，避免分支名与磁盘布局混淆（main 不在其中）。
LAYOUT_NAMES = {"metadata", "data", "staging", "splits", "quality_profiles"}
# 允许：字母数字与 . _ -，必须以字母数字开头，长度 1..64。
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def validate_name(name) -> str:
    """校验分支名**语法**；非法名字抛 BranchNameError（不落盘任何东西）。

    注意 ``main`` 是合法名字（它就是要绑定的正式分支）；保留只发生在
    “新建”时（不能新建第二个 main）与“撤销”时（main 受保护）。
    """
    if not isinstance(name, str) or not name:
        raise BranchNameError("branch name must be a non-empty string")
    if not _NAME_RE.match(name):
        raise BranchNameError(
            f"invalid branch name {name!r}: must match "
            f"{_NAME_RE.pattern} (letters, digits, '.', '_', '-' ; "
            "must start with a letter or digit; at most 64 chars)"
        )
    if name.startswith(".") or name.endswith("."):
        raise BranchNameError(
            f"invalid branch name {name!r}: must not start or end with '.'"
        )
    if name in LAYOUT_NAMES:
        raise BranchNameError(
            f"invalid branch name {name!r}: reserved for engine layout"
        )
    return name


def _validate_new_branch_name(name: str) -> str:
    """新建分支时的名字校验：语法合法且不占用保留名。"""
    validate_name(name)
    if name == mf.MAIN_BRANCH:
        raise BranchExistsError(
            f"branch name {name!r} is reserved for official data"
        )
    for reserved in RESERVED_NAMES:
        if name.casefold() == reserved.casefold():
            raise BranchExistsError(
                f"branch name {name!r} conflicts with reserved name {reserved!r}"
            )
    return name


def create_branch(table, name: str, from_version: int | None = None) -> dict:
    """从某个已有提交建立命名分支，返回其初始尖端信息。

    from_version=None 表示从**当前绑定分支的尖端**建立（通常是 main，
    也可以从另一条实验线再拉一条线）。基提交不必是本线尖端：可以从已有
    的任意提交建立（与 `git branch <name> <commit>` 一致）。
    """
    _validate_new_branch_name(name)
    if from_version is not None:
        if isinstance(from_version, bool) or not isinstance(from_version, int):
            raise BranchBaseError(
                f"base commit must be an int, got {from_version!r}"
            )
    with table.lock():
        if name in mf.load_branches(table.meta_root):
            raise BranchExistsError(f"branch {name!r} already exists")
        base = table.current_version() if from_version is None else from_version
        if not os.path.exists(mf.version_path(table.meta_root, base)):
            raise BranchBaseError(f"base commit v{base} does not exist")
        base_manifest = mf.load_manifest(table.meta_root, base)
        mf.set_branch_tip(table.meta_root, name, base)
        return {
            "name": name,
            "tip": base,
            "base_version": base,
            "parent": base_manifest.get("parent"),
            "copied_versions": 0,
            "shared_files": len(base_manifest["data_files"])
            + len(base_manifest["delete_files"]),
        }


def list_branches(table) -> list[dict]:
    """列举全部存活分支（main 在最前）及其尖端、父提交和提交元信息。"""
    with table.lock():
        tips = mf.all_tips(table.meta_root)
        out = []
        for name, tip in tips.items():
            m = mf.load_manifest(table.meta_root, tip)
            out.append(
                {
                    "name": name,
                    "tip": tip,
                    "parent": m.get("parent"),
                    "created_at": m.get("created_at"),
                    "num_data_files": len(m["data_files"]),
                    "num_delete_files": len(m["delete_files"]),
                    "ingest_batches": sorted(
                        (m.get("ingest") or {}).get("batches", {})
                    ),
                    "official": name == mf.MAIN_BRANCH,
                }
            )
        return out


def delete_branch(table, name: str) -> dict:
    """撤销实验分支：只移除引用，历史版本与数据交给 GC 按引用决定去留。

    main 受保护，明确拒绝；撤销当前对象自己绑定的分支时，对象随后无法
    再解析尖端（调用方应改用 checkout(main) / open(root)）。
    """
    validate_name(name)
    with table.lock():
        if name in RESERVED_NAMES:
            raise ProtectedBranchError("main branch cannot be deleted")
        branches = mf.load_branches(table.meta_root)
        if name not in branches:
            raise BranchNotFoundError(f"branch {name!r} does not exist")
        tip = branches[name]
        remaining_exclusive = _exclusive_versions(table.meta_root, tip)
        mf.remove_branch(table.meta_root, name)
        return {
            "name": name,
            "deleted_tip": tip,
            # 撤销后不再被任何尖端引用、但仍留在 versions/ 的提交；它们的
            # 数据文件由下一次 GC 按“所有存活分支 + 租约”判定去留。
            "exclusive_versions": remaining_exclusive,
        }


def _exclusive_versions(meta_root: str, tip: int) -> list[int]:
    """tip 回溯链上不在其他任何分支尖端祖先链里的提交（撤销后即无引用）。"""
    all_tips = mf.all_tips(meta_root)
    others = [t for t in all_tips.values() if t != tip]
    other_anc = set()
    for t in others:
        other_anc.update(mf.ancestors(meta_root, t))
    mine = set(mf.ancestors(meta_root, tip))
    return sorted(mine - other_anc)


def publish(table, expected_main_version: int) -> dict:
    """把绑定分支的尖端交给 main（快进），返回交付结果。

    规则：

      - 绑定对象本身就是 main → SourceIsMainError，无意义；
      - 先核对调用方的 expected_main_version 等于当前 main 尖端，
        否则 StaleMainError（防止检查后又被别的正式写者抢先推进）；
      - main 尖端必须是实验尖端的祖先（含同一提交），否则 DivergedError
        （两条线已分叉，不自动内容合并）；
      - 成功：只把 HEAD 原子指向实验尖端这个**已存在的提交**。不写新
        manifest、不复制任何数据文件；实验分支引用保留，可继续推进或撤销。
      - 任何拒绝路径都不移动双方尖端、不产生新版本、不动数据文件。
    """
    if not isinstance(expected_main_version, int) or isinstance(
        expected_main_version, bool
    ):
        raise PublishError(
            "invalid-expected-main",
            f"expected_main_version must be an int, got {expected_main_version!r}",
        )
    if table.branch == mf.MAIN_BRANCH:
        raise SourceIsMainError(table.current_version())

    with table.lock():
        main_tip = mf.current_version(table.meta_root)
        if expected_main_version != main_tip:
            raise StaleMainError(expected_main_version, main_tip)

        branches = mf.load_branches(table.meta_root)
        if table.branch not in branches:
            raise BranchNotFoundError(f"branch {table.branch!r} does not exist")
        source_tip = branches[table.branch]
        source_manifest = mf.load_manifest(table.meta_root, source_tip)

        if not mf.is_ancestor(table.meta_root, main_tip, source_tip):
            raise DivergedError(
                main_version=main_tip,
                source_version=source_tip,
                main_parent=mf.load_manifest(
                    table.meta_root, main_tip
                ).get("parent"),
                source_parent=source_manifest.get("parent"),
            )

        # 成功路径：main 指向已有实验提交。target manifest 已存在，
        # 这里只移动 HEAD（原子 rename），绝不重写它。
        mf.set_head(table.meta_root, source_tip)
        advanced = sorted(
            set(mf.ancestors(table.meta_root, source_tip))
            - set(mf.ancestors(table.meta_root, main_tip))
        )
        return {
            "branch": table.branch,
            "source_version": source_tip,
            "main_before": main_tip,
            "main_after": source_tip,
            "advanced_by": source_tip - main_tip,
            "commits": advanced,
            "reused_commit": True,
            "copied_rows": 0,
            "branch_still_exists": True,
        }
