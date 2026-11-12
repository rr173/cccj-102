"""Manifest 版本、HEAD 指针与分支尖端。

表 = 一串不可变 manifest 版本。每个版本完整列出：
  - data_files:   {data_file_id: {path, num_rows}}
  - delete_files: {delete_file_id: {path, target, num_deletes}}
      位置删除文件内的每一行显式记录 (data_file_id, pos)，
      pos 是该数据文件内从零开始的物理行号。

版本编号是**全库唯一坐标**：任何分支上的提交都在提交锁内从“全部已有
版本号的最大值 + 1”取号，两个分支不可能各自生成同号记录。父子关系
记在每个 manifest 的 ``parent`` 字段里，因此提交图不依赖分支名。

发布点 = HEAD / 分支尖端的原子替换：
  1. 先把新版本 manifest 写到 versions/vNNNNNN.json（不可变文件，rename 就位）；
  2. 再原子替换 HEAD（main）或 branches.json（实验分支）。
读者读 HEAD（或分支尖端）得到版本号后再读对应 manifest；由于 manifest
先于指针就位，读者要么看到旧版本、要么看到完整的新版本。
"""

from __future__ import annotations

import os

from . import storage
from .errors import BranchNotFoundError

VERSIONS_DIR = "versions"
HEAD_FILE = "HEAD"
BRANCHES_FILE = "branches.json"
MAIN_BRANCH = "main"


def versions_dir(meta_root: str) -> str:
    return os.path.join(meta_root, VERSIONS_DIR)


def version_path(meta_root: str, version: int) -> str:
    return os.path.join(versions_dir(meta_root), f"v{version:06d}.json")


def head_path(meta_root: str) -> str:
    return os.path.join(meta_root, HEAD_FILE)


def branches_path(meta_root: str) -> str:
    return os.path.join(meta_root, BRANCHES_FILE)


def current_version(meta_root: str) -> int:
    """main 分支尖端（HEAD 文件）。"""
    with open(head_path(meta_root), "r", encoding="utf-8") as f:
        return int(f.read().strip())


def load_manifest(meta_root: str, version: int) -> dict:
    return storage.read_json(version_path(meta_root, version))


def load_current(meta_root: str) -> dict:
    """main 分支尖端的 manifest（旧入口；分支表请用 LakeTable.manifest）。"""
    return load_manifest(meta_root, current_version(meta_root))


def write_manifest(meta_root: str, manifest: dict) -> None:
    """只写不可变 manifest 文件，不移动任何指针。"""
    storage.atomic_write_json(version_path(meta_root, manifest["version"]), manifest)


def set_head(meta_root: str, version: int) -> None:
    """把 main 分支尖端原子地移到 version（manifest 必须已就位）。"""
    if not os.path.exists(version_path(meta_root, version)):
        raise FileNotFoundError(f"manifest v{version} does not exist")
    storage.atomic_write_bytes(head_path(meta_root), f"{version}\n".encode("utf-8"))


def publish_manifest(meta_root: str, manifest: dict) -> None:
    """原子发布一个新 main 版本：先落 manifest 文件，再翻转 HEAD。

    保留此入口以兼容旧调用方；分支提交走 write_manifest + set_branch_tip。
    """
    write_manifest(meta_root, manifest)
    set_head(meta_root, manifest["version"])


# ---------------------------------------------------------------- 分支尖端
def load_branches(meta_root: str) -> dict[str, int]:
    """读实验分支尖端。旧格式目录没有 branches.json，自然视为空（只有 main）。"""
    path = branches_path(meta_root)
    if not os.path.exists(path):
        return {}
    data = storage.read_json(path)
    raw = data.get("branches") or {}
    return {str(name): int(tip) for name, tip in raw.items()}


def _write_branches(meta_root: str, branches: dict[str, int]) -> None:
    storage.atomic_write_json(
        branches_path(meta_root),
        {"branches": {name: int(tip) for name, tip in sorted(branches.items())}},
    )


def set_branch_tip(meta_root: str, name: str, version: int) -> None:
    """在提交锁内把某条实验分支的尖端移到已存在的 version。"""
    if not os.path.exists(version_path(meta_root, version)):
        raise FileNotFoundError(f"manifest v{version} does not exist")
    branches = load_branches(meta_root)
    branches[name] = version
    _write_branches(meta_root, branches)


def remove_branch(meta_root: str, name: str) -> None:
    branches = load_branches(meta_root)
    if name not in branches:
        raise BranchNotFoundError(f"branch {name!r} does not exist")
    del branches[name]
    _write_branches(meta_root, branches)


def branch_tip(meta_root: str, name: str) -> int:
    """解析分支尖端；main 始终来自 HEAD，因此旧目录无需迁移。"""
    if name == MAIN_BRANCH:
        return current_version(meta_root)
    branches = load_branches(meta_root)
    if name not in branches:
        raise BranchNotFoundError(f"branch {name!r} does not exist")
    return branches[name]


def all_tips(meta_root: str) -> dict[str, int]:
    """全部存活分支的尖端（含 main），GC 与全库取号都依赖它。"""
    tips = {MAIN_BRANCH: current_version(meta_root)}
    tips.update(load_branches(meta_root))
    return dict(sorted(tips.items()))


def next_version(meta_root: str) -> int:
    """全库唯一的下一提交号 = 所有已落盘版本号的最大值 + 1。

    调用方必须持有提交锁。manifest 只增不减，已落盘但指针未翻转的
    崩溃残骸也占用过号码，因此这里同时覆盖 versions/ 目录与全部尖端。
    """
    highest = 0
    vdir = versions_dir(meta_root)
    if os.path.isdir(vdir):
        for name in os.listdir(vdir):
            if name.startswith("v") and name.endswith(".json"):
                try:
                    highest = max(highest, int(name[1:-5]))
                except ValueError:
                    continue
    for tip in all_tips(meta_root).values():
        highest = max(highest, tip)
    return highest + 1


def ancestors(meta_root: str, version: int) -> list[int]:
    """从 version 沿 parent 链回溯到根（含 version 自身）。"""
    out = []
    seen = set()
    cur = version
    while cur is not None:
        if cur in seen:
            break  # 防御损坏的环
        seen.add(cur)
        out.append(cur)
        path = version_path(meta_root, cur)
        if not os.path.exists(path):
            break
        cur = load_manifest(meta_root, cur).get("parent")
    return out


def is_ancestor(meta_root: str, candidate: int, descendant: int) -> bool:
    """candidate 是否是 descendant 的祖先（或同一提交）。"""
    return candidate in set(ancestors(meta_root, descendant))

