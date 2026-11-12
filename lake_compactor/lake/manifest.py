"""Manifest 版本与 HEAD 指针。

表 = 一串不可变 manifest 版本。每个版本完整列出：
  - data_files:   {data_file_id: {path, num_rows}}
  - delete_files: {delete_file_id: {path, target, num_deletes}}
      位置删除文件内的每一行显式记录 (data_file_id, pos)，
      pos 是该数据文件内从零开始的物理行号。

发布点 = HEAD 文件的原子替换：
  1. 先把新版本 manifest 写到 versions/vNNNNNN.json（不可变文件，rename 就位）；
  2. 再原子替换 HEAD（内容为版本号）。
读者读 HEAD 得到版本号后再读对应 manifest；由于 manifest 先于 HEAD 就位，
读者要么看到旧版本、要么看到完整的新版本，这就是跨进程的原子发布点。
"""

from __future__ import annotations

import os

from . import storage

VERSIONS_DIR = "versions"
HEAD_FILE = "HEAD"


def versions_dir(meta_root: str) -> str:
    return os.path.join(meta_root, VERSIONS_DIR)


def version_path(meta_root: str, version: int) -> str:
    return os.path.join(versions_dir(meta_root), f"v{version:06d}.json")


def head_path(meta_root: str) -> str:
    return os.path.join(meta_root, HEAD_FILE)


def current_version(meta_root: str) -> int:
    with open(head_path(meta_root), "r", encoding="utf-8") as f:
        return int(f.read().strip())


def load_manifest(meta_root: str, version: int) -> dict:
    return storage.read_json(version_path(meta_root, version))


def load_current(meta_root: str) -> dict:
    return load_manifest(meta_root, current_version(meta_root))


def publish_manifest(meta_root: str, manifest: dict) -> None:
    """原子发布一个新版本：先落 manifest 文件，再翻转 HEAD。"""
    v = manifest["version"]
    storage.atomic_write_json(version_path(meta_root, v), manifest)
    storage.atomic_write_bytes(head_path(meta_root), f"{v}\n".encode("utf-8"))
