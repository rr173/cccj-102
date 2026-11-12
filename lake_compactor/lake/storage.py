"""底层存储原语：原子写、目录 fsync、跨进程文件锁。

发布点的原子性依赖 POSIX rename 在同一文件系统内的原子性：
先写临时文件并 fsync，再 os.replace 到目标名，最后 fsync 目录。
读者要么看到旧文件、要么看到完整的新文件，绝不会看到写了一半的内容。
"""

from __future__ import annotations

import fcntl
import json
import os
import tempfile


def fsync_dir(path: str) -> None:
    """fsync 目录本身，保证目录项（rename 结果）落盘。"""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: str, data: bytes) -> None:
    """把 data 原子地写到 path（同目录临时文件 + fsync + rename + 目录 fsync）。"""
    directory = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        fsync_dir(directory)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def atomic_write_json(path: str, obj) -> None:
    atomic_write_bytes(
        path, json.dumps(obj, indent=2, sort_keys=True).encode("utf-8")
    )


def read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


class FileLock:
    """基于 fcntl.flock 的跨进程排他锁（同一台机器上的多进程互斥）。

    所有会推进版本的写操作（append / publish_deletes / commit_plan）
    以及租约创建、垃圾回收都在此锁下执行，从而：
      - 版本号单调递增且每个版本恰好由一个写者发布；
      - GC 与租约创建互斥，GC 不会漏看任何一个存活租约。
    """

    def __init__(self, path: str):
        self.path = path
        self._fh = None

    def __enter__(self) -> "FileLock":
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._fh = open(self.path, "a+b")
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc) -> None:
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None
