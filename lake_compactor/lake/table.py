"""LakeTable：建表、追加、发布位置删除、扫描、读者租约。

数据文件布局（全部在单机本地文件系统上）：

    <root>/
      data/                  已发布的数据文件与位置删除文件（不可变）
      staging/               未发布的压实候选（不在读路径上）
      metadata/
        HEAD                 当前版本号（原子替换）
        LOCK                 flock 提交锁
        versions/v*.json     不可变 manifest 版本
        leases/lease-*.json  读者租约

数据文件带稳定 row_id 列，但删除一律按物理行号（0 起）记录与执行，
绝不从业务字段（包括 row_id）反推行号。
"""

from __future__ import annotations

import copy
import os
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from . import manifest as mf
from . import storage
from .errors import LeaseError

DATA_SCHEMA = pa.schema([("row_id", pa.string()), ("value", pa.int64())])
DELETE_SCHEMA = pa.schema([("data_file_id", pa.string()), ("pos", pa.int64())])


class LakeTable:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        self.meta_root = os.path.join(self.root, "metadata")
        self.data_dir = os.path.join(self.root, "data")
        self.staging_dir = os.path.join(self.root, "staging")
        self.leases_dir = os.path.join(self.meta_root, "leases")

    # ------------------------------------------------------------------ 建表
    @classmethod
    def create(cls, root: str) -> "LakeTable":
        t = cls(root)
        for d in (
            t.meta_root,
            t.data_dir,
            t.staging_dir,
            t.leases_dir,
            mf.versions_dir(t.meta_root),
        ):
            os.makedirs(d, exist_ok=True)
        m0 = {
            "version": 0,
            "parent": None,
            "created_at": time.time(),
            "data_files": {},
            "delete_files": {},
            "ingest": {"cursors": {}, "batches": {}},
        }
        mf.publish_manifest(t.meta_root, m0)
        return t

    @classmethod
    def open(cls, root: str) -> "LakeTable":
        t = cls(root)
        if not os.path.exists(mf.head_path(t.meta_root)):
            raise FileNotFoundError(f"no lake table at {root}")
        return t

    # ------------------------------------------------------------------ 锁
    def lock(self) -> storage.FileLock:
        return storage.FileLock(os.path.join(self.meta_root, "LOCK"))

    # ------------------------------------------------------------- manifest
    def current_version(self) -> int:
        return mf.current_version(self.meta_root)

    def manifest(self, version: int | None = None) -> dict:
        v = self.current_version() if version is None else version
        return mf.load_manifest(self.meta_root, v)

    def _publish(self, manifest: dict) -> None:
        mf.publish_manifest(self.meta_root, manifest)

    @staticmethod
    def _next(cur: dict) -> dict:
        """以 cur 为父版本构造下一版本的骨架（文件字典浅拷贝，ingest 深拷贝）。

        ingest 段（接入游标 + 批次索引）必须随每个新版本原样携带，
        这样追加 / 删除 / 压实 / 摄取任何一条提交路径都不会丢别人的进度。
        旧格式 manifest 没有该段，按空状态补齐。
        """
        return {
            "version": cur["version"] + 1,
            "parent": cur["version"],
            "created_at": time.time(),
            "data_files": dict(cur["data_files"]),
            "delete_files": dict(cur["delete_files"]),
            "ingest": copy.deepcopy(
                cur.get("ingest") or {"cursors": {}, "batches": {}}
            ),
        }

    # ---------------------------------------------------------------- 追加
    def append(self, rows) -> str:
        """追加一个数据文件，返回新的 data_file_id。rows: [(row_id, value), ...]"""
        rows = list(rows)
        fid = uuid.uuid4().hex
        table = pa.table(
            {
                "row_id": pa.array([r[0] for r in rows], pa.string()),
                "value": pa.array([r[1] for r in rows], pa.int64()),
            },
            schema=DATA_SCHEMA,
        )
        rel = f"data/data-{fid}.parquet"
        tmp = os.path.join(self.data_dir, f".tmp-{fid}")
        pq.write_table(table, tmp)
        with self.lock():
            # 先把数据文件就位，再发布引用它的 manifest，最后翻转 HEAD。
            os.replace(tmp, os.path.join(self.root, rel))
            storage.fsync_dir(self.data_dir)
            new = self._next(mf.load_current(self.meta_root))
            new["data_files"][fid] = {"path": rel, "num_rows": len(rows)}
            self._publish(new)
        return fid

    # ---------------------------------------------------------- 位置删除
    def publish_deletes(self, data_file_id: str, positions) -> int:
        """针对一个数据文件发布位置删除（0 起物理行号），返回新版本号。

        删除文件内每行显式记录 (data_file_id, pos)；一个删除文件只针对
        一个数据文件，便于压实时按文件整体吸收或翻译。
        """
        positions = sorted({int(p) for p in positions})
        if not positions:
            raise ValueError("positions must be non-empty")
        with self.lock():
            cur = mf.load_current(self.meta_root)
            meta = cur["data_files"].get(data_file_id)
            if meta is None:
                raise KeyError(f"unknown data file {data_file_id}")
            already = self._deleted_positions(cur, data_file_id)
            for p in positions:
                if not 0 <= p < meta["num_rows"]:
                    raise ValueError(
                        f"position {p} out of range [0, {meta['num_rows']})"
                    )
                if p in already:
                    raise ValueError(f"position {p} already deleted")
            did = uuid.uuid4().hex
            rel = f"data/deletes-{did}.parquet"
            deletes = pa.table(
                {
                    "data_file_id": pa.array(
                        [data_file_id] * len(positions), pa.string()
                    ),
                    "pos": pa.array(positions, pa.int64()),
                },
                schema=DELETE_SCHEMA,
            )
            tmp = os.path.join(self.data_dir, f".tmp-{did}")
            pq.write_table(deletes, tmp)
            os.replace(tmp, os.path.join(self.root, rel))
            storage.fsync_dir(self.data_dir)
            new = self._next(cur)
            new["delete_files"][did] = {
                "path": rel,
                "target": data_file_id,
                "num_deletes": len(positions),
            }
            self._publish(new)
            return new["version"]

    # ---------------------------------------------------------------- 扫描
    def scan(self, version: int | None = None) -> pa.Table:
        """固定一个 manifest 版本，按其中的位置删除过滤后返回活行。

        位置过滤用 PyArrow 完成：pyarrow.parquet 按文件物理顺序读出行，
        与删除项的 0 起物理行号严格对应（不依赖任何业务字段）。
        """
        m = self.manifest(version)
        deletes = self._all_deleted_positions(m)
        out = []
        for fid in sorted(m["data_files"]):
            meta = m["data_files"][fid]
            t = pq.read_table(os.path.join(self.root, meta["path"]))
            dead = deletes.get(fid)
            if dead:
                keep = [i for i in range(t.num_rows) if i not in dead]
                t = t.take(pa.array(keep, pa.int64()))
            out.append(t)
        if not out:
            return DATA_SCHEMA.empty_table()
        return pa.concat_tables(out)

    def row_ids(self, version: int | None = None) -> list[str]:
        return self.scan(version).column("row_id").to_pylist()

    # ---------------------------------------------------------------- 租约
    @contextmanager
    def lease(self, version: int | None = None):
        """读者租约：保护指定版本（默认当前版本）引用的全部文件不被 GC。

        租约的创建与 GC 都在同一把提交锁下执行，因此 GC 不会漏看
        一个刚刚创建好的租约；持租约期间可安全扫描旧版本。
        """
        with self.lock():
            v = self.current_version() if version is None else version
            if not os.path.exists(mf.version_path(self.meta_root, v)):
                raise LeaseError(f"version {v} no longer exists")
            lid = uuid.uuid4().hex
            lpath = os.path.join(self.leases_dir, f"lease-{lid}.json")
            storage.atomic_write_json(
                lpath,
                {
                    "lease_id": lid,
                    "version": v,
                    "pid": os.getpid(),
                    "created_at": time.time(),
                },
            )
        try:
            yield SimpleNamespace(lease_id=lid, version=v)
        finally:
            try:
                os.unlink(lpath)
            except FileNotFoundError:
                pass

    # ------------------------------------------------------------ 微批摄取
    def ingest_batch(self, batch_key: str, messages) -> dict:
        """可重放的微批摄取事务，详见 lake/ingest.py。"""
        from .ingest import ingest_batch

        return ingest_batch(self, batch_key, messages)

    def ingest_cursors(self, version: int | None = None) -> dict:
        """查询指定版本（默认当前）各来源分片的消费游标。"""
        from .ingest import ingest_cursors

        return ingest_cursors(self, version)

    def batch_record(self, batch_key: str) -> dict | None:
        """查询已提交批次的记录；未提交返回 None。"""
        from .ingest import batch_record

        return batch_record(self, batch_key)

    # ------------------------------------------------------- 成组抽样切分
    def create_split(
        self,
        train: int,
        validation: int,
        test: int,
        seed: int,
        version: int | None = None,
        batch_size: int | None = None,
    ) -> dict:
        """按 row_id 成组抽样产出 train/validation/test 三个 Parquet 子集。

        详见 lake/splits/split.py：同一 row_id 的全部可见行必然落在同一个
        子集；分配只依赖 seed 与 row_id，与 Parquet 文件顺序、压实无关。
        """
        from .splits.split import DEFAULT_BATCH_SIZE, create_split

        return create_split(
            self,
            version,
            train,
            validation,
            test,
            seed,
            batch_size=DEFAULT_BATCH_SIZE if batch_size is None else batch_size,
        )

    # ------------------------------------------------------------ 内部工具
    def _all_deleted_positions(self, manifest: dict) -> dict[str, set[int]]:
        """读取该 manifest 引用的全部删除文件，按 data_file_id 归组。"""
        out: dict[str, set[int]] = defaultdict(set)
        for dm in manifest["delete_files"].values():
            t = pq.read_table(os.path.join(self.root, dm["path"]))
            for fid, pos in zip(
                t.column("data_file_id").to_pylist(), t.column("pos").to_pylist()
            ):
                out[fid].add(pos)
        return out

    def _deleted_positions(self, manifest: dict, data_file_id: str) -> set[int]:
        return self._all_deleted_positions(manifest).get(data_file_id, set())
