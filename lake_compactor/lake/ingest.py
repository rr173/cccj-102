"""可重放的微批摄取事务：源端消费进度与入湖记录同生共死。

训练数据接入端会因确认超时重发整批消息。本模块为湖表增加
`ingest_batch(batch_key, messages)`：一批消息可含多个来源分片
`(source, partition)`，每个分片内 offset 从 0 起连续编号；批内消息
允许乱序，规范化（按分片排序）后必须恰好覆盖该分片当前游标之后的
连续区间 `[cursor, cursor + n)`。同一批的所有分片是一个事务：

  - 任意分片缺号 / 批内重复编号 / 与既往区间交叠 → 整批拒绝
    （BatchRejectedError），任何分片的游标都不推进，数据不入湖；
  - 未出现在批内的分片维持原游标；
  - 成功后游标、请求摘要、receipt 与本批数据文件在同一个 manifest
    版本里原子发布（本线尖端翻转即唯一可见性边界），不存在
    “先写进度再补数据”的窗口，也不需要补偿修正。

分支隔离（实验分支）：

  - 游标与批次索引都随 manifest 携带，因此天然**按分支独立**：两条
    分支各自从自己的父提交继承进度，各自推进，互不可见。
  - receipt 由 `batch_key + 内容摘要 + 分支名` 决定，同一请求键与内容
    在两条分支上会得到不同 receipt；一条分支的回执绝不被另一条借用。
  - 批次记录文件名同样带分支名，同 key 的两条分支不会互相覆盖审计载体。
  - 只有当前绑定分支尖端的批次索引能证明“已提交”：在另一条分支上
    重放同一 key 不会命中本线之外的索引。

幂等与重放：

  - 相同 batch_key + 相同规范化内容（与输入顺序无关）→ 返回原
    receipt，不新增行、不推进版本；
  - 相同 batch_key + 不同内容 → IdempotencyConflictError；
  - 不同 batch_key 争抢同一区间 → 由游标规则裁决，最多一个成功。

崩溃恢复（配合测试钩子 LAKE_INGEST_CRASH_POINT，仅测试使用）：

  - prepared：临时数据文件已写、未进提交锁 → 游标未被吃掉，重发全量重做；
  - recorded：批次记录已写、manifest 未发布 → 记录是孤儿，不被任何
    manifest 引用即不可见，重发全量重做，GC 清理孤儿；
  - published：尖端已翻转、确认未返回 → 重启重放从 manifest 找到
    原 receipt 直接返回。
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from . import manifest as mf
from . import schema_evo
from . import storage
from .errors import (
    BatchRejectedError,
    IdempotencyConflictError,
    SchemaConflictError,
    SchemaError,
)
from .table import DATA_SCHEMA, LakeTable

# manifest["ingest"] 的结构：
#   {"cursors": {source: {partition: next_offset}},     # 各分片下一个期望 offset
#    "batches": {batch_key: {"record": rel_path, "receipt": ..., "version": ...,
#                            "digest": ..., "num_rows": ...,
#                            "cursors_after": {source: {partition: next_offset}}}}}
# batches 只增不减，随每个新版本原样携带，因此“批次是否生效”以本线
# manifest 为准，记录文件本身不构成提交证据。
EMPTY_INGEST = {"cursors": {}, "batches": {}}

INGEST_REL_DIR = os.path.join("metadata", "ingest", "batches")


# --------------------------------------------------------------------------
# 规范化与摘要
# --------------------------------------------------------------------------
def normalize_messages(messages) -> list[list]:
    """把输入消息规范化为排序后的 [source, partition, offset, row_id, value]
    （携带扩展字段时为 [..., fields]）。

    每条消息必须是含 source/partition/offset/row_id/value 五个字段的
    映射，可再带一个 ``fields`` 映射给出其余列的取值（键为字段名，
    不允许重复 row_id/value——那两个值固定走顶层键）。partition 统一
    规范为字符串（partition=1 与 "1" 视为同一分片）。排序键为
    (source, partition, offset)，fields 按键排序，因此同一批消息无论
    输入顺序如何都得到相同的规范化结果——这是“乱序重试视为相同内容”
    的基础。输入形状不合法抛 ValueError（客户端错误，与游标状态无关）。
    """
    msgs = list(messages)
    if not msgs:
        raise ValueError("messages must be non-empty")
    out = []
    for i, m in enumerate(msgs):
        try:
            source, partition = m["source"], m["partition"]
            offset, row_id, value = m["offset"], m["row_id"], m["value"]
        except (TypeError, KeyError) as e:
            raise ValueError(f"message {i}: missing or invalid field: {e}") from e
        if not isinstance(source, str) or not source:
            raise ValueError(f"message {i}: source must be a non-empty str")
        if isinstance(partition, bool) or not isinstance(partition, (str, int)):
            raise ValueError(f"message {i}: partition must be str or int")
        partition = str(partition)
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError(f"message {i}: offset must be an int >= 0")
        if not isinstance(row_id, str):
            raise ValueError(f"message {i}: row_id must be a str")
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"message {i}: value must be an int (int64)")
        rec = [source, partition, offset, row_id, value]
        fields = m.get("fields")
        if fields is not None:
            if not isinstance(fields, dict) or not fields:
                raise ValueError(f"message {i}: fields must be a non-empty dict")
            for k, v in fields.items():
                if not isinstance(k, str) or not k:
                    raise ValueError(f"message {i}: fields keys must be non-empty str")
                if k in ("row_id", "value"):
                    raise ValueError(
                        f"message {i}: fields must not repeat {k!r} "
                        f"(use the top-level message key)"
                    )
                if v is not None and not isinstance(v, (bool, int, float, str)):
                    raise ValueError(
                        f"message {i}: fields[{k!r}] must be a scalar "
                        f"(bool/int/float/str/None)"
                    )
            rec.append([list(p) for p in sorted(fields.items())])
        out.append(rec)
    out.sort(key=lambda r: (r[0], r[1], r[2]))
    return out


def messages_digest(normalized: list[list]) -> str:
    """规范化消息的确定性摘要（请求摘要，随批次一起入 manifest）。"""
    blob = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def make_receipt(batch_key: str, digest: str, branch: str = mf.MAIN_BRANCH) -> str:
    """稳定 receipt：由 batch_key、内容摘要与分支名确定性导出。

    分支名进入哈希，因此同一请求键与内容在两条分支上的回执不同——
    回执只在本线有效，不会被另一条线重放借用。main 分支的表达式与
    旧实现逐字节兼容（branch 不参与主分支的哈希），旧表回执不变。
    """
    material = f"{batch_key}\x00{digest}"
    if branch != mf.MAIN_BRANCH:
        material += f"\x00{branch}"
    h = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return f"rcpt-{h[:32]}"


def _record_rel_path(table: LakeTable, batch_key: str, branch: str) -> str:
    key_hash = hashlib.sha256(batch_key.encode("utf-8")).hexdigest()[:32]
    if branch == mf.MAIN_BRANCH:
        name = f"batch-{key_hash}.json"
    else:
        branch_hash = hashlib.sha256(branch.encode("utf-8")).hexdigest()[:16]
        name = f"batch-{key_hash}-{branch_hash}.json"
    return os.path.join(INGEST_REL_DIR, name)


# --------------------------------------------------------------------------
# 状态读取（进度查询契约）
# --------------------------------------------------------------------------
def ingest_state(manifest_obj: dict) -> dict:
    """取 manifest 的 ingest 段；旧格式 manifest 没有该段，按空状态处理。"""
    return manifest_obj.get("ingest") or copy.deepcopy(EMPTY_INGEST)


def ingest_cursors(table: LakeTable, version: int | None = None) -> dict:
    """查询绑定分支（或显式提交）各分片的消费游标（下一个期望 offset）。"""
    return copy.deepcopy(ingest_state(table.manifest(version))["cursors"])


def batch_record(table: LakeTable, batch_key: str) -> dict | None:
    """查询绑定分支尖端已提交批次的完整记录；未提交返回 None。

    是否生效以**本线**尖端 manifest 的批次索引为准；另一条分支上的
    同名请求键不属于本线，记录文件只是审计载体。
    """
    committed = ingest_state(table.manifest())["batches"].get(batch_key)
    if committed is None:
        return None
    try:
        return storage.read_json(os.path.join(table.root, committed["record"]))
    except FileNotFoundError:
        return dict(committed)  # 记录文件缺失时退回 manifest 内的摘要信息


# --------------------------------------------------------------------------
# 游标校验
# --------------------------------------------------------------------------
def _validate_against_cursors(cursors: dict, normalized: list[list]) -> list[str]:
    """只读校验：整批所有分片都必须恰好接续当前游标，返回违规原因列表。"""
    by_shard: dict[tuple[str, str], list[int]] = {}
    for rec in normalized:
        source, part, offset = rec[0], rec[1], rec[2]
        by_shard.setdefault((source, part), []).append(offset)

    reasons = []
    for (source, part), offsets in sorted(by_shard.items()):
        shard = f"{source}/{part}"
        cursor = cursors.get(source, {}).get(part, 0)
        uniq = sorted(set(offsets))
        dups = sorted({o for o in uniq if offsets.count(o) > 1})
        if dups:
            reasons.append(f"shard {shard}: duplicate offsets in batch: {dups}")
        lo, hi = uniq[0], uniq[-1]
        if lo < cursor:
            reasons.append(
                f"shard {shard}: offsets [{lo}, ...] overlap already-ingested "
                f"range (cursor at {cursor})"
            )
            continue
        if lo > cursor:
            reasons.append(
                f"shard {shard}: gap before batch: expected start {cursor}, got {lo}"
            )
            continue
        missing = [o for o in range(lo, hi + 1) if o not in set(uniq)]
        if missing:
            reasons.append(f"shard {shard}: gap within batch, missing {missing}")
    return reasons


def _shards_of(normalized: list[list]) -> dict[tuple[str, str], int]:
    """每个分片在本批中的消息条数（已按 (source, partition) 排序）。"""
    counts: dict[tuple[str, str], int] = {}
    for rec in normalized:
        source, part = rec[0], rec[1]
        counts[(source, part)] = counts.get((source, part), 0) + 1
    return counts


# --------------------------------------------------------------------------
# 测试钩子：在关键位置硬退出（仅 LAKE_INGEST_CRASH_POINT 命中时生效）
# --------------------------------------------------------------------------
def _maybe_crash(point: str) -> None:
    if os.environ.get("LAKE_INGEST_CRASH_POINT") == point:
        os._exit(42)


# --------------------------------------------------------------------------
# 摄取事务
# --------------------------------------------------------------------------
def ingest_batch(table: LakeTable, batch_key: str, messages) -> dict:
    """把一个微批作为一个事务摄取入湖，返回结果字典：

        {"receipt": 稳定回执, "batch_key": ..., "version": 批次生效的湖版本,
         "cursors": {source: {partition: 新游标}},   # 仅本批涉及的分片
         "num_rows": 行数, "replayed": 是否幂等重放}

    失败契约：
      - ValueError                请求形状不合法（与表状态无关）；
      - BatchRejectedError        缺号 / 批内重复 / 与既往区间交叠，整批拒绝；
      - IdempotencyConflictError  相同 batch_key 携带不同内容；
      - SchemaValidationError     消息（含 fields 扩展列）不符合当前结构；
      - SchemaConflictError       准备期间结构已变更，整批未入湖，重读后重发；
      - SchemaError               当前结构与摄取契约不兼容（id1/id2 列缺失
                                  或类型不符、存在无法填充的必填列）。
    """
    if not isinstance(batch_key, str) or not batch_key:
        raise ValueError("batch_key must be a non-empty str")
    normalized = normalize_messages(messages)
    digest = messages_digest(normalized)

    # 快路径：本线已提交的批次直接重放。manifest 不可变且批次索引只增
    # 不减，读到的任何本线版本都足以确证“已提交”；读不到则走完整路径
    # 在锁内复核。另一条分支的同名 key 不在本线索引里，自然不会被命中。
    m0 = table.manifest()
    committed = ingest_state(m0)["batches"].get(batch_key)
    if committed is not None:
        return _replay_result(table.branch, batch_key, committed, digest)

    # 结构解析：旧式表（无结构元数据）走与旧实现逐字节一致的物理路径；
    # 结构受管表上把消息的 row_id/value 按字段 id 1/2 映射进当前结构，
    # fields 扩展键按名字落列，整批按当前结构校验（与追加、扫描同一套
    # 字段映射规则）。准备时看到的结构版本会在锁内发布前重新确认。
    writer_schema = None
    normalized_rows = None
    if "schemas" in m0:
        writer_schema = schema_evo.current_schema(m0)
        mapped = schema_evo.legacy_write_mapping(writer_schema)
        if mapped is None:
            raise SchemaError(
                "ingest_batch is not compatible with the current schema "
                "(field id 1 must be a string column, id 2 must accept "
                "integer values, and every other column must be nullable "
                "or have a default); evolve the schema back or use "
                "append_rows"
            )
        name1, name2 = mapped
        normalized_rows = []
        for rec in normalized:
            row = {name1: rec[3], name2: rec[4]}
            if len(rec) == 6:
                row.update(dict(rec[5]))
            normalized_rows.append(row)
        normalized_rows = schema_evo.validate_rows(writer_schema, normalized_rows)

    # 准备阶段：数据文件先写临时名（不在读路径上），崩溃只留下 GC 可回收的
    # .tmp 残骸，游标、receipt、版本都未被动过——重发等于全量重做。
    fid = uuid.uuid4().hex
    tmp = os.path.join(table.data_dir, f".tmp-{fid}")
    if writer_schema is None:
        data = pa.table(
            {
                "row_id": pa.array([r[3] for r in normalized], pa.string()),
                "value": pa.array([r[4] for r in normalized], pa.int64()),
            },
            schema=DATA_SCHEMA,
        )
    else:
        data = schema_evo.build_table(writer_schema, normalized_rows)
    pq.write_table(data, tmp)
    _maybe_crash("prepared")

    try:
        with table.lock():
            # 锁内以**本线尖端**为父复核：另一条线推进不构成本线冲突。
            cur = table._current_manifest()
            state = ingest_state(cur)
            committed = state["batches"].get(batch_key)
            if committed is not None:  # 锁内权威复核：本线并发提交者已抢先
                return _replay_result(table.branch, batch_key, committed, digest)

            # 结构版本复核：准备期间结构已变更的，整批明确失败（数据文件
            # 是按旧定义物化的，绝不按新定义静默解释），调用方重读后重发。
            if writer_schema is not None:
                cur_schema_version = schema_evo.current_schema(cur)["version"]
                if cur_schema_version != writer_schema["version"]:
                    raise SchemaConflictError(
                        writer_schema["version"], cur_schema_version
                    )

            reasons = _validate_against_cursors(state["cursors"], normalized)
            if reasons:
                raise BatchRejectedError(reasons)

            # 1) 数据文件就位（仍在锁内，与 manifest 发布同一临界区）。
            rel = f"data/data-{fid}.parquet"
            os.replace(tmp, os.path.join(table.root, rel))
            tmp = None
            storage.fsync_dir(table.data_dir)

            # 2) 游标推进（仅本批涉及的分片；未出现的分片维持原游标）。
            cursors_after: dict[str, dict[str, int]] = {}
            for (source, part), n in sorted(_shards_of(normalized).items()):
                new_cursor = state["cursors"].get(source, {}).get(part, 0) + n
                cursors_after.setdefault(source, {})[part] = new_cursor

            # 3) 批次记录（请求摘要 + receipt + 规范化消息）先于 manifest 就位。
            receipt = make_receipt(batch_key, digest, table.branch)
            new = table._next(cur)
            version = new["version"]
            record_rel = _record_rel_path(table, batch_key, table.branch)
            record = {
                "batch_key": batch_key,
                "branch": table.branch,
                "digest": digest,
                "receipt": receipt,
                "version": version,
                "num_rows": len(normalized),
                "messages": normalized,
                "cursors_before": {
                    s: dict(p) for s, p in state["cursors"].items()
                },
                "cursors_after": cursors_after,
                "data_file_id": fid,
                "created_at": time.time(),
            }
            os.makedirs(os.path.join(table.root, INGEST_REL_DIR), exist_ok=True)
            storage.atomic_write_json(os.path.join(table.root, record_rel), record)
            _maybe_crash("recorded")

            # 4) 游标 + 请求摘要 + receipt + 数据文件引用随同一 manifest 发布；
            #    本线尖端翻转即唯一可见性边界，对本线既有成果原样携带。
            new["data_files"][fid] = {"path": rel, "num_rows": len(normalized)}
            if writer_schema is not None:
                new["data_files"][fid]["schema_version"] = writer_schema["version"]
            ing = new["ingest"]
            for source, parts in cursors_after.items():
                ing["cursors"].setdefault(source, {}).update(parts)
            ing["batches"][batch_key] = {
                "record": record_rel,
                "receipt": receipt,
                "version": version,
                "digest": digest,
                "num_rows": len(normalized),
                "cursors_after": cursors_after,
            }
            table._publish(new)
            result = {
                "receipt": receipt,
                "batch_key": batch_key,
                "branch": table.branch,
                "version": version,
                "cursors": cursors_after,
                "num_rows": len(normalized),
                "replayed": False,
            }
    finally:
        if tmp is not None:  # 拒绝 / 重放 / 异常路径：清理未就位的临时文件
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass

    _maybe_crash("published")  # 已生效、确认未返回：重放须找到原 receipt
    return result


def _replay_result(branch: str, batch_key: str, committed: dict, digest: str) -> dict:
    """已提交批次的重放：内容一致返回原 receipt，不一致报幂等冲突。"""
    if committed["digest"] != digest:
        raise IdempotencyConflictError(
            batch_key=batch_key,
            receipt=committed["receipt"],
            version=committed["version"],
            expected_digest=committed["digest"],
            got_digest=digest,
        )
    return {
        "receipt": committed["receipt"],
        "batch_key": batch_key,
        "branch": branch,
        "version": committed["version"],
        "cursors": copy.deepcopy(committed["cursors_after"]),
        "num_rows": committed["num_rows"],
        "replayed": True,
    }
