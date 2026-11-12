"""临时隐私视图：按已提交表状态与字段规则逐批产出处理后的 Arrow 批次。

调用方选择一个**已提交版本**与一组绑定稳定字段 id 的规则，迭代取得经过
隐藏 / 假名 / 时间截断处理的 ``pyarrow.RecordBatch``。视图只在当前进程
内存中存在：

  - **不写回湖表**：整个生命周期不创建/修改/删除任何湖表文件、版本、
    manifest，也不产生可在之后单独打开的导出目录（不落盘、不写 Parquet）；
  - **不保存转换结果**：每个输出批次只在迭代期间存活，消费者取下一批时
    上一批即可释放，转换按 batch 前进，任何时刻内存里只有一个批次；
  - **读者保护必释放**：建立视图时取该版本的读者租约（与 GC 共用提交
    锁），无论正常读完、提前 ``break``、下游抛错还是读取失败，租约都在
    finally 中解除，之后 GC 可正常回收。

所有“能不能读”的判定（缺 key、规则指向已不存在的字段、目标列类型不合
适、别名冲突、版本/文件缺失）都在**第一批结果交出之前**完成；读取途中
的 I/O 失败抛 PrivacyViewError(kind="read")，同样不泄出半成品批次并
释放保护。接口不运行 SQL、不接受 Python 表达式，只执行内置的三类规则。
"""

from __future__ import annotations

import os

import pyarrow as pa
import pyarrow.parquet as pq

from .. import schema_evo
from ..errors import LeaseError, PrivacyViewError
from .key import DEFAULT_ENV_VAR, resolve_key, rules_require_key
from .rules import normalize_rules
from .transform import (
    OutputColumn,
    output_arrow_schema,
    plan_output_columns,
    transform_batch,
)

DEFAULT_BATCH_SIZE = 4096


class PrivacyView:
    """一个已建立、持读者租约的隐私视图（上下文管理器 + 可迭代）。

    通常不直接构造，用模块级 :func:`open_privacy_view`。对象持有租约与
    key 字节；``close()`` 幂等且在 ``__exit__`` / 迭代结束 / 异常路径上
    都会执行。``schema`` 是输出 Arrow schema（只含允许的列名，无 metadata）。

    迭代与清理的实现选择：:meth:`__iter__` 返回一个**生成器**，租约的
    解除放在该生成器的 ``finally`` 里。这是唯一能同时确定性覆盖三种结束
    方式的原语——

      - 正常读完：生成器自然结束，运行 finally；
      - 消费者提前 ``break``：for 循环对生成器调用 ``close()``，向内抛
        ``GeneratorExit``，触发 finally；
      - 下游在循环体内抛错：异常在生成器挂起的 ``yield`` 处重新抛出，
        生成器栈展开时运行 finally（异常继续传回消费者）。

    读取途中的 I/O/Arrow 错误在生成器内包装成
    PrivacyViewError(kind="read")，同样经 finally 释放保护，不交出半成品。
    另提供 :meth:`close` 与上下文管理器作为显式/兜底释放路径。
    """

    def __init__(
        self,
        table,
        version: int,
        manifest: dict,
        read_schema: dict,
        columns: list[OutputColumn],
        key: bytes | None,
        batch_size: int,
        lease,
    ):
        self._table = table
        self.version = version
        self._manifest = manifest
        self._read_schema = read_schema
        self._columns = columns
        self._key = key
        self._batch_size = batch_size
        self._lease_cm = lease
        self._lease = None
        self._closed = False
        self._iterating = False
        self.schema = output_arrow_schema(columns)

    # ------------------------------------------------------------- 生命周期
    def __enter__(self) -> "PrivacyView":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        """释放读者租约（幂等）。关闭后再迭代只得到空序列。"""
        if self._closed:
            return
        self._closed = True
        if self._lease_cm is not None:
            # LeaseError 已在建立阶段处理；这里的退出只负责删除租约文件。
            self._lease_cm.__exit__(None, None, None)
            self._lease_cm = None
        self._lease = None
        self._iterating = False
        # 主动丢掉 key 引用，帮助内存尽早回收（不做任何持久化）。
        self._key = None

    def __del__(self):  # pragma: no cover - 兜底，正常路径靠生成器/close
        try:
            self.close()
        except Exception:
            pass

    @property
    def lease_id(self) -> str | None:
        return self._lease.lease_id if self._lease is not None else None

    @property
    def output_columns(self) -> list[dict]:
        """输出列描述（列名/类型/来源字段 id/动作），供诊断与测试。"""
        return [
            {
                "name": col.name,
                "type": str(col.arrow_type),
                "source_field_id": col.source_id,
                "kind": col.kind,
            }
            for col in self._columns
        ]

    # --------------------------------------------------------------- 迭代
    def __iter__(self):
        if self._closed:
            # 已关闭视图迭代为空序列（不重新打开、不重新取租约）。
            return iter(())
        if self._iterating:
            # 一个视图对应一条受保护的流式扫描，不支持并发重复迭代。
            raise PrivacyViewError(
                "privacy view is already being iterated; open a separate view "
                "for another scan"
            )
        self._iterating = True
        return self._generate()

    def _generate(self):
        try:
            for logical_batch in _iter_logical_batches(
                self._table, self._manifest, self._read_schema, self._batch_size
            ):
                yield transform_batch(
                    logical_batch,
                    self._read_schema,
                    self._columns,
                    # 无 pseudonymize 规则时 key 为 None；transform 不会用到
                    self._key if self._key is not None else b"",
                )
        except PrivacyViewError:
            raise
        except (OSError, pa.ArrowException) as e:
            raise PrivacyViewError(
                f"failed while reading the protected version: "
                f"{type(e).__name__}",
                kind="read",
            ) from e
        finally:
            # 正常读完 / break（GeneratorExit）/ 下游异常透传 / 读取失败，
            # 全部在此解除版本读取保护。close 幂等，重复调用安全。
            self._iterating = False
            self.close()

    def read_all(self) -> pa.Table:
        """把剩余批次消费成一张 Arrow 表（便捷入口；仍是逐批转换）。

        注意这会把结果物化到调用方内存；流式场景优先直接迭代视图。
        """
        batches = list(self)
        return pa.Table.from_batches(batches, schema=self.schema)


# --------------------------------------------------------------------------
# 视图建立：全部静态校验在第一批数据之前完成
# --------------------------------------------------------------------------
def open_privacy_view(
    table,
    rules,
    *,
    version: int | None = None,
    hmac_key=None,
    env_var: str = DEFAULT_ENV_VAR,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> PrivacyView:
    """在一个已提交版本上建立隐私视图。

    参数
    ----
    table:
        已打开的 :class:`~lake.table.LakeTable`（绑定分支只用于解析
        version=None 时的尖端；显式 version 是全库坐标）。
    rules:
        字段规则列表（见 :mod:`lake.privacy.rules`），可为空/None——
        空规则即原样透传，保持与普通扫描一致的行为。
    version:
        已提交版本号；None 取绑定分支当前尖端。
    hmac_key / env_var:
        HMAC key 的内存参数与环境变量名（见 :mod:`lake.privacy.key`）。
        key 只在内存中使用，不进配置/manifest/metadata/异常/日志。
    batch_size:
        源 Parquet 的读取批次大小，也是转换推进粒度。

    抛出
    ----
    PrivacyViewError:
        kind 为 key/rule/field/type/version，全部在交出第一批数据前抛出。
    """
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise PrivacyViewError(
            f"batch_size must be an integer, got {type(batch_size).__name__}",
            kind="rule",
        )
    if batch_size <= 0:
        raise PrivacyViewError("batch_size must be positive", kind="rule")

    # 1) 规则规范化（纯静态，不碰数据/文件）。
    norm_rules = normalize_rules(rules)

    # 2) 定位版本并在提交锁内建立读者保护。lease 上下文在同一临界区里
    #    读 manifest 并 require_files，因此“定位 -> 保护”没有窗口；
    #    版本不存在/文件已回收在这里以 version 错误明确拒绝。
    target_version = table.current_version() if version is None else version
    lease_cm = table.lease(target_version, require_files=True)
    try:
        lease = lease_cm.__enter__()
    except LeaseError as e:
        # 不泄露任何路径以外的敏感信息；LeaseError 文本只含版本/文件名。
        raise PrivacyViewError(
            f"the selected version cannot be opened for a privacy view: "
            f"{type(e).__name__}",
            kind="version",
        ) from e

    try:
        manifest = lease.manifest
        chain = schema_evo.schema_chain(manifest)
        read_schema = chain[-1]

        # 3) 输出列规划：字段存在性、目标列类型、别名冲突全部在此判定。
        columns = plan_output_columns(read_schema, norm_rules)

        # 4) key：仅当存在 pseudonymize 规则时才必需。
        key = resolve_key(
            hmac_key,
            env_var=env_var,
            require=rules_require_key(norm_rules),
        )

        view = PrivacyView(
            table,
            target_version,
            manifest,
            read_schema,
            columns,
            key,
            batch_size,
            lease_cm,
        )
        view._lease = lease
        return view
    except BaseException:
        # 建立阶段任何失败（规则/字段/类型/key）：不交出数据，立即解除
        # 已取得的保护，避免泄漏租约。
        lease_cm.__exit__(None, None, None)
        raise


def privacy_batches(
    table,
    rules,
    *,
    version: int | None = None,
    hmac_key=None,
    env_var: str = DEFAULT_ENV_VAR,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> "PrivacyView":
    """便捷入口：建立并返回一个可逐批迭代的隐私视图。

    适合 ``for batch in privacy_batches(...)`` 的直用形态；需要检查输出
    schema、显式 ``close()`` 或作为上下文管理器使用时，直接用
    :func:`open_privacy_view`。返回视图的 ``__iter__`` 是生成器，消费者在
    循环里抛错、提前 break 或正常读完都会在该生成器的 finally 中立即释放
    读者租约（不依赖等待垃圾回收）。
    """
    return open_privacy_view(
        table,
        rules,
        version=version,
        hmac_key=hmac_key,
        env_var=env_var,
        batch_size=batch_size,
    )


# --------------------------------------------------------------------------
# 逐批逻辑读取：物理批次 -> 字段 id 投影 -> 逻辑批次（与 scan 同映射）
# --------------------------------------------------------------------------
def _iter_logical_batches(table, manifest: dict, read_schema: dict,
                          batch_size: int):
    """对固定 manifest 的可见行逐批产出“逻辑结构 RecordBatch”。

    与 LakeTable.scan 的可见行语义一致（固定 manifest -> 归组位置删除 ->
    按物理行号过滤），但：

      - 逐文件、逐批流式产出，全表不进内存；
      - 每个物理批次经 schema_evo.projection_plan/project_batch 按字段 id
        投影到 read_schema 的当前列名/类型（改名、缺列补 null、无损扩宽
        与整表扫描完全同口径）；
      - 位置删除按文件内 0 起物理行号执行，绝不从业务字段反推。
    """
    chain = schema_evo.schema_chain(manifest)
    deletes = table._all_deleted_positions(manifest)

    for fid in sorted(manifest["data_files"]):
        meta = manifest["data_files"][fid]
        path = os.path.join(table.root, meta["path"])
        file_schema = schema_evo.file_schema_of(chain, meta)
        physical_names = set(pq.ParquetFile(path).schema_arrow.names)
        plan = schema_evo.projection_plan(
            physical_names, file_schema, read_schema
        )
        dead = deletes.get(fid)
        pf = pq.ParquetFile(path)
        row_offset = 0
        for physical_batch in pf.iter_batches(batch_size=batch_size):
            n = physical_batch.num_rows
            if dead:
                keep = [
                    i for i in range(n) if (row_offset + i) not in dead
                ]
                if len(keep) != n:
                    physical_batch = physical_batch.take(
                        pa.array(keep, pa.int64())
                    )
            row_offset += n
            if physical_batch.num_rows == 0:
                continue
            yield schema_evo.project_batch(
                physical_batch, plan, physical_batch.num_rows
            )
