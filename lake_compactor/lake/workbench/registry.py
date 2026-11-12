"""逻辑名称注册表：把名称绑定到（本机湖表目录, 精确版本号）。

工作台的输入边界只有这一种来源：每个逻辑名称绑定一个目录和一个**精确**
版本号。查询里的表名只能是登记过的逻辑名称，底层 data/ 文件路径既不
会出现在 SQL 中，也不会被交给 DuckDB（DuckDB 只看到内存中的 Arrow 表）。

注册阶段只做形状校验，不碰任何湖表文件；版本定位在每次查询开始时统一
完成（见 resolve_sources），因此“一次请求的所有输入版本在执行前确定”。
"""

from __future__ import annotations

import os

from .. import manifest as mf
from ..errors import MappingError, VersionError
from ..table import LakeTable

_BINDING_KEYS_DIR = ("lake", "lake_dir", "dir", "directory", "path", "root")


def _normalize_name(name) -> str:
    if not isinstance(name, str):
        raise MappingError(f"逻辑名称必须是字符串，实际为 {type(name).__name__}")
    name = name.strip()
    if not name:
        raise MappingError("逻辑名称不能为空")
    if "\x00" in name:
        raise MappingError("逻辑名称不能包含 NUL 字符")
    if "\n" in name or "\r" in name:
        raise MappingError("逻辑名称不能包含换行")
    return name


class LogicalBinding:
    """一个逻辑名称 -> (湖表目录, 精确版本号) 的绑定。"""

    __slots__ = ("name", "lake_dir", "version")

    def __init__(self, name: str, lake_dir: str, version: int):
        self.name = _normalize_name(name)
        if not isinstance(lake_dir, str) or not lake_dir.strip():
            raise MappingError(
                f"逻辑名称 {self.name!r} 的湖表目录必须是非空路径字符串"
            )
        self.lake_dir = os.path.abspath(lake_dir)
        if isinstance(version, bool) or not isinstance(version, int):
            raise VersionError(
                f"逻辑名称 {self.name!r} 的版本号必须是整数，实际为 {version!r}"
            )
        if version < 0:
            raise VersionError(
                f"逻辑名称 {self.name!r} 的版本号不能为负: {version}"
            )
        self.version = int(version)

    def __repr__(self) -> str:  # pragma: no cover - 调试辅助
        return (
            f"LogicalBinding(name={self.name!r}, lake_dir={self.lake_dir!r}, "
            f"version={self.version})"
        )


def _binding_from_mapping(name, spec) -> LogicalBinding:
    if isinstance(spec, LogicalBinding):
        if spec.name != name:
            raise MappingError(
                f"逻辑名称 {name!r} 与绑定对象里的名称 {spec.name!r} 不一致"
            )
        return spec
    if isinstance(spec, (tuple, list)):
        if len(spec) != 2:
            raise MappingError(
                f"逻辑名称 {name!r} 的绑定必须是 (目录, 版本号) 二元组"
            )
        return LogicalBinding(name, spec[0], spec[1])
    if isinstance(spec, dict):
        lake = None
        for key in _BINDING_KEYS_DIR:
            if key in spec:
                lake = spec[key]
                break
        if lake is None:
            raise MappingError(
                f"逻辑名称 {name!r} 的绑定缺少湖表目录（可用键: "
                f"{', '.join(_BINDING_KEYS_DIR)}）"
            )
        if "version" not in spec:
            raise MappingError(f"逻辑名称 {name!r} 的绑定缺少 version（精确版本号）")
        return LogicalBinding(name, lake, spec["version"])
    raise MappingError(
        f"逻辑名称 {name!r} 的绑定形状不受支持: {type(spec).__name__}；"
        "应为 (目录, 版本号) 或含 lake/version 的映射"
    )


def normalize_bindings(bindings, *, check_exists: bool = True) -> list[LogicalBinding]:
    """把调用方给出的登记表规范化为一组 LogicalBinding。

    接受三种形状：

      - {name: (lake_dir, version)}
      - {name: {"lake": lake_dir, "version": v}}
      - [{"name": ..., "lake": ..., "version": ...}, ...]

    名称重复（按大小写不敏感比较，与 DuckDB 的未引号标识符折叠一致）
    直接拒绝：两次登记同一个名字只会让查询语义含糊。

    ``check_exists=False`` 只校验形状（CLI 的 ``spec`` 预检用），不要求
    目录此刻存在；真正执行查询时版本定位仍会逐项校验。
    """
    if bindings is None:
        raise MappingError("至少需要登记一个逻辑名称")
    out: list[LogicalBinding] = []
    if isinstance(bindings, dict):
        if not bindings:
            raise MappingError("至少需要登记一个逻辑名称")
        for name, spec in bindings.items():
            out.append(_binding_from_mapping(name, spec))
    elif isinstance(bindings, (list, tuple)):
        for i, item in enumerate(bindings):
            if isinstance(item, LogicalBinding):
                out.append(item)
                continue
            if not isinstance(item, dict) or "name" not in item:
                raise MappingError(
                    f"第 {i} 项绑定必须是含 name/lake/version 的映射"
                )
            out.append(_binding_from_mapping(item["name"], item))
    elif isinstance(bindings, LogicalBinding):
        out.append(bindings)
    else:
        raise MappingError(
            "登记表形状不受支持；应为 {名称: (目录, 版本号)}、"
            "{名称: {lake, version}} 或绑定列表"
        )

    seen: dict[str, str] = {}
    for b in out:
        key = b.name.casefold()
        if key in seen:
            raise MappingError(
                f"逻辑名称 {b.name!r} 重复登记（与 {seen[key]!r} 大小写不敏感冲突）"
            )
        seen[key] = b.name
        if check_exists and not os.path.isdir(b.lake_dir):
            raise VersionError(
                f"版本定位错误: 逻辑名称 {b.name!r} 的湖表目录不存在: "
                f"{b.lake_dir}"
            )
    return out


class ResolvedSource:
    """一个已定位到精确版本的来源（尚未读取任何数据行）。"""

    __slots__ = ("name", "lake_dir", "version", "table", "manifest")

    def __init__(self, binding: LogicalBinding, table: LakeTable, manifest: dict):
        self.name = binding.name
        self.lake_dir = binding.lake_dir
        self.version = binding.version
        self.table = table
        self.manifest = manifest

    def describe(self) -> dict:
        return {
            "name": self.name,
            "lake": self.lake_dir,
            "version": self.version,
            "manifest_data_files": len(self.manifest.get("data_files") or {}),
            "manifest_delete_files": len(self.manifest.get("delete_files") or {}),
        }


def resolve_source(binding: LogicalBinding) -> ResolvedSource:
    """在读取任何数据之前定位表目录与精确版本号。

    只读取 metadata/HEAD 与 versions/vNNNNNN.json（必要时读 HEAD 只是为了
    确认这是一个湖表）；不读 data/、不改写任何文件。版本不存在、目录不是
    湖表、manifest 损坏都归入版本定位错误。
    """
    try:
        table = LakeTable.open(binding.lake_dir)
    except FileNotFoundError as e:
        raise VersionError(
            f"版本定位错误: 逻辑名称 {binding.name!r} 指向的目录不是湖表"
            f"（缺少 metadata/HEAD）: {binding.lake_dir}"
        ) from e
    except OSError as e:
        raise VersionError(
            f"版本定位错误: 逻辑名称 {binding.name!r} 的湖表目录无法打开: "
            f"{binding.lake_dir}: {e}"
        ) from e

    vpath = mf.version_path(table.meta_root, binding.version)
    if not os.path.exists(vpath):
        raise VersionError(
            f"版本定位错误: 逻辑名称 {binding.name!r} 绑定的版本 "
            f"v{binding.version} 不存在于 {binding.lake_dir}"
        )
    try:
        manifest = mf.load_manifest(table.meta_root, binding.version)
    except (OSError, ValueError) as e:
        raise VersionError(
            f"版本定位错误: 逻辑名称 {binding.name!r} 的版本 v{binding.version} "
            f"manifest 无法读取: {e}"
        ) from e
    if manifest.get("version") != binding.version:
        raise VersionError(
            f"版本定位错误: 逻辑名称 {binding.name!r} 的 manifest 版本号 "
            f"{manifest.get('version')!r} 与登记的 v{binding.version} 不一致"
        )
    return ResolvedSource(binding, table, manifest)


def resolve_sources(bindings) -> list[ResolvedSource]:
    """一次请求的全部来源在数据读取前统一定位；任何一个失败即整次拒绝。"""
    return [resolve_source(b) for b in bindings]
