"""按 row_id 成组抽样的训练集切分（train / validation / test）。

对外入口：

    create_split(table, version, train, validation, test, seed)
        -> 交付清单 dict（reused 标明是否复用原切分）
    check_split(directory)
        -> 独立校验交付目录（manifest / 校验和 / 分组 / 不重不漏）
    export_subset(directory, subset, dest=None)
        -> 复制出一个可直接读取的 Parquet 文件

磁盘布局：<root>/splits/<split_id>/{train,validation,test}.parquet + manifest.json
"""

from .split import (
    ALGORITHM_VERSION,
    MANIFEST_NAME,
    SPLITS_DIRNAME,
    SUBSETS,
    TOTAL_WEIGHT,
    assign_subset,
    check_split,
    collect_groups,
    create_split,
    export_subset,
    group_hash,
    load_split,
    normalize_seed,
    normalize_weights,
    read_manifest,
    split_dir,
    split_identity,
    subset_path,
)

__all__ = [
    "ALGORITHM_VERSION",
    "MANIFEST_NAME",
    "SPLITS_DIRNAME",
    "SUBSETS",
    "TOTAL_WEIGHT",
    "assign_subset",
    "check_split",
    "collect_groups",
    "create_split",
    "export_subset",
    "group_hash",
    "load_split",
    "normalize_seed",
    "normalize_weights",
    "read_manifest",
    "split_dir",
    "split_identity",
    "subset_path",
]
