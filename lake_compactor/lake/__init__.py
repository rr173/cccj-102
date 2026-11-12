"""小型湖表压实引擎：扫描 / 生成压实计划 / 提交计划 / 垃圾回收 / 微批摄取。"""

from .compact import commit_plan, create_plan, load_plan
from .errors import (
    BatchRejectedError,
    ConflictError,
    IdempotencyConflictError,
    LakeError,
    LeaseError,
    SplitConflictError,
    SplitError,
)
from .gc import collect
from .ingest import (
    batch_record,
    ingest_batch,
    ingest_cursors,
    messages_digest,
    normalize_messages,
)
from .splits import (
    ALGORITHM_VERSION,
    SUBSETS,
    TOTAL_WEIGHT,
    assign_subset,
    check_split,
    create_split,
    export_subset,
    group_hash,
    split_identity,
)
from .table import DATA_SCHEMA, DELETE_SCHEMA, LakeTable
from . import verify

__all__ = [
    "LakeTable",
    "DATA_SCHEMA",
    "DELETE_SCHEMA",
    "create_plan",
    "commit_plan",
    "load_plan",
    "collect",
    "verify",
    "ingest_batch",
    "ingest_cursors",
    "batch_record",
    "normalize_messages",
    "messages_digest",
    "create_split",
    "check_split",
    "export_subset",
    "assign_subset",
    "group_hash",
    "split_identity",
    "SUBSETS",
    "TOTAL_WEIGHT",
    "ALGORITHM_VERSION",
    "LakeError",
    "ConflictError",
    "LeaseError",
    "BatchRejectedError",
    "IdempotencyConflictError",
    "SplitError",
    "SplitConflictError",
]
