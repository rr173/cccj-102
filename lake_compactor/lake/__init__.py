"""小型湖表压实引擎：扫描 / 生成压实计划 / 提交计划 / 垃圾回收 / 微批摄取。"""

from .compact import commit_plan, create_plan, load_plan
from .errors import (
    BatchRejectedError,
    ConflictError,
    IdempotencyConflictError,
    LakeError,
    LeaseError,
)
from .gc import collect
from .ingest import (
    batch_record,
    ingest_batch,
    ingest_cursors,
    messages_digest,
    normalize_messages,
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
    "LakeError",
    "ConflictError",
    "LeaseError",
    "BatchRejectedError",
    "IdempotencyConflictError",
]
