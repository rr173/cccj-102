"""引擎异常类型。"""


class LakeError(Exception):
    """所有湖表引擎异常的基类。"""


class ConflictError(LakeError):
    """压实计划提交时检测到写冲突（如选中文件已被其他压实替换）。"""


class LeaseError(LakeError):
    """读者租约相关错误（如租约指向的版本已不存在）。"""


class BatchRejectedError(LakeError):
    """微批摄取被整批拒绝：缺号、批内重复编号或与既往区间交叠。

    整批拒绝是原子的：任何一个分片不合法，所有分片的游标都不推进、
    数据不入湖。reasons 携带每个违规分片的具体原因，供接入端修正后重发。
    """

    def __init__(self, reasons):
        self.reasons = [str(r) for r in reasons]
        super().__init__("batch rejected: " + "; ".join(self.reasons))


class IdempotencyConflictError(LakeError):
    """相同 batch_key 携带不同规范化内容重复请求（幂等冲突）。

    携带原批次的 receipt / version / 摘要，调用方可据此定位已生效的那一批。
    """

    def __init__(self, batch_key, receipt, version, expected_digest, got_digest):
        self.batch_key = batch_key
        self.receipt = receipt
        self.version = version
        self.expected_digest = expected_digest
        self.got_digest = got_digest
        super().__init__(
            f"batch_key {batch_key!r} already committed at version {version} "
            f"with different content (receipt {receipt!r})"
        )


class RuleSpecError(LakeError):
    """画像规则本身格式错误。

    与“数据触犯规则”不同：规则格式错误时不生成、不落盘任何报告。
    """


class ProfileNotFoundError(LakeError):
    """指定（修订号, 规则摘要）身份的质量画像不存在。"""


class SplitError(LakeError):
    """分组抽样切分无法产出（整次拒绝，不留下任何交付物）。

    典型原因：输入可见行存在空 row_id（无法用于对象分组）、权重非法
    （负数、非整数、总和不是 10000）、目标快照不存在等。
    """


class SplitConflictError(LakeError):
    """目标交付目录已存在但身份不同，不能替换旧数据。"""


class WorkbenchError(LakeError):
    """查询工作台拒绝请求的基类。

    ``kind`` 标明拒绝发生在哪一层，便于调用方按类别处理或展示：

      - ``mapping``       逻辑名称登记表本身不合法（名字重复、绑定形状错误）；
      - ``version``       逻辑名称绑定的表目录或精确版本号无法定位；
      - ``syntax``        查询文本不合法（空查询、多语句、非 SELECT）；
      - ``permission``    语句越过工作台的数据边界（写语句、文件读写、
                          联网、扩展/配置等）；
      - ``query``         查询本身引用了未登记的名称或列（DuckDB 绑定期错误）；
      - ``internal``      工作台自身状态异常（如并发清理后源文件消失）。
    """

    kind = "internal"

    def __init__(self, message: str, *, detail: str | None = None):
        self.detail = detail
        super().__init__(message)


class MappingError(WorkbenchError):
    """逻辑名称登记表不合法：重复名称、空名称、绑定形状错误。"""

    kind = "mapping"


class VersionError(WorkbenchError):
    """绑定指向的湖表目录或精确版本号无法定位。"""

    kind = "version"


class QuerySyntaxError(WorkbenchError):
    """查询文本本身不合法：空查询、多语句、非 SELECT/CALL 之外的语句。"""

    kind = "syntax"


class PermissionBoundaryError(WorkbenchError):
    """语句越过只读湖表数据边界：写语句、直接读文件、外部数据源、联网。"""

    kind = "permission"


class QueryReferenceError(WorkbenchError):
    """查询绑定失败：列名不存在、列名歧义、未知函数等。"""

    kind = "query"


class QueryExecutionError(WorkbenchError):
    """查询执行失败：类型转换、除零、内存不足等运行期错误。"""

    kind = "query"
