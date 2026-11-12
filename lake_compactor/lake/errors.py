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


class BranchError(LakeError):
    """分支操作失败的基类。"""


class BranchNameError(BranchError):
    """分支名不合法（空名、含非法字符、以 '-' 开头、过长等）。"""


class BranchExistsError(BranchError):
    """分支名已被占用（包括保留名 main）。"""


class BranchNotFoundError(BranchError):
    """指定名字的分支不存在（切换 / 撤销 / 定位尖端时）。"""


class ProtectedBranchError(BranchError):
    """试图撤销受保护的分支（main 不能撤销）。"""


class BranchBaseError(BranchError):
    """建分支时指定的基提交不存在。"""


class BranchMismatchError(BranchError):
    """在一条分支上提交另一条分支的压实计划（跨分支借用成果）。"""


class PublishError(LakeError):
    """publish（把实验尖端交给 main）被拒绝。

    ``reason`` 显式标明拒绝原因：``stale-main``（调用方预期的 main
    尖端已过期，期间有别的正式写者推进）、``diverged``（两条线已分叉，
    不是快进）、``source-is-main``（源本身就是 main）。拒绝路径只读，
    不改变任何一方的尖端、版本或数据文件。
    """

    def __init__(self, reason: str, message: str, **details):
        self.reason = reason
        self.details = details
        super().__init__(message)


class StaleMainError(PublishError):
    """调用方给出的 expected main 尖端与当前 main 不符。"""

    def __init__(self, expected, actual, source_version=None):
        self.expected_main_version = expected
        self.actual_main_version = actual
        super().__init__(
            "stale-main",
            f"expected main tip v{expected}, but main is already at v{actual}; "
            "another production writer advanced main after the check",
            expected_main_version=expected,
            actual_main_version=actual,
            source_version=source_version,
        )


class DivergedError(PublishError):
    """main 尖端不是实验尖端的祖先，不能快进；需人工处理，不自动合并。"""

    def __init__(self, main_version, source_version, main_parent, source_parent):
        self.main_version = main_version
        self.source_version = source_version
        super().__init__(
            "diverged",
            f"cannot fast-forward: main tip v{main_version} is not an ancestor "
            f"of experiment tip v{source_version}; the two lines have diverged "
            "(no automatic content merge is performed)",
            main_version=main_version,
            source_version=source_version,
            main_parent=main_parent,
            source_parent=source_parent,
        )


class SourceIsMainError(PublishError):
    """试图 publish 绑定到 main 的表（没有实验尖端可交付）。"""

    def __init__(self, version):
        super().__init__(
            "source-is-main",
            f"table is bound to main (tip v{version}); nothing to publish",
            source_version=version,
        )


class SplitError(LakeError):
    """分组抽样切分无法产出（整次拒绝，不留下任何交付物）。

    典型原因：输入可见行存在空 row_id（无法用于对象分组）、权重非法
    （负数、非整数、总和不是 10000）、目标快照不存在等。
    """


class SchemaError(LakeError):
    """字段结构（schema）相关错误的基类：结构定义、结构演进、按结构校验数据。"""


class SchemaEvolutionError(SchemaError):
    """结构变更被拒绝：违反演进规则，整次变更不生效。

    ``reasons`` 逐条列出不满足的规则（重复 ID、缩窄类型、删除仍被文件
    引用的列、没有默认值的必填列等），调用方可据此修正后重新提交。
    拒绝路径只读：结构、版本与数据文件都不变。
    """

    def __init__(self, reasons):
        self.reasons = [str(r) for r in reasons]
        super().__init__("schema evolution rejected: " + "; ".join(self.reasons))


class SchemaValidationError(SchemaError):
    """写入的数据不符合指定结构版本（未知列、缺必填列、null 违反约束、类型不符）。

    整批拒绝：任何一行不合法，整批不入湖。``reasons`` 逐条列出违规行与字段。
    """

    def __init__(self, reasons):
        self.reasons = [str(r) for r in reasons]
        super().__init__("rows rejected by schema validation: " + "; ".join(self.reasons))


class SchemaConflictError(ConflictError, SchemaError):
    """写者携带的结构版本在发布时已不是当前定义（结构已变更）。

    写者在开始处理时读到结构版本 S，发布时当前版本已变为 S'：写入明确
    失败，绝不按新定义静默解释已准备好的数据。调用方应重新读取当前
    结构、按新定义校验/补齐数据后重试。
    """

    def __init__(self, expected, actual):
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"schema changed during write: prepared under schema v{expected}, "
            f"but current schema is v{actual}; the write was not applied "
            f"(re-read the current schema and retry)"
        )


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


class PrivacyViewError(LakeError):
    """隐私视图无法建立或读取。

    所有错误都必须在**交出第一批结果之前**判定（读取途中的 I/O 失败
    除外，它在语义上就是“没有再交出新批次”）。错误文本刻意只引用字段
    id、别名与规则动作，**绝不**包含 HMAC key、原始客户编号或任何数据
    值——异常文本本身也是泄漏面。

    ``kind`` 标明拒绝发生在哪一层：

      - ``key``        缺少 HMAC key（环境变量与内存参数都没提供），
                       或 key 形状不合法；
      - ``rule``       规则形状不合法、动作未知、别名缺失/冲突、
                       截断粒度未知、同一字段 id 给出多条规则；
      - ``field``      规则绑定的字段 id 在所选提交的结构里不存在；
      - ``type``       目标列类型不支持该动作（假名只接受 string，
                       时间截断只接受 timestamp）；
      - ``version``    所选提交版本不存在或其文件已被回收；
      - ``read``       读取途中数据文件不可用（I/O / Arrow 错误）。
    """

    kind = "read"

    def __init__(self, message: str, *, kind: str | None = None):
        if kind is not None:
            self.kind = kind
        super().__init__(message)
