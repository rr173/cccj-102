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
