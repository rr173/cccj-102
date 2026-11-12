"""引擎异常类型。"""


class LakeError(Exception):
    """所有湖表引擎异常的基类。"""


class ConflictError(LakeError):
    """压实计划提交时检测到写冲突（如选中文件已被其他压实替换）。"""


class LeaseError(LakeError):
    """读者租约相关错误（如租约指向的版本已不存在）。"""
