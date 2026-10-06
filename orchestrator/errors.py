"""可区分的业务错误类型。

每类拒绝原因都有独立错误码，调用方可以据此区分处理，
服务端绝不以静默成功掩盖前置条件不满足等问题。
"""

# ---- 错误码常量 ----
ARTIFACT_NOT_FOUND = "ARTIFACT_NOT_FOUND"
VERSION_NOT_FOUND = "VERSION_NOT_FOUND"
ENV_NOT_FOUND = "ENV_NOT_FOUND"
INVALID_STATE = "INVALID_STATE"
INVALID_TRANSITION = "INVALID_TRANSITION"
PRECONDITION_FAILED = "PRECONDITION_FAILED"  # 上一环境未生效 / 构建未成功
REVISION_CONFLICT = "REVISION_CONFLICT"  # 乐观锁冲突
NOTHING_TO_ROLLBACK = "NOTHING_TO_ROLLBACK"
VERSION_NOT_ACTIVE = "VERSION_NOT_ACTIVE"
ALREADY_ACTIVE = "ALREADY_ACTIVE"
UPGRADE_BLOCKED = "UPGRADE_BLOCKED"  # 分批替换升级当前状态无法继续（链路断裂/基线不符）
DUPLICATE_REQUEST = "DUPLICATE_REQUEST"
DUPLICATE_REQUEST_MISMATCH = "DUPLICATE_REQUEST_MISMATCH"
INJECTED_FAILURE = "INJECTED_FAILURE"


class OrchestratorError(Exception):
    """所有可预期业务错误的基类。"""

    code: str = "ORCHESTRATOR_ERROR"

    def __init__(self, message: str, *, code: str | None = None):
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message}


class ArtifactNotFoundError(OrchestratorError):
    code = ARTIFACT_NOT_FOUND


class VersionNotFoundError(OrchestratorError):
    code = VERSION_NOT_FOUND


class EnvNotFoundError(OrchestratorError):
    code = ENV_NOT_FOUND


class InvalidStateError(OrchestratorError):
    code = INVALID_STATE


class InvalidTransitionError(OrchestratorError):
    code = INVALID_TRANSITION


class PreconditionFailedError(OrchestratorError):
    code = PRECONDITION_FAILED


class RevisionConflictError(OrchestratorError):
    code = REVISION_CONFLICT


class NothingToRollbackError(OrchestratorError):
    code = NOTHING_TO_ROLLBACK


class VersionNotActiveError(OrchestratorError):
    code = VERSION_NOT_ACTIVE


class AlreadyActiveError(OrchestratorError):
    code = ALREADY_ACTIVE


class UpgradeBlockedError(OrchestratorError):
    code = UPGRADE_BLOCKED


class DuplicateRequestError(OrchestratorError):
    def __init__(self, message: str, *, mismatch: bool = False):
        super().__init__(
            message,
            code=DUPLICATE_REQUEST_MISMATCH if mismatch else DUPLICATE_REQUEST,
        )
        self.mismatch = mismatch


class InjectedFailureError(OrchestratorError):
    code = INJECTED_FAILURE
