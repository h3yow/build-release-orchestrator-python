"""状态模型与错误码定义。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class EnvName(str, enum.Enum):
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


# 环境晋级顺序：dev -> staging -> prod
ENV_ORDER: list[EnvName] = [EnvName.DEV, EnvName.STAGING, EnvName.PROD]


class EnvState(str, enum.Enum):
    """某产物版本在某环境上的可见状态。"""

    ABSENT = "absent"            # 从未生效过
    ACTIVE = "active"            # 当前已生效
    ROLLED_BACK = "rolled_back"  # 曾生效、已撤回


class OpType(str, enum.Enum):
    PROMOTE = "promote"
    ROLLBACK = "rollback"


class OpStatus(str, enum.Enum):
    COMMITTED = "committed"
    REJECTED = "rejected"
    FAILED = "failed"


class ErrorCode(str, enum.Enum):
    ARTIFACT_NOT_FOUND = "artifact_not_found"            # 版本不存在
    BUILD_NOT_SUCCEEDED = "build_not_succeeded"          # 构建未成功
    ILLEGAL_TRANSITION = "illegal_transition"            # 非法状态迁移（跨级等）
    PRECONDITION_NOT_MET = "precondition_not_met"        # 前置环境未生效
    PRECONDITION_CONFLICT = "precondition_conflict"      # 预期前置条件与现状冲突（可安全重试）
    ENV_NOT_ACTIVE = "env_not_active"                    # 回滚目标环境当前未生效


@dataclass
class Artifact:
    """产物版本记录。"""

    version: str
    build_status: str  # "succeeded" | "failed" | "building"


@dataclass
class EnvStatus:
    """某环境当前生效的版本。"""

    env: EnvName
    active_version: str | None = None


@dataclass
class OpRecord:
    """操作流水记录，追加写入，用于回滚与续做的可观测。"""

    op_id: str
    op_type: OpType
    version: str
    env: EnvName
    status: OpStatus
    expected: str | None       # 写入时声明的预期前置状态（该环境操作前生效版本）
    result: str | None         # 操作完成后该环境生效版本
    reason: str | None = None  # 拒绝 / 失败原因（ErrorCode 或描述）
    request_id: str | None = None  # 幂等键：同一请求重复提交返回同一结果
    ts: float = 0.0


@dataclass
class OpResult:
    """编排操作的返回。"""

    ok: bool
    op: OpRecord
    error: ErrorCode | None = None
    detail: str = ""
