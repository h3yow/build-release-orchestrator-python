"""领域模型与 API 模型的类型定义。

状态模型概览
============
* 产物版本(Version): 归属于某个产物(artifact_id)，带构建状态
  (PENDING / SUCCESS / FAILED)。只有 SUCCESS 的版本可参与晋级。
* 环境(EnvState): 流水线固定为 dev -> staging -> prod。每个环境在任意
  时刻至多有一个生效版本(active_version)，并带一个单调递增的 revision，
  用于乐观并发控制。
* 操作流水(Operation): 每次晋级/回滚都落一条不可变语义的流水记录，
  包含请求方提供的预期前置条件、结果与失败原因。
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field

# 默认晋级流水线：索引即层级，只能从低层级向高层级晋级
DEFAULT_PIPELINE = ("dev", "staging", "prod")
FIRST_ENV = DEFAULT_PIPELINE[0]


class BuildStatus(enum.StrEnum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class DeploymentStatus(enum.StrEnum):
    """某次环境部署在历史中的状态。"""

    ACTIVE = "ACTIVE"          # 当前生效
    SUPERSEDED = "SUPERSEDED"  # 被新版本替换
    ROLLED_BACK = "ROLLED_BACK"  # 被回滚撤下


class OperationType(enum.StrEnum):
    REGISTER = "REGISTER"
    PROMOTE = "PROMOTE"
    ROLLBACK = "ROLLBACK"
    UPGRADE = "UPGRADE"  # 分批替换升级：旧版本已全量，逐环境换成新版本


class UpgradePhase(enum.StrEnum):
    """某版本分批替换升级的整体阶段。"""

    NOT_STARTED = "NOT_STARTED"    # 还没有任何环境换到该版本
    IN_PROGRESS = "IN_PROGRESS"    # 低环境已换新、高环境仍是旧版本（混合窗口）
    COMPLETED = "COMPLETED"        # 全部环境都已换到该版本


class OperationStatus(enum.StrEnum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


# ---------------- 领域模型（磁盘持久化） ----------------


@dataclass
class Version:
    artifact_id: str
    version: str
    build_status: BuildStatus
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "version": self.version,
            "build_status": self.build_status.value,
            "created_at": self.created_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Version:
        return cls(
            artifact_id=d["artifact_id"],
            version=d["version"],
            build_status=BuildStatus(d["build_status"]),
            created_at=d["created_at"],
            metadata=d.get("metadata", {}),
        )


@dataclass
class EnvHistoryEntry:
    """环境历史中的一次动作（晋级生效/回滚）。"""

    version: str | None
    action: str  # OperationType 值
    status: DeploymentStatus
    revision: int
    request_id: str
    timestamp: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "action": self.action,
            "status": self.status.value,
            "revision": self.revision,
            "request_id": self.request_id,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> EnvHistoryEntry:
        return cls(
            version=d.get("version"),
            action=d["action"],
            status=DeploymentStatus(d["status"]),
            revision=d["revision"],
            request_id=d["request_id"],
            timestamp=d["timestamp"],
        )


@dataclass
class EnvState:
    env: str
    active_version: str | None = None
    revision: int = 0
    history: list[EnvHistoryEntry] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "env": self.env,
            "active_version": self.active_version,
            "revision": self.revision,
            "history": [h.to_dict() for h in self.history],
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> EnvState:
        return cls(
            env=d["env"],
            active_version=d.get("active_version"),
            revision=d.get("revision", 0),
            history=[EnvHistoryEntry.from_dict(h) for h in d.get("history", [])],
        )


@dataclass
class OperationStep:
    """级联晋级中单个环境的一步。"""

    env: str
    status: OperationStatus
    from_version: str | None
    to_version: str | None
    revision_before: int
    revision_after: int | None
    error_code: str | None = None
    error_message: str | None = None
    rolled_back: bool = False  # 中途失败后该步是否已被补偿回落

    def to_dict(self) -> dict[str, Any]:
        return {
            "env": self.env,
            "status": self.status.value,
            "from_version": self.from_version,
            "to_version": self.to_version,
            "revision_before": self.revision_before,
            "revision_after": self.revision_after,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "rolled_back": self.rolled_back,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> OperationStep:
        return cls(
            env=d["env"],
            status=OperationStatus(d["status"]),
            from_version=d.get("from_version"),
            to_version=d.get("to_version"),
            revision_before=d["revision_before"],
            revision_after=d.get("revision_after"),
            error_code=d.get("error_code"),
            error_message=d.get("error_message"),
            rolled_back=d.get("rolled_back", False),
        )


@dataclass
class Operation:
    request_id: str
    op_type: OperationType
    artifact_id: str
    env: str | None
    version: str | None
    expected_revision: int | None
    status: OperationStatus
    created_at: str
    updated_at: str
    from_version: str | None = None
    to_version: str | None = None
    revision_before: int | None = None
    revision_after: int | None = None
    error_code: str | None = None
    error_message: str | None = None
    attempts: int = 1
    steps: list[OperationStep] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "op_type": self.op_type.value,
            "artifact_id": self.artifact_id,
            "env": self.env,
            "version": self.version,
            "expected_revision": self.expected_revision,
            "status": self.status.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "from_version": self.from_version,
            "to_version": self.to_version,
            "revision_before": self.revision_before,
            "revision_after": self.revision_after,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "attempts": self.attempts,
            "steps": [s.to_dict() for s in self.steps],
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Operation:
        return cls(
            request_id=d["request_id"],
            op_type=OperationType(d["op_type"]),
            artifact_id=d["artifact_id"],
            env=d.get("env"),
            version=d.get("version"),
            expected_revision=d.get("expected_revision"),
            status=OperationStatus(d["status"]),
            created_at=d["created_at"],
            updated_at=d["updated_at"],
            from_version=d.get("from_version"),
            to_version=d.get("to_version"),
            revision_before=d.get("revision_before"),
            revision_after=d.get("revision_after"),
            error_code=d.get("error_code"),
            error_message=d.get("error_message"),
            attempts=d.get("attempts", 1),
            steps=[OperationStep.from_dict(s) for s in d.get("steps", [])],
        )


# ---------------- API 模型（Pydantic） ----------------


class RegisterVersionRequest(BaseModel):
    artifact_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    build_status: BuildStatus = BuildStatus.SUCCESS
    metadata: dict[str, Any] = Field(default_factory=dict)


class ReportBuildRequest(BaseModel):
    build_status: BuildStatus


class PromoteRequest(BaseModel):
    request_id: str = Field(min_length=1)
    artifact_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    target_env: str = Field(min_length=1)
    # 调用方观察到的目标环境预期 revision；None 表示不校验（仍在锁内执行）
    expected_revision: int | None = None
    # 测试用：在指定环境的写入环节注入失败，模拟中途失败
    fail_at_env: str | None = None


class RollbackRequest(BaseModel):
    request_id: str = Field(min_length=1)
    artifact_id: str = Field(min_length=1)
    env: str = Field(min_length=1)
    # 回滚到的版本；None 表示撤下当前生效版本（回到无版本状态）
    to_version: str | None = None
    expected_revision: int | None = None


class UpgradeRequest(BaseModel):
    """分批替换升级：把 target_env 及其之前尚未换新的环境逐环境替换成 version。

    与 PROMOTE 的区别：PROMOTE 面向"从空环境一次铺满/补缺口"，
    要求本版本链路不被别的版本隔断；UPGRADE 面向"旧版本已全量生效，
    逐环境替换"，允许低环境已是新版本、高环境仍是旧版本的混合窗口。
    """

    request_id: str = Field(min_length=1)
    artifact_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    target_env: str = Field(min_length=1)
    expected_revision: int | None = None
    fail_at_env: str | None = None


class EnvStateView(BaseModel):
    env: str
    active_version: str | None
    revision: int


class EnvUpgradeView(BaseModel):
    """分批升级进度查询中单个环境的视图。"""

    env: str
    active_version: str | None
    revision: int
    on_target_version: bool  # 当前生效版本是否已是被查询的目标版本


class UpgradeStatusView(BaseModel):
    """某产物某版本的分批替换升级进度（只读视图，查询不改状态）。"""

    artifact_id: str
    version: str
    phase: str  # UpgradePhase 值
    # 该版本在流水线中的生效位置是否构成合法前缀（可从当前状态继续往后推）
    continuable: bool
    # 不可继续时的原因（正常中间过渡为 None）
    blocking_reason: str | None
    envs: list[EnvUpgradeView] = Field(default_factory=list)


class OperationView(BaseModel):
    request_id: str
    op_type: str
    artifact_id: str
    env: str | None
    version: str | None
    expected_revision: int | None
    status: str
    error_code: str | None
    error_message: str | None
    attempts: int
    from_version: str | None
    to_version: str | None
    revision_before: int | None
    revision_after: int | None
    created_at: str
    updated_at: str
    steps: list[dict[str, Any]] = Field(default_factory=list)
