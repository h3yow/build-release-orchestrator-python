"""晋级 / 回滚编排逻辑。

规则：
- 晋级：产物须存在且构建成功；目标环境为 dev 时无前置环境要求，
  否则该版本须在前一环境处于 ACTIVE；版本不得已在目标环境 ACTIVE。
- 回滚：目标环境当前生效版本须等于被回滚版本；回落目标取自操作流水中
  该环境上一条已提交晋级的结果（无则回到无生效版本）。
- 每次写入都携带调用方声明的预期前置版本（CAS），冲突即拒绝、可安全重试。
- 晋级中途失败（模拟下游环节故障）时，环境回落到操作前可见状态，
  操作流水记录 FAILED，已生效环境不受影响。
- request_id 为幂等键：重复提交返回首次操作的结果，不重复执行。
"""
from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable

from .models import (
    ENV_ORDER,
    Artifact,
    EnvName,
    EnvState,
    ErrorCode,
    OpRecord,
    OpResult,
    OpStatus,
    OpType,
)
from .store import Store


class Orchestrator:
    def __init__(self, store: Store) -> None:
        self.store = store
        # 进程内锁：保证「校验 + CAS + 流水」对同一环境串行，交错请求不会互相覆盖
        self._mu = threading.RLock()

    # ---------- 查询 ----------

    def env_status(self) -> dict[str, str | None]:
        return {env.value: self.store.get_active(env) for env in ENV_ORDER}

    def version_env_state(self, version: str, env: EnvName) -> EnvState:
        """由环境现状与操作流水推导某版本在某环境的可见状态。"""
        if self.store.get_active(env) == version:
            return EnvState.ACTIVE
        for op in self.store.list_ops():
            if (op.op_type == OpType.PROMOTE and op.status == OpStatus.COMMITTED
                    and op.version == version and op.env == env):
                return EnvState.ROLLED_BACK
        return EnvState.ABSENT

    # ---------- 内部 ----------

    def _new_op(self, op_type: OpType, version: str, env: EnvName,
                expected: str | None, request_id: str | None) -> OpRecord:
        return OpRecord(
            op_id=uuid.uuid4().hex, op_type=op_type, version=version, env=env,
            status=OpStatus.REJECTED, expected=expected, result=None,
            request_id=request_id, ts=time.time(),
        )

    def _finish(self, op: OpRecord, status: OpStatus, result: str | None,
                reason: str | None, error: ErrorCode | None, detail: str) -> OpResult:
        op.status = status
        op.result = result
        op.reason = reason
        self.store.append_op(op)
        if op.request_id:
            self.store.put_request(op.request_id, op.op_id)
        return OpResult(ok=status == OpStatus.COMMITTED, op=op, error=error, detail=detail)

    def _reject(self, op: OpRecord, error: ErrorCode, detail: str) -> OpResult:
        return self._finish(op, OpStatus.REJECTED, self.store.get_active(op.env),
                            error.value, error, detail)

    def _idempotent_replay(self, request_id: str | None) -> OpResult | None:
        if not request_id:
            return None
        op_id = self.store.get_request(request_id)
        if op_id is None:
            return None
        op = self.store.get_op(op_id)
        if op is None:
            return None
        error = ErrorCode(op.reason) if op.status == OpStatus.REJECTED and op.reason in \
            {e.value for e in ErrorCode} else None
        return OpResult(ok=op.status == OpStatus.COMMITTED, op=op, error=error,
                        detail="idempotent replay")

    # ---------- 晋级 ----------

    def promote(self, version: str, env: EnvName, expected: str | None,
                request_id: str | None = None,
                fail_hook: Callable[[EnvName], bool] | None = None) -> OpResult:
        with self._mu:
            replay = self._idempotent_replay(request_id)
            if replay is not None:
                return replay

            op = self._new_op(OpType.PROMOTE, version, env, expected, request_id)

            artifact = self.store.get_artifact(version)
            if artifact is None:
                return self._reject(op, ErrorCode.ARTIFACT_NOT_FOUND,
                                    f"artifact {version} not found")
            if artifact.build_status != "succeeded":
                return self._reject(op, ErrorCode.BUILD_NOT_SUCCEEDED,
                                    f"build status is {artifact.build_status}")

            idx = ENV_ORDER.index(env)
            if idx > 0:
                prev = ENV_ORDER[idx - 1]
                if self.version_env_state(version, prev) != EnvState.ACTIVE:
                    return self._reject(op, ErrorCode.PRECONDITION_NOT_MET,
                                        f"version not active in previous env {prev.value}")
            if self.store.get_active(env) == version:
                return self._reject(op, ErrorCode.ILLEGAL_TRANSITION,
                                    f"version already active in {env.value}")

            if not self.store.compare_and_set(env, expected, version):
                return self._reject(op, ErrorCode.PRECONDITION_CONFLICT,
                                    f"expected {expected!r} but env {env.value} has "
                                    f"{self.store.get_active(env)!r}; safe to retry")

            # 模拟晋级中途下游环节失败：环境回落到操作前可见状态
            if fail_hook is not None and fail_hook(env):
                self.store.compare_and_set(env, version, expected)
                return self._finish(op, OpStatus.FAILED, expected,
                                    "downstream_step_failed", None,
                                    "promotion failed midway; env restored to pre-op state")

            return self._finish(op, OpStatus.COMMITTED, version, None, None, "promoted")

    # ---------- 回滚 ----------

    def _restore_target(self, env: EnvName, current: str) -> str | None:
        """从操作流水找该环境在 current 生效之前最近一次的生效版本。"""
        promotes = [op for op in self.store.list_ops()
                    if op.op_type == OpType.PROMOTE and op.env == env
                    and op.status == OpStatus.COMMITTED]
        promotes.sort(key=lambda op: op.ts)
        prev: str | None = None
        for op in promotes:
            if op.result == current:
                return prev
            prev = op.result
        return None

    def rollback(self, version: str, env: EnvName, expected: str | None,
                 request_id: str | None = None) -> OpResult:
        with self._mu:
            replay = self._idempotent_replay(request_id)
            if replay is not None:
                return replay

            op = self._new_op(OpType.ROLLBACK, version, env, expected, request_id)

            if self.store.get_artifact(version) is None:
                return self._reject(op, ErrorCode.ARTIFACT_NOT_FOUND,
                                    f"artifact {version} not found")
            current = self.store.get_active(env)
            if current != version:
                return self._reject(op, ErrorCode.ENV_NOT_ACTIVE,
                                    f"version {version} is not active in {env.value} "
                                    f"(active: {current!r})")

            target = self._restore_target(env, version)
            if not self.store.compare_and_set(env, expected, target):
                return self._reject(op, ErrorCode.PRECONDITION_CONFLICT,
                                    f"expected {expected!r} but env {env.value} has "
                                    f"{self.store.get_active(env)!r}; safe to retry")

            return self._finish(op, OpStatus.COMMITTED, target, None, None,
                                f"rolled back to {target!r}")
