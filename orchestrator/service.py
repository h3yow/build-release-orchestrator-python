"""发布编排服务：晋级 / 回滚 / 幂等 / 中途失败处置。

规则摘要
========
* 只有 build_status=SUCCESS 的版本可参与晋级。
* 流水线固定 dev -> staging -> prod，版本必须先在第 i-1 个环境生效，
  才能晋级到第 i 个环境；晋级到 target_env 时自动级联经过中间环境。
* 每次环境写入都使该环境 revision +1；调用方携带 expected_revision，
  与当前 revision 不符则返回 REVISION_CONFLICT，状态不变、可安全重试。
* 同一 request_id 的重复请求：成功过则原样返回（幂等重放）；失败过则
  累加 attempts 重新执行；请求体指纹不一致则明确拒绝。
* 级联晋级中途失败（如 fail_at_env 注入）时，已写入的环境按逆序回落到
  操作前可见状态（active_version 与 revision 均还原），失败与回滚过程
  全部记录在操作流水的 steps 中。
* 分批替换升级（UPGRADE）：旧版本已全量生效后，允许低环境已换新、
  高环境仍是旧版本的混合窗口；每次请求把 [0..target] 中尚未换新的
  环境逐环境替换，已换新的环境跳过不重复写入。并发、幂等、失败补偿
  规则与晋级一致。
"""

from __future__ import annotations

import datetime
import logging

from .errors import (
    AlreadyActiveError,
    ArtifactNotFoundError,
    EnvNotFoundError,
    InjectedFailureError,
    InvalidStateError,
    NothingToRollbackError,
    PreconditionFailedError,
    RevisionConflictError,
    VersionNotFoundError,
)
from .models import (
    DEFAULT_PIPELINE,
    DeploymentStatus,
    EnvState,
    EnvUpgradeView,
    Operation,
    OperationStatus,
    OperationStep,
    OperationType,
    PromoteRequest,
    RegisterVersionRequest,
    RollbackRequest,
    UpgradePhase,
    UpgradeRequest,
    UpgradeStatusView,
    Version,
)
from .store import Store

logger = logging.getLogger("orchestrator")


def _now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()


class ReleaseService:
    def __init__(self, store: Store) -> None:
        self.store = store

    # ================= 产物 / 构建 =================

    def register_version(
        self, req: RegisterVersionRequest, request_id: str
    ) -> Operation:
        with self.store.locked():
            if self.store.get_version(req.artifact_id, req.version) is not None:
                raise InvalidStateError(
                    f"产物 {req.artifact_id} 的版本 {req.version} 已注册，"
                    "不允许重复注册"
                )
            now = _now()
            version = Version(
                artifact_id=req.artifact_id,
                version=req.version,
                build_status=req.build_status,
                created_at=now,
                metadata=req.metadata,
            )
            self.store.put_version(version)
            self.store.persist()
            op = Operation(
                request_id=request_id,
                op_type=OperationType.REGISTER,
                artifact_id=req.artifact_id,
                env=None,
                version=req.version,
                expected_revision=None,
                status=OperationStatus.SUCCESS,
                created_at=now,
                updated_at=now,
                to_version=req.version,
            )
            self.store.put_operation(op)
            logger.info(
                "register request_id=%s artifact=%s version=%s build=%s -> SUCCESS",
                request_id, req.artifact_id, req.version, req.build_status.value,
            )
            return op

    def report_build(
        self, artifact_id: str, version: str, build_status
    ) -> Version:
        from .models import BuildStatus

        with self.store.locked():
            v = self._require_version(artifact_id, version)
            if build_status not in (BuildStatus.SUCCESS, BuildStatus.FAILED):
                raise InvalidStateError("构建结果只能上报为 SUCCESS 或 FAILED")
            if v.build_status != BuildStatus.PENDING:
                raise InvalidStateError(
                    f"版本构建状态已为 {v.build_status.value}，终态不可变更"
                )
            v.build_status = build_status
            self.store.persist()
            logger.info(
                "report_build artifact=%s version=%s -> %s",
                artifact_id, version, build_status.value,
            )
            return v

    # ================= 晋级 =================

    def promote(self, req: PromoteRequest) -> Operation:
        logger.info(
            "promote request_id=%s artifact=%s version=%s target=%s "
            "expected_revision=%s fail_at=%s",
            req.request_id, req.artifact_id, req.version, req.target_env,
            req.expected_revision, req.fail_at_env,
        )
        with self.store.locked():
            # 1) 幂等处理
            existing = self.store.get_operation(req.request_id)
            try:
                op = self._dedupe(
                    existing, req.request_id, OperationType.PROMOTE,
                    req.artifact_id, req.version, req.target_env,
                    req.expected_revision,
                )
            except _IdempotentReplay as replay:
                return replay.operation

            # 2) 前置校验（不改变任何状态）
            envs = self.store.list_envs()
            by_name = {e.env: e for e in envs}
            if req.target_env not in by_name:
                self._fail_and_raise(op, EnvNotFoundError(
                    f"环境 {req.target_env} 不存在"))
            target = by_name[req.target_env]
            v = self.store.get_version(req.artifact_id, req.version)
            if not self.store.has_artifact(req.artifact_id):
                self._fail_and_raise(op, ArtifactNotFoundError(
                    f"产物 {req.artifact_id} 不存在"))
            if v is None:
                self._fail_and_raise(op, VersionNotFoundError(
                    f"产物 {req.artifact_id} 的版本 {req.version} 不存在"))
            from .models import BuildStatus
            if v.build_status != BuildStatus.SUCCESS:
                self._fail_and_raise(op, PreconditionFailedError(
                    f"版本构建状态为 {v.build_status.value}，"
                    "只有构建成功的版本允许晋级"))

            # 3) 计算级联路径与前置条件（核心规则）：
            #    a) first_self = 本版本第一次生效的位置。若已在目标环境
            #       生效 -> ALREADY_ACTIVE。
            #    b) 路径 = [0, target] 中所有"尚未生效本版本"的环境，按
            #       流水线顺序写入（允许替换旧版本，也允许填补回滚缺口）。
            #    c) 首个写入环境必须是 dev；之后每写一个环境，其上一环
            #       境必须已是本版本（前置在锁内即时校验），从而保证
            #       "只有上一环境已生效才允许进入下一环境"。
            target_idx = DEFAULT_PIPELINE.index(req.target_env)
            # 前缀连续性校验：在 [0, target] 内，本版本一旦出现，
            # 其后直到 target 都必须是本版本或回滚缺口(None)，
            # 中间夹着别的版本即为交错断裂，明确拒绝。
            seen_self = False
            for i in range(target_idx + 1):
                cur = by_name[DEFAULT_PIPELINE[i]].active_version
                if cur == req.version:
                    seen_self = True
                elif seen_self and cur is not None:
                    self._fail_and_raise(op, InvalidStateError(
                        f"版本 {req.version} 的链路在环境 "
                        f"{DEFAULT_PIPELINE[i]} 处被别的版本 {cur} 隔断，"
                        "流水线状态交错，拒绝晋级"))
            path: list[EnvState] = [
                by_name[DEFAULT_PIPELINE[i]]
                for i in range(0, target_idx + 1)
                if by_name[DEFAULT_PIPELINE[i]].active_version != req.version
            ]
            # 链路全部已是本版本：无可写入环境，属于重复晋级
            if not path:
                self._fail_and_raise(op, AlreadyActiveError(
                    f"版本 {req.version} 已在 {req.target_env} "
                    "及其全部前置环境生效，禁止重复晋级"))

            # 4) 目标环境乐观锁校验
            if (req.expected_revision is not None
                    and target.revision != req.expected_revision):
                self._fail_and_raise(op, RevisionConflictError(
                    f"环境 {req.target_env} 当前 revision={target.revision}，"
                    f"与预期 revision={req.expected_revision} 不一致，"
                    "状态已被其他请求修改"))
            if req.fail_at_env and req.fail_at_env not in by_name:
                self._fail_and_raise(op, EnvNotFoundError(
                    f"fail_at_env 指定的环境 {req.fail_at_env} 不存在"))

            # 5) 逐级写入。合法性由路径连续性保证：
            #    - 路径必须从 dev(level=0) 起，或紧随某个已生效本版本的环境；
            #    - 路径内部不得有洞。这意味着回滚缺口(None)会被自动纳入补齐，
            #      而"dev=别的版本、staging=本版本"这类交错无法从中间起步。
            #    每写一步再即时确认紧邻上一环境此刻已是本版本。
            applied: list[tuple[EnvState, str | None, int]] = []
            for step_no, env_state in enumerate(path):
                level = DEFAULT_PIPELINE.index(env_state.env)
                if step_no == 0 and level != 0:
                    anchor = by_name[DEFAULT_PIPELINE[level - 1]]
                    if anchor.active_version != req.version:
                        self._fail_and_raise(op, PreconditionFailedError(
                            f"晋级 {env_state.env} 前置条件不满足："
                            f"上一环境 {anchor.env} 生效版本为 "
                            f"{anchor.active_version}，而非 {req.version}"))
                elif step_no > 0:
                    prev = by_name[DEFAULT_PIPELINE[level - 1]]
                    if prev.active_version != req.version:
                        self._compensate(applied, op)
                        self._fail_and_raise(op, PreconditionFailedError(
                            f"晋级 {env_state.env} 前置条件不满足："
                            f"上一环境 {prev.env} 生效版本为 "
                            f"{prev.active_version}，而非 {req.version}"))
                rev_before = env_state.revision
                from_ver = env_state.active_version
                self._mark_latest(env_state, DeploymentStatus.SUPERSEDED)
                env_state.active_version = req.version
                env_state.revision += 1
                env_state.history.append(self._history_entry(
                    env_state, req.version, OperationType.PROMOTE,
                    DeploymentStatus.ACTIVE, req.request_id))
                self.store.persist()
                applied.append((env_state, from_ver, rev_before))
                op.steps.append(OperationStep(
                    env=env_state.env,
                    status=OperationStatus.SUCCESS,
                    from_version=from_ver,
                    to_version=req.version,
                    revision_before=rev_before,
                    revision_after=env_state.revision,
                ))

                if req.fail_at_env == env_state.env:
                    # 模拟该环境写入后的后置环节失败（如通知/健康检查失败）
                    err = InjectedFailureError(
                        f"环境 {env_state.env} 的后置环节被注入失败，"
                        "本次晋级整体判定失败，开始回落")
                    op.error_code = err.code
                    op.error_message = err.message
                    self._compensate(applied, op)
                    self._finish(op, success=False)
                    logger.warning(
                        "promote request_id=%s FAILED at env=%s, rolled back; "
                        "final states=%s",
                        req.request_id, env_state.env, self._snapshot())
                    raise err

            # 6) 成功
            last = path[-1]
            op.from_version = None
            op.to_version = req.version
            op.revision_before = op.steps[0].revision_before
            op.revision_after = last.revision
            self._finish(op, success=True)
            logger.info(
                "promote request_id=%s SUCCESS path=%s final states=%s",
                req.request_id, [s.env for s in op.steps], self._snapshot())
            return op

    # ================= 分批替换升级 =================

    def upgrade(self, req: UpgradeRequest) -> Operation:
        """旧版本已全量生效后，把 [0..target_env] 中尚未换新的环境逐环境替换。

        与 promote 的规则差异：
        * 允许混合窗口：本版本已生效的环境构成流水线前缀，其余环境仍是
          旧版本，这是正常的进行中状态；
        * 路径 = [0..target] 中所有尚未生效本版本的环境（必为连续后缀），
          已换新的环境跳过、不重复写入；
        * 路径中每个环境当前必须有生效版本（有旧版本可替换）；若出现
          空环境说明不是"替换"场景，应走 promote 铺满，明确拒绝。
        """
        logger.info(
            "upgrade request_id=%s artifact=%s version=%s target=%s "
            "expected_revision=%s fail_at=%s",
            req.request_id, req.artifact_id, req.version, req.target_env,
            req.expected_revision, req.fail_at_env,
        )
        with self.store.locked():
            # 1) 幂等处理
            existing = self.store.get_operation(req.request_id)
            try:
                op = self._dedupe(
                    existing, req.request_id, OperationType.UPGRADE,
                    req.artifact_id, req.version, req.target_env,
                    req.expected_revision,
                )
            except _IdempotentReplay as replay:
                return replay.operation

            # 2) 前置校验（不改变任何状态）
            envs = self.store.list_envs()
            by_name = {e.env: e for e in envs}
            if req.target_env not in by_name:
                self._fail_and_raise(op, EnvNotFoundError(
                    f"环境 {req.target_env} 不存在"))
            target = by_name[req.target_env]
            if not self.store.has_artifact(req.artifact_id):
                self._fail_and_raise(op, ArtifactNotFoundError(
                    f"产物 {req.artifact_id} 不存在"))
            v = self.store.get_version(req.artifact_id, req.version)
            if v is None:
                self._fail_and_raise(op, VersionNotFoundError(
                    f"产物 {req.artifact_id} 的版本 {req.version} 不存在"))
            from .models import BuildStatus
            if v.build_status != BuildStatus.SUCCESS:
                self._fail_and_raise(op, PreconditionFailedError(
                    f"版本构建状态为 {v.build_status.value}，"
                    "只有构建成功的版本允许升级"))

            # 3) 前缀连续性：本版本已生效的环境必须构成流水线前缀，
            #    否则属于交错断裂，无法安全续推，明确拒绝。
            prefix_end = 0
            for i, name in enumerate(DEFAULT_PIPELINE):
                if by_name[name].active_version == req.version:
                    if i != prefix_end:
                        self._fail_and_raise(op, InvalidStateError(
                            f"版本 {req.version} 在环境 {name} 生效，但更低环境 "
                            f"{DEFAULT_PIPELINE[prefix_end]} 仍是 "
                            f"{by_name[DEFAULT_PIPELINE[prefix_end]].active_version}，"
                            "升级链路交错断裂，无法继续"))
                    prefix_end += 1
            target_idx = DEFAULT_PIPELINE.index(req.target_env)
            path: list[EnvState] = [
                by_name[DEFAULT_PIPELINE[i]]
                for i in range(target_idx + 1)
                if by_name[DEFAULT_PIPELINE[i]].active_version != req.version
            ]
            if not path:
                self._fail_and_raise(op, AlreadyActiveError(
                    f"版本 {req.version} 已在 {req.target_env} "
                    "及其全部前置环境生效，禁止重复升级"))
            # 替换语义：路径上每个环境都必须有旧版本在跑；
            # 空环境属于"从空铺满"场景，应走 promote。
            for env_state in path:
                if env_state.active_version is None:
                    self._fail_and_raise(op, PreconditionFailedError(
                        f"环境 {env_state.env} 当前无生效版本，"
                        "分批替换升级要求旧版本已在各环境生效；"
                        "空环境请使用晋级（promote）铺满"))

            # 4) 目标环境乐观锁校验
            if (req.expected_revision is not None
                    and target.revision != req.expected_revision):
                self._fail_and_raise(op, RevisionConflictError(
                    f"环境 {req.target_env} 当前 revision={target.revision}，"
                    f"与预期 revision={req.expected_revision} 不一致，"
                    "状态已被其他请求修改"))
            if req.fail_at_env and req.fail_at_env not in by_name:
                self._fail_and_raise(op, EnvNotFoundError(
                    f"fail_at_env 指定的环境 {req.fail_at_env} 不存在"))

            # 5) 逐级替换写入（全部在锁内，无跳级、无半写）
            applied: list[tuple[EnvState, str | None, int]] = []
            for env_state in path:
                rev_before = env_state.revision
                from_ver = env_state.active_version
                self._mark_latest(env_state, DeploymentStatus.SUPERSEDED)
                env_state.active_version = req.version
                env_state.revision += 1
                env_state.history.append(self._history_entry(
                    env_state, req.version, OperationType.UPGRADE,
                    DeploymentStatus.ACTIVE, req.request_id))
                self.store.persist()
                applied.append((env_state, from_ver, rev_before))
                op.steps.append(OperationStep(
                    env=env_state.env,
                    status=OperationStatus.SUCCESS,
                    from_version=from_ver,
                    to_version=req.version,
                    revision_before=rev_before,
                    revision_after=env_state.revision,
                ))

                if req.fail_at_env == env_state.env:
                    err = InjectedFailureError(
                        f"环境 {env_state.env} 的后置环节被注入失败，"
                        "本次升级整体判定失败，开始回落")
                    op.error_code = err.code
                    op.error_message = err.message
                    self._compensate(applied, op)
                    self._finish(op, success=False)
                    logger.warning(
                        "upgrade request_id=%s FAILED at env=%s, rolled back; "
                        "final states=%s",
                        req.request_id, env_state.env, self._snapshot())
                    raise err

            # 6) 成功
            last = path[-1]
            op.from_version = op.steps[0].from_version
            op.to_version = req.version
            op.revision_before = op.steps[0].revision_before
            op.revision_after = last.revision
            self._finish(op, success=True)
            logger.info(
                "upgrade request_id=%s SUCCESS path=%s final states=%s",
                req.request_id, [s.env for s in op.steps], self._snapshot())
            return op

    # ================= 回滚 =================

    def rollback(self, req: RollbackRequest) -> Operation:
        logger.info(
            "rollback request_id=%s artifact=%s env=%s to_version=%s "
            "expected_revision=%s",
            req.request_id, req.artifact_id, req.env,
            req.to_version, req.expected_revision,
        )
        with self.store.locked():
            existing = self.store.get_operation(req.request_id)
            try:
                op = self._dedupe(
                    existing, req.request_id, OperationType.ROLLBACK,
                    req.artifact_id, req.to_version, req.env,
                    req.expected_revision,
                )
            except _IdempotentReplay as replay:
                return replay.operation
            env_state = self.store.get_env(req.env)
            if env_state is None:
                self._fail_and_raise(op, EnvNotFoundError(
                    f"环境 {req.env} 不存在"))
            if not self.store.has_artifact(req.artifact_id):
                self._fail_and_raise(op, ArtifactNotFoundError(
                    f"产物 {req.artifact_id} 不存在"))

            # 乐观锁
            if (req.expected_revision is not None
                    and env_state.revision != req.expected_revision):
                self._fail_and_raise(op, RevisionConflictError(
                    f"环境 {req.env} 当前 revision={env_state.revision}，"
                    f"与预期 revision={req.expected_revision} 不一致"))

            current = env_state.active_version
            if req.to_version is None:
                # 撤下当前生效版本
                if current is None:
                    self._fail_and_raise(op, NothingToRollbackError(
                        f"环境 {req.env} 当前没有生效版本，无可回滚内容"))
                target_version: str | None = None
            else:
                tv = self.store.get_version(req.artifact_id, req.to_version)
                if tv is None:
                    self._fail_and_raise(op, VersionNotFoundError(
                        f"产物 {req.artifact_id} 的版本 "
                        f"{req.to_version} 不存在"))
                from .models import BuildStatus
                if tv.build_status != BuildStatus.SUCCESS:
                    self._fail_and_raise(op, PreconditionFailedError(
                        f"回滚目标版本构建状态为 {tv.build_status.value}，"
                        "不允许生效"))
                ever_deployed = any(
                    h.version == req.to_version for h in env_state.history)
                if not ever_deployed:
                    self._fail_and_raise(op, InvalidStateError(
                        f"版本 {req.to_version} 从未在环境 {req.env} 生效过，"
                        "回滚目标必须来自该环境的可观测历史记录"))
                if req.to_version == current:
                    self._fail_and_raise(op, AlreadyActiveError(
                        f"版本 {req.to_version} 已是环境 {req.env} 的生效版本"))
                target_version = req.to_version

            rev_before = env_state.revision
            self._mark_latest(env_state, DeploymentStatus.ROLLED_BACK)
            env_state.active_version = target_version
            env_state.revision += 1
            env_state.history.append(self._history_entry(
                env_state, target_version, OperationType.ROLLBACK,
                DeploymentStatus.ACTIVE, req.request_id))
            self.store.persist()

            op.from_version = current
            op.to_version = target_version
            op.revision_before = rev_before
            op.revision_after = env_state.revision
            op.steps.append(OperationStep(
                env=req.env,
                status=OperationStatus.SUCCESS,
                from_version=current,
                to_version=target_version,
                revision_before=rev_before,
                revision_after=env_state.revision,
            ))
            self._finish(op, success=True)
            logger.info(
                "rollback request_id=%s SUCCESS env=%s %s->%s final states=%s",
                req.request_id, req.env, current, target_version,
                self._snapshot())
            return op

    # ================= 查询 =================

    def get_version(self, artifact_id: str, version: str) -> Version:
        with self.store.locked():
            return self._require_version(artifact_id, version)

    def list_envs(self) -> list[EnvState]:
        with self.store.locked():
            return self.store.list_envs()

    def get_upgrade_status(self, artifact_id: str, version: str) -> UpgradeStatusView:
        """查询某版本的分批替换升级进度（只读，不改动任何环境状态）。

        阶段判定：
        * NOT_STARTED：没有任何环境生效该版本；
        * IN_PROGRESS：该版本已生效的环境构成流水线前缀，高环境仍是旧版本
          （正常的混合过渡窗口）；
        * COMPLETED：全部环境都已生效该版本。

        可继续性（continuable）区分"正常中间过渡"与"走不下去"：
        * 该版本出现但不构成前缀（交错断裂）-> 不可继续；
        * 前缀之后存在空环境（无旧版本可替换）-> 升级走不下去，
          需先通过 promote 铺满；
        * 其余情况可继续往后推。
        """
        with self.store.locked():
            if not self.store.has_artifact(artifact_id):
                raise ArtifactNotFoundError(f"产物 {artifact_id} 不存在")
            if self.store.get_version(artifact_id, version) is None:
                raise VersionNotFoundError(
                    f"产物 {artifact_id} 的版本 {version} 不存在")
            envs = self.store.list_envs()
            view_envs = [
                EnvUpgradeView(
                    env=e.env,
                    active_version=e.active_version,
                    revision=e.revision,
                    on_target_version=e.active_version == version,
                )
                for e in envs
            ]
            on_flags = [e.active_version == version for e in envs]
            if all(on_flags):
                return UpgradeStatusView(
                    artifact_id=artifact_id, version=version,
                    phase=UpgradePhase.COMPLETED, continuable=False,
                    blocking_reason=None, envs=view_envs)
            if not any(on_flags):
                return UpgradeStatusView(
                    artifact_id=artifact_id, version=version,
                    phase=UpgradePhase.NOT_STARTED, continuable=True,
                    blocking_reason=None, envs=view_envs)
            # 混合状态：校验是否构成合法前缀
            prefix_end = 0
            for i, e in enumerate(envs):
                if e.active_version == version:
                    if i != prefix_end:
                        return UpgradeStatusView(
                            artifact_id=artifact_id, version=version,
                            phase=UpgradePhase.IN_PROGRESS, continuable=False,
                            blocking_reason=(
                                f"版本 {version} 在环境 {e.env} 生效，但更低环境 "
                                f"{envs[prefix_end].env} 仍是 "
                                f"{envs[prefix_end].active_version}，"
                                "升级链路交错断裂，无法继续"),
                            envs=view_envs)
                    prefix_end += 1
            # 合法前缀：检查剩余环境是否都有旧版本可替换
            for e in envs[prefix_end:]:
                if e.active_version is None:
                    return UpgradeStatusView(
                        artifact_id=artifact_id, version=version,
                        phase=UpgradePhase.IN_PROGRESS, continuable=False,
                        blocking_reason=(
                            f"环境 {e.env} 当前无生效版本，无旧版本可替换；"
                            "需先通过晋级（promote）铺满后再继续升级"),
                        envs=view_envs)
            return UpgradeStatusView(
                artifact_id=artifact_id, version=version,
                phase=UpgradePhase.IN_PROGRESS, continuable=True,
                blocking_reason=None, envs=view_envs)

    def get_operation(self, request_id: str) -> Operation:
        with self.store.locked():
            op = self.store.get_operation(request_id)
            if op is None:
                raise VersionNotFoundError(  # 复用通用未找到语义不合适，单独抛
                    f"操作流水 {request_id} 不存在",
                    code="OPERATION_NOT_FOUND")
            return op

    def list_operations(self, artifact_id: str | None = None) -> list[Operation]:
        with self.store.locked():
            return self.store.list_operations(artifact_id)

    # ================= 内部辅助 =================

    def _require_version(self, artifact_id: str, version: str) -> Version:
        if not self.store.has_artifact(artifact_id):
            raise ArtifactNotFoundError(f"产物 {artifact_id} 不存在")
        v = self.store.get_version(artifact_id, version)
        if v is None:
            raise VersionNotFoundError(
                f"产物 {artifact_id} 的版本 {version} 不存在")
        return v

    def _dedupe(
        self, existing: Operation | None, request_id: str,
        op_type: OperationType, artifact_id: str, version: str | None,
        env: str, expected_revision: int | None,
    ) -> Operation:
        """返回可写入的 Operation 对象；命中成功幂等则直接抛回旧记录。"""
        now = _now()
        if existing is None:
            return Operation(
                request_id=request_id,
                op_type=op_type,
                artifact_id=artifact_id,
                env=env,
                version=version,
                expected_revision=expected_revision,
                status=OperationStatus.FAILED,  # 执行成功前一律视为失败态
                created_at=now,
                updated_at=now,
            )
        fingerprint_ok = (
            existing.op_type == op_type
            and existing.artifact_id == artifact_id
            and existing.version == version
            and existing.env == env
        )
        if not fingerprint_ok:
            from .errors import DuplicateRequestError
            raise DuplicateRequestError(
                f"request_id={request_id} 已用于另一个请求"
                f"（{existing.op_type.value} {existing.artifact_id}/"
                f"{existing.version}@{existing.env}），拒绝复用",
                mismatch=True,
            )
        if existing.status == OperationStatus.SUCCESS:
            logger.info(
                "request_id=%s 已成功，幂等返回既有操作记录", request_id)
            raise _IdempotentReplay(existing)
        # 失败重试：复用记录，attempts 累加
        existing.attempts += 1
        existing.steps = []
        existing.error_code = None
        existing.error_message = None
        existing.status = OperationStatus.FAILED
        existing.updated_at = now
        logger.info(
            "request_id=%s 为失败重试（attempts=%s）",
            request_id, existing.attempts)
        return existing

    def _finish(self, op: Operation, *, success: bool) -> None:
        op.status = (
            OperationStatus.SUCCESS if success else OperationStatus.FAILED)
        op.updated_at = _now()
        self.store.put_operation(op)

    def _fail_and_raise(self, op: Operation, err: Exception) -> None:
        """记录一条失败流水后抛出业务错误。"""
        code = getattr(err, "code", "INVALID_STATE")
        op.error_code = code
        op.error_message = str(err)
        self._finish(op, success=False)
        logger.warning(
            "operation request_id=%s REJECTED code=%s message=%s states=%s",
            op.request_id, code, err, self._snapshot())
        raise err

    def _compensate(
        self,
        applied: list[tuple[EnvState, str | None, int]],
        op: Operation,
    ) -> None:
        """把本次已写入的环境逆序回落到操作前可见状态。"""
        for env_state, from_ver, rev_before in reversed(applied):
            self._mark_latest(env_state, DeploymentStatus.ROLLED_BACK)
            env_state.active_version = from_ver
            env_state.revision = rev_before
            for step in op.steps:
                if step.env == env_state.env:
                    step.rolled_back = True
            env_state.history.append(self._history_entry(
                env_state, from_ver, OperationType.ROLLBACK,
                DeploymentStatus.ACTIVE, op.request_id))
            self.store.persist()
            logger.warning(
                "compensation env=%s restored to version=%s revision=%s",
                env_state.env, from_ver, rev_before)

    @staticmethod
    def _mark_latest(env_state: EnvState, status: DeploymentStatus) -> None:
        for h in reversed(env_state.history):
            if h.status == DeploymentStatus.ACTIVE:
                h.status = status
                return

    @staticmethod
    def _history_entry(
        env_state: EnvState, version: str | None,
        action: OperationType, status: DeploymentStatus,
        request_id: str,
    ):
        from .models import EnvHistoryEntry
        return EnvHistoryEntry(
            version=version,
            action=action.value,
            status=status,
            revision=env_state.revision,
            request_id=request_id,
            timestamp=_now(),
        )

    def _snapshot(self) -> dict[str, str | None]:
        return {e.env: e.active_version for e in self.store.list_envs()}


class _IdempotentReplay(Exception):
    """内部信号：命中已成功的相同请求，直接返回既有记录。"""

    def __init__(self, operation: Operation) -> None:
        self.operation = operation
