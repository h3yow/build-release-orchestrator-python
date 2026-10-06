"""发布编排服务：晋级 / 回滚 / 分批替换升级 / 幂等 / 中途失败处置。

规则摘要
========
* 只有 build_status=SUCCESS 的版本可参与晋级与替换升级。
* 流水线固定 dev -> staging -> prod，版本必须先在第 i-1 个环境生效，
  才能晋级到第 i 个环境；晋级到 target_env 时自动级联经过中间环境。
* 每次环境写入都使该环境 revision +1；调用方携带 expected_revision，
  与当前 revision 不符则返回 REVISION_CONFLICT，状态不变、可安全重试。
* 同一 request_id 的重复请求：成功过则原样返回（幂等重放）；失败过则
  累加 attempts 重新执行；请求体指纹不一致则明确拒绝。
* 级联晋级/替换升级中途失败（如 fail_at_env 注入）时，已写入的环境按
  逆序回落到操作前可见状态（active_version 与 revision 均还原），失败
  与回滚过程全部记录在操作流水的 steps 中。

分批替换升级（UPGRADE）
======================
* 适用场景：旧版本已铺满全部环境，要把新版本逐环境替换上去
  （先 dev、观察、再 staging、最后 prod）。
* 合法起点：所有环境生效版本一致（统一基线），或处于健康的替换中间
  窗口——存在切点 k，dev..k-1 已是新版本、k..prod 仍是同一旧版本。
* 中间混合窗口是正常进行中状态，不是异常；可只推下一级（target_env
  缺省），也可点名更靠后的环境自动补齐剩余环境。
* 链路断裂（剩余环境版本不一致、存在空环境、版本交错）时返回
  UPGRADE_BLOCKED，明确区分"正常过渡"与"确实没法继续"。
"""

from __future__ import annotations

import dataclasses
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
    UpgradeBlockedError,
    VersionNotFoundError,
)
from .models import (
    DEFAULT_PIPELINE,
    DeploymentStatus,
    EnvState,
    Operation,
    OperationStatus,
    OperationStep,
    OperationType,
    PromoteRequest,
    RegisterVersionRequest,
    ReplaceUpgradeRequest,
    RollbackRequest,
    UpgradePhase,
    Version,
)
from .store import Store

logger = logging.getLogger("orchestrator")


def _now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()


@dataclasses.dataclass(frozen=True)
class _UpgradeClassification:
    """对一组环境生效版本（按流水线序）针对目标版本的分类结果。"""

    phase: UpgradePhase
    cut: int  # 已连续换到新版本的环境个数（前缀长度）
    baseline: str | None  # 剩余环境上的统一旧版本（可识别时）
    continuable: bool  # 当前状态能否继续往下推进
    reason: str | None  # 不可继续时的原因


def _classify_upgrade(
    states: list[str | None], version: str
) -> _UpgradeClassification:
    """把各环境生效版本分类为 未开始/替换中/已完成，并判定能否继续。

    健康形态只有两种：
    * 统一基线：所有环境是同一个旧版本（尚未开始）；
    * 干净切口：前 cut 个环境是新版本，其余是同一个旧版本（替换中）。
    其余形态（空环境、剩余版本不一致、版本交错）都视为无法继续。
    """
    n = len(states)
    if all(s == version for s in states):
        return _UpgradeClassification(
            UpgradePhase.COMPLETED, n, None, False, "所有环境已是该版本")
    cut = 0
    while cut < n and states[cut] == version:
        cut += 1
    rest = states[cut:]
    first = rest[0]
    uniform_rest = first is not None and all(s == first for s in rest)
    if cut == 0:
        if all(s is None for s in states):
            return _UpgradeClassification(
                UpgradePhase.NOT_STARTED, 0, None, False,
                "各环境尚未部署任何版本：首次铺满请使用级联晋级 /promotions")
        if uniform_rest:
            return _UpgradeClassification(
                UpgradePhase.NOT_STARTED, 0, first, True, None)
        return _UpgradeClassification(
            UpgradePhase.NOT_STARTED, 0, None, False,
            "各环境基线版本不一致或存在空环境，无法作为替换升级起点")
    # cut > 0：新版本已经开始替换
    if uniform_rest:
        return _UpgradeClassification(
            UpgradePhase.REPLACING, cut, first, True, None)
    return _UpgradeClassification(
        UpgradePhase.REPLACING, cut, None, False,
        "替换链路断裂：剩余环境版本不一致或存在空环境，"
        "请先通过回滚/晋级修复后再继续")


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

    # ================= 分批替换升级 =================

    def upgrade(self, req: ReplaceUpgradeRequest) -> Operation:
        """旧版本全量生效后，把新版本逐环境替换上去。

        * target_env 缺省：只推进下一个待替换环境；
        * target_env 点名更靠后的环境：自动把剩余环境补齐到该环境；
        * 已换到新版本的环境绝不重复写入（路径从切点起算）。
        """
        logger.info(
            "upgrade request_id=%s artifact=%s version=%s target=%s "
            "expected_revision=%s fail_at=%s",
            req.request_id, req.artifact_id, req.version, req.target_env,
            req.expected_revision, req.fail_at_env,
        )
        with self.store.locked():
            # 1) 幂等处理（与晋级/回滚同一套指纹与重放规则）
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
            if req.target_env is not None and req.target_env not in by_name:
                self._fail_and_raise(op, EnvNotFoundError(
                    f"环境 {req.target_env} 不存在"))
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
                    "只有构建成功的版本允许替换升级"))

            # 3) 分类当前状态：统一基线 / 健康中间窗口可继续，其余拒绝
            states = [e.active_version for e in envs]
            cls = _classify_upgrade(states, req.version)
            if cls.phase is UpgradePhase.COMPLETED:
                self._fail_and_raise(op, AlreadyActiveError(
                    f"版本 {req.version} 已在全部环境生效，禁止重复升级"))
            if not cls.continuable:
                self._fail_and_raise(op, UpgradeBlockedError(
                    f"版本 {req.version} 的替换升级无法继续：{cls.reason}"))

            # 4) 计算写入路径：从切点起，到目标环境（缺省只推下一级）
            start = cls.cut
            if req.target_env is None:
                target_idx = start
            else:
                target_idx = DEFAULT_PIPELINE.index(req.target_env)
                if target_idx < start:
                    self._fail_and_raise(op, AlreadyActiveError(
                        f"环境 {req.target_env} 已是版本 {req.version}，"
                        "请点名更靠后的环境或缺省只推下一级"))
            path: list[EnvState] = [
                by_name[DEFAULT_PIPELINE[i]]
                for i in range(start, target_idx + 1)
            ]

            # 5) 目标环境乐观锁校验
            target = path[-1]
            if (req.expected_revision is not None
                    and target.revision != req.expected_revision):
                self._fail_and_raise(op, RevisionConflictError(
                    f"环境 {target.env} 当前 revision={target.revision}，"
                    f"与预期 revision={req.expected_revision} 不一致，"
                    "状态已被其他请求修改"))
            if req.fail_at_env and req.fail_at_env not in by_name:
                self._fail_and_raise(op, EnvNotFoundError(
                    f"fail_at_env 指定的环境 {req.fail_at_env} 不存在"))

            # 6) 逐级写入。每写一步即时确认紧邻上一环境此刻已是新版本
            #    （切点之前的健康窗口或本步刚写入的环境保证其成立）。
            applied: list[tuple[EnvState, str | None, int]] = []
            for step_no, env_state in enumerate(path):
                level = DEFAULT_PIPELINE.index(env_state.env)
                if level > 0:
                    prev = by_name[DEFAULT_PIPELINE[level - 1]]
                    if prev.active_version != req.version:
                        if step_no > 0:
                            self._compensate(applied, op)
                        self._fail_and_raise(op, PreconditionFailedError(
                            f"替换 {env_state.env} 前置条件不满足："
                            f"上一环境 {prev.env} 生效版本为 "
                            f"{prev.active_version}，而非 {req.version}"))
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
                    # 模拟该环境写入后的后置环节失败（如通知/健康检查失败）
                    err = InjectedFailureError(
                        f"环境 {env_state.env} 的后置环节被注入失败，"
                        "本次替换升级整体判定失败，开始回落")
                    op.error_code = err.code
                    op.error_message = err.message
                    self._compensate(applied, op)
                    self._finish(op, success=False)
                    logger.warning(
                        "upgrade request_id=%s FAILED at env=%s, rolled back; "
                        "final states=%s",
                        req.request_id, env_state.env, self._snapshot())
                    raise err

            # 7) 成功
            op.from_version = cls.baseline
            op.to_version = req.version
            op.revision_before = op.steps[0].revision_before
            op.revision_after = path[-1].revision
            self._finish(op, success=True)
            logger.info(
                "upgrade request_id=%s SUCCESS path=%s final states=%s",
                req.request_id, [s.env for s in op.steps], self._snapshot())
            return op

    # ================= 查询 =================

    def get_version(self, artifact_id: str, version: str) -> Version:
        with self.store.locked():
            return self._require_version(artifact_id, version)

    def list_envs(self) -> list[EnvState]:
        with self.store.locked():
            return self.store.list_envs()

    def get_upgrade_progress(self, artifact_id: str, version: str):
        """查询某产物针对某版本的分批替换升级进度。

        纯只读：不加任何写、不改 revision、不落操作流水。
        """
        from .models import UpgradeEnvProgress, UpgradeProgressView

        with self.store.locked():
            self._require_version(artifact_id, version)
            envs = self.store.list_envs()
            cls = _classify_upgrade(
                [e.active_version for e in envs], version)
            next_env = (
                DEFAULT_PIPELINE[cls.cut]
                if cls.continuable and cls.cut < len(envs) else None
            )
            view = UpgradeProgressView(
                artifact_id=artifact_id,
                version=version,
                phase=cls.phase,
                continuable=cls.continuable,
                reason=cls.reason,
                baseline_version=cls.baseline,
                next_env=next_env,
                envs=[
                    UpgradeEnvProgress(
                        env=e.env,
                        active_version=e.active_version,
                        revision=e.revision,
                        replaced=e.active_version == version,
                    )
                    for e in envs
                ],
            )
            logger.info(
                "upgrade_progress artifact=%s version=%s phase=%s "
                "continuable=%s next_env=%s",
                artifact_id, version, cls.phase.value,
                cls.continuable, next_env,
            )
            return view

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
