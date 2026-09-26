"""编排规则：晋级链、可区分的拒绝原因、回滚回落。"""
import logging

from app.models import Artifact, EnvName, EnvState, ErrorCode, OpStatus

from .conftest import log_envs, log_op

log = logging.getLogger("test")


def _add(orch, version, status="succeeded"):
    orch.store.put_artifact(Artifact(version, status))


def test_promote_chain_happy_path(orch):
    _add(orch, "1.0.0")
    for env, expected in [(EnvName.DEV, None), (EnvName.STAGING, None), (EnvName.PROD, None)]:
        req = {"version": "1.0.0", "env": env.value, "expected": expected}
        r = orch.promote("1.0.0", env, expected)
        log_op(log, "promote", req, r)
        assert r.ok and r.op.status == OpStatus.COMMITTED
    log_envs(log, orch)
    assert orch.env_status() == {"dev": "1.0.0", "staging": "1.0.0", "prod": "1.0.0"}
    for env in EnvName:
        assert orch.version_env_state("1.0.0", env) == EnvState.ACTIVE


def test_reject_reasons_are_distinguishable(orch):
    # 版本不存在
    r = orch.promote("ghost", EnvName.DEV, None)
    log_op(log, "promote", {"version": "ghost", "env": "dev"}, r)
    assert r.error == ErrorCode.ARTIFACT_NOT_FOUND

    # 构建未成功
    _add(orch, "0.9.0", status="failed")
    r = orch.promote("0.9.0", EnvName.DEV, None)
    log_op(log, "promote", {"version": "0.9.0", "env": "dev"}, r)
    assert r.error == ErrorCode.BUILD_NOT_SUCCEEDED

    # 前置环境未生效（跨级晋级）
    _add(orch, "1.0.0")
    r = orch.promote("1.0.0", EnvName.STAGING, None)
    log_op(log, "promote", {"version": "1.0.0", "env": "staging"}, r)
    assert r.error == ErrorCode.PRECONDITION_NOT_MET

    # 已在目标环境生效，重复晋级属非法迁移
    assert orch.promote("1.0.0", EnvName.DEV, None).ok
    r = orch.promote("1.0.0", EnvName.DEV, "1.0.0")
    log_op(log, "promote", {"version": "1.0.0", "env": "dev (again)"}, r)
    assert r.error == ErrorCode.ILLEGAL_TRANSITION

    # 回滚未生效的版本
    r = orch.rollback("1.0.0", EnvName.STAGING, None)
    log_op(log, "rollback", {"version": "1.0.0", "env": "staging"}, r)
    assert r.error == ErrorCode.ENV_NOT_ACTIVE

    # 所有拒绝都必须落流水，不得静默成功
    rejected = [op for op in orch.store.list_ops() if op.status == OpStatus.REJECTED]
    assert len(rejected) == 5
    log_envs(log, orch)
    assert orch.env_status() == {"dev": "1.0.0", "staging": None, "prod": None}


def test_rollback_restores_previous_version(orch):
    _add(orch, "1.0.0")
    _add(orch, "1.1.0")
    assert orch.promote("1.0.0", EnvName.DEV, None).ok
    assert orch.promote("1.1.0", EnvName.DEV, "1.0.0").ok

    req = {"version": "1.1.0", "env": "dev", "expected": "1.1.0"}
    r = orch.rollback("1.1.0", EnvName.DEV, "1.1.0")
    log_op(log, "rollback", req, r)
    assert r.ok and r.op.result == "1.0.0"  # 基于操作流水回落到上一生效版本
    log_envs(log, orch)
    assert orch.store.get_active(EnvName.DEV) == "1.0.0"
    assert orch.version_env_state("1.1.0", EnvName.DEV) == EnvState.ROLLED_BACK

    # 再回滚 1.0.0 -> 无生效版本
    r = orch.rollback("1.0.0", EnvName.DEV, "1.0.0")
    log_op(log, "rollback", {"version": "1.0.0", "env": "dev"}, r)
    assert r.ok and r.op.result is None
    assert orch.store.get_active(EnvName.DEV) is None
