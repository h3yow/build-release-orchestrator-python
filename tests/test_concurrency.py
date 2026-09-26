"""并发冲突、重复提交与中途失败。"""
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from app.models import Artifact, EnvName, ErrorCode, OpStatus

from .conftest import log_envs, log_op

log = logging.getLogger("test")


def _add(orch, version, status="succeeded"):
    orch.store.put_artifact(Artifact(version, status))


def test_concurrent_promotes_same_env_no_overwrite(orch):
    """两个客户端同时向同一环境晋级不同版本：恰好一个成功，
    另一个收到可安全重试的前置条件冲突，环境状态不得跳变或覆盖。"""
    _add(orch, "1.0.0")
    _add(orch, "2.0.0")
    barrier = threading.Barrier(2)

    def do(version):
        barrier.wait()  # 强制交错
        req = {"version": version, "env": "dev", "expected": None}
        r = orch.promote(version, EnvName.DEV, None)
        log_op(log, "promote", req, r)
        return r

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(do, ["1.0.0", "2.0.0"]))

    committed = [r for r in results if r.ok]
    conflicts = [r for r in results if r.error == ErrorCode.PRECONDITION_CONFLICT]
    assert len(committed) == 1 and len(conflicts) == 1

    winner = committed[0].op.version
    log_envs(log, orch)
    assert orch.store.get_active(EnvName.DEV) == winner

    # 冲突方可安全重试：携带最新前置条件后成功
    loser = conflicts[0].op.version
    req = {"version": loser, "env": "dev", "expected": winner}
    r = orch.promote(loser, EnvName.DEV, winner)
    log_op(log, "promote(retry)", req, r)
    assert r.ok
    log_envs(log, orch)
    assert orch.store.get_active(EnvName.DEV) == loser


def test_concurrent_promote_and_rollback_consistent(orch):
    """晋级与回滚交错：任一时刻环境状态都与操作流水一致，无半成品。"""
    _add(orch, "1.0.0")
    _add(orch, "1.1.0")
    assert orch.promote("1.0.0", EnvName.DEV, None).ok

    barrier = threading.Barrier(2)

    def promote_next():
        barrier.wait()
        r = orch.promote("1.1.0", EnvName.DEV, "1.0.0")
        log_op(log, "promote", {"version": "1.1.0", "expected": "1.0.0"}, r)
        return r

    def rollback_cur():
        barrier.wait()
        r = orch.rollback("1.0.0", EnvName.DEV, "1.0.0")
        log_op(log, "rollback", {"version": "1.0.0", "expected": "1.0.0"}, r)
        return r

    with ThreadPoolExecutor(2) as pool:
        f1 = pool.submit(promote_next)
        f2 = pool.submit(rollback_cur)
        r_promote, r_rollback = f1.result(), f2.result()

    # 两个操作基于同一前置版本 "1.0.0"，互斥：恰一个成功
    assert (r_promote.ok) != (r_rollback.ok)
    final = orch.store.get_active(EnvName.DEV)
    committed = [op for op in orch.store.list_ops() if op.status == OpStatus.COMMITTED]
    log_envs(log, orch)
    # 最终状态必须等于最后一条已提交操作的结果，不得是中间态
    assert final == committed[-1].result
    assert final in (None, "1.0.0", "1.1.0")


def test_duplicate_request_id_is_idempotent(orch):
    """同一 request_id 重复提交：返回同一操作结果，状态只应用一次。"""
    _add(orch, "1.0.0")
    req = {"version": "1.0.0", "env": "dev", "expected": None, "request_id": "req-42"}
    r1 = orch.promote("1.0.0", EnvName.DEV, None, request_id="req-42")
    log_op(log, "promote", req, r1)

    # 并发重复提交同一幂等键
    barrier = threading.Barrier(5)

    def dup():
        barrier.wait()
        return orch.promote("1.0.0", EnvName.DEV, None, request_id="req-42")

    with ThreadPoolExecutor(5) as pool:
        dups = list(pool.map(lambda _: dup(), range(5)))

    for r in dups:
        assert r.ok and r.op.op_id == r1.op.op_id
    log_envs(log, orch)
    assert orch.store.get_active(EnvName.DEV) == "1.0.0"
    committed = [op for op in orch.store.list_ops()
                 if op.status == OpStatus.COMMITTED and op.request_id == "req-42"]
    assert len(committed) == 1  # 流水里只有一条，未重复执行


def test_midway_failure_restores_pre_op_state(orch):
    """晋级中途失败：环境回落到操作前可见状态，流水记录 FAILED，
    已生效环境保持生效，失败原因可区分。"""
    _add(orch, "1.0.0")
    _add(orch, "1.1.0")
    assert orch.promote("1.0.0", EnvName.DEV, None).ok
    assert orch.promote("1.0.0", EnvName.STAGING, None).ok
    # 1.1.0 须先在前置环境 dev 生效，才允许晋级 staging
    assert orch.promote("1.1.0", EnvName.DEV, "1.0.0").ok

    # staging 晋级 1.1.0 时模拟下游环节失败
    req = {"version": "1.1.0", "env": "staging", "expected": "1.0.0"}
    r = orch.promote("1.1.0", EnvName.STAGING, "1.0.0",
                     fail_hook=lambda env: env == EnvName.STAGING)
    log_op(log, "promote(fail)", req, r)
    assert not r.ok and r.op.status == OpStatus.FAILED
    assert r.op.reason == "downstream_step_failed"

    log_envs(log, orch)
    # 失败环境回落到操作前状态，其他环境不受影响
    assert orch.store.get_active(EnvName.STAGING) == "1.0.0"
    assert orch.store.get_active(EnvName.DEV) == "1.1.0"
    # 流水可解释：FAILED 记录的 result 即回落后的可见状态
    failed = [op for op in orch.store.list_ops() if op.status == OpStatus.FAILED]
    assert len(failed) == 1 and failed[0].result == "1.0.0"

    # 失败后可续做：同一前置条件重试成功
    r = orch.promote("1.1.0", EnvName.STAGING, "1.0.0")
    log_op(log, "promote(resume)", req, r)
    assert r.ok
    log_envs(log, orch)
    assert orch.store.get_active(EnvName.STAGING) == "1.1.0"
