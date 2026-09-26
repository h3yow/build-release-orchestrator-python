"""晋级 / 回滚规则测试：合法路径与各类可区分的拒绝原因。"""

from __future__ import annotations

import logging

from .conftest import envs, log_case, promote, register, rollback

logger = logging.getLogger("orchestrator.test")


def test_cascade_promotion_to_prod(client):
    register(client, "app", "1.0")
    req = dict(request_id="p1", artifact_id="app", version="1.0",
               target_env="prod")
    resp = promote(client, **req)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "SUCCESS"
    assert [s["env"] for s in body["steps"]] == ["dev", "staging", "prod"]
    final = envs(client)
    log_case("级联晋级 dev->staging->prod", req,
             {"expected_revision": None}, final)
    assert final == {"dev": ("1.0", 1), "staging": ("1.0", 1),
                     "prod": ("1.0", 1)}


def test_promote_step_by_step(client):
    register(client, "app", "1.0")
    for target in ("dev", "staging", "prod"):
        r = promote(client, request_id=f"p-{target}", artifact_id="app",
                    version="1.0", target_env=target)
        assert r.status_code == 200, r.text
    final = envs(client)
    assert final == {"dev": ("1.0", 1), "staging": ("1.0", 1),
                     "prod": ("1.0", 1)}


def test_reject_failed_build(client):
    register(client, "app", "1.0", build_status="FAILED")
    req = dict(request_id="p1", artifact_id="app", version="1.0",
               target_env="dev")
    r = promote(client, **req)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "PRECONDITION_FAILED"
    log_case("构建失败版本晋级被拒", req,
             {"build_status": "FAILED"}, envs(client))
    assert envs(client)["dev"] == (None, 0)


def test_reject_previous_env_blocked_by_other_version(service):
    """防御性校验：构造交错状态 dev=1.0, staging=2.0（2.0 未经 dev 前置），
    此时 2.0 想续做到 prod，必须在写 prod 前的前置检查处被拒绝
    （上一环境 staging 虽然是 2.0，但 dev 是别的版本——
    本系统的交错仅可能由外部直接改写磁盘造成）。"""
    from orchestrator.models import (
        RegisterVersionRequest, PromoteRequest, EnvState)
    service.register_version(RegisterVersionRequest(
        artifact_id="app", version="2.0"), "r1")
    service.register_version(RegisterVersionRequest(
        artifact_id="app", version="1.0"), "r2")
    # 正常：1.0 全量到 prod，再把 dev 换成 2.0 -> dev=2.0,staging=1.0,prod=1.0
    service.promote(PromoteRequest(
        request_id="p1", artifact_id="app", version="1.0",
        target_env="prod"))
    service.promote(PromoteRequest(
        request_id="p2", artifact_id="app", version="2.0",
        target_env="dev", expected_revision=1))
    # 直接把 staging 改写成 2.0（模拟外部交错写盘）
    staging = service.store.get_env("staging")
    staging.active_version = "2.0"
    service.store.persist()
    # 2.0 续做到 prod：first_self=0(dev)，路径=[staging, prod]
    # staging 已被本版本占用但不在路径；写 prod 时 prev=staging=2.0 -> 通过。
    # 因此需要前置链路检查：dev=2.0, staging=2.0 连续，实际会成功。
    # 真正非法的是 dev=别的版本而 staging=2.0：
    service.store.get_env("dev").active_version = "1.0"
    service.store.persist()
    req = dict(request_id="p3", artifact_id="app", version="2.0",
               target_env="prod")
    from orchestrator.errors import OrchestratorError
    try:
        service.promote(PromoteRequest(**req))
        raised = None
    except OrchestratorError as e:
        raised = e
    log_case("交错状态续做旧版本被拒", req,
             {"dev": "1.0", "staging": "2.0", "prod": "1.0"},
             {e.env: (e.active_version, e.revision)
              for e in service.list_envs()})
    assert raised is not None
    assert raised.code in ("INVALID_STATE", "PRECONDITION_FAILED")


def test_reject_already_active(client):
    register(client, "app", "1.0")
    promote(client, request_id="p1", artifact_id="app", version="1.0",
            target_env="dev")
    req = dict(request_id="p2", artifact_id="app", version="1.0",
               target_env="dev")
    r = promote(client, **req)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "ALREADY_ACTIVE"
    log_case("重复晋级已生效版本被拒", req, {"dev": "1.0"}, envs(client))


def test_reject_unknown_artifact_version_env(client):
    r1 = promote(client, request_id="e1", artifact_id="nope", version="1",
                 target_env="dev")
    assert r1.status_code == 404
    assert r1.json()["error"]["code"] == "ARTIFACT_NOT_FOUND"

    register(client, "app", "1.0")
    r2 = promote(client, request_id="e2", artifact_id="app", version="9.9",
                 target_env="dev")
    assert r2.status_code == 404
    assert r2.json()["error"]["code"] == "VERSION_NOT_FOUND"

    r3 = promote(client, request_id="e3", artifact_id="app", version="1.0",
                 target_env="qa")
    assert r3.status_code == 404
    assert r3.json()["error"]["code"] == "ENV_NOT_FOUND"
    log_case("产物/版本/环境不存在分别报错",
             {"artifact": "nope/9.9", "env": "qa"},
             {}, envs(client))


def test_rollback_to_previous_version(client):
    register(client, "app", "1.0")
    register(client, "app", "2.0")
    promote(client, request_id="p1", artifact_id="app", version="1.0",
            target_env="dev")
    promote(client, request_id="p2", artifact_id="app", version="2.0",
            target_env="dev", expected_revision=1)
    assert envs(client)["dev"] == ("2.0", 2)

    req = dict(request_id="rb1", artifact_id="app", env="dev",
               to_version="1.0", expected_revision=2)
    r = rollback(client, **req)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["from_version"] == "2.0"
    assert body["to_version"] == "1.0"
    final = envs(client)
    log_case("回滚到历史版本", req, {"expected_revision": 2}, final)
    assert final["dev"] == ("1.0", 3)
    assert final["staging"] == (None, 0)


def test_rollback_withdraw(client):
    register(client, "app", "1.0")
    promote(client, request_id="p1", artifact_id="app", version="1.0",
            target_env="staging")
    req = dict(request_id="rb1", artifact_id="app", env="staging")
    r = rollback(client, **req)
    assert r.status_code == 200
    final = envs(client)
    log_case("撤下当前生效版本", req, {"staging": "1.0"}, final)
    assert final["staging"] == (None, 2)
    assert final["dev"] == ("1.0", 1)


def test_reject_rollback_empty_env(client):
    register(client, "app", "1.0")
    r = rollback(client, request_id="rb1", artifact_id="app", env="prod")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "NOTHING_TO_ROLLBACK"


def test_reject_rollback_to_never_deployed(client):
    register(client, "app", "1.0")
    register(client, "app", "2.0")
    promote(client, request_id="p1", artifact_id="app", version="1.0",
            target_env="dev")
    req = dict(request_id="rb1", artifact_id="app", env="dev",
               to_version="2.0")
    r = rollback(client, **req)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "INVALID_STATE"
    log_case("回滚到从未部署版本被拒", req,
             {"2.0 deployed": False}, envs(client))
    assert envs(client)["dev"] == ("1.0", 1)


def test_continue_after_rollback_re_enters_from_lowest_gap(client):
    """续做：晋级中途失败在低环境留下缺口后，重试只补缺口、不重复生效环境。"""
    register(client, "app", "2.0")
    # 全量到 prod
    promote(client, request_id="p1", artifact_id="app", version="2.0",
            target_env="prod")
    # dev 回滚撤下，产生 dev=None, staging=2.0, prod=2.0 的合法部分状态
    r = rollback(client, request_id="rb1", artifact_id="app", env="dev")
    assert r.status_code == 200
    before = envs(client)
    assert before == {"dev": (None, 2), "staging": ("2.0", 1),
                      "prod": ("2.0", 1)}
    # 续做（目标点名 prod）：路径只包含缺口 dev，staging/prod 不重复写
    req = dict(request_id="p2", artifact_id="app", version="2.0",
               target_env="prod")
    r = promote(client, **req)
    assert r.status_code == 200, r.text
    assert [s["env"] for s in r.json()["steps"]] == ["dev"]
    final = envs(client)
    log_case("回滚后续做仅重走缺口环境", req, before, final)
    assert final == {"dev": ("2.0", 3), "staging": ("2.0", 1),
                     "prod": ("2.0", 1)}


def test_reject_starting_from_middle_when_dev_is_other_version(service):
    """交错防御：dev=1.0 而 staging 被外部改成 2.0 时，
    2.0 点名 prod（路径=[staging, prod]）必须因上一环境(staging 的前一
    环境 dev)不是本版本而拒绝，不得从中间环境起步覆盖。"""
    from orchestrator.errors import OrchestratorError
    from orchestrator.models import PromoteRequest, RegisterVersionRequest

    service.register_version(RegisterVersionRequest(
        artifact_id="app", version="1.0"), "r1")
    service.register_version(RegisterVersionRequest(
        artifact_id="app", version="2.0"), "r2")
    service.promote(PromoteRequest(
        request_id="p1", artifact_id="app", version="1.0",
        target_env="prod"))
    # 外部直接改写磁盘制造交错：dev=1.0, staging=2.0, prod=1.0
    staging = service.store.get_env("staging")
    staging.active_version = "2.0"
    service.store.persist()

    req = PromoteRequest(request_id="p2", artifact_id="app",
                         version="2.0", target_env="prod")
    try:
        service.promote(req)
        raised = None
    except OrchestratorError as e:
        raised = e
    final = {e.env: (e.active_version, e.revision)
             for e in service.list_envs()}
    log_case("交错状态禁止从中间环境起步",
             {"request": "promote 2.0 -> prod"},
             {"dev": "1.0", "staging": "2.0(外部写入)", "prod": "1.0"},
             {"error": raised.code if raised else None, "final": final})
    assert raised is not None
    assert raised.code in ("INVALID_STATE", "PRECONDITION_FAILED")
    # staging/prod 未被本次请求改动
    assert final["staging"] == ("2.0", 1)
    assert final["prod"] == ("1.0", 1)
