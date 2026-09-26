"""中途失败处置测试：部分环节失败时的回落、已生效环境保护与流水一致性。"""

from __future__ import annotations

from .conftest import envs, log_case, promote, register


def test_mid_cascade_failure_rolls_back_all_written_envs(client):
    register(client, "app", "1.0")
    req = dict(request_id="f1", artifact_id="app", version="1.0",
               target_env="prod", fail_at_env="staging")
    r = promote(client, **req)
    # 注入失败整体 500
    assert r.status_code == 500
    assert r.json()["error"]["code"] == "INJECTED_FAILURE"
    final = envs(client)
    log_case("级联中途失败（staging 注入）", req,
             {"before": {"dev": None, "staging": None, "prod": None}},
             final)
    # dev、staging 都已写入但必须回落到操作前；prod 从未写入
    assert final == {"dev": (None, 0), "staging": (None, 0),
                     "prod": (None, 0)}
    op = client.get("/operations/f1").json()
    assert op["status"] == "FAILED"
    steps = {(s["env"], s["rolled_back"]) for s in op["steps"]}
    assert ("dev", True) in steps and ("staging", True) in steps


def test_failure_at_last_env_rolls_back_prefix(client):
    register(client, "app", "1.0")
    req = dict(request_id="f2", artifact_id="app", version="1.0",
               target_env="prod", fail_at_env="prod")
    r = promote(client, **req)
    assert r.status_code == 500
    final = envs(client)
    log_case("最后一环节失败全部回落", req,
             {"fail_at": "prod"}, final)
    assert final == {"dev": (None, 0), "staging": (None, 0),
                     "prod": (None, 0)}


def test_failure_does_not_touch_unrelated_active_envs(client):
    """失败的是新版本 2.0 的晋级；环境上已生效的 1.0 必须保持生效。"""
    register(client, "app", "1.0")
    promote(client, request_id="ok1", artifact_id="app", version="1.0",
            target_env="staging")  # dev=1.0, staging=1.0
    register(client, "app", "2.0")
    # 2.0 晋级 prod：路径=[dev, staging, prod]（全是旧版本，会替换）
    # 在 staging 注入失败 -> dev 被写成 2.0 后回滚恢复为 1.0
    req = dict(request_id="f3", artifact_id="app", version="2.0",
               target_env="prod", fail_at_env="staging")
    r = promote(client, **req)
    assert r.status_code == 500
    final = envs(client)
    log_case("新版本失败，已生效旧版本保持", req,
             {"before": {"dev": "1.0", "staging": "1.0", "prod": None}},
             final)
    assert final == {"dev": ("1.0", 1), "staging": ("1.0", 1),
                     "prod": (None, 0)}


def test_retry_after_injected_failure_succeeds(client):
    register(client, "app", "1.0")
    req = dict(request_id="f4", artifact_id="app", version="1.0",
               target_env="prod", fail_at_env="staging")
    promote(client, **req)
    # 同 id、去掉注入后重试
    req.pop("fail_at_env")
    r = promote(client, **req)
    assert r.status_code == 200
    op = r.json()
    log_case("注入失败后同 id 续做成功", req,
             {"attempts": op["attempts"]}, envs(client))
    assert op["attempts"] == 2
    assert [s["env"] for s in op["steps"]] == ["dev", "staging", "prod"]
    assert envs(client) == {"dev": ("1.0", 1), "staging": ("1.0", 1),
                            "prod": ("1.0", 1)}


def test_no_half_state_after_precondition_rejection(client):
    """纯前置拒绝（构建未成功）不得写任何环境、不得产生半成品。"""
    register(client, "app", "1.0", build_status="FAILED")
    r = promote(client, request_id="f5", artifact_id="app",
                version="1.0", target_env="prod")
    assert r.status_code == 422
    final = envs(client)
    log_case("前置拒绝不产生写入",
             {"build": "FAILED", "target": "prod"}, {}, final)
    assert final == {"dev": (None, 0), "staging": (None, 0),
                     "prod": (None, 0)}
    op = client.get("/operations/f5").json()
    assert op["status"] == "FAILED"
    assert op["steps"] == []


def test_operation_ledger_records_every_attempt(client):
    """操作流水可观测：失败原因、每步、attempts 均可查。"""
    register(client, "app", "1.0")
    promote(client, request_id="l1", artifact_id="app", version="1.0",
            target_env="prod", fail_at_env="dev")
    promote(client, request_id="l1", artifact_id="app", version="1.0",
            target_env="prod")
    ops = client.get("/operations").json()
    assert any(o["request_id"] == "l1" and o["status"] == "SUCCESS"
               for o in ops)
    op = client.get("/operations/l1").json()
    log_case("流水记录可观测", {"request_id": "l1"},
             {"attempts": op["attempts"], "steps": len(op["steps"])},
             envs(client))
    assert op["attempts"] == 2
