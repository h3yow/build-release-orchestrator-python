"""重复提交测试：相同 request_id 的幂等重放与请求体冲突拒绝。"""

from __future__ import annotations

from .conftest import envs, log_case, promote, register, rollback


def test_duplicate_promotion_same_request_id_returns_same_result(client):
    """成功请求重复提交：返回同一结果、状态只变化一次（revision 不增加）。"""
    register(client, "app", "1.0")
    req = dict(request_id="dup-1", artifact_id="app", version="1.0",
               target_env="dev")
    r1 = promote(client, **req)
    assert r1.status_code == 200
    rev_after_first = envs(client)["dev"][1]

    r2 = promote(client, **req)
    assert r2.status_code == 200
    assert r2.json()["request_id"] == "dup-1"
    r3 = promote(client, **req)
    assert r3.status_code == 200

    final = envs(client)
    log_case("成功请求重复提交三次", req,
             {"revision after first": rev_after_first}, final)
    assert final["dev"] == ("1.0", 1)  # 仅一次写入


def test_duplicate_rollback_same_request_id(client):
    register(client, "app", "1.0")
    promote(client, request_id="p1", artifact_id="app", version="1.0",
            target_env="dev")
    req = dict(request_id="dup-rb", artifact_id="app", env="dev")
    r1 = rollback(client, **req)
    r2 = rollback(client, **req)
    assert r1.status_code == r2.status_code == 200
    # 幂等返回同一结果（撤下），不会第二次 +revision
    assert r2.json()["to_version"] is None
    final = envs(client)
    log_case("回滚请求重复提交", req, {"dev": "1.0"}, final)
    assert final["dev"] == (None, 2)


def test_same_request_id_with_different_body_rejected(client):
    """request_id 被复用于语义不同的请求，必须明确拒绝，不得执行第二个。"""
    register(client, "app", "1.0")
    register(client, "app", "2.0")
    r1 = promote(client, request_id="x1", artifact_id="app",
                 version="1.0", target_env="dev")
    assert r1.status_code == 200
    # 同 request_id 但换版本
    r2 = promote(client, request_id="x1", artifact_id="app",
                 version="2.0", target_env="dev")
    assert r2.status_code == 409
    assert r2.json()["error"]["code"] == "DUPLICATE_REQUEST_MISMATCH"
    log_case("request_id 复用但请求体冲突",
             {"request_id": "x1"},
             {"first": "1.0", "second": "2.0"}, envs(client))
    assert envs(client)["dev"] == ("1.0", 1)


def test_failed_request_retry_with_same_id_increments_attempts(client):
    """前置冲突的失败请求，用同一 request_id 在条件满足后重试可成功。"""
    register(client, "app", "1.0")
    req = dict(request_id="retry-1", artifact_id="app", version="1.0",
               target_env="dev", expected_revision=5)
    r1 = promote(client, **req)
    assert r1.status_code == 409
    assert r1.json()["error"]["code"] == "REVISION_CONFLICT"
    # 条件修正后同 id 重试
    req["expected_revision"] = 0
    r2 = promote(client, **req)
    assert r2.status_code == 200, r2.text
    op = client.get("/operations/retry-1").json()
    log_case("失败请求同 id 重试成功", req,
             {"attempts": op["attempts"]}, envs(client))
    assert op["attempts"] == 2
    assert op["status"] == "SUCCESS"
