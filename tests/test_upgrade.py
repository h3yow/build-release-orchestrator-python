"""分批替换升级测试：旧版本全量生效后逐环境换成新版本。

覆盖：主链路混合窗口、从混合状态点名高环境自动补齐、进度查询阶段与
可继续性、并发抢同一环境、同一步重复提交、升级中途失败回落，
以及原 promote 路径不退化。
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from .conftest import (
    envs,
    log_case,
    promote,
    register,
    upgrade,
    upgrade_status,
)

logger = logging.getLogger("orchestrator.test")


def _full_rollout(client, artifact: str, version: str):
    """前置：旧版本一次铺满全部环境。"""
    register(client, artifact, version)
    r = promote(client, request_id=f"full-{version}", artifact_id=artifact,
                version=version, target_env="prod")
    assert r.status_code == 200, r.text


def test_rolling_upgrade_env_by_env(client):
    """主链路：1.0 全量在跑 -> 2.0 逐环境替换 dev -> staging -> prod。

    每一步之后都是合法混合窗口：低环境已是新版本、高环境仍是旧版本；
    已换新的环境在后续步骤中不被重复写入（revision 不变）。
    """
    _full_rollout(client, "app", "1.0")
    register(client, "app", "2.0")
    before = envs(client)

    # 第 1 步：换 dev
    req1 = dict(request_id="u1", artifact_id="app", version="2.0",
                target_env="dev", expected_revision=1)
    r = upgrade(client, **req1)
    assert r.status_code == 200, r.text
    mid1 = envs(client)
    log_case("分批升级第 1 步：替换 dev", req1, before, mid1)
    assert mid1 == {"dev": ("2.0", 2), "staging": ("1.0", 1),
                    "prod": ("1.0", 1)}
    st = upgrade_status(client, "app", "2.0").json()
    assert st["phase"] == "IN_PROGRESS" and st["continuable"] is True

    # 第 2 步：观察期结束，换 staging
    req2 = dict(request_id="u2", artifact_id="app", version="2.0",
                target_env="staging", expected_revision=1)
    r = upgrade(client, **req2)
    assert r.status_code == 200, r.text
    mid2 = envs(client)
    log_case("分批升级第 2 步：替换 staging", req2, mid1, mid2)
    assert mid2 == {"dev": ("2.0", 2), "staging": ("2.0", 2),
                    "prod": ("1.0", 1)}

    # 第 3 步：换 prod，全量完成
    req3 = dict(request_id="u3", artifact_id="app", version="2.0",
                target_env="prod", expected_revision=1)
    r = upgrade(client, **req3)
    assert r.status_code == 200, r.text
    final = envs(client)
    log_case("分批升级第 3 步：替换 prod，全量完成", req3, mid2, final)
    assert final == {"dev": ("2.0", 2), "staging": ("2.0", 2),
                     "prod": ("2.0", 2)}
    st = upgrade_status(client, "app", "2.0").json()
    assert st["phase"] == "COMPLETED"
    # dev/staging 在第 3 步未被重复写入
    assert [s["env"] for s in r.json()["steps"]] == ["prod"]


def test_upgrade_from_mixed_state_fills_remaining_envs(client):
    """从混合窗口直接点名 prod：自动补齐 staging/prod，dev 不重复写。"""
    _full_rollout(client, "app", "1.0")
    register(client, "app", "2.0")
    upgrade(client, request_id="u1", artifact_id="app", version="2.0",
            target_env="dev")
    before = envs(client)
    assert before["dev"] == ("2.0", 2)

    req = dict(request_id="u2", artifact_id="app", version="2.0",
               target_env="prod", expected_revision=1)
    r = upgrade(client, **req)
    assert r.status_code == 200, r.text
    body = r.json()
    final = envs(client)
    log_case("混合状态下点名 prod 自动补齐剩余环境", req, before, final)
    assert [s["env"] for s in body["steps"]] == ["staging", "prod"]
    assert final == {"dev": ("2.0", 2), "staging": ("2.0", 2),
                     "prod": ("2.0", 2)}


def test_upgrade_status_phases_and_readonly(client):
    """进度查询：NOT_STARTED -> IN_PROGRESS -> COMPLETED，且查询不改状态。"""
    _full_rollout(client, "app", "1.0")
    register(client, "app", "2.0")

    st = upgrade_status(client, "app", "2.0").json()
    snapshot0 = envs(client)
    assert st["phase"] == "NOT_STARTED" and st["continuable"] is True
    assert [e["on_target_version"] for e in st["envs"]] == [False] * 3

    upgrade(client, request_id="u1", artifact_id="app", version="2.0",
            target_env="dev")
    st = upgrade_status(client, "app", "2.0").json()
    log_case("混合窗口进度查询", {"query": "status of 2.0"},
             {"dev": "2.0", "staging/prod": "1.0"}, st)
    assert st["phase"] == "IN_PROGRESS" and st["continuable"] is True
    assert st["blocking_reason"] is None
    assert [e["on_target_version"] for e in st["envs"]] == [True, False, False]
    # 查询是只读的：状态与查询前一致
    assert envs(client) == {**snapshot0, "dev": ("2.0", 2)}

    upgrade(client, request_id="u2", artifact_id="app", version="2.0",
            target_env="prod")
    st = upgrade_status(client, "app", "2.0").json()
    assert st["phase"] == "COMPLETED"
    assert [e["on_target_version"] for e in st["envs"]] == [True] * 3


def test_upgrade_status_blocked_when_interleaved(service):
    """交错断裂（dev 仍是旧版本、staging 被外部改成新版本）：
    查询要报"不可继续"，升级请求也要被明确拒绝。"""
    from orchestrator.errors import OrchestratorError
    from orchestrator.models import (
        PromoteRequest,
        RegisterVersionRequest,
        UpgradeRequest,
    )

    service.register_version(RegisterVersionRequest(
        artifact_id="app", version="1.0"), "r1")
    service.register_version(RegisterVersionRequest(
        artifact_id="app", version="2.0"), "r2")
    service.promote(PromoteRequest(
        request_id="p1", artifact_id="app", version="1.0", target_env="prod"))
    # 外部直接改写磁盘制造交错：dev=1.0, staging=2.0, prod=1.0
    service.store.get_env("staging").active_version = "2.0"
    service.store.persist()

    st = service.get_upgrade_status("app", "2.0")
    log_case("交错状态进度查询", {"query": "status of 2.0"},
             {"dev": "1.0", "staging": "2.0(外部写入)", "prod": "1.0"},
             {"phase": st.phase, "continuable": st.continuable,
              "reason": st.blocking_reason})
    assert st.phase == "IN_PROGRESS"
    assert st.continuable is False
    assert st.blocking_reason is not None

    req = dict(request_id="u1", artifact_id="app", version="2.0",
               target_env="prod")
    try:
        service.upgrade(UpgradeRequest(**req))
        raised = None
    except OrchestratorError as e:
        raised = e
    final = {e.env: (e.active_version, e.revision)
             for e in service.list_envs()}
    log_case("交错状态升级被拒", req, {"continuable": False},
             {"error": raised.code if raised else None, "final": final})
    assert raised is not None and raised.code == "INVALID_STATE"
    assert final == {"dev": ("1.0", 1), "staging": ("2.0", 1),
                     "prod": ("1.0", 1)}


def test_upgrade_rejects_empty_env_points_to_promote(client):
    """从空环境直接发起替换升级：无旧版本可替换，明确拒绝并指向 promote。"""
    register(client, "app", "1.0")
    req = dict(request_id="u1", artifact_id="app", version="1.0",
               target_env="dev")
    r = upgrade(client, **req)
    log_case("空环境发起替换升级被拒", req, {"dev": None}, envs(client))
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "PRECONDITION_FAILED"
    assert envs(client)["dev"] == (None, 0)


def test_reject_repeat_upgrade_when_fully_replaced(client):
    """全部环境都已是新版本后再次升级：ALREADY_ACTIVE。"""
    _full_rollout(client, "app", "1.0")
    register(client, "app", "2.0")
    upgrade(client, request_id="u1", artifact_id="app", version="2.0",
            target_env="prod")
    req = dict(request_id="u2", artifact_id="app", version="2.0",
               target_env="prod")
    r = upgrade(client, **req)
    log_case("重复升级已全量换新的版本被拒", req, {"all": "2.0"}, envs(client))
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "ALREADY_ACTIVE"


def test_concurrent_upgrades_same_env_conflict_and_retry(client):
    """两个客户端持同一 expected_revision 抢同一个环境（不同新版本）：
    只允许一个成功，另一个 REVISION_CONFLICT，刷新状态后重试成功。"""
    _full_rollout(client, "app", "1.0")
    register(client, "app", "2.0")
    register(client, "app", "3.0")

    results: list = []
    barrier = threading.Barrier(2)

    def worker(version: str, request_id: str):
        barrier.wait()
        req = dict(request_id=request_id, artifact_id="app",
                   version=version, target_env="dev", expected_revision=1)
        resp = upgrade(client, **req)
        results.append((version, resp.status_code,
                        resp.json().get("error", {}).get("code")))

    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = [ex.submit(worker, "2.0", "c1"),
                ex.submit(worker, "3.0", "c2")]
        [f.result() for f in futs]

    mid = envs(client)
    log_case("并发替换升级抢同一环境",
             {"requests": [
                 {"version": "2.0", "expected_revision": 1},
                 {"version": "3.0", "expected_revision": 1}]},
             {"rule": "仅一方 CAS 成功"},
             {"results": results, "mid": mid})
    codes = sorted(r[2] or "SUCCESS" for r in results)
    assert codes == ["REVISION_CONFLICT", "SUCCESS"], results
    assert mid["dev"][1] == 2  # 只发生一次写入
    loser = "3.0" if mid["dev"][0] == "2.0" else "2.0"

    # 冲突方读取最新 revision 后安全重试
    cur = {e["env"]: e["revision"] for e in client.get("/envs").json()}
    req = dict(request_id="retry", artifact_id="app", version=loser,
               target_env="dev", expected_revision=cur["dev"])
    r = upgrade(client, **req)
    final = envs(client)
    log_case("冲突方刷新 revision 重试", req, mid, final)
    assert r.status_code == 200, r.text
    assert final["dev"] == (loser, 3)


def test_concurrent_duplicate_same_step_idempotent(client):
    """同一步被两个客户端用同一 request_id 重复提交：
    幂等重放，dev 只写入一次。"""
    _full_rollout(client, "app", "1.0")
    register(client, "app", "2.0")

    results: list = []
    barrier = threading.Barrier(2)

    def worker():
        barrier.wait()
        resp = upgrade(client, request_id="dup", artifact_id="app",
                       version="2.0", target_env="dev", expected_revision=1)
        results.append(resp.status_code)

    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(worker)
        f2 = ex.submit(worker)
        f1.result(); f2.result()

    final = envs(client)
    log_case("同一步重复提交（幂等）",
             {"request_id": "dup", "count": 2},
             {"dev": ("1.0", 1)},
             {"statuses": sorted(results), "final": final})
    assert sorted(results) == [200, 200]
    assert final["dev"] == ("2.0", 2)  # 只 +1 一次


def test_upgrade_mid_failure_rolls_back_and_preserves_old(client):
    """升级中途失败：本次已换新的环境回落到操作前可见状态，
    之前已生效的旧版本不受影响；同 request_id 重试可续做成功。"""
    _full_rollout(client, "app", "1.0")
    register(client, "app", "2.0")
    before = envs(client)

    req = dict(request_id="u1", artifact_id="app", version="2.0",
               target_env="prod", fail_at_env="staging")
    r = upgrade(client, **req)
    assert r.status_code == 500
    assert r.json()["error"]["code"] == "INJECTED_FAILURE"
    final = envs(client)
    log_case("升级中途失败（staging 注入）", req, before, final)
    # dev/staging 已写入但全部回落到操作前；prod 从未写入
    assert final == before == {"dev": ("1.0", 1), "staging": ("1.0", 1),
                               "prod": ("1.0", 1)}
    op = client.get("/operations/u1").json()
    assert op["status"] == "FAILED"
    steps = {(s["env"], s["rolled_back"]) for s in op["steps"]}
    assert ("dev", True) in steps and ("staging", True) in steps

    # 同 request_id 去掉注入重试：续做成功，attempts 累加
    req2 = dict(request_id="u1", artifact_id="app", version="2.0",
                target_env="prod")
    r = upgrade(client, **req2)
    assert r.status_code == 200, r.text
    body = r.json()
    log_case("失败后同 id 续做成功", req2, {"attempts_before": 1},
             {"attempts": body["attempts"], "final": envs(client)})
    assert body["attempts"] == 2
    assert envs(client) == {"dev": ("2.0", 2), "staging": ("2.0", 2),
                            "prod": ("2.0", 2)}


def test_upgrade_idempotent_replay(client):
    """成功的升级请求重复提交：原样返回，不重复写状态。"""
    _full_rollout(client, "app", "1.0")
    register(client, "app", "2.0")
    req = dict(request_id="u1", artifact_id="app", version="2.0",
               target_env="dev")
    r1 = upgrade(client, **req)
    assert r1.status_code == 200
    r2 = upgrade(client, **req)
    assert r2.status_code == 200
    final = envs(client)
    log_case("成功升级幂等重放", req, {"dev": ("1.0", 1)}, final)
    assert r2.json()["request_id"] == "u1"
    assert final["dev"] == ("2.0", 2)  # revision 只 +1 一次


def test_concurrent_query_during_upgrade_sees_consistent_prefix(client):
    """查询与升级并发：任何一次查询看到的"已换新"环境都构成前缀，
    不会看到跳级或半写的视图。"""
    _full_rollout(client, "app", "1.0")
    register(client, "app", "2.0")

    observations: list = []
    stop = threading.Event()

    def upgrader():
        for target in ("dev", "staging", "prod"):
            upgrade(client, request_id=f"u-{target}", artifact_id="app",
                    version="2.0", target_env=target)

    def watcher():
        while not stop.is_set():
            st = upgrade_status(client, "app", "2.0").json()
            flags = [e["on_target_version"] for e in st["envs"]]
            observations.append(flags)

    t = threading.Thread(target=watcher)
    t.start()
    upgrader()
    stop.set()
    t.join()

    log_case("查询与升级并发", {"upgrade": "dev->staging->prod"},
             {"watcher": "并发轮询 status"},
             {"observations": len(observations),
              "final": envs(client)})
    assert observations, "应至少观察到一次"
    for flags in observations:
        # 已换新的环境必须是连续前缀：False 之后不得再出现 True
        assert flags == sorted(flags, reverse=True), flags
    assert envs(client) == {"dev": ("2.0", 2), "staging": ("2.0", 2),
                            "prod": ("2.0", 2)}


def test_promote_from_empty_not_regressed(client):
    """原路径不退化：新版本仍可从空环境一次铺满；
    且 promote 的交错防御仍然生效——混合窗口下想经 promote 把新版本
    续推到高环境仍被拒（必须走 upgrade），两条路径语义保持分离。"""
    register(client, "app", "1.0")
    req = dict(request_id="p1", artifact_id="app", version="1.0",
               target_env="prod")
    r = promote(client, **req)
    assert r.status_code == 200, r.text
    assert [s["env"] for s in r.json()["steps"]] == ["dev", "staging", "prod"]

    # 进入混合窗口：dev=2.0, staging/prod=1.0
    register(client, "app", "2.0")
    upgrade(client, request_id="u1", artifact_id="app", version="2.0",
            target_env="dev")
    before = envs(client)
    # 2.0 经 promote 续推到 prod：其链路在 staging 被 1.0 隔断，仍被拒
    req2 = dict(request_id="p2", artifact_id="app", version="2.0",
                target_env="prod")
    r = promote(client, **req2)
    final = envs(client)
    log_case("混合窗口下 promote 交错防御不退化", req2, before,
             {"status": r.status_code, "final": final})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "INVALID_STATE"
    assert final == before
