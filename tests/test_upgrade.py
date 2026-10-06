"""分批替换升级测试：旧版本全量生效后逐环境换成新版本。

覆盖：主链路逐环境替换、中间混合窗口、从混合状态点名补齐、
进度查询（只读）、基线/断裂状态拒绝、并发抢同一环境、
同一步重复提交、升级中途失败补偿。
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from .conftest import (
    envs, log_case, promote, register, rollback, upgrade, upgrade_progress,
)

logger = logging.getLogger("orchestrator.test")


def _full_deploy(client, artifact: str, version: str) -> None:
    """前置：把 version 一次铺满全部环境（老版本全量生效）。"""
    register(client, artifact, version)
    r = promote(client, request_id=f"full-{version}", artifact_id=artifact,
                version=version, target_env="prod")
    assert r.status_code == 200, r.text


def test_rolling_upgrade_main_flow_env_by_env(client):
    """主链路：1.0 全量生效后，2.0 一次只推一级，逐环境替换到 prod。"""
    _full_deploy(client, "app", "1.0")
    register(client, "app", "2.0")
    before = envs(client)
    assert before == {"dev": ("1.0", 1), "staging": ("1.0", 1),
                      "prod": ("1.0", 1)}

    # 未开始：进度查询为 NOT_STARTED 且可继续
    p = upgrade_progress(client, "app", "2.0")
    assert (p["phase"], p["continuable"], p["next_env"]) == (
        "NOT_STARTED", True, "dev")
    assert p["baseline_version"] == "1.0"

    # 第 1 步：只换 dev（target_env 缺省 = 只推下一级）
    req1 = dict(request_id="u1", artifact_id="app", version="2.0")
    r = upgrade(client, **req1)
    assert r.status_code == 200, r.text
    assert [s["env"] for s in r.json()["steps"]] == ["dev"]
    mid1 = envs(client)
    log_case("替换升级第 1 步：只换 dev", req1, before, mid1)
    # 中间混合窗口：dev 已换新，staging/prod 仍是旧版本，且未被动过
    assert mid1 == {"dev": ("2.0", 2), "staging": ("1.0", 1),
                    "prod": ("1.0", 1)}

    # 混合窗口是正常进行中状态：REPLACING 且可继续
    p = upgrade_progress(client, "app", "2.0")
    log_case("混合窗口进度查询", {"query": "2.0"}, mid1, p)
    assert (p["phase"], p["continuable"], p["next_env"]) == (
        "REPLACING", True, "staging")
    assert [(e["env"], e["replaced"]) for e in p["envs"]] == [
        ("dev", True), ("staging", False), ("prod", False)]

    # 第 2 步：观察期结束，换 staging
    req2 = dict(request_id="u2", artifact_id="app", version="2.0")
    r = upgrade(client, **req2)
    assert r.status_code == 200, r.text
    assert [s["env"] for s in r.json()["steps"]] == ["staging"]
    mid2 = envs(client)
    assert mid2 == {"dev": ("2.0", 2), "staging": ("2.0", 2),
                    "prod": ("1.0", 1)}

    # 第 3 步：换 prod，全量完成
    req3 = dict(request_id="u3", artifact_id="app", version="2.0")
    r = upgrade(client, **req3)
    assert r.status_code == 200, r.text
    final = envs(client)
    log_case("替换升级完成", req3, mid2, final)
    assert final == {"dev": ("2.0", 2), "staging": ("2.0", 2),
                     "prod": ("2.0", 2)}
    p = upgrade_progress(client, "app", "2.0")
    assert p["phase"] == "COMPLETED"
    assert p["continuable"] is False

    # 已完成后再升级：明确拒绝，不重复写入
    r = upgrade(client, request_id="u4", artifact_id="app", version="2.0")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "ALREADY_ACTIVE"
    assert envs(client) == final


def test_continue_from_mixed_window_fill_to_named_env(client):
    """从混合窗口直接点名 prod：自动补齐 staging+prod，dev 不重复写入。"""
    _full_deploy(client, "app", "1.0")
    register(client, "app", "2.0")
    upgrade(client, request_id="u1", artifact_id="app", version="2.0")
    before = envs(client)
    assert before == {"dev": ("2.0", 2), "staging": ("1.0", 1),
                      "prod": ("1.0", 1)}

    req = dict(request_id="u2", artifact_id="app", version="2.0",
               target_env="prod")
    r = upgrade(client, **req)
    assert r.status_code == 200, r.text
    body = r.json()
    final = envs(client)
    log_case("混合窗口点名 prod 自动补齐", req, before, final)
    # 只写 staging/prod；dev 已是新版本，不重复写入、revision 不变
    assert [s["env"] for s in body["steps"]] == ["staging", "prod"]
    assert final == {"dev": ("2.0", 2), "staging": ("2.0", 2),
                     "prod": ("2.0", 2)}

    # 点名已经换好的环境：拒绝
    r = upgrade(client, request_id="u3", artifact_id="app", version="2.0",
                target_env="dev")
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "ALREADY_ACTIVE"


def test_progress_query_is_readonly(client):
    """进度查询本身不改任何环境状态：反复查询，revision 与版本保持不变。"""
    _full_deploy(client, "app", "1.0")
    register(client, "app", "2.0")
    upgrade(client, request_id="u1", artifact_id="app", version="2.0")
    before = envs(client)
    for _ in range(3):
        p = upgrade_progress(client, "app", "2.0")
        assert p["phase"] == "REPLACING"
    after = envs(client)
    log_case("进度查询只读", {"query": "upgrade-progress x3"}, before, after)
    assert after == before
    # 查询不产生操作流水
    ops = client.get("/operations").json()
    assert all(o["op_type"] != "QUERY" for o in ops)
    assert not any(o["request_id"].startswith("query") for o in ops)


def test_upgrade_requires_uniform_baseline(client):
    """基线不满足时拒绝：空环境、未铺满都不能走替换升级。"""
    register(client, "app", "1.0")
    # 空环境：首次铺满应走 /promotions
    req = dict(request_id="u1", artifact_id="app", version="1.0")
    r = upgrade(client, **req)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "UPGRADE_BLOCKED"
    log_case("空环境拒绝替换升级", req, {"envs": "all None"}, envs(client))

    # 只铺到 dev（未全量）：同样拒绝
    promote(client, request_id="p1", artifact_id="app", version="1.0",
            target_env="dev")
    register(client, "app", "2.0")
    req = dict(request_id="u2", artifact_id="app", version="2.0")
    r = upgrade(client, **req)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "UPGRADE_BLOCKED"
    final = envs(client)
    log_case("未铺满拒绝替换升级", req, {"dev": "1.0", "staging": None}, final)
    assert final == {"dev": ("1.0", 1), "staging": (None, 0),
                     "prod": (None, 0)}


def test_broken_window_distinguished_from_healthy_transition(client):
    """断裂状态与正常过渡可区分：混合窗口中把 staging 回滚撤下后，
    查询报 continuable=false，升级被 UPGRADE_BLOCKED 拒绝。"""
    _full_deploy(client, "app", "1.0")
    register(client, "app", "2.0")
    upgrade(client, request_id="u1", artifact_id="app", version="2.0",
            target_env="staging")
    before = envs(client)
    assert before == {"dev": ("2.0", 2), "staging": ("2.0", 2),
                      "prod": ("1.0", 1)}
    # 健康窗口：可继续
    p = upgrade_progress(client, "app", "2.0")
    assert (p["phase"], p["continuable"]) == ("REPLACING", True)

    # 外部干预：staging 被回滚撤下 -> 链路断裂
    r = rollback(client, request_id="rb1", artifact_id="app", env="staging")
    assert r.status_code == 200
    broken = envs(client)
    p = upgrade_progress(client, "app", "2.0")
    log_case("断裂窗口 vs 正常过渡", {"query": "2.0"}, broken, p)
    assert p["phase"] == "REPLACING"       # 新版本确实已出现
    assert p["continuable"] is False       # 但这个状态没法继续
    assert p["reason"]

    req = dict(request_id="u2", artifact_id="app", version="2.0")
    r = upgrade(client, **req)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "UPGRADE_BLOCKED"
    # 拒绝不产生任何写入
    assert envs(client) == broken


def test_concurrent_upgrades_race_same_env(client):
    """两个新版本同时抢 dev（都持 revision=1）：只允许一个成功；
    另一个到达时统一基线已被打破，被明确识别为 UPGRADE_BLOCKED（409），
    不得覆盖已换好的环境；拿最新状态重试仍被明确拒绝。"""
    _full_deploy(client, "app", "1.0")
    register(client, "app", "2.0")
    register(client, "app", "3.0")

    results: list = []
    barrier = threading.Barrier(2)

    def worker(version: str, request_id: str):
        barrier.wait()
        req = dict(request_id=request_id, artifact_id="app",
                   version=version, expected_revision=1)
        resp = upgrade(client, **req)
        results.append((version, resp.status_code,
                        resp.json().get("error", {}).get("code")))

    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = [ex.submit(worker, "2.0", "c1"), ex.submit(worker, "3.0", "c2")]
        [f.result() for f in futs]

    final = envs(client)
    log_case("并发替换升级抢同一环境",
             {"requests": [{"version": "2.0", "expected_revision": 1},
                           {"version": "3.0", "expected_revision": 1}]},
             {"rule": "仅一方成功，另一方基线已破被明确拒绝"},
             {"results": results, "final": final})
    codes = sorted(r[2] or "SUCCESS" for r in results)
    assert codes == ["SUCCESS", "UPGRADE_BLOCKED"], results
    # dev 只被写入一次；staging/prod 保持旧版本不动
    assert final["dev"][1] == 2
    winner = final["dev"][0]
    assert winner in ("2.0", "3.0")
    assert final["staging"] == ("1.0", 1)
    assert final["prod"] == ("1.0", 1)

    # 冲突方拿最新状态重试：基线已破，仍被明确识别为 UPGRADE_BLOCKED
    loser = "3.0" if winner == "2.0" else "2.0"
    cur = {e["env"]: e["revision"] for e in client.get("/envs").json()}
    req = dict(request_id="c-retry", artifact_id="app", version=loser,
               expected_revision=cur["dev"])
    r = upgrade(client, **req)
    log_case("冲突方刷新状态后重试", req, final, envs(client))
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "UPGRADE_BLOCKED"
    assert envs(client) == final  # 重试被拒不产生任何写入


def test_concurrent_upgrade_and_rollback_same_env(client):
    """混合窗口中：一个客户端续推 staging（升级），另一个回滚 staging，
    都持 revision=1：只允许一个成功；另一个被明确识别为冲突
    （REVISION_CONFLICT：revision 已变；或 UPGRADE_BLOCKED：回滚撤下
    导致替换链路断裂），最终 staging 的版本与 revision 自洽、不跳变。"""
    _full_deploy(client, "app", "1.0")
    register(client, "app", "2.0")
    upgrade(client, request_id="u1", artifact_id="app", version="2.0")
    before = envs(client)
    assert before["dev"] == ("2.0", 2)
    assert before["staging"] == ("1.0", 1)

    results: list = []
    barrier = threading.Barrier(2)

    def upgrade_worker():
        barrier.wait()
        resp = upgrade(client, request_id="cu", artifact_id="app",
                       version="2.0", expected_revision=1)
        results.append(("upgrade", resp.status_code,
                        resp.json().get("error", {}).get("code")))

    def rollback_worker():
        barrier.wait()
        resp = rollback(client, request_id="cr", artifact_id="app",
                        env="staging", expected_revision=1)
        results.append(("rollback", resp.status_code,
                        resp.json().get("error", {}).get("code")))

    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(upgrade_worker)
        f2 = ex.submit(rollback_worker)
        f1.result(); f2.result()

    final = envs(client)
    log_case("升级与回滚并发抢 staging",
             {"requests": ["upgrade 2.0 next-env", "rollback staging"]},
             {"both expected_revision": 1},
             {"results": results, "final": final})
    assert sum(1 for r in results if r[1] == 200) == 1
    # 败方必须被明确识别为冲突（两种 409 均可安全重试）
    conflicts = [r for r in results if r[1] == 409]
    assert len(conflicts) == 1
    assert conflicts[0][2] in ("REVISION_CONFLICT", "UPGRADE_BLOCKED")
    # 只发生一次成功写入：staging revision=2，版本与胜者一致
    assert final["staging"][1] == 2
    by_name = {r[0]: r for r in results}
    if by_name["upgrade"][1] == 200:  # 升级胜出
        assert final["staging"] == ("2.0", 2)
    else:                             # 回滚胜出
        assert final["staging"] == (None, 2)
    assert final["dev"] == ("2.0", 2)  # 已换好的环境不受影响


def test_concurrent_duplicate_submit_same_step(client):
    """同一步被两个客户端同时重复提交（同 request_id）：幂等，
    两个都拿到成功结果，但环境只被写入一次。"""
    _full_deploy(client, "app", "1.0")
    register(client, "app", "2.0")

    results: list = []
    barrier = threading.Barrier(2)

    def worker():
        barrier.wait()
        resp = upgrade(client, request_id="dup1", artifact_id="app",
                       version="2.0")
        results.append(resp.status_code)

    with ThreadPoolExecutor(max_workers=2) as ex:
        f1, f2 = ex.submit(worker), ex.submit(worker)
        f1.result(); f2.result()

    final = envs(client)
    log_case("同一步并发重复提交", {"request_id": "dup1 x2"},
             {"dev": ("1.0", 1)}, {"results": results, "final": final})
    assert results == [200, 200]
    assert final["dev"] == ("2.0", 2)  # 只写入一次
    assert final["staging"] == ("1.0", 1)
    op = client.get("/operations/dup1").json()
    assert op["status"] == "SUCCESS"
    assert op["attempts"] == 1  # 第二次是幂等重放，不算新尝试


def test_concurrent_query_during_upgrade(client):
    """查询与升级并发：每次查询都读到自洽快照（不抛错、环境数完整、
    阶段取值合法），升级完成后状态一致。"""
    _full_deploy(client, "app", "1.0")
    register(client, "app", "2.0")

    snapshots: list = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            p = upgrade_progress(client, "app", "2.0")
            assert len(p["envs"]) == 3
            assert p["phase"] in ("NOT_STARTED", "REPLACING", "COMPLETED")
            snapshots.append(p["phase"])

    t = threading.Thread(target=reader)
    t.start()
    for i in range(3):
        r = upgrade(client, request_id=f"u{i}", artifact_id="app",
                    version="2.0")
        assert r.status_code == 200, r.text
    stop.set()
    t.join()

    final = envs(client)
    log_case("查询与升级并发", {"upgrades": 3, "queries": len(snapshots)},
             {"phases_seen": sorted(set(snapshots))}, final)
    assert snapshots  # 至少读到一次
    assert final == {"dev": ("2.0", 2), "staging": ("2.0", 2),
                     "prod": ("2.0", 2)}


def test_upgrade_mid_failure_rolls_back_written_envs(client):
    """从统一基线升级中途失败：本次已换的环境全部回落到操作前，
    进度回到 NOT_STARTED，可用同一 request_id 续做成功。"""
    _full_deploy(client, "app", "1.0")
    register(client, "app", "2.0")
    before = envs(client)

    req = dict(request_id="f1", artifact_id="app", version="2.0",
               target_env="prod", fail_at_env="staging")
    r = upgrade(client, **req)
    assert r.status_code == 500
    assert r.json()["error"]["code"] == "INJECTED_FAILURE"
    final = envs(client)
    log_case("替换升级中途失败（staging 注入）", req, before, final)
    # dev/staging 本次已写入，必须回落到操作前可见状态；prod 未写入
    assert final == before

    op = client.get("/operations/f1").json()
    assert op["status"] == "FAILED"
    steps = {(s["env"], s["rolled_back"]) for s in op["steps"]}
    assert ("dev", True) in steps and ("staging", True) in steps

    # 进度查询：回到未开始、可继续
    p = upgrade_progress(client, "app", "2.0")
    assert (p["phase"], p["continuable"]) == ("NOT_STARTED", True)

    # 同一 request_id 去掉注入后续做成功
    req.pop("fail_at_env")
    r = upgrade(client, **req)
    assert r.status_code == 200, r.text
    assert r.json()["attempts"] == 2
    assert envs(client) == {"dev": ("2.0", 2), "staging": ("2.0", 2),
                            "prod": ("2.0", 2)}


def test_upgrade_mid_failure_in_window_preserves_previous(client):
    """混合窗口中续推失败：只回落本次写入的环境，
    之前就已换好的 dev 保持新版本不受影响。"""
    _full_deploy(client, "app", "1.0")
    register(client, "app", "2.0")
    upgrade(client, request_id="u1", artifact_id="app", version="2.0")
    before = envs(client)
    assert before == {"dev": ("2.0", 2), "staging": ("1.0", 1),
                      "prod": ("1.0", 1)}

    req = dict(request_id="f2", artifact_id="app", version="2.0",
               target_env="prod", fail_at_env="prod")
    r = upgrade(client, **req)
    assert r.status_code == 500
    final = envs(client)
    log_case("窗口内续推失败（prod 注入）", req, before, final)
    # staging 本次写入被回落；dev 是之前就已生效的，不受影响
    assert final == before

    op = client.get("/operations/f2").json()
    assert op["status"] == "FAILED"
    steps = {s["env"]: s["rolled_back"] for s in op["steps"]}
    assert steps == {"staging": True, "prod": True}

    # 仍处于健康混合窗口，可继续
    p = upgrade_progress(client, "app", "2.0")
    assert (p["phase"], p["continuable"], p["next_env"]) == (
        "REPLACING", True, "staging")

    # 环境状态与操作流水对得上：staging 历史里本次写入已被补偿
    ops = client.get("/operations").json()
    failed = [o for o in ops if o["request_id"] == "f2"]
    assert len(failed) == 1 and failed[0]["status"] == "FAILED"
