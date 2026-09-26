"""并发测试：同一环境的并发晋级/回滚不得互相覆盖或跳变。

每个用例都打印：请求内容、前置条件、每个线程的结果与最终状态。
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from .conftest import envs, log_case, promote, register, rollback

logger = logging.getLogger("orchestrator.test")


def test_concurrent_promotions_same_env_one_wins(client):
    """两个客户端都基于 revision=0 向 dev 晋级不同版本：只允许一个成功，
    另一个必须收到 REVISION_CONFLICT，最终 dev 只有一个版本、revision=1。"""
    register(client, "app", "1.0")
    register(client, "app", "2.0")

    results: list = []
    barrier = threading.Barrier(2)

    def worker(version: str, request_id: str):
        barrier.wait()  # 尽量让两个请求同时发出
        req = dict(request_id=request_id, artifact_id="app",
                   version=version, target_env="dev",
                   expected_revision=0)
        resp = promote(client, **req)
        results.append((version, resp.status_code,
                        resp.json().get("error", {}).get("code")))
        return resp

    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = [ex.submit(worker, "1.0", "c1"),
                ex.submit(worker, "2.0", "c2")]
        [f.result() for f in futs]

    final = envs(client)
    log_case("并发晋级同一环境",
             {"requests": [
                 {"version": "1.0", "expected_revision": 0},
                 {"version": "2.0", "expected_revision": 0}]},
             {"rule": "仅一方 CAS 成功"},
             {"results": results, "final": final})

    codes = sorted(r[2] or "SUCCESS" for r in results)
    assert codes == ["REVISION_CONFLICT", "SUCCESS"], results
    assert final["dev"][1] == 1  # 只允许一次写入
    assert final["dev"][0] in ("1.0", "2.0")
    assert final["staging"] == (None, 0)
    assert final["prod"] == (None, 0)


def test_conflicted_request_can_safely_retry(client):
    """冲突失败的客户端刷新 revision 后重试，必须成功。"""
    register(client, "app", "1.0")
    register(client, "app", "2.0")

    # 客户端 A 先成功（revision 0 -> 1）
    r = promote(client, request_id="a1", artifact_id="app",
                version="1.0", target_env="dev", expected_revision=0)
    assert r.status_code == 200
    # 客户端 B 持旧 revision=0 冲突
    r = promote(client, request_id="b1", artifact_id="app",
                version="2.0", target_env="dev", expected_revision=0)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "REVISION_CONFLICT"
    # B 读取当前 revision=1 后安全重试
    cur = {e["env"]: e["revision"] for e in client.get("/envs").json()}
    r = promote(client, request_id="b2", artifact_id="app",
                version="2.0", target_env="dev",
                expected_revision=cur["dev"])
    assert r.status_code == 200, r.text
    final = envs(client)
    log_case("冲突后刷新 revision 安全重试",
             {"request": "promote 2.0 to dev"},
             {"first expected": 0, "retry expected": 1},
             final)
    assert final["dev"] == ("2.0", 2)


def test_concurrent_ops_on_independent_envs_both_succeed(client):
    """操作两个互不相交的环境行（一个写 dev、一个回滚 staging），
    CAS 前置分别针对各自 revision，并发下都成功。"""
    register(client, "app", "1.0")
    register(client, "app", "2.0")
    # 预置：dev=1.0(rev1)；staging=2.0(rev1，先全量再把 dev 换回来)
    promote(client, request_id="pre1", artifact_id="app", version="1.0",
            target_env="prod")
    promote(client, request_id="pre2", artifact_id="app", version="2.0",
            target_env="dev", expected_revision=1)
    # 当前 dev=2.0(rev2), staging=1.0(rev1), prod=1.0(rev1)
    barrier = threading.Barrier(2)
    out = []

    def worker_promote():
        barrier.wait()
        # 1.0 续做到 dev：dev 当前 2.0，路径=[dev]，期望 rev=2
        resp = promote(client, request_id="cp", artifact_id="app",
                       version="1.0", target_env="dev",
                       expected_revision=2)
        out.append(("promote-dev", resp.status_code))

    def worker_rollback():
        barrier.wait()
        # staging 撤下 1.0，期望 rev=1（与 dev 的 revision 完全独立）
        resp = rollback(client, request_id="cr", artifact_id="app",
                        env="staging", expected_revision=1)
        out.append(("rollback-staging", resp.status_code))

    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(worker_promote)
        f2 = ex.submit(worker_rollback)
        f1.result(); f2.result()

    final = envs(client)
    log_case("互不相交环境上的并发操作",
             {"requests": ["promote 1.0->dev", "rollback staging"]},
             {"dev expected_rev": 2, "staging expected_rev": 1},
             {"results": sorted(out), "final": final})
    assert sorted(out) == [("promote-dev", 200), ("rollback-staging", 200)]
    assert final["dev"] == ("1.0", 3)
    assert final["staging"] == (None, 2)
    assert final["prod"] == ("1.0", 1)


def test_concurrent_rollback_and_promote_no_overwrite(client):
    """并发：一个线程晋级 2.0 到 dev，另一个线程把 dev 回滚撤下。
    两者都持 revision 前置，只允许一个成功，不得出现版本残留与 revision 跳变。"""
    register(client, "app", "1.0")
    register(client, "app", "2.0")
    promote(client, request_id="init", artifact_id="app", version="1.0",
            target_env="dev")  # dev=1.0 rev=1

    barrier = threading.Barrier(2)
    results = []

    def promote_worker():
        barrier.wait()
        resp = promote(client, request_id="cp", artifact_id="app",
                       version="2.0", target_env="dev",
                       expected_revision=1)
        results.append(("promote", resp.status_code,
                        resp.json().get("error", {}).get("code")))

    def rollback_worker():
        barrier.wait()
        resp = rollback(client, request_id="cr", artifact_id="app",
                        env="dev", expected_revision=1)
        results.append(("rollback", resp.status_code,
                        resp.json().get("error", {}).get("code")))

    with ThreadPoolExecutor(max_workers=2) as ex:
        f1 = ex.submit(promote_worker)
        f2 = ex.submit(rollback_worker)
        f1.result(); f2.result()

    final = envs(client)
    log_case("晋级与回滚并发",
             {"requests": ["promote 2.0", "rollback dev"]},
             {"both expected_revision": 1},
             {"results": results, "final": final})
    statuses = {(r[0], r[1]) for r in results}
    # 恰好一个 200、一个 409
    assert sum(1 for r in results if r[1] == 200) == 1
    assert sum(1 for r in results if r[1] == 409) == 1
    # 最终 revision 必须与可见状态自洽（只发生一次成功写入 rev=2）
    assert final["dev"][1] == 2
    log_case("最终状态自洽性确认", {"check": "revision==visible state"}, {}, {"dev": final["dev"]})
