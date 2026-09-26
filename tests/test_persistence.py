"""磁盘持久化与历史一致性测试。"""

from __future__ import annotations

import os

from .conftest import envs, log_case, promote, register, rollback


def test_state_survives_restart(client, data_dir, monkeypatch):
    register(client, "app", "1.0")
    promote(client, request_id="p1", artifact_id="app", version="1.0",
            target_env="prod")
    promote(client, request_id="p2", artifact_id="app", version="1.0",
            target_env="dev")  # 重复 -> 409，不应影响磁盘

    # 用同一数据目录重新加载应用，状态必须完整恢复
    import importlib
    import main
    importlib.reload(main)
    from fastapi.testclient import TestClient
    with TestClient(main.app) as c2:
        final = {e["env"]: (e["active_version"], e["revision"])
                 for e in c2.get("/envs").json()}
        log_case("服务重启后状态恢复",
                 {"data_dir": str(data_dir)},
                 {"before": "1.0 on all envs"}, final)
        assert final == {"dev": ("1.0", 1), "staging": ("1.0", 1),
                         "prod": ("1.0", 1)}
        op = c2.get("/operations/p1").json()
        assert op["status"] == "SUCCESS"


def test_env_history_is_explainable(client):
    """环境历史记录每次晋级/回滚，可解释当前状态来源。"""
    register(client, "app", "1.0")
    register(client, "app", "2.0")
    promote(client, request_id="p1", artifact_id="app", version="1.0",
            target_env="dev")
    promote(client, request_id="p2", artifact_id="app", version="2.0",
            target_env="dev", expected_revision=1)
    rollback(client, request_id="rb1", artifact_id="app", env="dev",
             to_version="1.0", expected_revision=2)

    # 通过内部 store 直接校验历史（API 以流水为准）
    store = client._module.service.store
    with store.locked():
        dev = store.get_env("dev")
        actions = [(h.action, h.version, h.status.value) for h in dev.history]
        log_case("dev 环境历史轨迹",
                 {"history": actions}, {},
                 {"active": dev.active_version, "revision": dev.revision})
    assert dev.active_version == "1.0"
    assert dev.revision == 3
    # 三次写入：1.0 晋级、2.0 晋级（前者 SUPERSEDED）、回滚 1.0（2.0 ROLLED_BACK）
    assert ("PROMOTE", "2.0", "ROLLED_BACK") in actions
    assert ("ROLLBACK", "1.0", "ACTIVE") in actions
