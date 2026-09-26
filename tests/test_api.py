"""HTTP 层：错误码映射、幂等重放与状态查询。"""
import logging

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import router
from app.service import Orchestrator
from app.store import Store

log = logging.getLogger("test")


@pytest.fixture()
def client(tmp_path):
    app = FastAPI()
    orch = Orchestrator(Store(tmp_path / "data"))
    app.state.orchestrator = orch
    app.include_router(router)
    return TestClient(app)


def test_api_flow_and_error_mapping(client):
    assert client.post("/artifacts", json={
        "version": "1.0.0", "build_status": "succeeded"}).status_code == 201

    # 404 版本不存在
    r = client.post("/promotions", json={"version": "ghost", "env": "dev"})
    log.info("[REQ ] POST /promotions version=ghost -> %s %s", r.status_code, r.json())
    assert r.status_code == 404 and r.json()["error"] == "artifact_not_found"

    # 200 正常晋级 dev
    r = client.post("/promotions", json={
        "version": "1.0.0", "env": "dev", "expected": None, "request_id": "r1"})
    log.info("[REQ ] POST /promotions version=1.0.0 env=dev -> %s %s", r.status_code, r.json())
    assert r.status_code == 200 and r.json()["status"] == "committed"
    op_id = r.json()["op_id"]

    # 幂等重放：同一 request_id 返回同一 op_id
    r = client.post("/promotions", json={
        "version": "1.0.0", "env": "dev", "expected": None, "request_id": "r1"})
    assert r.status_code == 200 and r.json()["op_id"] == op_id

    # 422 前置环境未生效（跨级到 prod）
    r = client.post("/promotions", json={"version": "1.0.0", "env": "prod"})
    log.info("[REQ ] POST /promotions env=prod -> %s %s", r.status_code, r.json())
    assert r.status_code == 422 and r.json()["error"] == "precondition_not_met"

    # 409 前置条件冲突（expected 与现状不符）
    r = client.post("/promotions", json={
        "version": "1.0.0", "env": "staging", "expected": "9.9.9"})
    log.info("[REQ ] POST /promotions expected=9.9.9 -> %s %s", r.status_code, r.json())
    assert r.status_code == 409 and r.json()["error"] == "precondition_conflict"

    # 500 中途失败，环境回落
    r = client.post("/promotions", json={
        "version": "1.0.0", "env": "staging", "expected": None,
        "simulate_failure": True})
    log.info("[REQ ] POST /promotions simulate_failure -> %s %s", r.status_code, r.json())
    assert r.status_code == 500 and r.json()["status"] == "failed"

    envs = client.get("/envs").json()
    log.info("[ENV ] final env status=%s", envs)
    assert envs == {"dev": "1.0.0", "staging": None, "prod": None}

    # 操作流水可查询、可解释
    ops = client.get("/ops").json()
    assert any(op["status"] == "failed" for op in ops)
    assert any(op["status"] == "rejected" for op in ops)

    # 产物维度状态视图
    view = client.get("/artifacts/1.0.0").json()
    assert view["envs"] == {"dev": "active", "staging": "absent", "prod": "absent"}
