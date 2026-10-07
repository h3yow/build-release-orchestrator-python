"""公共夹具与测试日志辅助。

每个用例使用独立临时数据目录，并通过 RELEASE_ORCHESTRATOR_DATA_DIR
让 FastAPI 应用加载该目录下的 Store。
"""

from __future__ import annotations

import importlib
import logging
import os
import tempfile
from pathlib import Path

import pytest


@pytest.fixture()
def data_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture()
def client(data_dir: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("RELEASE_ORCHESTRATOR_DATA_DIR", str(data_dir))
    import main
    importlib.reload(main)
    from fastapi.testclient import TestClient
    with TestClient(main.app) as c:
        c._module = main
        yield c


@pytest.fixture()
def service(data_dir: Path):
    from orchestrator.store import Store
    from orchestrator.service import ReleaseService
    store = Store(data_dir)
    return ReleaseService(store)


def log_case(title: str, request: dict, precondition: dict, final_state) -> None:
    """按要求打印：请求内容 / 前置条件 / 最终状态。"""
    logging.getLogger("orchestrator.test").info(
        "\n==== %s ====\n请求内容: %s\n前置条件: %s\n最终状态: %s",
        title, request, precondition, final_state,
    )


def register(client, artifact: str, version: str, build_status: str = "SUCCESS"):
    body = {
        "artifact_id": artifact,
        "version": version,
        "build_status": build_status,
    }
    resp = client.post("/artifacts/versions", json=body)
    return resp


def promote(client, **kw):
    return client.post("/promotions", json=kw)


def rollback(client, **kw):
    return client.post("/rollbacks", json=kw)


def upgrade(client, **kw):
    return client.post("/upgrades", json=kw)


def upgrade_status(client, artifact: str, version: str):
    return client.get(f"/artifacts/{artifact}/upgrades/{version}/status")


def envs(client):
    return {e["env"]: (e["active_version"], e["revision"])
            for e in client.get("/envs").json()}
