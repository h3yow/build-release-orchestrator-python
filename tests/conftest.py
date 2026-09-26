import logging

import pytest

from app.models import Artifact
from app.service import Orchestrator
from app.store import Store

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("test")


@pytest.fixture()
def orch(tmp_path):
    store = Store(tmp_path / "data")
    return Orchestrator(store)


def log_op(logger, action: str, request: dict, result) -> None:
    """单测日志：打印请求内容、前置条件与最终状态。"""
    op = result.op
    logger.info(
        "[REQ ] %s request=%s", action, request)
    logger.info(
        "[RESP] op=%s status=%s expected=%r result=%r error=%s detail=%s",
        op.op_id, op.status.value, op.expected, op.result,
        result.error.value if result.error else None, result.detail)


def log_envs(logger, orch: Orchestrator) -> None:
    logger.info("[ENV ] final env status=%s", orch.env_status())
