"""FastAPI 入口：本地构建产物发布编排服务。

数据目录由环境变量 RELEASE_ORCHESTRATOR_DATA_DIR 指定，默认 ./data。
"""

from __future__ import annotations

import os
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from orchestrator.errors import OrchestratorError
from orchestrator.models import (
    OperationView,
    PromoteRequest,
    RegisterVersionRequest,
    ReportBuildRequest,
    RollbackRequest,
)
from orchestrator.service import ReleaseService
from orchestrator.store import Store

DATA_DIR = os.environ.get("RELEASE_ORCHESTRATOR_DATA_DIR", "data")

app = FastAPI(title="Build Release Orchestrator", version="0.1.0")
_store = Store(DATA_DIR)
service = ReleaseService(_store)


@app.exception_handler(OrchestratorError)
async def orchestrator_error_handler(request: Request, exc: OrchestratorError):
    # 409 表示与当前磁盘状态冲突（可安全重试），其余业务拒绝为 422/404
    conflict_codes = {"REVISION_CONFLICT", "ALREADY_ACTIVE", "DUPLICATE_REQUEST",
                      "DUPLICATE_REQUEST_MISMATCH"}
    not_found_codes = {"ARTIFACT_NOT_FOUND", "VERSION_NOT_FOUND",
                       "ENV_NOT_FOUND", "OPERATION_NOT_FOUND"}
    if exc.code == "INJECTED_FAILURE":
        status_code = 500  # 模拟晋级中途环节失败
    elif exc.code in conflict_codes:
        status_code = 409
    elif exc.code in not_found_codes:
        status_code = 404
    else:
        status_code = 422
    return JSONResponse(status_code=status_code, content={"error": exc.to_dict()})


@app.post("/artifacts/versions")
def register_version(req: RegisterVersionRequest):
    request_id = f"reg-{uuid.uuid4().hex[:12]}"
    op = service.register_version(req, request_id)
    return _op_view(op)


@app.put("/artifacts/{artifact_id}/versions/{version}/build")
def report_build(artifact_id: str, version: str, req: ReportBuildRequest):
    v = service.report_build(artifact_id, version, req.build_status)
    return v.to_dict()


@app.get("/artifacts/{artifact_id}/versions/{version}")
def get_version(artifact_id: str, version: str):
    return service.get_version(artifact_id, version).to_dict()


@app.post("/promotions")
def promote(req: PromoteRequest):
    op = service.promote(req)
    return _op_view(op)


@app.post("/rollbacks")
def rollback(req: RollbackRequest):
    op = service.rollback(req)
    return _op_view(op)


@app.get("/envs")
def list_envs():
    return [
        {"env": e.env, "active_version": e.active_version, "revision": e.revision}
        for e in service.list_envs()
    ]


@app.get("/operations/{request_id}")
def get_operation(request_id: str):
    return _op_view(service.get_operation(request_id))


@app.get("/operations")
def list_operations(artifact_id: str | None = None):
    return [_op_view_dict(op) for op in service.list_operations(artifact_id)]


def _op_view_dict(op) -> dict:
    return OperationView(
        request_id=op.request_id,
        op_type=op.op_type.value,
        artifact_id=op.artifact_id,
        env=op.env,
        version=op.version,
        expected_revision=op.expected_revision,
        status=op.status.value,
        error_code=op.error_code,
        error_message=op.error_message,
        attempts=op.attempts,
        from_version=op.from_version,
        to_version=op.to_version,
        revision_before=op.revision_before,
        revision_after=op.revision_after,
        created_at=op.created_at,
        updated_at=op.updated_at,
        steps=[s.to_dict() for s in op.steps],
    ).model_dump()


def _op_view(op) -> JSONResponse:
    from fastapi.encoders import jsonable_encoder
    return JSONResponse(jsonable_encoder(_op_view_dict(op)))
