"""FastAPI 路由：产物登记、晋级、回滚、状态与流水查询。"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .models import ENV_ORDER, EnvName, EnvState, ErrorCode, OpResult, OpStatus

router = APIRouter()

_ERROR_HTTP = {
    ErrorCode.ARTIFACT_NOT_FOUND: 404,
    ErrorCode.PRECONDITION_CONFLICT: 409,
    ErrorCode.BUILD_NOT_SUCCEEDED: 422,
    ErrorCode.ILLEGAL_TRANSITION: 422,
    ErrorCode.PRECONDITION_NOT_MET: 422,
    ErrorCode.ENV_NOT_ACTIVE: 422,
}


class ArtifactIn(BaseModel):
    version: str
    build_status: str  # "succeeded" | "failed" | "building"


class OpIn(BaseModel):
    version: str
    env: EnvName
    expected: str | None = None   # 预期该环境当前生效版本（前置条件）
    request_id: str | None = None  # 幂等键
    simulate_failure: bool = False  # 测试用：模拟晋级中途下游环节失败


def _op_view(r: OpResult) -> dict:
    op = r.op
    return {
        "ok": r.ok,
        "op_id": op.op_id,
        "op_type": op.op_type.value,
        "version": op.version,
        "env": op.env.value,
        "status": op.status.value,
        "expected": op.expected,
        "result": op.result,
        "error": r.error.value if r.error else None,
        "detail": r.detail,
    }


def _respond(r: OpResult) -> JSONResponse:
    if r.ok:
        return JSONResponse(status_code=200, content=_op_view(r))
    if r.op.status == OpStatus.FAILED:
        return JSONResponse(status_code=500, content=_op_view(r))
    code = _ERROR_HTTP.get(r.error, 400)
    return JSONResponse(status_code=code, content=_op_view(r))


@router.post("/artifacts", status_code=201)
def register_artifact(body: ArtifactIn, request: Request):
    from .models import Artifact
    orch = request.app.state.orchestrator
    orch.store.put_artifact(Artifact(version=body.version, build_status=body.build_status))
    return {"version": body.version, "build_status": body.build_status}


@router.get("/artifacts/{version}")
def get_artifact(version: str, request: Request):
    orch = request.app.state.orchestrator
    artifact = orch.store.get_artifact(version)
    if artifact is None:
        return JSONResponse(status_code=404,
                            content={"error": ErrorCode.ARTIFACT_NOT_FOUND.value})
    return {
        "version": artifact.version,
        "build_status": artifact.build_status,
        "envs": {env.value: orch.version_env_state(version, env).value
                 for env in ENV_ORDER},
    }


@router.get("/envs")
def get_envs(request: Request):
    return request.app.state.orchestrator.env_status()


@router.post("/promotions")
def promote(body: OpIn, request: Request):
    orch = request.app.state.orchestrator
    hook = (lambda env: True) if body.simulate_failure else None
    return _respond(orch.promote(body.version, body.env, body.expected,
                                 request_id=body.request_id, fail_hook=hook))


@router.post("/rollbacks")
def rollback(body: OpIn, request: Request):
    orch = request.app.state.orchestrator
    return _respond(orch.rollback(body.version, body.env, body.expected,
                                  request_id=body.request_id))


@router.get("/ops")
def list_ops(request: Request):
    ops = request.app.state.orchestrator.store.list_ops()
    return [{"op_id": op.op_id, "op_type": op.op_type.value, "version": op.version,
             "env": op.env.value, "status": op.status.value, "expected": op.expected,
             "result": op.result, "reason": op.reason, "request_id": op.request_id,
             "ts": op.ts} for op in ops]
