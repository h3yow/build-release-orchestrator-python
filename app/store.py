"""基于本地磁盘的产物仓库与环境状态存储。

目录布局::

    root/
      artifacts/{version}.json    产物版本记录（含构建状态）
      envs/{env}/active           该环境当前生效版本（文件内容即版本号，文件缺失表示无生效版本）
      ops/{op_id}.json            操作流水（追加，不覆盖）
      requests/{request_id}.json  幂等键 -> op_id 映射

并发控制：进程内锁保护「读-校验-写」临界区；所有写文件均先写临时文件再
os.replace 原子落盘，保证任意时刻磁盘上不会出现半成品文件。
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path

from .models import Artifact, EnvName, EnvState, OpRecord, OpStatus, OpType


class Store:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.lock = threading.RLock()
        for sub in ("artifacts", "ops", "requests"):
            (self.root / sub).mkdir(parents=True, exist_ok=True)
        for env in EnvName:
            (self.root / "envs" / env.value).mkdir(parents=True, exist_ok=True)

    # ---------- 内部工具 ----------

    def _atomic_write(self, path: Path, content: str) -> None:
        tmp = path.with_name(path.name + f".tmp.{os.getpid()}.{threading.get_ident()}")
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, path)

    # ---------- 产物仓库 ----------

    def put_artifact(self, artifact: Artifact) -> None:
        path = self.root / "artifacts" / f"{artifact.version}.json"
        with self.lock:
            self._atomic_write(path, json.dumps(
                {"version": artifact.version, "build_status": artifact.build_status}))

    def get_artifact(self, version: str) -> Artifact | None:
        path = self.root / "artifacts" / f"{version}.json"
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        return Artifact(version=data["version"], build_status=data["build_status"])

    # ---------- 环境状态 ----------

    def _active_path(self, env: EnvName) -> Path:
        return self.root / "envs" / env.value / "active"

    def get_active(self, env: EnvName) -> str | None:
        """返回该环境当前生效版本，无生效版本返回 None。"""
        path = self._active_path(env)
        if not path.exists():
            return None
        return path.read_text(encoding="utf-8").strip() or None

    def compare_and_set(self, env: EnvName, expected: str | None, new: str | None) -> bool:
        """CAS 写入：仅当当前生效版本等于 expected 时才写入 new。

        new 为 None 表示清除生效版本（删除 active 文件）。
        返回是否写入成功；失败表示前置条件冲突，调用方可安全重试。
        """
        with self.lock:
            current = self.get_active(env)
            if current != expected:
                return False
            path = self._active_path(env)
            if new is None:
                if path.exists():
                    path.unlink()
            else:
                self._atomic_write(path, new)
            return True

    # ---------- 操作流水 ----------

    def append_op(self, op: OpRecord) -> None:
        path = self.root / "ops" / f"{op.op_id}.json"
        with self.lock:
            self._atomic_write(path, json.dumps({
                "op_id": op.op_id,
                "op_type": op.op_type.value,
                "version": op.version,
                "env": op.env.value,
                "status": op.status.value,
                "expected": op.expected,
                "result": op.result,
                "reason": op.reason,
                "request_id": op.request_id,
                "ts": op.ts,
            }))

    def get_op(self, op_id: str) -> OpRecord | None:
        path = self.root / "ops" / f"{op_id}.json"
        if not path.exists():
            return None
        d = json.loads(path.read_text(encoding="utf-8"))
        return OpRecord(
            op_id=d["op_id"], op_type=OpType(d["op_type"]), version=d["version"],
            env=EnvName(d["env"]), status=OpStatus(d["status"]),
            expected=d["expected"], result=d["result"], reason=d["reason"],
            request_id=d["request_id"], ts=d["ts"],
        )

    def list_ops(self) -> list[OpRecord]:
        ops = []
        for path in sorted((self.root / "ops").glob("*.json")):
            ops.append(self.get_op(path.stem))
        return sorted((op for op in ops if op is not None), key=lambda op: op.ts)

    # ---------- 幂等键 ----------

    def get_request(self, request_id: str) -> str | None:
        """返回该幂等键已对应的 op_id，未见过返回 None。"""
        path = self.root / "requests" / f"{request_id}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))["op_id"]

    def put_request(self, request_id: str, op_id: str) -> None:
        path = self.root / "requests" / f"{request_id}.json"
        with self.lock:
            self._atomic_write(path, json.dumps({"op_id": op_id}))
