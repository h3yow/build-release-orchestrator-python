"""磁盘状态存储。

用本地文件模拟产物仓库与各环境的目标状态：
* state.json  —— 产物版本与各环境当前状态（原子写：临时文件 + os.replace）
* operations.jsonl —— 操作流水（每次尝试追加一行，按 request_id 索引）
* .lock —— fcntl 文件锁，配合进程内 threading.RLock 保证读改写串行化
"""

from __future__ import annotations

import fcntl
import json
import os
import threading
from contextlib import contextmanager
from pathlib import Path
from collections.abc import Iterator

from .models import (
    DEFAULT_PIPELINE,
    EnvState,
    Operation,
    Version,
)


class Store:
    """进程内状态 + 磁盘持久化。

    所有读改写必须在 :meth:`locked` 上下文内完成；
    每次状态变更后调用 :meth:`persist` 原子落盘。
    """

    def __init__(self, data_dir: str | Path | None = None) -> None:
        self.data_dir = Path(data_dir) if data_dir else Path("data")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.data_dir / "state.json"
        self.ops_path = self.data_dir / "operations.jsonl"
        self.lock_path = self.data_dir / ".lock"
        # 锁文件需在 Store 整个生命周期内保持打开，供 fcntl.flock 使用其 fd
        self._lock_file = open(self.lock_path, "a+")  # noqa: SIM115
        # 进程内锁：同一进程的线程串行化（fcntl 锁对同进程线程不可靠）
        self._lock = threading.RLock()

        # 内存状态
        self._versions: dict[str, dict[str, Version]] = {}
        self._envs: dict[str, EnvState] = {
            name: EnvState(env=name) for name in DEFAULT_PIPELINE
        }
        self._operations: dict[str, Operation] = {}

        self._load()

    # ---------------- 锁 ----------------

    @contextmanager
    def locked(self) -> Iterator[None]:
        with self._lock:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)

    # ---------------- 产物版本 ----------------

    def get_version(self, artifact_id: str, version: str) -> Version | None:
        return self._versions.get(artifact_id, {}).get(version)

    def list_versions(self, artifact_id: str) -> list[Version]:
        return list(self._versions.get(artifact_id, {}).values())

    def put_version(self, version: Version) -> None:
        self._versions.setdefault(version.artifact_id, {})[version.version] = version

    def has_artifact(self, artifact_id: str) -> bool:
        return artifact_id in self._versions

    # ---------------- 环境 ----------------

    def get_env(self, env: str) -> EnvState | None:
        return self._envs.get(env)

    def list_envs(self) -> list[EnvState]:
        return [self._envs[name] for name in DEFAULT_PIPELINE if name in self._envs]

    # ---------------- 操作流水 ----------------

    def get_operation(self, request_id: str) -> Operation | None:
        return self._operations.get(request_id)

    def list_operations(self, artifact_id: str | None = None) -> list[Operation]:
        ops = list(self._operations.values())
        if artifact_id is not None:
            ops = [o for o in ops if o.artifact_id == artifact_id]
        ops.sort(key=lambda o: o.created_at)
        return ops

    def put_operation(self, operation: Operation) -> None:
        """落一条操作流水（按 request_id upsert），并追加一行到 jsonl。"""
        self._operations[operation.request_id] = operation
        with open(self.ops_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(operation.to_dict(), ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

    # ---------------- 持久化 ----------------

    def persist(self) -> None:
        """把当前内存状态原子写入 state.json（临时文件 + replace）。"""
        payload = {
            "versions": {
                aid: {v: ver.to_dict() for v, ver in vers.items()}
                for aid, vers in self._versions.items()
            },
            "envs": {name: env.to_dict() for name, env in self._envs.items()},
        }
        tmp = self.state_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.state_path)

    # ---------------- 装载 ----------------

    def _load(self) -> None:
        if self.state_path.exists():
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            self._versions = {
                aid: {v: Version.from_dict(d) for v, d in vers.items()}
                for aid, vers in data.get("versions", {}).items()
            }
            saved_envs = {
                name: EnvState.from_dict(d)
                for name, d in data.get("envs", {}).items()
            }
            # 以流水线定义为准，合并已保存状态（防止旧文件缺环境）
            for name in DEFAULT_PIPELINE:
                if name in saved_envs:
                    self._envs[name] = saved_envs[name]
        if self.ops_path.exists():
            for line in self.ops_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                op = Operation.from_dict(json.loads(line))
                # 同一 request_id 的最后一次尝试为当前视图
                self._operations[op.request_id] = op
