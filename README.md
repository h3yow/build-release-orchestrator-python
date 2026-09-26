# 本地构建产物发布编排服务（Build Release Orchestrator）

一个只依赖**本地文件与进程内资源**的 FastAPI 服务，用磁盘状态模拟“产物仓库”与
“各环境目标状态”，提供产物版本在多环境之间的**晋级（promote）/ 回滚（rollback）**
编排能力。并发的晋级、回滚与重复请求都能得到**一致且可解释**的结果：
前置条件不满足会被明确拒绝（原因可区分，绝不静默成功），冲突可安全重试，
中途失败会回滚到操作前可见状态。

不接入任何外部服务、真实凭据或付费组件。

---

## 1. 快速开始

```bash
uv sync                                  # 安装依赖
uv run uvicorn main:app --port 8000      # 启动服务
uv run pytest                            # 运行全部测试（实时打印请求/前置/状态）
```

数据目录由环境变量 `RELEASE_ORCHESTRATOR_DATA_DIR` 指定，默认 `./data`。
首次启动自动创建三个环境：`dev`、`staging`、`prod`。

---

## 2. 状态模型

### 2.1 产物版本（Artifact Version）

| 字段 | 说明 |
| --- | --- |
| `artifact_id` / `version` | 产物与版本标识，二者唯一 |
| `build_status` | `PENDING` / `SUCCESS` / `FAILED`。**只有 `SUCCESS` 可晋级** |
| `metadata` | 任意附加信息 |

### 2.2 环境状态（EnvState）

流水线固定为 `dev → staging → prod`（索引即层级）。每个环境包含：

| 字段 | 说明 |
| --- | --- |
| `active_version` | 当前生效版本；任意时刻至多一个，可为空 |
| `revision` | 单调递增整数，**每次写入 +1**，用于乐观并发控制 |
| `history[]` | 该环境的生效/替换/回滚轨迹，每条含 `action/version/status/revision/request_id/timestamp` |

历史条目状态：`ACTIVE`（当前生效）、`SUPERSEDED`（被新版本替换）、
`ROLLED_BACK`（被回滚撤下）。

### 2.3 操作流水（Operation）

每次晋级 / 回滚 / 注册都落一条以 `request_id` 为键的流水，只增语义地追加到
`operations.jsonl`，字段包括：操作类型、目标、**调用方携带的 `expected_revision`**、
`from_version/to_version`、`revision_before/revision_after`、
`status(SUCCESS/FAILED)`、`error_code/error_message`、`attempts`，
以及级联晋级的逐环境 `steps[]`（每步是否已被补偿 `rolled_back`）。

### 2.4 磁盘文件

```
data/
├── state.json        # 产物版本 + 各环境当前状态（临时文件 + os.replace 原子写）
├── operations.jsonl  # 操作流水，每次尝试追加一行（fsync 后落盘）
└── .lock             # fcntl 排他锁文件
```

重启后从 `state.json` 恢复环境、从 `operations.jsonl` 重建流水视图。

---

## 3. 晋级规则（Promote）

1. **构建门槛**：版本必须 `build_status=SUCCESS`，否则返回
   `PRECONDITION_FAILED`。
2. **流水线顺序**：版本只能沿 `dev → staging → prod` 前进。
   一次请求若点名更高环境（如直接晋级 `prod`），系统会**自动级联**经过中间
   尚未生效本版本的环境，逐环境写入、逐环境校验前置。
3. **前置即时校验**：写入每个环境前，在锁内检查“上一环境当前生效版本必须
   是本版本”，不满足返回 `PRECONDITION_FAILED`，且本次已写入的环境全部补偿。
4. **允许替换与补缺口**：
   - 新版本晋级到某环境时，替换该环境上的旧版本是正常操作。
   - 回滚造成的低环境缺口（如 `dev=null, staging=v, prod=v`）允许通过续做
     从最低缺口环境重新补齐，已生效的中间/高环境不重复写入、不重复增加 revision。
5. **禁止重复晋级**：目标及其全部前置环境都已是本版本时，返回 `ALREADY_ACTIVE`。
6. **交错防御**：本版本的生效链路中间夹着**别的版本**（流水线断裂）时，
   返回 `INVALID_STATE`，拒绝写入。

### 级联写入与中途失败

- 请求支持 `fail_at_env`，在指定环境写入完成后注入一次“后置环节失败”，
  用于模拟晋级过程中部分环节失败（生产代码中对应通知 / 健康检查等失败点）。
- 失败发生后：
  - **本次请求已写入的环境**按**逆序补偿**：`active_version` 与 `revision`
    都还原到操作前，并在该环境历史追加一条 `ROLLBACK` 记录；
  - 失败**之前就已生效、不属于本次写入**的环境**保持生效不动**；
  - 操作流水标记 `FAILED`，每个被补偿的步骤置 `rolled_back=true`。
- 用**同一 `request_id` 重试**即为“续做”：失败记录的 `attempts` 累加，
  基于当前可观测状态重新计算缺口路径并推进。

## 4. 回滚规则（Rollback）

- `POST /rollbacks` 指定 `env`：
  - 不传 `to_version`：**撤下**当前生效版本，环境回到“无生效版本”，revision +1；
  - 传 `to_version`：回滚到该环境**历史上生效过**的版本，revision +1。
- 拒绝情况（错误码可区分）：
  - `NOTHING_TO_ROLLBACK`：环境本来就没有生效版本却要求撤下；
  - `VERSION_NOT_FOUND` / `PRECONDITION_FAILED`：目标版本不存在或构建未成功；
  - `INVALID_STATE`：目标版本从未在该环境生效过（回滚来源必须可观测）；
  - `ALREADY_ACTIVE`：目标版本就是当前生效版本；
  - `REVISION_CONFLICT`：环境 revision 与预期不符。
- 回滚只影响**单个环境**，其它环境保持现状（满足“已生效环境保持生效”）。

---

## 5. 并发控制策略

采用“**全局串行化 + 每环境乐观条件（CAS）**”：

1. **串行化**：所有读改写都在同一把锁内完成——进程内 `threading.RLock`
   负责多线程互斥，`fcntl.flock` 排他锁负责多进程互斥。交错请求不会读到
   半成品状态。
2. **可校验的写入前置条件**：写请求可携带 `expected_revision`。提交时在锁内
   比对目标环境当前 revision：
   - 一致 → 执行写入并 `revision += 1`；
   - 不一致 → 返回 **`409 REVISION_CONFLICT`**，**状态不变**，客户端读取最新
     revision 后可安全重试。
3. **幂等键**：晋级/回滚必须带 `request_id`。
   - 同 `request_id` + 同请求指纹且**已成功** → 原样返回既有结果
     （幂等重放，不重复写状态、不重复 +revision）；
   - 同 `request_id` + 同请求指纹但**曾失败** → 视为续做重试，`attempts+1`；
   - 同 `request_id` 但请求体语义不同 → 返回
     **`409 DUPLICATE_REQUEST_MISMATCH`**，明确拒绝复用。

由此，多客户端的重复或交错请求都不会覆盖 / 跳变同一环境状态。

### 错误码与 HTTP 状态

| HTTP | 错误码 | 含义 |
| --- | --- | --- |
| 404 | `ARTIFACT_NOT_FOUND` / `VERSION_NOT_FOUND` / `ENV_NOT_FOUND` / `OPERATION_NOT_FOUND` | 资源不存在 |
| 422 | `PRECONDITION_FAILED` | 构建未成功 / 上一环境未生效 |
| 422 | `INVALID_STATE` | 流水线断裂、回滚目标无历史、非法构建状态等 |
| 422 | `INVALID_TRANSITION` / `NOTHING_TO_ROLLBACK` | 非法迁移 / 无版本可回滚 |
| 409 | `REVISION_CONFLICT` | 乐观锁冲突，可安全重试 |
| 409 | `ALREADY_ACTIVE` | 重复晋级 / 回滚到当前版本 |
| 409 | `DUPLICATE_REQUEST(_MISMATCH)` | 幂等重放 / request_id 指纹冲突 |
| 500 | `INJECTED_FAILURE` | 注入的中途失败（仅测试使用） |

---

## 6. HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/artifacts/versions` | 注册产物版本（可带 build_status） |
| `PUT`  | `/artifacts/{aid}/versions/{ver}/build` | 上报构建结果 |
| `GET`  | `/artifacts/{aid}/versions/{ver}` | 查询版本 |
| `POST` | `/promotions` | 晋级（级联），body 见下 |
| `POST` | `/rollbacks` | 回滚 / 撤下 |
| `GET`  | `/envs` | 各环境当前 `active_version/revision` |
| `GET`  | `/operations/{request_id}` | 查询单条操作流水（含 steps） |
| `GET`  | `/operations?artifact_id=` | 查询操作流水列表 |

晋级请求示例：

```json
POST /promotions
{
  "request_id": "req-20260926-001",
  "artifact_id": "checkout",
  "version": "1.4.2",
  "target_env": "prod",
  "expected_revision": 7,
  "fail_at_env": null
}
```

回滚请求示例：

```json
POST /rollbacks
{
  "request_id": "req-20260926-002",
  "artifact_id": "checkout",
  "env": "staging",
  "to_version": "1.4.1",
  "expected_revision": 8
}
```

---

## 7. 验证方法

```bash
uv run pytest                      # 全量测试（28 个用例）
uv run pytest -k concurrent        # 只看并发冲突用例
uv run pytest --log-cli-level=INFO # 打印每个用例的请求内容/前置条件/最终状态
```

测试分层：

| 文件 | 覆盖内容 |
| --- | --- |
| `tests/test_rules.py` | 级联晋级、逐步晋级、构建门槛、跳级/交错拒绝、重复晋级、各类 404、回滚到历史版本、撤下、回滚来源校验、回滚后续做补缺口 |
| `tests/test_concurrency.py` | 同环境并发晋级只一方成功、冲突后刷新 revision 安全重试、互不相交环境并发都成功、晋级与回滚并发无覆盖 |
| `tests/test_idempotency.py` | 成功请求重复提交幂等、回滚幂等、request_id 指纹冲突拒绝、失败重试 attempts 累加 |
| `tests/test_failure_handling.py` | 中途失败逐级回落、末环失败回落、已生效旧版本不被破坏、失败后续做成功、前置拒绝无半成品、流水记录每次尝试 |
| `tests/test_persistence.py` | 重启后状态/流水恢复、环境历史轨迹可解释 |

每个用例通过 `log_case()` 打印三段内容：**请求内容、前置条件、最终状态**，
冲突用例还会打印每个并发线程的状态码与错误码，便于核对“无跳变、无残留”。
