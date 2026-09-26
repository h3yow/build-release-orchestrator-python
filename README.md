# build-release-orchestrator

本地构建产物的发布编排服务（FastAPI）。用磁盘目录模拟产物仓库与各环境的
目标状态，支持产物版本在 `dev → staging → prod` 之间的晋级与回滚。全部能力
只依赖本地文件与进程内资源，不接入外部服务、真实凭据或付费组件。

## 运行

```bash
uv run python main.py                 # 启动服务（数据目录默认 ./data，可用 ORCHESTRATOR_DATA_DIR 覆盖）
uv run pytest tests/ -q               # 运行测试
uv run pytest tests/ -q -s            # 打印单测日志：请求内容、前置条件与最终状态
```

## 状态模型

磁盘布局即状态，任意时刻可直接检查：

```
data/
  artifacts/{version}.json   产物版本记录：{"version", "build_status"}
  envs/{env}/active          该环境当前生效版本（文件内容即版本号；文件缺失 = 无生效版本）
  ops/{op_id}.json           操作流水（追加不覆盖，按 ts 排序读取）
  requests/{request_id}.json 幂等键 -> op_id 映射
```

- 产物版本在某环境上的可见状态：`absent`（从未生效）/ `active`（当前生效）/
  `rolled_back`（曾生效、已撤回），由环境现状与操作流水推导，不单独落盘。
- 操作流水中的每条记录携带 `expected`（写入时声明的预期前置版本）与
  `result`（操作完成后该环境生效版本），状态与流水始终可对照、可解释。

## 晋级与回滚规则

**晋级 `POST /promotions`**（`{version, env, expected, request_id?}`）：

1. 产物版本必须存在，否则 `artifact_not_found`（404）。
2. 构建状态必须为 `succeeded`，否则 `build_not_succeeded`（422）。
3. 目标环境非 `dev` 时，该版本必须在前一环境处于 `active`，
   否则 `precondition_not_met`（422）——不允许跨级晋级。
4. 版本不得已在目标环境生效，否则 `illegal_transition`（422）。
5. 校验通过后以 CAS 写入：仅当环境当前生效版本等于请求携带的 `expected`
   才落盘，否则 `precondition_conflict`（409），可携带最新前置条件安全重试。

**回滚 `POST /rollbacks`**（同参数）：目标环境当前生效版本必须等于被回滚
版本（否则 `env_not_active`，422）；回落目标取自操作流水中该环境上一条
已提交晋级的结果（无则回到无生效版本），同样以 CAS 写入。

**中途失败**：晋级落盘后若下游环节失败（测试可用 `simulate_failure` 或
`fail_hook` 注入），环境立即回落到操作前可见状态，流水记录 `failed` 与原因
`downstream_step_failed`（HTTP 500），已生效的其他环境不受影响；之后可按
原前置条件续做。

所有被拒绝的请求也会写入流水（`status=rejected` + `reason`），
拒绝原因互相可区分，绝不静默成功。

## 并发控制策略

- **进程内互斥锁**：校验、CAS、写流水在同一临界区内完成，来自多个客户端的
  交错请求对同一环境串行生效，不会互相覆盖或跳变。
- **CAS 前置条件**：每次状态写入都携带调用方声明的 `expected` 版本，
  与现状不符即拒绝（409）并可安全重试——乐观并发控制，冲突显式化。
- **幂等键**：`request_id` 相同的重复提交（含并发重复）返回首次操作的
  结果，状态只应用一次，流水不重复。
- **原子落盘**：所有文件先写临时文件再 `os.replace`，磁盘上任意时刻
  不会出现半成品文件。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/artifacts` | 登记产物版本 `{version, build_status}` |
| GET | `/artifacts/{version}` | 产物信息 + 各环境可见状态 |
| GET | `/envs` | 各环境当前生效版本 |
| POST | `/promotions` | 晋级（参数见上） |
| POST | `/rollbacks` | 回滚（参数见上） |
| GET | `/ops` | 操作流水 |

## 验证方法

`tests/` 覆盖：

- `test_orchestrator.py`：晋级链全链路；五类拒绝原因可区分且全部落流水；
  回滚基于流水回落到上一生效版本直至无生效版本。
- `test_concurrency.py`：并发晋级同一环境恰一个成功、冲突方可重试；
  晋级与回滚交错时最终状态等于最后一条已提交流水、无中间态；同一幂等键
  并发重复提交只执行一次；中途失败回落到操作前状态且可续做。
- `test_api.py`：HTTP 层错误码映射（404/409/422/500）、幂等重放、
  状态与流水查询。

单测日志（`-s`）会打印每次请求内容、`expected` 前置条件、操作结果与
最终环境状态，便于核对「状态与流水一致」。
