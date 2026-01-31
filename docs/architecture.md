# 工程架构梳理

- 当前梳理时间: 2026-01-31 14:58:46

## 项目概览
- 项目定位: FastAPI 服务，作为 backtest-hub 的中转层，接收研究端回测请求并转发至 backtest docker，同时维护 backtest_id 映射、状态查询、日志下载与日志流；并在 hub 侧提供并发队列限制（避免 backtest docker 被无限提交打爆）。
- 主要能力: 接收 multipart 上传 run_spec.json 与策略文件；校验回测参数；落盘保存运行资料；按 backtest docker 当前 `running` 数量进行并发限制与排队；调用 backtest docker 提交任务；查询 backtest_id 映射、状态、日志下载与 WebSocket 日志流；提供队列查询与队列任务删除接口。
- 关键输出: backtest_id/backtest_docker_run_id（可能为空，表示 queued）；`/opt/backtest/runs` 下的运行目录；`run_mapping.json`；`submit_queue.json`；日志下载/日志流代理返回。
- 参数说明: backtest-hub 的 `backtest_id` 会作为 backtest docker 的 `backtest_id` 字段；backtest docker 返回的 `run_id` 记录为 `backtest_docker_run_id`。

## 工程逻辑梳理

### 入口与启动
- 入口文件/命令: `Dockerfile` 使用 `uv run uvicorn app:app --host 0.0.0.0 --port 10033` 启动；主入口为 `app.py`。
- 启动流程概述: Uvicorn 加载 `app.py` 创建 FastAPI 应用，启动时输出日志；请求经中间件记录耗时与状态。

### 核心模块
- 模块划分:
  - `app.py`: API 层、参数校验、文件落盘、转发 backtest API、backtest_id 映射管理。
  - `scripts/run_backtest.py`: 回测 runner 脚本（由 backtest docker 执行），加载策略、构建配置并运行回测。
  - `backtest_hub_cli/cli.py`: Hub CLI 包入口，负责提交/查询/下载日志/日志流，并提供 `init` 生成模板脚本。
  - `backtest_hub_cli/scripts/generate_run_spec.py`: CLI 模板脚本，`init` 命令会复制到项目 `./scripts/generate_run_spec.py`。
  - `scripts/generate_run_spec.py`: 用户自定义的 run_spec 生成脚本（由 `init` 创建，用于本地生成 `run_spec.json`）。
  - `scripts/submit_run.py`: 提交 CLI 的薄封装（转发至 `backtest_hub_cli.cli`）。
  - `scripts/backtest_hub_sdk.py`: 兼容旧命令的 CLI 封装（转发至 `backtest_hub_cli.cli`）。
  - `strategies/`: 示例策略实现（如 `spot_futures_arb_diagnostics.py`）。
  - `docker-compose.yml`: 服务运行环境与环境变量配置。
  - `run_spec.json`: 回测配置样例。
- 关键职责:
  - `app.py`: 提供 `/health`、`POST /runs`、`GET /runs/{backtest_id}`、`GET /runs/backtest/{backtest_id}`、`GET /runs/{backtest_id}/logs`、`/runs/backtest/{backtest_id}/logs/stream`、`GET /queue`、`DELETE /queue`；校验字段/时间范围；生成 backtest_id；落盘保存；依据 backtest docker 的 `GET /v1/runs`（`status=="running"`）执行并发上限与 FIFO 排队；后台调度器自动出队并提交；维护 mapping（包含 queued/submitted/cancelled 等状态与时间戳）；通过 WebSocket 代理日志流。
  - `scripts/run_backtest.py`: 校验 run_spec；动态加载策略模块；构建 spot/futures instruments 与 backtest configs；执行 `BacktestNode`；写入 `status.json`（运行/成功/失败）。
  - `backtest_hub_cli/cli.py`: `init` 复制模板脚本到 `./scripts/generate_run_spec.py`；`submit` 调用本地 `scripts/generate_run_spec.py` 生成 run_spec（若未 init 则提示）；可选 `--follow-logs` WebSocket 写入 `./live_logs/{backtest_id}.log`；`status` 查询 backtest 状态；`logs` 下载日志；并写入 `./backtest_run_id_history` 记录历史。
  - `scripts/submit_run.py`: 仅负责转发 CLI 的 submit 命令。
  - `scripts/backtest_hub_sdk.py`: 兼容命令 `get-backtest-status`/`download-backtest-logs`，内部转发至 CLI 的 `status`/`logs`。
- 主要依赖:
  - Web 服务: `fastapi`, `uvicorn`, `python-multipart`, `aiohttp`（WebSocket 代理）。
  - 回测引擎: `quant_trade_v1`（由 backtest docker 运行时提供）。
  - HTTP 转发: `urllib.request` 调用 backtest docker API。

### 依赖关系
- 外部依赖:
  - backtest docker API（`BACKTEST_API_BASE` + `BACKTEST_SUBMIT_PATH`/`BACKTEST_STATUS_PATH`/`BACKTEST_LOGS_PATH`/`BACKTEST_RUNS_PATH`）与日志流 WebSocket（`BACKTEST_WS_LOGS_PATH`）。
  - 本地/挂载存储（默认 `/opt/backtest`）用于保存 run_spec 与策略文件。
  - backtest 执行环境需提供 `quant_trade_v1`、`CATALOG_PATH` 数据目录与日志目录（默认 `/opt/backtest_logs`）。
  - CLI 依赖 backtest-hub HTTP/WS 服务（`BACKTEST_HUB_BASE_URL`）。
- 内部依赖:
  - `RUNNER_PATH` 指向 `scripts/run_backtest.py` 供转发。
  - `RUN_STORAGE_PATH` 与 `RUN_MAPPING_PATH` 用于 backtest_id 存档与映射。
  - `QUEUE_PATH` 用于保存 hub 侧提交队列（FIFO）。

### 数据流/控制流
- 数据来源: 客户端 `POST /runs` 上传 `run_spec.json` 与策略文件（multipart/form-data）。
- 数据处理链路:
  1) 校验 run_spec（字段/费用/时间范围）。
  2) 生成 `backtest_id`，写入 `/opt/backtest/runs/{backtest_id}`。
  3) 保存策略文件与更新后的 `run_spec.json`（写入 backtest_id 与实际文件名）。
  4) 查询 backtest docker 任务列表（`GET /v1/runs`），统计 `status=="running"` 的数量；若达到并发上限或队列非空则入队，否则立即转发提交。
  5) 若立即提交：读取 runner 脚本，与 run_spec/策略一并转发至 backtest docker（multipart 字段: `runer`/`strategies`/`configs`），并记录 `backtest_docker_run_id`。
  6) 保存 mapping（包含 `status`、`queued_at`/`submitted_at`/`cancelled_at`、`created_at` 等）与队列文件（如排队）。
  7) 返回 `backtest_id`，并返回 `status`（`submitted` 或 `queued`）；当 `queued` 时 `backtest_docker_run_id` 为空。
- 查询链路:
  - `GET /runs/{backtest_id}` 读取映射并返回。
  - `GET /runs/backtest/{backtest_id}` 透传 backtest docker 状态（status/pid/started_at）。
  - `GET /runs/{backtest_id}/logs` 代理下载 backtest docker 日志。
- 队列管理链路:
  - `GET /queue` 查询当前 hub 队列（支持分页与查询 position）。
  - `DELETE /queue` 批量删除队列中的任务（仅影响 queued 任务，删除后标记为 cancelled）。
- 队列调度链路:
  - hub 启动后后台调度器周期性拉取 `GET /v1/runs`，当 `running < MAX_RUNNING_BACKTESTS` 且队列非空时自动出队并提交回测。
- 日志流链路:
  - 客户端连接 `/runs/backtest/{backtest_id}/logs/stream`，服务端建立到 backtest docker 的 WebSocket 连接并双向转发消息。
  - `backtest-hub-cli submit --follow-logs`（或 `scripts/submit_run.py --follow-logs`）通过 WebSocket 拉取日志并落盘到 `./live_logs/{backtest_id}.log`。
- 控制/调度流程:
  - 并发限制以 backtest docker 的 `running` 数为准，并叠加 hub 本地的 `inflight` 预占槽位，保证不会在瞬间超额提交（`running + inflight <= MAX_RUNNING_BACKTESTS`）。
  - 队列与 inflight 通过 `asyncio.Lock` 保护；mapping 文件通过 `threading.Lock` 保护读写。
  - 异常以 HTTP 4xx/5xx 返回并记录日志；backtest docker 请求失败统一返回 502；调度提交失败会回队并记录 `last_error`。

### 关键配置
- 配置文件: `docker-compose.yml`, `run_spec.json`, `pyproject.toml`。
- 关键参数:
  - Hub 服务: `BACKTEST_API_BASE`, `BACKTEST_SUBMIT_PATH`, `BACKTEST_STATUS_PATH`, `BACKTEST_LOGS_PATH`, `BACKTEST_API_KEY`,
    `BACKTEST_WS_LOGS_PATH`, `BACKTEST_RUNS_PATH`, `DATA_MOUNT_PATH`, `RUN_STORAGE_PATH`, `RUN_MAPPING_PATH`, `QUEUE_PATH`,
    `BACKTEST_RUNNER_PATH`, `MAX_SYMBOLS`, `MAX_RANGE_DAYS`, `MAX_RUNNING_BACKTESTS`, `QUEUE_POLL_INTERVAL_SECONDS`。
  - CLI: `BACKTEST_HUB_BASE_URL`（默认 `http://100.99.101.120:10033`）。
  - Runner 脚本: `CATALOG_PATH`，`BACKTEST_LOGS_PATH`（日志目录，默认 `/opt/backtest_logs`，与 Hub 的 `BACKTEST_LOGS_PATH` 为 URL 路径含义不同）。
- 运行环境约束: Python >= 3.12；可访问 backtest docker；挂载 `/opt/backtest` 数据目录；回测执行环境需要 `quant_trade_v1`。

### 运行流程
- 运行步骤:
  1) 启动服务（Docker/uvicorn）。
  2) 执行 `backtest-hub-cli init` 生成 `./scripts/generate_run_spec.py`，按需修改参数。
  3) 调用 `POST /runs` 上传 run_spec 与策略文件（或使用 `backtest-hub-cli submit`/`scripts/submit_run.py`）。
  4) 服务落盘；若 backtest docker 当前 `running` 未达上限且队列为空则立即转发提交，否则进入队列并返回 `status=queued`。
  5) 使用 `GET /runs/{backtest_id}` 查询映射信息。
  6) 使用 `GET /runs/backtest/{backtest_id}` 查询 backtest docker 状态。
  7) 使用 `GET /runs/{backtest_id}/logs` 拉取 backtest 日志。
  8) 可选：连接 `/runs/backtest/{backtest_id}/logs/stream` 实时查看日志（或 `backtest-hub-cli submit --follow-logs`/`scripts/submit_run.py --follow-logs` 自动落盘）。
  9) 可选：使用 `GET /queue` 查询排队情况；使用 `DELETE /queue` 批量删除排队任务（标记 cancelled）。
- 异常/边界处理: 参数校验失败返回 400；缺少 runner 返回 500；backtest docker 返回错误码时透传；backtest_id 不存在返回 404。
- 观测与日志: `app.py` 统一记录请求日志；`scripts/run_backtest.py` 写入 `status.json`（包含状态/错误/traceback）。

## 改动概要/变更记录

### 2026-01-31 14:58:46
- 本次新增/更新要点: 引入 hub 侧并发队列限制（按 backtest docker `GET /v1/runs` 的 `status=="running"` 计数，超过 `MAX_RUNNING_BACKTESTS` 则入队）；新增后台调度器自动出队提交；新增 `GET /queue` 与 `DELETE /queue`（批量删除）接口；扩展 mapping 记录 queued/submitted/cancelled 状态与时间戳。
- 变更动机/需求来源: 用户要求 hub 对回测提交做并发队列限制，避免 submit 无限制导致 backtest docker 同时跑过多任务。
- 当前更新时间: 2026-01-31 14:58:46

### 2026-01-29 20:19:57
- 本次新增/更新要点: 移除 `/runs` 与 `/runs/{backtest_id}` 的 API key 鉴权要求；同步更新 CLI 与文档说明。
- 变更动机/需求来源: 用户要求取消鉴权。
- 当前更新时间: 2026-01-29 20:19:57

### 2026-01-29 20:06:02
- 本次新增/更新要点: CLI 迁移至 `backtest_hub_cli` 包；新增 `init` 命令以生成 `scripts/generate_run_spec.py` 模板；submit 改为调用本地脚本；更新历史记录落盘路径为当前目录。
- 变更动机/需求来源: 用户要求根据最新代码更新架构文档。
- 当前更新时间: 2026-01-29 20:06:02

### 2026-01-29 16:20:55
- 本次新增/更新要点: 补充 CLI 体系（scripts/cli.py 与 submit/backtest_hub_sdk 封装）；更新运行环境约束为 Python>=3.12；补全日志流/提交链路字段与配置项说明。
- 变更动机/需求来源: 用户要求根据最新代码更新架构文档。
- 当前更新时间: 2026-01-29 16:20:55

### 2026-01-29 13:28:35
- 本次新增/更新要点: 补充 WebSocket 日志流代理与提交端跟随日志能力；更新依赖与配置项（aiohttp、BACKTEST_WS_LOGS_PATH）；补充日志流数据链路与运行步骤。
- 变更动机/需求来源: 用户要求根据最新代码更新架构文档。
- 当前更新时间: 2026-01-29 13:28:35

### 2026-01-29 11:26:08
- 本次新增/更新要点: 更新 API 端点、配置项与脚本清单；补充 backtest 状态查询与日志代理流程；修正文档中的参数关系说明。
- 变更动机/需求来源: 用户要求根据最新代码更新架构文档。
- 当前更新时间: 2026-01-29 11:26:08

### 2026-01-28 22:18:53
- 本次新增/更新要点: 新建 `docs/architecture.md`，梳理入口、核心模块、依赖关系、数据流、关键配置与运行流程。
- 变更动机/需求来源: 用户要求执行 architecture-doc-updater，补全工程架构文档。
- 当前更新时间: 2026-01-28 22:18:53
