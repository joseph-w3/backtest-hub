# 工程架构梳理

- 当前梳理时间: 2026-01-29 16:20:55

## 项目概览
- 项目定位: FastAPI 服务，作为 backtest-hub 的中转层，接收研究端回测请求并转发至 backtest docker，同时维护 backtest_id 映射、状态查询、日志下载与日志流。
- 主要能力: 接收 multipart 上传 run_spec.json 与策略文件；校验回测参数；落盘保存运行资料；调用 backtest docker 提交任务；查询 backtest_id 映射、状态、日志下载与 WebSocket 日志流。
- 关键输出: backtest_id/backtest_docker_run_id；`/opt/backtest/runs` 下的运行目录；`run_mapping.json`；日志下载/日志流代理返回。
- 参数说明: backtest-hub 的 `backtest_id` 会作为 backtest docker 的 `backtest_id` 字段；backtest docker 返回的 `run_id` 记录为 `backtest_docker_run_id`。

## 工程逻辑梳理

### 入口与启动
- 入口文件/命令: `Dockerfile` 使用 `uv run uvicorn app:app --host 0.0.0.0 --port 10033` 启动；主入口为 `app.py`。
- 启动流程概述: Uvicorn 加载 `app.py` 创建 FastAPI 应用，启动时输出日志；请求经中间件记录耗时与状态。

### 核心模块
- 模块划分:
  - `app.py`: API 层、参数校验、文件落盘、转发 backtest API、backtest_id 映射管理。
  - `scripts/run_backtest.py`: 回测 runner 脚本（由 backtest docker 执行），加载策略、构建配置并运行回测。
  - `scripts/generate_run_spec.py`: 生成示例 `run_spec.json`。
  - `scripts/cli.py`: Hub CLI，负责提交/查询/下载日志/日志流。
  - `scripts/submit_run.py`: 提交 CLI 的薄封装（转发至 `scripts/cli.py submit`）。
  - `scripts/backtest_hub_sdk.py`: 兼容旧命令的 CLI 封装（转发至 `scripts/cli.py`）。
  - `strategies/`: 示例策略实现（如 `spot_futures_arb_diagnostics.py`）。
  - `docker-compose.yml`: 服务运行环境与环境变量配置。
  - `run_spec.json`: 回测配置样例。
- 关键职责:
  - `app.py`: 提供 `/health`、`POST /runs`、`GET /runs/{backtest_id}`、`GET /runs/backtest/{backtest_id}`、`GET /runs/{backtest_id}/logs`、`/runs/backtest/{backtest_id}/logs/stream`；校验字段/时间范围；生成 backtest_id；落盘保存；转发并记录 backtest_docker_run_id；维护映射；通过 WebSocket 代理日志流。
  - `scripts/run_backtest.py`: 校验 run_spec；动态加载策略模块；构建 spot/futures instruments 与 backtest configs；执行 `BacktestNode`；写入 `status.json`（运行/成功/失败）。
  - `scripts/cli.py`: `submit` 提交 run_spec 与策略（可自动生成 run_spec），可选 `--follow-logs` WebSocket 写入 `./live_logs/{backtest_id}.log`；`status` 查询 backtest 状态；`logs` 下载日志；并写入 `scripts/backtest_run_id_history` 记录历史。
  - `scripts/submit_run.py`: 仅负责转发 CLI 的 submit 命令。
  - `scripts/backtest_hub_sdk.py`: 兼容命令 `get-backtest-status`/`download-backtest-logs`，内部转发至 CLI 的 `status`/`logs`。
- 主要依赖:
  - Web 服务: `fastapi`, `uvicorn`, `python-multipart`, `aiohttp`（WebSocket 代理）。
  - 回测引擎: `quant_trade_v1`（由 backtest docker 运行时提供）。
  - HTTP 转发: `urllib.request` 调用 backtest docker API。

### 依赖关系
- 外部依赖:
  - backtest docker API（`BACKTEST_API_BASE` + `BACKTEST_SUBMIT_PATH`/`BACKTEST_STATUS_PATH`/`BACKTEST_LOGS_PATH`）与日志流 WebSocket（`BACKTEST_WS_LOGS_PATH`）。
  - 本地/挂载存储（默认 `/opt/backtest`）用于保存 run_spec 与策略文件。
  - backtest 执行环境需提供 `quant_trade_v1`、`CATALOG_PATH` 数据目录与日志目录（默认 `/opt/backtest_logs`）。
  - CLI 依赖 backtest-hub HTTP/WS 服务（`BACKTEST_HUB_BASE_URL`）。
- 内部依赖:
  - `RUNNER_PATH` 指向 `scripts/run_backtest.py` 供转发。
  - `RUN_STORAGE_PATH` 与 `RUN_MAPPING_PATH` 用于 backtest_id 存档与映射。

### 数据流/控制流
- 数据来源: 客户端 `POST /runs` 上传 `run_spec.json` 与策略文件（multipart/form-data）。
- 数据处理链路:
  1) 校验 run_spec（字段/费用/时间范围）。
  2) 生成 `backtest_id`，写入 `/opt/backtest/runs/{backtest_id}`。
  3) 保存策略文件与更新后的 `run_spec.json`（写入 backtest_id 与实际文件名）。
  4) 读取 runner 脚本，与 run_spec/策略一并转发至 backtest docker（multipart 字段: `runer`/`strategies`/`configs`）。
  5) 保存 `backtest_id -> backtest_docker_run_id` 映射（含 `created_at`）。
  6) 返回 `backtest_id` 与 `backtest_docker_run_id`。
- 查询链路:
  - `GET /runs/{backtest_id}`（需 API Key）读取映射并返回。
  - `GET /runs/backtest/{backtest_id}` 透传 backtest docker 状态（status/pid/started_at）。
  - `GET /runs/{backtest_id}/logs` 代理下载 backtest docker 日志。
- 日志流链路:
  - 客户端连接 `/runs/backtest/{backtest_id}/logs/stream`，服务端建立到 backtest docker 的 WebSocket 连接并双向转发消息。
  - `scripts/cli.py submit --follow-logs`（或 `scripts/submit_run.py --follow-logs`）通过 WebSocket 拉取日志并落盘到 `./live_logs/{backtest_id}.log`。
- 控制/调度流程: 映射文件通过 `threading.Lock` 保护读写；异常以 HTTP 4xx/5xx 返回并记录日志；backtest docker 请求失败统一返回 502。

### 关键配置
- 配置文件: `docker-compose.yml`, `run_spec.json`, `pyproject.toml`。
- 关键参数:
  - Hub 服务: `HOST_API_KEY`, `BACKTEST_API_BASE`, `BACKTEST_SUBMIT_PATH`, `BACKTEST_STATUS_PATH`, `BACKTEST_LOGS_PATH`, `BACKTEST_API_KEY`,
    `BACKTEST_WS_LOGS_PATH`, `DATA_MOUNT_PATH`, `RUN_STORAGE_PATH`, `RUN_MAPPING_PATH`, `BACKTEST_RUNNER_PATH`, `MAX_SYMBOLS`, `MAX_RANGE_DAYS`。
  - CLI: `BACKTEST_HUB_BASE_URL`（默认 `http://100.99.101.120:10033`）。
  - Runner 脚本: `CATALOG_PATH`，`BACKTEST_LOGS_PATH`（日志目录，默认 `/opt/backtest_logs`，与 Hub 的 `BACKTEST_LOGS_PATH` 为 URL 路径含义不同）。
- 运行环境约束: Python >= 3.12；可访问 backtest docker；挂载 `/opt/backtest` 数据目录；回测执行环境需要 `quant_trade_v1`。

### 运行流程
- 运行步骤:
  1) 启动服务（Docker/uvicorn）。
  2) 调用 `POST /runs` 上传 run_spec 与策略文件（需 `X-API-KEY`，或使用 `backtest-hub-cli submit`/`scripts/submit_run.py`）。
  3) 服务落盘并转发至 backtest docker，返回 `backtest_id`。
  4) 使用 `GET /runs/{backtest_id}` 查询映射信息（需 `X-API-KEY`）。
  5) 使用 `GET /runs/backtest/{backtest_id}` 查询 backtest docker 状态。
  6) 使用 `GET /runs/{backtest_id}/logs` 拉取 backtest 日志。
  7) 可选：连接 `/runs/backtest/{backtest_id}/logs/stream` 实时查看日志（或 `backtest-hub-cli submit --follow-logs`/`scripts/submit_run.py --follow-logs` 自动落盘）。
- 异常/边界处理: 参数校验失败返回 400；缺少 runner 返回 500；backtest docker 返回错误码时透传；backtest_id 不存在返回 404；`/runs` 与 `/runs/{backtest_id}` 需 `X-API-KEY`，其余查询/日志接口不做 API key 校验。
- 观测与日志: `app.py` 统一记录请求日志；`scripts/run_backtest.py` 写入 `status.json`（包含状态/错误/traceback）。

## 改动概要/变更记录

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
