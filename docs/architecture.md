# 工程架构梳理

- 当前梳理时间: 2026-01-28 22:18:53

## 项目概览
- 项目定位: FastAPI 服务，作为 backtest-hub 的中转层，接收研究端回测请求并转发至 backtest docker，同时维护 run_id 映射与日志查询。
- 主要能力: 接收 multipart 上传 run_spec.json 与策略文件；校验回测参数；落盘保存运行资料；调用 backtest docker 提交任务；查询 run_id 与日志。
- 关键输出: run_id/backtest_docker_run_id；/opt/backtest/runs 下的运行目录；run_mapping.json；日志代理返回。
- 参数说明: backtest-hub的run_id 等于backtest docker的 backtest_id

## 工程逻辑梳理

### 入口与启动
- 入口文件/命令: `Dockerfile` 使用 `uv run uvicorn app:app --host 0.0.0.0 --port 10033` 启动；主入口为 `app.py`。
- 启动流程概述: Uvicorn 加载 `app.py` 创建 FastAPI 应用，启动时输出日志；请求经中间件记录请求耗时与状态。

### 核心模块
- 模块划分:
  - `app.py`: API 层、参数校验、文件落盘、转发 backtest API、run_id 映射管理。
  - `scripts/run_backtest.py`: 回测 runner 脚本（由 backtest docker 执行），加载策略、构建配置并运行回测。
  - `strategies/`: 示例策略实现（如 `spot_futures_arb_diagnostics.py`）。
  - `docker-compose.yml`: 服务运行环境与环境变量配置。
  - `run_spec.json`: 回测配置样例。
- 关键职责:
  - `app.py`: 提供 `/health`、`POST /runs`、`GET /runs/{run_id}`、`GET /runs/{run_id}/logs`；校验字段/时间范围；生成 run_id；落盘保存；转发并记录 backtest_docker_run_id。
  - `scripts/run_backtest.py`: 校验 run_spec；加载策略模块；构建 spot/futures instruments 与 backtest configs；执行 `BacktestNode`；写入 status.json。
- 主要依赖:
  - Web 服务: `fastapi`, `uvicorn`, `python-multipart`。
  - 回测引擎: `quant_trade_v1`（由 backtest docker 运行时提供）。
  - HTTP 转发: `urllib.request` 调用 backtest docker API。

### 依赖关系
- 外部依赖:
  - backtest docker API（`BACKTEST_API_BASE` + `BACKTEST_SUBMIT_PATH`/`BACKTEST_LOGS_PATH`）。
  - 本地/挂载存储（默认 `/opt/backtest`）用于保存 run_spec 与策略文件。
- 内部依赖:
  - `RUNNER_PATH` 指向 `scripts/run_backtest.py` 供转发。
  - `RUN_STORAGE_PATH` 与 `RUN_MAPPING_PATH` 用于 run_id 存档与映射。

### 数据流/控制流
- 数据来源: 客户端 `POST /runs` 上传 `run_spec.json` 与策略文件（multipart/form-data）。
- 数据处理链路: 校验 run_spec → 生成 run_id → 写入 `/opt/backtest/runs/{run_id}` → 读取 runner 脚本 → 调用 backtest docker → 保存 run_id 映射 → 返回 run_id 与 backtest_docker_run_id。
- 控制/调度流程: 映射文件通过 `threading.Lock` 保护读写；异常以 HTTP 4xx/5xx 返回并记录日志。

### 关键配置
- 配置文件: `.env.example`, `docker-compose.yml`, `run_spec.json`。
- 关键参数: `HOST_API_KEY`, `BACKTEST_API_BASE`, `BACKTEST_SUBMIT_PATH`, `BACKTEST_LOGS_PATH`, `BACKTEST_API_KEY`, `DATA_MOUNT_PATH`, `RUN_STORAGE_PATH`, `RUN_MAPPING_PATH`, `BACKTEST_RUNNER_PATH`, `MAX_SYMBOLS`, `MAX_RANGE_DAYS`。
- 运行环境约束: Python >= 3.13；可访问 backtest docker；挂载 `/opt/backtest` 数据目录；回测执行环境需要 `quant_trade_v1`。

### 运行流程
- 运行步骤:
  1) 启动服务（Docker/uvicorn）。
  2) 调用 `POST /runs` 上传 run_spec 与策略文件。
  3) 服务落盘并转发至 backtest docker，返回 run_id。
  4) 使用 `GET /runs/{run_id}` 查询映射信息。
  5) 使用 `GET /runs/{run_id}/logs` 拉取 backtest 日志。
- 异常/边界处理: 参数校验失败返回 400；缺少 runner 返回 500；backtest docker 返回错误码时透传；run_id 不存在返回 404。
- 观测与日志: `app.py` 统一记录请求日志；`scripts/run_backtest.py` 写入 `status.json`（包含状态/错误/traceback）。

## 改动概要/变更记录

### 2026-01-28 22:18:53
- 本次新增/更新要点: 新建 `docs/architecture.md`，梳理入口、核心模块、依赖关系、数据流、关键配置与运行流程。
- 变更动机/需求来源: 用户要求执行 architecture-doc-updater，补全工程架构文档。
- 当前更新时间: 2026-01-28 22:18:53
