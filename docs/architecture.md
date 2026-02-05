# 工程架构梳理

- 当前梳理时间: 2026-02-05 01:50:00

## 项目概览
- 项目定位: FastAPI 服务，作为 backtest-hub 的中转层，接收研究端回测请求并转发至 backtest docker，同时维护 backtest_id 映射、状态查询、日志下载与日志流；并在 hub 侧提供并发队列限制（避免 backtest docker 被无限提交打爆）。
- 主要能力: 接收 multipart 上传 run_spec.json 与策略文件；校验回测参数；落盘保存运行资料；按 system metrics + symbols 估算内存进行调度与排队；调用 backtest docker 提交任务；查询 backtest_id 映射、状态、日志下载与 WebSocket 日志流；提供队列查询与队列任务删除接口。
- 关键输出: backtest_id/backtest_docker_run_id（可能为空，表示 queued）；`/opt/backtest/runs` 下的运行目录；`run_mapping.json`；`submit_queue.json`；日志下载/日志流代理返回。
- 参数说明: backtest-hub 的 `backtest_id` 会作为 backtest docker 的 `backtest_id` 字段；backtest docker 返回的 `run_id` 记录为 `backtest_docker_run_id`。

## 工程逻辑梳理

### 入口与启动
- 入口文件/命令: `Dockerfile` 使用 `uv run uvicorn app:app --host 0.0.0.0 --port 10033` 启动；主入口为 `app.py`。
- 启动流程概述: Uvicorn 加载 `app.py` 创建 FastAPI 应用，启动时输出日志；请求经中间件记录耗时与状态。

### 核心模块
- 模块划分:
  - `app.py`: API 层、参数校验、文件落盘、转发 backtest API、backtest_id 映射管理。
  - `services/scheduler.py`: 多 backtest docker 调度逻辑（拉取 system metrics、按 symbols 估算内存、选择目标 docker）。
  - `services/report_service.py`: 回测报告透传与 SQLite 缓存、聚合分页与 requested_by 过滤；提供 `/runs/backtest/reports`、`/runs/backtest/{backtest_id}/report`、`/runs/backtest/{backtest_id}/download_data`、`/runs/backtest/active` 的路由实现与独立 Swagger 文档。
  - `scripts/run_backtest.py`: 回测 runner 脚本（由 backtest docker 执行），加载策略、构建配置并运行回测。
  - `backtest_hub_cli/cli.py`: Hub CLI 包入口，负责提交/查询/下载日志/日志流；`submit` 支持 `--strategy-file`，未提供则回退 run_spec 或 `./strategies/<basename>`；提供 `init` 生成模板脚本与 `help` 命令。
  - `backtest_hub_cli/scripts/generate_run_spec.py`: CLI 模板脚本，`init` 命令会复制到项目 `./scripts/generate_run_spec.py`（包含 `latency_config` 示例字段）。
  - `scripts/generate_run_spec.py`: 用户自定义的 run_spec 生成脚本（由 `init` 创建，用于本地生成 `run_spec.json`）。
  - `scripts/submit_run.py`: 提交 CLI 的薄封装（转发至 `backtest_hub_cli.cli`）。
  - `scripts/backtest_hub_sdk.py`: 兼容旧命令的 CLI 封装（转发至 `backtest_hub_cli.cli`）。
  - `strategies/`: 示例策略实现（如 `spot_futures_arb_diagnostics.py`）。
  - `docker-compose.yml`: 服务运行环境与环境变量配置。
  - `run_spec.json`: 回测配置样例。
- 关键职责:
  - `app.py`: 提供 `/health`、`POST /runs`、`GET /runs/{backtest_id}`、`GET /runs/backtest/reports`、`GET /runs/backtest/active`、`GET /runs/backtest/{backtest_id}`、`GET /runs/backtest/{backtest_id}/report`、`GET /runs/{backtest_id}/logs`、`GET /runs/backtest/{backtest_id}/download_csv`、`GET /runs/backtest/{backtest_id}/download_data`、`POST /runs/backtest/{backtest_id}/kill`、`/runs/backtest/{backtest_id}/logs/stream`、`GET /queue`、`DELETE /queue`；校验字段/时间范围/`latency_config`；生成 backtest_id；落盘保存；调用调度器按 system metrics + symbols 估算内存选择目标 backtest docker；队列非空或内存不足则入队；后台调度器按 FIFO 出队并提交；维护 mapping（包含 queued/submitted/cancelled 状态、时间戳与 `backtest_api_base`）；通过 WebSocket 代理日志流；回测报告透传与 SQLite 缓存（支持分页聚合与 requested_by 过滤）。
  - `scripts/run_backtest.py`: 校验 run_spec；动态加载策略模块；构建 spot/futures instruments 与 backtest configs；执行 `BacktestNode`；写入 `status.json`（运行/成功/失败）。
  - `backtest_hub_cli/cli.py`: `init` 复制模板脚本到 `./scripts/generate_run_spec.py`；`submit` 调用本地 `scripts/generate_run_spec.py` 生成 run_spec（若未 init 则提示），`--strategy-file` 可指定策略文件（否则回退 run_spec 或 `./strategies/<basename>`）；可选 `--follow-logs` WebSocket 写入 `./live_logs/{backtest_id}.log`；`status` 查询 backtest 状态；`logs` 下载日志；`download-csv` 下载回测 CSV ZIP；`kill` 停止回测；`help` 展示命令总览；并写入 `./backtest_run_id_history` 记录历史。
  - `scripts/submit_run.py`: 仅负责转发 CLI 的 submit 命令。
  - `scripts/backtest_hub_sdk.py`: 兼容命令 `get-backtest-status`/`download-backtest-logs`，内部转发至 CLI 的 `status`/`logs`。
- 主要依赖:
  - Web 服务: `fastapi`, `uvicorn`, `python-multipart`, `aiohttp`（WebSocket 代理）。
  - 回测引擎: `quant_trade_v1`（由 backtest docker 运行时提供）。
  - HTTP 转发: `urllib.request` 调用 backtest docker API。

### 依赖关系
- 外部依赖:
  - backtest docker API（`BACKTEST_API_BASES` + `BACKTEST_SUBMIT_PATH`/`BACKTEST_STATUS_PATH`/`BACKTEST_REPORT_PATH`/`BACKTEST_RUNS_PATH`）与日志下载固定路径 `/v1/runs/backtest/{backtest_id}/logs/download`、CSV `/v1/runs/backtest/{backtest_id}/download_csv`、数据包 `/v1/runs/backtest/{backtest_id}/download_data`、kill `/v1/runs/backtest/{backtest_id}/kill`；日志流 WebSocket（`BACKTEST_WS_LOGS_PATH`）。
  - backtest docker system metrics（`BACKTEST_METRICS_PATH`），用于 CPU/内存占用与调度决策。
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
  1) 校验 run_spec（字段/费用/时间范围/`latency_config`）。
  2) 生成 `backtest_id`，写入 `/opt/backtest/runs/{backtest_id}`。
  3) 保存策略文件与更新后的 `run_spec.json`（写入 backtest_id 与实际文件名）。
  4) 拉取各 backtest docker 的 system metrics（`BACKTEST_METRICS_PATH`），按 symbols 数量估算所需内存（1 symbol ≈ 1GB），选择可用内存满足且运行数未超限的 docker；若队列非空则直接入队（不绕过 FIFO），内存不足也入队。
  5) 若立即提交：读取 runner 脚本，与 run_spec/策略一并转发至 backtest docker（multipart 字段: `runer`/`strategies`/`configs`），并记录 `backtest_docker_run_id`。
  6) 保存 mapping（包含 `status`、`queued_at`/`submitted_at`/`cancelled_at`、`created_at` 等）与队列文件（如排队）。
  7) 返回 `backtest_id`，并返回 `status`（`submitted` 或 `queued`）；当 `queued` 时 `backtest_docker_run_id` 为空。
- 查询链路:
  - `GET /runs/{backtest_id}` 读取映射并返回。
  - `GET /runs/backtest/{backtest_id}` 根据映射中的 `backtest_api_base` 透传目标 backtest docker 状态（status/pid/started_at）。
  - `GET /runs/backtest/{backtest_id}/report` 根据映射中的 `backtest_api_base` 透传回测报告，并在 hub 侧落入 SQLite 缓存（仅成功报告）。
  - `GET /runs/backtest/reports` 按映射中的时间倒序聚合回测报告（分页 + requested_by 过滤）；优先命中 SQLite 缓存，未命中则透传 backtest docker，失败或不可用报告跳过；聚合结果每条记录补充 `backtest_api_base`。
  - `GET /runs/backtest/active` 透传并汇总所有 backtest docker 的活跃任务（starting/running/stopping），按 created_at 倒序返回；聚合结果每条记录补充 `backtest_api_base`。
  - `GET /runs/{backtest_id}/logs` 根据映射中的 `backtest_api_base` 代理下载 backtest docker 日志。
  - `GET /runs/backtest/{backtest_id}/download_csv` 根据映射中的 `backtest_api_base` 代理下载 backtest docker 回测 CSV ZIP。
  - `GET /runs/backtest/{backtest_id}/download_data` 根据映射中的 `backtest_api_base` 代理下载 backtest docker 回测完整数据包 ZIP。
  - `POST /runs/backtest/{backtest_id}/kill` 根据映射中的 `backtest_api_base` 代理停止回测任务并返回更新后的任务记录。
- 队列管理链路:
  - `GET /queue` 查询当前 hub 队列（支持分页与查询 position）。
  - `DELETE /queue` 批量删除队列中的任务（仅影响 queued 任务，删除后标记为 cancelled）。
- 队列调度链路:
  - hub 启动后后台调度器周期性拉取各 backtest docker 的 system metrics，按可用内存（扣除 inflight 预占）与 FIFO 出队并提交回测；若内存不足则保持排队。
  - 每次成功提交后等待 `QUEUE_DISPATCH_DELAY_SECONDS`（默认 30s），让目标节点 CPU/RAM 有时间反映新负载，避免并发提交绕过 80% CPU 阈值检查。
- 日志流链路:
  - 客户端连接 `/runs/backtest/{backtest_id}/logs/stream`，服务端按映射中的 `backtest_api_base` 建立到目标 backtest docker 的 WebSocket 连接并双向转发消息。
  - `backtest-hub-cli submit --follow-logs`（或 `scripts/submit_run.py --follow-logs`）通过 WebSocket 拉取日志并落盘到 `./live_logs/{backtest_id}.log`。
- 控制/调度流程:
  - 调度要求 CPU < 80%；按内存优先：每个 symbol 估算 1GB，依据 metrics 计算可用内存（`total - used - inflight`）决定是否可提交；满足内存后再检查 running 上限（按 docker 维度）。
  - 若 symbols 数量 < 6 且青铜分组 docker 的 CPU < 80% 且可用内存大于估算内存（1 symbol ≈ 1GB），则优先调度到青铜分组。
  - 队列非空时新提交直接排队，避免绕过 FIFO；提交前预留 inflight 内存/计数，避免并发超发。
  - 队列与 inflight 通过 `asyncio.Lock` 保护；mapping 文件通过 `threading.Lock` 保护读写。
  - 异常以 HTTP 4xx/5xx 返回并记录日志；backtest docker 请求失败统一返回 502；调度提交失败会回队并记录 `last_error`。

### 关键配置
- 配置文件: `docker-compose.yml`, `run_spec.json`, `pyproject.toml`。
- 关键参数:
  - Hub 服务: `HOST_API_KEY`（预留鉴权开关，当前接口未启用）、`BACKTEST_API_BASES`, `BACKTEST_BRONZE_API_BASES`, `BACKTEST_SUBMIT_PATH`, `BACKTEST_STATUS_PATH`,
    `BACKTEST_REPORT_PATH`, `BACKTEST_API_KEY`, `BACKTEST_WS_LOGS_PATH`, `BACKTEST_RUNS_PATH`, `BACKTEST_METRICS_PATH`, `BACKTEST_DATA_DOWNLOAD_PATH`,
    `BACKTEST_METRICS_TIMEOUT_SECONDS`, `DATA_MOUNT_PATH`, `RUN_STORAGE_PATH`, `RUN_MAPPING_PATH`, `REPORT_CACHE_PATH`, `QUEUE_PATH`,
    `BACKTEST_RUNNER_PATH`, `MAX_SYMBOLS`, `MAX_RANGE_DAYS`, `MAX_RUNNING_BACKTESTS`, `MAX_REPORT_PAGE_SIZE`,
    `QUEUE_POLL_INTERVAL_SECONDS`, `QUEUE_DISPATCH_DELAY_SECONDS`。
  - 固定路径: 日志下载 `/v1/runs/backtest/{backtest_id}/logs/download`，CSV `/v1/runs/backtest/{backtest_id}/download_csv`，数据包 `/v1/runs/backtest/{backtest_id}/download_data`，kill `/v1/runs/backtest/{backtest_id}/kill`。
  - 文档: report_service 独立 Swagger `/report-service/docs`（OpenAPI `/report-service/openapi.json`）；主 Swagger `/docs` 不包含 report_service 路由。
  - CLI: `BACKTEST_HUB_BASE_URL`（默认 `http://100.87.155.67:10033`）。
  - Runner 脚本: `CATALOG_PATH`，`BACKTEST_LOGS_PATH`（日志目录，默认 `/opt/backtest_logs`，与 Hub 的日志下载固定路径含义不同）。
- 运行环境约束: Python >= 3.12；可访问 backtest docker；挂载 `/opt/backtest` 数据目录；回测执行环境需要 `quant_trade_v1`。

### 运行流程
- 运行步骤:
  1) 启动服务（Docker/uvicorn）。
  2) 执行 `backtest-hub-cli init` 生成 `./scripts/generate_run_spec.py`，按需修改参数。
  3) 调用 `POST /runs` 上传 run_spec 与策略文件（或使用 `backtest-hub-cli submit`/`scripts/submit_run.py`）。
  4) 服务落盘；若队列非空则新提交直接排队；队列为空且目标 backtest docker 可用内存满足（且未达 running 上限）则立即转发提交，否则进入队列并返回 `status=queued`。
  5) 使用 `GET /runs/{backtest_id}` 查询映射信息。
  6) 使用 `GET /runs/backtest/{backtest_id}` 查询 backtest docker 状态。
  7) 使用 `GET /runs/{backtest_id}/logs` 拉取 backtest 日志。
  8) 使用 `GET /runs/backtest/{backtest_id}/download_csv` 下载回测 CSV ZIP。
  9) 使用 `GET /runs/backtest/{backtest_id}/download_data` 下载回测完整数据包 ZIP。
  10) 使用 `GET /runs/backtest/active` 查看活跃回测任务列表。
  11) 使用 `POST /runs/backtest/{backtest_id}/kill` 停止回测任务。
  12) 可选：连接 `/runs/backtest/{backtest_id}/logs/stream` 实时查看日志（或 `backtest-hub-cli submit --follow-logs`/`scripts/submit_run.py --follow-logs` 自动落盘）。
  13) 可选：使用 `GET /queue` 查询排队情况；使用 `DELETE /queue` 批量删除排队任务（标记 cancelled）。
- 异常/边界处理: 参数校验失败返回 400；缺少 runner 返回 500；backtest docker 返回错误码时透传；backtest_id 不存在返回 404。
- 观测与日志: `app.py` 统一记录请求日志；`scripts/run_backtest.py` 写入 `status.json`（包含状态/错误/traceback）。

## 改动概要/变更记录

### 2026-02-05 01:50:00
- 本次新增/更新要点: 新增 `QUEUE_DISPATCH_DELAY_SECONDS` 配置（默认 30s）；队列调度成功提交后等待延时，让目标节点 metrics 有时间更新，避免并发提交绕过 80% CPU 阈值检查；新增 `tests/test_scheduler.py` 单元测试（33 tests）覆盖调度器核心函数。
- 变更动机/需求来源: 用户要求补充调度器测试并解决并发提交打满服务器 CPU/RAM 的问题。
- 当前更新时间: 2026-02-05 01:50:00

### 2026-02-04 21:15:25
- 本次新增/更新要点: `/runs/backtest/reports` 与 `/runs/backtest/active` 聚合结果补充 `backtest_api_base`，便于识别来源 backtest docker。
- 变更动机/需求来源: 用户要求聚合接口体现数据来源的 backtest docker。
- 当前更新时间: 2026-02-04 21:15:25

### 2026-02-04 20:49:35
- 本次新增/更新要点: 主 Swagger `/docs` 过滤 report_service 路由；report_service 仅在 `/report-service/docs` 展示。
- 变更动机/需求来源: 用户要求 report_service 仅在独立 Swagger 文档路径展示。
- 当前更新时间: 2026-02-04 20:49:35

### 2026-02-04 20:20:57
- 本次新增/更新要点: 新增 `/runs/backtest/active` 汇总 backtest docker 活跃任务接口；report_service 聚合活跃任务列表并按 created_at 倒序返回。
- 变更动机/需求来源: 用户要求 hub 新增活跃回测任务列表聚合接口。
- 当前更新时间: 2026-02-04 20:20:57

### 2026-02-04 20:09:56
- 本次新增/更新要点: `/runs/backtest/{backtest_id}/download_data` 与 report_service 相关接口路由迁移到 `services/report_service.py`；新增 report_service 独立 Swagger 文档路径。
- 变更动机/需求来源: 用户要求 report_service 接口具备独立文档并将 download_data 迁移至 report_service。
- 当前更新时间: 2026-02-04 20:09:56

### 2026-02-04 19:24:35
- 本次新增/更新要点: 新增 `/runs/backtest/{backtest_id}/download_data` 代理 backtest docker 回测完整数据包下载；新增 `BACKTEST_DATA_DOWNLOAD_PATH` 配置；新增 download_data 的 backtest_id 非法字符校验。
- 变更动机/需求来源: 用户要求 hub 对接 backtest docker 下载回测完整数据包接口。
- 当前更新时间: 2026-02-04 19:24:35

### 2026-02-04 19:03:28
- 本次新增/更新要点: 补充 run_spec 的 `latency_config` 可选校验与模板说明；更新队列 FIFO 规则与 inflight 预占说明；修正日志下载固定路径与 CLI 默认地址；补充 CLI `--strategy-file` 回退逻辑。
- 变更动机/需求来源: 用户要求根据最新代码更新架构文档。
- 当前更新时间: 2026-02-04 19:03:28

### 2026-02-04 16:51:58
- 本次新增/更新要点: 新增回测报告透传接口与分页聚合接口；报告成功结果落入 SQLite 缓存；支持 requested_by 过滤。
- 变更动机/需求来源: 用户要求 hub 聚合多 backtest docker 回测报告并提供缓存能力。
- 当前更新时间: 2026-02-04 16:51:58

### 2026-02-04 12:25:04
- 本次新增/更新要点: 普通调度新增 CPU < 80% 过滤门槛；统一 CPU 阈值配置。
- 变更动机/需求来源: 用户要求普通分组也受 CPU 阈值限制。
- 当前更新时间: 2026-02-04 12:25:04

### 2026-02-04 12:14:57
- 本次新增/更新要点: 新增青铜分组配置 `BACKTEST_BRONZE_API_BASES`；调度规则新增“小币对优先青铜”（symbols < 6 且 CPU < 80% 且可用内存大于估算内存）。
- 变更动机/需求来源: 用户要求优化调度逻辑并新增青铜分组配置。
- 当前更新时间: 2026-02-04 12:14:57

### 2026-02-03 19:24:19
- 本次新增/更新要点: Hub 新增 `/runs/backtest/{backtest_id}/kill` 代理停止回测；CLI 新增 `kill` 命令。
- 变更动机/需求来源: 用户要求 Hub 代理 backtest docker kill 接口并提供 CLI 能力。
- 当前更新时间: 2026-02-03 19:24:19

### 2026-02-03 18:12:18
- 本次新增/更新要点: Hub 新增 `/runs/backtest/{backtest_id}/download_csv` 代理 backtest docker CSV ZIP 下载；CLI 新增 `download-csv` 命令。
- 变更动机/需求来源: 用户要求 Hub 提供 CSV 下载代理与 CLI 能力。
- 当前更新时间: 2026-02-03 18:12:18

### 2026-02-03 16:34:37
- 本次新增/更新要点: 新增 `services/scheduler.py`，按 system metrics + symbols 估算内存选择目标 backtest docker；提交/队列调度按目标 docker 路由；日志/状态/日志流代理依据 `backtest_api_base`；新增多 docker 配置项与 metrics 参数。
- 变更动机/需求来源: 用户要求支持多 backtest docker 并基于内存调度。
- 当前更新时间: 2026-02-03 16:34:37

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
