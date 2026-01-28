# Quant Script API 对接文档

本文档描述了 quant-script-api 的所有 HTTP 和 WebSocket 接口。

**注意**: 所有路径均基于 API 前缀 (默认为空或根据配置设定)。

## 1. 系统检测 (Health Check)

*   **URL**: `GET /health`
*   **描述**: 检查服务状态及基本配置信息。
*   **鉴权**: 无
*   **请求参数**: 无
*   **响应**:
    ```json
    {
      "status": "ok",
      "scripts_root": "/path/to/scripts",
      "jwt_auth": true
    }
    ```

## 2. 脚本管理


### 2.4 运行回测 (Run Backtest)

*   **URL**: `POST /scripts/run_backtest`
*   **描述**: 上传 runner、策略和配置文件，并执行回测任务。
*   **鉴权**: `scripts:write`, `scripts:run`
*   **请求数据 (Form Data)**:
    *   `backtest_id` (String): 唯一的回测 ID，将用于创建独立目录。
    *   `runer` (File): Runner 脚本文件。
    *   `strategies` (File): 策略文件。
    *   `configs` (File): 配置文件。
*   **响应 (RunRecord)**:
    ```json
    {
      "run_id": "550e8400-e29b-41d4-a716-446655440000",
      "script": "backtest_001/runner.py",
      "argv": [
        "/usr/bin/python3",
        "-u",
        "/absolute/path/to/scripts/backtest_001/runner.py",
        "--run-spec",
        "config.json"
      ],
      "status": "starting",
      "pid": 12345,
      "return_code": null,
      "created_at": "2023-10-27T10:00:00.000000+00:00",
      "started_at": null,
      "finished_at": null,
      "stdout_path": "/absolute/path/to/logs/550e8400-e29b-41d4-a716-446655440000.stdout.log",
      "stderr_path": "/absolute/path/to/logs/550e8400-e29b-41d4-a716-446655440000.stderr.log",
      "error": null
    }
    ```

## 3. 任务管理 (Runs)

### 3.1 获取所有任务列表

*   **URL**: `GET /runs`
*   **描述**: 获取历史及当前所有运行任务。
*   **鉴权**: `scripts:read`
*   **请求参数**: 无
*   **响应**:
    ```json
    {
      "count": 1,
      "runs": [
        {
          "run_id": "550e8400-e29b-41d4-a716-446655440000",
          "script": "strategies/demo.py",
          "argv": ["python", "..."],
          "status": "succeeded",
          "pid": 12345,
          "return_code": 0,
          "created_at": "2023-10-27T10:00:00+00:00",
          "started_at": "2023-10-27T10:00:01+00:00",
          "finished_at": "2023-10-27T10:05:00+00:00",
          "stdout_path": "/path/to/logs/....stdout.log",
          "stderr_path": "/path/to/logs/....stderr.log",
          "error": null
        }
      ]
    }
    ```

### 3.2 获取活跃任务列表

*   **URL**: `GET /runs/active`
*   **描述**: 仅获取当前正在运行或启动中的任务。
*   **鉴权**: `scripts:read`
*   **请求参数**: 无
*   **响应**: 同 `GET /runs`，仅包含状态为 `starting`, `running`, `stopping` 的任务。

### 3.3 启动新任务

*   **URL**: `POST /runs`
*   **描述**: 运行指定的脚本。
*   **鉴权**: `scripts:run`
*   **请求体 (JSON)**:
    ```json
    {
      "script": "strategies/demo.py",
      "args": ["--verbose", "--mode=prod"],
      "env": {
        "MY_VAR": "value"
      },
      "cwd": ".",
      "duplicate": false
    }
    ```
*   **响应**:
    ```json
    {
      "run_id": "new-uuid-string",
      "script": "strategies/demo.py",
      "argv": ["python", ...],
      "status": "starting",
      "pid": null,
      "return_code": null,
      "created_at": "2023-10-27T12:00:00+00:00",
      "started_at": null,
      "finished_at": null,
      "stdout_path": "/path/to/logs/....stdout.log",
      "stderr_path": "/path/to/logs/....stderr.log",
      "error": null
    }
    ```

### 3.4 获取回测任务详情 (根据 Backtest ID)

*   **URL**: `GET /runs/backtest/{backtest_id}`
*   **描述**: 获取特定回测任务的详细状态。
*   **鉴权**: `scripts:read`
*   **响应**:
    ```json
    {
      "run_id": "550e8400-e29b-41d4-a716-446655440000",
      "script": "strategies/demo.py",
      "argv": [
        "python",
        "strategies/demo.py",
        "--run-spec",
        "config.json"
      ],
      "status": "running",
      "pid": 12345,
      "return_code": null,
      "created_at": "2023-10-27T10:00:00+00:00",
      "started_at": "2023-10-27T10:00:01+00:00",
      "finished_at": null,
      "stdout_path": "/path/to/logs/....stdout.log",
      "stderr_path": "/path/to/logs/....stderr.log",
      "error": null
    }
    ```

### 3.5 获取回测任务状态 (Hub 代理)

*   **URL**: `GET /runs/backtest/{backtest_id}`
*   **描述**: backtest-hub 代理调用 backtest docker 的任务详情接口，仅返回必要字段供研究端轮询。
*   **鉴权**: 无
*   **响应**:
    ```json
    {
      "status": "running",
      "pid": 12345,
      "started_at": "2023-10-27T10:00:01+00:00"
    }
    ```


## 4. 日志服务


### 4.3 获取回测日志 (根据 Backtest ID)

*   **URL**: `GET /runs/backtest/{backtest_id}/logs`
*   **描述**: 通过 Backtest ID 获取对应任务的日志。
*   **鉴权**: `logs:read`
*   **请求参数**:
    *   `stream` (Query, Optional): 日志流类型，可选值 `stdout`, `stderr`, `both`。默认为 `stdout`。
    *   `tail_lines` (Query, Optional): 获取最后多少行日志。默认为 `100`。
*   **响应**:
    ```json
    {
      "run_id": "550e8400-e29b-41d4-a716-446655440000",
      "stream": "stdout",
      "tail_lines": 100,
      "stdout": "Starting backtest...\nLoading data...",
      "stderr": null
    }
    ```

### 4.4 实时回测日志流 (WebSocket)

*   **URL**: `WS /runs/backtest/{backtest_id}/logs/stream`
*   **描述**: 通过 Backtest ID 建立 WebSocket 日志流。
*   **消息格式 (Server -> Client)**:
    ```json
    {
      "stream": "stdout",
      "data": "Processing chunk 1...\n"
    }
    ```
