# Backtest Hub

Backtest Hub is a central service for managing and executing trading strategy backtests. It provides an API for submitting backtest jobs, monitoring their status, and retrieving results.

## Features

- **Job Queue Management**: Handles backtest submissions and queues them for execution.
- **Docker Integration**: Executes backtests in isolated Docker containers.
- **Reporting Service**: Integrates with a reporting service to generate and serve backtest reports.
- **Real-time Status**: Provides endpoints to check the status of running backtests.
- **Data & Logs**: APIs for downloading backtest data and logs.

## Architecture

Built with Python using **FastAPI**. It orchestrates backtest runs by spinning up Docker containers and managing their lifecycle.

## Configuration

Top-level configuration is handled via environment variables, including:
- `BACKTEST_API_BASES`: List of available backtest execution nodes.
- `DATA_MOUNT_PATH`: Path for mounting data volumes.
- `RUN_STORAGE_PATH`: Path for storing run artifacts.

## API Documentation

The API documentation is available at `/docs` or `/redoc` when the service is running.
