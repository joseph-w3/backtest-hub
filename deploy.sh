#!/usr/bin/env bash
set -euo pipefail

docker-compose down
docker-compose build --no-cache
docker-compose up -d

echo "backtest-hub 已部署，监听端口 10033。"
