#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

docker-compose down
docker-compose build --no-cache
docker-compose up -d

echo "backtest-hub 已部署，监听端口 10033。"
