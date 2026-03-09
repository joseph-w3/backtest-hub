# Change Log

## [Unreleased]

### Added
- Linked `download_code` and `/logs/download` endpoints in `services/report_service.py` to proxy requests to backtest nodes using `httpx`.
- Fixed critical bug in `download_logs` and `download_code` where `httpx.AsyncClient` was closed prematurely due to async generator lifecycle issues. Now managing client lifecycle in the endpoint.
- Fixed resource leak (double-closure) in download endpoints and ensured proper cleanup in streaming generators.
- Added `backtest_id` input validation to prevent SSRF and path injection risks.
- Deleted redundant `services/report_service_bak.py`.
- Added timeout configuration to `httpx.AsyncClient` for report downloads.
- Fixed 404 error in `download_logs` and `download_code` by adding missing `/v1` prefix to upstream URLs.
- Changed default scheduler metrics source to `v1/system/metrics` in `docker-compose.yml` so worker selection uses host-level memory instead of docker cgroup memory unless explicitly overridden.


## [Unreleased]

### Added
- Added `BACKTEST_METRICS_PATH` configuration endpoint setting.
