from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from typing import Any

DEFAULT_BASE_URL = os.getenv("BACKTEST_HUB_BASE_URL", "http://100.99.101.120:10033")


def _build_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}{path}"


def _request_json(url: str) -> dict[str, Any]:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read()
        payload = json.loads(body.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
    except Exception as exc:
        raise RuntimeError(f"Request failed: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("Invalid response payload")
    return payload


def _request_bytes(url: str) -> bytes:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
    except Exception as exc:
        raise RuntimeError(f"Request failed: {exc}") from exc


def get_backtest_status(backtest_id: str, base_url: str | None = None) -> dict[str, Any]:
    base = base_url or DEFAULT_BASE_URL
    url = _build_url(base, f"/runs/backtest/{backtest_id}")
    return _request_json(url)


def download_backtest_logs(
    backtest_id: str,
    output_path: str | None = None,
    base_url: str | None = None,
) -> str:
    base = base_url or DEFAULT_BASE_URL
    url = _build_url(base, f"/runs/{backtest_id}/logs")
    content = _request_bytes(url)
    target_path = output_path or f"{backtest_id}.log"
    with open(target_path, "wb") as handle:
        handle.write(content)
    return target_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="backtest-hub SDK CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser(
        "get-backtest-status",
        help="Get backtest status (status/pid/started_at) from backtest-hub",
    )
    status_parser.add_argument("backtest_id", help="Backtest ID")

    logs_parser = subparsers.add_parser(
        "download-backtest-logs",
        help="Download backtest stdout logs from backtest-hub",
    )
    logs_parser.add_argument("backtest_id", help="Backtest ID")
    logs_parser.add_argument(
        "--out",
        default=None,
        help="Output file path (default: {backtest_id}.log)",
    )

    return parser.parse_args()

#uv run scripts/backtest_hub_sdk.py get-backtest-status 20260128T150900Z_1d9ab8
#uv run scripts/backtest_hub_sdk.py download-backtest-logs 20260128T150900Z_1d9ab8
def main() -> int:
    args = _parse_args()
    if args.command == "get-backtest-status":
        payload = get_backtest_status(args.backtest_id)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    if args.command == "download-backtest-logs":
        target_path = download_backtest_logs(args.backtest_id, args.out)
        print(target_path)
        return 0
    raise RuntimeError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
