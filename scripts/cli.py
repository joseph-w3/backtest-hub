from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import aiohttp

HUB_BASE_URL = os.getenv("BACKTEST_HUB_BASE_URL", "http://100.87.155.67:10033")


def build_multipart_form(files: list[tuple[str, str, str, bytes]]) -> tuple[bytes, str]:
    boundary = "----research-submit-" + os.urandom(16).hex()
    body = bytearray()
    for field_name, filename, content_type, content in files:
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(
            f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'.encode(
                "utf-8"
            )
        )
        body.extend(f"Content-Type: {content_type}\r\n\r\n".encode("utf-8"))
        body.extend(content)
        body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode("utf-8"))
    content_type = f"multipart/form-data; boundary={boundary}"
    return bytes(body), content_type


def generate_run_spec(run_spec_path: Path, strategy_path: Path) -> None:
    script_path = Path(__file__).resolve().parent / "generate_run_spec.py"
    command = [
        sys.executable,
        str(script_path),
        "--output",
        str(run_spec_path),
        "--strategy-file",
        str(strategy_path),
    ]
    subprocess.run(command, check=True)


def extract_run_id(response_body: str) -> str | None:
    try:
        data = json.loads(response_body)
    except json.JSONDecodeError:
        return None
    if isinstance(data, dict) and data.get("backtest_id"):
        return str(data["backtest_id"])
    return None


def write_run_id_history(backtest_id: str, name: str, history_path: Path) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    safe_name = " ".join(str(name).split())
    new_line = f"{timestamp} {backtest_id} {safe_name}\n"
    if history_path.exists():
        existing = history_path.read_text(encoding="utf-8")
    else:
        existing = ""
    history_path.write_text(new_line + existing, encoding="utf-8")


def build_ws_url(base_url: str, path: str) -> str:
    parsed = urlparse(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    base_path = parsed.path.rstrip("/")
    normalized_path = "/" + path.lstrip("/")
    full_path = f"{base_path}{normalized_path}"
    return urlunparse((scheme, parsed.netloc, full_path, "", "", ""))


async def stream_logs(ws_url: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Streaming logs to {output_path}")
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url) as ws:
            with output_path.open("a", encoding="utf-8") as handle:
                async for message in ws:
                    if message.type == aiohttp.WSMsgType.TEXT:
                        text = message.data
                    elif message.type == aiohttp.WSMsgType.BINARY:
                        text = message.data.decode("utf-8", errors="replace")
                    elif message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                        break
                    elif message.type == aiohttp.WSMsgType.ERROR:
                        break
                    else:
                        continue
                    data = None
                    try:
                        payload = json.loads(text)
                        if isinstance(payload, dict):
                            data = payload.get("data")
                    except json.JSONDecodeError:
                        data = text
                    if data:
                        handle.write(str(data))
                        handle.flush()


def build_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}{path}"


def request_json(url: str) -> dict[str, Any]:
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


def request_bytes(url: str) -> bytes:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
    except Exception as exc:
        raise RuntimeError(f"Request failed: {exc}") from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest Hub CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    help_parser = subparsers.add_parser("help", help="Show command overview and key parameters")
    help_parser.set_defaults(_command_description=True)

    submit_parser = subparsers.add_parser("submit", help="Submit run_spec.json and strategy file")
    submit_parser.add_argument("--api-key", default=os.getenv("HOST_API_KEY", ""))
    submit_parser.add_argument(
        "--strategy-file",
        default="strategies/spot_futures_arb_diagnostics.py",
        help="Path to strategy file",
    )
    submit_parser.add_argument(
        "--run-spec",
        default="run_spec.json",
        help="Path to run_spec.json (will be generated if missing or --no-generate is not set)",
    )
    submit_parser.add_argument(
        "--name",
        required=True,
        help="Strategy name for this submission (required, used in run id history)",
    )
    submit_parser.add_argument(
        "--no-generate",
        action="store_true",
        help="Skip generating run_spec.json",
    )
    submit_parser.add_argument(
        "--follow-logs",
        action="store_true",
        help="After submit, stream WebSocket logs into ./live_logs/{backtest_id}.log",
    )

    status_parser = subparsers.add_parser("status", help="Get backtest status (status/pid/started_at)")
    status_parser.add_argument("backtest_id", help="Backtest ID")

    logs_parser = subparsers.add_parser("logs", help="Download backtest logs")
    logs_parser.add_argument("backtest_id", help="Backtest ID")
    logs_parser.add_argument(
        "--out",
        default=None,
        help="Output file path (default: {backtest_id}.log)",
    )

    return parser.parse_args(argv)


def command_submit(args: argparse.Namespace) -> int:
    if not args.api_key:
        print("HOST_API_KEY is required")
        return 1

    run_spec_path = Path(args.run_spec)
    strategy_path = Path(args.strategy_file)
    if not strategy_path.is_file():
        print(f"strategy file not found: {strategy_path}")
        return 1

    if not args.no_generate or not run_spec_path.exists():
        try:
            generate_run_spec(run_spec_path, strategy_path)
        except subprocess.CalledProcessError as exc:
            print(f"generate_run_spec failed: {exc}")
            return 1

    with run_spec_path.open("r") as handle:
        payload = json.load(handle)

    run_spec_bytes = json.dumps(payload).encode("utf-8")
    strategy_bytes = strategy_path.read_bytes()
    body, content_type = build_multipart_form(
        files=[
            ("run_spec", "run_spec.json", "application/json", run_spec_bytes),
            ("strategy_file", strategy_path.name, "text/x-python", strategy_bytes),
        ]
    )

    req = urllib.request.Request(
        f"{HUB_BASE_URL.rstrip('/')}/runs",
        data=body,
        headers={"Content-Type": content_type, "X-API-KEY": args.api_key},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req) as resp:
            body_text = resp.read().decode("utf-8")
            print(body_text)
            backtest_id = extract_run_id(body_text)
            if backtest_id:
                history_path = Path(__file__).resolve().parent / "backtest_run_id_history"
                write_run_id_history(backtest_id, args.name, history_path)
                if args.follow_logs:
                    ws_url = build_ws_url(
                        HUB_BASE_URL,
                        f"/runs/backtest/{backtest_id}/logs/stream",
                    )
                    output_path = Path.cwd() / "live_logs" / f"{backtest_id}.log"
                    try:
                        asyncio.run(stream_logs(ws_url, output_path))
                    except KeyboardInterrupt:
                        print("\nLog streaming interrupted.")
            else:
                print("backtest_id not found in response; skip writing history", file=sys.stderr)
            return 0
    except urllib.error.HTTPError as exc:
        print(exc.read().decode("utf-8"))
        return 1
    except Exception as exc:
        print(str(exc))
        return 1


def command_status(args: argparse.Namespace) -> int:
    url = build_url(HUB_BASE_URL, f"/runs/backtest/{args.backtest_id}")
    try:
        payload = request_json(url)
    except RuntimeError as exc:
        print(str(exc))
        return 1
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def command_logs(args: argparse.Namespace) -> int:
    url = build_url(HUB_BASE_URL, f"/runs/{args.backtest_id}/logs")
    try:
        content = request_bytes(url)
    except RuntimeError as exc:
        print(str(exc))
        return 1
    target_path = args.out or f"{args.backtest_id}.log"
    with open(target_path, "wb") as handle:
        handle.write(content)
    print(target_path)
    return 0


def command_help() -> int:
    print(
        "\n".join(
            [
                "Backtest Hub CLI commands:",
                "",
                "submit  Submit run_spec.json and strategy file to /runs",
                "  --api-key        API key (default: $HOST_API_KEY)",
                "  --strategy-file  Strategy file path",
                "  --run-spec       Run spec path (auto-generate unless --no-generate)",
                "  --name           Strategy name (required, recorded in history)",
                "  --no-generate    Skip run_spec generation",
                "  --follow-logs    Stream WebSocket logs into ./live_logs/{backtest_id}.log",
                "",
                "status  Get backtest status (status/pid/started_at)",
                "  backtest_id      Backtest ID",
                "",
                "logs    Download backtest logs",
                "  backtest_id      Backtest ID",
                "  --out            Output file path (default: {backtest_id}.log)",
                "",
                "Global:",
                "  BACKTEST_HUB_BASE_URL  Hub base URL (default: http://100.99.101.120:10033)",
            ]
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "help":
        return command_help()
    if args.command == "submit":
        return command_submit(args)
    if args.command == "status":
        return command_status(args)
    if args.command == "logs":
        return command_logs(args)
    raise RuntimeError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
