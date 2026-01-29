#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import subprocess
import sys
import urllib.request
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import aiohttp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Submit RunSpec to Host API")
    parser.add_argument("--host", default="http://100.99.101.120:10033")
    parser.add_argument("--api-key", default=os.getenv("HOST_API_KEY", ""))
    parser.add_argument(
        "--strategy-file",
        default="strategies/spot_futures_arb_diagnostics.py",
        help="Path to strategy file",
    )
    parser.add_argument(
        "--run-spec",
        default="run_spec.json",
        help="Path to run_spec.json (will be generated if missing or --no-generate is not set)",
    )
    parser.add_argument(
        "--no-generate",
        action="store_true",
        help="Skip generating run_spec.json",
    )
    parser.add_argument(
        "--follow-logs",
        action="store_true",
        help="After submit, stream WebSocket logs into ./live_logs/{backtest_id}.log",
    )
    return parser.parse_args()


def build_multipart_form(files: list[tuple[str, str, str, bytes]]) -> tuple[bytes, str]:
    # 构造标准 multipart/form-data 请求体。
    # files: [(字段名, 文件名, Content-Type, 二进制内容)]
    # 返回值: (body_bytes, content_type_header)
    boundary = "----research-submit-" + os.urandom(16).hex()
    body = bytearray()
    for field_name, filename, content_type, content in files:
        # 每个文件段的边界与头部
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(
            f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'.encode(
                "utf-8"
            )
        )
        body.extend(f"Content-Type: {content_type}\r\n\r\n".encode("utf-8"))
        body.extend(content)
        body.extend(b"\r\n")
    # 结束边界
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


def write_run_id_history(backtest_id: str, history_path: Path) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_line = f"{timestamp} {backtest_id}\n"
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


def main() -> int:
    args = parse_args()
    if not args.api_key:
        print("HOST_API_KEY is required")
        return 1

    # 1) 准备策略文件与 run_spec（必要时先生成 run_spec）
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

    # 2) 读取 run_spec 内容
    with run_spec_path.open("r") as handle:
        payload = json.load(handle)

    # 3) 组装 multipart/form-data：run_spec + strategy_file
    run_spec_bytes = json.dumps(payload).encode("utf-8")
    strategy_bytes = strategy_path.read_bytes()
    body, content_type = build_multipart_form(
        files=[
            ("run_spec", "run_spec.json", "application/json", run_spec_bytes),
            ("strategy_file", strategy_path.name, "text/x-python", strategy_bytes),
        ]
    )
    # 4) 发送到 backtest hub 的 /runs
    req = urllib.request.Request(
        f"{args.host.rstrip('/')}/runs",
        data=body,
        headers={"Content-Type": content_type, "X-API-KEY": args.api_key},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode("utf-8")
            print(body)
            backtest_id = extract_run_id(body)
            if backtest_id:
                history_path = Path(__file__).resolve().parent / "backtest_run_id_history"
                write_run_id_history(backtest_id, history_path)
                if args.follow_logs:
                    ws_url = build_ws_url(
                        args.host,
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


if __name__ == "__main__":
    raise SystemExit(main())
