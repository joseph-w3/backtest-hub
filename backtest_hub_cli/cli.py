from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse, urlunparse

import aiohttp

HUB_BASE_URL = os.getenv("BACKTEST_HUB_BASE_URL", "http://100.87.155.67:10033")
FOLLOW_LOGS_DELAY_SECONDS = 35

DEFAULT_STRATEGY_IGNORE = [
    ".git/",
    "__pycache__/",
    "*.pyc",
    "*.pyo",
    "*.pyd",
    ".Python",
    "build/",
    "dist/",
    "*.egg-info/",
    ".DS_Store",
    ".venv/",
    "venv/",
    ".mypy_cache/",
    ".pytest_cache/",
    ".ruff_cache/",
    ".idea/",
    ".vscode/",
    ".strategyignore",
]


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


def load_strategyignore(root_dir: Path) -> list[str]:
    patterns = list(DEFAULT_STRATEGY_IGNORE)
    ignore_path = root_dir / ".strategyignore"
    if ignore_path.is_file():
        try:
            for line in ignore_path.read_text(encoding="utf-8").splitlines():
                raw = line.strip()
                if not raw or raw.startswith("#"):
                    continue
                patterns.append(raw)
        except OSError:
            pass
    return patterns


def _pattern_matches(path: PurePosixPath, pattern: str) -> bool:
    raw = pattern.strip()
    if not raw:
        return False
    anchored = raw.startswith("/")
    if anchored:
        raw = raw.lstrip("/")
    dir_only = raw.endswith("/")
    if dir_only:
        raw = raw.rstrip("/")
    if not raw:
        return False
    if dir_only:
        if "/" not in raw:
            if anchored:
                return bool(path.parts) and PurePosixPath(path.parts[0]).match(raw)
            for part in path.parts[:-1]:
                if PurePosixPath(part).match(raw):
                    return True
            return False
        return path.match(f"{raw}/**")
    if "/" not in raw:
        if anchored:
            return len(path.parts) == 1 and PurePosixPath(path.parts[0]).match(raw)
        for part in path.parts:
            if PurePosixPath(part).match(raw):
                return True
        return False
    return path.match(raw)


def is_strategy_ignored(rel_posix: str, patterns: list[str]) -> bool:
    path = PurePosixPath(rel_posix)
    ignored = False
    for pattern in patterns:
        negate = pattern.startswith("!")
        raw = pattern[1:] if negate else pattern
        if not raw:
            continue
        if _pattern_matches(path, raw):
            ignored = not negate
    return ignored


def validate_strategy_bundle_zip(data: bytes, source: str) -> str:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        top_dirs: set[str] = set()
        has_file = False
        for info in zf.infolist():
            name = info.filename
            if not name:
                continue
            path = PurePosixPath(name)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(f"strategy_bundle contains unsafe path: {name}")
            if name.endswith("/"):
                parts = path.parts
                if len(parts) >= 1:
                    top_dirs.add(parts[0])
                continue
            parts = path.parts
            if len(parts) < 2:
                raise ValueError("strategy_bundle must contain a single top-level directory")
            top_dirs.add(parts[0])
            has_file = True
        if not has_file:
            raise ValueError("strategy_bundle is empty")
        if len(top_dirs) != 1:
            raise ValueError(f"strategy_bundle must contain exactly one top-level directory: {sorted(top_dirs)}")
        return next(iter(top_dirs))


def bundle_strategy_dir(dir_path: Path) -> tuple[str, bytes]:
    patterns = load_strategyignore(dir_path)
    buffer = io.BytesIO()
    bundle_name = f"{dir_path.name}.zip"
    added = 0
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in sorted(dir_path.rglob("*")):
            if not file_path.is_file():
                continue
            rel = file_path.relative_to(dir_path).as_posix()
            if is_strategy_ignored(rel, patterns):
                continue
            arcname = f"{dir_path.name}/{rel}"
            zf.write(file_path, arcname)
            added += 1
    if added == 0:
        raise ValueError("strategy bundle is empty after applying .strategyignore")
    data = buffer.getvalue()
    validate_strategy_bundle_zip(data, source=bundle_name)
    return bundle_name, data


def load_strategy_bundle(path: Path) -> tuple[str, bytes]:
    data = path.read_bytes()
    validate_strategy_bundle_zip(data, source=str(path))
    return path.name, data


def generate_run_spec(
    run_spec_path: Path,
    strategy_path: Path | None,
    script_path: Path,
    strategy_bundle: str | None = None,
) -> None:
    command = [sys.executable, str(script_path), "--output", str(run_spec_path)]
    if strategy_bundle is not None:
        command.extend(["--strategy-bundle", strategy_bundle])
    elif strategy_path is not None:
        command.extend(["--strategy-file", str(strategy_path)])
    subprocess.run(command, check=True)


def resolve_local_run_spec_script() -> Path | None:
    candidate = Path.cwd() / "scripts" / "generate_run_spec.py"
    if candidate.is_file():
        return candidate
    return None


def get_template_script_path() -> Path:
    return Path(__file__).resolve().parent / "scripts" / "generate_run_spec.py"


def init_run_spec_script() -> Path:
    target = Path.cwd() / "scripts" / "generate_run_spec.py"
    if target.exists():
        raise FileExistsError(f"{target} already exists")
    target.parent.mkdir(parents=True, exist_ok=True)
    template = get_template_script_path()
    target.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")
    return target


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


def request_json(url: str, method: str = "GET") -> dict[str, Any]:
    req = urllib.request.Request(url, method=method)
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
    submit_parser.add_argument(
        "--strategy-file",
        default=None,
        help="Path to strategy file (optional; fallback to run_spec.json: strategy_file; and ./strategies/<same-name>)",
    )
    submit_parser.add_argument(
        "--strategy-bundle",
        default=None,
        help=(
            "Path to strategy bundle (.zip or top-level directory). When set, ignores --strategy-file. "
            "If a directory is given, it must be the top-level package directory; ALL imported code must live under it. "
            "If a zip is given, it must contain exactly one top-level directory holding all code. "
            "CLI does NOT analyze imports."
        ),
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

    subparsers.add_parser("init", help="Create scripts/generate_run_spec.py in current directory")

    status_parser = subparsers.add_parser("status", help="Get backtest status (status/pid/started_at)")
    status_parser.add_argument("backtest_id", help="Backtest ID")

    logs_parser = subparsers.add_parser("logs", help="Download backtest logs")
    logs_parser.add_argument("backtest_id", help="Backtest ID")
    logs_parser.add_argument(
        "--out",
        default=None,
        help="Output file path (default: {backtest_id}.log)",
    )

    csv_parser = subparsers.add_parser("download-csv", help="Download backtest CSV logs (zip)")
    csv_parser.add_argument("backtest_id", help="Backtest ID")
    csv_parser.add_argument(
        "--out",
        default=None,
        help="Output file path (default: {backtest_id}.zip)",
    )

    kill_parser = subparsers.add_parser("kill", help="Stop a running backtest by backtest_id")
    kill_parser.add_argument("backtest_id", help="Backtest ID")

    return parser.parse_args(argv)


def resolve_strategy_path(raw: str) -> Path | None:
    """
    Resolve a strategy file path.

    Resolution order:
    1) Use the given path (absolute or relative to cwd) if it exists.
    2) If not found and the path is not absolute, fallback to ./strategies/<basename>.
    """
    candidate = Path(str(raw))
    if candidate.is_file():
        return candidate
    if not candidate.is_absolute():
        fallback = Path.cwd() / "strategies" / candidate.name
        if fallback.is_file():
            return fallback
    return None


def resolve_strategy_bundle_path(raw: str) -> Path | None:
    candidate = Path(str(raw))
    if candidate.exists() and (candidate.is_file() or candidate.is_dir()):
        return candidate
    return None


def command_submit(args: argparse.Namespace) -> int:
    run_spec_path = Path(args.run_spec)

    # Read run_spec.json first to get strategy_file if not specified.
    run_spec_data: dict[str, Any] = {}
    if run_spec_path.exists():
        try:
            with run_spec_path.open("r", encoding="utf-8") as handle:
                loaded = json.load(handle)
                if isinstance(loaded, dict):
                    run_spec_data = loaded
        except json.JSONDecodeError as exc:
            print(f"Invalid run_spec.json: {exc}", file=sys.stderr)
            return 1

    if args.strategy_file and args.strategy_bundle:
        print("strategy_file and strategy_bundle are mutually exclusive", file=sys.stderr)
        return 1

    generated_for_defaults = False
    strategy_mode = None
    raw_strategy = None
    raw_bundle = None
    if args.strategy_bundle:
        strategy_mode = "bundle"
        raw_bundle = args.strategy_bundle
    elif args.strategy_file:
        strategy_mode = "file"
        raw_strategy = args.strategy_file
    elif run_spec_data.get("strategy_bundle"):
        strategy_mode = "bundle"
        raw_bundle = str(run_spec_data["strategy_bundle"])
    elif run_spec_data.get("strategy_file"):
        strategy_mode = "file"
        raw_strategy = str(run_spec_data["strategy_file"])
    else:
        if not args.no_generate or not run_spec_path.exists():
            script_path = resolve_local_run_spec_script()
            if script_path is None:
                print("Missing scripts/generate_run_spec.py. Run: backtest-hub-cli init", file=sys.stderr)
                return 1
            try:
                generate_run_spec(run_spec_path, None, script_path)
                generated_for_defaults = True
            except subprocess.CalledProcessError as exc:
                print(f"generate_run_spec failed: {exc}")
                return 1
            run_spec_data = {}
            if run_spec_path.exists():
                try:
                    with run_spec_path.open("r", encoding="utf-8") as handle:
                        loaded = json.load(handle)
                        if isinstance(loaded, dict):
                            run_spec_data = loaded
                except json.JSONDecodeError as exc:
                    print(f"Invalid run_spec.json: {exc}", file=sys.stderr)
                    return 1
            if run_spec_data.get("strategy_bundle"):
                strategy_mode = "bundle"
                raw_bundle = str(run_spec_data["strategy_bundle"])
            elif run_spec_data.get("strategy_file"):
                strategy_mode = "file"
                raw_strategy = str(run_spec_data["strategy_file"])
            else:
                print("strategy file/bundle not specified and not found in run_spec.json", file=sys.stderr)
                return 1
        else:
            print("strategy file/bundle not specified and not found in run_spec.json", file=sys.stderr)
            return 1

    strategy_path: Path | None = None
    bundle_name: str | None = None
    bundle_bytes: bytes | None = None
    if strategy_mode == "file":
        assert raw_strategy is not None
        strategy_path = resolve_strategy_path(raw_strategy)
        if strategy_path is None:
            candidate = Path(str(raw_strategy))
            tried_fallback = Path.cwd() / "strategies" / candidate.name
            if candidate.is_absolute():
                print(f"strategy file not found: {candidate}", file=sys.stderr)
            else:
                print(
                    f"strategy file not found: {candidate} (also tried: {tried_fallback})",
                    file=sys.stderr,
                )
            return 1
        print(f"Using strategy file: {strategy_path.resolve()}")
    else:
        assert raw_bundle is not None
        bundle_path = resolve_strategy_bundle_path(raw_bundle)
        auto_pack_dir: Path | None = None
        if bundle_path is None:
            candidate = Path(str(raw_bundle))
            if candidate.suffix == ".zip":
                candidate_dir = candidate.with_suffix("")
                if candidate_dir.is_dir():
                    bundle_path = candidate_dir
                    auto_pack_dir = candidate_dir
            if bundle_path is None:
                print(f"strategy bundle not found: {candidate}", file=sys.stderr)
                return 1
        if auto_pack_dir is not None:
            print(f"strategy bundle zip not found; auto packing from directory: {auto_pack_dir.resolve()}")
        print(f"Using strategy bundle: {bundle_path.resolve()}")
        try:
            if bundle_path.is_dir():
                bundle_name, bundle_bytes = bundle_strategy_dir(bundle_path)
            else:
                bundle_name, bundle_bytes = load_strategy_bundle(bundle_path)
        except Exception as exc:
            print(f"invalid strategy bundle: {exc}", file=sys.stderr)
            return 1

    if (not args.no_generate or not run_spec_path.exists()) and not generated_for_defaults:
        script_path = resolve_local_run_spec_script()
        if script_path is None:
            print("Missing scripts/generate_run_spec.py. Run: backtest-hub-cli init", file=sys.stderr)
            return 1
        try:
            if strategy_mode == "bundle":
                assert bundle_name is not None
                generate_run_spec(run_spec_path, None, script_path, strategy_bundle=bundle_name)
            else:
                generate_run_spec(run_spec_path, strategy_path, script_path)
        except subprocess.CalledProcessError as exc:
            print(f"generate_run_spec failed: {exc}")
            return 1

    with run_spec_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    payload_changed = False
    if "backtest_id" in payload:
        payload.pop("backtest_id", None)
        payload_changed = True
    requested_by = os.getenv("JUPYTERHUB_USER")
    if requested_by and payload.get("requested_by") != requested_by:
        payload["requested_by"] = requested_by
        payload_changed = True

    if strategy_mode == "bundle":
        assert bundle_name is not None
        if payload.get("strategy_bundle") != bundle_name:
            payload["strategy_bundle"] = bundle_name
            payload_changed = True
        if "strategy_file" in payload:
            payload.pop("strategy_file", None)
            payload_changed = True
    else:
        assert strategy_path is not None
        if payload.get("strategy_file") != strategy_path.name:
            payload["strategy_file"] = strategy_path.name
            payload_changed = True
        if "strategy_bundle" in payload:
            payload.pop("strategy_bundle", None)
            payload_changed = True

    if payload_changed:
        run_spec_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    run_spec_bytes = json.dumps(payload).encode("utf-8")
    files = [("run_spec", "run_spec.json", "application/json", run_spec_bytes)]
    if strategy_mode == "bundle":
        assert bundle_name is not None
        assert bundle_bytes is not None
        files.append(("strategy_bundle", bundle_name, "application/zip", bundle_bytes))
    else:
        assert strategy_path is not None
        strategy_bytes = strategy_path.read_bytes()
        files.append(("strategy_file", strategy_path.name, "text/x-python", strategy_bytes))
    body, content_type = build_multipart_form(
        files=files
    )

    req = urllib.request.Request(
        f"{HUB_BASE_URL.rstrip('/')}/runs",
        data=body,
        headers={"Content-Type": content_type},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req) as resp:
            body_text = resp.read().decode("utf-8")
            print(body_text)
            backtest_id = extract_run_id(body_text)
            if backtest_id:
                history_path = Path.cwd() / "backtest_run_id_history"
                write_run_id_history(backtest_id, args.name, history_path)
                if args.follow_logs:
                    ws_url = build_ws_url(
                        HUB_BASE_URL,
                        f"/runs/backtest/{backtest_id}/logs/stream",
                    )
                    output_path = Path.cwd() / "live_logs" / f"{backtest_id}.log"
                    try:
                        print(f"Waiting {FOLLOW_LOGS_DELAY_SECONDS}s before streaming logs...")
                        time.sleep(FOLLOW_LOGS_DELAY_SECONDS)
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


def command_download_csv(args: argparse.Namespace) -> int:
    url = build_url(HUB_BASE_URL, f"/runs/backtest/{args.backtest_id}/download_csv")
    try:
        content = request_bytes(url)
    except RuntimeError as exc:
        print(str(exc))
        return 1
    target_path = args.out or f"{args.backtest_id}.zip"
    with open(target_path, "wb") as handle:
        handle.write(content)
    print(target_path)
    return 0


def command_kill(args: argparse.Namespace) -> int:
    url = build_url(HUB_BASE_URL, f"/runs/backtest/{args.backtest_id}/kill")
    try:
        payload = request_json(url, method="POST")
    except RuntimeError as exc:
        print(str(exc))
        return 1
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def command_help() -> int:
    print(
        "\n".join(
            [
                "Backtest Hub CLI commands:",
                "",
                "submit  Submit run_spec.json and strategy file to /runs",
                "  --strategy-file  Strategy file path (optional; fallback to run_spec.json: strategy_file; and ./strategies/<same-name>)",
                "  --strategy-bundle  Strategy bundle (.zip or directory); overrides --strategy-file",
                "                   (If a directory: top-level package dir; include ALL imported code under it)",
                "                   (If a zip: must contain exactly one top-level dir holding all code)",
                "                   (CLI does NOT analyze imports)",
                "  --run-spec       Run spec path (auto-generate unless --no-generate)",
                "  --name           Strategy name (required, recorded in history)",
                "  --no-generate    Skip run_spec generation",
                "  --follow-logs    Stream WebSocket logs into ./live_logs/{backtest_id}.log",
                "",
                "init    Create scripts/generate_run_spec.py in current directory",
                "",
                "status  Get backtest status (status/pid/started_at)",
                "  backtest_id      Backtest ID",
                "",
                "logs    Download backtest logs",
                "  backtest_id      Backtest ID",
                "  --out            Output file path (default: {backtest_id}.log)",
                "",
                "download-csv  Download backtest CSV logs (zip)",
                "  backtest_id      Backtest ID",
                "  --out            Output file path (default: {backtest_id}.zip)",
                "",
                "kill    Stop a running backtest",
                "  backtest_id      Backtest ID",
                "",
            ]
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "help":
        return command_help()
    if args.command == "init":
        try:
            target = init_run_spec_script()
        except FileExistsError as exc:
            print(str(exc))
            return 1
        print(f"Created {target}")
        return 0
    if args.command == "submit":
        return command_submit(args)
    if args.command == "status":
        return command_status(args)
    if args.command == "logs":
        return command_logs(args)
    if args.command == "download-csv":
        return command_download_csv(args)
    if args.command == "kill":
        return command_kill(args)
    raise RuntimeError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
