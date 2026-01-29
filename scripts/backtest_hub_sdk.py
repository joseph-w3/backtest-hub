from __future__ import annotations

import sys
from pathlib import Path


def _load_cli_main():
    try:
        from backtest_hub_cli.cli import main as cli_main

        return cli_main
    except ModuleNotFoundError:
        repo_root = Path(__file__).resolve().parents[1]
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))
        from backtest_hub_cli.cli import main as cli_main

        return cli_main


def main() -> int:
    cli_main = _load_cli_main()
    argv = sys.argv[1:]
    if not argv:
        return cli_main(argv)

    command = argv[0]
    if command == "get-backtest-status":
        return cli_main(["status", *argv[1:]])
    if command == "download-backtest-logs":
        return cli_main(["logs", *argv[1:]])
    return cli_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
