#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path


def _load_cli_main():
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from scripts.cli import main as cli_main

    return cli_main


def main() -> int:
    cli_main = _load_cli_main()
    return cli_main(["submit", *sys.argv[1:]])


if __name__ == "__main__":
    raise SystemExit(main())
