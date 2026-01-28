#!/usr/bin/env python3
import argparse
import json
import os
import sys
import urllib.request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Submit RunSpec to Host API")
    parser.add_argument("--file", required=True, help="Path to run_spec.json")
    parser.add_argument("--host", default="http://100.99.101.120:10033")
    parser.add_argument("--api-key", default=os.getenv("HOST_API_KEY", ""))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.api_key:
        print("HOST_API_KEY is required")
        return 1

    with open(args.file, "r") as handle:
        payload = json.load(handle)

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{args.host.rstrip('/')}/runs",
        data=data,
        headers={"Content-Type": "application/json", "X-API-KEY": args.api_key},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read().decode("utf-8")
            print(body)
            return 0
    except urllib.error.HTTPError as exc:
        print(exc.read().decode("utf-8"))
        return 1
    except Exception as exc:
        print(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
