#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sys
from typing import Any, Dict
from urllib.parse import parse_qsl

import requests
from dotenv import load_dotenv


def parse_key_values(values: list[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for raw in values:
        if "=" not in raw:
            raise ValueError(f"Invalid key=value pair: {raw}")
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError(f"Empty key in pair: {raw}")
        out[key] = value
    return out


def maybe_json(value: str) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return value


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Probe Bazarr API and print raw response for debugging.",
    )
    p.add_argument(
        "--base-url",
        default=os.getenv("BAZARR_URL", "http://127.0.0.1:6767"),
        help="Bazarr base URL (default: env BAZARR_URL or http://127.0.0.1:6767)",
    )
    p.add_argument(
        "--api-key",
        default=os.getenv("BAZARR_API_KEY", ""),
        help="Bazarr API key (default: env BAZARR_API_KEY)",
    )
    p.add_argument(
        "--api-key-header",
        default=os.getenv("BAZARR_API_KEY_HEADER", "X-Api-Key"),
        help="Header name for API key (default: env BAZARR_API_KEY_HEADER or X-Api-Key)",
    )
    p.add_argument(
        "--method",
        default="GET",
        choices=["GET", "POST", "PUT", "PATCH", "DELETE"],
        help="HTTP method (default: GET)",
    )
    p.add_argument(
        "--endpoint",
        default="/api/movies",
        help="Endpoint path, e.g. /api/movies or /api/movies/history",
    )
    p.add_argument(
        "--query",
        action="append",
        default=[],
        help="Query key=value (can repeat), e.g. --query radarrid=11",
    )
    p.add_argument(
        "--query-string",
        default="",
        help="Raw query string, e.g. 'radarrid=11&radarrId=11'",
    )
    p.add_argument(
        "--body",
        default="",
        help="Request body as JSON string. Example: '{\"radarrId\":11}'",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Timeout in seconds (default: 20)",
    )
    p.add_argument(
        "--insecure",
        action="store_true",
        help="Disable SSL verification",
    )
    return p


def main() -> int:
    load_dotenv(dotenv_path=".env", override=False)
    parser = build_parser()
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    endpoint = args.endpoint if args.endpoint.startswith("/") else f"/{args.endpoint}"
    url = f"{base_url}{endpoint}"

    params = {}
    if args.query_string:
        params.update(dict(parse_qsl(args.query_string, keep_blank_values=True)))
    if args.query:
        params.update(parse_key_values(args.query))

    headers = {
        "Accept": "application/json",
        "User-Agent": "bazarr-api-probe/1.0",
    }
    if args.api_key:
        headers[args.api_key_header] = args.api_key

    json_body = None
    if args.body:
        parsed = maybe_json(args.body)
        if isinstance(parsed, str):
            print("ERROR: --body must be valid JSON", file=sys.stderr)
            return 2
        json_body = parsed

    print("=== REQUEST ===")
    print(f"Method   : {args.method}")
    print(f"URL      : {url}")
    print(f"Params   : {json.dumps(params, ensure_ascii=False)}")
    print(f"Headers  : {json.dumps({k: ('***' if k == args.api_key_header else v) for k, v in headers.items()}, ensure_ascii=False)}")
    print(f"JSON body: {json.dumps(json_body, ensure_ascii=False) if json_body is not None else '<none>'}")

    try:
        response = requests.request(
            method=args.method,
            url=url,
            headers=headers,
            params=params if params else None,
            json=json_body,
            timeout=args.timeout,
            verify=not args.insecure,
        )
    except requests.RequestException as exc:
        print("\n=== ERROR ===")
        print(str(exc))
        return 1

    print("\n=== RESPONSE ===")
    print(f"Status   : {response.status_code}")
    print(f"URL final: {response.url}")
    content_type = response.headers.get("Content-Type", "")
    print(f"Content-Type: {content_type}")

    try:
        payload = response.json()
        print("\n--- JSON ---")
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    except Exception:
        print("\n--- TEXT ---")
        print(response.text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
