from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def enrich_rows_with_event_ref(
    rows: list[dict[str, Any]],
    event_unique_name: str | None,
    event_id: int | None,
) -> list[dict[str, Any]]:
    if not event_unique_name and event_id is None:
        return rows

    enriched: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        if event_unique_name:
            payload["event_unique_name"] = event_unique_name
        if event_id is not None:
            payload["event_id"] = event_id
        enriched.append(payload)
    return enriched


def post_json(endpoint: str, rows: list[dict[str, Any]], token: str | None, dry_run: bool) -> str:
    query = ""
    if dry_run:
        query = "?" + urlencode({"dry_run": "1"})

    payload = json.dumps({"rows": rows}, ensure_ascii=False).encode("utf-8")
    request = Request(endpoint + query, data=payload, method="POST")
    request.add_header("Content-Type", "application/json; charset=utf-8")
    if token:
        request.add_header("X-Ingest-Token", token)

    with urlopen(request, timeout=120) as response:
        return response.read().decode("utf-8", errors="replace")


def post_csv(endpoint: str, csv_path: Path, token: str | None, dry_run: bool) -> str:
    query = ""
    if dry_run:
        query = "?" + urlencode({"dry_run": "1"})

    payload = csv_path.read_bytes()
    request = Request(endpoint + query, data=payload, method="POST")
    request.add_header("Content-Type", "text/csv; charset=utf-8")
    if token:
        request.add_header("X-Ingest-Token", token)

    with urlopen(request, timeout=120) as response:
        return response.read().decode("utf-8", errors="replace")


def main() -> None:
    parser = argparse.ArgumentParser(description="Push parsed results from local parser to web Django ingestion endpoint")
    parser.add_argument("--endpoint", type=str, required=True, help="Web endpoint URL, e.g. https://host/api/ingest-results/")
    parser.add_argument("--startlist-csv", type=Path, required=True, help="Path to startlist CSV produced by parser")
    parser.add_argument(
        "--payload",
        choices=["json", "csv"],
        default="json",
        help="Upload format to web app",
    )
    parser.add_argument(
        "--event-unique-name",
        type=str,
        default=None,
        help="Optional event unique name to inject into each JSON row (e.g. cs-1005252)",
    )
    parser.add_argument(
        "--event-id",
        type=int,
        default=None,
        help="Optional event id to inject into each JSON row",
    )
    parser.add_argument("--token", type=str, default=None, help="Optional ingest token sent as X-Ingest-Token header")
    parser.add_argument("--dry-run", action="store_true", help="Ask server to validate without DB write")
    args = parser.parse_args()

    if not args.startlist_csv.exists():
        raise FileNotFoundError(f"CSV file not found: {args.startlist_csv}")

    try:
        if args.payload == "json":
            rows = read_csv_rows(args.startlist_csv)
            rows = enrich_rows_with_event_ref(rows, args.event_unique_name, args.event_id)
            response_text = post_json(args.endpoint, rows, args.token, args.dry_run)
        else:
            response_text = post_csv(args.endpoint, args.startlist_csv, args.token, args.dry_run)

        print(response_text)

    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {error.code}: {body}") from error
    except URLError as error:
        raise RuntimeError(f"Failed to reach endpoint: {error}") from error


if __name__ == "__main__":
    main()
