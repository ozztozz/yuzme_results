from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import fitz

from create_event import scrape_event_detail
from parse_startlist_to_csv import extract_event_order, parse_events_only
from scrape_results import start_scraper


EVENT_NAME_WITH_PREFIX_RE = re.compile(r"^[^,]+,\s*(.+)$", re.IGNORECASE)
DISTANCE_STYLE_RE = re.compile(r"^(\d+(?:\s*x\s*\d+)?m)\s+(.+)$", re.IGNORECASE)

GENDER_ALIASES = {
    "erkek": "Erkekler",
    "erkekler": "Erkekler",
    "kız": "Kadinlar",
    "kiz": "Kadinlar",
    "kızlar": "Kadinlar",
    "kizlar": "Kadinlar",
    "bayan": "Kadinlar",
    "bayanlar": "Kadinlar",
    "kadın": "Kadinlar",
    "kadin": "Kadinlar",
    "kadinlar": "Kadinlar",
    "kadınlar": "Kadinlar",
}

SCRAPED_ROOT = Path("scraped")
LOCAL_WEB_BASE_URL = "http://127.0.0.1:8000"
REMOTE_WEB_BASE_URL = os.getenv("YUZME_WEB_BASE_URL", "https://ozztozz.pythonanywhere.com/").rstrip("/")
INGEST_TOKEN = os.getenv("YUZME_INGEST_TOKEN", "").strip() or None


def build_target_base_urls() -> list[str]:
    candidates = [LOCAL_WEB_BASE_URL.rstrip("/"), REMOTE_WEB_BASE_URL.rstrip("/")]
    targets: list[str] = []
    seen: set[str] = set()

    for base_url in candidates:
        if not base_url:
            continue
        if base_url in seen:
            continue
        seen.add(base_url)
        targets.append(base_url)

    return targets


def parse_distance_style(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None

    candidate = value.strip().strip(",")
    match = DISTANCE_STYLE_RE.match(candidate)
    if not match:
        return None, None

    return match.group(1).strip(), match.group(2).strip(" ,")


def normalize_gender(value: str | None) -> str | None:
    if not value:
        return None
    lowered = value.strip().lower()
    return GENDER_ALIASES.get(lowered, value)


def parse_event_descriptor(event_name: str | None) -> dict[str, str | None]:
    if not event_name:
        return {"event_name": None, "gender": None, "distance": None, "swimming_style": None}

    descriptor = event_name.strip()
    prefixed_match = EVENT_NAME_WITH_PREFIX_RE.match(descriptor)
    if prefixed_match:
        descriptor = prefixed_match.group(1).strip()

    gender: str | None = None
    
    distance: str | None = None
    swimming_style: str | None = None

    parts = [part.strip() for part in descriptor.split(",") if part.strip()]
    descriptor_tail = descriptor

    if parts:
        possible_gender = normalize_gender(parts[0])
        if possible_gender in {"Erkekler", "Kadinlar"}:
            gender = possible_gender
            descriptor_tail = " ".join(parts[1:]).strip() if len(parts) > 1 else ""

    distance, swimming_style = parse_distance_style(descriptor_tail)

    if distance is None and len(parts) >= 2:
        distance, swimming_style = parse_distance_style(" ".join(parts[1:]))

    if distance is None:
        distance, swimming_style = parse_distance_style(descriptor)

    return {
        "event_name": event_name,
        "gender": gender,
        "distance": distance,
        "swimming_style": swimming_style,
    }


def build_event_info_map(data: dict[str, Any]) -> dict[str, dict[str, str | None]]:
    event_map: dict[str, dict[str, str | None]] = {}
    for event in data.get("events", []):
        event_no = str(event.get("event_no") or "").strip()
        if not event_no:
            continue
        event_map[event_no] = parse_event_descriptor(event.get("event_name"))
    return event_map


def extract_text_from_pdf(pdf_path: Path) -> str:
    pages: list[str] = []
    with fitz.open(pdf_path) as document:
        for page in document:
            page_text = page.get_text("text") or ""
            pages.append(page_text)

    text = "\n".join(pages).strip()
    if not text:
        raise ValueError(f"No text extracted from PDF (expected text-based PDF): {pdf_path}")
    return text


def parse_startlist_pdfs(startlist_pdfs: list[Path], event_unique_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    counter = 1

    for pdf_path in sorted(startlist_pdfs):
        text = extract_text_from_pdf(pdf_path)
        data = parse_events_only(text)
        event_info_map = build_event_info_map(data)
        file_event_order = extract_event_order(pdf_path)

        for entry in data.get("entries", []):
            event_no_text = str(entry.get("event_no") or "").strip()
            event_info = event_info_map.get(
                event_no_text,
                {"event_name": None, "gender": None, "distance": None, "swimming_style": None},
            )

            event_order = 0
            if event_no_text.isdigit():
                event_order = int(event_no_text)
            elif file_event_order is not None:
                event_order = int(file_event_order)

            rows.append(
                {
                    "event_unique_name": event_unique_name,
                    "startlist_unique_name": f"{event_unique_name}-{counter:06d}",
                    "event_order": event_order,
                    "swimmer_name": str(entry.get("name") or "").strip(),
                    "year_of_birth": str(entry.get("year_of_birth") or "").strip(),
                    "gender": str(event_info.get("gender") or "").strip(),
                    "club": str(entry.get("club") or "").strip(),
                    "swimming_style": str(event_info.get("swimming_style") or "").strip(),
                    "distance": str(event_info.get("distance") or "").strip(),
                    "seri_no": str(entry.get("seri_no") or "").strip(),
                    "lane": str(entry.get("lane") or "").strip(),
                    "seed": str(entry.get("seed") or "").strip(),
                    "result": "",
                    "rank": 0,
                }
            )
            counter += 1

    filtered = [row for row in rows if row.get("swimmer_name")]
    return filtered


def post_json(endpoint: str, rows: list[dict[str, Any]], token: str | None, dry_run: bool = False) -> dict[str, Any]:
    query = ""
    if dry_run:
        query = "?" + urlencode({"dry_run": "1"})

    payload = json.dumps({"rows": rows}, ensure_ascii=False).encode("utf-8")
    request = Request(endpoint + query, data=payload, method="POST")
    request.add_header("Content-Type", "application/json; charset=utf-8")
    if token:
        request.add_header("X-Ingest-Token", token)

    with urlopen(request, timeout=120) as response:
        response_text = response.read().decode("utf-8", errors="replace")

    parsed = json.loads(response_text)
    if not isinstance(parsed, dict):
        raise ValueError(f"Unexpected API response from {endpoint}: {response_text}")
    return parsed


def run_pipeline(event_url: str) -> None:
    event_details = scrape_event_detail(event_url)

    downloads = start_scraper(event_url, output_root=SCRAPED_ROOT)
    startlist_pdfs: list[Path] = []
    for item in downloads:
        saved_to = Path(str(item.get("saved_to") or "").strip())
        if saved_to.suffix.lower() == ".pdf" and saved_to.exists():
            startlist_pdfs.append(saved_to)

    if not startlist_pdfs:
        raise RuntimeError("No startlist PDF was downloaded from event URL")

    event_rows = [
        {
            "unique_name": event_details["unique_name"],
            "title": event_details["title"],
            "date": event_details["date"],
            "location": event_details["location"],
        }
    ]

    result_rows = parse_startlist_pdfs(startlist_pdfs, event_details["unique_name"])
    if not result_rows:
        raise RuntimeError("No swimmer rows parsed from startlist PDFs")

    targets = build_target_base_urls()
    success_payloads: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    failures: list[tuple[str, str]] = []

    for base_url in targets:
        events_endpoint = f"{base_url}/api/ingest-events/"
        results_endpoint = f"{base_url}/api/ingest-results/"

        try:
            event_response = post_json(events_endpoint, event_rows, token=INGEST_TOKEN)
            result_response = post_json(results_endpoint, result_rows, token=INGEST_TOKEN)
            success_payloads.append((base_url, event_response, result_response))
        except Exception as error:
            failures.append((base_url, str(error)))

    print(f"Event URL: {event_url}")
    print(f"Event unique_name: {event_details['unique_name']}")
    print(f"Startlist PDFs downloaded: {len(startlist_pdfs)}")
    print(f"Parsed startlist rows: {len(result_rows)}")

    for base_url, event_response, result_response in success_payloads:
        print(f"[{base_url}] Event ingest response: {json.dumps(event_response, ensure_ascii=False)}")
        print(f"[{base_url}] Result ingest response: {json.dumps(result_response, ensure_ascii=False)}")

    for base_url, message in failures:
        print(f"[{base_url}] Upload failed: {message}")

    if not success_payloads:
        joined = " | ".join(f"{base}: {msg}" for base, msg in failures) or "no target available"
        raise RuntimeError(f"Upload failed for all targets: {joined}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean event+startlist pipeline (single input: event URL)")
    parser.add_argument("event_url", type=str, help="Event page URL, e.g. https://canli.tyf.gov.tr/ankara/cs-1004952/")
    args = parser.parse_args()

    try:
        run_pipeline(args.event_url)
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise SystemExit(f"HTTP {error.code}: {body}") from error
    except URLError as error:
        raise SystemExit(f"Failed to reach URL/API: {error}") from error
    except Exception as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
