from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Any


VERSION_RE = re.compile(r"Splash\s+Meet\s+Manager\s*,\s*([0-9]+(?:\.[0-9]+)+)", re.IGNORECASE)
EVENT_SINGLE_LINE_RE = re.compile(r"^Yarış\s+(\d+)\s*,\s*(.+)$", re.IGNORECASE)
EVENT_START_RE = re.compile(r"^Yarış\s+(\d+)\s*$", re.IGNORECASE)
HEAT_RE = re.compile(r"^(?:Seri|Heat)\s+(\d+)\s*(?:of|/)\s*(\d+)$", re.IGNORECASE)
# Lane numbers are typically 1-8 (sometimes up to 10). Avoid treating club names
# like "15 Temmuz ..." or "29 Ekim ..." as lane lines.
LANE_NAME_RE = re.compile(r"^([1-9]|10)\s+(.+)$")
YEAR_RE = re.compile(r"^\d{2}$")
SEED_RE = re.compile(r"^\d{1,2}(?:[:\-.]\d{2})?(?:[:\-.]\d{2})$")
NT_RE = re.compile(r"^NT$", re.IGNORECASE)
PAGE_MARKER_RE = re.compile(r"^---\s*Page\s*\d+\s*---$", re.IGNORECASE)
REGISTERED_RE = re.compile(r"^Registered\b", re.IGNORECASE)
DATE_LINE_RE = re.compile(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b")
EVENT_DATE_TIME_RE = re.compile(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b\s*-\s*\d{1,2}:\d{2}")
LOCATION_LINE_RE = re.compile(r"^([A-ZÇĞİÖŞÜ]+)\s*,")
EVENT_DESCRIPTOR_RE = re.compile(r"^(Erkekler|Kızlar)\s*,\s*(\d{1,4}m)\s+(.+)$", re.IGNORECASE)
TAG_RE = re.compile(r"^\((?:Tk|Fd|Td)\)\s*", re.IGNORECASE)
SPLASH_SUFFIX_RE = re.compile(r"\s*-\s*SPLASH\s+Meet\s+Manager\s*\d*", re.IGNORECASE)


EVENT_ORDER_RE = re.compile(r"(\d+)")


def extract_event_order(path: Path) -> int | None:
    """Return the first integer found in the PDF/text filename stem, e.g. start1 -> 1."""
    match = EVENT_ORDER_RE.search(path.stem)
    return int(match.group(1)) if match else None


def detect_splash_version(text: str) -> str:
    match = VERSION_RE.search(text)
    if not match:
        raise ValueError("Could not detect Splash Meet Manager version in input text")
    return match.group(1)


def clean_line(line: str) -> str:
    return re.sub(r"\s+", " ", line).strip()


def _extract_date_only(value: str | None) -> str | None:
    if not value:
        return None
    match = DATE_LINE_RE.search(value)
    return match.group(0) if match else None


def extract_header_metadata(lines: list[str]) -> dict[str, str | None]:
    event_title: str | None = None
    event_date: str | None = None
    event_location: str | None = None
    event_name: str | None = None
    gender: str | None = None
    style: str | None = None
    distance: str | None = None
    event_order: int | None = None

    # First, prefer explicit event schedule date-time if present anywhere near top.
    for line in lines[:200]:
        if not line:
            continue
        dt_match = EVENT_DATE_TIME_RE.search(line)
        if dt_match:
            event_date = _extract_date_only(dt_match.group(0))
            break

    for idx, line in enumerate(lines[:80]):
        if not line:
            continue
        if PAGE_MARKER_RE.match(line):
            continue
        if VERSION_RE.search(line):
            continue
        if REGISTERED_RE.match(line):
            continue
        if line.lower().startswith("yarış"):
            if event_name is None:
                event_name = line
            break

        if event_location is None:
            location_match = LOCATION_LINE_RE.match(line)
            if location_match:
                event_location = location_match.group(1).strip()

        # First descriptive non-system line is generally the event title.
        if event_title is None and not DATE_LINE_RE.search(line):
            event_title = SPLASH_SUFFIX_RE.sub("", line).strip()

    # Parse first event block for document-level event metadata.
    first_event_idx = None
    for idx, line in enumerate(lines):
        if EVENT_START_RE.match(line) or EVENT_SINGLE_LINE_RE.match(line):
            first_event_idx = idx
            break

    if first_event_idx is not None:
        start_line = lines[first_event_idx]
        if event_name is None:
            event_name = start_line

        single_match = EVENT_SINGLE_LINE_RE.match(start_line)
        if single_match:
            descriptor = single_match.group(2).strip()
            descriptor_match = EVENT_DESCRIPTOR_RE.match(descriptor)
            if descriptor_match:
                gender = descriptor_match.group(1)
                distance = descriptor_match.group(2)
                style = descriptor_match.group(3).strip()
                if event_name is None:
                    event_name = f"Yarış {single_match.group(1)}"
            elif event_name is None:
                event_name = f"Yarış {single_match.group(1)}, {descriptor}"
        else:
            for j in range(first_event_idx + 1, min(first_event_idx + 6, len(lines))):
                candidate = lines[j]
                if not candidate:
                    continue
                descriptor_match = EVENT_DESCRIPTOR_RE.match(candidate)
                if descriptor_match:
                    gender = descriptor_match.group(1)
                    distance = descriptor_match.group(2)
                    style = descriptor_match.group(3).strip()
                    break

    # Fallback: find first date-containing line near the header.
    if event_date is None:
        for line in lines[:120]:
            if not line:
                continue
            date_match = DATE_LINE_RE.search(line)
            if date_match:
                event_date = _extract_date_only(date_match.group(0))
                break

    return {
        "event_title": event_title,
        "event_date": event_date,
        "event_location": event_location,
        "event_name": event_name,
        "gender": gender,
        "style": style,
        "distance": distance,
    }


def parse_events_only(text: str) -> dict[str, Any]:
    lines = [clean_line(line) for line in text.replace("\r\n", "\n").split("\n")]
    metadata = extract_header_metadata(lines)

    events: list[dict[str, str]] = []
    series: list[dict[str, Any]] = []
    entries: list[dict[str, Any]] = []
    current_event_no: str | None = None
    current_seri_no: int | None = None
    current_seri_total: int | None = None
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line:
            i += 1
            continue

        single_match = EVENT_SINGLE_LINE_RE.match(line)
        if single_match:
            current_event_no = single_match.group(1)
            events.append(
                {
                    "event_no": single_match.group(1),
                    "event_name": f"Yarış {single_match.group(1)}, {single_match.group(2).strip()}",
                }
            )
            i += 1
            continue

        start_match = EVENT_START_RE.match(line)
        if start_match:
            event_no = start_match.group(1)
            current_event_no = event_no
            event_name = f"Yarış {event_no}"

            # In many files, next non-empty line contains event details.
            j = i + 1
            while j < len(lines) and not lines[j]:
                j += 1
            if j < len(lines) and not EVENT_START_RE.match(lines[j]):
                event_name = f"{event_name}, {lines[j]}"

            events.append({"event_no": event_no, "event_name": event_name})
            i += 1
            continue

        heat_match = HEAT_RE.match(line)
        if heat_match:
            series.append(
                {
                    "event_no": current_event_no,
                    "seri_no": int(heat_match.group(1)),
                    "seri_total": int(heat_match.group(2)),
                }
            )
            current_seri_no = int(heat_match.group(1))
            current_seri_total = int(heat_match.group(2))
            i += 1
            continue

        lane_name_match = LANE_NAME_RE.match(line)
        if lane_name_match and current_seri_no is not None:
            lane = int(lane_name_match.group(1))
            name = TAG_RE.sub("", lane_name_match.group(2)).strip()

            year_of_birth: str | None = None
            club: str | None = None
            seed: str | None = None

            j = i + 1
            if j < len(lines) and YEAR_RE.match(lines[j]):
                year_of_birth = lines[j]
                j += 1

            if j < len(lines) and lines[j]:
                candidate_club = lines[j]
                # Skip accidental next lane line.
                if not LANE_NAME_RE.match(candidate_club) and not HEAT_RE.match(candidate_club):
                    club = TAG_RE.sub("", candidate_club).strip()
                    j += 1

            if j < len(lines):
                if SEED_RE.match(lines[j]):
                    seed = lines[j].replace("-", ":")
                elif NT_RE.match(lines[j]):
                    seed = "NT"

            entries.append(
                {
                    "event_no": current_event_no,
                    "seri_no": current_seri_no,
                    "seri_total": current_seri_total,
                    "lane": lane,
                    "name": name,
                    "year_of_birth": year_of_birth,
                    "club": club,
                    "seed": seed,
                }
            )
            i += 1
            continue

        i += 1

    return {
        "metadata": metadata,
        "event_count": len(events),
        "events": events,
        "series_count": len(series),
        "series": series,
        "entry_count": len(entries),
        "entries": entries,
        "line_count": len(lines),
    }


def write_entries_csv(data: dict[str, Any], output_csv: Path) -> int:
    entries = data.get("entries", [])
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "event_order",
        "event_no",
        "seri_no",
        "seri_total",
        "lane",
        "name",
        "year_of_birth",
        "club",
        "seed",
    ]

    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for entry in entries:
            writer.writerow({key: entry.get(key) for key in fieldnames})

    return len(entries)


def main() -> None:
    parser = argparse.ArgumentParser(description="Step-1 StartList parser (events only)")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("results/StartList_20_text_easyocr.txt"),
        help="Input startlist OCR text file",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("results/StartList_20_step1_events.json"),
        help="Output JSON path",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("results/StartList_20_step1_entries.csv"),
        help="Output CSV path for parsed entries",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise FileNotFoundError(f"Input text file not found: {args.input}")

    event_order = extract_event_order(args.input)

    try:
        text = args.input.read_text(encoding="utf-8")
        version = detect_splash_version(text)
        data = parse_events_only(text)
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error

    for entry in data.get("entries", []):
        entry["event_order"] = event_order

    payload = {
        "detected_version": version,
        "step": "step-1-events-only",
        "event_order": event_order,
        "data": data,
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    row_count = write_entries_csv(data, args.output_csv)

    print(f"Detected Splash version: {version}")
    print(f"Events found: {data['event_count']}")
    print(f"Entries found: {row_count}")
    print(f"Saved JSON to: {args.output_json}")
    print(f"Saved CSV to: {args.output_csv}")


if __name__ == "__main__":
    main()
