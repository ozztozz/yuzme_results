from __future__ import annotations

import re
from typing import Any

from django.db import transaction

from .models import Event, Result


DISTANCE_RE = re.compile(r"^(?:(\d+)\s*[xX]\s*)?(\d+)\s*m$", re.IGNORECASE)


def _parse_int(value: Any, default: int = 0) -> int:
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        return default


def _parse_date(value: Any) -> str:
    text = str(value or "").strip()
    return text


def _parse_distance(value: Any) -> int:
    text = str(value or "").strip()
    match = DISTANCE_RE.match(text)
    if not match:
        return _parse_int(value, default=0)

    multiplier = int(match.group(1)) if match.group(1) else 1
    base = int(match.group(2))
    return multiplier * base


def ingest_rows(rows: list[dict[str, Any]], dry_run: bool = False) -> dict[str, int]:
    summary = {
        "events_seen": 0,
        "rows_seen": 0,
        "rows_with_result": 0,
        "event_created": 0,
        "event_updated": 0,
        "result_created": 0,
        "result_updated": 0,
    }

    event_keys: set[tuple[str, str, str]] = set()
    normalized_rows: list[dict[str, Any]] = []

    for row in rows:
        title = str(row.get("event_title_folder") or row.get("event_title") or "").strip() or "Unknown Event"
        event_date = _parse_date(row.get("event_date"))
        location = str(row.get("event_location") or row.get("location") or "").strip()

        swimmer_name = str(row.get("name") or row.get("swimmer_name") or "").strip()
        if not swimmer_name:
            continue

        event_keys.add((title, event_date, location))

        result_value = str(row.get("result") or row.get("time") or "").strip()
        summary["rows_seen"] += 1
        if result_value:
            summary["rows_with_result"] += 1

        normalized_rows.append(
            {
                "event_title": title,
                "event_date": event_date,
                "event_location": location,
                "event_order": _parse_int(row.get("event_order"), default=0),
                "swimmer_name": swimmer_name,
                "year_of_birth": _parse_int(row.get("year_of_birth"), default=0),
                "gender": str(row.get("gender") or "").strip(),
                "club": str(row.get("club") or "").strip(),
                "swimming_style": str(row.get("swimming_style") or "").strip(),
                "distance": _parse_distance(row.get("distance")),
                "seri_no": _parse_int(row.get("seri_no"), default=0),
                "lane": _parse_int(row.get("lane"), default=0),
                "seed": str(row.get("seed") or "").strip(),
                "result": result_value,
                "rank": _parse_int(row.get("rank"), default=0),
            }
        )

    summary["events_seen"] = len(event_keys)

    if dry_run:
        return summary

    event_cache: dict[tuple[str, str, str], Event] = {}

    with transaction.atomic():
        for title, event_date, location in event_keys:
            event_obj, created = Event.objects.update_or_create(
                title=title,
                date=event_date,
                location=location,
                defaults={},
            )
            event_cache[(title, event_date, location)] = event_obj
            if created:
                summary["event_created"] += 1
            else:
                summary["event_updated"] += 1

        for row in normalized_rows:
            event_key = (row["event_title"], row["event_date"], row["event_location"])
            event_obj = event_cache[event_key]

            lookup = {
                "event": event_obj,
                "swimmer_name": row["swimmer_name"],
                "year_of_birth": row["year_of_birth"],
                "club": row["club"],
                "swimming_style": row["swimming_style"],
                "distance": row["distance"],
                "seri_no": row["seri_no"],
                "lane": row["lane"],
            }
            defaults = {
                "event_order": row["event_order"],
                "gender": row["gender"],
                "seed": row["seed"],
                "result": row["result"],
                "rank": row["rank"],
            }

            _, created = Result.objects.update_or_create(**lookup, defaults=defaults)
            if created:
                summary["result_created"] += 1
            else:
                summary["result_updated"] += 1

    return summary
