from __future__ import annotations

import hashlib
import re
from typing import Any

from django.db import transaction

from .models import Event, Result


DISTANCE_RE = re.compile(r"^(?:(\d+)\s*[xX]\s*)?(\d+)\s*m$", re.IGNORECASE)
SLUG_RE = re.compile(r"[^a-z0-9]+")


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


def _slugify(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = SLUG_RE.sub("-", text).strip("-")
    return text


def _sanitize_unique_name(value: Any) -> str:
    slug = _slugify(value)
    if not slug:
        return ""
    return slug[:100]


def _build_default_unique_name(title: str, event_date: str, location: str) -> str:
    title_slug = _slugify(title)[:36] or "event"
    location_slug = _slugify(location)[:20]
    source = f"{title}|{event_date}|{location}".encode("utf-8", errors="ignore")
    digest = hashlib.sha1(source).hexdigest()[:8]

    base = f"parsed-{title_slug}"
    if location_slug:
        base = f"{base}-{location_slug}"
    return f"{base}-{digest}"[:100]


def _next_available_unique_name(base_name: str, reserved_names: set[str]) -> str:
    candidate = (base_name or "event")[:100]
    if candidate and candidate not in reserved_names and not Event.objects.filter(unique_name=candidate).exists():
        reserved_names.add(candidate)
        return candidate

    for index in range(2, 10000):
        suffix = f"-{index}"
        truncated = candidate[: 100 - len(suffix)]
        option = f"{truncated}{suffix}"
        if option in reserved_names:
            continue
        if Event.objects.filter(unique_name=option).exists():
            continue
        reserved_names.add(option)
        return option

    raise ValueError("Unable to allocate unique unique_name for event")


def _create_or_get_event_by_details(
    title: str,
    event_date: str,
    location: str,
    reserved_names: set[str],
    preferred_unique_name: str | None = None,
) -> tuple[Event, bool]:
    event = Event.objects.filter(title=title, date=event_date, location=location).first()
    if event is not None:
        if event.unique_name:
            reserved_names.add(event.unique_name)
        return event, False

    preferred = _sanitize_unique_name(preferred_unique_name)
    base_unique_name = preferred or _build_default_unique_name(title, event_date, location)
    unique_name = _next_available_unique_name(base_unique_name, reserved_names)
    created_event = Event.objects.create(
        unique_name=unique_name,
        title=title,
        date=event_date,
        location=location,
    )
    return created_event, True


def ingest_event_rows(rows: list[dict[str, Any]], dry_run: bool = False) -> dict[str, int]:
    summary = {
        "rows_seen": 0,
        "events_seen": 0,
        "event_created": 0,
        "event_updated": 0,
    }

    event_map: dict[tuple[str, str, str], str] = {}

    for row in rows:
        summary["rows_seen"] += 1
        title = str(row.get("event_title_folder") or row.get("event_title") or row.get("title") or "").strip() or "Unknown Event"
        event_date = _parse_date(row.get("event_date") or row.get("date"))
        location = str(row.get("event_location") or row.get("location") or "").strip()
        unique_name = str(row.get("unique_name") or "").strip()

        event_key = (title, event_date, location)
        if event_key not in event_map:
            event_map[event_key] = unique_name

    summary["events_seen"] = len(event_map)

    if dry_run:
        return summary

    reserved_names: set[str] = set()
    with transaction.atomic():
        for (title, event_date, location), preferred_unique_name in event_map.items():
            _, created = _create_or_get_event_by_details(
                title,
                event_date,
                location,
                reserved_names=reserved_names,
                preferred_unique_name=preferred_unique_name,
            )
            if created:
                summary["event_created"] += 1
            else:
                summary["event_updated"] += 1

    return summary


def _resolve_event_for_result_row(
    row: dict[str, Any],
    cache_by_id: dict[int, Event],
    cache_by_unique_name: dict[str, Event],
) -> Event:
    event_id_text = str(row.get("event_id") or "").strip()
    event_unique_name = str(row.get("event_unique_name") or row.get("unique_name") or "").strip()

    if event_id_text:
        event_id = _parse_int(event_id_text, default=0)
        if event_id <= 0:
            raise ValueError(f"Invalid event_id value: {event_id_text}")

        cached = cache_by_id.get(event_id)
        if cached is not None:
            return cached

        event_obj = Event.objects.filter(id=event_id).first()
        if event_obj is None:
            raise ValueError(f"Event not found for event_id={event_id}")

        cache_by_id[event_id] = event_obj
        if event_obj.unique_name:
            cache_by_unique_name[event_obj.unique_name] = event_obj
        return event_obj

    if event_unique_name:
        cached = cache_by_unique_name.get(event_unique_name)
        if cached is not None:
            return cached

        event_obj = Event.objects.filter(unique_name=event_unique_name).first()
        if event_obj is None:
            raise ValueError(f"Event not found for event_unique_name={event_unique_name}")

        cache_by_unique_name[event_unique_name] = event_obj
        cache_by_id[event_obj.id] = event_obj
        return event_obj

    raise ValueError("Each result row must include event_id or event_unique_name")


def _extract_startlist_increment(value: str, prefix: str) -> int | None:
    if not value.startswith(prefix):
        return None

    suffix = value[len(prefix) :]
    if not suffix.isdigit():
        return None

    try:
        return int(suffix)
    except ValueError:
        return None


def _next_startlist_unique_name(
    event_obj: Event,
    next_increment_by_event: dict[int, int],
    reserved_keys: set[str],
) -> str:
    event_id = event_obj.id
    prefix = f"{event_obj.unique_name}-"

    if event_id not in next_increment_by_event:
        max_seen = 0
        existing_keys = Result.objects.filter(event=event_obj).values_list("startlist_unique_name", flat=True)
        for key in existing_keys:
            if not key:
                continue
            parsed = _extract_startlist_increment(str(key), prefix)
            if parsed is not None and parsed > max_seen:
                max_seen = parsed
        next_increment_by_event[event_id] = max_seen + 1

    while True:
        increment = next_increment_by_event[event_id]
        next_increment_by_event[event_id] = increment + 1
        candidate = f"{prefix}{increment:06d}"
        if candidate in reserved_keys:
            continue
        reserved_keys.add(candidate)
        return candidate


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

    event_ids_seen: set[int] = set()
    normalized_rows: list[dict[str, Any]] = []
    event_cache_by_id: dict[int, Event] = {}
    event_cache_by_unique_name: dict[str, Event] = {}

    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise ValueError(f"Row {index} must be an object")

        event_obj = _resolve_event_for_result_row(row, event_cache_by_id, event_cache_by_unique_name)
        event_ids_seen.add(event_obj.id)

        swimmer_name = str(row.get("name") or row.get("swimmer_name") or "").strip()
        if not swimmer_name:
            continue

        result_value = str(row.get("result") or row.get("time") or "").strip()
        summary["rows_seen"] += 1
        if result_value:
            summary["rows_with_result"] += 1

        normalized_rows.append(
            {
                "event": event_obj,
                "startlist_unique_name": str(
                    row.get("startlist_unique_name") or row.get("startlist_key") or row.get("startlist_uid") or ""
                ).strip(),
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

    summary["events_seen"] = len(event_ids_seen)

    if dry_run:
        return summary

    with transaction.atomic():
        # Result ingestion only links to existing events.
        summary["event_created"] = 0
        summary["event_updated"] = len(event_ids_seen)
        next_increment_by_event: dict[int, int] = {}
        reserved_keys: set[str] = set()

        for row in normalized_rows:
            event_obj = row["event"]
            provided_startlist_key = str(row.get("startlist_unique_name") or "").strip() or None

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

            if provided_startlist_key:
                result_obj, created = Result.objects.update_or_create(
                    startlist_unique_name=provided_startlist_key,
                    defaults={**lookup, **defaults},
                )
                reserved_keys.add(provided_startlist_key)
            else:
                result_obj = Result.objects.filter(**lookup).first()
                created = result_obj is None

                if result_obj is None:
                    allocated_key = _next_startlist_unique_name(event_obj, next_increment_by_event, reserved_keys)
                    result_obj = Result.objects.create(
                        startlist_unique_name=allocated_key,
                        **lookup,
                        **defaults,
                    )
                else:
                    for field_name, field_value in defaults.items():
                        setattr(result_obj, field_name, field_value)

                    if not result_obj.startlist_unique_name:
                        result_obj.startlist_unique_name = _next_startlist_unique_name(
                            event_obj,
                            next_increment_by_event,
                            reserved_keys,
                        )
                    else:
                        reserved_keys.add(result_obj.startlist_unique_name)

                    result_obj.save()

            if result_obj.startlist_unique_name:
                reserved_keys.add(result_obj.startlist_unique_name)

            if created:
                summary["result_created"] += 1
            else:
                summary["result_updated"] += 1

    return summary
