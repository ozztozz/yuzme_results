from __future__ import annotations

import re
from typing import Any


PAGE_MARKER_RE = re.compile(r"^---\s*Page\s*(\d+)\s*---$", re.IGNORECASE)
LOCATION_DATE_RE = re.compile(
    r"^(?P<location>[A-ZÇĞİÖŞÜ]+)\s*;\s*(?P<date_part>\d{1,2}\.\d{1,2}\.?)*$"
)
DATE_LINE_RE = re.compile(r"^\d{1,2}\.\d{1,2}\.\d{4}$")
SPLASH_VERSION_RE = re.compile(r"^Splash\s+Meet\s+Manager\s*,\s*([0-9]+(?:\.[0-9]+)+)$", re.IGNORECASE)
EVENT_SINGLE_LINE_RE = re.compile(r"^Yarış\s+(\d+)\s*,\s*(.+)$", re.IGNORECASE)
EVENT_START_RE = re.compile(r"^Yarış\s+(\d+)\s*$", re.IGNORECASE)
HEADER_SIRA_RE = re.compile(r"^Sira$", re.IGNORECASE)
HEADER_YB_RE = re.compile(r"^YB$", re.IGNORECASE)
HEADER_ZAMAN_RE = re.compile(r"^Zaman\s+\w+$", re.IGNORECASE)
RANK_RE = re.compile(r"^(\d{1,3})\s*\.?\s*$")
YEAR_RE = re.compile(r"^\d{2}$")
# Time pattern that also works when OCR glues text and time together (e.g. Ortaoku28.45)
TIME_RE = re.compile(r"(?<!\d)\d{1,2}[:.]\d{2}(?:[:.]\d{2})?(?!\d)")
POINTS_RE = re.compile(r"^\d{1,4}$")
REGISTERED_RE = re.compile(r"^Registered\b", re.IGNORECASE)
SAYFA_RE = re.compile(r"^Sayfa\b", re.IGNORECASE)
DISK_MARKER_RE = re.compile(r"^dis[kq]\s*\.?$", re.IGNORECASE)
TD_MARKER_RE = re.compile(r"^td\s*\.?$", re.IGNORECASE)
SW_RULE_RE = re.compile(r"^SW\s*[0-9]+(?:\.[0-9]+)?$", re.IGNORECASE)
ZAMAN_NOTE_RE = re.compile(r"^\(Zaman:\s*([^)]+)\)$", re.IGNORECASE)
STATUS_NOTE_RE = re.compile(r"^(DO|DQ)$", re.IGNORECASE)

OCR_TEXT_REPLACEMENTS: list[tuple[str, str]] = [
    (r"\bS[O0]m\b", "50m"),
    (r"\bKüçukler\b", "Küçükler"),
    (r"\bBirinciligi\b", "Birinciliği"),
    (r"\((Fd|Td|Tk)[\}\]]", r"(\1)"),
]

SWIMMER_NAME_REPLACEMENTS: list[tuple[str, str]] = [
    (r"\bEren\s+GÜRC0\b", "Eren GÜRCÜ"),
    (r"\bÇınar\s+KrYSÜREN\b", "Çınar KÖYSÜREN"),
    (r"\bRuzgar\b", "Rüzgar"),
]


def _clean_line(line: str) -> str:
    return re.sub(r"\s+", " ", line).strip()


def _normalize_ocr_text(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = value
    for pattern, replacement in OCR_TEXT_REPLACEMENTS:
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)

    return re.sub(r"\s+", " ", normalized).strip()


def _clean_club_name(value: str | None) -> str | None:
    normalized = _normalize_ocr_text(value)
    if not normalized:
        return None

    cleaned = re.sub(r"\((?:Tk|Fd)\)", "", normalized, flags=re.IGNORECASE)
    cleaned = cleaned.replace('"', "")
    cleaned = cleaned.replace(",", "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _clean_swimmer_name(value: str | None) -> str | None:
    normalized = _normalize_ocr_text(value)
    if not normalized:
        return None

    cleaned = re.sub(r"\((?:Fd|Td)\)", "", normalized, flags=re.IGNORECASE)
    cleaned = cleaned.replace('"', "")
    cleaned = cleaned.replace(",", "")
    for pattern, replacement in SWIMMER_NAME_REPLACEMENTS:
        cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _looks_like_name_continuation(line: str) -> bool:
    candidate = line.strip()
    if not candidate:
        return False
    if any(ch.isdigit() for ch in candidate):
        return False
    if "(" in candidate or ")" in candidate:
        return False
    if ";" in candidate or "," in candidate:
        return False

    tokens = candidate.split()
    if not tokens or len(tokens) > 3:
        return False
    if not all(re.fullmatch(r"[A-Za-zÇĞİÖŞÜçğıöşü]+", token) for token in tokens):
        return False

    return candidate == candidate.upper()


def _compose_event_name(event_no: str, details_line: str | None, phase_line: str | None) -> str:
    segments: list[str] = [f"Yarış {event_no}"]
    if details_line:
        segments.append(details_line)
    if phase_line:
        segments.append(phase_line)
    event_name = "; ".join(segments[:1]) + (", " + "; ".join(segments[1:]) if len(segments) > 1 else "")
    return _normalize_ocr_text(event_name) or event_name


def _parse_event_components(details_line: str | None, phase_line: str | None) -> dict[str, str | None]:
    category = None
    distance_style = None
    level = phase_line

    if details_line:
        parts = [part.strip() for part in details_line.split(";") if part.strip()]
        if parts:
            category = parts[0]
        if len(parts) >= 2:
            distance_style = parts[1]
        if len(parts) >= 3 and not level:
            level = parts[2]

    return {
        "category": _normalize_ocr_text(category),
        "distance_style": _normalize_ocr_text(distance_style),
        "level": _normalize_ocr_text(level),
    }


def _is_special_boundary(lines: list[str], line_no: int) -> bool:
    line = lines[line_no - 1]
    if not line:
        return False
    if PAGE_MARKER_RE.match(line) or SPLASH_VERSION_RE.match(line):
        return True
    if EVENT_SINGLE_LINE_RE.match(line) or EVENT_START_RE.match(line):
        return True
    if HEADER_SIRA_RE.match(line) or HEADER_YB_RE.match(line) or HEADER_ZAMAN_RE.match(line):
        return True
    if REGISTERED_RE.match(line) or SAYFA_RE.match(line):
        return True
    if DISK_MARKER_RE.match(line) or TD_MARKER_RE.match(line):
        return True
    return False


def _prev_non_empty(lines: list[str], line_no: int) -> str | None:
    for i in range(line_no - 1, 0, -1):
        candidate = lines[i - 1]
        if candidate:
            return candidate
    return None


def _next_non_empty(lines: list[str], line_no: int) -> str | None:
    for i in range(line_no + 1, len(lines) + 1):
        candidate = lines[i - 1]
        if candidate:
            return candidate
    return None


def _rank_from_line(line: str, prev_line: str | None, next_line: str | None = None) -> int | None:
    match = RANK_RE.fullmatch(line)
    if not match:
        return None

    rank = int(match.group(1))
    if rank < 1:
        return None

    # If OCR preserved trailing dot, this is a rank line.
    if "." in line:
        return rank

    # If next visible line is a dotted rank (e.g. "13."), current numeric line
    # is likely a points value from previous record.
    if next_line and RANK_RE.fullmatch(next_line) and "." in next_line:
        return None

    # Without dot, disambiguate against year values like 14/15.
    if prev_line is None:
        return None

    prev = prev_line.strip()
    if PAGE_MARKER_RE.match(prev):
        return rank
    if HEADER_ZAMAN_RE.match(prev) or HEADER_YB_RE.match(prev) or HEADER_SIRA_RE.match(prev):
        return rank
    if TIME_RE.search(prev):
        return rank
    if POINTS_RE.fullmatch(prev):
        return rank

    return None


def _extract_time_parts(candidate: str) -> tuple[str | None, str, str]:
    time_match = TIME_RE.search(candidate)
    if time_match:
        return time_match.group(0), candidate[: time_match.start()].strip(), candidate[time_match.end() :].strip()

    # OCR fallback: values like "3234" often mean "32.34".
    compact_match = re.fullmatch(r"(\d{2})(\d{2})", candidate)
    if compact_match:
        return f"{compact_match.group(1)}.{compact_match.group(2)}", "", ""

    return None, "", ""


def _extract_metadata(lines: list[str]) -> tuple[dict[str, str | None], set[int], set[str]]:
    title: str | None = None
    location: str | None = None
    date: str | None = None

    title_line_indexes: set[int] = set()
    title_fragments: set[str] = set()

    first_location_idx: int | None = None
    first_date_part: str | None = None

    for index, line in enumerate(lines):
        if not line:
            continue

        location_match = LOCATION_DATE_RE.match(line)
        if location_match:
            location = location_match.group("location")
            first_date_part = (location_match.group("date_part") or "").strip()
            first_location_idx = index
            break

    if first_location_idx is not None:
        date_second_part: str | None = None
        if first_location_idx + 1 < len(lines):
            next_line = lines[first_location_idx + 1]
            if DATE_LINE_RE.fullmatch(next_line):
                date_second_part = next_line

        if first_date_part and date_second_part:
            date = f"{first_date_part}{date_second_part}"
        elif first_date_part:
            date = first_date_part
        elif date_second_part:
            date = date_second_part

        scan_start = 0
        for i in range(first_location_idx - 1, -1, -1):
            if PAGE_MARKER_RE.match(lines[i]):
                scan_start = i + 1
                break

        title_parts: list[str] = []
        for i in range(scan_start, first_location_idx):
            candidate = lines[i]
            if not candidate:
                continue
            if SPLASH_VERSION_RE.match(candidate):
                continue
            title_parts.append(candidate)
            title_line_indexes.add(i)
            title_fragments.add(candidate)

        if title_parts:
            title = _normalize_ocr_text(" ".join(title_parts))

    return (
        {
            "title": title,
            "location": location,
            "date": date,
        },
        title_line_indexes,
        title_fragments,
    )


def _line_pages(lines: list[str]) -> list[int | None]:
    pages: list[int | None] = []
    current_page: int | None = None
    for line in lines:
        page_match = PAGE_MARKER_RE.match(line)
        if page_match:
            current_page = int(page_match.group(1))
        pages.append(current_page)
    return pages


def parse(text: str) -> dict[str, Any]:
    """Stage-2 parser for Splash Meet Manager 11.83565 OCR text.

    Extracts metadata, event info, ranked result rows, and special records
    (such as disk/td blocks), then annotates all lines as structured/unmatched.
    """
    lines = [_clean_line(line) for line in text.replace("\r\n", "\n").split("\n")]
    pages = _line_pages(lines)

    metadata, title_line_indexes, title_fragments = _extract_metadata(lines)

    line_labels: dict[int, dict[str, Any]] = {}

    def mark(line_no: int, label: str, structured: bool = True, extras: dict[str, Any] | None = None) -> None:
        payload = {
            "label": label,
            "structured": structured,
        }
        if extras:
            payload.update(extras)
        line_labels[line_no] = payload

    events: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    special_records: list[dict[str, Any]] = []
    current_event: dict[str, Any] | None = None

    line_no = 1
    total_lines = len(lines)
    while line_no <= total_lines:
        line = lines[line_no - 1]

        if not line:
            line_no += 1
            continue

        page_match = PAGE_MARKER_RE.match(line)
        if page_match:
            mark(line_no, "page_marker", extras={"page_no": int(page_match.group(1))})
            line_no += 1
            continue

        if SPLASH_VERSION_RE.match(line):
            mark(line_no, "splash_version")
            line_no += 1
            continue

        if line in title_fragments or (line_no - 1) in title_line_indexes:
            mark(line_no, "title_line")
            line_no += 1
            continue

        if LOCATION_DATE_RE.match(line):
            mark(line_no, "location_and_date_part")
            line_no += 1
            continue

        if DATE_LINE_RE.fullmatch(line):
            mark(line_no, "date_line")
            line_no += 1
            continue

        if HEADER_SIRA_RE.match(line):
            mark(line_no, "header_rank")
            line_no += 1
            continue

        if HEADER_YB_RE.match(line):
            mark(line_no, "header_year")
            line_no += 1
            continue

        if HEADER_ZAMAN_RE.match(line):
            mark(line_no, "header_time_points")
            line_no += 1
            continue

        if REGISTERED_RE.match(line):
            mark(line_no, "footer_registered")
            line_no += 1
            continue

        if SAYFA_RE.match(line):
            mark(line_no, "footer_page")
            line_no += 1
            continue

        if DISK_MARKER_RE.match(line) or TD_MARKER_RE.match(line):
            marker_line = line
            special_type = "disk" if DISK_MARKER_RE.match(line) else "td"
            special_start = line_no
            mark(line_no, "special_marker", extras={"special_type": special_type})
            line_no += 1

            swimmer_name: str | None = None
            birth_year: str | None = None
            club_fragments: list[str] = []
            time_value: str | None = None
            points_value: str | None = None
            rule_code: str | None = None
            status_code: str | None = "DQ" if special_type == "disk" else None
            note_lines: list[str] = []
            note_time: str | None = None

            while line_no <= total_lines:
                candidate = lines[line_no - 1]

                if not candidate:
                    line_no += 1
                    continue

                if _is_special_boundary(lines, line_no):
                    break

                if swimmer_name is None and not YEAR_RE.fullmatch(candidate):
                    swimmer_name = _clean_swimmer_name(candidate)
                    mark(line_no, "special_swimmer_name", extras={"special_type": special_type})
                    line_no += 1

                    while line_no <= total_lines:
                        extra_candidate = lines[line_no - 1]
                        if not extra_candidate or not _looks_like_name_continuation(extra_candidate):
                            break
                        swimmer_name = _clean_swimmer_name(f"{swimmer_name} {extra_candidate}")
                        mark(line_no, "special_swimmer_name_continuation", extras={"special_type": special_type})
                        line_no += 1
                    continue

                if birth_year is None and YEAR_RE.fullmatch(candidate):
                    birth_year = candidate
                    mark(line_no, "special_birth_year", extras={"special_type": special_type})
                    line_no += 1
                    continue

                if rule_code is None and SW_RULE_RE.fullmatch(candidate):
                    rule_code = candidate
                    mark(line_no, "special_rule", extras={"special_type": special_type})
                    line_no += 1
                    continue

                zaman_match = ZAMAN_NOTE_RE.fullmatch(candidate)
                if zaman_match:
                    note_time = zaman_match.group(1).strip()
                    note_lines.append(candidate)
                    mark(line_no, "special_note_time", extras={"special_type": special_type})
                    line_no += 1
                    continue

                if STATUS_NOTE_RE.fullmatch(candidate):
                    status_code = candidate.upper()
                    mark(line_no, "special_status", extras={"special_type": special_type})
                    line_no += 1
                    continue

                if time_value is None:
                    extracted_time, left, right = _extract_time_parts(candidate)
                    if extracted_time:
                        time_value = extracted_time
                        if left:
                            club_fragments.append(left)
                        if right and not POINTS_RE.fullmatch(right):
                            club_fragments.append(right)
                        mark(line_no, "special_time_line", extras={"special_type": special_type})
                        line_no += 1

                        if line_no <= total_lines:
                            possible_points = lines[line_no - 1]
                            if POINTS_RE.fullmatch(possible_points):
                                points_value = possible_points
                                mark(line_no, "special_points_line", extras={"special_type": special_type})
                                line_no += 1
                        continue

                if points_value is None and time_value is not None and POINTS_RE.fullmatch(candidate):
                    points_value = candidate
                    mark(line_no, "special_points_line", extras={"special_type": special_type})
                    line_no += 1
                    continue

                note_lines.append(candidate)
                club_fragments.append(candidate)
                mark(line_no, "special_note_or_club", extras={"special_type": special_type})
                line_no += 1

            special_records.append(
                {
                    "event_no": current_event.get("event_no") if current_event else None,
                    "event_name": current_event.get("event_name") if current_event else None,
                    "page": pages[special_start - 1],
                    "special_type": special_type,
                    "status": status_code,
                    "swimmer_name": _clean_swimmer_name(swimmer_name),
                    "birth_year": birth_year,
                    "club": _clean_club_name(" ".join(part for part in club_fragments if part).strip()),
                    "time": time_value,
                    "points": points_value,
                    "rule": rule_code,
                    "note_time": note_time,
                    "notes": note_lines,
                    "marker_text": marker_line,
                    "line_start": special_start,
                    "line_end": line_no - 1,
                }
            )
            continue

        # One-line event format, for example: "Yarış 20, Erkekler; 50m Serbest; Açık"
        event_single_match = EVENT_SINGLE_LINE_RE.match(line)
        if event_single_match:
            event_no = event_single_match.group(1)
            details_all = event_single_match.group(2).strip()
            details_parts = [part.strip() for part in details_all.split(";") if part.strip()]

            details_line = "; ".join(details_parts[:2]) if details_parts else None
            phase_line = details_parts[2] if len(details_parts) >= 3 else None

            components = _parse_event_components(details_line, phase_line)
            event_name = _compose_event_name(event_no, details_line, phase_line)

            current_event = {
                "event_no": event_no,
                "event_name": event_name,
                "page": pages[line_no - 1],
                **components,
            }
            events.append(current_event)
            mark(line_no, "event_header", extras={"event_no": event_no})
            line_no += 1
            continue

        # Multi-line event format, for example: "Yarış 20" + next lines.
        event_start_match = EVENT_START_RE.match(line)
        if event_start_match:
            event_no = event_start_match.group(1)
            details_line: str | None = None
            phase_line: str | None = None
            consumed = 1

            if line_no < total_lines:
                next_line = lines[line_no]
                if next_line and ";" in next_line:
                    details_line = next_line
                    consumed = 2

            if line_no + consumed - 1 < total_lines:
                candidate = lines[line_no + consumed - 1]
                if candidate and not PAGE_MARKER_RE.match(candidate) and not EVENT_START_RE.match(candidate):
                    if candidate.lower() in {"açık", "acik", "final", "seri"}:
                        phase_line = candidate
                        consumed += 1

            components = _parse_event_components(details_line, phase_line)
            event_name = _compose_event_name(event_no, details_line, phase_line)
            current_event = {
                "event_no": event_no,
                "event_name": event_name,
                "page": pages[line_no - 1],
                **components,
            }
            events.append(current_event)

            mark(line_no, "event_header", extras={"event_no": event_no})
            if consumed >= 2:
                mark(line_no + 1, "event_header_detail", extras={"event_no": event_no})
            if consumed >= 3:
                mark(line_no + 2, "event_header_level", extras={"event_no": event_no})

            line_no += consumed
            continue

        rank = _rank_from_line(line, _prev_non_empty(lines, line_no), _next_non_empty(lines, line_no))
        if rank is not None:
            record_line_start = line_no
            mark(line_no, "rank_line", extras={"rank": rank})
            line_no += 1

            swimmer_name: str | None = None
            birth_year: str | None = None
            club_fragments: list[str] = []
            time_value: str | None = None
            points_value: str | None = None

            while line_no <= total_lines:
                candidate = lines[line_no - 1]
                if not candidate:
                    line_no += 1
                    continue

                if PAGE_MARKER_RE.match(candidate) or SPLASH_VERSION_RE.match(candidate):
                    break

                if EVENT_SINGLE_LINE_RE.match(candidate) or EVENT_START_RE.match(candidate):
                    break

                if DISK_MARKER_RE.match(candidate) or TD_MARKER_RE.match(candidate):
                    break

                next_rank = _rank_from_line(candidate, _prev_non_empty(lines, line_no), _next_non_empty(lines, line_no))
                if next_rank is not None and (swimmer_name or time_value or club_fragments):
                    break

                if swimmer_name is None and not YEAR_RE.fullmatch(candidate):
                    swimmer_name = _clean_swimmer_name(candidate)
                    mark(line_no, "swimmer_name")
                    line_no += 1

                    while line_no <= total_lines:
                        extra_candidate = lines[line_no - 1]
                        if not extra_candidate or not _looks_like_name_continuation(extra_candidate):
                            break
                        swimmer_name = _clean_swimmer_name(f"{swimmer_name} {extra_candidate}")
                        mark(line_no, "swimmer_name_continuation")
                        line_no += 1
                    continue

                if birth_year is None and YEAR_RE.fullmatch(candidate):
                    birth_year = candidate
                    mark(line_no, "birth_year")
                    line_no += 1
                    continue

                if time_value is None:
                    extracted_time, left, right = _extract_time_parts(candidate)
                    if extracted_time:
                        time_value = extracted_time
                        if left:
                            club_fragments.append(left)
                        if right and not POINTS_RE.fullmatch(right):
                            club_fragments.append(right)
                        mark(line_no, "time_line")
                        line_no += 1

                        if line_no <= total_lines:
                            possible_points = lines[line_no - 1]
                            if POINTS_RE.fullmatch(possible_points):
                                points_value = possible_points
                                mark(line_no, "points_line")
                                line_no += 1
                        continue

                if points_value is None and time_value is not None and POINTS_RE.fullmatch(candidate):
                    points_value = candidate
                    mark(line_no, "points_line")
                    line_no += 1
                    continue

                club_fragments.append(candidate)
                mark(line_no, "club_line")
                line_no += 1

            records.append(
                {
                    "event_no": current_event.get("event_no") if current_event else None,
                    "event_name": current_event.get("event_name") if current_event else None,
                    "page": pages[record_line_start - 1],
                    "rank": rank,
                    "swimmer_name": _clean_swimmer_name(swimmer_name),
                    "birth_year": birth_year,
                    "club": _clean_club_name(" ".join(part for part in club_fragments if part).strip()),
                    "time": time_value,
                    "points": points_value,
                    "line_start": record_line_start,
                    "line_end": line_no - 1,
                }
            )
            continue

        line_no += 1

    annotations: list[dict[str, Any]] = []
    unmatched_lines: list[dict[str, Any]] = []

    for index, line in enumerate(lines, start=1):
        page = pages[index - 1]
        if not line:
            entry = {
                "line_no": index,
                "page": page,
                "text": line,
                "label": "empty",
                "structured": True,
            }
            annotations.append(entry)
            continue

        label_info = line_labels.get(index)
        if label_info is None:
            entry = {
                "line_no": index,
                "page": page,
                "text": line,
                "label": "unmatched",
                "structured": False,
            }
            annotations.append(entry)
            unmatched_lines.append(entry)
            continue

        entry = {
            "line_no": index,
            "page": page,
            "text": line,
            **label_info,
        }
        annotations.append(entry)

    return {
        "version": "11.83565",
        "metadata": metadata,
        "events": events,
        "record_count": len(records),
        "records": records,
        "special_record_count": len(special_records),
        "special_records": special_records,
        "line_count": len(lines),
        "structured_count": sum(1 for item in annotations if item.get("structured")),
        "unmatched_count": len(unmatched_lines),
        "line_annotations": annotations,
        "unmatched_lines": unmatched_lines,
    }
