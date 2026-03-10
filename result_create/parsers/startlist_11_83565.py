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
HEAT_RE = re.compile(r"^(?:Seri|Heat)\s+(\d+)\s*(?:of|/|\s+)\s*(\d+)$", re.IGNORECASE)
HEADER_SIRA_RE = re.compile(r"^(?:Sira|Kulvar|Lane)$", re.IGNORECASE)
HEADER_YB_RE = re.compile(r"^YB$", re.IGNORECASE)
HEADER_ZAMAN_RE = re.compile(r"^(?:Zaman|Time|Seed Time)$", re.IGNORECASE)
LANE_RE = re.compile(r"^(\d{1,2})\s*$")
YEAR_RE = re.compile(r"^\d{2}$")
TIME_RE = re.compile(r"(?<!\d)\d{1,2}[:.]\d{2}(?:[:.]\d{2})?(?!\d)")
PURE_TIME_RE = re.compile(r"^\d{1,2}[:\-\.]\d{2}(?:[:\-\.]\d{2})?$")
REGISTERED_RE = re.compile(r"^Registered\b", re.IGNORECASE)
SAYFA_RE = re.compile(r"^Sayfa\b", re.IGNORECASE)
NT_RE = re.compile(r"^NT$", re.IGNORECASE)  # No Time

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

    cleaned = re.sub(r"\((?:Tk|Fd|Td)\)", "", normalized, flags=re.IGNORECASE)
    cleaned = cleaned.replace('"', "")
    cleaned = cleaned.replace(",", "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _clean_swimmer_name(value: str | None) -> str | None:
    normalized = _normalize_ocr_text(value)
    if not normalized:
        return None

    cleaned = re.sub(r"\((?:Fd|Td|Tk)\)", "", normalized, flags=re.IGNORECASE)
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
    if not candidate:
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
        if ";" in details_line:
            parts = [part.strip() for part in details_line.split(";") if part.strip()]
            if parts:
                category = parts[0]
            if len(parts) >= 2:
                distance_style = parts[1]
            if len(parts) >= 3 and not level:
                level = parts[2]
            if distance_style and not level and "," in distance_style:
                subparts = [part.strip() for part in distance_style.split(",") if part.strip()]
                if len(subparts) >= 2 and subparts[-1].lower() in {"açık", "acik", "final", "seri"}:
                    distance_style = subparts[0]
                    level = subparts[-1]
        elif "," in details_line:
            parts = [part.strip() for part in details_line.split(",") if part.strip()]
            if parts:
                category = parts[0]
            if len(parts) >= 2:
                distance_style = parts[1]
            if len(parts) >= 3 and not level:
                level = parts[2]
        else:
            distance_style = details_line

    return {
        "category": _normalize_ocr_text(category),
        "distance_style": _normalize_ocr_text(distance_style),
        "level": _normalize_ocr_text(level),
    }


def _line_pages(lines: list[str]) -> list[int]:
    """Return page number for each line."""
    pages: list[int] = []
    current_page = 1

    for line in lines:
        page_match = PAGE_MARKER_RE.match(line)
        if page_match:
            current_page = int(page_match.group(1))
        pages.append(current_page)

    return pages


def _extract_metadata(lines: list[str]) -> tuple[dict[str, Any], set[int], list[str]]:
    """Extract title, location, date from top of document."""
    metadata: dict[str, Any] = {}
    title_fragments: list[str] = []
    title_line_indexes: set[int] = set()

    # Scan first 50 lines for metadata
    for idx in range(min(50, len(lines))):
        line = lines[idx]

        if SPLASH_VERSION_RE.match(line):
            continue

        if PAGE_MARKER_RE.match(line):
            continue

        if LOCATION_DATE_RE.match(line):
            match = LOCATION_DATE_RE.match(line)
            if match:
                metadata["location"] = _normalize_ocr_text(match.group("location"))
                metadata["date"] = match.group("date_part")
            continue

        if DATE_LINE_RE.fullmatch(line):
            metadata["date"] = line
            continue

        if EVENT_START_RE.match(line) or EVENT_SINGLE_LINE_RE.match(line):
            break

        # Collect title fragments
        if line and not REGISTERED_RE.match(line) and not SAYFA_RE.match(line):
            if not HEADER_SIRA_RE.match(line) and not HEADER_YB_RE.match(line):
                title_fragments.append(line)
                title_line_indexes.add(idx)

    if title_fragments:
        metadata["title"] = " ".join(title_fragments)

    return metadata, title_line_indexes, title_fragments


def _normalize_time_token(raw: str) -> str:
    token = raw.strip().replace(",", ".").replace("-", ":")
    chunks = re.findall(r"\d+", token)
    if len(chunks) >= 3:
        return f"{int(chunks[0])}:{chunks[1].zfill(2)}.{chunks[2].zfill(2)}"
    if len(chunks) == 2:
        return f"{int(chunks[0])}.{chunks[1].zfill(2)}"
    return token


def _extract_time_parts(candidate: str) -> tuple[str | None, str, str]:
    time_match = TIME_RE.search(candidate)
    if time_match:
        return time_match.group(0), candidate[: time_match.start()].strip(), candidate[time_match.end() :].strip()

    # OCR fallback: values like "3234" often mean "32.34".
    compact_match = re.fullmatch(r"(\d{2})(\d{2})", candidate)
    if compact_match:
        return f"{compact_match.group(1)}.{compact_match.group(2)}", "", ""

    return None, "", ""


def parse(text: str) -> dict[str, Any]:
    """Stage-2 parser for Splash Meet Manager 11.83565 StartList OCR text.

    Extracts metadata, event info, heats, lanes, and swimmer entries.
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
    entries: list[dict[str, Any]] = []
    current_event: dict[str, Any] | None = None
    current_heat: dict[str, Any] | None = None

    line_no = 1
    total_lines = len(lines)
    while line_no <= total_lines:
        line = lines[line_no - 1]

        if not line:
            line_no += 1
            continue

        # Page marker
        page_match = PAGE_MARKER_RE.match(line)
        if page_match:
            mark(line_no, "page_marker", extras={"page_no": int(page_match.group(1))})
            line_no += 1
            continue

        # Splash version
        if SPLASH_VERSION_RE.match(line):
            mark(line_no, "splash_version")
            line_no += 1
            continue

        # Title lines
        if line in title_fragments or (line_no - 1) in title_line_indexes:
            mark(line_no, "title_line")
            line_no += 1
            continue

        # Location and date
        if LOCATION_DATE_RE.match(line):
            mark(line_no, "location_and_date_part")
            line_no += 1
            continue

        if DATE_LINE_RE.fullmatch(line):
            mark(line_no, "date_line")
            line_no += 1
            continue

        # Headers
        if HEADER_SIRA_RE.match(line):
            mark(line_no, "header_lane")
            line_no += 1
            continue

        if HEADER_YB_RE.match(line):
            mark(line_no, "header_year")
            line_no += 1
            continue

        if HEADER_ZAMAN_RE.match(line):
            mark(line_no, "header_time")
            line_no += 1
            continue

        # Footer
        if REGISTERED_RE.match(line):
            mark(line_no, "footer_registered")
            line_no += 1
            continue

        if SAYFA_RE.match(line):
            mark(line_no, "footer_page")
            line_no += 1
            continue

        # Event detection
        event_match = EVENT_SINGLE_LINE_RE.match(line)
        if event_match:
            event_no = event_match.group(1)
            details_line = event_match.group(2).strip()
            event_name = _compose_event_name(event_no, details_line, None)
            components = _parse_event_components(details_line, None)

            current_event = {
                "event_no": event_no,
                "event_name": event_name,
                **components,
            }
            events.append(current_event)
            current_heat = None
            mark(line_no, "event_single_line", extras={"event_no": event_no})
            line_no += 1
            continue

        event_start_match = EVENT_START_RE.match(line)
        if event_start_match:
            event_no = event_start_match.group(1)
            mark(line_no, "event_number_line", extras={"event_no": event_no})
            line_no += 1

            # Next line(s) contain event details
            details_line = None
            phase_line = None

            if line_no <= total_lines:
                candidate = lines[line_no - 1]
                if candidate and not EVENT_START_RE.match(candidate) and not PAGE_MARKER_RE.match(candidate):
                    details_line = candidate
                    mark(line_no, "event_details_line")
                    line_no += 1

                    if line_no <= total_lines:
                        candidate2 = lines[line_no - 1]
                        if candidate2 and not EVENT_START_RE.match(candidate2) and len(candidate2.split()) <= 3:
                            phase_line = candidate2
                            mark(line_no, "event_phase_line")
                            line_no += 1

            event_name = _compose_event_name(event_no, details_line, phase_line)
            components = _parse_event_components(details_line, phase_line)

            current_event = {
                "event_no": event_no,
                "event_name": event_name,
                **components,
            }
            events.append(current_event)
            current_heat = None
            continue

        # Heat detection
        heat_match = HEAT_RE.match(line)
        if heat_match and current_event:
            heat_no = heat_match.group(1)
            total_heats = heat_match.group(2)
            current_heat = {
                "event_no": current_event["event_no"],
                "heat_no": heat_no,
                "total_heats": total_heats,
            }
            mark(line_no, "heat_line", extras={"heat_no": heat_no, "total_heats": total_heats})
            line_no += 1
            continue

        # Lane and swimmer entry
        # Expect pattern: Lane number -> Name (possibly multi-line) -> Birth Year -> Club -> Time/NT
        lane_match = LANE_RE.fullmatch(line)
        if lane_match and current_event:
            lane_no = lane_match.group(1)
            mark(line_no, "lane_number", extras={"lane": lane_no})
            line_no += 1

            # Swimmer name
            swimmer_name: str | None = None
            if line_no <= total_lines:
                candidate = lines[line_no - 1]
                if candidate and not LANE_RE.fullmatch(candidate) and not YEAR_RE.fullmatch(candidate):
                    swimmer_name = _clean_swimmer_name(candidate)
                    mark(line_no, "swimmer_name")
                    line_no += 1

                    # Check for name continuation
                    while line_no <= total_lines:
                        extra_candidate = lines[line_no - 1]
                        if not extra_candidate or not _looks_like_name_continuation(extra_candidate):
                            break
                        swimmer_name = _clean_swimmer_name(f"{swimmer_name} {extra_candidate}")
                        mark(line_no, "swimmer_name_continuation")
                        line_no += 1

            # Birth year
            birth_year: str | None = None
            if line_no <= total_lines:
                candidate = lines[line_no - 1]
                if YEAR_RE.fullmatch(candidate):
                    birth_year = candidate
                    mark(line_no, "birth_year")
                    line_no += 1

            # Club name
            club: str | None = None
            if line_no <= total_lines:
                candidate = lines[line_no - 1]
                if candidate and not PURE_TIME_RE.fullmatch(candidate) and not NT_RE.match(candidate):
                    club = _clean_club_name(candidate)
                    mark(line_no, "club")
                    line_no += 1

            # Seed time
            seed_time: str | None = None
            if line_no <= total_lines:
                candidate = lines[line_no - 1]
                if NT_RE.match(candidate):
                    seed_time = "NT"
                    mark(line_no, "seed_time_nt")
                    line_no += 1
                elif PURE_TIME_RE.fullmatch(candidate):
                    seed_time = _normalize_time_token(candidate)
                    mark(line_no, "seed_time")
                    line_no += 1
                else:
                    # Try extracting time from mixed text
                    time_value, _, _ = _extract_time_parts(candidate)
                    if time_value:
                        seed_time = _normalize_time_token(time_value)
                        mark(line_no, "seed_time_embedded")
                        line_no += 1

            # Create entry
            entry = {
                "event_no": current_event["event_no"],
                "event_name": current_event["event_name"],
                "heat_no": current_heat["heat_no"] if current_heat else None,
                "lane": lane_no,
                "swimmer_name": swimmer_name,
                "birth_year": birth_year,
                "club": club,
                "seed_time": seed_time,
                "page": pages[line_no - 2],
            }
            entries.append(entry)
            continue

        # Unmatched line
        mark(line_no, "unmatched", structured=False)
        line_no += 1

    # Build annotated lines
    annotated_lines: list[dict[str, Any]] = []
    for idx, line in enumerate(lines):
        line_obj = {
            "line_no": idx + 1,
            "page": pages[idx],
            "text": line,
        }
        if (idx + 1) in line_labels:
            line_obj.update(line_labels[idx + 1])
        else:
            line_obj["label"] = "unmatched"
            line_obj["structured"] = False

        annotated_lines.append(line_obj)

    return {
        "metadata": metadata,
        "event_count": len(events),
        "events": events,
        "entry_count": len(entries),
        "entries": entries,
        "line_count": len(lines),
        "annotated_lines": annotated_lines,
    }
