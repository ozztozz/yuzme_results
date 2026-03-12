from __future__ import annotations

import argparse
from difflib import SequenceMatcher
import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import fitz

from create_event import scrape_event_detail
from extract_result import extract_pdf_text
from parse_splash_result import detect_splash_version as detect_result_version
from parse_splash_result import load_version_parser
from parse_startlist_to_csv import parse_events_only
from read_ocr import detect_tessdata_path
from scrape_results import result_scraper, start_scraper


LIST_NUMBER_RE = re.compile(r"(?i)(?:start|star|result|results)\s*list[_\-\s]*(\d+)")
LEADING_RANK_RE = re.compile(r"^\s*\d{1,3}\s*\.?\s+")
TRAILING_YEAR_RE = re.compile(r"\s+\d{2}\s*$")

TURKISH_ASCII_MAP = str.maketrans(
    {
        "ı": "i",
        "İ": "i",
        "ş": "s",
        "Ş": "s",
        "ğ": "g",
        "Ğ": "g",
        "ü": "u",
        "Ü": "u",
        "ö": "o",
        "Ö": "o",
        "ç": "c",
        "Ç": "c",
    }
)

SCRAPED_ROOT = Path("scraped")
STATE_DIR = Path("results")
LOCAL_WEB_BASE_URL = "http://127.0.0.1:8000"
REMOTE_WEB_BASE_URL = os.getenv("YUZME_WEB_BASE_URL", "https://ozztozz.pythonanywhere.com/").rstrip("/")
INGEST_TOKEN = os.getenv("YUZME_INGEST_TOKEN", "").strip() or None
TEMP_RESULT_TEXT_DIR = Path("results/_tmp_result_text_clean_update")
CONTROL_TEXT_DIR = Path("results/_tmp_extracted_text_clean_update")
PARSERS_DIR = Path(__file__).parent / "parsers"


class NonTextPdfError(ValueError):
    pass


def _parse_version_token(value: str) -> tuple[int, int] | None:
    parts = str(value).strip().split(".")
    if len(parts) != 2:
        return None
    if not parts[0].isdigit() or not parts[1].isdigit():
        return None
    return int(parts[0]), int(parts[1])


def _available_parser_versions(parsers_dir: Path) -> list[str]:
    versions: list[str] = []
    for parser_file in parsers_dir.glob("*.py"):
        if parser_file.name == "__init__.py":
            continue
        token = parser_file.stem.replace("_", ".")
        if _parse_version_token(token) is None:
            continue
        versions.append(token)
    return versions


def _nearest_parser_version(detected_version: str, available_versions: list[str]) -> str | None:
    detected = _parse_version_token(detected_version)
    if detected is None:
        return None

    same_major: list[tuple[int, str]] = []
    for version in available_versions:
        parsed = _parse_version_token(version)
        if parsed is None:
            continue
        if parsed[0] != detected[0]:
            continue
        diff = abs(parsed[1] - detected[1])
        same_major.append((diff, version))

    if not same_major:
        return None

    same_major.sort(key=lambda item: item[0])
    return same_major[0][1]


def load_result_parser_with_fallback(detected_version: str):
    try:
        return detected_version, load_version_parser(detected_version, PARSERS_DIR)
    except FileNotFoundError:
        available_versions = _available_parser_versions(PARSERS_DIR)
        fallback = _nearest_parser_version(detected_version, available_versions)
        if not fallback:
            raise
        print(
            f"Parser fallback: using {fallback} for detected version {detected_version}")
        return fallback, load_version_parser(fallback, PARSERS_DIR)


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


def normalize_match_text(value: Any) -> str:
    if value is None:
        return ""

    text = str(value).strip().translate(TURKISH_ASCII_MAP).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]", "", text)
    return text


def _normalize_result_swimmer_name(value: str) -> str:
    cleaned = LEADING_RANK_RE.sub("", str(value or "").strip())
    cleaned = TRAILING_YEAR_RE.sub("", cleaned)
    cleaned = re.sub(r"[\"'`]+", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _looks_like_club_text(value: str) -> bool:
    lowered = str(value or "").strip().lower()
    if not lowered:
        return False

    club_markers = (
        "spor",
        "kulub",
        "kulüb",
        "belediye",
        "kolej",
        "universite",
        "üniversite",
        "ferdi",
        "federasyon",
    )
    return any(marker in lowered for marker in club_markers)


def _looks_like_fragment_name(swimmer_name: str, club_value: str) -> bool:
    name = str(swimmer_name or "").strip()
    if not name:
        return False

    club_norm = normalize_match_text(club_value)
    # Common OCR split: swimmer_name becomes club prefix and club becomes only "kulubu".
    if not club_norm.startswith("kulub"):
        return False

    tokens = [token for token in name.split() if token]
    if not tokens or len(tokens) > 2:
        return False

    # Genuine swimmer rows usually have an all-caps surname token.
    has_upper_surname = any(token.isupper() and len(token) >= 2 for token in tokens)
    if has_upper_surname:
        return False

    # Organization/location-like short phrase paired with generic club tail.
    return True


def _similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def _name_tokens(value: str) -> list[str]:
    tokenized = normalize_match_text(value)
    if not tokenized:
        return []

    # Re-tokenize from raw text to preserve word boundaries for surname fallback.
    raw = str(value or "").translate(TURKISH_ASCII_MAP).lower()
    raw = unicodedata.normalize("NFKD", raw)
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    parts = re.split(r"[^a-z0-9]+", raw)
    return [part for part in parts if part]


def _drop_single_token_ocr_duplicates(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for rec in records:
        key = (str(rec.get("result") or ""), str(rec.get("_club_norm") or ""), int(rec.get("rank") or 0))
        grouped.setdefault(key, []).append(rec)

    filtered: list[dict[str, Any]] = []
    for rec in records:
        key = (str(rec.get("result") or ""), str(rec.get("_club_norm") or ""), int(rec.get("rank") or 0))
        siblings = grouped.get(key, [])
        if len(siblings) <= 1:
            filtered.append(rec)
            continue

        rec_tokens = _name_tokens(str(rec.get("swimmer_name") or ""))
        if len(rec_tokens) != 1:
            filtered.append(rec)
            continue

        token = rec_tokens[0]
        has_richer_duplicate = any(
            sib is not rec
            and len(_name_tokens(str(sib.get("swimmer_name") or ""))) >= 2
            and token in _name_tokens(str(sib.get("swimmer_name") or ""))
            for sib in siblings
        )

        if has_richer_duplicate:
            continue

        filtered.append(rec)

    return filtered


def extract_list_number(file_path: Path) -> str | None:
    stem = file_path.stem.translate(TURKISH_ASCII_MAP)
    match = LIST_NUMBER_RE.search(stem)
    if not match:
        return None
    return match.group(1)


def extract_text_from_pdf(pdf_path: Path) -> str:
    pages: list[str] = []
    with fitz.open(pdf_path) as document:
        for page in document:
            pages.append(page.get_text("text") or "")

    text = "\n".join(pages).strip()
    if not text:
        raise NonTextPdfError(f"No text extracted from PDF (expected text-based PDF): {pdf_path}")
    return text


def _safe_name(value: str, fallback: str = "untitled") -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "")).strip()
    cleaned = re.sub(r"[<>:\"/\\|?*]+", "_", cleaned)
    cleaned = cleaned.strip(" .")
    return cleaned or fallback


def _save_control_text(pdf_path: Path, category: str, source: str, text: str) -> Path:
    CONTROL_TEXT_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{category}_{_safe_name(pdf_path.stem)}_{source}.txt"
    destination = CONTROL_TEXT_DIR / filename
    destination.write_text(text, encoding="utf-8")
    return destination


def extract_text_from_result_pdf_with_fallback(pdf_path: Path) -> str:
    try:
        text = extract_text_from_pdf(pdf_path)
        _save_control_text(pdf_path, category="result", source="native", text=text)
        return text
    except NonTextPdfError:
        TEMP_RESULT_TEXT_DIR.mkdir(parents=True, exist_ok=True)
        temp_txt = TEMP_RESULT_TEXT_DIR / f"ocr_{_safe_name(pdf_path.stem)}.txt"
        tessdata = detect_tessdata_path()

        extract_pdf_text(
            pdf_path,
            temp_txt,
            ocr_language="tur+eng",
            tessdata=tessdata,
            needs_ocr=True,
            ocr_backend="easyocr",
            ocr_dpi=300,
            ocr_scale=3.0,
        )

        ocr_text = temp_txt.read_text(encoding="utf-8").strip()
        if not ocr_text:
            raise NonTextPdfError(f"OCR fallback produced no text: {pdf_path}")

        _save_control_text(pdf_path, category="result", source="ocr", text=ocr_text)

        return ocr_text


def parse_startlist_rows(startlist_pdfs: list[Path], event_unique_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    counter = 1

    for pdf_path in sorted(startlist_pdfs):
        list_no = extract_list_number(pdf_path)
        if not list_no:
            continue

        text = extract_text_from_pdf(pdf_path)
        _save_control_text(pdf_path, category="startlist", source="native", text=text)
        data = parse_events_only(text)

        for entry in data.get("entries", []):
            swimmer_name = str(entry.get("name") or "").strip()
            club_value = str(entry.get("club") or "").strip()
            row = {
                "event_unique_name": event_unique_name,
                "startlist_unique_name": f"{event_unique_name}-{counter:06d}",
                "swimmer_name": swimmer_name,
                "club": club_value,
                "result": "",
                "rank": 0,
                "_list_no": list_no,
                "_name_norm": normalize_match_text(swimmer_name),
                "_club_norm": normalize_match_text(club_value),
            }
            rows.append(row)
            counter += 1

    return [row for row in rows if row.get("swimmer_name")]


def parse_result_records(result_pdf: Path) -> list[dict[str, Any]]:
    text = extract_text_from_result_pdf_with_fallback(result_pdf)
    version = detect_result_version(text)
    parser_version, parser = load_result_parser_with_fallback(version)
    if parser_version != version:
        print(f"{result_pdf.name}: parsed with fallback parser {parser_version}")
    payload = parser(text)

    parsed_records: list[dict[str, Any]] = []
    for record in [*payload.get("records", []), *payload.get("special_records", [])]:
        swimmer_name = _normalize_result_swimmer_name(str(record.get("swimmer_name") or "").strip())
        result_value = str(record.get("time") or "").strip()
        if not swimmer_name or not result_value:
            continue

        club_value = str(record.get("club") or "").strip()
        if _looks_like_club_text(swimmer_name) or _looks_like_fragment_name(swimmer_name, club_value):
            # OCR can occasionally shift club fragments into swimmer_name.
            continue

        rank_value = record.get("rank")
        try:
            rank_int = int(str(rank_value).strip()) if rank_value is not None and str(rank_value).strip() else 0
        except ValueError:
            rank_int = 0
        if rank_int > 120:
            # In noisy OCR, points can leak into rank field (e.g. 364, 311).
            rank_int = 0

        parsed_records.append(
            {
                "swimmer_name": swimmer_name,
                "club": club_value,
                "result": result_value,
                "rank": rank_int,
                "_name_norm": normalize_match_text(swimmer_name),
                "_club_norm": normalize_match_text(club_value),
            }
        )

    return _drop_single_token_ocr_duplicates(parsed_records)


def build_startlist_index(rows: list[dict[str, Any]]) -> dict[str, dict[tuple[str, str], list[int]]]:
    index: dict[str, dict[tuple[str, str], list[int]]] = {}

    for row_index, row in enumerate(rows):
        list_no = str(row.get("_list_no") or "")
        if not list_no:
            continue

        list_map = index.setdefault(list_no, {})
        key_exact = (str(row.get("_name_norm") or ""), str(row.get("_club_norm") or ""))
        key_name_only = (str(row.get("_name_norm") or ""), "")

        list_map.setdefault(key_exact, []).append(row_index)
        if key_name_only != key_exact:
            list_map.setdefault(key_name_only, []).append(row_index)

    return index


def build_startlist_rows_by_list(rows: list[dict[str, Any]]) -> dict[str, list[int]]:
    rows_by_list: dict[str, list[int]] = {}
    for row_index, row in enumerate(rows):
        list_no = str(row.get("_list_no") or "")
        if not list_no:
            continue
        rows_by_list.setdefault(list_no, []).append(row_index)
    return rows_by_list


def find_matching_row_index(
    result_record: dict[str, Any],
    list_index: dict[tuple[str, str], list[int]],
    used_rows: set[int],
    list_row_indexes: list[int] | None = None,
    startlist_rows: list[dict[str, Any]] | None = None,
) -> int | None:
    candidates = [
        (str(result_record.get("_name_norm") or ""), str(result_record.get("_club_norm") or "")),
        (str(result_record.get("_name_norm") or ""), ""),
    ]

    for key in candidates:
        for row_index in list_index.get(key, []):
            if row_index in used_rows:
                continue
            return row_index

    if not list_row_indexes or not startlist_rows:
        return None

    result_name = str(result_record.get("_name_norm") or "")
    result_club = str(result_record.get("_club_norm") or "")
    if not result_name:
        return None

    best_index: int | None = None
    best_name_ratio = 0.0
    best_club_ratio = 0.0
    best_score = 0.0

    for row_index in list_row_indexes:
        if row_index in used_rows:
            continue

        row = startlist_rows[row_index]
        row_name = str(row.get("_name_norm") or "")
        if not row_name:
            continue

        name_ratio = _similarity(result_name, row_name)
        if name_ratio < 0.78:
            continue

        row_club = str(row.get("_club_norm") or "")
        club_ratio = _similarity(result_club, row_club) if result_club and row_club else 0.0
        score = name_ratio + (0.15 * club_ratio)

        if score > best_score:
            best_index = row_index
            best_score = score
            best_name_ratio = name_ratio
            best_club_ratio = club_ratio

    if best_index is not None:
        if best_name_ratio >= 0.93:
            return best_index
        if best_name_ratio >= 0.86 and (best_club_ratio >= 0.45 or not result_club):
            return best_index
        if best_name_ratio >= 0.80 and best_club_ratio >= 0.80:
            return best_index

    # Micro fallback 1: unique surname match inside the same result list.
    result_tokens = _name_tokens(str(result_record.get("swimmer_name") or ""))
    if result_tokens:
        result_last = result_tokens[-1]
        if len(result_last) >= 4:
            surname_candidates: list[int] = []
            for row_index in list_row_indexes:
                if row_index in used_rows:
                    continue
                row_tokens = _name_tokens(str(startlist_rows[row_index].get("swimmer_name") or ""))
                if row_tokens and row_tokens[-1] == result_last:
                    surname_candidates.append(row_index)
            if len(surname_candidates) == 1:
                return surname_candidates[0]

    # Micro fallback 2: single-token OCR names like "DEMIRALAY" or "SAFRAN".
    if len(result_tokens) == 1 and len(result_tokens[0]) >= 5:
        token = result_tokens[0]
        token_candidates: list[int] = []
        for row_index in list_row_indexes:
            if row_index in used_rows:
                continue
            row_tokens = _name_tokens(str(startlist_rows[row_index].get("swimmer_name") or ""))
            if token in row_tokens:
                token_candidates.append(row_index)

        if len(token_candidates) == 1:
            return token_candidates[0]

        if len(token_candidates) > 1 and result_club:
            best_token_row: int | None = None
            best_token_club = 0.0
            for row_index in token_candidates:
                row_club = str(startlist_rows[row_index].get("_club_norm") or "")
                club_ratio = _similarity(result_club, row_club) if row_club else 0.0
                if club_ratio > best_token_club:
                    best_token_club = club_ratio
                    best_token_row = row_index
            if best_token_row is not None and best_token_club >= 0.60:
                return best_token_row

    return None


def _is_likely_consumed_duplicate(
    result_record: dict[str, Any],
    list_row_indexes: list[int],
    startlist_rows: list[dict[str, Any]],
    used_rows: set[int],
) -> bool:
    result_tokens = _name_tokens(str(result_record.get("swimmer_name") or ""))
    if len(result_tokens) != 1:
        return False

    token = result_tokens[0]
    if len(token) < 5:
        return False

    result_club = str(result_record.get("_club_norm") or "")
    if not result_club:
        return False

    for row_index in list_row_indexes:
        if row_index not in used_rows:
            continue

        row = startlist_rows[row_index]
        row_tokens = _name_tokens(str(row.get("swimmer_name") or ""))
        if token not in row_tokens:
            continue

        row_club = str(row.get("_club_norm") or "")
        club_ratio = _similarity(result_club, row_club) if row_club else 0.0
        if club_ratio >= 0.75:
            return True

    return False


def match_results_to_startlist_rows(
    startlist_rows: list[dict[str, Any]],
    result_pdfs: list[Path],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    index_by_list_no = build_startlist_index(startlist_rows)
    rows_by_list_no = build_startlist_rows_by_list(startlist_rows)
    used_rows: set[int] = set()
    matched_rows: dict[int, dict[str, Any]] = {}

    stats = {
        "result_files_checked": len(result_pdfs),
        "result_records_seen": 0,
        "result_records_matched": 0,
        "result_records_unmatched": 0,
        "result_files_without_startlist": 0,
        "result_files_skipped_parse_error": 0,
    }

    for result_pdf in sorted(result_pdfs):
        list_no = extract_list_number(result_pdf)
        if not list_no:
            continue

        list_index = index_by_list_no.get(list_no)
        if not list_index:
            stats["result_files_without_startlist"] += 1
            continue
        list_row_indexes = rows_by_list_no.get(list_no, [])

        try:
            parsed_records = parse_result_records(result_pdf)
        except Exception as error:
            stats["result_files_skipped_parse_error"] += 1
            print(f"Skipped result file {result_pdf.name}: {error}")
            continue

        for record in parsed_records:
            stats["result_records_seen"] += 1
            row_index = find_matching_row_index(
                record,
                list_index,
                used_rows,
                list_row_indexes=list_row_indexes,
                startlist_rows=startlist_rows,
            )
            if row_index is None:
                if _is_likely_consumed_duplicate(record, list_row_indexes, startlist_rows, used_rows):
                    continue
                stats["result_records_unmatched"] += 1
                continue

            used_rows.add(row_index)
            stats["result_records_matched"] += 1

            row = dict(startlist_rows[row_index])
            row["result"] = record["result"]
            row["rank"] = record["rank"]
            matched_rows[row_index] = row

    ordered_rows = [matched_rows[row_index] for row_index in sorted(matched_rows.keys())]
    return ordered_rows, stats


def post_json(endpoint: str, rows: list[dict[str, Any]], token: str | None) -> dict[str, Any]:
    payload = json.dumps({"rows": rows}, ensure_ascii=False).encode("utf-8")
    request = Request(endpoint, data=payload, method="POST")
    request.add_header("Content-Type", "application/json; charset=utf-8")
    if token:
        request.add_header("X-Ingest-Token", token)

    with urlopen(request, timeout=120) as response:
        response_text = response.read().decode("utf-8", errors="replace")

    parsed = json.loads(response_text)
    if not isinstance(parsed, dict):
        raise ValueError(f"Unexpected API response from {endpoint}: {response_text}")
    return parsed


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"processed_result_files": []}

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"processed_result_files": []}

    if not isinstance(payload, dict):
        return {"processed_result_files": []}

    files = payload.get("processed_result_files")
    if not isinstance(files, list):
        payload["processed_result_files"] = []

    return payload


def save_state(path: Path, processed_files: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"processed_result_files": sorted(set(processed_files))}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_pipeline(event_url: str) -> None:
    event_details = scrape_event_detail(event_url)
    event_unique_name = str(event_details.get("unique_name") or "").strip()
    if not event_unique_name:
        raise RuntimeError("Event unique_name could not be detected from URL")

    start_downloads = start_scraper(event_url, output_root=SCRAPED_ROOT)
    result_downloads = result_scraper(event_url, output_root=SCRAPED_ROOT)

    startlist_pdfs = [Path(item["saved_to"]) for item in start_downloads if str(item.get("saved_to") or "").endswith(".pdf")]
    result_pdfs = [Path(item["saved_to"]) for item in result_downloads if str(item.get("saved_to") or "").endswith(".pdf")]

    if not startlist_pdfs:
        raise RuntimeError("No startlist PDF found for matching")
    if not result_pdfs:
        print("No result PDF found on event page")
        return

    state_path = STATE_DIR / f"result_update_state_{event_unique_name}.json"
    state = load_state(state_path)
    processed_names = {str(name) for name in state.get("processed_result_files", [])}

    new_result_pdfs: list[Path] = []
    for pdf_path in sorted(result_pdfs):
        if pdf_path.name in processed_names:
            continue
        new_result_pdfs.append(pdf_path)

    if not new_result_pdfs:
        print("No new result files detected on event link")
        return

    startlist_rows = parse_startlist_rows(startlist_pdfs, event_unique_name=event_unique_name)
    if not startlist_rows:
        raise RuntimeError("Startlist parsing produced no rows")

    matched_rows, match_stats = match_results_to_startlist_rows(startlist_rows, new_result_pdfs)
    if not matched_rows:
        print("No matched result rows for update")
        return

    for row in matched_rows:
        row.pop("_list_no", None)
        row.pop("_name_norm", None)
        row.pop("_club_norm", None)

    targets = build_target_base_urls()
    successes: list[tuple[str, dict[str, Any]]] = []
    failures: list[tuple[str, str]] = []

    for base_url in targets:
        endpoint = f"{base_url}/api/ingest-results/"
        try:
            response = post_json(endpoint, matched_rows, token=INGEST_TOKEN)
            successes.append((base_url, response))
        except Exception as error:
            failures.append((base_url, str(error)))

    print(f"Event URL: {event_url}")
    print(f"Event unique_name: {event_unique_name}")
    print(f"Startlist PDFs: {len(startlist_pdfs)}")
    print(f"Result PDFs found: {len(result_pdfs)}")
    print(f"New result PDFs: {len(new_result_pdfs)}")
    print(f"Matched rows to update: {len(matched_rows)}")
    print(f"Match stats: {json.dumps(match_stats, ensure_ascii=False)}")

    for base_url, response in successes:
        print(f"[{base_url}] Update response: {json.dumps(response, ensure_ascii=False)}")

    for base_url, message in failures:
        print(f"[{base_url}] Update failed: {message}")

    if not successes:
        joined = " | ".join(f"{base}: {msg}" for base, msg in failures) or "no target available"
        raise RuntimeError(f"Result update failed for all targets: {joined}")

    processed_names.update(pdf.name for pdf in new_result_pdfs)
    save_state(state_path, sorted(processed_names))


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean result list parser and updater (single input: event URL)")
    parser.add_argument("event_url", type=str, help="Event page URL")
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
