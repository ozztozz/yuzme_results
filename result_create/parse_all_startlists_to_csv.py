from __future__ import annotations

import argparse
import csv
import importlib
import io
import re
import sys
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from extract_result import extract_pdf_text
from parse_splash_result import load_version_parser, parse_file as parse_result_text_file
from parse_startlist_to_csv import detect_splash_version, parse_events_only
from read_ocr import detect_pdf_needs_ocr, detect_tessdata_path


INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*]+')
EVENT_NAME_WITH_PREFIX_RE = re.compile(r"^Yarış\s+\d+\s*,\s*(.+)$", re.IGNORECASE)
DISTANCE_STYLE_RE = re.compile(r"^(\d+(?:\s*x\s*\d+)?m)\s+(.+)$", re.IGNORECASE)
LIST_NUMBER_RE = re.compile(r"(?i)(?:start|result)list_(\d+)")
SPLASH_SUFFIX_RE = re.compile(r"\s*-\s*SPLASH\s+Meet\s+Manager\s*\d*", re.IGNORECASE)
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
GENDER_ALIASES = {
    "erkek": "Erkekler",
    "erkekler": "Erkekler",
    "kız": "Kadınlar",
    "kiz": "Kadınlar",
    "kızlar": "Kadınlar",
    "kizlar": "Kadınlar",
    "bayan": "Kadınlar",
    "bayanlar": "Kadınlar",
    "kadın": "Kadınlar",
    "kadin": "Kadınlar",
    "kadınlar": "Kadınlar",
    "kadinlar": "Kadınlar",
}

ENTRY_FIELDS = [
    "event_no",
    "seri_no",
    "seri_total",
    "lane",
    "name",
    "year_of_birth",
    "club",
    "seed",
]

OUTPUT_FIELDS = [
    "event_title_folder",
    "source_pdf",
    "result_pdf",
    "detected_version",
    "event_location",
    "event_date",
    "event_name",
    "gender",
    "distance",
    "swimming_style",
    *ENTRY_FIELDS,
    "result",
    "rank",
]


@dataclass
class ResultScoreCandidate:
    name_norm: str
    club_norm: str
    result_value: str
    rank_value: str | None = None
    used: bool = False


def safe_name(value: str, fallback: str = "untitled") -> str:
    value = re.sub(r"\s+", " ", value).strip()
    value = INVALID_FILENAME_CHARS_RE.sub("_", value)
    value = value.strip(" .")
    return value or fallback


def infer_event_title_from_path(pdf_path: Path) -> str:
    # New layout: <base>/<event_title>/startlists/<file>.pdf
    if pdf_path.parent.name.lower() == "startlists" and pdf_path.parent.parent.name:
        return SPLASH_SUFFIX_RE.sub("", pdf_path.parent.parent.name).strip()

    # Legacy layout: <base>/startlists/<event_title>/<file>.pdf
    if pdf_path.parent.parent.name.lower() == "startlists" and pdf_path.parent.name:
        return SPLASH_SUFFIX_RE.sub("", pdf_path.parent.name).strip()

    return SPLASH_SUFFIX_RE.sub("", pdf_path.parent.name or "unknown_event").strip()


def extract_list_number(file_path: Path) -> str | None:
    match = LIST_NUMBER_RE.search(file_path.stem)
    if not match:
        return None
    return match.group(1)


def corresponding_result_pdf(startlist_pdf: Path) -> Path | None:
    list_no = extract_list_number(startlist_pdf)
    if not list_no:
        return None

    candidate_names = [f"ResultList_{list_no}.pdf", f"resultlist_{list_no}.pdf"]
    candidates: list[Path] = []

    # New layout: <base>/<event_title>/startlists/StartList_N.pdf -> <base>/<event_title>/results/ResultList_N.pdf
    if startlist_pdf.parent.name.lower() == "startlists":
        event_dir = startlist_pdf.parent.parent
        for name in candidate_names:
            candidates.append(event_dir / "results" / name)

    # Legacy layout: <base>/startlists/<event_title>/StartList_N.pdf -> <base>/results/<event_title>/ResultList_N.pdf
    if startlist_pdf.parent.parent.name.lower() == "startlists":
        event_dir = startlist_pdf.parent
        base_dir = startlist_pdf.parent.parent.parent
        for name in candidate_names:
            candidates.append(base_dir / "results" / event_dir.name / name)

    for path in candidates:
        if path.exists():
            return path

    return None


def discover_startlist_pdfs(root_dir: Path, event_title: str | None = None) -> list[Path]:
    candidates: list[Path] = []

    if event_title:
        title_dir = root_dir / event_title / "startlists"
        if title_dir.exists():
            candidates.extend(sorted(title_dir.glob("*.pdf")))
        return candidates

    # Preferred layout.
    candidates.extend(sorted(root_dir.glob("*/startlists/*.pdf")))

    # Legacy layout support.
    candidates.extend(sorted(root_dir.glob("startlists/*/*.pdf")))

    # Deduplicate while preserving order.
    seen: set[Path] = set()
    deduped: list[Path] = []
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(path)

    return deduped


def normalize_gender(value: str | None) -> str | None:
    if not value:
        return None
    lowered = value.strip().lower()
    return GENDER_ALIASES.get(lowered, value)


def parse_distance_style(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None

    candidate = value.strip().strip(",")
    match = DISTANCE_STYLE_RE.match(candidate)
    if not match:
        return None, None

    return match.group(1).strip(), match.group(2).strip(" ,")


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
        if possible_gender in {"Erkekler", "Kadınlar"}:
            gender = possible_gender
            descriptor_tail = " ".join(parts[1:]).strip() if len(parts) > 1 else ""

    distance, swimming_style = parse_distance_style(descriptor_tail)

    if distance is None and len(parts) >= 2:
        # Fallback for cases where distance/style are split with commas.
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


def normalize_match_text(value: Any) -> str:
    if value is None:
        return ""

    text = str(value).strip().translate(TURKISH_ASCII_MAP).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]", "", text)
    return text


def make_match_keys(name: Any, club: Any) -> list[tuple[str, str]]:
    normalized_name = normalize_match_text(name)
    normalized_club = normalize_match_text(club)

    return [
        (normalized_name, normalized_club),
        (normalized_name, ""),
    ]


def parse_result_pdf_scores(
    result_pdf_path: Path,
    temp_text_dir: Path,
    preferred_version: str | None,
    ocr_language: str,
    tessdata: str | None,
    ocr_backend: str,
    ocr_dpi: int,
    ocr_scale: float,
) -> tuple[dict[tuple[str, str], str], list[ResultScoreCandidate]]:
    temp_name = f"result_{safe_name(result_pdf_path.parent.parent.name)}_{result_pdf_path.stem}.txt"
    temp_text_path = temp_text_dir / temp_name

    needs_ocr = detect_pdf_needs_ocr(result_pdf_path)
    extract_pdf_text(
        result_pdf_path,
        temp_text_path,
        ocr_language=ocr_language,
        tessdata=tessdata,
        needs_ocr=needs_ocr,
        ocr_backend=ocr_backend,
        ocr_dpi=ocr_dpi,
        ocr_scale=ocr_scale,
    )

    payload: dict[str, Any]

    preferred_error: Exception | None = None
    if preferred_version:
        try:
            text = temp_text_path.read_text(encoding="utf-8")
            parser_func = load_version_parser(preferred_version, Path(__file__).parent / "parsers")
            payload = parser_func(text)
        except Exception as error:
            preferred_error = error
            payload = {}
    else:
        payload = {}

    if not payload:
        try:
            parsed = parse_result_text_file(temp_text_path)
            payload = parsed.get("data", {})
        except Exception as error:
            if preferred_error is not None:
                raise preferred_error from error
            raise

    records = payload.get("records", [])
    special_records = payload.get("special_records", [])

    score_map: dict[tuple[str, str], str] = {}
    rank_map: dict[tuple[str, str], str] = {}
    candidates: list[ResultScoreCandidate] = []
    dedupe_candidates: set[tuple[str, str, str]] = set()

    for record in [*records, *special_records]:
        result_value = record.get("time")
        if result_value is None:
            continue

        swimmer_name = record.get("swimmer_name")
        club = record.get("club")
        rank_raw = record.get("rank")
        rank_text = str(rank_raw) if rank_raw is not None else None

        name_norm = normalize_match_text(swimmer_name)
        club_norm = normalize_match_text(club)
        result_text = str(result_value)

        if not name_norm:
            continue

        for key in make_match_keys(swimmer_name, club):
            if key not in score_map:
                score_map[key] = result_text
            if rank_text is not None and key not in rank_map:
                rank_map[key] = rank_text

        candidate_key = (name_norm, club_norm, result_text)
        if candidate_key in dedupe_candidates:
            continue

        dedupe_candidates.add(candidate_key)
        candidates.append(
            ResultScoreCandidate(name_norm=name_norm, club_norm=club_norm, result_value=result_text, rank_value=rank_text)
        )

    return score_map, rank_map, candidates


def _gemini_rows_from_csv_text(csv_text: str) -> list[dict[str, Any]]:
    text = (csv_text or "").strip()
    if not text:
        return []

    if text.startswith("```"):
        text = re.sub(r"^```(?:csv)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text).strip()

    reader = csv.reader(io.StringIO(text), delimiter=";")
    rows = [row for row in reader if row]
    if not rows:
        return []

    header = [item.strip().lower() for item in rows[0]]

    def find_index(candidates: list[str]) -> int | None:
        for candidate in candidates:
            if candidate in header:
                return header.index(candidate)
        return None

    name_index = find_index(["name", "swimmer name"])
    club_index = find_index(["club", "club/team", "team"])
    time_index = find_index(["time"])

    data_rows = rows[1:]
    if name_index is None or time_index is None:
        # Fallback to known format: Position;Name;BirthYear;Club;Time;Points
        name_index, club_index, time_index = 1, 3, 4
        data_rows = rows

    output: list[dict[str, Any]] = []
    for row in data_rows:
        if name_index >= len(row) or time_index >= len(row):
            continue
        output.append(
            {
                "name": row[name_index].strip() if name_index < len(row) else "",
                "club": row[club_index].strip() if club_index is not None and club_index < len(row) else "",
                "time": row[time_index].strip() if time_index < len(row) else "",
            }
        )

    return output


def parse_result_pdf_scores_gemini(result_pdf_path: Path) -> tuple[dict[tuple[str, str], str], list[ResultScoreCandidate]]:
    try:
        gemini_module = importlib.import_module("gemini_pdf_parser")
    except Exception as error:
        raise RuntimeError(f"Failed to import gemini_pdf_parser: {error}") from error

    parse_func = getattr(gemini_module, "parse_pdf_with_gemini", None)
    if parse_func is None or not callable(parse_func):
        raise RuntimeError("gemini_pdf_parser.parse_pdf_with_gemini is not available")

    response = parse_func(result_pdf_path, output_format="json")
    format_value = str(response.get("format") or "").lower()

    rows: list[dict[str, Any]] = []
    if format_value == "json" and isinstance(response.get("data"), list):
        rows = [item for item in response.get("data", []) if isinstance(item, dict)]
    elif format_value == "csv" and isinstance(response.get("data"), str):
        rows = _gemini_rows_from_csv_text(response["data"])
    else:
        # Fallback attempt: request CSV and parse it.
        fallback_response = parse_func(result_pdf_path, output_format="csv")
        if str(fallback_response.get("format") or "").lower() == "csv" and isinstance(
            fallback_response.get("data"), str
        ):
            rows = _gemini_rows_from_csv_text(fallback_response["data"])

    score_map: dict[tuple[str, str], str] = {}
    candidates: list[ResultScoreCandidate] = []
    dedupe_candidates: set[tuple[str, str, str]] = set()

    for row in rows:
        result_value = row.get("time")
        if result_value is None:
            continue

        swimmer_name = row.get("name")
        club = row.get("club")

        name_norm = normalize_match_text(swimmer_name)
        club_norm = normalize_match_text(club)
        result_text = str(result_value).strip()

        if not name_norm or not result_text:
            continue

        for key in make_match_keys(swimmer_name, club):
            if key not in score_map:
                score_map[key] = result_text

        candidate_key = (name_norm, club_norm, result_text)
        if candidate_key in dedupe_candidates:
            continue

        dedupe_candidates.add(candidate_key)
        candidates.append(ResultScoreCandidate(name_norm=name_norm, club_norm=club_norm, result_value=result_text))

    return score_map, {}, candidates


def _fuzzy_best_candidate(
    candidates: list[ResultScoreCandidate],
    row_name_norm: str,
    row_club_norm: str,
) -> ResultScoreCandidate | None:
    best_candidate: ResultScoreCandidate | None = None
    best_rank: float = 0.0

    for candidate in candidates:
        if candidate.used:
            continue

        name_ratio = SequenceMatcher(None, row_name_norm, candidate.name_norm).ratio()
        if name_ratio < 0.84:
            continue

        club_ratio = 0.0
        if row_club_norm and candidate.club_norm:
            club_ratio = SequenceMatcher(None, row_club_norm, candidate.club_norm).ratio()

        rank = name_ratio
        if row_club_norm and candidate.club_norm:
            rank += club_ratio * 0.12

        min_rank = 0.90
        if row_club_norm:
            min_rank = 0.86

        if rank < min_rank:
            continue

        if rank > best_rank:
            best_rank = rank
            best_candidate = candidate

    return best_candidate


def apply_scores_to_rows(
    rows: list[dict[str, Any]],
    score_data: tuple[dict[tuple[str, str], str], dict[tuple[str, str], str], list[ResultScoreCandidate]],
) -> tuple[int, int]:
    score_map, rank_map, candidates = score_data
    matched_count = 0
    fuzzy_matched_count = 0

    for row in rows:
        row["result"] = None
        row["rank"] = None

        name = row.get("name")
        if not normalize_match_text(name):
            continue

        for key in make_match_keys(row.get("name"), row.get("club")):
            score = score_map.get(key)
            if score is not None:
                row["result"] = score
                row["rank"] = rank_map.get(key)
                matched_count += 1
                break

        if row.get("result") is not None:
            continue

        row_name_norm = normalize_match_text(row.get("name"))
        row_club_norm = normalize_match_text(row.get("club"))

        candidate = _fuzzy_best_candidate(candidates, row_name_norm, row_club_norm)
        if candidate is not None:
            candidate.used = True
            row["result"] = candidate.result_value
            row["rank"] = candidate.rank_value
            matched_count += 1
            fuzzy_matched_count += 1

    return matched_count, fuzzy_matched_count


def parse_startlist_pdf(
    pdf_path: Path,
    temp_text_dir: Path,
    ocr_language: str,
    tessdata: str | None,
    ocr_backend: str,
    ocr_dpi: int,
    ocr_scale: float,
) -> tuple[str, list[dict[str, Any]]]:
    event_title_folder = infer_event_title_from_path(pdf_path)
    temp_name = f"{safe_name(event_title_folder)}_{pdf_path.stem}.txt"
    temp_text_path = temp_text_dir / temp_name

    needs_ocr = detect_pdf_needs_ocr(pdf_path)
    extract_pdf_text(
        pdf_path,
        temp_text_path,
        ocr_language=ocr_language,
        tessdata=tessdata,
        needs_ocr=needs_ocr,
        ocr_backend=ocr_backend,
        ocr_dpi=ocr_dpi,
        ocr_scale=ocr_scale,
    )

    text = temp_text_path.read_text(encoding="utf-8")
    version = detect_splash_version(text)
    data = parse_events_only(text)
    metadata = data.get("metadata", {}) if isinstance(data, dict) else {}
    event_info_map = build_event_info_map(data)

    rows: list[dict[str, Any]] = []
    for entry in data.get("entries", []):
        event_no = str(entry.get("event_no") or "").strip()
        event_info = event_info_map.get(
            event_no,
            {"event_name": None, "gender": None, "distance": None, "swimming_style": None},
        )
        row: dict[str, Any] = {
            "event_title_folder": event_title_folder,
            "source_pdf": str(pdf_path),
            "result_pdf": None,
            "detected_version": version,
            "event_location": metadata.get("event_location"),
            "event_date": metadata.get("event_date"),
            "event_name": event_info.get("event_name"),
            "gender": event_info.get("gender"),
            "distance": event_info.get("distance"),
            "swimming_style": event_info.get("swimming_style"),
        }
        for field in ENTRY_FIELDS:
            row[field] = entry.get(field)
        rows.append(row)

    return version, rows


def write_combined_csv(rows: list[dict[str, Any]], output_csv: Path) -> int:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in OUTPUT_FIELDS})
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse all startlist PDFs and write one combined CSV using parse_startlist_to_csv logic"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("scraped"),
        help="Root scraped directory",
    )
    parser.add_argument(
        "--event-title",
        type=str,
        default=None,
        help="Optional event title folder to process only one event",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("results/startlists_all_entries.csv"),
        help="Combined CSV output path",
    )
    parser.add_argument(
        "--temp-text-dir",
        type=Path,
        default=Path("results/_tmp_startlist_text"),
        help="Temporary directory for extracted text files",
    )
    parser.add_argument(
        "--temp-result-text-dir",
        type=Path,
        default=Path("results/_tmp_result_text"),
        help="Temporary directory for extracted result text files",
    )
    parser.add_argument(
        "--ocr-language",
        type=str,
        default="tur+eng",
        help="OCR language used when native PDF text is missing",
    )
    parser.add_argument(
        "--ocr-backend",
        type=str,
        default="easyocr",
        choices=["fitz", "easyocr", "paddleocr"],
        help="OCR backend: easyocr (default), fitz, or paddleocr",
    )
    parser.add_argument(
        "--result-parser",
        type=str,
        default="splash",
        choices=["splash", "gemini"],
        help="Result parsing backend: splash parser from OCR text, or gemini_pdf_parser",
    )
    parser.add_argument(
        "--ocr-dpi",
        type=int,
        default=300,
        help="DPI for fitz OCR (300 default, 600 for complex fonts)",
    )
    parser.add_argument(
        "--ocr-scale",
        type=float,
        default=3.0,
        help="Scale factor for easyocr/paddleocr (3.0 default = ~900 DPI)",
    )
    parser.add_argument(
        "--tessdata",
        type=str,
        default=None,
        help="Optional explicit tessdata directory",
    )
    args = parser.parse_args()

    if not args.input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {args.input_dir}")

    args.temp_text_dir.mkdir(parents=True, exist_ok=True)
    args.temp_result_text_dir.mkdir(parents=True, exist_ok=True)

    pdf_files = discover_startlist_pdfs(args.input_dir, args.event_title)
    if not pdf_files:
        print("No startlist PDFs found.", file=sys.stderr)
        raise SystemExit(1)

    tessdata = args.tessdata or detect_tessdata_path()
    all_rows: list[dict[str, Any]] = []
    failures: list[tuple[Path, str]] = []
    score_failures: list[tuple[Path, str]] = []
    score_cache: dict[
        Path,
        tuple[dict[tuple[str, str], str], dict[tuple[str, str], str], list[ResultScoreCandidate]],
    ] = {}
    with_result_total = 0
    without_result_total = 0
    matched_scores_total = 0
    fuzzy_scores_total = 0

    for pdf_file in pdf_files:
        try:
            startlist_version, rows = parse_startlist_pdf(
                pdf_file,
                temp_text_dir=args.temp_text_dir,
                ocr_language=args.ocr_language,
                tessdata=tessdata,
                ocr_backend=args.ocr_backend,
                ocr_dpi=args.ocr_dpi,
                ocr_scale=args.ocr_scale,
            )

            result_pdf = corresponding_result_pdf(pdf_file)
            if result_pdf is not None:
                with_result_total += 1
                for row in rows:
                    row["result_pdf"] = str(result_pdf)

                if result_pdf not in score_cache:
                    try:
                        if args.result_parser == "gemini":
                            score_cache[result_pdf] = parse_result_pdf_scores_gemini(result_pdf)
                        else:
                            score_cache[result_pdf] = parse_result_pdf_scores(
                                result_pdf,
                                temp_text_dir=args.temp_result_text_dir,
                                preferred_version=startlist_version,
                                ocr_language=args.ocr_language,
                                tessdata=tessdata,
                                ocr_backend=args.ocr_backend,
                                ocr_dpi=args.ocr_dpi,
                                ocr_scale=args.ocr_scale,
                            )
                    except Exception as error:
                        score_cache[result_pdf] = ({}, [])
                        score_failures.append((result_pdf, str(error)))
                        print(f"Failed score parse {result_pdf.name}: {error}", file=sys.stderr)

                matched, fuzzy_matched = apply_scores_to_rows(rows, score_cache.get(result_pdf, ({}, {}, [])))
                matched_scores_total += matched
                fuzzy_scores_total += fuzzy_matched
                print(
                    f"Matched results {pdf_file.name} -> {result_pdf.name}: "
                    f"{matched}/{len(rows)} (fuzzy: {fuzzy_matched})"
                )
            else:
                without_result_total += 1
                print(f"No corresponding result file for {pdf_file.name}")

            all_rows.extend(rows)
            print(f"Parsed {pdf_file.name}: {len(rows)} rows")
        except Exception as error:
            failures.append((pdf_file, str(error)))
            print(f"Failed {pdf_file.name}: {error}", file=sys.stderr)

    row_count = write_combined_csv(all_rows, args.output_csv)

    print(f"Startlist PDFs found: {len(pdf_files)}")
    print(f"Startlists with corresponding results: {with_result_total}")
    print(f"Startlists without corresponding results: {without_result_total}")
    print(f"Rows with matched results: {matched_scores_total}")
    print(f"Rows with fuzzy matched results: {fuzzy_scores_total}")
    print(f"Rows written: {row_count}")
    print(f"Combined CSV: {args.output_csv}")

    if failures:
        print(f"Files failed: {len(failures)}", file=sys.stderr)
        for path, message in failures:
            print(f"- {path}: {message}", file=sys.stderr)

    if score_failures:
        print(f"Result score parse failures: {len(score_failures)}", file=sys.stderr)
        for path, message in score_failures:
            print(f"- {path}: {message}", file=sys.stderr)


if __name__ == "__main__":
    main()
