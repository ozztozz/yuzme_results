from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from parse_splash_result import detect_splash_version, load_version_parser


LIST_NUMBER_RE = re.compile(r"(?i)resultlist_(\d+)")
INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*]+')
WHITESPACE_RE = re.compile(r"\s+")
GLUED_TIME_PREFIX_RE = re.compile(r"(?<=[A-Za-zÇĞİÖŞÜçğıöşü])(\d{1,2}[:.]\d{2}(?:[:.]\d{2})?)(?!\d)")
GLUED_TIME_SUFFIX_RE = re.compile(r"(\d{1,2}[:.]\d{2}(?:[:.]\d{2})?)(?=[A-Za-zÇĞİÖŞÜçğıöşü])")
ALPHA_DIGIT_GLUE_RE = re.compile(r"(?<=[A-Za-zÇĞİÖŞÜçğıöşü])(?=\d{2,4}(?:\b|$))")
HTML_TAG_RE = re.compile(r"<[^>]+>")
RACE_TIME_RE = re.compile(r"(?<!\d)(\d{1,2}[:.]\d{2}(?:[:.]\d{2})?)(?![\d.])")
CLUB_HINT_RE = re.compile(
    r"(?i)(\((?:Tk|Fd|Td)\))|\b(okul|okulu|ortaokul|ortaokulu|kolej|koleji|vakfı|lisesi|ilkokul)\b"
)
MARKER_TAG_RE = re.compile(r"\((?:Tk|Fd|Td)\)", re.IGNORECASE)
PURE_NUMERIC_NAME_RE = re.compile(r"^\d{1,4}(?:[.:]\d{1,2})?$")
NAME_YEAR_IN_TEXT_RE = re.compile(r"\b[A-Za-zÇĞİÖŞÜçğıöşüİIı']+(?:\s+[A-Za-zÇĞİÖŞÜçğıöşüİIı']+){1,3}\s+\d{2}\b")
TRAILING_POINTS_RE = re.compile(r"\b(\d{1,4})\s*$")

OUTPUT_FIELDS = [
    "event_title_folder",
    "result_pdf",
    "result_text_file",
    "detected_version",
    "event_no",
    "event_name",
    "swimmer_name",
    "birth_year",
    "club",
    "time",
    "rank",
    "points",
    "status",
    "special_type",
    "rule",
    "note_time",
]


def safe_name(value: str, fallback: str = "untitled") -> str:
    value = WHITESPACE_RE.sub(" ", value).strip()
    value = INVALID_FILENAME_CHARS_RE.sub("_", value)
    value = value.strip(" .")
    return value or fallback


def extract_list_number(file_path: Path) -> str | None:
    match = LIST_NUMBER_RE.search(file_path.stem)
    if not match:
        return None
    return match.group(1)


def infer_event_title_from_result_path(result_pdf: Path) -> str:
    # New layout: <base>/<event_title>/results/ResultList_N.pdf
    if result_pdf.parent.name.lower() == "results" and result_pdf.parent.parent.name:
        return result_pdf.parent.parent.name

    # Legacy layout: <base>/results/<event_title>/ResultList_N.pdf
    if result_pdf.parent.parent.name.lower() == "results" and result_pdf.parent.name:
        return result_pdf.parent.name

    return result_pdf.parent.name or "unknown_event"


def discover_result_pdfs(root_dir: Path, event_title: str | None = None) -> list[Path]:
    candidates: list[Path] = []

    if event_title:
        title_dir = root_dir / event_title / "results"
        if title_dir.exists():
            candidates.extend(
                sorted(title_dir.glob("ResultList_*.pdf"), key=lambda p: int(extract_list_number(p) or 0))
            )
        return candidates

    candidates.extend(
        sorted(root_dir.glob("*/results/ResultList_*.pdf"), key=lambda p: int(extract_list_number(p) or 0))
    )
    candidates.extend(
        sorted(root_dir.glob("results/*/ResultList_*.pdf"), key=lambda p: int(extract_list_number(p) or 0))
    )

    seen: set[Path] = set()
    deduped: list[Path] = []
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(path)

    return deduped


def resolve_surya_executable(explicit_path: str | None) -> str:
    if explicit_path:
        candidate = Path(explicit_path)
        if candidate.exists():
            return str(candidate)
        raise FileNotFoundError(f"Surya executable not found: {explicit_path}")

    for name in ["surya_ocr", "surya_ocr.exe"]:
        found = shutil.which(name)
        if found:
            return found

    # Also check alongside the current Python interpreter for venv installs
    python_scripts_dir = Path(sys.executable).resolve().parent
    for name in ["surya_ocr.exe", "surya_ocr"]:
        candidate = python_scripts_dir / name
        if candidate.exists():
            return str(candidate)

    raise RuntimeError(
        "Surya OCR CLI not found. Install Surya and ensure 'surya_ocr' is in PATH, "
        "or pass --surya-executable."
    )


def _extract_surya_text_lines(payload: Any) -> list[str]:
    lines: list[str] = []

    if not isinstance(payload, dict):
        return lines

    # Native Surya output shape: {doc_stem: [{..., "text_lines": [{"text": ...}, ...]}, ...]}
    for doc_pages in payload.values():
        if not isinstance(doc_pages, list):
            continue
        for page in doc_pages:
            if not isinstance(page, dict):
                continue
            text_lines = page.get("text_lines")
            if not isinstance(text_lines, list):
                continue
            for entry in text_lines:
                if isinstance(entry, dict):
                    maybe = entry.get("text")
                else:
                    maybe = entry
                if isinstance(maybe, str) and maybe.strip():
                    lines.append(maybe.strip())

    return lines


def _extract_text_from_json_fallback(value: Any, output: list[str]) -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            if key in {"chars", "words", "bbox", "polygon", "confidence", "image_bbox", "page"}:
                continue
            if key in {"text", "value", "label", "recognized_text", "content"} and isinstance(nested, str):
                stripped = nested.strip()
                if stripped:
                    output.append(stripped)
                continue
            _extract_text_from_json_fallback(nested, output)
        return

    if isinstance(value, list):
        for item in value:
            _extract_text_from_json_fallback(item, output)
        return

    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            output.append(stripped)


def _clean_ocr_lines(lines: list[str]) -> list[str]:
    cleaned: list[str] = []
    previous = ""

    for raw in lines:
        line = raw
        line = HTML_TAG_RE.sub(" ", line)
        line = GLUED_TIME_PREFIX_RE.sub(r" \1", line)
        line = GLUED_TIME_SUFFIX_RE.sub(r"\1 ", line)
        line = ALPHA_DIGIT_GLUE_RE.sub(" ", line)
        line = WHITESPACE_RE.sub(" ", line).strip()
        if not line:
            continue
        for normalized_line in _split_line_for_parser(line):
            if len(normalized_line) == 1:
                continue
            if normalized_line == previous:
                continue
            cleaned.append(normalized_line)
            previous = normalized_line

    return cleaned


def _split_line_for_parser(line: str) -> list[str]:
    time_matches = list(RACE_TIME_RE.finditer(line))
    if len(time_matches) != 1:
        return [line]

    match = time_matches[0]
    left = line[: match.start()].strip()
    time_token = match.group(1).strip()
    right = line[match.end() :].strip()

    # Split only record-like club lines to avoid corrupting event/date/header text.
    if not left or not CLUB_HINT_RE.search(left):
        return [line]

    parts: list[str] = [f"{left} {time_token}".strip()]

    right_digit_tokens = re.findall(r"\d{1,4}", right)
    if right_digit_tokens:
        parts.append(right_digit_tokens[-1])

    return [part for part in parts if part]


def _normalize_field(value: Any) -> str:
    if value is None:
        return ""
    return WHITESPACE_RE.sub(" ", str(value)).strip()


def _extract_name_prefix_from_club(club: str) -> tuple[str | None, str]:
    tokens = club.split()
    if len(tokens) < 3:
        return None, club

    def _candidate(length: int) -> tuple[str | None, str]:
        if len(tokens) < length + 1:
            return None, club
        prefix_tokens = tokens[:length]
        suffix_tokens = tokens[length:]
        prefix = " ".join(prefix_tokens).strip()
        suffix = " ".join(suffix_tokens).strip()

        if not prefix or not suffix:
            return None, club
        if any(ch.isdigit() for ch in prefix):
            return None, club
        if CLUB_HINT_RE.search(prefix):
            return None, club

        last = prefix_tokens[-1]
        letters = "".join(ch for ch in last if ch.isalpha())
        if not letters:
            return None, club
        if not (letters.isupper() or letters[0].isupper()):
            return None, club

        return prefix, suffix

    name3, rest3 = _candidate(3)
    if name3:
        return name3, rest3

    name2, rest2 = _candidate(2)
    if name2:
        return name2, rest2

    return None, club


def _normalize_surya_record_fields(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)

    swimmer_name = _normalize_field(normalized.get("swimmer_name"))
    birth_year = _normalize_field(normalized.get("birth_year"))
    club = _normalize_field(normalized.get("club"))
    time_value = _normalize_field(normalized.get("time"))
    points = _normalize_field(normalized.get("points"))

    swimmer_name = MARKER_TAG_RE.sub("", swimmer_name).strip(" .")
    swimmer_name = re.sub(r"^\d{1,3}\.\s+", "", swimmer_name).strip()

    club = MARKER_TAG_RE.sub("", club).strip(" .")

    # Trim appended swimmer fragment from club tails: "... Ortaokulu Eylül ARIK 14 ..."
    embedded_name_year = NAME_YEAR_IN_TEXT_RE.search(club)
    if embedded_name_year and CLUB_HINT_RE.search(club[: embedded_name_year.start()]):
        club = club[: embedded_name_year.start()].strip()

    # Move embedded time from club to time field if parser missed it.
    if not time_value:
        time_match = RACE_TIME_RE.search(club)
        if time_match:
            time_value = time_match.group(1)
            club = f"{club[:time_match.start()]} {club[time_match.end():]}"
            club = WHITESPACE_RE.sub(" ", club).strip()

    # Extract trailing points from club when appropriate.
    if not points:
        tail_points = TRAILING_POINTS_RE.search(club)
        if tail_points:
            points = tail_points.group(1)
            club = club[: tail_points.start()].strip()

    # If swimmer name is numeric noise, try recovering from club prefix.
    if PURE_NUMERIC_NAME_RE.fullmatch(swimmer_name) and club:
        recovered_name, remaining_club = _extract_name_prefix_from_club(club)
        if recovered_name:
            swimmer_name = recovered_name
            club = remaining_club

    normalized["swimmer_name"] = swimmer_name or None
    normalized["birth_year"] = birth_year or None
    normalized["club"] = club or None
    normalized["time"] = time_value or None
    normalized["points"] = points or None
    return normalized


def convert_surya_output_to_text(source_file: Path, output_txt: Path) -> int:
    suffix = source_file.suffix.lower()

    if suffix in {".txt", ".md"}:
        text = source_file.read_text(encoding="utf-8", errors="ignore")
        output_txt.write_text(text, encoding="utf-8")
        return len(text)

    if suffix == ".json":
        payload = json.loads(source_file.read_text(encoding="utf-8", errors="ignore"))
        lines = _extract_surya_text_lines(payload)
        if not lines:
            lines = []
            _extract_text_from_json_fallback(payload, lines)
        text = "\n".join(_clean_ocr_lines(lines))
        output_txt.write_text(text, encoding="utf-8")
        return len(text)

    # Fallback: treat any file as UTF-8 text.
    text = source_file.read_text(encoding="utf-8", errors="ignore")
    output_txt.write_text(text, encoding="utf-8")
    return len(text)


def run_surya_ocr(
    input_pdf: Path,
    output_txt: Path,
    surya_output_dir: Path,
    surya_executable: str,
    surya_command_template: str | None,
    surya_languages: str,
    timeout_sec: int,
) -> str:
    output_txt.parent.mkdir(parents=True, exist_ok=True)
    surya_output_dir.mkdir(parents=True, exist_ok=True)
    previous_output_mtime = output_txt.stat().st_mtime if output_txt.exists() else None

    before_files = {
        path.resolve()
        for path in surya_output_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in {".txt", ".md", ".json"}
    }

    if surya_command_template:
        command = surya_command_template.format(
            surya_executable=surya_executable,
            input_pdf=str(input_pdf),
            output_txt=str(output_txt),
            output_dir=str(surya_output_dir),
            output_stem=output_txt.stem,
            languages=surya_languages,
        )
        try:
            completed = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout_sec,
            )
            template_diagnostic = f"custom(exit={completed.returncode})"
            if completed.stderr and completed.stderr.strip():
                template_diagnostic += f" stderr={completed.stderr.strip()[:300]}"
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(f"Surya command timed out after {timeout_sec}s for {input_pdf.name}") from error
    else:
        attempts = [
            [surya_executable, str(input_pdf), "--output_dir", str(surya_output_dir)],
            [
                sys.executable,
                "-m",
                "surya.scripts.ocr_text",
                str(input_pdf),
                "--output_dir",
                str(surya_output_dir),
            ],
        ]
        diagnostics: list[str] = []
        for cmd in attempts:
            try:
                completed = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=timeout_sec,
                )
            except (FileNotFoundError, subprocess.TimeoutExpired):
                diagnostics.append(f"cmd={' '.join(cmd)} failed_or_timeout")
                continue

            diagnostic = f"cmd={' '.join(cmd)} exit={completed.returncode}"
            if completed.stderr and completed.stderr.strip():
                diagnostic += f" stderr={completed.stderr.strip()[:300]}"
            diagnostics.append(diagnostic)

            if output_txt.exists() and output_txt.stat().st_size > 0:
                if previous_output_mtime is None or output_txt.stat().st_mtime > previous_output_mtime:
                    return "surya output already available"

    after_candidates = [
        path.resolve()
        for path in surya_output_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in {".txt", ".md", ".json"}
    ]

    # Prefer newly created files containing the current PDF stem.
    new_candidates = [path for path in after_candidates if path not in before_files]
    pdf_stem_lower = input_pdf.stem.lower()

    def _path_mentions_pdf_stem(path: Path) -> bool:
        if pdf_stem_lower in path.stem.lower():
            return True
        if pdf_stem_lower in path.parent.name.lower():
            return True
        return False

    stem_candidates = [path for path in new_candidates if _path_mentions_pdf_stem(path)]
    if not stem_candidates:
        stem_candidates = [path for path in after_candidates if _path_mentions_pdf_stem(path)]

    if not stem_candidates:
        diagnostic_text = ""
        if surya_command_template:
            diagnostic_text = f" ({template_diagnostic})"
        elif 'diagnostics' in locals() and diagnostics:
            diagnostic_text = f" attempts: {' | '.join(diagnostics)}"
        raise RuntimeError(
            "Surya OCR completed but no output artifact was found. "
            "Pass --surya-command-template if your local Surya CLI syntax differs."
            f"{diagnostic_text}"
        )

    source_file = sorted(stem_candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0]
    text_len = convert_surya_output_to_text(source_file, output_txt)

    if text_len <= 0:
        raise RuntimeError(f"Surya output file was empty: {source_file}")

    return f"source={source_file.name} chars={text_len}"


def parse_result_text(txt_path: Path, preferred_version: str | None) -> tuple[str, dict[str, Any]]:
    text = txt_path.read_text(encoding="utf-8")

    if preferred_version:
        try:
            parser_func = load_version_parser(preferred_version, Path(__file__).parent / "parsers")
            return preferred_version, parser_func(text)
        except Exception:
            pass

    detected_version = detect_splash_version(text)
    parser_func = load_version_parser(detected_version, Path(__file__).parent / "parsers")
    payload = parser_func(text)
    return detected_version, payload


def rows_from_payload(
    event_title_folder: str,
    result_pdf: Path,
    txt_path: Path,
    detected_version: str,
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    records = list(payload.get("records", [])) + list(payload.get("special_records", []))
    for record in records:
        normalized_record = dict(record)
        if not normalized_record.get("special_type"):
            normalized_record = _normalize_surya_record_fields(normalized_record)

        rows.append(
            {
                "event_title_folder": event_title_folder,
                "result_pdf": str(result_pdf),
                "result_text_file": str(txt_path),
                "detected_version": detected_version,
                "event_no": normalized_record.get("event_no"),
                "event_name": normalized_record.get("event_name"),
                "swimmer_name": normalized_record.get("swimmer_name"),
                "birth_year": normalized_record.get("birth_year"),
                "club": normalized_record.get("club"),
                "time": normalized_record.get("time"),
                "rank": normalized_record.get("rank"),
                "points": normalized_record.get("points"),
                "status": normalized_record.get("status"),
                "special_type": normalized_record.get("special_type"),
                "rule": normalized_record.get("rule"),
                "note_time": normalized_record.get("note_time"),
            }
        )

    return rows


def write_csv(rows: list[dict[str, Any]], output_csv: Path) -> int:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in OUTPUT_FIELDS})
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse all ResultList PDFs to CSV using Surya OCR")
    parser.add_argument("--input-dir", type=Path, default=Path("scraped"), help="Root scraped directory")
    parser.add_argument("--event-title", type=str, default=None, help="Optional event title folder")
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("results/resultlists_all_parsed_surya.csv"),
        help="Output CSV path",
    )
    parser.add_argument(
        "--temp-result-text-dir",
        type=Path,
        default=Path("results/_tmp_result_text_surya"),
        help="Temporary directory for normalized Surya text files",
    )
    parser.add_argument(
        "--surya-output-dir",
        type=Path,
        default=Path("results/_tmp_surya_raw"),
        help="Directory where Surya writes raw OCR artifacts",
    )
    parser.add_argument(
        "--preferred-version",
        type=str,
        default="11.83565",
        help="Preferred Splash parser version before auto-detect fallback",
    )
    parser.add_argument(
        "--surya-executable",
        type=str,
        default=None,
        help="Path to surya_ocr executable (default: auto-detect from PATH)",
    )
    parser.add_argument(
        "--surya-command-template",
        type=str,
        default=None,
        help=(
            "Optional custom Surya command template with placeholders: "
            "{surya_executable}, {input_pdf}, {output_txt}, {output_dir}, {output_stem}, {languages}"
        ),
    )
    parser.add_argument(
        "--surya-languages",
        type=str,
        default="tr,en",
        help="Surya language code list passed to CLI",
    )
    parser.add_argument(
        "--ocr-timeout-sec",
        type=int,
        default=600,
        help="Timeout (seconds) for each Surya OCR command",
    )
    parser.add_argument(
        "--force-reextract",
        action="store_true",
        help="Force Surya OCR extraction even if normalized TXT exists",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Process only the first N result files (0 means all)",
    )
    args = parser.parse_args()

    if not args.input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {args.input_dir}")

    result_pdfs = discover_result_pdfs(args.input_dir, args.event_title)
    if args.max_files > 0:
        result_pdfs = result_pdfs[: args.max_files]

    if not result_pdfs:
        print("No ResultList PDFs found.", file=sys.stderr)
        raise SystemExit(1)

    args.temp_result_text_dir.mkdir(parents=True, exist_ok=True)
    args.surya_output_dir.mkdir(parents=True, exist_ok=True)

    surya_executable = resolve_surya_executable(args.surya_executable)

    all_rows: list[dict[str, Any]] = []
    failures: list[tuple[Path, str]] = []

    for result_pdf in result_pdfs:
        try:
            event_title_folder = infer_event_title_from_result_path(result_pdf)
            output_txt = args.temp_result_text_dir / f"result_{safe_name(event_title_folder)}_{result_pdf.stem}.txt"

            if args.force_reextract or not output_txt.exists():
                if args.force_reextract and output_txt.exists():
                    output_txt.unlink()
                note = run_surya_ocr(
                    input_pdf=result_pdf,
                    output_txt=output_txt,
                    surya_output_dir=args.surya_output_dir,
                    surya_executable=surya_executable,
                    surya_command_template=args.surya_command_template,
                    surya_languages=args.surya_languages,
                    timeout_sec=args.ocr_timeout_sec,
                )
                print(f"OCR {result_pdf.name}: {note}")

            detected_version, payload = parse_result_text(output_txt, args.preferred_version)
            rows = rows_from_payload(event_title_folder, result_pdf, output_txt, detected_version, payload)
            all_rows.extend(rows)
            print(f"Parsed {result_pdf.name}: {len(rows)} rows")

        except Exception as error:
            failures.append((result_pdf, str(error)))
            print(f"Failed {result_pdf.name}: {error}", file=sys.stderr)

    row_count = write_csv(all_rows, args.output_csv)

    print(f"Result PDFs found: {len(result_pdfs)}")
    print(f"Rows written: {row_count}")
    print(f"Saved parsed results CSV: {args.output_csv}")

    if failures:
        print(f"Files failed: {len(failures)}", file=sys.stderr)
        for path, message in failures:
            print(f"- {path}: {message}", file=sys.stderr)


if __name__ == "__main__":
    main()
