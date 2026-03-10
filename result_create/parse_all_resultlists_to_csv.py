from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Any

from extract_result import extract_pdf_text
from parse_splash_result import load_version_parser, parse_file as parse_result_text_with_detect
from read_ocr import detect_pdf_needs_ocr, detect_tessdata_path


LIST_NUMBER_RE = re.compile(r"(?i)resultlist_(\d+)")
INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*]+')
SPLASH_SUFFIX_RE = re.compile(r"\s*-\s*SPLASH\s+Meet\s+Manager\s*\d*", re.IGNORECASE)

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
    value = re.sub(r"\s+", " ", value).strip()
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
        return SPLASH_SUFFIX_RE.sub("", result_pdf.parent.parent.name).strip()

    # Legacy layout: <base>/results/<event_title>/ResultList_N.pdf
    if result_pdf.parent.parent.name.lower() == "results" and result_pdf.parent.name:
        return SPLASH_SUFFIX_RE.sub("", result_pdf.parent.name).strip()

    return SPLASH_SUFFIX_RE.sub("", result_pdf.parent.name or "unknown_event").strip()


def discover_result_pdfs(root_dir: Path, event_title: str | None = None) -> list[Path]:
    candidates: list[Path] = []

    if event_title:
        title_dir = root_dir / event_title / "results"
        if title_dir.exists():
            candidates.extend(sorted(title_dir.glob("ResultList_*.pdf"), key=lambda p: int(extract_list_number(p) or 0)))
        return candidates

    candidates.extend(sorted(root_dir.glob("*/results/ResultList_*.pdf"), key=lambda p: int(extract_list_number(p) or 0)))
    candidates.extend(sorted(root_dir.glob("results/*/ResultList_*.pdf"), key=lambda p: int(extract_list_number(p) or 0)))

    seen: set[Path] = set()
    deduped: list[Path] = []
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(path)

    return deduped


def ensure_result_text(
    result_pdf: Path,
    temp_result_text_dir: Path,
    ocr_language: str,
    tessdata: str | None,
    ocr_backend: str,
    ocr_dpi: int,
    ocr_scale: float,
    force_reextract: bool,
) -> Path:
    event_title_folder = infer_event_title_from_result_path(result_pdf)
    output_txt = temp_result_text_dir / f"result_{safe_name(event_title_folder)}_{result_pdf.stem}.txt"

    if output_txt.exists() and not force_reextract:
        return output_txt

    needs_ocr = detect_pdf_needs_ocr(result_pdf)
    extract_pdf_text(
        result_pdf,
        output_txt,
        ocr_language=ocr_language,
        tessdata=tessdata,
        needs_ocr=needs_ocr,
        ocr_backend=ocr_backend,
        ocr_dpi=ocr_dpi,
        ocr_scale=ocr_scale,
    )
    return output_txt


def parse_result_text(txt_path: Path, preferred_version: str | None) -> tuple[str, dict[str, Any]]:
    if preferred_version:
        try:
            text = txt_path.read_text(encoding="utf-8")
            parser_func = load_version_parser(preferred_version, Path(__file__).parent / "parsers")
            return preferred_version, parser_func(text)
        except Exception:
            pass

    parsed = parse_result_text_with_detect(txt_path)
    return parsed.get("detected_version", "unknown"), parsed.get("data", {})


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
        rows.append(
            {
                "event_title_folder": event_title_folder,
                "result_pdf": str(result_pdf),
                "result_text_file": str(txt_path),
                "detected_version": detected_version,
                "event_no": record.get("event_no"),
                "event_name": record.get("event_name"),
                "swimmer_name": record.get("swimmer_name"),
                "birth_year": record.get("birth_year"),
                "club": record.get("club"),
                "time": record.get("time"),
                "rank": record.get("rank"),
                "points": record.get("points"),
                "status": record.get("status"),
                "special_type": record.get("special_type"),
                "rule": record.get("rule"),
                "note_time": record.get("note_time"),
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
    parser = argparse.ArgumentParser(description="Parse all ResultList PDFs and export parsed rows to CSV")
    parser.add_argument("--input-dir", type=Path, default=Path("scraped"), help="Root scraped directory")
    parser.add_argument("--event-title", type=str, default=None, help="Optional event title folder")
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("results/resultlists_all_parsed.csv"),
        help="Output CSV path",
    )
    parser.add_argument(
        "--temp-result-text-dir",
        type=Path,
        default=Path("results/_tmp_result_text"),
        help="Temporary directory for extracted result text files",
    )
    parser.add_argument(
        "--preferred-version",
        type=str,
        default="11.83565",
        help="Preferred Splash parser version to use before auto-detect fallback",
    )
    parser.add_argument(
        "--force-reextract",
        action="store_true",
        help="Re-run OCR and overwrite existing extracted text files",
    )
    parser.add_argument("--ocr-language", type=str, default="tur+eng", help="OCR language")
    parser.add_argument(
        "--ocr-backend",
        type=str,
        default="fitz",
        choices=["fitz", "easyocr", "paddleocr"],
        help="OCR backend for missing text files",
    )
    parser.add_argument("--ocr-dpi", type=int, default=600, help="DPI for fitz OCR")
    parser.add_argument("--ocr-scale", type=float, default=4.5, help="Scale for easyocr/paddleocr")
    parser.add_argument("--tessdata", type=str, default=None, help="Optional tessdata path")
    args = parser.parse_args()

    if not args.input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {args.input_dir}")

    args.temp_result_text_dir.mkdir(parents=True, exist_ok=True)

    result_pdfs = discover_result_pdfs(args.input_dir, args.event_title)
    if not result_pdfs:
        print("No ResultList PDFs found.", file=sys.stderr)
        raise SystemExit(1)

    tessdata = args.tessdata or detect_tessdata_path()
    all_rows: list[dict[str, Any]] = []
    failures: list[tuple[Path, str]] = []

    for result_pdf in result_pdfs:
        try:
            txt_path = ensure_result_text(
                result_pdf,
                temp_result_text_dir=args.temp_result_text_dir,
                ocr_language=args.ocr_language,
                tessdata=tessdata,
                ocr_backend=args.ocr_backend,
                ocr_dpi=args.ocr_dpi,
                ocr_scale=args.ocr_scale,
                force_reextract=args.force_reextract,
            )
            detected_version, payload = parse_result_text(txt_path, args.preferred_version)
            event_title_folder = infer_event_title_from_result_path(result_pdf)
            rows = rows_from_payload(event_title_folder, result_pdf, txt_path, detected_version, payload)
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
