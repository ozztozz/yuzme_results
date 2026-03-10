from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Any

from parse_splash_result import detect_splash_version, load_version_parser


LIST_NUMBER_RE = re.compile(r"(?i)resultlist_(\d+)")
INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*]+')
WHITESPACE_RE = re.compile(r"\s+")

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


def get_doctr_components() -> tuple[Any, Any]:
    try:
        from doctr.io import DocumentFile
        from doctr.models import ocr_predictor
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Doctr is not installed. Install it with: pip install \"python-doctr[torch]\""
        ) from error

    return DocumentFile, ocr_predictor


def extract_text_with_doctr(input_pdf: Path, output_txt: Path, model: Any, document_file_cls: Any) -> int:
    output_txt.parent.mkdir(parents=True, exist_ok=True)

    document = document_file_cls.from_pdf(str(input_pdf))
    result = model(document)
    exported = result.export()

    chunks: list[str] = []
    page_count = 0
    for page_index, page in enumerate(exported.get("pages", []), start=1):
        page_lines: list[str] = []
        for block in page.get("blocks", []):
            for line in block.get("lines", []):
                words = [
                    str(word.get("value", "")).strip()
                    for word in line.get("words", [])
                    if str(word.get("value", "")).strip()
                ]
                if not words:
                    continue
                line_text = WHITESPACE_RE.sub(" ", " ".join(words)).strip()
                if line_text:
                    page_lines.append(line_text)

        if page_lines:
            chunks.append(f"--- Page {page_index} ---")
            chunks.extend(page_lines)
            chunks.append("")
            page_count += 1

    output_txt.write_text("\n".join(chunks), encoding="utf-8")
    return page_count


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
    parser = argparse.ArgumentParser(description="Parse all ResultList PDFs to CSV using Doctr OCR")
    parser.add_argument("--input-dir", type=Path, default=Path("scraped"), help="Root scraped directory")
    parser.add_argument("--event-title", type=str, default=None, help="Optional event title folder")
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("results/resultlists_all_parsed_doctr.csv"),
        help="Output CSV path",
    )
    parser.add_argument(
        "--temp-result-text-dir",
        type=Path,
        default=Path("results/_tmp_result_text_doctr"),
        help="Temporary directory for Doctr extracted result text files",
    )
    parser.add_argument(
        "--preferred-version",
        type=str,
        default="11.83565",
        help="Preferred Splash parser version before auto-detect fallback",
    )
    parser.add_argument(
        "--force-reextract",
        action="store_true",
        help="Force Doctr OCR extraction even if TXT files already exist",
    )
    args = parser.parse_args()

    if not args.input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {args.input_dir}")

    result_pdfs = discover_result_pdfs(args.input_dir, args.event_title)
    if not result_pdfs:
        print("No ResultList PDFs found.", file=sys.stderr)
        raise SystemExit(1)

    args.temp_result_text_dir.mkdir(parents=True, exist_ok=True)

    document_file_cls, ocr_predictor = get_doctr_components()
    model = ocr_predictor(pretrained=True)

    all_rows: list[dict[str, Any]] = []
    failures: list[tuple[Path, str]] = []

    for result_pdf in result_pdfs:
        try:
            event_title_folder = infer_event_title_from_result_path(result_pdf)
            output_txt = args.temp_result_text_dir / f"result_{safe_name(event_title_folder)}_{result_pdf.stem}.txt"

            if args.force_reextract or not output_txt.exists():
                page_count = extract_text_with_doctr(result_pdf, output_txt, model, document_file_cls)
                print(f"OCR {result_pdf.name}: {page_count} pages")

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
