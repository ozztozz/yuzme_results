"""
Local integration test for the matching pipeline.
Uses already-downloaded PDFs in scraped/ — no network access needed.

Usage (from yuzme_results/result_create/):
    python test_local_match.py
    python test_local_match.py --lists 1 2 3   # test only specific list numbers
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from clean_result_update_pipeline import (
    DEBUG_DIR,
    parse_startlist_rows,
    match_results_to_startlist_rows,
    extract_list_number,
)

SCRAPED_ROOT = Path("scraped")
EVENT_UNIQUE_NAME = "test-local"


def find_pdfs(subdir: str) -> list[Path]:
    pdfs: list[Path] = []
    for event_dir in SCRAPED_ROOT.iterdir():
        candidate = event_dir / subdir
        if candidate.is_dir():
            pdfs.extend(sorted(candidate.glob("*.pdf")))
    return sorted(pdfs)


def main() -> None:
    parser = argparse.ArgumentParser(description="Local match pipeline test")
    parser.add_argument(
        "--lists",
        nargs="*",
        metavar="N",
        help="Only process these list numbers (e.g. --lists 1 2 3). Default: all.",
    )
    parser.add_argument(
        "--cached-only",
        action="store_true",
        help="Skip result PDFs that have no saved OCR/native control text (avoids slow re-OCR).",
    )
    args = parser.parse_args()

    filter_nos: set[str] | None = None
    if args.lists:
        filter_nos = set(args.lists)

    all_startlist_pdfs = find_pdfs("startlists")
    all_result_pdfs = find_pdfs("results")

    if not all_startlist_pdfs:
        sys.exit("No startlist PDFs found under scraped/*/startlists/")
    if not all_result_pdfs:
        sys.exit("No result PDFs found under scraped/*/results/")

    if filter_nos:
        all_result_pdfs = [p for p in all_result_pdfs if extract_list_number(p) in filter_nos]
        needed = {extract_list_number(p) for p in all_result_pdfs} - {None}
        all_startlist_pdfs = [p for p in all_startlist_pdfs if extract_list_number(p) in needed]
        print(f"Filtered to list numbers: {sorted(filter_nos)}")

    if args.cached_only:
        from clean_result_update_pipeline import CONTROL_TEXT_DIR, _safe_name
        def _has_cache(pdf: Path) -> bool:
            stem = _safe_name(pdf.stem)
            return (
                (CONTROL_TEXT_DIR / f"result_{stem}_ocr.txt").exists()
                or (CONTROL_TEXT_DIR / f"result_{stem}_native.txt").exists()
            )
        before = len(all_result_pdfs)
        all_result_pdfs = [p for p in all_result_pdfs if _has_cache(p)]
        print(f"--cached-only: kept {len(all_result_pdfs)}/{before} result PDFs with existing text cache")
        needed = {extract_list_number(p) for p in all_result_pdfs} - {None}
        all_startlist_pdfs = [p for p in all_startlist_pdfs if extract_list_number(p) in needed]

    print(f"Startlist PDFs : {len(all_startlist_pdfs)}")
    print(f"Result PDFs    : {len(all_result_pdfs)}")
    print()

    startlist_rows = parse_startlist_rows(all_startlist_pdfs, event_unique_name=EVENT_UNIQUE_NAME)
    print(f"Startlist rows parsed : {len(startlist_rows)}")

    matched_rows, stats = match_results_to_startlist_rows(startlist_rows, all_result_pdfs)

    print()
    print("=== Match stats ===")
    print(json.dumps(stats, indent=2, ensure_ascii=False))
    print()
    print(f"Matched rows returned : {len(matched_rows)}")

    if stats["result_records_seen"] > 0:
        pct = 100 * stats["result_records_matched"] / stats["result_records_seen"]
        print(f"Match rate            : {pct:.1f}%")

    print()
    print(f"Debug files written to: {DEBUG_DIR.resolve()}")


if __name__ == "__main__":
    main()
