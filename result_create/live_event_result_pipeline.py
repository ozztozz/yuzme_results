from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import parse_all_resultlists_to_csv as fitz_parser
import parse_all_resultlists_to_csv_doctr as doctr_parser
import parse_all_resultlists_to_csv_surya as surya_parser
import parse_all_startlists_to_csv as startlist_parser
import scrape_results
from read_ocr import detect_tessdata_path


PIPELINE_RESULT_FIELDS = list(fitz_parser.OUTPUT_FIELDS) + ["parser_backend"]


@dataclass
class ParseAttempt:
    backend: str
    rows: list[dict[str, Any]]
    score: int
    time_count: int
    points_count: int
    error: str | None = None


class RuntimeContext:
    def __init__(self, workspace_root: Path, preferred_version: str, ocr_language: str, ocr_dpi: int, ocr_scale: float):
        self.workspace_root = workspace_root
        self.preferred_version = preferred_version
        self.ocr_language = ocr_language
        self.ocr_dpi = ocr_dpi
        self.ocr_scale = ocr_scale
        self.tessdata = detect_tessdata_path()

        self.fitz_text_dir = workspace_root / "results" / "_tmp_result_text_live_fitz"
        self.doctr_text_dir = workspace_root / "results" / "_tmp_result_text_live_doctr"
        self.surya_text_dir = workspace_root / "results" / "_tmp_result_text_live_surya"
        self.surya_raw_dir = workspace_root / "results" / "_tmp_surya_raw_live"

        self.fitz_text_dir.mkdir(parents=True, exist_ok=True)
        self.doctr_text_dir.mkdir(parents=True, exist_ok=True)
        self.surya_text_dir.mkdir(parents=True, exist_ok=True)
        self.surya_raw_dir.mkdir(parents=True, exist_ok=True)

        self.doctr_model: Any | None = None
        self.doctr_document_cls: Any | None = None
        self.surya_executable: str | None = None


def safe_slug(value: str) -> str:
    out = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value.strip())
    out = "_".join(part for part in out.split("_") if part)
    return out or "event"


def load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {
            "parsed_result_pdfs": [],
            "parser_stats": {},
            "poll_count": 0,
        }

    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "parsed_result_pdfs": [],
            "parser_stats": {},
            "poll_count": 0,
        }


def save_state(state_path: Path, payload: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_existing_parsed_pdfs(result_csv: Path) -> set[str]:
    if not result_csv.exists():
        return set()

    parsed: set[str] = set()
    with result_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            result_pdf = str(row.get("result_pdf") or "").strip()
            if result_pdf:
                parsed.add(result_pdf)
    return parsed


def read_existing_parsed_pdfs_jsonl(result_jsonl: Path) -> set[str]:
    if not result_jsonl.exists():
        return set()

    parsed: set[str] = set()
    with result_jsonl.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            result_pdf = str(payload.get("result_pdf") or "").strip()
            if result_pdf:
                parsed.add(result_pdf)
    return parsed


def append_rows(output_csv: Path, rows: list[dict[str, Any]]) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    file_exists = output_csv.exists()

    with output_csv.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PIPELINE_RESULT_FIELDS)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in PIPELINE_RESULT_FIELDS})


def append_jsonl_payload(
    output_jsonl: Path,
    event_title: str,
    result_pdf: Path,
    selected_attempt: ParseAttempt,
    rows: list[dict[str, Any]],
) -> None:
    payload = {
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),
        "event_title": event_title,
        "result_pdf": str(result_pdf),
        "parser_backend": selected_attempt.backend,
        "row_count": len(rows),
        "time_count": selected_attempt.time_count,
        "points_count": selected_attempt.points_count,
        "score": selected_attempt.score,
        "ingestion_key": f"{event_title}|{result_pdf.name}|{selected_attempt.backend}",
        "rows": rows,
    }

    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with output_jsonl.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def read_startlist_rows(startlist_csv: Path) -> list[dict[str, Any]]:
    if not startlist_csv.exists():
        return []

    with startlist_csv.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_startlist_rows(startlist_csv: Path, rows: list[dict[str, Any]]) -> None:
    startlist_csv.parent.mkdir(parents=True, exist_ok=True)
    with startlist_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=startlist_parser.OUTPUT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in startlist_parser.OUTPUT_FIELDS})


def ensure_startlist_result_pdf_links(rows: list[dict[str, Any]]) -> int:
    def _build_result_pdf_from_source(source_pdf_text: str) -> str | None:
        source_text = str(source_pdf_text or "").strip()
        if not source_text:
            return None

        match = re.search(r"(?i)startlist_(\d+)\.pdf$", source_text)
        if not match:
            return None

        list_no = match.group(1)
        candidate = re.sub(r"(?i)startlists", "results", source_text)
        candidate = re.sub(r"(?i)startlist_\d+\.pdf$", f"ResultList_{list_no}.pdf", candidate)
        return candidate

    linked = 0
    for row in rows:
        existing = str(row.get("result_pdf") or "").strip()
        if existing:
            continue

        source_pdf = str(row.get("source_pdf") or "").strip()
        if not source_pdf:
            continue

        candidate = startlist_parser.corresponding_result_pdf(Path(source_pdf))
        if candidate is not None:
            row["result_pdf"] = str(candidate)
            linked += 1
            continue

        # Fallback for OCR/encoding-mangled paths: derive by list number shape only.
        fallback_candidate = _build_result_pdf_from_source(source_pdf)
        if not fallback_candidate:
            continue

        row["result_pdf"] = fallback_candidate
        linked += 1

    return linked


def build_score_data_from_parsed_rows(
    parsed_rows: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str], str], dict[tuple[str, str], str], list[Any]]:
    score_map: dict[tuple[str, str], str] = {}
    rank_map: dict[tuple[str, str], str] = {}
    candidates: list[Any] = []
    dedupe_candidates: set[tuple[str, str, str]] = set()

    for record in parsed_rows:
        result_value = str(record.get("time") or "").strip()
        if not result_value:
            continue

        swimmer_name = record.get("swimmer_name")
        club = record.get("club")
        rank_raw = record.get("rank")
        rank_text = str(rank_raw) if rank_raw is not None else None

        name_norm = startlist_parser.normalize_match_text(swimmer_name)
        club_norm = startlist_parser.normalize_match_text(club)
        if not name_norm:
            continue

        for key in startlist_parser.make_match_keys(swimmer_name, club):
            if key not in score_map:
                score_map[key] = result_value
            if rank_text is not None and key not in rank_map:
                rank_map[key] = rank_text

        candidate_key = (name_norm, club_norm, result_value)
        if candidate_key in dedupe_candidates:
            continue

        dedupe_candidates.add(candidate_key)
        candidates.append(
            startlist_parser.ResultScoreCandidate(
                name_norm=name_norm,
                club_norm=club_norm,
                result_value=result_value,
                rank_value=rank_text,
            )
        )

    return score_map, rank_map, candidates


def update_startlist_rows_for_result(
    startlist_rows: list[dict[str, Any]],
    result_pdf: Path,
    score_data: tuple[dict[tuple[str, str], str], dict[tuple[str, str], str], list[Any]],
) -> tuple[int, int, int]:
    fallback_used = 0
    result_pdf_text = str(result_pdf)
    target_rows = [row for row in startlist_rows if str(row.get("result_pdf") or "").strip() == result_pdf_text]

    if not target_rows:
        # Fallback by list number when path text differs in separators/format.
        result_no = fitz_parser.extract_list_number(result_pdf)
        if result_no:
            for row in startlist_rows:
                source_pdf = str(row.get("source_pdf") or "")
                source_no = fitz_parser.extract_list_number(Path(source_pdf)) if source_pdf else None
                if source_no == result_no:
                    target_rows.append(row)

    if not target_rows:
        # Some meets use non-aligned StartList/ResultList numbering.
        # In that case, try matching against unresolved rows globally.
        target_rows = [row for row in startlist_rows if not str(row.get("result") or "").strip()]
        fallback_used = 1

    if not target_rows:
        return 0, 0, 0

    matched, fuzzy = startlist_parser.apply_scores_to_rows(target_rows, score_data)
    if fallback_used:
        return -len(target_rows), matched, fuzzy
    return len(target_rows), matched, fuzzy


def count_non_empty(rows: list[dict[str, Any]], key: str) -> int:
    return sum(1 for row in rows if str(row.get(key) or "").strip())


def compute_score(rows: list[dict[str, Any]]) -> tuple[int, int, int]:
    time_count = count_non_empty(rows, "time")
    points_count = count_non_empty(rows, "points")
    name_count = count_non_empty(rows, "swimmer_name")
    score = (time_count * 5) + (points_count * 2) + name_count
    return score, time_count, points_count


def is_success(rows: list[dict[str, Any]], time_count: int, points_count: int) -> bool:
    return bool(rows) and (time_count > 0 or points_count > 0)


def ensure_doctr_model(context: RuntimeContext) -> tuple[Any, Any]:
    if context.doctr_model is not None and context.doctr_document_cls is not None:
        return context.doctr_model, context.doctr_document_cls

    document_cls, ocr_predictor = doctr_parser.get_doctr_components()
    context.doctr_document_cls = document_cls
    context.doctr_model = ocr_predictor(pretrained=True)
    return context.doctr_model, context.doctr_document_cls


def ensure_surya_executable(context: RuntimeContext) -> str:
    if context.surya_executable:
        return context.surya_executable
    context.surya_executable = surya_parser.resolve_surya_executable(None)
    return context.surya_executable


def parse_with_fitz(result_pdf: Path, event_title: str, context: RuntimeContext) -> ParseAttempt:
    txt_path = fitz_parser.ensure_result_text(
        result_pdf=result_pdf,
        temp_result_text_dir=context.fitz_text_dir,
        ocr_language=context.ocr_language,
        tessdata=context.tessdata,
        ocr_backend="fitz",
        ocr_dpi=context.ocr_dpi,
        ocr_scale=context.ocr_scale,
        force_reextract=False,
    )
    detected_version, payload = fitz_parser.parse_result_text(txt_path, context.preferred_version)
    rows = fitz_parser.rows_from_payload(event_title, result_pdf, txt_path, detected_version, payload)
    score, time_count, points_count = compute_score(rows)
    return ParseAttempt("fitz", rows, score, time_count, points_count)


def parse_with_doctr(result_pdf: Path, event_title: str, context: RuntimeContext) -> ParseAttempt:
    model, document_cls = ensure_doctr_model(context)
    txt_path = context.doctr_text_dir / f"result_{doctr_parser.safe_name(event_title)}_{result_pdf.stem}.txt"
    doctr_parser.extract_text_with_doctr(result_pdf, txt_path, model, document_cls)
    detected_version, payload = doctr_parser.parse_result_text(txt_path, context.preferred_version)
    rows = doctr_parser.rows_from_payload(event_title, result_pdf, txt_path, detected_version, payload)
    score, time_count, points_count = compute_score(rows)
    return ParseAttempt("doctr", rows, score, time_count, points_count)


def parse_with_surya(result_pdf: Path, event_title: str, context: RuntimeContext) -> ParseAttempt:
    surya_executable = ensure_surya_executable(context)
    txt_path = context.surya_text_dir / f"result_{surya_parser.safe_name(event_title)}_{result_pdf.stem}.txt"
    surya_parser.run_surya_ocr(
        input_pdf=result_pdf,
        output_txt=txt_path,
        surya_output_dir=context.surya_raw_dir,
        surya_executable=surya_executable,
        surya_command_template=None,
        surya_languages="tr,en",
        timeout_sec=600,
    )
    detected_version, payload = surya_parser.parse_result_text(txt_path, context.preferred_version)
    rows = surya_parser.rows_from_payload(event_title, result_pdf, txt_path, detected_version, payload)
    score, time_count, points_count = compute_score(rows)
    return ParseAttempt("surya", rows, score, time_count, points_count)


def pick_parser_order(base_order: list[str], state: dict[str, Any], dynamic_order: bool) -> list[str]:
    if not dynamic_order:
        return base_order

    stats = state.get("parser_stats", {})

    def success_ratio(name: str) -> tuple[float, int]:
        item = stats.get(name, {})
        success = int(item.get("success", 0))
        failure = int(item.get("failure", 0))
        # Laplace smoothing so unseen parsers keep baseline position.
        ratio = (success + 1.0) / (success + failure + 2.0)
        return ratio, success

    indexed = list(enumerate(base_order))
    indexed.sort(key=lambda pair: (success_ratio(pair[1])[0], success_ratio(pair[1])[1], -pair[0]), reverse=True)
    return [name for _, name in indexed]


def update_parser_stats(state: dict[str, Any], backend: str, success: bool) -> None:
    stats = state.setdefault("parser_stats", {})
    item = stats.setdefault(backend, {"success": 0, "failure": 0})
    if success:
        item["success"] = int(item.get("success", 0)) + 1
    else:
        item["failure"] = int(item.get("failure", 0)) + 1


def run_startlist_parse(workspace_root: Path, input_dir: Path, event_title: str, output_csv: Path) -> None:
    command = [
        sys.executable,
        str(workspace_root / "parse_all_startlists_to_csv.py"),
        "--input-dir",
        str(input_dir),
        "--event-title",
        event_title,
        "--output-csv",
        str(output_csv),
        "--ocr-backend",
        "fitz",
    ]

    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        message = stderr or stdout or "startlist parse failed"
        raise RuntimeError(message)


def detect_event_title(start_downloads: list[dict[str, Any]], explicit_title: str | None) -> str:
    if explicit_title:
        return explicit_title
    if start_downloads:
        inferred = str(start_downloads[0].get("event_title") or "").strip()
        if inferred:
            return inferred
    raise RuntimeError("Could not determine event title; pass --event-title explicitly.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Live event pipeline: parse startlists first, then poll for new results and parse each new "
            "ResultList with ordered fitz/doctr/surya fallback."
        )
    )
    parser.add_argument("--event-url", type=str, required=True, help="Event page URL")
    parser.add_argument("--event-title", type=str, default=None, help="Optional fixed event title folder")
    parser.add_argument("--input-dir", type=Path, default=Path("scraped"), help="Scraped base directory")
    parser.add_argument(
        "--startlist-output-csv",
        type=Path,
        default=Path("results/live_startlists_all_entries.csv"),
        help="Output CSV for startlist parsing",
    )
    parser.add_argument(
        "--result-output-csv",
        type=Path,
        default=Path("results/live_resultlists_incremental.csv"),
        help="Incremental output CSV for newly parsed result rows",
    )
    parser.add_argument(
        "--result-output-jsonl",
        type=Path,
        default=Path("results/live_resultlists_incremental.jsonl"),
        help="Incremental JSONL output for Django ingestion (one parsed ResultList per line)",
    )
    parser.add_argument(
        "--state-json",
        type=Path,
        default=None,
        help="Optional pipeline state JSON path (default: results/pipeline_state_<event>.json)",
    )
    parser.add_argument(
        "--parser-order",
        type=str,
        default="fitz,doctr,surya",
        help="Ordered parser fallback list, comma separated",
    )
    parser.add_argument(
        "--dynamic-parser-order",
        action="store_true",
        help="Reorder parser attempts by observed success statistics",
    )
    parser.add_argument("--poll-seconds", type=int, default=60, help="Polling interval in seconds")
    parser.add_argument(
        "--max-polls",
        type=int,
        default=0,
        help="Maximum poll iterations (0 means run forever)",
    )
    parser.add_argument(
        "--force-startlist-parse",
        action="store_true",
        help="Force startlist parsing even if output CSV already exists",
    )
    parser.add_argument(
        "--refresh-startlists-after-results",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Use parsed new results to update corresponding rows in base startlist CSV "
            "after each poll"
        ),
    )
    parser.add_argument(
        "--preferred-version",
        type=str,
        default="11.83565",
        help="Preferred Splash parser version before auto-detect fallback",
    )
    parser.add_argument("--ocr-language", type=str, default="tur+eng", help="OCR language for fitz backend")
    parser.add_argument("--ocr-dpi", type=int, default=600, help="DPI for fitz OCR")
    parser.add_argument("--ocr-scale", type=float, default=4.5, help="OCR scale for fitz fallback")
    args = parser.parse_args()

    workspace_root = Path(__file__).parent
    parser_order = [item.strip().lower() for item in args.parser_order.split(",") if item.strip()]
    allowed_backends = {"fitz", "doctr", "surya"}
    invalid = [name for name in parser_order if name not in allowed_backends]
    if invalid:
        raise ValueError(f"Unsupported parser names in --parser-order: {invalid}")
    if not parser_order:
        raise ValueError("--parser-order must include at least one backend")

    start_downloads = scrape_results.start_scraper(args.event_url, args.input_dir, event_title=args.event_title)
    event_title = detect_event_title(start_downloads, args.event_title)
    print(f"Event title: {event_title}")
    print(f"Startlist PDFs downloaded now: {len(start_downloads)}")

    if args.force_startlist_parse or not args.startlist_output_csv.exists():
        print("Parsing startlists...")
        run_startlist_parse(workspace_root, args.input_dir, event_title, args.startlist_output_csv)
        print(f"Startlists parsed -> {args.startlist_output_csv}")
    else:
        print(f"Startlist CSV exists, skipping parse: {args.startlist_output_csv}")

    base_startlist_rows = read_startlist_rows(args.startlist_output_csv)

    state_path = args.state_json
    if state_path is None:
        state_path = Path("results") / f"pipeline_state_{safe_slug(event_title)}.json"

    state = load_state(state_path)
    parsed_result_pdfs: set[str] = set(str(item) for item in state.get("parsed_result_pdfs", []))
    parsed_result_pdfs.update(read_existing_parsed_pdfs(args.result_output_csv))
    parsed_result_pdfs.update(read_existing_parsed_pdfs_jsonl(args.result_output_jsonl))

    context = RuntimeContext(
        workspace_root=workspace_root,
        preferred_version=args.preferred_version,
        ocr_language=args.ocr_language,
        ocr_dpi=args.ocr_dpi,
        ocr_scale=args.ocr_scale,
    )

    parser_functions = {
        "fitz": parse_with_fitz,
        "doctr": parse_with_doctr,
        "surya": parse_with_surya,
    }

    poll_index = int(state.get("poll_count", 0))
    while True:
        poll_index += 1
        print(f"\n=== Poll {poll_index} ===")

        downloaded_results = scrape_results.result_scraper(args.event_url, args.input_dir, event_title=event_title)
        print(f"Result PDFs downloaded this poll: {len(downloaded_results)}")

        startlist_dirty = False
        if args.refresh_startlists_after_results and base_startlist_rows:
            linked_count = ensure_startlist_result_pdf_links(base_startlist_rows)
            if linked_count:
                startlist_dirty = True
                print(f"Linked startlist rows to result PDFs: {linked_count}")

        all_result_pdfs = fitz_parser.discover_result_pdfs(args.input_dir, event_title)
        new_result_pdfs = [pdf for pdf in all_result_pdfs if str(pdf) not in parsed_result_pdfs]
        print(f"New result PDFs detected: {len(new_result_pdfs)}")
        parsed_new_result_count = 0

        for result_pdf in new_result_pdfs:
            ordered_backends = pick_parser_order(parser_order, state, args.dynamic_parser_order)
            print(f"Parsing {result_pdf.name} with order: {ordered_backends}")

            selected_attempt: ParseAttempt | None = None
            errors: list[str] = []

            for backend in ordered_backends:
                parse_func = parser_functions[backend]
                try:
                    attempt = parse_func(result_pdf, event_title, context)
                    ok = is_success(attempt.rows, attempt.time_count, attempt.points_count)
                    update_parser_stats(state, backend, ok)

                    print(
                        f"- {backend}: rows={len(attempt.rows)} time={attempt.time_count} "
                        f"points={attempt.points_count} score={attempt.score} success={ok}"
                    )

                    if ok:
                        selected_attempt = attempt
                        break
                except Exception as error:
                    update_parser_stats(state, backend, False)
                    errors.append(f"{backend}: {error}")
                    print(f"- {backend}: failed ({error})")

            if selected_attempt is None:
                if errors:
                    print(f"Failed {result_pdf.name}. Errors: {' | '.join(errors)}")
                else:
                    print(f"Failed {result_pdf.name}. No backend produced usable rows.")
                continue

            rows_to_append: list[dict[str, Any]] = []
            for row in selected_attempt.rows:
                tagged = dict(row)
                tagged["parser_backend"] = selected_attempt.backend
                rows_to_append.append(tagged)

            append_rows(args.result_output_csv, rows_to_append)
            append_jsonl_payload(
                output_jsonl=args.result_output_jsonl,
                event_title=event_title,
                result_pdf=result_pdf,
                selected_attempt=selected_attempt,
                rows=rows_to_append,
            )

            if args.refresh_startlists_after_results and base_startlist_rows:
                score_data = build_score_data_from_parsed_rows(selected_attempt.rows)
                target_count, matched_count, fuzzy_count = update_startlist_rows_for_result(
                    base_startlist_rows,
                    result_pdf,
                    score_data,
                )
                if target_count != 0:
                    startlist_dirty = True
                    mode = "fallback" if target_count < 0 else "direct"
                    print(
                        f"Updated startlist rows for {result_pdf.name}: "
                        f"mode={mode} target={abs(target_count)} matched={matched_count} fuzzy={fuzzy_count}"
                    )

            parsed_result_pdfs.add(str(result_pdf))
            parsed_new_result_count += 1
            print(
                f"Selected backend for {result_pdf.name}: {selected_attempt.backend} "
                f"(rows={len(selected_attempt.rows)}, time={selected_attempt.time_count})"
            )

        if args.refresh_startlists_after_results and startlist_dirty:
            write_startlist_rows(args.startlist_output_csv, base_startlist_rows)
            print(f"Startlist base CSV updated -> {args.startlist_output_csv}")

        state["parsed_result_pdfs"] = sorted(parsed_result_pdfs)
        state["poll_count"] = poll_index
        save_state(state_path, state)

        if args.max_polls > 0 and poll_index >= args.max_polls:
            print("Reached max polls, stopping.")
            break

        print(f"Sleeping {args.poll_seconds}s before next poll...")
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
