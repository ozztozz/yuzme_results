from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_once(command: list[str], workdir: Path) -> int:
    started = time.monotonic()

    print(f"[{timestamp()}] Running: {' '.join(command)}")
    result = subprocess.run(command, cwd=str(workdir), check=False)

    duration = time.monotonic() - started
    print(
        f"[{timestamp()}] Finished run in {duration:.1f}s with exit code {result.returncode}"
    )
    return result.returncode


def extract_event_unique_name(event_url: str) -> str | None:
    match = re.search(r"/(cs-\d+)(?:/|$)", event_url.strip(), flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).lower()


def build_pipeline_command(
    python_executable: str,
    workdir: Path,
    event_url: str,
    event_unique_name: str | None,
) -> list[str]:
    clean_pipeline = workdir / "clean_result_update_pipeline.py"
    if clean_pipeline.exists():
        return [python_executable, str(clean_pipeline), event_url]

    live_pipeline = workdir / "live_event_result_pipeline.py"
    if not live_pipeline.exists():
        raise SystemExit(
            f"No supported pipeline script found in {workdir}. "
            "Expected clean_result_update_pipeline.py or live_event_result_pipeline.py"
        )

    state_name = f"pipeline_state_{event_unique_name}.json" if event_unique_name else "pipeline_state_watch.json"
    return [
        python_executable,
        str(live_pipeline),
        "--event-url",
        event_url,
        "--input-dir",
        str(workdir / "scraped"),
        "--startlist-output-csv",
        str(workdir / "results" / "live_startlists_all_entries.csv"),
        "--result-output-csv",
        str(workdir / "results" / "live_resultlists_incremental.csv"),
        "--result-output-jsonl",
        str(workdir / "results" / "live_resultlists_incremental.jsonl"),
        "--state-json",
        str(workdir / "results" / state_name),
        "--max-polls",
        "1",
        "--poll-seconds",
        "30",
    ]


def build_push_commands(
    python_executable: str,
    workdir: Path,
    event_unique_name: str | None,
) -> list[list[str]]:
    push_script = workdir / "push_results_to_web.py"
    if not push_script.exists():
        return []

    startlist_csv = workdir / "results" / "live_startlists_all_entries.csv"
    token = os.getenv("YUZME_INGEST_TOKEN", "").strip()
    remote_base = os.getenv("YUZME_WEB_BASE_URL", "https://ozztozz.pythonanywhere.com/").rstrip("/")
    endpoints = [
        "http://127.0.0.1:8000/api/ingest-results/",
        f"{remote_base}/api/ingest-results/",
    ]

    commands: list[list[str]] = []
    for endpoint in endpoints:
        command = [
            python_executable,
            str(push_script),
            "--endpoint",
            endpoint,
            "--startlist-csv",
            str(startlist_csv),
        ]
        if event_unique_name:
            command.extend(["--event-unique-name", event_unique_name])
        if token:
            command.extend(["--token", token])
        commands.append(command)

    return commands


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run clean_result_update_pipeline periodically for a single event URL."
    )
    parser.add_argument("event_url", help="Event page URL")
    parser.add_argument(
        "--interval",
        type=float,
        default=30.0,
        help="Seconds between checks (default: 30)",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop loop if a run exits with non-zero status.",
    )
    parser.add_argument(
        "--event-unique-name",
        default=None,
        help="Optional event unique name (e.g. cs-1005252). If omitted, extracted from URL when possible.",
    )
    parser.add_argument(
        "--skip-push",
        action="store_true",
        help="Skip posting parsed rows to API endpoints after each run.",
    )
    args = parser.parse_args()

    if args.interval <= 0:
        raise SystemExit("Interval must be greater than 0")

    workdir = Path(__file__).resolve().parent
    event_unique_name = args.event_unique_name or extract_event_unique_name(args.event_url)
    pipeline_command = build_pipeline_command(sys.executable, workdir, args.event_url, event_unique_name)
    push_commands = [] if args.skip_push else build_push_commands(sys.executable, workdir, event_unique_name)

    print(f"[{timestamp()}] Watcher started")
    print(f"[{timestamp()}] Event URL: {args.event_url}")
    if event_unique_name:
        print(f"[{timestamp()}] Event unique name: {event_unique_name}")
    print(f"[{timestamp()}] Interval: {args.interval:.1f}s")

    run_no = 0
    try:
        while True:
            run_no += 1
            print(f"[{timestamp()}] ---- Run #{run_no} ----")
            exit_code = run_once(pipeline_command, workdir)

            if exit_code == 0 and push_commands:
                for push_command in push_commands:
                    push_exit_code = run_once(push_command, workdir)
                    if push_exit_code != 0 and args.stop_on_error:
                        print(f"[{timestamp()}] Stopping because --stop-on-error is enabled")
                        return push_exit_code

            if exit_code != 0 and args.stop_on_error:
                print(f"[{timestamp()}] Stopping because --stop-on-error is enabled")
                return exit_code

            print(f"[{timestamp()}] Sleeping {args.interval:.1f}s")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"[{timestamp()}] Watcher stopped by user")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
