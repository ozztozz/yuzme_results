from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


def timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_once(python_executable: str, pipeline_script: Path, event_url: str, workdir: Path) -> int:
    command = [python_executable, str(pipeline_script), event_url]
    started = time.monotonic()

    print(f"[{timestamp()}] Running: {' '.join(command)}")
    result = subprocess.run(command, cwd=str(workdir), check=False)

    duration = time.monotonic() - started
    print(
        f"[{timestamp()}] Finished run in {duration:.1f}s with exit code {result.returncode}"
    )
    return result.returncode


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
    args = parser.parse_args()

    if args.interval <= 0:
        raise SystemExit("Interval must be greater than 0")

    workdir = Path(__file__).resolve().parent
    pipeline_script = workdir / "clean_result_update_pipeline.py"
    if not pipeline_script.exists():
        raise SystemExit(f"Pipeline script not found: {pipeline_script}")

    print(f"[{timestamp()}] Watcher started")
    print(f"[{timestamp()}] Event URL: {args.event_url}")
    print(f"[{timestamp()}] Interval: {args.interval:.1f}s")

    run_no = 0
    try:
        while True:
            run_no += 1
            print(f"[{timestamp()}] ---- Run #{run_no} ----")
            exit_code = run_once(sys.executable, pipeline_script, args.event_url, workdir)

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
