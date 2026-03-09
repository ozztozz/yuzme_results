from __future__ import annotations

import csv
import re
from pathlib import Path

from parse_all_startlists_to_csv import normalize_match_text
from parse_splash_result import load_version_parser


def main() -> None:
    csv_path = Path("results/ANKARA_startlists_all_entries.csv")
    text_dir = Path("results/_tmp_result_text")
    parser = load_version_parser("11.83565", Path("parsers"))

    rows_by_no: dict[str, int] = {}
    matched_by_no: dict[str, int] = {}

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            match = re.search(r"StartList_(\d+)\.pdf", row.get("source_pdf", ""))
            if not match:
                continue

            number = match.group(1)
            rows_by_no[number] = rows_by_no.get(number, 0) + 1

            if (row.get("result") or "").strip():
                matched_by_no[number] = matched_by_no.get(number, 0) + 1

    print("no,start_rows,parsed_time_rows,unique_event_name_keys,matched_rows")

    total_start = 0
    total_parsed = 0
    total_keys = 0
    total_matched = 0

    for number in sorted(rows_by_no.keys(), key=lambda value: int(value)):
        text_path = text_dir / (
            "result_ANKARA Okul Sporları Küçükler Yüzme İl Birinciliği_"
            f"ResultList_{number}.txt"
        )

        parsed_time_rows = 0
        unique_keys = 0

        if text_path.exists():
            text = text_path.read_text(encoding="utf-8")
            payload = parser(text)
            records = list(payload.get("records", [])) + list(payload.get("special_records", []))

            keys: set[tuple[str, str]] = set()
            for record in records:
                time_value = record.get("time")
                if not time_value:
                    continue

                parsed_time_rows += 1
                event_no = str(record.get("event_no") or "").strip()
                swimmer_name = normalize_match_text(record.get("swimmer_name"))
                if event_no and swimmer_name:
                    keys.add((event_no, swimmer_name))

            unique_keys = len(keys)

        start_rows = rows_by_no.get(number, 0)
        matched_rows = matched_by_no.get(number, 0)

        print(f"{number},{start_rows},{parsed_time_rows},{unique_keys},{matched_rows}")

        total_start += start_rows
        total_parsed += parsed_time_rows
        total_keys += unique_keys
        total_matched += matched_rows

    print(f"TOTAL,{total_start},{total_parsed},{total_keys},{total_matched}")


if __name__ == "__main__":
    main()
