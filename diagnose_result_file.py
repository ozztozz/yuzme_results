from __future__ import annotations

import argparse
from pathlib import Path

from parse_splash_result import load_version_parser


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--number", required=True)
    parser.add_argument("--version", default="11.83565")
    args = parser.parse_args()

    text_path = Path("results/_tmp_result_text") / (
        "result_ANKARA Okul Sporları Küçükler Yüzme İl Birinciliği_"
        f"ResultList_{args.number}.txt"
    )

    parse_func = load_version_parser(args.version, Path("parsers"))
    text = text_path.read_text(encoding="utf-8")
    payload = parse_func(text)

    records = list(payload.get("records", [])) + list(payload.get("special_records", []))
    with_time = [record for record in records if record.get("time")]

    missing_event = [record for record in with_time if not str(record.get("event_no") or "").strip()]
    missing_name = [record for record in with_time if not str(record.get("swimmer_name") or "").strip()]
    missing_birth = [record for record in with_time if not str(record.get("birth_year") or "").strip()]
    missing_club = [record for record in with_time if not str(record.get("club") or "").strip()]

    print(f"ResultList_{args.number}")
    print(f"total_records={len(records)} with_time={len(with_time)}")
    print(f"missing_event_no={len(missing_event)}")
    print(f"missing_name={len(missing_name)}")
    print(f"missing_birth_year={len(missing_birth)}")
    print(f"missing_club={len(missing_club)}")

    print("sample_with_time:")
    for rec in with_time[:8]:
        print(
            f"event={rec.get('event_no')} "
            f"name={rec.get('swimmer_name')} "
            f"birth={rec.get('birth_year')} "
            f"club={rec.get('club')} "
            f"time={rec.get('time')}"
        )


if __name__ == "__main__":
    main()
