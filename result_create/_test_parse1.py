from parse_splash_result import load_version_parser
from pathlib import Path

parser = load_version_parser("11.83082", Path("parsers"))
text = open("results/_tmp_extracted_text_clean_update/result_ResultList_1_ocr.txt", encoding="utf-8").read()
payload = parser(text)
for r in [*payload.get("records", []), *payload.get("special_records", [])]:
    print(f"  rank={r['rank']}  name={r['swimmer_name']}  club={r['club']}  time={r['time']}  splits={r['split_times']}")
