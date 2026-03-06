from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable


VERSION_RE = re.compile(r"Splash\s+Meet\s+Manager\s*,\s*([0-9]+(?:\.[0-9]+)+)", re.IGNORECASE)


def detect_splash_version(text: str) -> str:
    match = VERSION_RE.search(text)
    if not match:
        raise ValueError("Could not detect Splash Meet Manager version in input text")
    return match.group(1)


def version_to_parser_filename(version: str) -> str:
    # Python module/file friendly form for versions like 11.83565 -> 11_83565.py
    return f"{version.replace('.', '_')}.py"


def load_version_parser(version: str, parsers_dir: Path) -> Callable[[str], dict[str, Any]]:
    parser_filename = version_to_parser_filename(version)
    parser_path = parsers_dir / parser_filename

    if not parser_path.exists():
        raise FileNotFoundError(
            f"No parser found for Splash Meet Manager version {version}. "
            f"Expected file: {parser_path}"
        )

    module_name = f"splash_parser_{version.replace('.', '_')}"
    spec = importlib.util.spec_from_file_location(module_name, parser_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load parser module: {parser_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    parse_func = getattr(module, "parse", None)
    if parse_func is None or not callable(parse_func):
        raise RuntimeError(f"Parser file {parser_path} does not define callable parse(text)")

    return parse_func


def parse_file(input_path: Path, output_path: Path | None = None) -> dict[str, Any]:
    text = input_path.read_text(encoding="utf-8")
    version = detect_splash_version(text)

    parsers_dir = Path(__file__).parent / "parsers"
    parse_func = load_version_parser(version, parsers_dir)
    payload = parse_func(text)

    result = {
        "detected_version": version,
        "parser_file": str(parsers_dir / version_to_parser_filename(version)),
        "data": payload,
    }

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Detect Splash Meet Manager version and parse file with version-specific parser"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("results/ResultList_20_text_easyocr.txt"),
        help="Input OCR text file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/ResultList_20_parsed.json"),
        help="Output JSON path",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise FileNotFoundError(f"Input text file not found: {args.input}")

    try:
        result = parse_file(args.input, args.output)
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error

    print(f"Detected Splash Meet Manager version: {result['detected_version']}")
    print(f"Parser used: {result['parser_file']}")
    print(f"Saved parsed output to: {args.output}")


if __name__ == "__main__":
    main()
