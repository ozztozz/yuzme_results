from __future__ import annotations

import argparse
import sys
from pathlib import Path

from docling.document_converter import DocumentConverter


def export_document_text(document: object) -> str:
    """Export Docling document content as text with version-safe fallbacks."""
    if hasattr(document, "export_to_text"):
        return str(document.export_to_text())

    if hasattr(document, "export_to_markdown"):
        # Markdown is still plain text and preserves structure for downstream parsing.
        return str(document.export_to_markdown())

    return str(document)


def parse_pdf_to_text(input_pdf: Path, output_text: Path) -> Path:
    if not input_pdf.exists():
        raise FileNotFoundError(f"Input PDF not found: {input_pdf}")

    converter = DocumentConverter()
    conversion_result = converter.convert(str(input_pdf))
    text = export_document_text(conversion_result.document)

    output_text.parent.mkdir(parents=True, exist_ok=True)
    output_text.write_text(text, encoding="utf-8")
    return output_text


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert a Results PDF to text using Docling"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("results/ResultList_20.pdf"),
        help="Input Results PDF path",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/ResultList_20_text_docling.txt"),
        help="Output text file path",
    )
    args = parser.parse_args()

    try:
        output_path = parse_pdf_to_text(args.input, args.output)
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error

    print(f"Converted PDF to text: {output_path}")


if __name__ == "__main__":
    main()
