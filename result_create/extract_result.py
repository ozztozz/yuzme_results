from __future__ import annotations

import argparse
import sys
from pathlib import Path

import fitz

from read_ocr import detect_pdf_needs_ocr, detect_tessdata_path, extract_page_text


def extract_pdf_text(
    input_pdf: Path,
    output_txt: Path,
    ocr_language: str,
    tessdata: str | None,
    needs_ocr: bool,
    ocr_backend: str,
    ocr_dpi: int = 300,
    ocr_scale: float = 3.0,
) -> int:
    """Extract text from PDF, using OCR if needed.
    
    For improved OCR accuracy:
    - Increase ocr_dpi to 600 for complex fonts (default 300)
    - Increase ocr_scale to 4.0+ for dense text (default 3.0)
    - Preprocess source PDFs: enhance contrast, deskew pages
    """
    output_txt.parent.mkdir(parents=True, exist_ok=True)

    pages_written = 0
    chunks: list[str] = []

    with fitz.open(input_pdf) as document:
        for page_index, page in enumerate(document, start=1):
            text = extract_page_text(
                page,
                ocr_language=ocr_language,
                tessdata=tessdata,
                force_ocr=needs_ocr,
                ocr_backend=ocr_backend,
                ocr_dpi=ocr_dpi,
                ocr_scale=ocr_scale,
            )
            if not text:
                continue

            chunks.append(f"--- Page {page_index} ---")
            chunks.append(text)
            chunks.append("")
            pages_written += 1

    output_txt.write_text("\n".join(chunks), encoding="utf-8")
    return pages_written


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract text from ResultList_20.pdf into a TXT file")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("results/ResultList_20.pdf"),
        help="Input PDF file path",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/ResultList_20_text.txt"),
        help="Output TXT file path",
    )
    parser.add_argument(
        "--ocr-language",
        type=str,
        default="tur+eng",
        help="OCR language used when native PDF text is missing",
    )
    parser.add_argument(
        "--ocr-backend",
        type=str,
        default="easyocr",
        choices=["fitz", "easyocr", "paddleocr"],
        help="OCR backend: easyocr (default), fitz, or paddleocr",
    )
    parser.add_argument(
        "--ocr-dpi",
        type=int,
        default=300,
        help="DPI for fitz OCR (300 default, 600 for complex fonts)",
    )
    parser.add_argument(
        "--ocr-scale",
        type=float,
        default=3.0,
        help="Scale factor for easyocr/paddleocr (3.0 default = ~900 DPI)",
    )
    parser.add_argument(
        "--tessdata",
        type=str,
        default=None,
        help="Optional explicit tessdata directory",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise FileNotFoundError(f"Input PDF not found: {args.input}")

    tessdata = args.tessdata or detect_tessdata_path()
    ocr_language = args.ocr_language
    needs_ocr = detect_pdf_needs_ocr(args.input)
    try:
        page_count = extract_pdf_text(
            args.input,
            args.output,
            ocr_language=ocr_language,
            tessdata=tessdata,
            needs_ocr=needs_ocr,
            ocr_backend=args.ocr_backend,
            ocr_dpi=args.ocr_dpi,
            ocr_scale=args.ocr_scale,
        )
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error

    print(f"OCR needed: {needs_ocr}")
    print(f"Saved text from {page_count} pages to {args.output}")


if __name__ == "__main__":
    main()
