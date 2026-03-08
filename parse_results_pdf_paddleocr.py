from __future__ import annotations

import argparse
import sys
from pathlib import Path

import fitz

from read_ocr import detect_pdf_needs_ocr, detect_tessdata_path, extract_page_text


def extract_pdf_text_with_paddleocr(
    input_pdf: Path,
    output_txt: Path,
    ocr_language: str,
    tessdata: str | None,
    force_ocr: bool,
    ocr_scale: float,
    ocr_dpi: int,
    ocr_backend: str = "paddleocr",
) -> int:
    """Extract text from PDF with OCR backend."""
    output_txt.parent.mkdir(parents=True, exist_ok=True)

    pages_written = 0
    chunks: list[str] = []

    with fitz.open(input_pdf) as document:
        for page_index, page in enumerate(document, start=1):
            text = extract_page_text(
                page,
                ocr_language=ocr_language,
                tessdata=tessdata,
                force_ocr=force_ocr,
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
    parser = argparse.ArgumentParser(
        description="Convert a Results PDF to text using OCR (PaddleOCR or EasyOCR)"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("results/ResultList_22.pdf"),
        help="Input Results PDF path",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/ResultList_22_text_paddleocr.txt"),
        help="Output text file path",
    )
    parser.add_argument(
        "--ocr-language",
        type=str,
        default="tur+eng",
        help="OCR language preference",
    )
    parser.add_argument(
        "--ocr-scale",
        type=float,
        default=4.0,
        help="Scale factor for OCR rendering (higher can improve small text)",
    )
    parser.add_argument(
        "--ocr-dpi",
        type=int,
        default=300,
        help="Fallback DPI value passed through for compatibility",
    )
    parser.add_argument(
        "--force-ocr",
        action="store_true",
        help="Force OCR even when native text is present",
    )
    parser.add_argument(
        "--tessdata",
        type=str,
        default=None,
        help="Optional tessdata directory (used only if fallback OCR backend is triggered)",
    )
    parser.add_argument(
        "--backend",
        type=str,
        default="easyocr",
        choices=["easyocr", "paddleocr"],
        help="OCR backend to use (default: easyocr for WSL2 stability)",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise FileNotFoundError(f"Input PDF not found: {args.input}")

    tessdata = args.tessdata or detect_tessdata_path()
    needs_ocr = detect_pdf_needs_ocr(args.input)
    use_force_ocr = args.force_ocr or needs_ocr

    try:
        page_count = extract_pdf_text_with_paddleocr(
            input_pdf=args.input,
            output_txt=args.output,
            ocr_language=args.ocr_language,
            tessdata=tessdata,
            force_ocr=use_force_ocr,
            ocr_scale=args.ocr_scale,
            ocr_dpi=args.ocr_dpi,
            ocr_backend=args.backend,
        )
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error

    print(f"OCR needed: {needs_ocr}")
    print(f"Force OCR used: {use_force_ocr}")
    print(f"Backend: {args.backend}")
    print(f"Saved text from {page_count} pages to {args.output}")


if __name__ == "__main__":
    main()
