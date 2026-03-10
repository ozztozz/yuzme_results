from __future__ import annotations

from pathlib import Path
import re
from io import BytesIO
import importlib

import fitz


FAILED_OCR_LANGUAGES: set[str] = set()
PADDLE_OCR_INSTANCES: dict[str, object] = {}
EASY_OCR_INSTANCES: dict[tuple[str, ...], object] = {}

TURKISH_OCR_REPLACEMENTS: list[tuple[str, str]] = [
    (r"\bOkul\s+Sporlani\b", "Okul Sporları"),
    (r"\bOkul\s+Sporlari\b", "Okul Sporları"),
    (r"\bKigdkler\b", "Küçükler"),
    (r"\bKiigukler\b", "Küçükler"),
    (r"\bKigukler\b", "Küçükler"),
    (r"\bKucukler\b", "Küçükler"),
    (r"\bYizme\b", "Yüzme"),
    (r"\bYiizme\b", "Yüzme"),
    (r"\bYuzme\b", "Yüzme"),
    (r"\bil\s+Birinciigi\b", "İl Birinciliği"),
    (r"\bil\s+Birinciligi\b", "İl Birinciliği"),
    (r"\bYaris\b", "Yarış"),
    (r"\bAgik\b", "Açık"),
    (r"\bAcik\b", "Açık"),
    (r"\bSonuglar\b", "Sonuçlar"),
    (r"\bSia\b", "Sıra"),
    (r"\bOzel\b", "Özel"),
    (r"\bSehit\b", "Şehit"),
    (r"\bCankaya\b", "Çankaya"),
    (r"\bGankaya\b", "Çankaya"),
    (r"\bOztiirk\b", "Öztürk"),
    (r"\bOzkan\b", "Özkan"),
    (r"\bIhsan\b", "İhsan"),
    (r"\bDogramaci\b", "Doğramacı"),
    (r"\bIIkokulu\b", "İlkokulu"),
    (r"\bllkokulu\b", "İlkokulu"),
    (r"\bIIkokul\b", "İlkokul"),
    (r"\bllkokul\b", "İlkokul"),
]


def _language_available(tessdata: str | None, language: str) -> bool:
    if not tessdata:
        return True

    tessdata_path = Path(tessdata)
    language_parts = [part.strip() for part in language.split("+") if part.strip()]
    if not language_parts:
        return True

    return all((tessdata_path / f"{part}.traineddata").exists() for part in language_parts)


def normalize_turkish_ocr_text(text: str) -> str:
    normalized_lines: list[str] = []
    for line in text.replace("\r\n", "\n").split("\n"):
        normalized_line = re.sub(r"\s+", " ", line).strip()
        for pattern, replacement in TURKISH_OCR_REPLACEMENTS:
            normalized_line = re.sub(pattern, replacement, normalized_line, flags=re.IGNORECASE)
        normalized_lines.append(normalized_line)

    return "\n".join(normalized_lines).strip()


def detect_tessdata_path() -> str | None:
    local_default = Path("tessdata")
    if local_default.exists():
        return str(local_default)

    windows_default = Path(r"C:/Program Files/Tesseract-OCR/tessdata")
    if windows_default.exists():
        return str(windows_default)

    return None


def detect_pdf_needs_ocr(input_pdf: Path, sample_pages: int = 5) -> bool:
    if sample_pages < 1:
        sample_pages = 1

    with fitz.open(input_pdf) as document:
        page_count = min(len(document), sample_pages)
        if page_count == 0:
            return False

        native_char_count = 0
        result_like_lines = 0

        for page in list(document)[:page_count]:
            text = page.get_text("text") or ""
            text = text.strip()
            if not text:
                continue

            native_char_count += len(text)
            if re.search(r"\b\d{1,3}\.\s+.+?(\d{1,2}:\d{2}\.\d{2}|\d{1,2}\.\d{2})\b", text):
                result_like_lines += 1

    if result_like_lines > 0 and native_char_count >= 200:
        return False

    return native_char_count < 200


def _extract_with_fitz_ocr(page: fitz.Page, ocr_language: str, tessdata: str | None, dpi: int = 300) -> str:
    """Extract text using fitz (PyMuPDF) OCR with Tesseract.
    
    OCR Quality Tips:
    - Use 300 DPI minimum, 600 DPI for complex fonts or dense Turkish text
    - For best results, preprocess PDFs: increase contrast, deskew pages
    """
    normalized_language = ocr_language
    if not _language_available(tessdata, normalized_language):
        normalized_language = "eng"

    ocr_languages_to_try = [normalized_language]
    if normalized_language != "eng":
        ocr_languages_to_try.append("eng")

    last_error: Exception | None = None
    for language in ocr_languages_to_try:
        if language in FAILED_OCR_LANGUAGES:
            continue

        kwargs = {"language": language, "dpi": dpi}
        if tessdata:
            kwargs["tessdata"] = tessdata

        try:
            text_page = page.get_textpage_ocr(**kwargs)
            text = page.get_text("text", textpage=text_page).strip()
            if text:
                return normalize_turkish_ocr_text(text)
        except Exception as error:
            last_error = error
            FAILED_OCR_LANGUAGES.add(language)
            continue

    if last_error is not None:
        raise last_error

    return ""


def _map_to_paddle_lang_candidates(ocr_language: str) -> list[str]:
    language = ocr_language.lower()
    candidates: list[str] = []

    if "tur" in language:
        # PaddleOCR expects language code, not recognition family name.
        candidates.append("tr")
    if "eng" in language:
        candidates.append("en")

    if not candidates:
        candidates = ["tr", "en"]

    # Keep order but remove duplicates.
    deduped: list[str] = []
    for code in candidates:
        if code not in deduped:
            deduped.append(code)

    return deduped


def _get_paddle_ocr(lang: str) -> object:
    if lang in PADDLE_OCR_INSTANCES:
        return PADDLE_OCR_INSTANCES[lang]

    paddleocr_module = importlib.import_module("paddleocr")
    PaddleOCR = getattr(paddleocr_module, "PaddleOCR")

    # Initialize with minimal valid parameters for current PaddleOCR version
    reader = PaddleOCR(
        lang=lang,
        ocr_version="PP-OCRv5",
    )
    PADDLE_OCR_INSTANCES[lang] = reader
    return reader


def _extract_with_paddleocr(page: fitz.Page, ocr_language: str, scale: float = 3.0) -> str:
    """Extract text using PaddleOCR.
    
    Scale factor controls effective DPI (3.0 = ~900 DPI, 2.0 = ~600 DPI).
    Higher scale improves accuracy but increases processing time.
    """
    import numpy as np
    from PIL import Image

    matrix = fitz.Matrix(scale, scale)
    pixmap = page.get_pixmap(matrix=matrix, alpha=False)
    image = Image.open(BytesIO(pixmap.tobytes("png"))).convert("RGB")

    result = None
    last_error: Exception | None = None
    for paddle_lang in _map_to_paddle_lang_candidates(ocr_language):
        try:
            ocr = _get_paddle_ocr(paddle_lang)
            result = ocr.predict(np.array(image))
            break
        except Exception as error:
            error_message = str(error)
            # Only try next language if this exact model-language combination is unsupported.
            if "No models are available for the language" in error_message:
                last_error = error
                continue
            raise

    if result is None:
        if last_error is not None:
            raise last_error
        raise RuntimeError("paddleocr returned no result for all language candidates")

    lines: list[str] = []
    # Handle result format: predict() returns list of (box, text, confidence) tuples per page
    for page_result in result or []:
        if not page_result:
            continue
        for item in page_result:
            # Extract text from tuple (box_coords, text, confidence)
            if isinstance(item, (tuple, list)) and len(item) >= 2:
                # Format: (box, text, confidence)
                text_part = item[1] if len(item) > 1 else item[0]
                text = str(text_part).strip()
                if text:
                    lines.append(text)

    return normalize_turkish_ocr_text("\n".join(lines))


def _map_to_easyocr_langs(ocr_language: str) -> list[str]:
    language = ocr_language.lower()
    langs: list[str] = []

    if "tur" in language:
        langs.append("tr")
    if "eng" in language:
        langs.append("en")

    if not langs:
        langs = ["tr", "en"]

    return langs


def _get_easyocr_reader(langs: list[str]) -> object:
    lang_key = tuple(langs)
    if lang_key in EASY_OCR_INSTANCES:
        return EASY_OCR_INSTANCES[lang_key]

    easyocr_module = importlib.import_module("easyocr")
    Reader = getattr(easyocr_module, "Reader")

    reader = Reader(list(lang_key), gpu=False)
    EASY_OCR_INSTANCES[lang_key] = reader
    return reader


def _extract_with_easyocr(page: fitz.Page, ocr_language: str, scale: float = 3.0) -> str:
    """Extract text using EasyOCR.
    
    Scale factor controls effective DPI (3.0 = ~900 DPI, 2.0 = ~600 DPI).
    For Turkish sports result PDFs with small text, 3.0+ recommended.
    """
    import numpy as np
    from PIL import Image

    matrix = fitz.Matrix(scale, scale)
    pixmap = page.get_pixmap(matrix=matrix, alpha=False)
    image = Image.open(BytesIO(pixmap.tobytes("png"))).convert("RGB")

    reader = _get_easyocr_reader(_map_to_easyocr_langs(ocr_language))
    # High-quality preset for dense race result tables.
    result = reader.readtext(
        np.array(image),
        detail=0,
        paragraph=False,
        decoder="beamsearch",
        beamWidth=10,
        contrast_ths=0.05,
        adjust_contrast=0.7,
        text_threshold=0.5,
        low_text=0.3,
        link_threshold=0.3,
    )

    lines: list[str] = []
    for item in result or []:
        text = str(item).strip()
        if text:
            lines.append(text)

    return normalize_turkish_ocr_text("\n".join(lines))


def _is_windows_torch_dll_error(error: Exception) -> bool:
    message = str(error).lower()
    return "c10.dll" in message or "winerror 1114" in message


def _build_easyocr_windows_fix_message(error: Exception) -> str:
    base = f"easyocr backend failed: {error}"
    if not _is_windows_torch_dll_error(error):
        return base

    return (
        f"{base}\n"
        "Detected Windows Torch DLL load issue (c10.dll / WinError 1114).\n"
        "Fix steps:\n"
        "1) Install Microsoft Visual C++ Redistributable 2015-2022 (x64, and x86 if needed).\n"
        "2) Restart terminal/VS Code after installation.\n"
        "3) Reinstall torch in this venv and retry easyocr.\n"
        "4) Use '--ocr-backend fitz' until easyocr works."
    )


def extract_page_text(
    page: fitz.Page,
    ocr_language: str,
    tessdata: str | None,
    force_ocr: bool = False,
    ocr_backend: str = "easyocr",
    ocr_dpi: int = 300,
    ocr_scale: float = 3.0,
) -> str:
    """Extract text from a PDF page, using OCR if needed.
    
    Args:
        ocr_dpi: DPI for fitz/Tesseract OCR (300 min, 600 for complex fonts)
        ocr_scale: Scale factor for easyocr/paddleocr (3.0 = ~900 DPI equivalent)
    """
    if not force_ocr:
        text = page.get_text("text").strip()
        if text:
            return normalize_turkish_ocr_text(text)

    normalized_backend = ocr_backend.strip().lower()
    if normalized_backend == "easyocr":
        try:
            text = _extract_with_easyocr(page, ocr_language, ocr_scale)
            return text
        except ModuleNotFoundError as error:
            raise RuntimeError(
                "easyocr backend selected but package is not installed. "
                "Install 'easyocr' first."
            ) from error
        except Exception as error:
            raise RuntimeError(_build_easyocr_windows_fix_message(error)) from error

    if normalized_backend == "paddleocr":
        try:
            text = _extract_with_paddleocr(page, ocr_language, ocr_scale)
            return text
        except ModuleNotFoundError as error:
            missing_module = getattr(error, "name", None) or "unknown"
            raise RuntimeError(
                "paddleocr backend dependency is missing "
                f"(module: {missing_module}). Install 'paddleocr' and a compatible "
                "'paddlepaddle' build first."
            ) from error
        except Exception as error:
            raise RuntimeError(f"paddleocr backend failed: {error}") from error

    if normalized_backend not in {"fitz", "easyocr", "paddleocr"}:
        raise ValueError(f"Unknown OCR backend: {ocr_backend}")

    text = _extract_with_fitz_ocr(page, ocr_language, tessdata, ocr_dpi)
    return normalize_turkish_ocr_text(text)
