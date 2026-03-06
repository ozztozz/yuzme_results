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


def _extract_with_fitz_ocr(page: fitz.Page, ocr_language: str, tessdata: str | None) -> str:
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

        kwargs = {"language": language, "dpi": 300}
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


def _map_to_paddle_lang(ocr_language: str) -> str:
    language = ocr_language.lower()
    if "tur" in language:
        return "latin"
    if "eng" in language:
        return "en"
    return "latin"


def _get_paddle_ocr(lang: str) -> object:
    if lang in PADDLE_OCR_INSTANCES:
        return PADDLE_OCR_INSTANCES[lang]

    paddleocr_module = importlib.import_module("paddleocr")
    PaddleOCR = getattr(paddleocr_module, "PaddleOCR")

    reader = PaddleOCR(
        use_angle_cls=True,
        lang=lang,
        show_log=False,
    )
    PADDLE_OCR_INSTANCES[lang] = reader
    return reader


def _extract_with_paddleocr(page: fitz.Page, ocr_language: str) -> str:
    import numpy as np
    from PIL import Image

    matrix = fitz.Matrix(3, 3)
    pixmap = page.get_pixmap(matrix=matrix, alpha=False)
    image = Image.open(BytesIO(pixmap.tobytes("png"))).convert("RGB")

    ocr = _get_paddle_ocr(_map_to_paddle_lang(ocr_language))
    result = ocr.ocr(np.array(image), cls=True)

    lines: list[str] = []
    for page_result in result or []:
        if not page_result:
            continue
        for item in page_result:
            if len(item) < 2:
                continue
            text_part = item[1]
            if isinstance(text_part, (tuple, list)) and text_part:
                text = str(text_part[0]).strip()
            else:
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


def _extract_with_easyocr(page: fitz.Page, ocr_language: str) -> str:
    import numpy as np
    from PIL import Image

    matrix = fitz.Matrix(3, 3)
    pixmap = page.get_pixmap(matrix=matrix, alpha=False)
    image = Image.open(BytesIO(pixmap.tobytes("png"))).convert("RGB")

    reader = _get_easyocr_reader(_map_to_easyocr_langs(ocr_language))
    result = reader.readtext(np.array(image), detail=0, paragraph=False)

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
) -> str:
    if not force_ocr:
        text = page.get_text("text").strip()
        if text:
            return normalize_turkish_ocr_text(text)

    normalized_backend = ocr_backend.strip().lower()
    if normalized_backend == "easyocr":
        try:
            text = _extract_with_easyocr(page, ocr_language)
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
            text = _extract_with_paddleocr(page, ocr_language)
            return text
        except ModuleNotFoundError as error:
            raise RuntimeError(
                "paddleocr backend selected but package is not installed. "
                "Install 'paddleocr' and a compatible 'paddlepaddle' build first."
            ) from error
        except Exception as error:
            raise RuntimeError(f"paddleocr backend failed: {error}") from error

    if normalized_backend not in {"fitz", "easyocr", "paddleocr"}:
        raise ValueError(f"Unknown OCR backend: {ocr_backend}")

    text = _extract_with_fitz_ocr(page, ocr_language, tessdata)
    return normalize_turkish_ocr_text(text)
