from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse
from urllib.request import Request, urlopen


START_KEYWORDS = (
    "start list",
    "start listesi",
    "startlist",
    "entry list",
    "entries",
    "seeding",
    "program",
)

RESULT_KEYWORDS = (
    "result",
    "results",
    "sonuc",
    "sonuclar",
    "sonuç",
    "final",
    "timing",
)

INVALID_FILENAME_CHARS_RE = re.compile(r'[<>:"/\\|?*]+')
WHITESPACE_RE = re.compile(r"\s+")


@dataclass
class PdfLink:
    url: str
    text: str
    title: str


class PdfLinkExtractor(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links: list[PdfLink] = []

        self.page_title = ""
        self.last_heading = ""

        self._in_title = False
        self._title_parts: list[str] = []

        self._heading_tag: str | None = None
        self._heading_parts: list[str] = []

        self._in_anchor = False
        self._anchor_href: str | None = None
        self._anchor_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key.lower(): value for key, value in attrs}

        if tag.lower() == "title":
            self._in_title = True
            self._title_parts = []
            return

        if tag.lower() in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._heading_tag = tag.lower()
            self._heading_parts = []
            return

        if tag.lower() == "a":
            self._in_anchor = True
            self._anchor_href = attr_map.get("href")
            self._anchor_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self._title_parts.append(data)

        if self._heading_tag is not None:
            self._heading_parts.append(data)

        if self._in_anchor:
            self._anchor_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        lower_tag = tag.lower()

        if lower_tag == "title" and self._in_title:
            self._in_title = False
            self.page_title = clean_text(" ".join(self._title_parts))
            self._title_parts = []
            return

        if self._heading_tag == lower_tag:
            self.last_heading = clean_text(" ".join(self._heading_parts))
            self._heading_tag = None
            self._heading_parts = []
            return

        if lower_tag == "a" and self._in_anchor:
            self._in_anchor = False
            href = (self._anchor_href or "").strip()
            text = clean_text(" ".join(self._anchor_parts))
            self._anchor_href = None
            self._anchor_parts = []

            if not href:
                return

            full_url = urljoin(self.base_url, href)
            if not looks_like_pdf(full_url):
                return

            title = self.last_heading or self.page_title or "untitled"
            self.links.append(PdfLink(url=full_url, text=text, title=title))


def clean_text(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value).strip()


def looks_like_pdf(url: str) -> bool:
    lowered = url.lower()
    return lowered.endswith(".pdf") or ".pdf?" in lowered


def safe_name(value: str, fallback: str = "untitled") -> str:
    value = clean_text(value)
    value = INVALID_FILENAME_CHARS_RE.sub("_", value)
    value = value.strip(" .")
    if not value:
        value = fallback
    return value[:120]


def safe_pdf_filename(value: str, fallback: str) -> str:
    candidate = safe_name(value, fallback=fallback)
    if candidate.lower().endswith(".pdf"):
        stem = candidate[:-4].rstrip(" .")
        return f"{stem}.pdf" if stem else f"{fallback}.pdf"
    return f"{candidate}.pdf"


def fetch_html(url: str, timeout: int = 30) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        },
    )
    with urlopen(request, timeout=timeout) as response:
        content_type = response.headers.get("Content-Type", "")
        charset = response.headers.get_content_charset() or "utf-8"
        data = response.read()

    if "text" not in content_type.lower() and "html" not in content_type.lower():
        raise ValueError(f"URL does not look like an HTML page: {url}")

    return data.decode(charset, errors="replace")


def download_pdf(url: str, destination: Path, timeout: int = 60) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        },
    )
    with urlopen(request, timeout=timeout) as response:
        data = response.read()

    destination.write_bytes(data)


def keyword_match(link: PdfLink, keywords: tuple[str, ...]) -> bool:
    haystack = f"{link.text} {link.title} {link.url}".lower()
    return any(keyword in haystack for keyword in keywords)


def choose_links(links: list[PdfLink], keywords: tuple[str, ...]) -> list[PdfLink]:
    matched = [link for link in links if keyword_match(link, keywords)]
    if matched:
        return matched
    return links


def scrape_and_download(
    page_url: str,
    output_root: Path,
    keywords: tuple[str, ...],
    category_subfolder: str,
    event_title_override: str | None = None,
) -> list[dict[str, Any]]:
    html = fetch_html(page_url)
    parser = PdfLinkExtractor(base_url=page_url)
    parser.feed(html)

    selected = choose_links(parser.links, keywords)
    seen_urls: set[str] = set()
    downloaded: list[dict[str, Any]] = []

    for index, link in enumerate(selected, start=1):
        if link.url in seen_urls:
            continue
        seen_urls.add(link.url)

        event_title = safe_name(event_title_override or link.title, fallback="untitled_event")
        folder = output_root / event_title / category_subfolder

        parsed_url = urlparse(link.url)
        url_filename = Path(unquote(parsed_url.path)).name
        base_filename = url_filename or link.text or f"file_{index}"
        pdf_filename = safe_pdf_filename(base_filename, fallback=f"file_{index}")
        destination = folder / pdf_filename

        download_pdf(link.url, destination)
        downloaded.append(
            {
                "event_title": event_title,
                "pdf_url": link.url,
                "link_text": link.text,
                "saved_to": str(destination),
            }
        )

    return downloaded


def start_scraper(
    page_url: str,
    output_root: str | Path = "scraped",
    event_title: str | None = None,
) -> list[dict[str, Any]]:
    return scrape_and_download(
        page_url,
        Path(output_root),
        START_KEYWORDS,
        category_subfolder="startlists",
        event_title_override=event_title,
    )


def start_scaraper(
    page_url: str,
    output_root: str | Path = "scraped",
    event_title: str | None = None,
) -> list[dict[str, Any]]:
    # Backward-compatible alias for typo in earlier naming.
    return start_scraper(page_url, output_root, event_title=event_title)


def result_scraper(
    page_url: str,
    output_root: str | Path = "scraped",
    event_title: str | None = None,
) -> list[dict[str, Any]]:
    return scrape_and_download(
        page_url,
        Path(output_root),
        RESULT_KEYWORDS,
        category_subfolder="results",
        event_title_override=event_title,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape startlist/result PDF links from event pages and download PDFs "
            "into title-based folders"
        )
    )
    parser.add_argument("--start-url", type=str, help="Page URL that contains startlist PDF links")
    parser.add_argument("--result-url", type=str, help="Page URL that contains result PDF links")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("scraped"),
        help="Base output directory; files are saved under <base>/<event_title>/startlists|results",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to save JSON summary",
    )
    parser.add_argument(
        "--event-title",
        type=str,
        default=None,
        help="Optional fixed event title for output folder naming",
    )
    args = parser.parse_args()

    if not args.start_url and not args.result_url:
        parser.error("Provide at least one of --start-url or --result-url")

    payload: dict[str, Any] = {"startlists": [], "results": []}

    try:
        if args.start_url:
            payload["startlists"] = start_scraper(
                args.start_url,
                args.output_dir,
                event_title=args.event_title,
            )

        if args.result_url:
            payload["results"] = result_scraper(
                args.result_url,
                args.output_dir,
                event_title=args.event_title,
            )

    except Exception as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error

    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Startlist PDFs downloaded: {len(payload['startlists'])}")
    print(f"Result PDFs downloaded: {len(payload['results'])}")
    if args.output_json:
        print(f"Saved summary JSON to: {args.output_json}")


if __name__ == "__main__":
    main()
