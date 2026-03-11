from __future__ import annotations

import html
import re
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .models import Event


UNIQUE_NAME_RE = re.compile(r"^[a-z]{2,}-\d+$", re.IGNORECASE)


def _clean_html_text(value: str) -> str:
	text = re.sub(r"<[^>]+>", " ", value)
	text = html.unescape(text)
	text = text.replace("\xa0", " ")
	return " ".join(text.split()).strip()


def extract_unique_name(event_url: str) -> str:
	parsed = urlparse(str(event_url or "").strip())
	segments = [segment for segment in parsed.path.split("/") if segment]
	if not segments:
		raise ValueError("Event URL path is empty")

	unique_name = segments[-1]
	if not UNIQUE_NAME_RE.match(unique_name):
		raise ValueError(f"Could not extract valid unique_name from URL: {event_url}")
	return unique_name


def scrape_event_detail(event_url: str) -> dict[str, str]:
	unique_name = extract_unique_name(event_url)
	request = Request(event_url, headers={"User-Agent": "Mozilla/5.0"})

	with urlopen(request, timeout=20) as response:
		charset = response.headers.get_content_charset() or "utf-8"
		html_text = response.read().decode(charset, errors="replace")

	header_match = re.search(r"<div[^>]+id=[\"']header[\"'][^>]*>(.*?)</div>", html_text, re.IGNORECASE | re.DOTALL)
	if not header_match:
		raise ValueError("Could not find header section in event page")

	header_html = header_match.group(1)
	td_values = re.findall(r"<td[^>]*>(.*?)</td>", header_html, re.IGNORECASE | re.DOTALL)
	if len(td_values) < 4:
		raise ValueError("Could not parse title/location/date from event header")

	title = _clean_html_text(td_values[0])
	location = _clean_html_text(td_values[2])
	date_text = _clean_html_text(td_values[3])

	if not title or not location or not date_text:
		raise ValueError("Parsed event detail fields are empty")

	return {
		"unique_name": unique_name,
		"title": title,
		"date": date_text,
		"location": location,
	}


def get_or_create_event_from_url(event_url: str) -> tuple[Event, bool, dict[str, Any]]:
	scraped = scrape_event_detail(event_url)
	event, created = Event.objects.get_or_create(
		unique_name=scraped["unique_name"],
		defaults={
			"title": scraped["title"],
			"date": scraped["date"],
			"location": scraped["location"],
		},
	)
	return event, created, scraped
