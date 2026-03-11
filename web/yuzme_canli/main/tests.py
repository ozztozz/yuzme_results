from django.test import TestCase
from django.urls import reverse
from unittest.mock import patch

from .event_creator import get_or_create_event_from_url, scrape_event_detail
from .models import Event


HTML_SAMPLE = """
<!DOCTYPE html>
<html>
<body>
  <div id="header">
	<table>
	  <tr><td>10 Yas Yuzme Gelisim Musabakasi</td><td align="right">Uzun Kulvar (50m)</td></tr>
	  <tr><td>ANKARA (TUR)</td><td align="right">13. - 14.12.2025</td></tr>
	</table>
  </div>
</body>
</html>
"""


class _DummyHeaders:
	def get_content_charset(self):
		return "utf-8"


class _DummyResponse:
	def __init__(self, body: bytes):
		self._body = body
		self.headers = _DummyHeaders()

	def read(self) -> bytes:
		return self._body

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc, tb):
		return False


class EventCreatorTests(TestCase):
	@patch("main.event_creator.urlopen")
	def test_scrape_event_detail(self, mock_urlopen):
		mock_urlopen.return_value = _DummyResponse(HTML_SAMPLE.encode("utf-8"))

		details = scrape_event_detail("https://canli.tyf.gov.tr/ankara/cs-1004952/")

		self.assertEqual(details["unique_name"], "cs-1004952")
		self.assertEqual(details["title"], "10 Yas Yuzme Gelisim Musabakasi")
		self.assertEqual(details["location"], "ANKARA (TUR)")
		self.assertEqual(details["date"], "13. - 14.12.2025")

	@patch("main.event_creator.urlopen")
	def test_get_or_create_event_by_unique_name(self, mock_urlopen):
		mock_urlopen.return_value = _DummyResponse(HTML_SAMPLE.encode("utf-8"))

		url = "https://canli.tyf.gov.tr/ankara/cs-1004952/"
		event1, created1, _ = get_or_create_event_from_url(url)
		event2, created2, _ = get_or_create_event_from_url(url)

		self.assertTrue(created1)
		self.assertFalse(created2)
		self.assertEqual(event1.id, event2.id)
		self.assertEqual(Event.objects.filter(unique_name="cs-1004952").count(), 1)


class CreateEventPageTests(TestCase):
	def test_create_event_page_get(self):
		response = self.client.get(reverse("create_event_page"))
		self.assertEqual(response.status_code, 200)
		self.assertContains(response, "URL ile Etkinlik Ekle")

	@patch("main.event_creator.urlopen")
	def test_create_event_page_post(self, mock_urlopen):
		mock_urlopen.return_value = _DummyResponse(HTML_SAMPLE.encode("utf-8"))

		response = self.client.post(
			reverse("create_event_page"),
			{"url": "https://canli.tyf.gov.tr/ankara/cs-1004952/"},
		)

		self.assertEqual(response.status_code, 200)
		self.assertContains(response, "Etkinlik eklendi")
		self.assertEqual(Event.objects.filter(unique_name="cs-1004952").count(), 1)
