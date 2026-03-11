from django.test import TestCase
from django.urls import reverse
from unittest.mock import patch

from .event_creator import get_or_create_event_from_url, scrape_event_detail
from .models import Event, Result


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


class IngestEventsApiTests(TestCase):
	def test_ingest_events_json_creates_event(self):
		payload = [
			{
				"event_title_folder": "10 Yas Yuzme Gelisim Musabakasi",
				"event_date": "13. - 14.12.2025",
				"event_location": "ANKARA (TUR)",
			},
			{
				"event_title": "10 Yas Yuzme Gelisim Musabakasi",
				"event_date": "13. - 14.12.2025",
				"location": "ANKARA (TUR)",
			},
		]

		response = self.client.post(
			reverse("ingest_events"),
			data=payload,
			content_type="application/json",
		)

		self.assertEqual(response.status_code, 200)
		data = response.json()
		self.assertTrue(data["ok"])
		self.assertEqual(data["summary"]["events_seen"], 1)
		self.assertEqual(data["summary"]["event_created"], 1)
		self.assertEqual(Event.objects.count(), 1)

	def test_ingest_events_dry_run_does_not_write(self):
		payload = [
			{
				"event_title_folder": "Okul Sporlari Kucukler",
				"event_date": "10. - 11.01.2026",
				"event_location": "ANKARA (TUR)",
			}
		]

		response = self.client.post(
			f"{reverse('ingest_events')}?dry_run=1",
			data=payload,
			content_type="application/json",
		)

		self.assertEqual(response.status_code, 200)
		data = response.json()
		self.assertTrue(data["ok"])
		self.assertTrue(data["dry_run"])
		self.assertEqual(data["summary"]["events_seen"], 1)
		self.assertEqual(Event.objects.count(), 0)


class IngestResultsEventLinkTests(TestCase):
	def test_ingest_results_uses_event_unique_name(self):
		event = Event.objects.create(
			unique_name="cs-1004952",
			title="10 Yas Yuzme Gelisim Musabakasi",
			date="13. - 14.12.2025",
			location="ANKARA (TUR)",
		)

		payload = [
			{
				"event_unique_name": "cs-1004952",
				"swimmer_name": "Test Swimmer",
				"year_of_birth": 2014,
				"gender": "Erkek",
				"club": "Test Club",
				"swimming_style": "Serbest",
				"distance": "50m",
				"seri_no": 1,
				"lane": 4,
				"seed": "00:32.00",
				"result": "00:31.80",
				"rank": 1,
			}
		]

		response = self.client.post(
			reverse("ingest_results"),
			data=payload,
			content_type="application/json",
		)

		self.assertEqual(response.status_code, 200)
		data = response.json()
		self.assertTrue(data["ok"])
		self.assertEqual(data["summary"]["events_seen"], 1)
		self.assertEqual(data["summary"]["event_created"], 0)
		self.assertEqual(data["summary"]["result_created"], 1)
		self.assertEqual(Result.objects.count(), 1)
		self.assertEqual(Result.objects.first().event_id, event.id)
		self.assertTrue(str(Result.objects.first().startlist_unique_name).startswith("cs-1004952-"))

	def test_ingest_results_rejects_missing_event_reference(self):
		payload = [
			{
				"swimmer_name": "No Event",
				"year_of_birth": 2013,
				"club": "Club",
				"swimming_style": "Serbest",
				"distance": "50m",
				"seri_no": 1,
				"lane": 2,
			}
		]

		response = self.client.post(
			reverse("ingest_results"),
			data=payload,
			content_type="application/json",
		)

		self.assertEqual(response.status_code, 400)
		data = response.json()
		self.assertIn("event_id or event_unique_name", str(data.get("error") or ""))

	def test_ingest_results_generates_incremental_startlist_keys(self):
		Event.objects.create(
			unique_name="cs-1004952",
			title="10 Yas Yuzme Gelisim Musabakasi",
			date="13. - 14.12.2025",
			location="ANKARA (TUR)",
		)

		payload = [
			{
				"event_unique_name": "cs-1004952",
				"swimmer_name": "Swimmer One",
				"year_of_birth": 2014,
				"gender": "Erkek",
				"club": "Club A",
				"swimming_style": "Serbest",
				"distance": "50m",
				"seri_no": 1,
				"lane": 4,
				"seed": "00:32.00",
				"result": "00:31.80",
				"rank": 1,
			},
			{
				"event_unique_name": "cs-1004952",
				"swimmer_name": "Swimmer Two",
				"year_of_birth": 2014,
				"gender": "Erkek",
				"club": "Club A",
				"swimming_style": "Serbest",
				"distance": "50m",
				"seri_no": 1,
				"lane": 5,
				"seed": "00:33.00",
				"result": "00:32.10",
				"rank": 2,
			},
		]

		response = self.client.post(
			reverse("ingest_results"),
			data=payload,
			content_type="application/json",
		)

		self.assertEqual(response.status_code, 200)
		keys = list(Result.objects.order_by("lane").values_list("startlist_unique_name", flat=True))
		self.assertEqual(keys, ["cs-1004952-000001", "cs-1004952-000002"])

	def test_ingest_results_updates_by_startlist_unique_name(self):
		event = Event.objects.create(
			unique_name="cs-1004952",
			title="10 Yas Yuzme Gelisim Musabakasi",
			date="13. - 14.12.2025",
			location="ANKARA (TUR)",
		)
		Result.objects.create(
			event=event,
			startlist_unique_name="cs-1004952-000123",
			event_order=1,
			swimmer_name="Swimmer Three",
			year_of_birth=2014,
			gender="Erkek",
			club="Club A",
			swimming_style="Serbest",
			distance=50,
			seri_no=1,
			lane=6,
			seed="00:35.00",
			result="00:34.50",
			rank=3,
		)

		payload = [
			{
				"event_unique_name": "cs-1004952",
				"startlist_unique_name": "cs-1004952-000123",
				"swimmer_name": "Swimmer Three",
				"year_of_birth": 2014,
				"gender": "Erkek",
				"club": "Club A",
				"swimming_style": "Serbest",
				"distance": "50m",
				"seri_no": 1,
				"lane": 6,
				"seed": "00:35.00",
				"result": "00:33.90",
				"rank": 1,
			},
		]

		response = self.client.post(
			reverse("ingest_results"),
			data=payload,
			content_type="application/json",
		)

		self.assertEqual(response.status_code, 200)
		self.assertEqual(Result.objects.count(), 1)
		updated = Result.objects.get(startlist_unique_name="cs-1004952-000123")
		self.assertEqual(updated.result, "00:33.90")
		self.assertEqual(updated.rank, 1)
