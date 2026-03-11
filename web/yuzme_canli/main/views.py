import csv
import io
import json
import os

from django.db.models import Count, Max, Q
from django.http import HttpRequest, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.csrf import csrf_exempt

from .event_creator import get_or_create_event_from_url
from .ingest import ingest_event_rows, ingest_rows
from .models import Event, Result

# Create your views here.
def home(request):
    event_count = Event.objects.count()
    result_count = Result.objects.count()
    latest_events = Event.objects.order_by("-date", "title")[:5]

    context = {
        "event_count": event_count,
        "result_count": result_count,
        "latest_events": latest_events,
    }
    return render(request, "home.html", context)

def event_list(request):
    events = Event.objects.order_by("-date", "title")
    return render(request, "event_list.html", {"events": events})


def event_detail(request, event_id):
    event = get_object_or_404(Event, id=event_id)
    event_results = Result.objects.filter(event=event)
    distinct_groups = list(
        event_results
        .values("event_order", "swimming_style", "gender", "distance")
        .annotate(
            result_count=Count("id"),
            non_empty_result_count=Count(
                "id",
                filter=Q(result__isnull=False) & ~Q(result__exact=""),
            ),
            max_seri_no=Max("seri_no"),
        )
        .order_by("event_order", "gender", "distance")
    )

    context = {
        "event": event,
        "distinct_groups": distinct_groups,
        "group_count": len(distinct_groups),
        "result_count": sum(int(group["result_count"]) for group in distinct_groups),
        "non_empty_result_count": sum(
            int(group["non_empty_result_count"]) for group in distinct_groups
        ),
    }
    return render(request, "event_detail.html", context)


def event_selected_results(request, event_id):
    event = get_object_or_404(Event, id=event_id)

    selected_style = str(request.GET.get("style") or "").strip()
    selected_gender = str(request.GET.get("gender") or "").strip()
    selected_distance_raw = str(request.GET.get("distance") or "").strip()

    if not selected_style or not selected_gender or not selected_distance_raw:
        return redirect("event_detail", event_id=event.id)

    try:
        selected_distance = int(selected_distance_raw)
    except ValueError:
        return redirect("event_detail", event_id=event.id)

    filtered_results = Result.objects.filter(
        event=event,
        swimming_style=selected_style,
        gender=selected_gender,
        distance=selected_distance,
    ).order_by("rank","seri_no", "lane", "swimmer_name")

    context = {
        "event": event,
        "selected_style": selected_style,
        "selected_gender": selected_gender,
        "selected_distance": selected_distance,
        "results": filtered_results,
        "result_count": filtered_results.count(),
    }
    return render(request, "event_selected_results.html", context)


def swimmer_results(request):
    events = Event.objects.order_by("-date", "title")
    selected_event_id = str(request.GET.get("event_id") or "").strip()
    selected_swimmer = str(request.GET.get("swimmer") or "").strip()

    swimmer_source = Result.objects.exclude(swimmer_name__exact="")
    if selected_event_id.isdigit():
        swimmer_source = swimmer_source.filter(event_id=int(selected_event_id))

    swimmers = (
        swimmer_source.values_list("swimmer_name", flat=True)
        .distinct()
        .order_by("swimmer_name")
    )

    filtered_results = Result.objects.select_related("event").order_by(
        "swimmer_name", "swimming_style", "distance", "seri_no", "lane"
    )
    if selected_event_id.isdigit():
        filtered_results = filtered_results.filter(event_id=int(selected_event_id))
    if selected_swimmer:
        filtered_results = filtered_results.filter(swimmer_name=selected_swimmer)

    context = {
        "events": events,
        "swimmers": swimmers,
        "selected_event_id": selected_event_id,
        "selected_swimmer": selected_swimmer,
        "results": filtered_results,
        "result_count": filtered_results.count(),
    }
    return render(request, "swimmer_results.html", context)


def club_select(request: HttpRequest):
    if request.method == "POST":
        action = str(request.POST.get("action") or "").strip().lower()
        selected = str(request.POST.get("club") or "").strip()

        if action == "clear":
            request.session.pop("selected_club", None)
        elif selected:
            request.session["selected_club"] = selected

        request.session.modified = True
        return redirect("club_select")

    clubs = (
        Result.objects.exclude(club__isnull=True)
        .exclude(club__exact="")
        .values_list("club", flat=True)
        .distinct()
        .order_by("club")
    )

    selected_club = str(request.session.get("selected_club") or "").strip()

    club_result_count = 0
    club_event_count = 0
    latest_club_rows = []
    if selected_club:
        club_result_qs = Result.objects.filter(club=selected_club).select_related("event")
        club_result_count = club_result_qs.count()
        club_event_count = Event.objects.filter(result__club=selected_club).distinct().count()
        latest_club_rows = club_result_qs.order_by("-event__date", "event__title", "swimmer_name","swimming_style", "distance")[:50]

    context = {
        "clubs": clubs,
        "selected_club": selected_club,
        "club_result_count": club_result_count,
        "club_event_count": club_event_count,
        "latest_club_rows": latest_club_rows,
    }
    return render(request, "club_select.html", context)


def create_event_page(request: HttpRequest):
    context: dict[str, object] = {
        "submitted_url": "",
        "created": None,
        "event": None,
        "scraped": None,
        "error": "",
    }

    if request.method == "POST":
        event_url = str(request.POST.get("url") or "").strip()
        context["submitted_url"] = event_url

        if not event_url:
            context["error"] = "Lutfen etkinlik URL girin."
        else:
            try:
                event, created, scraped = get_or_create_event_from_url(event_url)
                context["created"] = created
                context["event"] = event
                context["scraped"] = scraped
            except Exception as error:
                context["error"] = str(error)

    return render(request, "create_event.html", context)


def _extract_rows_from_csv_text(csv_text: str) -> list[dict[str, str]]:
    reader = csv.DictReader(io.StringIO(csv_text))
    return list(reader)


@csrf_exempt
def ingest_results(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"error": "Only POST is allowed"}, status=405)

    required_token = os.getenv("YUZME_INGEST_TOKEN", "").strip()
    provided_token = request.headers.get("X-Ingest-Token", "").strip()
    if required_token and provided_token != required_token:
        return JsonResponse({"error": "Unauthorized"}, status=403)

    dry_run = request.GET.get("dry_run") == "1"

    try:
        rows: list[dict[str, object]]

        uploaded = request.FILES.get("file")
        if uploaded is not None:
            payload = uploaded.read().decode("utf-8-sig", errors="replace")
            if uploaded.name.lower().endswith(".json"):
                parsed = json.loads(payload)
                if isinstance(parsed, dict) and isinstance(parsed.get("rows"), list):
                    rows = parsed["rows"]
                elif isinstance(parsed, list):
                    rows = parsed
                else:
                    return JsonResponse({"error": "JSON must be a list or {\"rows\": [...] }"}, status=400)
            else:
                rows = _extract_rows_from_csv_text(payload)
        else:
            content_type = (request.content_type or "").lower()
            body_text = request.body.decode("utf-8-sig", errors="replace")

            if "application/json" in content_type or body_text.lstrip().startswith("[") or body_text.lstrip().startswith("{"):
                parsed = json.loads(body_text)
                if isinstance(parsed, dict) and isinstance(parsed.get("rows"), list):
                    rows = parsed["rows"]
                elif isinstance(parsed, list):
                    rows = parsed
                else:
                    return JsonResponse({"error": "JSON must be a list or {\"rows\": [...] }"}, status=400)
            else:
                rows = _extract_rows_from_csv_text(body_text)

        summary = ingest_rows(rows, dry_run=dry_run)
        return JsonResponse({"ok": True, "dry_run": dry_run, "summary": summary})

    except json.JSONDecodeError as error:
        return JsonResponse({"error": f"Invalid JSON: {error}"}, status=400)
    except Exception as error:
        return JsonResponse({"error": str(error)}, status=400)


@csrf_exempt
def ingest_events(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"error": "Only POST is allowed"}, status=405)

    required_token = os.getenv("YUZME_INGEST_TOKEN", "").strip()
    provided_token = request.headers.get("X-Ingest-Token", "").strip()
    if required_token and provided_token != required_token:
        return JsonResponse({"error": "Unauthorized"}, status=403)

    dry_run = request.GET.get("dry_run") == "1"

    try:
        rows: list[dict[str, object]]

        uploaded = request.FILES.get("file")
        if uploaded is not None:
            payload = uploaded.read().decode("utf-8-sig", errors="replace")
            if uploaded.name.lower().endswith(".json"):
                parsed = json.loads(payload)
                if isinstance(parsed, dict) and isinstance(parsed.get("rows"), list):
                    rows = parsed["rows"]
                elif isinstance(parsed, list):
                    rows = parsed
                else:
                    return JsonResponse({"error": "JSON must be a list or {\"rows\": [...] }"}, status=400)
            else:
                rows = _extract_rows_from_csv_text(payload)
        else:
            content_type = (request.content_type or "").lower()
            body_text = request.body.decode("utf-8-sig", errors="replace")

            if "application/json" in content_type or body_text.lstrip().startswith("[") or body_text.lstrip().startswith("{"):
                parsed = json.loads(body_text)
                if isinstance(parsed, dict) and isinstance(parsed.get("rows"), list):
                    rows = parsed["rows"]
                elif isinstance(parsed, list):
                    rows = parsed
                else:
                    return JsonResponse({"error": "JSON must be a list or {\"rows\": [...] }"}, status=400)
            else:
                rows = _extract_rows_from_csv_text(body_text)

        summary = ingest_event_rows(rows, dry_run=dry_run)
        return JsonResponse({"ok": True, "dry_run": dry_run, "summary": summary})

    except json.JSONDecodeError as error:
        return JsonResponse({"error": f"Invalid JSON: {error}"}, status=400)
    except Exception as error:
        return JsonResponse({"error": str(error)}, status=400)


@csrf_exempt
def create_event_from_url(request: HttpRequest) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"error": "Only POST is allowed"}, status=405)

    try:
        payload: dict[str, object] = {}
        if request.content_type and "application/json" in request.content_type.lower():
            payload = json.loads(request.body.decode("utf-8-sig", errors="replace") or "{}")
            if not isinstance(payload, dict):
                return JsonResponse({"error": "JSON body must be an object"}, status=400)

        event_url = str(payload.get("url") if payload else request.POST.get("url") or "").strip()
        if not event_url:
            return JsonResponse({"error": "url is required"}, status=400)

        event, created, scraped = get_or_create_event_from_url(event_url)
        return JsonResponse(
            {
                "ok": True,
                "created": created,
                "event": {
                    "id": event.id,
                    "unique_name": event.unique_name,
                    "title": event.title,
                    "date": event.date,
                    "location": event.location,
                },
                "scraped": scraped,
            }
        )
    except json.JSONDecodeError as error:
        return JsonResponse({"error": f"Invalid JSON: {error}"}, status=400)
    except Exception as error:
        return JsonResponse({"error": str(error)}, status=400)
