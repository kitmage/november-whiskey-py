import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import msal
import requests
from dotenv import load_dotenv

load_dotenv()

# =========================
# Required environment vars
# =========================
TENANT_ID = os.environ["TENANT_ID"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
ROB_ID = os.environ["ROB_ID"]
TOM_ID = os.environ["TOM_ID"]
MIKE_ID = os.environ["MIKE_ID"]

# =========================
# Microsoft Graph settings
# =========================
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["https://graph.microsoft.com/.default"]
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
GRAPH_TIMEZONE = "Pacific Standard Time"

# =========================
# Local/business settings
# =========================
LOCAL_TZ = ZoneInfo("America/Los_Angeles")

USERS = [
    TOM_ID,
    ROB_ID,
    MIKE_ID,
]

BOOKING_WINDOW_START_HOURS = 144
BOOKING_WINDOW_END_HOURS = 240

BUSINESS_DAY_START_HOUR = 10   # 10:00 AM
BUSINESS_DAY_END_HOUR = 16     # 4:00 PM

LUNCH_BREAK_START_HOUR = 11
LUNCH_BREAK_START_MINUTE = 30
LUNCH_BREAK_END_HOUR = 13
LUNCH_BREAK_END_MINUTE = 0

INTERVAL_MINUTES = 30
FRIDAY_AFTERNOON_START_HOUR = 12  # 12:00 PM


def get_access_token() -> str:
    """Acquire an app-only Microsoft Graph token using client credentials."""
    app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET,
    )

    result = app.acquire_token_for_client(scopes=SCOPES)

    if "access_token" not in result:
        raise RuntimeError(f"Could not acquire token: {result}")

    return result["access_token"]


def ceil_to_interval(dt: datetime, interval_minutes: int) -> datetime:
    """Round a datetime up to the next slot boundary used by Graph availabilityView."""
    dt = dt.replace(second=0, microsecond=0)

    minutes_past_interval = dt.minute % interval_minutes
    if minutes_past_interval == 0:
        return dt

    minutes_to_add = interval_minutes - minutes_past_interval
    return dt + timedelta(minutes=minutes_to_add)


def build_search_window() -> tuple[datetime, datetime]:
    """
    Compute the local search window for booking and return naive datetimes.

    Returned values are naive by design because Graph receives an explicit
    `timeZone` in the request payload (`GRAPH_TIMEZONE`).
    """
    now_local = datetime.now(LOCAL_TZ)

    raw_start_dt = now_local + timedelta(hours=BOOKING_WINDOW_START_HOURS)
    raw_end_dt = now_local + timedelta(hours=BOOKING_WINDOW_END_HOURS)

    start_dt = ceil_to_interval(raw_start_dt, INTERVAL_MINUTES)
    end_dt = ceil_to_interval(raw_end_dt, INTERVAL_MINUTES)

    if end_dt <= start_dt:
        raise ValueError("BOOKING_WINDOW_END_HOURS must be greater than BOOKING_WINDOW_START_HOURS")

    return start_dt.replace(tzinfo=None), end_dt.replace(tzinfo=None)


def call_get_schedule(
    access_token: str,
    anchor_user: str,
    schedules: list[str],
    start_dt: datetime,
    end_dt: datetime,
    interval_minutes: int,
) -> dict:
    """Call Graph `getSchedule` for all users using one anchor mailbox endpoint."""
    url = f"{GRAPH_BASE}/users/{anchor_user}/calendar/getSchedule"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Prefer": f'outlook.timezone="{GRAPH_TIMEZONE}"',
    }

    payload = {
        "schedules": schedules,
        "startTime": {
            "dateTime": start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "timeZone": GRAPH_TIMEZONE,
        },
        "endTime": {
            "dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "timeZone": GRAPH_TIMEZONE,
        },
        "availabilityViewInterval": interval_minutes,
    }

    # `availabilityViewInterval` controls the granularity of the response string.
    response = requests.post(url, headers=headers, json=payload, timeout=30)

    if response.status_code != 200:
        raise RuntimeError(f"Graph call failed: {response.status_code}\n{response.text}")

    return response.json()


def mutual_free_slots(
    schedule_response: dict,
    start_dt: datetime,
    interval_minutes: int,
) -> list[tuple[datetime, datetime]]:
    """
    Convert Graph availability strings into contiguous ranges where everyone is free.

    Graph returns one character per interval (`0` means free). We align all user
    strings by index and keep only intervals where every user has `0`.
    """
    values = schedule_response.get("value", [])
    if not values:
        raise ValueError("Expected at least one schedule result.")

    availability_strings = []
    for entry in values:
        availability_view = entry.get("availabilityView")
        if availability_view is None:
            raise ValueError(f"Missing availabilityView for {entry.get('scheduleId')}")
        availability_strings.append(availability_view)

    # Use the shortest string defensively in case Graph returns uneven lengths.
    slot_count = min(len(s) for s in availability_strings)

    free_ranges: list[tuple[datetime, datetime]] = []
    current_start: datetime | None = None

    for i in range(slot_count):
        slot_chars = [s[i] for s in availability_strings]
        everyone_free = all(char == "0" for char in slot_chars)

        slot_start = start_dt + timedelta(minutes=i * interval_minutes)

        if everyone_free:
            if current_start is None:
                current_start = slot_start
        else:
            if current_start is not None:
                free_ranges.append((current_start, slot_start))
                current_start = None

    if current_start is not None:
        final_end = start_dt + timedelta(minutes=slot_count * interval_minutes)
        free_ranges.append((current_start, final_end))

    return free_ranges


def filter_to_business_hours(
    free_slots: list[tuple[datetime, datetime]],
    start_hour: int,
    end_hour: int,
    interval_minutes: int,
    lunch_start_hour: int,
    lunch_start_minute: int,
    lunch_end_hour: int,
    lunch_end_minute: int,
) -> list[tuple[datetime, datetime]]:
    """Filter free ranges down to weekday business slots, excluding lunch overlap."""
    filtered: list[tuple[datetime, datetime]] = []

    for range_start, range_end in free_slots:
        current = range_start

        while current < range_end:
            next_slot = min(current + timedelta(minutes=interval_minutes), range_end)

            is_weekday = current.weekday() < 5
            is_in_business_hours = start_hour <= current.hour < end_hour

            lunch_start = current.replace(
                hour=lunch_start_hour,
                minute=lunch_start_minute,
                second=0,
                microsecond=0,
            )
            lunch_end = current.replace(
                hour=lunch_end_hour,
                minute=lunch_end_minute,
                second=0,
                microsecond=0,
            )

            overlaps_lunch = current < lunch_end and next_slot > lunch_start

            if is_weekday and is_in_business_hours and not overlaps_lunch:
                filtered.append((current, next_slot))

            current = next_slot

    return filtered


def expand_slots_to_scored_starts(
    slots: list[tuple[datetime, datetime]],
    interval_minutes: int,
) -> list[dict]:
    """
    Score each possible start by surrounding free-space buffer.

    Higher score means more contiguous free blocks on both sides, favoring starts
    that are less likely to collide with adjacent meetings.
    """
    starts: list[datetime] = []

    for start, end in slots:
        current = start
        while current < end:
            starts.append(current)
            current += timedelta(minutes=interval_minutes)

    if not starts:
        return []

    interval = timedelta(minutes=interval_minutes)
    scored: list[dict] = []

    for i, current in enumerate(starts):
        buffer_before = 0
        j = i - 1
        while j >= 0 and starts[j + 1] - starts[j] == interval:
            buffer_before += 1
            j -= 1

        buffer_after = 0
        j = i + 1
        while j < len(starts) and starts[j] - starts[j - 1] == interval:
            buffer_after += 1
            j += 1

        score = min(buffer_before, buffer_after)

        scored.append({
            "start": current,
            "score": score,
            "buffer_before_blocks": buffer_before,
            "buffer_after_blocks": buffer_after,
        })

    return scored


def filter_out_friday_afternoons(
    slots: list[tuple[datetime, datetime]],
    friday_afternoon_start_hour: int,
) -> list[tuple[datetime, datetime]]:
    """Remove slots that start on Friday afternoon before scoring."""
    filtered_slots: list[tuple[datetime, datetime]] = []
    for slot_start, slot_end in slots:
        is_friday_afternoon = slot_start.weekday() == 4 and slot_start.hour >= friday_afternoon_start_hour
        if not is_friday_afternoon:
            filtered_slots.append((slot_start, slot_end))
    return filtered_slots


def select_earliest_best_start(scored_starts: list[dict]) -> dict | None:
    """Pick the highest-scoring start; break ties by earliest datetime."""
    if not scored_starts:
        return None

    best = max(scored_starts, key=lambda x: x["score"])
    best_score = best["score"]

    best_candidates = [s for s in scored_starts if s["score"] == best_score]
    earliest_best = min(best_candidates, key=lambda x: x["start"])

    return {
        "start": earliest_best["start"].isoformat(),
        "score": earliest_best["score"],
        "buffer_before_blocks": earliest_best["buffer_before_blocks"],
        "buffer_after_blocks": earliest_best["buffer_after_blocks"],
    }


def main():
    if len(USERS) < 2:
        raise ValueError("USERS must contain at least two users for comparison.")

    search_start, search_end = build_search_window()
    token = get_access_token()
    anchor_user = USERS[0]

    # 1) Pull raw schedule availability from Graph.
    graph_response = call_get_schedule(
        access_token=token,
        anchor_user=anchor_user,
        schedules=USERS,
        start_dt=search_start,
        end_dt=search_end,
        interval_minutes=INTERVAL_MINUTES,
    )

    # 2) Keep only intervals where all selected users are simultaneously free.
    free_slots = mutual_free_slots(
        schedule_response=graph_response,
        start_dt=search_start,
        interval_minutes=INTERVAL_MINUTES,
    )

    # 3) Restrict to bookable business hours and remove lunch-time overlap.
    free_slots = filter_to_business_hours(
        free_slots=free_slots,
        start_hour=BUSINESS_DAY_START_HOUR,
        end_hour=BUSINESS_DAY_END_HOUR,
        interval_minutes=INTERVAL_MINUTES,
        lunch_start_hour=LUNCH_BREAK_START_HOUR,
        lunch_start_minute=LUNCH_BREAK_START_MINUTE,
        lunch_end_hour=LUNCH_BREAK_END_HOUR,
        lunch_end_minute=LUNCH_BREAK_END_MINUTE,
    )

    # 4) Exclude Friday afternoon starts before scoring candidate options.
    free_slots = filter_out_friday_afternoons(
        slots=free_slots,
        friday_afternoon_start_hour=FRIDAY_AFTERNOON_START_HOUR,
    )

    # 5) Score candidate starts by buffer on each side, then choose best.
    scored_starts = expand_slots_to_scored_starts(
        slots=free_slots,
        interval_minutes=INTERVAL_MINUTES,
    )

    best_start = select_earliest_best_start(scored_starts)

    print(json.dumps({"best_start_time": best_start}, indent=2))


if __name__ == "__main__":
    main()
