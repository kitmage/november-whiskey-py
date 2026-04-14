#!/usr/bin/env python3
"""
create_mike_event.py

Fetches best_start_time from availability.py and creates a calendar event
directly on Mike's Outlook calendar.

Example:
  python3 create_mike_event.py \
    --signal-input signal.json \
    --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests


GRAPH_ROOT = "https://graph.microsoft.com/v1.0"
TOKEN_URL_TMPL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
DEFAULT_TIMEZONE = "Pacific Standard Time"
DEFAULT_DURATION_MINUTES = 30
DEFAULT_INTER_EVENT_DELAY_SECONDS = 1.0
DEFAULT_SUBJECT_TEMPLATE = "30min Meeting - {customer_name}"
DEFAULT_DEBUG_LOG_PATH = "create_mike_event.log"
SCRIPT_DIR = Path(__file__).resolve().parent
SIGNAL_FINDER_PATH = SCRIPT_DIR / "signal_finder.py"
AVAILABILITY_PATH = SCRIPT_DIR / "availability.py"

LOGGER = logging.getLogger("create_mike_event")


class GraphError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    """Parse event metadata and execution mode flags for Graph event creation."""
    parser = argparse.ArgumentParser(description="Create an Outlook calendar event on Mike's calendar.")
    parser.add_argument(
        "--input",
        help="Optional path to JSON file from availability.py. If omitted, availability.py is executed.",
    )
    parser.add_argument("--signal-input", help="Optional path to JSON output from signal_finder.py.")
    parser.add_argument("--customer-name", default="")
    parser.add_argument("--customer-email", default="")
    parser.add_argument("--customer-phone", default="")
    parser.add_argument("--customer-notes", default="")
    parser.add_argument("--subject", default="")
    parser.add_argument("--location", default="Microsoft Teams")
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--duration-minutes", type=int, default=DEFAULT_DURATION_MINUTES)
    parser.add_argument("--inter-event-delay-seconds", type=float, default=DEFAULT_INTER_EVENT_DELAY_SECONDS)
    parser.add_argument("--debug", action="store_true", help="Enable persistent debug logging to a local log file.")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def configure_logging(debug: bool, log_path: str = DEFAULT_DEBUG_LOG_PATH) -> None:
    """Configure persistent file logging when debug mode is enabled."""
    LOGGER.handlers.clear()
    LOGGER.propagate = False

    if not debug:
        LOGGER.setLevel(logging.CRITICAL)
        return

    LOGGER.setLevel(logging.DEBUG)
    handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.debug("Debug logging enabled.")


def load_env(name: str) -> str:
    """Read a required env var and fail fast with a clear message if missing."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def read_input_json(path: Optional[str]) -> Dict[str, Any]:
    """Load availability payload from file."""
    if path:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    raise RuntimeError("--input path is required when reading from a file.")


def _as_contact_payload(obj: Any) -> Optional[Dict[str, Any]]:
    """Return object as contact payload when it looks like signal_finder output."""
    if isinstance(obj, dict) and ("email" in obj or "fullName" in obj):
        return obj
    return None


def load_signal_contacts(path: Optional[str]) -> List[Dict[str, Any]]:
    """
    Load signal contacts from a file path.

    Supports:
      - Single JSON object
      - JSON array of objects
      - NDJSON (one JSON object per line)
    """
    if not path:
        return []

    with open(path, "r", encoding="utf-8") as f:
        raw = f.read().strip()

    if not raw:
        return []

    # Try standard JSON first.
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [item for item in parsed if _as_contact_payload(item)]
        contact = _as_contact_payload(parsed)
        return [contact] if contact else []
    except json.JSONDecodeError:
        pass

    # Fallback: NDJSON
    contacts: List[Dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line.startswith("{") or not line.endswith("}"):
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        contact = _as_contact_payload(obj)
        if contact:
            contacts.append(contact)
    return contacts


def fetch_signal_contacts() -> List[Dict[str, Any]]:
    """
    Run signal_finder.py and return all contact-like JSON rows.

    Expected shape includes keys like: contactId, email, fullName, openCount.
    """
    try:
        LOGGER.debug("Running signal_finder.py for contact resolution.")
        result = subprocess.run(
            [sys.executable, str(SIGNAL_FINDER_PATH)],
            check=True,
            capture_output=True,
            text=True,
            cwd=str(SCRIPT_DIR),
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        raise RuntimeError(
            "signal_finder.py failed while resolving customer identity"
            + (f": {stderr}" if stderr else "")
        ) from exc

    contacts: List[Dict[str, Any]] = []
    for line in (result.stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{") or not line.endswith("}"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        contact = _as_contact_payload(payload)
        if contact:
            contacts.append(contact)

    LOGGER.debug("Resolved %d contacts from signal_finder.py output.", len(contacts))
    return contacts


def require_best_start(payload: Dict[str, Any]) -> str:
    """
    Validate the `availability.py` output contract and extract start datetime.

    Expected shape: {"best_start_time": {"start": "<ISO-8601 datetime>", ...}}
    """
    best = payload.get("best_start_time")
    if not isinstance(best, dict):
        raise RuntimeError('Input JSON must contain object key "best_start_time".')
    start = best.get("start")
    if not isinstance(start, str) or not start:
        raise RuntimeError('"best_start_time.start" is missing or invalid.')
    return start


def fetch_best_start_from_availability() -> str:
    """Run availability.py and extract the selected start time from its JSON output."""
    LOGGER.debug("Running availability.py to fetch best_start_time.")
    result = subprocess.run(
        [sys.executable, str(AVAILABILITY_PATH)],
        check=True,
        capture_output=True,
        text=True,
        cwd=str(SCRIPT_DIR),
    )

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"availability.py produced invalid JSON output: {exc}") from exc

    return require_best_start(payload)


def resolve_customer_identities(args: argparse.Namespace) -> List[Tuple[str, str]]:
    """Resolve one or more customer (name, email) tuples from CLI and signal inputs."""
    cli_name = (args.customer_name or "").strip()
    cli_email = (args.customer_email or "").strip()

    signal_contacts = load_signal_contacts(args.signal_input) if args.signal_input else []
    if not signal_contacts and (not cli_name or not cli_email):
        signal_contacts = fetch_signal_contacts()

    # If only one of the CLI identity fields is provided, fail fast.
    if bool(cli_name) != bool(cli_email):
        raise RuntimeError("Provide both --customer-name and --customer-email, or neither.")

    # If both values are explicitly provided, treat as a single-contact run.
    if cli_name and cli_email:
        return [(cli_name, cli_email)]

    identities: List[Tuple[str, str]] = []
    for signal in signal_contacts:
        name = cli_name or str(signal.get("fullName") or "").strip()
        email = cli_email or str(signal.get("email") or "").strip()
        if name and email:
            identities.append((name, email))

    LOGGER.debug("Resolved %d customer identities.", len(identities))
    return identities


def get_access_token() -> str:
    """Request an app-only Microsoft Graph token via OAuth2 client credentials."""
    tenant_id = load_env("TENANT_ID")
    client_id = load_env("CLIENT_ID")
    client_secret = load_env("CLIENT_SECRET")

    token_url = TOKEN_URL_TMPL.format(tenant_id=tenant_id)
    resp = requests.post(
        token_url,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    if not resp.ok:
        raise GraphError(f"Token request failed: {resp.status_code} {resp.text}")

    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise GraphError(f"No access_token in token response: {data}")
    return token


class GraphClient:
    """Minimal Graph wrapper for authenticated JSON POST calls."""
    def __init__(self, token: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    def post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{GRAPH_ROOT}{path}"
        resp = self.session.post(url, data=json.dumps(body), timeout=30)
        if not resp.ok:
            raise GraphError(f"POST {url} failed: {resp.status_code} {resp.text}")
        return resp.json()

    def get(self, path: str) -> Dict[str, Any]:
        url = f"{GRAPH_ROOT}{path}"
        resp = self.session.get(url, timeout=30)
        if not resp.ok:
            raise GraphError(f"GET {url} failed: {resp.status_code} {resp.text}")
        return resp.json()


def get_teams_join_url(event_payload: Dict[str, Any]) -> Optional[str]:
    """Extract the best-available Teams meeting URL from a Graph event payload."""
    online_meeting = event_payload.get("onlineMeeting")
    if isinstance(online_meeting, dict):
        join_url = online_meeting.get("joinUrl")
        if isinstance(join_url, str) and join_url.strip():
            return join_url.strip()

    online_meeting_url = event_payload.get("onlineMeetingUrl")
    if isinstance(online_meeting_url, str) and online_meeting_url.strip():
        return online_meeting_url.strip()

    return None


def make_datetime_pair(start_str: str, duration_minutes: int) -> tuple[str, str]:
    """Derive `(start, end)` ISO strings from selected start and meeting duration."""
    start_dt = datetime.fromisoformat(start_str)
    end_dt = start_dt + timedelta(minutes=duration_minutes)
    return start_dt.isoformat(), end_dt.isoformat()


def format_pci_datetime(start_str: str, timezone: str) -> str:
    """Convert an ISO datetime string into a human-readable PCI datetime string."""
    start_dt = datetime.fromisoformat(start_str)
    weekday = start_dt.strftime("%A")
    two_digit_year = start_dt.strftime("%y")
    minute = start_dt.strftime("%M")
    hour_12 = start_dt.hour % 12 or 12
    am_pm = "am" if start_dt.hour < 12 else "pm"
    return (
        f"{weekday}, {start_dt.month}/{start_dt.day}/{two_digit_year} "
        f"at {hour_12}:{minute} {am_pm} ({timezone})"
    )


def build_event_body(
    args: argparse.Namespace,
    start_str: str,
    customer_name: str,
    customer_email: str,
) -> Dict[str, Any]:
    """
    Build a Graph event payload from CLI/customer context.

    Produces a Teams meeting event and includes customer metadata in plain-text
    body content for quick internal reference.
    """
    start_iso, end_iso = make_datetime_pair(start_str, args.duration_minutes)

    subject = args.subject.strip() or DEFAULT_SUBJECT_TEMPLATE.format(
        customer_name=customer_name
    )

    lines = [
        f"Customer: {customer_name}",
        f"Email: {customer_email}",
    ]
    if args.customer_phone:
        lines.append(f"Phone: {args.customer_phone}")
    if args.customer_notes:
        lines.append("")
        lines.append("Notes:")
        lines.append(args.customer_notes)

    return {
        "subject": subject,
        "body": {
            "contentType": "Text",
            "content": "\n".join(lines),
        },
        "start": {
            "dateTime": start_iso,
            "timeZone": args.timezone,
        },
        "end": {
            "dateTime": end_iso,
            "timeZone": args.timezone,
        },
        "location": {
            "displayName": args.location,
        },
        "attendees": [
            {
                "emailAddress": {
                    "address": customer_email,
                    "name": customer_name,
                },
                "type": "required",
            }
        ],
        "isOnlineMeeting": True,
        "onlineMeetingProvider": "teamsForBusiness",
    }


def send_contact_to_form_submitter(
    customer_name: str,
    customer_email: str,
    pci_datetime: str,
    teams_join_url: Optional[str],
    dry_run: bool,
) -> bool:
    """Forward contact info to form_submitter.py helpers after scheduling."""
    from form_submitter import extract_submission_data, submit_form

    signal_event = {
        "email": customer_email,
        "fullName": customer_name,
        "pci_datetime": pci_datetime,
        "teams_join_url": teams_join_url,
    }
    submission_data = extract_submission_data(signal_event)
    LOGGER.debug("Submitting contact to form_submitter for email=%s dry_run=%s", customer_email, dry_run)
    return submit_form(customer_email, submission_data, dry_run=dry_run)


def get_email_domain(email: str) -> str:
    """Extract and return domain portion of an email address when present."""
    local_part, separator, domain = email.partition("@")
    if separator and local_part and domain:
        return domain
    return email


def main() -> None:
    args = parse_args()
    configure_logging(args.debug)
    LOGGER.debug("Starting create_mike_event.py with dry_run=%s", args.dry_run)
    customer_identities = resolve_customer_identities(args)
    if not customer_identities:
        LOGGER.debug("No qualifying contacts resolved. Exiting with PCI scan no-contacts message.")
        print("PCI Scan found no eligible contacts.")
        return
    mike_email = load_env("MIKE_ID")

    # Authenticate once, then reuse the session-backed client for API calls.
    token = get_access_token()
    client = GraphClient(token)

    outputs: List[Dict[str, Any]] = []
    for i, (customer_name, customer_email) in enumerate(customer_identities):
        LOGGER.debug("Processing contact %d/%d: %s <%s>", i + 1, len(customer_identities), customer_name, customer_email)
        # Fetch fresh availability for each contact when no fixed input was provided.
        if args.input:
            input_payload = read_input_json(args.input)
            best_start = require_best_start(input_payload)
        else:
            best_start = fetch_best_start_from_availability()

        event_body = build_event_body(args, best_start, customer_name, customer_email)

        if args.dry_run:
            outputs.append({
                "dry_run": True,
                "target_calendar_user": mike_email,
                "customer_name": customer_name,
                "customer_email": customer_email,
                "event_payload": event_body,
            })
        else:
            # Create the event directly on Mike's calendar.
            result = client.post(f"/users/{mike_email}/events", event_body)
            LOGGER.debug("Created event id=%s for email=%s", result.get("id"), customer_email)
            teams_join_url = get_teams_join_url(result)
            # Some tenants omit onlineMeeting details in the create response.
            # Follow-up GET ensures we can return the Teams link when available.
            if not teams_join_url and result.get("id"):
                event_id = result["id"]
                expanded = client.get(
                    f"/users/{mike_email}/events/{event_id}"
                    "?$select=id,webLink,subject,start,end,onlineMeeting,onlineMeetingUrl"
                )
                teams_join_url = get_teams_join_url(expanded)
                # Keep richer fields from expanded payload when present.
                result = {**result, **expanded}
            pci_datetime = format_pci_datetime(best_start, args.timezone)
            form_submitted = send_contact_to_form_submitter(
                customer_name=customer_name,
                customer_email=customer_email,
                pci_datetime=pci_datetime,
                teams_join_url=teams_join_url,
                dry_run=args.dry_run,
            )
            LOGGER.debug("Form submission result for email=%s submitted=%s", customer_email, form_submitted)
            outputs.append({
                "target_calendar_user": mike_email,
                "customer_name": customer_name,
                "customer_email": customer_email,
                "event_id": result.get("id"),
                "web_link": result.get("webLink"),
                "teams_join_url": teams_join_url,
                "subject": result.get("subject"),
                "start": result.get("start"),
                "end": result.get("end"),
                "pci_datetime": pci_datetime,
                "form_submitted": form_submitted,
            })

        # Keep a small gap between event creations to be friendlier to upstream APIs.
        if i < len(customer_identities) - 1 and args.inter_event_delay_seconds > 0:
            LOGGER.debug("Sleeping %.2f seconds before next contact.", args.inter_event_delay_seconds)
            time.sleep(args.inter_event_delay_seconds)

    LOGGER.debug("Completed run with %d output records.", len(outputs))
    if args.dry_run:
        print(json.dumps(outputs, indent=2))
        return

    summary_blocks: List[str] = []
    for output in outputs:
        customer_name = str(output.get("customer_name") or "").strip()
        customer_email = str(output.get("customer_email") or "").strip()
        pci_datetime = str(output.get("pci_datetime") or "").strip()
        email_domain = get_email_domain(customer_email)
        summary_blocks.append(
            "\n".join(
                [
                    "🎉 Meeting booked!",
                    f"Who: {customer_name}, {email_domain}",
                    f"When: {pci_datetime}",
                ]
            )
        )
    print("\n\n".join(summary_blocks))


if __name__ == "__main__":
    main()
