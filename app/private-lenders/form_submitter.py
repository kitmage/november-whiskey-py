"""
This script should be triggered periodically via cron or similar process.
It takes input from signal_finder.py and may submit a Hubspot form.
"""

#!/usr/bin/env python3
import os
import sys
import json
import time
import argparse
import subprocess
from typing import Dict, Any

import requests

HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN")
HUBSPOT_APP_ID = int(os.environ.get("HUBSPOT_APP_ID", "2286"))  # not strictly needed here, but available

# HubSpot portal + form configuration
PORTAL_ID = "5526411"
FORM_ID = "2710c2e4-faad-4ddc-83af-faa9520d81a4"

# NOTE: Marketing Forms v3 integration submit uses api.hsforms.com, not api.hubapi.com
BASE_URL = "https://api.hsforms.com"


def parse_args() -> argparse.Namespace:
    """Parse CLI flags for safe dry-run execution."""
    parser = argparse.ArgumentParser(
        description="Submit contacts (from signal_finder.py JSON lines) to a HubSpot form."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be submitted without calling the HubSpot API.",
    )
    return parser.parse_args()


def get_signal_finder_output_lines() -> list[str]:
    """
    Run `signal_finder.py` and return its stdout as individual lines.

    The script is executed with the current Python interpreter so it uses the
    same environment (including HubSpot credentials).
    """
    proc = subprocess.run(
        [sys.executable, "signal_finder.py"],
        capture_output=True,
        text=True,
        check=False,
    )

    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        raise RuntimeError(
            f"signal_finder.py failed with exit code {proc.returncode}"
            + (f": {stderr}" if stderr else "")
        )

    return (proc.stdout or "").splitlines()


def is_contact_event_line(line: str) -> bool:
    """Heuristically detect lines that look like contact JSON from signal_finder."""
    line = line.strip()
    if not line.startswith("{") or not line.endswith("}"):
        return False
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return False

    # Minimal contract from signal_finder output:
    # {"contactId": "...", "email": "...", "openCount": N}
    return "email" in obj and "contactId" in obj


def extract_submission_data(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map the event fields into form field values.
    """

    email = event.get("email")
    open_count = event.get("openCount")
    email_id = event.get("emailId")
    email_campaign_id = event.get("emailCampaignId")

    # This is whatever string you want to send for the PCI - DateTime property.
    # Example: event may already contain it, or you might build it here.
    pci_datetime = event.get("pci_datetime")  # e.g. "2026-04-02T13:45:00Z"
    teams_join_url = event.get("teams_join_url")

    fields = [
        {"name": "email", "value": email},
    ]

    if open_count is not None:
        fields.append({"name": "open_count", "value": str(open_count)})
    if email_id is not None:
        fields.append({"name": "email_id", "value": str(email_id)})
    if email_campaign_id is not None:
        fields.append({"name": "email_campaign_id", "value": str(email_campaign_id)})

    # NEW: send pci_datetime as a string
    if pci_datetime is not None:
        fields.append({"name": "pci_datetime", "value": str(pci_datetime)})
    if teams_join_url is not None:
        fields.append({"name": "teams_join_url", "value": str(teams_join_url)})

    submission = {
        "fields": fields,
    }
    return submission


def submit_form(email: str, submission_data: Dict[str, Any], dry_run: bool = False) -> bool:
    """
    Submit a single form submission for the given email.

    Returns True on success, False on failure.
    """

    if dry_run:
        # Dry-run mode preserves parsing/validation behavior while avoiding writes
        # to HubSpot; useful for testing pipelines end-to-end.
        print(f"[DRY RUN] Would submit for {email}: {json.dumps(submission_data)}")
        return True

    if not HUBSPOT_TOKEN:
        raise RuntimeError("HUBSPOT_TOKEN is not set in environment")

    # Marketing Forms v3 integration submit endpoint
    url = f"{BASE_URL}/submissions/v3/integration/submit/{PORTAL_ID}/{FORM_ID}"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }

    # Endpoint expects a payload with a `fields` array.
    resp = requests.post(url, headers=headers, json=submission_data)

    if 200 <= resp.status_code < 300:
        print(f"Submitted form for {email} (status={resp.status_code})")
        return True
    else:
        # Print some diagnostics but keep going
        print(
            f"Failed to submit form for {email} "
            f"(status={resp.status_code}): {resp.text}",
            file=sys.stderr,
        )
        return False


def main() -> None:
    args = parse_args()

    if not HUBSPOT_TOKEN and not args.dry_run:
        print("Error: HUBSPOT_TOKEN environment variable is required.", file=sys.stderr)
        sys.exit(1)

    print("Running signal_finder.py to collect contact events...")

    count_total = 0
    count_submitted = 0

    # Per workflow contract, always source fresh events from signal_finder.py.
    signal_lines = get_signal_finder_output_lines()
    if not signal_lines:
        print("null")
        return

    contact_lines = [line for line in signal_lines if is_contact_event_line(line)]
    if not contact_lines:
        print("null")
        return

    for raw_line in contact_lines:
        raw_line = raw_line.strip()
        if not raw_line:
            continue

        try:
            event = json.loads(raw_line)
        except json.JSONDecodeError:
            print(f"Skipping non-JSON line: {raw_line}", file=sys.stderr)
            continue

        email = event.get("email")
        if not email:
            print(f"Skipping event without email: {event}", file=sys.stderr)
            continue

        count_total += 1

        # Transform signal event fields into the exact HubSpot form payload schema.
        submission_data = extract_submission_data(event)
        if submit_form(email, submission_data, dry_run=args.dry_run):
            count_submitted += 1

        # Optional small delay if you're worried about rate limits
        time.sleep(0.05)

    print(f"Done. Processed {count_total} events, submitted {count_submitted} forms.")


if __name__ == "__main__":
    main()
