"""
This script scans for an intent signal in HubSpot defined as a number of Opens
above a defined threshold.
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime, timedelta, timezone

import requests

LOGGER = logging.getLogger(__name__)

# ---- ENV / CONSTANTS --------------------------------------------------------

HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN")
HUBSPOT_APP_ID = int(os.environ.get("HUBSPOT_APP_ID", "2286"))  # 2286 = HubSpot marketing email app

LIST_ID = 677  # HubSpot segment/list ID
PROPERTY_NAME = "pci_automation"

CAMPAIGN_ID = "6afccccd-1f8b-4036-ba17-3eea85f23a05"
BASE_URL = "https://api.hubapi.com"

# Lookback window in hours (e.g. 12 = last 12 hours)
LOOKBACK_WINDOW_HOURS = 360

# Signal threshold for minimum number of opens
SIGNAL_THRESHOLD = 3

# Compute "now minus LOOKBACK_WINDOW_HOURS" in Unix milliseconds (UTC)
NOW_UTC = datetime.now(timezone.utc)
LOOKBACK_DT = NOW_UTC - timedelta(hours=LOOKBACK_WINDOW_HOURS)
LOOKBACK_TS = int(LOOKBACK_DT.timestamp() * 1000)

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

# ---- SHARED UTILS -----------------------------------------------------------


def require_env():
    """Validate required environment variables before any network calls are made."""
    missing = []
    if not HUBSPOT_TOKEN:
        missing.append("HUBSPOT_TOKEN")
    if not HUBSPOT_APP_ID:
        missing.append("HUBSPOT_APP_ID")
    if missing:
        # print(f"Missing required env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)


def hs_get(path, params=None):
    """Thin wrapper for GET with basic error handling."""
    url = f"{BASE_URL}{path}"
    resp = requests.get(url, headers=HEADERS, params=params or {})
    if not resp.ok:
        # print(
        #     f"GET {url} failed ({resp.status_code}): {resp.text}",
        #     file=sys.stderr,
        # )
        resp.raise_for_status()
    return resp.json()


# ---- PART 1: EMAIL OPEN COUNTS ----------------------------------------------


def get_marketing_email_ids_for_campaign(campaign_guid):
    """
    Uses Campaigns v3:
      GET /marketing/v3/campaigns/{campaignGuid}/assets/MARKETING_EMAIL

    Returns:
      list[str] of marketing email IDs.
    """
    email_ids = []
    after = None
    while True:
        params = {"limit": 100}
        if after:
            params["after"] = after
        data = hs_get(
            f"/marketing/v3/campaigns/{campaign_guid}/assets/MARKETING_EMAIL",
            params=params,
        )
        LOGGER.debug("Fetched campaign assets page (after=%s): keys=%s", after, list(data.keys()))

        # API shape can be either:
        # 1) { "results": [ { "id": "832", ... }, ... ] }
        # 2) { "assets": { "MARKETING_EMAIL": { "results": [...], "paging": {...} } } }
        results = []
        if isinstance(data, dict) and "results" in data and isinstance(data["results"], list):
            results = data["results"]
        elif (
            isinstance(data, dict)
            and "assets" in data
            and "MARKETING_EMAIL" in data["assets"]
            and "results" in data["assets"]["MARKETING_EMAIL"]
        ):
            results = data["assets"]["MARKETING_EMAIL"]["results"]

        for asset in results:
            email_id = asset.get("id")
            if email_id is not None:
                email_ids.append(str(email_id))

        # Handle paging for either top-level or nested form
        paging = data.get("paging")
        if not paging and "assets" in data and "MARKETING_EMAIL" in data["assets"]:
            paging = data["assets"]["MARKETING_EMAIL"].get("paging")
        if paging and "next" in paging and "after" in paging["next"]:
            after = paging["next"]["after"]
        else:
            break

    return email_ids


def get_email_campaign_ids_for_email(email_id):
    """
    Uses Marketing Emails v3:
      GET /marketing/v3/emails/{emailId}

    Returns:
      list[int] of legacy emailCampaignIds for that email.
    """
    data = hs_get(f"/marketing/v3/emails/{email_id}")
    LOGGER.debug("Fetched email metadata for email_id=%s", email_id)
    email_campaign_ids = set()

    # allEmailCampaignIds is usually an array of strings
    for cid in data.get("allEmailCampaignIds", []):
        try:
            email_campaign_ids.add(int(cid))
        except (TypeError, ValueError):
            continue

    # primaryEmailCampaignId is a single string
    primary = data.get("primaryEmailCampaignId")
    if primary:
        try:
            email_campaign_ids.add(int(primary))
        except (TypeError, ValueError):
            pass

    return sorted(email_campaign_ids)


def get_open_events_for_email_campaign(email_campaign_id, app_id=HUBSPOT_APP_ID):
    """
    Uses Email Events API v1:
      GET /email/public/v1/events?appId={appId}&emailCampaignId={id}&type=OPEN

    Returns:
      list[dict] of OPEN events.
    """
    events = []
    offset = None
    while True:
        params = {
            "appId": app_id,
            "emailCampaignId": email_campaign_id,
            "type": "OPEN",
            "limit": 1000,  # max per page
        }
        if offset is not None:
            params["offset"] = offset

        data = hs_get("/email/public/v1/events", params=params)
        batch = data.get("events", [])
        LOGGER.debug(
            "Fetched OPEN events page for emailCampaignId=%s (offset=%s): %d events",
            email_campaign_id,
            offset,
            len(batch),
        )
        events.extend(batch)

        if not data.get("hasMore"):
            break

        offset = data.get("offset")
        if not offset:
            break

    return events


def get_open_counts_for_campaign(campaign_id):
    """
    Returns:
      dict[(emailId:str, emailCampaignId:int, recipient:str)] = open_count:int
    Only counts events with created >= LOOKBACK_TS.
    """
    # print(
    #     f"Fetching marketing emails for campaign {campaign_id} "
    #     f"(appId={HUBSPOT_APP_ID})...",
    #     file=sys.stderr,
    # )
    # print(
    #     f"Using lookback window: last {LOOKBACK_WINDOW_HOURS} hours "
    #     f"(events created >= {LOOKBACK_TS})",
    #     file=sys.stderr,
    # )

    email_ids = get_marketing_email_ids_for_campaign(campaign_id)
    if not email_ids:
        # print("No MARKETING_EMAIL assets found for this campaign.", file=sys.stderr)
        return {}
    LOGGER.debug("Resolved %d marketing email IDs for campaign %s", len(email_ids), campaign_id)

    # print(f"Found {len(email_ids)} marketing emails.", file=sys.stderr)

    open_counts = {}

    for email_id in email_ids:
        email_campaign_ids = get_email_campaign_ids_for_email(email_id)
        if not email_campaign_ids:
            # print(
            #     f"  No legacy emailCampaignIds found for email {email_id}.",
            #     file=sys.stderr,
            # )
            continue
        LOGGER.debug("email_id=%s has %d legacy campaign IDs", email_id, len(email_campaign_ids))

        for ecid in email_campaign_ids:
            # print(
            #     f"  Fetching OPEN events for emailId={email_id}, "
            #     f"emailCampaignId={ecid} (appId={HUBSPOT_APP_ID})",
            #     file=sys.stderr,
            # )
            open_events = get_open_events_for_email_campaign(ecid, HUBSPOT_APP_ID)
            if not open_events:
                LOGGER.debug("No OPEN events for email_id=%s emailCampaignId=%s", email_id, ecid)
                continue

            for ev in open_events:
                created = ev.get("created")
                # Skip if created is missing or older than lookback
                if not isinstance(created, (int, float)) or created < LOOKBACK_TS:
                    continue

                recipient = (ev.get("recipient") or "").strip().lower()
                if not recipient:
                    continue

                key = (str(email_id), int(ecid), recipient)
                open_counts[key] = open_counts.get(key, 0) + 1

    LOGGER.debug("Built %d (emailId, emailCampaignId, recipient) open-count buckets", len(open_counts))
    return open_counts


# ---- PART 2: LIST CONTACTS + PCI FLAG ---------------------------------------


def get_list_contacts(list_id, properties=None):
    """
    Returns a list of contact records (dicts) from a list/segment.
    Uses v3 CRM Lists API + Batch read for properties.
    """
    # 1) Get all contact IDs in the list
    endpoint = f"{BASE_URL}/crm/v3/lists/{list_id}/memberships"
    after = None
    contact_ids = []

    while True:
        params = {"limit": 100}
        if after:
            params["after"] = after

        resp = requests.get(endpoint, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        LOGGER.debug("Fetched list memberships page (after=%s): %d rows", after, len(data.get("results", [])))

        results = data.get("results", [])
        if not results:
            break

        for item in results:
            contact_id = item.get("recordId")
            if not contact_id:
                raise RuntimeError(f"Could not locate contact id in membership item: {item}")
            contact_ids.append(str(contact_id))

        paging = data.get("paging", {})
        next_link = paging.get("next", {})
        after = next_link.get("after")
        if not after:
            break

    if not contact_ids:
        LOGGER.debug("No contacts found in list_id=%s", list_id)
        return []

    # 2) Batch read contacts to get properties
    contacts = []
    batch_endpoint = f"{BASE_URL}/crm/v3/objects/contacts/batch/read"
    batch_size = 100

    for i in range(0, len(contact_ids), batch_size):
        batch_ids = contact_ids[i: i + batch_size]
        payload = {
            "properties": properties or [],
            "inputs": [{"id": cid} for cid in batch_ids],
        }
        resp = requests.post(batch_endpoint, headers=HEADERS, json=payload)
        resp.raise_for_status()
        data = resp.json()
        LOGGER.debug("Fetched contact batch [%d:%d): %d records", i, i + len(batch_ids), len(data.get("results", [])))
        contacts.extend(data.get("results", []))

    LOGGER.debug("Loaded %d contacts from list_id=%s", len(contacts), list_id)
    return contacts


def get_contacts_by_pci_flag(list_id, property_name):
    """
    Partition list contacts by the PCI eligibility flag.

    Contacts with `{property_name}` in {"pci_started", "pci_completed"} are
    considered PCI-ineligible and are separated from the set we can act on in
    downstream automation.
    """
    contacts = get_list_contacts(
        list_id,
        properties=[property_name, "email", "firstname", "lastname"],
    )
    pci_eligible = []
    pci_ineligible = []

    for contact in contacts:
        props = contact.get("properties", {}) or {}
        pci_automation_val = props.get(property_name)
        is_ineligible = str(pci_automation_val).lower() in {
            "pci_started",
            "pci_completed",
        }

        if is_ineligible:
            pci_ineligible.append(contact)
        else:
            pci_eligible.append(contact)

    LOGGER.debug(
        "Partitioned contacts by %s: eligible=%d ineligible=%d",
        property_name,
        len(pci_eligible),
        len(pci_ineligible),
    )
    return pci_eligible, pci_ineligible


# ---- MAIN: JOIN EMAIL OPENS WITH LIST CONTACTS ------------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Find contacts with above-threshold HubSpot email opens."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose debug logging to stderr.",
    )
    return parser.parse_args()


def configure_logging(debug=False):
    level = logging.DEBUG if debug else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def main():
    args = parse_args()
    configure_logging(args.debug)
    require_env()

    # 1) Get open counts for this campaign (filtered by LOOKBACK_TS)
    # This returns per-(email asset, campaign id, recipient) open totals so we
    # can apply a signal threshold before acting on any contact.
    open_counts = get_open_counts_for_campaign(CAMPAIGN_ID)
    LOGGER.debug("open_counts bucket count=%d", len(open_counts))

    # 2) Get contacts in the list with PCI flag + email
    pci_eligible, pci_ineligible = get_contacts_by_pci_flag(LIST_ID, PROPERTY_NAME)
    # `pci_ineligible` is intentionally retained for clarity/documentation of
    # the split, even though this first step only processes eligible contacts.

    # Build a lookup: email (lowercased) -> eligible contact info
    # ONLY PCI-ELIGIBLE contacts here
    eligible_by_email = {}
    for c in pci_eligible:
        props = c.get("properties", {}) or {}
        email = (props.get("email") or "").strip().lower()
        if not email:
            continue

        firstname = (props.get("firstname") or "").strip()
        lastname = (props.get("lastname") or "").strip()
        # Avoid emitting null for fullName, since downstream consumers treat it
        # as a display-ready string. Fall back to the contact email when name
        # properties are absent.
        full_name = (firstname + " " + lastname).strip() or email

        eligible_by_email[email] = {
            "id": c.get("id"),
            "email": email,
            "fullName": full_name,
            "pci_automation": "",  # by definition of pci_eligible
        }
    LOGGER.debug("Built eligible_by_email lookup with %d entries", len(eligible_by_email))

    # 3) Aggregate above-threshold opens per contact
    # key: contactId, value: {"contactId", "email", "fullName", "openCount"}
    aggregated_by_contact = {}

    for (email_id, ecid, recipient), count in sorted(open_counts.items()):
        # Apply signal threshold: skip if below threshold
        if count < SIGNAL_THRESHOLD:
            continue

        # Join HubSpot email event recipients to CRM contacts by normalized email.
        contact = eligible_by_email.get(recipient)
        if not contact:
            # recipient not in the PCI_ELIGIBLE portion of the list; skip
            continue

        contact_id = contact["id"]
        if contact_id not in aggregated_by_contact:
            aggregated_by_contact[contact_id] = {
                "contactId": contact_id,
                "email": contact["email"],
                "fullName": contact.get("fullName") or contact["email"],
                "openCount": 0,
            }

        # Only counts that already passed the threshold are accumulated
        aggregated_by_contact[contact_id]["openCount"] += count

    # 4) Emit NDJSON (one JSON object per line) for downstream automation steps.
    #    This format is easy to stream into queue/workflow processors.
    for contact_summary in aggregated_by_contact.values():
        LOGGER.debug("Emitting contact summary: %s", contact_summary)
        print(json.dumps(contact_summary))


if __name__ == "__main__":
    if not HUBSPOT_TOKEN:
        # print("HUBSPOT_TOKEN environment variable is not set.", file=sys.stderr)
        sys.exit(1)
    main()
