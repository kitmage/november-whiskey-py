# November Whiskey Scheduling + Signal Automation

This repository automates a simple outbound workflow:

1. **Find engaged contacts** from HubSpot email opens (`signal_finder.py`).
2. Optionally **submit those contacts to a HubSpot form** (`form_submitter.py`).
3. **Find a shared meeting slot** across internal calendars (`availability.py`).
4. **Create Outlook events** for one or more contacts (`create_mike_event.py`).

---

## Project Files

- `signal_finder.py`  
  Scans HubSpot campaign engagement and prints qualifying contacts as JSON lines.
- `form_submitter.py`  
  Submits contacts to a HubSpot form (or dry-run output).
- `availability.py`  
  Calls Microsoft Graph `getSchedule` and outputs a best start time JSON payload.
- `create_mike_event.py`  
  Creates Outlook events, can resolve contacts from `signal_finder.py`, fetch availability, and forward contact info to `form_submitter.py`.

---

## Requirements

- Ubuntu server (22.04+ recommended)
- Python 3.10+
- Network access to:
  - Microsoft Graph
  - HubSpot APIs
- Service credentials / environment variables listed below

---

## Setup on Ubuntu

### 1) Clone and enter the repo

```bash
git clone <your-repo-url> november-whiskey-dev
cd november-whiskey-dev
```

### 2) Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

### 3) Install dependencies

```bash
pip install requests msal python-dotenv hubspot-api-client
```

### Troubleshooting: `ModuleNotFoundError: No module named 'requests'`

This error means your current Python interpreter does not have project dependencies installed.

Use this exact sequence from the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install requests msal python-dotenv hubspot-api-client
python -c "import requests; print(requests.__version__)"
python signal_finder.py
```

If it still fails, verify you are running the script with the same interpreter as the virtual environment:

```bash
which python
python -V
python -m pip -V
```

Each path should point into `.../november-whiskey-dev/.venv/...`.
---
# Env Config

> If you maintain a `requirements.txt` in your environment, you can install from that instead.

---

## Environment Variables

Set these in your shell (or better: in a protected env file sourced by cron/systemd):

```bash
export HUBSPOT_TOKEN='pat-na2...'
export HUBSPOT_USER_ID='123...'
export HUBSPOT_APP_ID='2286'

export TENANT_ID='123-abc...'
export CLIENT_ID='123-abc'
export CLIENT_SECRET='123_abc...'
export CLIENT_SECRET_ID='123-abc'

export TOM_ID='tom@example.com'
export MIKE_ID='salesmarketing@example.com'
export ROB_ID='rob@example.com'
```

### Optional `.env` support

`availability.py` calls `load_dotenv()`, so you can also create a local `.env` file:

```bash
cat > .env <<'ENV'
HUBSPOT_TOKEN=pat-na2...
HUBSPOT_USER_ID=123...
HUBSPOT_APP_ID=2286
TENANT_ID=123-abc...
CLIENT_ID=123-abc
CLIENT_SECRET=123_abc...
CLIENT_SECRET_ID=123-abc
TOM_ID=tom@example.com
MIKE_ID=salesmarketing@example.com
ROB_ID=rob@example.com
ENV
chmod 600 .env
```

---

## Script Usage

## 1) Find signal contacts

```bash
python3 signal_finder.py
```

Output is NDJSON (one JSON object per line), for example:

```json
{"contactId":"464406510295","email":"mail@virtualsangha.org","fullName":"mike test","openCount":4}
```

---

## 2) Submit signal contacts to HubSpot form

Dry-run:

```bash
python3 form_submitter.py --dry-run
```

Live submit:

```bash
python3 form_submitter.py
```

---

## 3) Compute best start time

```bash
python3 availability.py
```

Example output:

```json
{
  "best_start_time": {
    "start": "2026-04-08T10:00:00",
    "score": 3,
    "buffer_before_blocks": 4,
    "buffer_after_blocks": 3
  }
}
```

---

## 4) Create events (`create_mike_event.py`)

### Common modes

#### A) Fully automatic (contacts + availability resolved by scripts)

```bash
python3 create_mike_event.py --debug
```

This will:
- discover contacts via `signal_finder.py`
- fetch fresh availability via `availability.py` for each contact
- create events on Mike's calendar
- forward contact data to `form_submitter.py`

#### B) Dry-run only

```bash
python3 create_mike_event.py --dry-run --debug
```

#### C) Manual single contact override

```bash
python3 create_mike_event.py \
  --customer-name "Prospect Name" \
  --customer-email "prospect@example.com"
```

#### D) Use precomputed files

```bash
python3 signal_finder.py > signal.ndjson
python3 availability.py > availability.json

python3 create_mike_event.py \
  --signal-input signal.ndjson \
  --input availability.json \
  --dry-run
```

### Useful CLI flags

- `--signal-input <path>`: read contacts from JSON/JSON-array/NDJSON file
- `--input <path>`: read availability JSON from file instead of running `availability.py`
- `--inter-event-delay-seconds <float>`: pause between contacts (default `1.0`)
- `--debug`: write persistent debug logs to `create_mike_event.log`
- `--dry-run`: no writes to Graph/HubSpot; prints planned payloads

---

## Logging & Troubleshooting

When `--debug` is enabled, `create_mike_event.py` appends debug logs to:

```text
create_mike_event.log
```

Tail logs live:

```bash
tail -f create_mike_event.log
```

If no contacts qualify, `create_mike_event.py` exits gracefully and prints:

```text
null
```

---

## Run Periodically on Ubuntu (Cron)

## Option 1: User crontab

Edit crontab:

```bash
crontab -e
```

Example: run every 30 minutes:

```cron
*/30 * * * * cd /home/ubuntu/november-whiskey-dev && /home/ubuntu/november-whiskey-dev/.venv/bin/python create_mike_event.py --debug >> /home/ubuntu/november-whiskey-dev/cron.log 2>&1
```

Tips:
- Use **absolute paths** in cron.
- Ensure env vars are available (see wrapper script below).

## Option 2: Cron + wrapper script (recommended)

Create `/home/ubuntu/november-whiskey-dev/run_create_mike_event.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

cd /home/ubuntu/november-whiskey-dev
source .venv/bin/activate

# Load env vars from protected file
set -a
source /home/ubuntu/november-whiskey-dev/.env
set +a

python create_mike_event.py --debug
```

Then:

```bash
chmod +x /home/ubuntu/november-whiskey-dev/run_create_mike_event.sh
crontab -e
```

Crontab entry:

```cron
*/30 * * * * /home/ubuntu/november-whiskey-dev/run_create_mike_event.sh >> /home/ubuntu/november-whiskey-dev/cron.log 2>&1
```

---

## Security Notes

- Never commit secrets (`.env`, tokens, client secrets) to git.
- Restrict file permissions on secrets:

```bash
chmod 600 .env
```

- Use dedicated service principals / least-privilege app permissions where possible.

---

## Quick Health Check

Run this after setup:

```bash
source .venv/bin/activate
python3 -m py_compile create_mike_event.py availability.py signal_finder.py form_submitter.py
python3 create_mike_event.py --dry-run --debug
```
