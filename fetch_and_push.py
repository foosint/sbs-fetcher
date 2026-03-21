#!/usr/bin/env python3
"""
fetch_and_push.py
-----------------
Fetches latest SBS battlefield stats, writes them into a local SQLite DB,
then uploads the updated DB directly to GitHub via the Contents API
(no local clone of sbs-stats required).

Run via cron every 10 minutes:
    */10 * * * * cd /home/pi/sbs-fetcher && uv run fetch_and_push.py >> cron.log 2>&1

Configuration via .env (see .env.example).
"""

import base64
import hashlib
import os
import sqlite3
import time
import zoneinfo
import requests
from datetime import datetime, timezone
from pathlib import Path

# ─── Load .env ────────────────────────────────────────────────────────────────

_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

# ─── Config ───────────────────────────────────────────────────────────────────

DB_PATH       = os.environ.get("SBS_DB_PATH",    str(Path(__file__).parent / "sbs.db"))
GITHUB_TOKEN  = os.environ["GITHUB_TOKEN"]          # required — fail fast if missing
GITHUB_REPO   = os.environ["GITHUB_REPO"]           # e.g. "yourname/sbs-stats"
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH",  "main")
GITHUB_DB_PATH= os.environ.get("GITHUB_DB_PATH", "data/sbs.db")
GIT_NAME      = os.environ.get("GIT_USER_NAME",  "sbs-fetcher")
GIT_EMAIL     = os.environ.get("GIT_USER_EMAIL", "")
# Note: GIT_SIGN_KEY is not used — see comment in push_db_to_github()

KYIV_TZ = zoneinfo.ZoneInfo("Europe/Kyiv")

# ─── SBS API endpoints ────────────────────────────────────────────────────────

DAILY_URL = "https://sbs-group.army/api/public/statistics/68b0c85589944c4bfb2a5edc/68fa98652f31834f2e051459"
YESTERDAY_URL = "https://sbs-group.army/api/public/statistics/68b0c85589944c4bfb2a5edc/68b4852e792cdf918400daf1"

MONTHLY_URLS: dict[str, str] = {
    "2025-06": "https://sbs-group.army/api/public/statistics/68b0c85589944c4bfb2a5edc/68e4e5d484f8ca462d9a7c77",
    "2025-07": "https://sbs-group.army/api/public/statistics/68b0c85589944c4bfb2a5edc/68e4e67684f8ca462d9a80db",
    "2025-08": "https://sbs-group.army/api/public/statistics/68b0c85589944c4bfb2a5edc/68b47e50792cdf918400b06c",
    "2025-09": "https://sbs-group.army/api/public/statistics/68b0c85589944c4bfb2a5edc/68b1c0f79d5ff32bdf80e705",
    "2025-10": "https://sbs-group.army/api/public/statistics/68b0c85589944c4bfb2a5edc/68dd1906de22367a4e908027",
    "2025-11": "https://sbs-group.army/api/public/statistics/68b0c85589944c4bfb2a5edc/690531b56519eae72cea27b4",
    "2025-12": "https://sbs-group.army/api/public/statistics/68b0c85589944c4bfb2a5edc/692cbeb9145504ee2b0171e5",
    "2026-01": "https://sbs-group.army/api/public/statistics/68b0c85589944c4bfb2a5edc/69559d35869d2691543f313f",
    "2026-02": "https://sbs-group.army/api/public/statistics/68b0c85589944c4bfb2a5edc/697e7bbb1ae0eb20ad9dbf56",
    "2026-03": "https://sbs-group.army/api/public/statistics/68b0c85589944c4bfb2a5edc/69a365b5e9679075a2e69d57",
}

SBS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}

# ─── Schema ───────────────────────────────────────────────────────────────────

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_stats (
            date DATE, hour INTEGER,
            data_collected_at TEXT, last_updated DATETIME,
            personnel_killed INTEGER, personnel_wounded INTEGER,
            total_targets_hit INTEGER, total_targets_destroyed INTEGER,
            total_personnel_casualties INTEGER,
            PRIMARY KEY (date, hour)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monthly_stats (
            date DATE,
            data_collected_at TEXT, last_updated DATETIME,
            personnel_killed INTEGER, personnel_wounded INTEGER,
            total_targets_hit INTEGER, total_targets_destroyed INTEGER,
            total_personnel_casualties INTEGER,
            PRIMARY KEY (date)
        )
    """)
    conn.commit()


def ensure_columns(conn: sqlite3.Connection, table: str, tids: list[int]) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    for tid in tids:
        for prefix in ("hit", "destroyed"):
            col = f"{prefix}_{tid}"
            if col not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} INTEGER")
    conn.commit()

# ─── SBS HTTP fetch with retries ──────────────────────────────────────────────

def fetch_json(url: str, retries: int = 5, backoff: int = 5) -> dict:
    session = requests.Session()
    session.headers.update(SBS_HEADERS)
    for attempt in range(1, retries + 1):
        ts = datetime.now().strftime("[%d.%m.%Y %H:%M:%S]")
        try:
            r = session.get(url, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if "525" in str(e):
                print(f"⚠️  {ts} HTTP 525. Attempt {attempt}/{retries}...")
                if attempt < retries:
                    time.sleep(backoff * attempt)
                    continue
            raise
        except requests.exceptions.RequestException as e:
            print(f"⚠️  {ts} Network error: {e}. Attempt {attempt}/{retries}...")
            if attempt < retries:
                time.sleep(backoff * attempt)
                continue
            raise
    raise RuntimeError(f"Failed after {retries} attempts: {url}")

# ─── Parse SBS response ───────────────────────────────────────────────────────

def parse_response(raw: dict) -> dict:
    data = raw.get("data", {})
    if not data:
        raise ValueError("API response missing 'data' field")
    personnel = data.get("personnel", {})
    targets = {
        t["targetClassId"]: {"hit": t.get("hit"), "destroyed": t.get("destroyed")}
        for t in data.get("targetsByType", [])
        if t.get("targetClassId") is not None
    }
    return {
        "data_collected_at":          data.get("dataCollectedAt"),
        "start_date":          data.get("startDate"),
        "last_updated":               data.get("lastUpdated"),
        "personnel_killed":           personnel.get("killed"),
        "personnel_wounded":          personnel.get("wounded"),
        "total_targets_hit":          data.get("totalTargetsHit"),
        "total_targets_destroyed":    data.get("totalTargetsDestroyed"),
        "total_personnel_casualties": data.get("totalPersonnelCasualties"),
        "targets": targets,
    }


def to_kyiv(iso_str: str) -> datetime:
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone(KYIV_TZ)

# ─── Upsert ───────────────────────────────────────────────────────────────────

def _upsert(conn: sqlite3.Connection, table: str, pk_cols: list[str], data: dict) -> None:
    targets: dict[int, dict] = data.pop("targets", {})
    tids = list(targets.keys())
    ensure_columns(conn, table, tids)

    data["last_updated"] = datetime.now(timezone.utc).isoformat()
    for tid in tids:
        data[f"hit_{tid}"]       = targets[tid]["hit"]
        data[f"destroyed_{tid}"] = targets[tid]["destroyed"]

    cols    = list(data.keys())
    ph      = ", ".join("?" * len(cols))
    updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in pk_cols)
    pk      = ", ".join(pk_cols)

    conn.execute(
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph})"
        f" ON CONFLICT({pk}) DO UPDATE SET {updates}",
        list(data.values())
    )
    conn.commit()


def upsert_daily(conn: sqlite3.Connection, data: dict) -> None:
    date, hour = data["date"], data["hour"]
    del data["start_date"] # remove field
    _upsert(conn, "daily_stats", ["date", "hour"], data)
    print(f"  ✅ daily_stats:   {date} hour {hour:02d}")


def upsert_monthly(conn: sqlite3.Connection, data: dict) -> None:
    date = data["date"]
    del data["start_date"] # remove field
    _upsert(conn, "monthly_stats", ["date"], data)
    print(f"  ✅ monthly_stats: {date}")

# ─── GitHub Contents API push ─────────────────────────────────────────────────

def push_db_to_github() -> None:
    """
    Upload DB to GitHub via the Contents API.
    Uses PUT /repos/{owner}/{repo}/contents/{path}
    If the file already exists, fetches its SHA first (required for updates).
    Optionally creates a GPG-signed commit if GIT_SIGN_KEY is set.
    """
    db_bytes = Path(DB_PATH).read_bytes()
    new_sha   = hashlib.sha256(db_bytes).hexdigest()

    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_DB_PATH}"
    gh_headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    # Get current file SHA (needed to update an existing file)
    file_sha: str | None = None
    r = requests.get(
        api_url,
        headers=gh_headers,
        params={"ref": GITHUB_BRANCH},
        timeout=15,
    )
    if r.status_code == 200:
        file_sha = r.json().get("sha")
        # Compare content hash to avoid unnecessary pushes
        remote_content = base64.b64decode(r.json().get("content", ""))
        if hashlib.sha256(remote_content).hexdigest() == new_sha:
            print("  ℹ️  sbs.db unchanged, skipping push.")
            return
    elif r.status_code != 404:
        r.raise_for_status()

    # Build commit payload
    ts = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
    payload: dict = {
        "message": f"chore: update sbs.db [skip ci]\n\nFetched at {ts}",
        "content": base64.b64encode(db_bytes).decode(),
        "branch":  GITHUB_BRANCH,
        "committer": {"name": GIT_NAME, "email": GIT_EMAIL},
        "author":    {"name": GIT_NAME, "email": GIT_EMAIL},
    }
    if file_sha:
        payload["sha"] = file_sha

    # Note: the GitHub Contents API does not support SSH commit signing —
    # the "signature" field only accepts GPG armored signatures, and SSH-format
    # signing is only honoured by git itself (not the REST API).
    # Commits pushed via this API will show as "Unverified" on GitHub.
    # This is a GitHub API limitation, not a bug in this script.

    r = requests.put(api_url, headers=gh_headers, json=payload, timeout=30)
    r.raise_for_status()
    print(f"  ✅ Pushed sbs.db to GitHub ({GITHUB_REPO} @ {GITHUB_BRANCH})")

# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    ts = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    print(f"\n[{ts}] Starting fetch...")

    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)

    # Daily
    print("Fetching daily...")
    p = parse_response(fetch_json(DAILY_URL))
    kyiv_dt   = to_kyiv(p["data_collected_at"])
    p["date"] = kyiv_dt.strftime("%Y-%m-%d")
    p["hour"] = kyiv_dt.hour
    upsert_daily(conn, p)

    # Yesterday (add the adjusted value to the last hour of the previous day)
    print("Fetching yesterday...")
    p = parse_response(fetch_json(YESTERDAY_URL))
    kyiv_dt = datetime.fromisoformat(p["start_date"])
    p["date"] = kyiv_dt.strftime("%Y-%m-%d")
    p["hour"] = 23
    upsert_daily(conn, p)

    # Monthly
    for month, url in MONTHLY_URLS.items():
        print(f"Fetching monthly {month}...")
        try:
            pm = parse_response(fetch_json(url))
            pm["date"] = f"{month}-01"
            upsert_monthly(conn, pm)
        except Exception as e:
            print(f"  ⚠️  Skipping {month}: {e}")

    # Checkpoint WAL and close — ensures all data is flushed to the DB file
    # before we read the bytes and push to GitHub
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()

    # Push to GitHub
    print("Pushing to GitHub...")
    push_db_to_github()

    print("Done.")


if __name__ == "__main__":
    main()
