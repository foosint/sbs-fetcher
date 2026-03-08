#!/usr/bin/env python3
"""
init_db.py
----------
Run this ONCE locally to create the initial data/sbs.db file
before pushing your repo to GitHub.

    python scripts/init_db.py
"""

import os
import sqlite3

DB_PATH = os.environ.get("DB_PATH", "./sbs.db")
TARGET_IDS = [1, 2, 5, 7, 18, 21, 22, 25]


def init() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_stats (
            date DATE,
            hour INTEGER,
            data_collected_at TEXT,
            last_updated DATETIME,
            personnel_killed INTEGER,
            personnel_wounded INTEGER,
            total_targets_hit INTEGER,
            total_targets_destroyed INTEGER,
            total_personnel_casualties INTEGER,
            PRIMARY KEY (date, hour)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS monthly_stats (
            date DATE,
            data_collected_at TEXT,
            last_updated DATETIME,
            personnel_killed INTEGER,
            personnel_wounded INTEGER,
            total_targets_hit INTEGER,
            total_targets_destroyed INTEGER,
            total_personnel_casualties INTEGER,
            PRIMARY KEY (date)
        )
    """)

    # Pre-add all known target columns so the DB isn't empty on first deploy
    for table in ("daily_stats", "monthly_stats"):
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in cur.fetchall()}
        for tid in TARGET_IDS:
            for prefix in ("hit", "destroyed"):
                col = f"{prefix}_{tid}"
                if col not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} INTEGER")

    conn.commit()
    conn.close()
    print(f"Initialised {DB_PATH}")


if __name__ == "__main__":
    init()
