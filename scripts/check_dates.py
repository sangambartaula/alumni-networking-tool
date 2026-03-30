#!/usr/bin/env python3
"""Check date fields for relevant-but-zero-months alumni."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "scraper"))

import logging
logging.basicConfig(level=logging.WARNING)

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from database import get_connection, init_db

try:
    init_db()
except Exception:
    pass

conn = get_connection()
cur = conn.cursor(dictionary=True)
cur.execute("""
    SELECT linkedin_url, current_job_title, 
           job_start_date, job_end_date,
           exp2_title, exp2_dates,
           exp3_title, exp3_dates,
           job_1_is_relevant, relevant_experience_months
    FROM alumni
    WHERE CAST(COALESCE(NULLIF(relevant_experience_months, ''), '0') AS INTEGER) = 0
      AND job_1_is_relevant = 1
    ORDER BY last_name, first_name
""")
rows = cur.fetchall() or []
conn.close()

print(f"{'PROFILE':<30} {'TITLE':<35} {'START':<15} {'END':<15} {'EXP2_DATES':<20}")
print("-" * 115)
for r in rows:
    url = (r.get("linkedin_url") or "").split("/in/")[-1][:29]
    title = (r.get("current_job_title") or "")[:34]
    start = repr(r.get("job_start_date"))[:14]
    end = repr(r.get("job_end_date"))[:14]
    e2d = repr(r.get("exp2_dates"))[:19]
    print(f"{url:<30} {title:<35} {start:<15} {end:<15} {e2d:<20}")

print(f"\n--- {len(rows)} relevant alumni with 0 months (missing dates?) ---")
