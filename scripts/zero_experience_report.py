#!/usr/bin/env python3
"""Print alumni with relevant_experience_months = 0 for manual review."""

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
    SELECT linkedin_url, major, current_job_title, company,
           job_1_relevance_score, job_1_is_relevant
    FROM alumni
    WHERE CAST(COALESCE(NULLIF(relevant_experience_months, ''), '0') AS INTEGER) = 0
      AND current_job_title IS NOT NULL AND current_job_title != ''
    ORDER BY last_name, first_name
""")
rows = cur.fetchall() or []
conn.close()

out = ROOT / "scripts" / "zero_experience_check.tsv"
with open(out, "w") as f:
    f.write("linkedin_url\tmajor\traw_job_title\tcompany\tscore\tis_relevant\n")
    for r in rows:
        f.write("\t".join(str(r.get(k) or "") for k in
            ["linkedin_url", "major", "current_job_title", "company",
             "job_1_relevance_score", "job_1_is_relevant"]) + "\n")
        print(f"{r.get('linkedin_url','')}\t{r.get('major','')}\t{r.get('current_job_title','')}\t{r.get('job_1_relevance_score','')}\t{r.get('job_1_is_relevant','')}")

print(f"\n--- {len(rows)} alumni with 0 months and a job title ---")
print(f"Full report: {out}")
