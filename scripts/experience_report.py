#!/usr/bin/env python3
"""
Diagnostic: dump all alumni experience relevance data to a TSV report.

Usage:
    python scripts/experience_report.py

Output:
    scripts/experience_report.tsv
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "scraper"))

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from database import get_connection, init_db


def run():
    try:
        init_db()
    except Exception:
        pass

    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
                a.id,
                a.first_name,
                a.last_name,
                a.linkedin_url,
                a.current_job_title,
                a.company,
                a.job_start_date,
                a.job_end_date,
                a.exp2_title,
                a.exp2_company,
                a.exp2_dates,
                a.exp3_title,
                a.exp3_company,
                a.exp3_dates,
                a.job_1_relevance_score,
                a.job_2_relevance_score,
                a.job_3_relevance_score,
                a.job_1_is_relevant,
                a.job_2_is_relevant,
                a.job_3_is_relevant,
                a.relevant_experience_months
            FROM alumni a
            ORDER BY a.last_name, a.first_name
        """)
        rows = cur.fetchall() or []

        # Summary stats
        total = len(rows)
        has_months = sum(1 for r in rows if r.get("relevant_experience_months") not in (None, "", 0, "0"))
        has_job1_score = sum(1 for r in rows if r.get("job_1_relevance_score") not in (None, "", 0, "0"))
        has_job1_relevant = sum(1 for r in rows if r.get("job_1_is_relevant") not in (None, "", 0, "0"))

        logger.info(f"Total alumni:                 {total}")
        logger.info(f"Has relevant_experience_months: {has_months}")
        logger.info(f"Has job_1_relevance_score:     {has_job1_score}")
        logger.info(f"Has job_1_is_relevant:         {has_job1_relevant}")

        # Distribution of relevant_experience_months
        months_dist = {}
        for r in rows:
            v = r.get("relevant_experience_months")
            key = repr(v)
            months_dist[key] = months_dist.get(key, 0) + 1
        logger.info("\nrelevant_experience_months distribution:")
        for k, cnt in sorted(months_dist.items(), key=lambda x: -x[1]):
            logger.info(f"  {k}: {cnt}")

        # Write TSV report
        out_path = ROOT / "scripts" / "experience_report.tsv"
        with open(out_path, "w", encoding="utf-8") as f:
            headers = [
                "linkedin_url",
                "name",
                "current_title",
                "company",
                "job_start",
                "job_end",
                "job1_score",
                "job1_relevant",
                "exp2_title",
                "exp2_company",
                "exp2_dates",
                "job2_score",
                "job2_relevant",
                "exp3_title",
                "exp3_company",
                "exp3_dates",
                "job3_score",
                "job3_relevant",
                "relevant_experience_months",
            ]
            f.write("\t".join(headers) + "\n")

            for r in rows:
                name = f"{(r.get('first_name') or '').strip()} {(r.get('last_name') or '').strip()}".strip()
                vals = [
                    r.get("linkedin_url", ""),
                    name,
                    r.get("current_job_title", ""),
                    r.get("company", ""),
                    r.get("job_start_date", ""),
                    r.get("job_end_date", ""),
                    r.get("job_1_relevance_score", ""),
                    r.get("job_1_is_relevant", ""),
                    r.get("exp2_title", ""),
                    r.get("exp2_company", ""),
                    r.get("exp2_dates", ""),
                    r.get("job_2_relevance_score", ""),
                    r.get("job_2_is_relevant", ""),
                    r.get("exp3_title", ""),
                    r.get("exp3_company", ""),
                    r.get("exp3_dates", ""),
                    r.get("job_3_relevance_score", ""),
                    r.get("job_3_is_relevant", ""),
                    r.get("relevant_experience_months", ""),
                ]
                f.write("\t".join(str(v) if v is not None else "" for v in vals) + "\n")

        logger.info(f"\n✅ Report written to {out_path}")
        logger.info(f"   {total} rows exported")

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    run()
