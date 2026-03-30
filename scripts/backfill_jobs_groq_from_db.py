#!/usr/bin/env python3
"""
Re-extract experience fields via Groq from synthetic text built from DB columns
(no HTML). Updates titles (seniority prefixes stripped by Groq path), companies,
employment_type lines, dates, normalized job/company IDs, and seniority_level.

Usage:
  python scripts/backfill_jobs_groq_from_db.py
  python scripts/backfill_jobs_groq_from_db.py --dry-run
  python scripts/backfill_jobs_groq_from_db.py --limit 50
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "backend"))
sys.path.insert(0, str(PROJECT_ROOT / "scraper"))

import groq_retry_patch  # noqa: E402 — Groq HTTP retries (GROQ_RETRY_DELAY_SECONDS)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass


def _experience_blob(row: dict) -> str:
    lines: list[str] = []

    def add_slot(title, company, emp, date_line):
        title = (title or "").strip()
        company = (company or "").strip()
        emp = (emp or "").strip()
        date_line = (date_line or "").strip()
        if not (title or company or date_line):
            return
        if title:
            lines.append(title)
        if company and emp:
            lines.append(f"{company} · {emp}")
        elif company:
            lines.append(company)
        elif emp:
            lines.append(emp)
        if date_line:
            lines.append(date_line)
        lines.append("")

    js = (row.get("job_start_date") or "").strip()
    je = (row.get("job_end_date") or "").strip()
    if js and je:
        dl = f"{js} - {je}"
    elif js or je:
        dl = f"{js}{je}"
    else:
        dl = ""
    add_slot(row.get("current_job_title"), row.get("company"), row.get("job_employment_type"), dl)
    add_slot(
        row.get("exp2_title"),
        row.get("exp2_company"),
        row.get("exp2_employment_type"),
        (row.get("exp2_dates") or "").strip(),
    )
    add_slot(
        row.get("exp3_title"),
        row.get("exp3_company"),
        row.get("exp3_employment_type"),
        (row.get("exp3_dates") or "").strip(),
    )
    return "\n".join(lines).strip()


def _jobs_to_row_updates(jobs: list):
    from groq_client import parse_groq_date
    from groq_extractor_experience import strip_seniority_prefixes_from_title
    import utils as scraper_utils

    out = {
        "current_job_title": None,
        "company": None,
        "job_employment_type": None,
        "job_start_date": None,
        "job_end_date": None,
        "exp2_title": None,
        "exp2_company": None,
        "exp2_employment_type": None,
        "exp2_dates": None,
        "exp3_title": None,
        "exp3_company": None,
        "exp3_employment_type": None,
        "exp3_dates": None,
    }

    trimmed = jobs[:3]
    parsed = []
    for i, job in enumerate(trimmed):
        sd = parse_groq_date((job.get("start_date") or "").strip())
        ed = parse_groq_date((job.get("end_date") or "").strip())
        if not sd or not ed:
            logger.warning(
                "Unparseable dates for job slot %s: %r – %r",
                i + 1,
                job.get("start_date"),
                job.get("end_date"),
            )
            return None
        parsed.append((job, sd, ed))

    for i, (job, sd, ed) in enumerate(parsed):
        title = strip_seniority_prefixes_from_title((job.get("job_title") or "").strip())
        company = (job.get("company") or "").strip()
        emp = (job.get("employment_type") or "").strip()
        start_s = scraper_utils.format_date_for_storage(sd)
        end_s = scraper_utils.format_date_for_storage(ed)

        if i == 0:
            out["current_job_title"] = title or None
            out["company"] = company or None
            out["job_employment_type"] = emp or None
            out["job_start_date"] = start_s or None
            out["job_end_date"] = end_s or None
        elif i == 1:
            out["exp2_title"] = title or None
            out["exp2_company"] = company or None
            out["exp2_employment_type"] = emp or None
            out["exp2_dates"] = f"{start_s} - {end_s}" if (start_s and end_s) else None
        else:
            out["exp3_title"] = title or None
            out["exp3_company"] = company or None
            out["exp3_employment_type"] = emp or None
            out["exp3_dates"] = f"{start_s} - {end_s}" if (start_s and end_s) else None

    if len(trimmed) < 2:
        out["exp2_title"] = None
        out["exp2_company"] = None
        out["exp2_employment_type"] = None
        out["exp2_dates"] = None
    if len(trimmed) < 3:
        out["exp3_title"] = None
        out["exp3_company"] = None
        out["exp3_employment_type"] = None
        out["exp3_dates"] = None

    return out


def run(*, dry_run: bool, limit: int | None, sleep_s: float) -> None:
    from database import (
        get_connection,
        init_db,
        ensure_all_alumni_schema_migrations,
        _get_or_create_normalized_entity,
    )
    from groq_extractor_experience import extract_experiences_with_groq_from_text
    from groq_client import is_groq_available
    from job_title_normalization import normalize_title_deterministic
    from company_normalization import normalize_company_deterministic
    from seniority_detector import analyze_seniority

    if not is_groq_available():
        logger.error("Groq is not configured (GROQ_API_KEY). Aborting.")
        sys.exit(1)

    try:
        init_db()
    except Exception as e:
        logger.warning("init_db: %s", e)

    ensure_all_alumni_schema_migrations()

    conn = get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            sql = """
                SELECT id, linkedin_url, first_name, last_name,
                       current_job_title, company, job_employment_type,
                       job_start_date, job_end_date,
                       exp2_title, exp2_company, exp2_employment_type, exp2_dates,
                       exp3_title, exp3_company, exp3_employment_type, exp3_dates
                FROM alumni
                ORDER BY id ASC
            """
            if limit is not None:
                cur.execute(sql + " LIMIT %s", (int(limit),))
            else:
                cur.execute(sql)
            rows = cur.fetchall() or []
    finally:
        try:
            conn.close()
        except Exception:
            pass

    logger.info("Job Groq backfill: %s rows", len(rows))
    updated = 0
    skipped = 0
    dry_preview = 0

    conn = get_connection()
    try:
        cur = conn.cursor()
        for row in rows:
            rid = row["id"]
            blob = _experience_blob(row)
            if not blob:
                skipped += 1
                continue

            name = f"{row.get('first_name') or ''} {row.get('last_name') or ''}".strip() or f"id:{rid}"
            jobs, tokens = extract_experiences_with_groq_from_text(
                blob,
                max_jobs=3,
                profile_name=name,
            )
            if sleep_s > 0:
                time.sleep(sleep_s)

            if not jobs:
                logger.info("No jobs from Groq for %s (id=%s)", name, rid)
                skipped += 1
                continue

            payload = _jobs_to_row_updates(jobs)
            if payload is None:
                skipped += 1
                continue

            norm_title = normalize_title_deterministic(payload["current_job_title"] or "")
            norm_company = normalize_company_deterministic(payload["company"] or "")

            seniority = analyze_seniority(
                {
                    "current_job_title": payload["current_job_title"] or "",
                    "linkedin_url": row.get("linkedin_url") or "",
                    "job_employment_type": payload["job_employment_type"] or "",
                },
                None,
            )

            if dry_run:
                logger.info(
                    "[DRY RUN] id=%s jobs=%s seniority=%s title=%r company=%r",
                    rid,
                    len(jobs),
                    seniority,
                    payload.get("current_job_title"),
                    payload.get("company"),
                )
                dry_preview += 1
                continue

            norm_title_id = _get_or_create_normalized_entity(cur, "normalized_job_titles", "normalized_title", norm_title)
            norm_company_id = _get_or_create_normalized_entity(cur, "normalized_companies", "normalized_company", norm_company)

            cur.execute(
                """
                UPDATE alumni SET
                    current_job_title = %s,
                    company = %s,
                    job_employment_type = %s,
                    job_start_date = %s,
                    job_end_date = %s,
                    exp2_title = %s,
                    exp2_company = %s,
                    exp2_employment_type = %s,
                    exp2_dates = %s,
                    exp3_title = %s,
                    exp3_company = %s,
                    exp3_employment_type = %s,
                    exp3_dates = %s,
                    normalized_job_title_id = %s,
                    normalized_company_id = %s,
                    seniority_level = %s
                WHERE id = %s
                """,
                (
                    payload["current_job_title"],
                    payload["company"],
                    payload["job_employment_type"],
                    payload["job_start_date"],
                    payload["job_end_date"],
                    payload["exp2_title"],
                    payload["exp2_company"],
                    payload["exp2_employment_type"],
                    payload["exp2_dates"],
                    payload["exp3_title"],
                    payload["exp3_company"],
                    payload["exp3_employment_type"],
                    payload["exp3_dates"],
                    norm_title_id,
                    norm_company_id,
                    seniority,
                    rid,
                ),
            )
            updated += 1
            if updated % 25 == 0:
                conn.commit()
                logger.info("Committed %s updates…", updated)

        if not dry_run:
            conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    logger.info(
        "Done. Updated=%s dry_preview=%s skipped=%s (dry_run=%s).",
        updated,
        dry_preview if dry_run else 0,
        skipped,
        dry_run,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Re-run Groq experience extraction from DB-shaped text.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="Max rows (for testing).")
    ap.add_argument(
        "--sleep",
        type=float,
        default=float(os.getenv("GROQ_BACKFILL_SLEEP", "0.35")),
        help="Seconds between Groq calls (default 0.35 or GROQ_BACKFILL_SLEEP).",
    )
    args = ap.parse_args()
    run(dry_run=args.dry_run, limit=args.limit, sleep_s=args.sleep)


if __name__ == "__main__":
    main()
