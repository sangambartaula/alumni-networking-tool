#!/usr/bin/env python3
"""
Recompute standardized_degree(s), standardized_major(s), standardized_major_alt,
and discipline for every alumni row using the same rules as the live scraper.

Usage:
  python scripts/backfill_education_standardization.py
  python scripts/backfill_education_standardization.py --dry-run
  python scripts/backfill_education_standardization.py --no-llm   # skip Groq in discipline step
"""

from __future__ import annotations

import argparse
import logging
import sys
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


def _education_updates(row: dict, use_llm: bool) -> dict:
    from degree_normalization import standardize_degree, extract_hidden_degree
    from major_normalization import standardize_major_list
    from discipline_classification import infer_discipline

    job_title = (row.get("current_job_title") or "").strip()
    headline = (row.get("headline") or "").strip()
    exp2_title = (row.get("exp2_title") or "").strip()
    exp3_title = (row.get("exp3_title") or "").strip()

    out: dict = {}

    for suffix in ("", "2", "3"):
        deg_key = f"degree{suffix}" if suffix else "degree"
        maj_key = f"major{suffix}" if suffix else "major"
        std_deg_key = f"standardized_degree{suffix}" if suffix else "standardized_degree"
        std_maj_key = f"standardized_major{suffix}" if suffix else "standardized_major"

        raw_deg = (row.get(deg_key) or "").strip()
        raw_maj = (row.get(maj_key) or "").strip()

        if not raw_deg and raw_maj:
            extracted_deg, cleaned_maj = extract_hidden_degree(raw_maj)
            if extracted_deg:
                raw_deg = extracted_deg
                raw_maj = cleaned_maj

        out[std_deg_key] = standardize_degree(raw_deg) if raw_deg else None

        if raw_maj:
            majors = standardize_major_list(raw_maj, job_title)
            out[std_maj_key] = majors[0]
            if suffix == "":
                out["standardized_major_alt"] = majors[1] if len(majors) > 1 else None
        else:
            out[std_maj_key] = None
            if suffix == "":
                out["standardized_major_alt"] = None

    if "standardized_major_alt" not in out:
        out["standardized_major_alt"] = None

    education_entries = []
    for suffix in ("", "2", "3"):
        school_key = f"school{suffix}" if suffix else "school"
        deg_key = f"degree{suffix}" if suffix else "degree"
        maj_key = f"major{suffix}" if suffix else "major"
        std_deg_key = f"standardized_degree{suffix}" if suffix else "standardized_degree"
        std_maj_key = f"standardized_major{suffix}" if suffix else "standardized_major"

        school = (row.get(school_key) or "").strip()
        degree = (row.get(deg_key) or "").strip()
        major = (row.get(maj_key) or "").strip()
        standardized_degree = (out.get(std_deg_key) or "") or ""
        standardized_major = (out.get(std_maj_key) or "") or ""

        if not any([school, degree, major, standardized_degree, standardized_major]):
            continue

        entry = {
            "school": school,
            "degree": degree,
            "major": major,
            "standardized_degree": standardized_degree,
            "standardized_major": standardized_major,
        }
        if suffix == "":
            alt = out.get("standardized_major_alt") or ""
            if alt:
                entry["standardized_major_alt"] = alt
        education_entries.append(entry)

    deg_text = f"{row.get('degree') or ''} {row.get('major') or ''}".strip()
    out["discipline"] = infer_discipline(
        deg_text,
        job_title,
        headline,
        use_llm=use_llm,
        education_entries=education_entries if education_entries else None,
        older_job_titles=[exp2_title, exp3_title],
    )
    return out


def run(*, dry_run: bool, use_llm: bool) -> None:
    from database import get_connection, init_db, ensure_all_alumni_schema_migrations, ensure_education_columns

    try:
        init_db()
    except Exception as e:
        logger.warning("init_db: %s", e)

    ensure_education_columns()
    ensure_all_alumni_schema_migrations()

    conn = get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                """
                SELECT id, degree, degree2, degree3, major, major2, major3,
                       school, school2, school3,
                       current_job_title, headline, exp2_title, exp3_title
                FROM alumni
                ORDER BY id ASC
                """
            )
            rows = cur.fetchall() or []
    finally:
        try:
            conn.close()
        except Exception:
            pass

    logger.info("Education standardization backfill: %s rows", len(rows))
    updated = 0

    conn = get_connection()
    upd_cur = conn.cursor()
    for row in rows:
        rid = row["id"]
        payload = _education_updates(row, use_llm=use_llm)
        if dry_run:
            logger.info("[DRY RUN] id=%s discipline=%s majors=%s|%s|%s", rid, payload.get("discipline"), payload.get("standardized_major"), payload.get("standardized_major2"), payload.get("standardized_major3"))
            continue

        sql = """
            UPDATE alumni SET
                standardized_degree = %s,
                standardized_degree2 = %s,
                standardized_degree3 = %s,
                standardized_major = %s,
                standardized_major_alt = %s,
                standardized_major2 = %s,
                standardized_major3 = %s,
                discipline = %s
            WHERE id = %s
        """
        upd_cur.execute(
            sql,
            (
                payload.get("standardized_degree"),
                payload.get("standardized_degree2"),
                payload.get("standardized_degree3"),
                payload.get("standardized_major"),
                payload.get("standardized_major_alt"),
                payload.get("standardized_major2"),
                payload.get("standardized_major3"),
                payload.get("discipline"),
                rid,
            ),
        )
        updated += 1
        if updated % 50 == 0:
            conn.commit()
            logger.info("Committed %s rows…", updated)

    if not dry_run:
        conn.commit()
    try:
        conn.close()
    except Exception:
        pass
    logger.info("Done. Updated %s rows (dry_run=%s).", updated, dry_run)


def main() -> None:
    ap = argparse.ArgumentParser(description="Recompute education standardization for all alumni.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-llm", action="store_true", help="Do not call Groq inside infer_discipline.")
    args = ap.parse_args()
    run(dry_run=args.dry_run, use_llm=not args.no_llm)


if __name__ == "__main__":
    main()
