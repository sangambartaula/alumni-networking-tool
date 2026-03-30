"""
Retroactive Experience Analysis Backfill Script

Processes all existing alumni rows and populates:
- job_1_relevance_score, job_2_relevance_score, job_3_relevance_score
- job_1_is_relevant, job_2_is_relevant, job_3_is_relevant
- relevant_experience_months
- seniority_level

Works with both MySQL and SQLite.

Usage:
    python scripts/backfill_experience_analysis.py
    python scripts/backfill_experience_analysis.py --dry-run
    python scripts/backfill_experience_analysis.py --force   # Re-process all, even already-computed rows

Each run logs **old → new** relevance scores per job (at INFO) when Groq scoring runs,
unless ``--quiet-relevance-audit`` is set. Use this to audit historical rows after changing
``relevance_scorer.py``.
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# Ensure imports work
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'backend'))
sys.path.insert(0, str(PROJECT_ROOT / 'scraper'))

import groq_retry_patch  # noqa: E402 — Groq HTTP retries (GROQ_RETRY_DELAY_SECONDS)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    logger.warning("dotenv not installed — assuming env vars are already set")


def _norm_relevance_scalar(val):
    """Normalize DB/CSV score for display; None if missing."""
    if val is None or val == '':
        return None
    try:
        return round(float(val), 4)
    except (TypeError, ValueError):
        return val


def _relevance_audit_message(alumni_id, display_name, row, relevance, rel_flags):
    """
    Build one-line old→new relevance audit for three jobs + flags.

    rel_flags: (job_1_is_relevant, job_2_is_relevant, job_3_is_relevant) after scoring.
    """
    parts = []
    for i in range(1, 4):
        sk = f'job_{i}_relevance_score'
        fk = f'job_{i}_is_relevant'
        old_s = _norm_relevance_scalar(row.get(sk))
        new_s = _norm_relevance_scalar(relevance.get(sk))
        old_f = row.get(fk)
        new_f = rel_flags[i - 1] if i - 1 < len(rel_flags) else None
        parts.append(
            f"j{i}: {old_s}→{new_s} rel:{old_f}→{new_f}"
        )
    months_old = row.get('relevant_experience_months')
    months_new = relevance.get('relevant_experience_months')
    parts.append(f"months: {months_old}→{months_new}")
    return (
        f"[relevance-audit] id={alumni_id} name={display_name!r} | "
        + " | ".join(parts)
    )


def run_backfill(dry_run=False, force=False, quiet_relevance_audit=False):
    """Main backfill entry point."""
    from database import get_connection, ensure_experience_analysis_columns

    # Step 0: Ensure columns exist
    logger.info("=" * 60)
    logger.info("EXPERIENCE ANALYSIS BACKFILL")
    logger.info("=" * 60)
    
    logger.info("\n📦 Step 0: Ensuring schema is ready...")
    try:
        ensure_experience_analysis_columns()
    except Exception as e:
        logger.warning(f"Column ensure issue (may be fine): {e}")

    # Import scoring/seniority modules
    try:
        from relevance_scorer import analyze_profile_relevance, is_groq_available
        from seniority_detector import analyze_seniority
    except ImportError as e:
        logger.error(f"❌ Could not import scoring modules: {e}")
        logger.info("Make sure you're running from the project root directory.")
        return

    groq_ready = is_groq_available()
    if not groq_ready:
        logger.warning(
            "⚠️ Groq LLM not available — relevance scoring will be SKIPPED.\n"
            "   To enable: set GROQ_API_KEY in your .env file.\n"
            "   Seniority detection (keyword-based) will still run."
        )

    # Step 1: Fetch all alumni
    logger.info("\nStep 1: Fetching alumni records...")
    conn = get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            # Process ALL alumni rows (not only missing/empty fields).
            # Use --force to re-run even if values already exist.
            cur.execute("SELECT * FROM alumni ORDER BY id ASC")
            rows = cur.fetchall() or []
    except Exception as e:
        logger.error(f"❌ Error fetching alumni: {e}")
        conn.close()
        return
    
    logger.info(f"   Found {len(rows)} alumni to process")
    
    if not rows:
        logger.info("✅ All alumni already processed. Use --force to re-process.")
        conn.close()
        return

    # Step 2: Process each alumni
    logger.info("\nStep 2: Processing alumni...")
    processed = 0
    updated = 0
    errors = 0

    try:
        with conn.cursor() as cur:
            for row in rows:
                alumni_id = row.get('id')
                try:
                    # Build profile data dict compatible with scoring functions
                    profile_data = {
                        'title': row.get('current_job_title', ''),
                        'company': row.get('company', ''),
                        'major': row.get('major', ''),
                        'standardized_major': row.get('standardized_major', ''),
                        'job_start': row.get('job_start_date', ''),
                        'job_end': row.get('job_end_date', ''),
                        'job_employment_type': row.get('job_employment_type', ''),
                        'exp_2_title': row.get('exp2_title', ''),
                        'exp_2_company': row.get('exp2_company', ''),
                        'exp_2_dates': row.get('exp2_dates', ''),
                        'exp_3_title': row.get('exp3_title', ''),
                        'exp_3_company': row.get('exp3_company', ''),
                        'exp_3_dates': row.get('exp3_dates', ''),
                        'linkedin_url': row.get('linkedin_url', ''),
                    }

                    # Relevance scoring (requires Groq)
                    relevance = {}
                    if groq_ready:
                        relevance = analyze_profile_relevance(profile_data)

                    rel_flags = (
                        relevance.get('job_1_is_relevant'),
                        relevance.get('job_2_is_relevant'),
                        relevance.get('job_3_is_relevant'),
                    )
                    if groq_ready and relevance and not quiet_relevance_audit:
                        display_name = (
                            f"{row.get('first_name', '')} {row.get('last_name', '')}"
                            .strip() or f"#{alumni_id}"
                        )
                        logger.info(
                            _relevance_audit_message(
                                alumni_id, display_name, row, relevance, rel_flags
                            )
                        )

                    # Seniority detection (keyword-based, no LLM needed)
                    experience_months = relevance.get('relevant_experience_months')
                    seniority = analyze_seniority(profile_data, experience_months)

                    if dry_run:
                        name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
                        logger.info(
                            f"  [DRY RUN] {name}: seniority={seniority}, "
                            f"exp_months={experience_months}, "
                            f"scores={relevance.get('job_1_relevance_score')}/{relevance.get('job_2_relevance_score')}/{relevance.get('job_3_relevance_score')}"
                        )
                        processed += 1
                        continue

                    # Update the database row
                    try:
                        cur.execute("""
                            UPDATE alumni SET
                                job_1_relevance_score = %s,
                                job_2_relevance_score = %s,
                                job_3_relevance_score = %s,
                                job_1_is_relevant = %s,
                                job_2_is_relevant = %s,
                                job_3_is_relevant = %s,
                                relevant_experience_months = %s,
                                seniority_level = %s
                            WHERE id = %s
                        """, (
                            relevance.get('job_1_relevance_score'),
                            relevance.get('job_2_relevance_score'),
                            relevance.get('job_3_relevance_score'),
                            relevance.get('job_1_is_relevant'),
                            relevance.get('job_2_is_relevant'),
                            relevance.get('job_3_is_relevant'),
                            experience_months,
                            seniority,
                            alumni_id,
                        ))
                    except Exception:
                        # SQLite fallback
                        cur.execute("""
                            UPDATE alumni SET
                                job_1_relevance_score = ?,
                                job_2_relevance_score = ?,
                                job_3_relevance_score = ?,
                                job_1_is_relevant = ?,
                                job_2_is_relevant = ?,
                                job_3_is_relevant = ?,
                                relevant_experience_months = ?,
                                seniority_level = ?
                            WHERE id = ?
                        """, (
                            relevance.get('job_1_relevance_score'),
                            relevance.get('job_2_relevance_score'),
                            relevance.get('job_3_relevance_score'),
                            relevance.get('job_1_is_relevant'),
                            relevance.get('job_2_is_relevant'),
                            relevance.get('job_3_is_relevant'),
                            experience_months,
                            seniority,
                            alumni_id,
                        ))

                    updated += 1
                    processed += 1

                    # Batch commit every 25 rows
                    if processed % 25 == 0:
                        conn.commit()
                        logger.info(f"   Processed {processed}/{len(rows)} rows...")

                except Exception as e:
                    errors += 1
                    processed += 1
                    name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()
                    logger.warning(f"   ⚠️ Error processing {name} (ID {alumni_id}): {e}")
                    continue

            # Final commit
            if not dry_run:
                conn.commit()

    finally:
        try:
            conn.close()
        except Exception:
            pass

    # Step 3: Update CSV
    if not dry_run and updated > 0:
        _update_csv_from_db()

    # Summary
    logger.info("\n" + "=" * 60)
    if dry_run:
        logger.info(f"🔍 DRY RUN complete: {processed} profiles analyzed")
    else:
        logger.info(f"🎉 Backfill complete!")
        logger.info(f"   Processed: {processed}")
        logger.info(f"   Updated:   {updated}")
        logger.info(f"   Errors:    {errors}")
    logger.info("=" * 60)


def _update_csv_from_db():
    """Re-export the CSV from the database to include new columns."""
    import pandas as pd
    from database import get_connection
    import database_handler as dh

    csv_path = PROJECT_ROOT / 'scraper' / 'output' / 'UNT_Alumni_Data.csv'

    try:
        dh.ensure_alumni_output_csv(csv_path)
        if not csv_path.exists():
            logger.info("No CSV file after ensure; skipping CSV merge")
            return

        df = pd.read_csv(csv_path, encoding="utf-8")
        
        # Fetch updated data from DB
        conn = get_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT linkedin_url,
                           job_1_relevance_score, job_2_relevance_score, job_3_relevance_score,
                           job_1_is_relevant, job_2_is_relevant, job_3_is_relevant,
                           relevant_experience_months, seniority_level
                    FROM alumni
                    WHERE linkedin_url IS NOT NULL
                """)
                db_rows = cur.fetchall() or []
        finally:
            conn.close()

        if not db_rows:
            logger.info("No DB data to merge into CSV")
            return

        # Build lookup by URL
        url_data = {}
        for row in db_rows:
            url = (row.get('linkedin_url') or '').strip().rstrip('/')
            if url:
                url_data[url] = row

        # Ensure new columns exist in DataFrame with correct dtypes
        # to avoid FutureWarning about incompatible dtype assignment.
        _col_dtypes = {
            'job_1_relevance_score': 'float64',
            'job_2_relevance_score': 'float64',
            'job_3_relevance_score': 'float64',
            'job_1_is_relevant': 'Int64',  # nullable integer
            'job_2_is_relevant': 'Int64',
            'job_3_is_relevant': 'Int64',
            'relevant_experience_months': 'Int64',
            'seniority_level': 'object',
        }
        for col, dtype in _col_dtypes.items():
            if col not in df.columns:
                df[col] = pd.array([pd.NA] * len(df), dtype=dtype)
            else:
                # Cast existing columns to the correct dtype to avoid
                # FutureWarning when assigning values row-by-row.
                try:
                    df[col] = df[col].astype(dtype)
                except (ValueError, TypeError):
                    pass  # keep as-is if cast fails

        new_cols = list(_col_dtypes.keys())

        # Merge data
        updated = 0
        for idx, row in df.iterrows():
            url = str(row.get('linkedin_url', '')).strip().rstrip('/')
            if url in url_data:
                db_row = url_data[url]
                for col in new_cols:
                    val = db_row.get(col)
                    if val is not None:
                        df.at[idx, col] = val
                    else:
                        df.at[idx, col] = pd.NA
                updated += 1

        df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"✅ Updated CSV with {updated} rows")

    except Exception as e:
        logger.error(f"❌ Error updating CSV: {e}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Retroactive experience analysis backfill"
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Show what would be computed without writing to DB'
    )
    parser.add_argument(
        '--force', action='store_true',
        help='Re-process all alumni, even those already computed'
    )
    parser.add_argument(
        '--quiet-relevance-audit',
        action='store_true',
        help='Disable per-row [relevance-audit] INFO logs (old→new scores)',
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_backfill(
        dry_run=args.dry_run,
        force=args.force,
        quiet_relevance_audit=args.quiet_relevance_audit,
    )
