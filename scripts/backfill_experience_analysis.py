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


def run_backfill(dry_run=False, force=False):
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
            if force:
                cur.execute("SELECT * FROM alumni ORDER BY id ASC")
            else:
                # Only process rows that haven't been analyzed yet
                cur.execute("""
                    SELECT * FROM alumni
                    WHERE seniority_level IS NULL
                       OR (job_1_relevance_score IS NULL AND current_job_title IS NOT NULL AND current_job_title != '')
                    ORDER BY id ASC
                """)
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

        # Ensure new columns exist in DataFrame
        new_cols = [
            'job_1_relevance_score', 'job_2_relevance_score', 'job_3_relevance_score',
            'job_1_is_relevant', 'job_2_is_relevant', 'job_3_is_relevant',
            'relevant_experience_months', 'seniority_level',
        ]
        for col in new_cols:
            if col not in df.columns:
                df[col] = ''

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
                        df.at[idx, col] = ''
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
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_backfill(dry_run=args.dry_run, force=args.force)
