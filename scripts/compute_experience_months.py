"""
Retroactive Relevant Experience Months Computation Script

Computes relevant_experience_months for all alumni using EXISTING
job_X_is_relevant flags from the Groq-based Job Relevance Engine.
Does NOT re-call Groq — it only processes date arithmetic on the
already-scored job data.

For each alumni with up to 3 jobs:
  1. Filter to relevant-only jobs (job_X_is_relevant = 1/True)
  2. Parse start/end dates  (missing end → "Present")
  3. Merge overlapping date intervals
  4. Sum months → relevant_experience_months

Edge cases handled:
  - 0 jobs or no relevant jobs → 0
  - Missing/partial dates → skip that job gracefully
  - Overlapping ranges → merged before summing
  - Same-month start/end → counted as 1 month

Usage:
    python scripts/compute_experience_months.py
    python scripts/compute_experience_months.py --dry-run
    python scripts/compute_experience_months.py --force   # Re-process all rows
"""

import os
import sys
import re
import argparse
import logging
from pathlib import Path
from datetime import datetime

# ── Project setup ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'backend'))
sys.path.insert(0, str(PROJECT_ROOT / 'scraper'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    logger.warning("dotenv not installed — assuming env vars are already set")


# ── Date parsing helpers (self-contained, no Groq dependency) ──

# Month abbreviation lookup
_MONTH_MAP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
    'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
    'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'june': 6, 'july': 7, 'august': 8, 'september': 9,
    'october': 10, 'november': 11, 'december': 12,
}


def _parse_date_to_month_year(date_str):
    """
    Parse a date string to (year, month) tuple.

    Supported formats:
      - "Jan 2020", "January 2020"
      - "2020" (year only → defaults to January)
      - "Present" → current month
      - Empty/None → None

    Returns:
        (year, month) tuple or None
    """
    if not date_str:
        return None

    text = str(date_str).strip()
    if not text:
        return None

    # "Present" means current date
    if text.lower() in ('present', 'current', 'now'):
        now = datetime.now()
        return (now.year, now.month)

    # Try "Mon YYYY" or "Month YYYY" formats
    match = re.match(r'^([A-Za-z]+)\s+(\d{4})$', text)
    if match:
        month_str = match.group(1).lower()
        year = int(match.group(2))
        month = _MONTH_MAP.get(month_str)
        if month and 1900 <= year <= 2100:
            return (year, month)

    # Try "YYYY" (year only)
    match = re.match(r'^(\d{4})$', text)
    if match:
        year = int(match.group(1))
        if 1900 <= year <= 2100:
            return (year, 1)  # Default to January

    # Try to find a year in the text as last resort
    years = re.findall(r'(19\d{2}|20\d{2})', text)
    if years:
        year = int(years[-1])  # Use last year found
        # Try to find a month too
        for word in text.lower().split():
            if word in _MONTH_MAP:
                return (year, _MONTH_MAP[word])
        return (year, 1)

    return None


def _split_date_range(date_range_str):
    """
    Split "Mar 2020 - Dec 2022" into (start_str, end_str).
    Handles separators: " - ", " – ", " — ", " to ".
    """
    if not date_range_str:
        return ('', '')

    text = str(date_range_str).strip()
    for sep in [' - ', ' – ', ' — ', ' to ']:
        if sep in text:
            parts = text.split(sep, 1)
            return (parts[0].strip(), parts[1].strip())

    # Single date → treat as both start and end
    return (text, text)


def _merge_intervals(intervals):
    """Merge overlapping or adjacent (year, month) intervals."""
    if not intervals:
        return []

    sorted_intervals = sorted(intervals, key=lambda x: x[0])

    def _to_months(ym):
        return ym[0] * 12 + ym[1]

    merged = [sorted_intervals[0]]
    for start, end in sorted_intervals[1:]:
        prev_start, prev_end = merged[-1]
        # Overlapping or adjacent: start <= prev_end + 1 month
        if _to_months(start) <= _to_months(prev_end) + 1:
            new_end = max(prev_end, end)
            merged[-1] = (prev_start, new_end)
        else:
            merged.append((start, end))

    return merged


def compute_months_from_row(row):
    """
    Compute relevant_experience_months from a single alumni DB row.

    Uses job_X_is_relevant flags and date fields to compute total
    months of relevant experience, merging overlapping ranges.

    Args:
        row: dict with DB column values

    Returns:
        int: total relevant experience months (0 if no relevant jobs)
    """
    from database import _parse_bool
    intervals = []

    # ── Job 1 ──
    if _parse_bool(row.get('job_1_is_relevant')):
        start = _parse_date_to_month_year(row.get('job_start_date', ''))
        end = _parse_date_to_month_year(row.get('job_end_date', ''))

        if start is not None:
            if end is None:
                # Missing end date → assume "Present"
                now = datetime.now()
                end = (now.year, now.month)
            if start > end:
                start, end = end, start
            intervals.append((start, end))

    # ── Job 2 ──
    if _parse_bool(row.get('job_2_is_relevant')):
        dates2 = row.get('exp2_dates', '') or ''
        start_str, end_str = _split_date_range(dates2)
        start = _parse_date_to_month_year(start_str)
        end = _parse_date_to_month_year(end_str)

        if start is not None:
            if end is None:
                now = datetime.now()
                end = (now.year, now.month)
            if start > end:
                start, end = end, start
            intervals.append((start, end))

    # ── Job 3 ──
    if _parse_bool(row.get('job_3_is_relevant')):
        dates3 = row.get('exp3_dates', '') or ''
        start_str, end_str = _split_date_range(dates3)
        start = _parse_date_to_month_year(start_str)
        end = _parse_date_to_month_year(end_str)

        if start is not None:
            if end is None:
                now = datetime.now()
                end = (now.year, now.month)
            if start > end:
                start, end = end, start
            intervals.append((start, end))

    if not intervals:
        return 0

    # Merge overlapping intervals and sum months
    merged = _merge_intervals(intervals)
    total_months = 0
    for (sy, sm), (ey, em) in merged:
        months = (ey - sy) * 12 + (em - sm)
        total_months += max(months, 0) + 1  # +1 to include end month

    return total_months


# ── Main backfill logic ──────────────────────────────────────

def run_compute(dry_run=False, force=False):
    """Main entry point: compute relevant_experience_months for all alumni."""
    from database import get_connection, ensure_experience_analysis_columns

    logger.info("=" * 60)
    logger.info("RELEVANT EXPERIENCE MONTHS — RETROACTIVE COMPUTATION")
    logger.info("=" * 60)

    # Step 0: Ensure columns exist
    logger.info("\n📦 Step 0: Ensuring schema columns exist...")
    try:
        ensure_experience_analysis_columns()
    except Exception as e:
        logger.warning(f"Column ensure issue (may be fine): {e}")

    # Step 1: Fetch alumni rows
    logger.info("\nStep 1: Fetching alumni records...")
    conn = get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            if force:
                cur.execute("""
                    SELECT id, first_name, last_name,
                           job_start_date, job_end_date,
                           exp2_dates, exp3_dates,
                           job_1_is_relevant, job_2_is_relevant, job_3_is_relevant,
                           relevant_experience_months
                    FROM alumni
                    ORDER BY id ASC
                """)
            else:
                # Only rows that have at least one relevance flag set
                # but are missing relevant_experience_months
                cur.execute("""
                    SELECT id, first_name, last_name,
                           job_start_date, job_end_date,
                           exp2_dates, exp3_dates,
                           job_1_is_relevant, job_2_is_relevant, job_3_is_relevant,
                           relevant_experience_months
                    FROM alumni
                    WHERE relevant_experience_months IS NULL
                    ORDER BY id ASC
                """)
            rows = cur.fetchall() or []
    except Exception as e:
        logger.error(f"❌ Error fetching alumni: {e}")
        conn.close()
        return
    finally:
        pass  # conn stays open for updates

    logger.info(f"   Found {len(rows)} alumni to process")

    if not rows:
        logger.info("✅ All alumni already processed. Use --force to re-process.")
        conn.close()
        return

    # Step 2: Compute and update
    logger.info("\nStep 2: Computing relevant_experience_months...")
    processed = 0
    updated = 0
    errors = 0

    try:
        with conn.cursor() as cur:
            for row in rows:
                alumni_id = row.get('id')
                try:
                    months = compute_months_from_row(row)
                    name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip()

                    if dry_run:
                        logger.info(
                            f"  [DRY RUN] {name} (ID {alumni_id}): "
                            f"relevant_experience_months = {months}"
                        )
                        processed += 1
                        continue

                    # Update database — try MySQL syntax first, fall back to SQLite
                    try:
                        cur.execute(
                            "UPDATE alumni SET relevant_experience_months = %s WHERE id = %s",
                            (months, alumni_id)
                        )
                    except Exception:
                        cur.execute(
                            "UPDATE alumni SET relevant_experience_months = ? WHERE id = ?",
                            (months, alumni_id)
                        )

                    updated += 1
                    processed += 1

                    # Batch commit every 50 rows
                    if processed % 50 == 0:
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
        logger.info("\nStep 3: Updating CSV file...")
        _update_csv_from_db()

    # Summary
    logger.info("\n" + "=" * 60)
    if dry_run:
        logger.info(f"🔍 DRY RUN complete: {processed} profiles analyzed")
    else:
        logger.info(f"🎉 Computation complete!")
        logger.info(f"   Processed: {processed}")
        logger.info(f"   Updated:   {updated}")
        logger.info(f"   Errors:    {errors}")
    logger.info("=" * 60)


def _update_csv_from_db():
    """Re-export relevant_experience_months into the scraper CSV."""
    import pandas as pd
    from database import get_connection

    csv_path = PROJECT_ROOT / 'scraper' / 'output' / 'UNT_Alumni_Data.csv'

    if not csv_path.exists():
        logger.info("No CSV file found to update")
        return

    try:
        df = pd.read_csv(csv_path, encoding='utf-8')

        conn = get_connection()
        try:
            with conn.cursor(dictionary=True) as cur:
                cur.execute("""
                    SELECT linkedin_url, relevant_experience_months
                    FROM alumni
                    WHERE linkedin_url IS NOT NULL
                      AND relevant_experience_months IS NOT NULL
                """)
                db_rows = cur.fetchall() or []
        finally:
            conn.close()

        if not db_rows:
            logger.info("No DB data to merge into CSV")
            return

        # Build lookup by normalized URL
        url_data = {}
        for row in db_rows:
            url = (row.get('linkedin_url') or '').strip().rstrip('/')
            if url:
                url_data[url] = row.get('relevant_experience_months')

        # Ensure column exists and is numeric to avoid FutureWarning and type errors
        if 'relevant_experience_months' not in df.columns:
            df['relevant_experience_months'] = pd.array([pd.NA] * len(df), dtype='Int64')
        else:
            df['relevant_experience_months'] = pd.to_numeric(df['relevant_experience_months'], errors='coerce').astype('Int64')

        # Merge
        csv_updated = 0
        for idx, row in df.iterrows():
            url = str(row.get('linkedin_url', '')).strip().rstrip('/')
            if url in url_data:
                val = url_data[url]
                if val is not None:
                    try:
                        df.at[idx, 'relevant_experience_months'] = int(float(val))
                    except (ValueError, TypeError):
                        df.at[idx, 'relevant_experience_months'] = pd.NA
                else:
                    df.at[idx, 'relevant_experience_months'] = pd.NA
                csv_updated += 1

        df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"✅ Updated CSV with {csv_updated} experience month values")

    except Exception as e:
        logger.error(f"❌ Error updating CSV: {e}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute relevant_experience_months from existing job relevance flags"
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
    run_compute(dry_run=args.dry_run, force=args.force)
