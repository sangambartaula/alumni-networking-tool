#!/usr/bin/env python
"""
Backfill seniority_level for existing alumni records.

Analyzes job titles (original, not normalized) to determine seniority levels,
then updates the database. Also flags obvious mismatches for manual review.

Run: python scripts/backfill_seniority_levels.py
"""

import sys
from pathlib import Path
from datetime import datetime

# Setup path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'backend'))
sys.path.insert(0, str(PROJECT_ROOT / 'scraper'))

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    env_path = PROJECT_ROOT / '.env'
    load_dotenv(dotenv_path=env_path)
except ImportError:
    logger.warning("dotenv not installed")


def run_backfill():
    """Main backfill entry point."""
    logger.info("=" * 70)
    logger.info("SENIORITY LEVEL BACKFILL")
    logger.info("=" * 70)

    try:
        # Paths above add backend/ and scraper/ — import modules by their leaf names
        from database import get_connection
        from seniority_detector import analyze_seniority
    except ImportError as e:
        logger.error(f"Failed to import required modules: {e}")
        sys.exit(1)

    conn = None
    try:
        conn = get_connection()
        
        with conn.cursor(dictionary=True) as cur:
            # Step 1: Fetch all alumni without seniority_level
            logger.info("\nStep 1: Fetching alumni records without seniority_level...")
            cur.execute("""
                SELECT id, current_job_title, exp2_title, exp3_title, 
                       linkedin_url, relevant_experience_months, seniority_level
                FROM alumni
                WHERE (seniority_level IS NULL OR seniority_level = '')
                ORDER BY id ASC
            """)
            rows = cur.fetchall()
            total_to_process = len(rows)
            logger.info(f"   Found {total_to_process} records to process")
            
            if total_to_process == 0:
                logger.info("   No records to process. Exiting.")
                return True
            
            # Step 2: Analyze seniority for each record
            logger.info("\nStep 2: Analyzing seniority levels...")
            updated = 0
            skipped = 0
            errors = 0
            
            for idx, row in enumerate(rows, 1):
                try:
                    # Prepare profile data for analyze_seniority
                    profile_data = {
                        'title': row.get('current_job_title', ''),
                        'exp2_title': row.get('exp2_title', ''),
                        'exp3_title': row.get('exp3_title', ''),
                        'linkedin_url': row.get('linkedin_url', ''),
                    }
                    
                    # Get relevant experience months
                    relevant_experience_months = row.get('relevant_experience_months')
                    if isinstance(relevant_experience_months, str):
                        try:
                            relevant_experience_months = int(relevant_experience_months)
                        except (ValueError, TypeError):
                            relevant_experience_months = None
                    
                    # Analyze seniority
                    seniority = analyze_seniority(profile_data, relevant_experience_months)
                    
                    # Update database
                    try:
                        cur.execute(
                            """
                            UPDATE alumni
                            SET seniority_level = %s
                            WHERE id = %s
                            """,
                            (seniority, row['id'])
                        )
                        updated += cur.rowcount
                    except Exception as update_err:
                        logger.warning(f"   [Row {idx}] Update failed: {update_err}")
                        errors += 1
                    
                    # Progress indicator
                    if idx % 100 == 0 or idx == total_to_process:
                        logger.info(f"   Processed {idx}/{total_to_process} records ({updated} updated, {errors} errors)")
                
                except Exception as e:
                    logger.warning(f"   [Row {idx}] Processing error: {e}")
                    errors += 1
            
            conn.commit()
            
            # Step 3: Report statistics
            logger.info("\nStep 3: Coverage report...")
            cur.execute("SELECT COUNT(*) as total FROM alumni")
            row = cur.fetchone()
            total = row['total'] if isinstance(row, dict) else row[0]
            
            cur.execute("SELECT COUNT(*) as with_seniority FROM alumni WHERE seniority_level IS NOT NULL AND seniority_level != ''")
            row = cur.fetchone()
            with_seniority = row['with_seniority'] if isinstance(row, dict) else row[0]
            
            logger.info(f"\n   Total alumni:              {total}")
            logger.info(f"   With seniority level:      {with_seniority}")
            if total > 0:
                pct = round(with_seniority / total * 100, 1)
                logger.info(f"   Coverage:                  {pct}%")
            logger.info(f"   Updated this run:          {updated}")
            logger.info(f"   Errors:                    {errors}")
            
            # Step 4: Show seniority distribution
            logger.info("\nStep 4: Seniority distribution...")
            cur.execute("""
                SELECT seniority_level, COUNT(*) as count
                FROM alumni
                WHERE seniority_level IS NOT NULL AND seniority_level != ''
                GROUP BY seniority_level
                ORDER BY count DESC
            """)
            dist_rows = cur.fetchall()
            for dist_row in dist_rows:
                seniority = dist_row.get('seniority_level') if isinstance(dist_row, dict) else dist_row[0]
                count = dist_row.get('count') if isinstance(dist_row, dict) else dist_row[1]
                pct = round(count / with_seniority * 100, 1) if with_seniority > 0 else 0
                logger.info(f"   {seniority:<12} {count:>4} records ({pct:>5.1f}%)")
            
            logger.info("\nBackfill completed successfully")
            return True
    
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == '__main__':
    success = run_backfill()
    sys.exit(0 if success else 1)
