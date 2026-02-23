"""
Retroactive Job Title Normalization Migration

Idempotent script that:
1. Ensures the normalized_job_titles table and FK column exist
2. Queries all DISTINCT current_job_title from alumni
3. Normalizes each deterministically (no Groq API calls)
4. Populates the normalized_job_titles table
5. Updates alumni.normalized_job_title_id via a lookup

Run:  python migrations/migrate_normalize_titles.py
"""

import os
import sys
from pathlib import Path

# Ensure backend is importable
BACKEND_DIR = Path(__file__).resolve().parent.parent / 'backend'
sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv

# Load .env from project root
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from database import get_connection, init_db, ensure_normalized_job_title_column
from job_title_normalization import normalize_title_deterministic


def run_migration():
    """Main migration entry point."""
    logger.info("=" * 60)
    logger.info("JOB TITLE NORMALIZATION MIGRATION")
    logger.info("=" * 60)

    # Step 0: Ensure schema is up to date
    logger.info("\n📦 Step 0: Ensuring schema is ready...")
    try:
        init_db()
    except Exception as e:
        logger.warning(f"init_db() issue (may be fine if tables exist): {e}")
    ensure_normalized_job_title_column()

    conn = get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            # Step 1: Get all distinct raw titles
            logger.info("\nStep 1: Fetching distinct job titles...")
            cur.execute("""
                SELECT DISTINCT current_job_title
                FROM alumni
                WHERE current_job_title IS NOT NULL
                  AND current_job_title != ''
            """)
            rows = cur.fetchall()
            raw_titles = [r['current_job_title'] for r in rows]
            logger.info(f"   Found {len(raw_titles)} distinct raw titles")

            # Step 2: Normalize each title and insert into lookup table
            logger.info("\nStep 2: Normalizing titles...")
            norm_map = {}  # raw -> normalized
            inserted = 0
            for raw in raw_titles:
                norm = normalize_title_deterministic(raw)
                norm_map[raw] = norm
                if norm:
                    try:
                        cur.execute(
                            "INSERT INTO normalized_job_titles (normalized_title) VALUES (%s) "
                            "ON DUPLICATE KEY UPDATE normalized_title = VALUES(normalized_title)",
                            (norm,)
                        )
                        if cur.rowcount == 1:
                            inserted += 1
                    except Exception:
                        # SQLite fallback
                        try:
                            cur.execute(
                                "INSERT OR IGNORE INTO normalized_job_titles (normalized_title) VALUES (?)",
                                (norm,)
                            )
                            if cur.rowcount == 1:
                                inserted += 1
                        except Exception as e2:
                            logger.warning(f"   Failed to insert '{norm}': {e2}")

            conn.commit()

            unique_norms = set(v for v in norm_map.values() if v)
            logger.info(f"   {len(raw_titles)} raw titles → {len(unique_norms)} normalized categories")
            logger.info(f"   {inserted} new normalized titles inserted")

            # Step 3: Fetch all normalized title IDs
            logger.info("\nStep 3: Linking alumni to normalized titles...")
            cur.execute("SELECT id, normalized_title FROM normalized_job_titles")
            norm_rows = cur.fetchall()
            title_to_id = {r['normalized_title']: r['id'] for r in norm_rows}

            # Step 4: Update alumni records
            updated = 0
            skipped = 0
            for raw, norm in norm_map.items():
                norm_id = title_to_id.get(norm)
                if norm_id is None:
                    skipped += 1
                    continue

                try:
                    cur.execute(
                        "UPDATE alumni SET normalized_job_title_id = %s "
                        "WHERE current_job_title = %s AND (normalized_job_title_id IS NULL OR normalized_job_title_id != %s)",
                        (norm_id, raw, norm_id)
                    )
                except Exception:
                    cur.execute(
                        "UPDATE alumni SET normalized_job_title_id = ? "
                        "WHERE current_job_title = ? AND (normalized_job_title_id IS NULL OR normalized_job_title_id != ?)",
                        (norm_id, raw, norm_id)
                    )
                updated += cur.rowcount

            conn.commit()
            logger.info(f"   Updated {updated} alumni records")
            if skipped:
                logger.info(f"   Skipped {skipped} titles (no normalized mapping)")

            # Step 5: Report coverage
            logger.info("\nStep 5: Coverage report...")
            cur.execute("SELECT COUNT(*) as total FROM alumni")
            total = cur.fetchone()['total']
            cur.execute("SELECT COUNT(*) as linked FROM alumni WHERE normalized_job_title_id IS NOT NULL")
            linked = cur.fetchone()['linked']
            cur.execute("SELECT COUNT(*) as norms FROM normalized_job_titles")
            norms = cur.fetchone()['norms']

            coverage = round(linked / total * 100, 1) if total > 0 else 0
            logger.info(f"   Total alumni:           {total}")
            logger.info(f"   Linked to norm title:   {linked} ({coverage}%)")
            logger.info(f"   Normalized categories:  {norms}")

            # Show top 10 most common normalized titles
            cur.execute("""
                SELECT njt.normalized_title, COUNT(*) as cnt
                FROM alumni a
                JOIN normalized_job_titles njt ON a.normalized_job_title_id = njt.id
                GROUP BY njt.normalized_title
                ORDER BY cnt DESC
                LIMIT 10
            """)
            top = cur.fetchall()
            if top:
                logger.info("\n   Top 10 normalized titles:")
                for row in top:
                    logger.info(f"     {row['cnt']:4d}  {row['normalized_title']}")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass

    logger.info("\n" + "=" * 60)
    logger.info("🎉 Migration complete!")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_migration()
