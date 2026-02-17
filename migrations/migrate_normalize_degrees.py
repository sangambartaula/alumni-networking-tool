"""
Retroactive Degree Normalization Migration

This script:
1. Ensures the schema is ready (normalized_degrees table + alumni columns)
2. Reads all DISTINCT degree values from the alumni table
3. Normalizes each deterministically
4. Inserts canonical entries into normalized_degrees (idempotent)
5. Updates alumni.normalized_degree_id via lookup
6. Reports coverage statistics

Safe to run multiple times — does NOT modify raw degree data.
"""

import sys
import os

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "backend"))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scraper"))

from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# Force SQLite fallback mode so the migration works even when MySQL is unreachable
os.environ["USE_SQLITE_FALLBACK"] = "1"

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

from backend.database import get_connection, init_db, ensure_normalized_degree_column
from backend.degree_normalization import normalize_degree_deterministic


def _test_mysql_reachable(timeout=5) -> bool:
    """Quick check if MySQL is reachable (with timeout)."""
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=os.getenv('MYSQLHOST'),
            user=os.getenv('MYSQLUSER'),
            password=os.getenv('MYSQLPASSWORD'),
            database=os.getenv('MYSQL_DATABASE'),
            port=int(os.getenv('MYSQLPORT', 3306)),
            connection_timeout=timeout
        )
        conn.close()
        return True
    except Exception:
        return False


def run_migration():
    """Run the retroactive degree normalization migration."""

    logger.info("=" * 60)
    logger.info("RETROACTIVE DEGREE NORMALIZATION MIGRATION")
    logger.info("=" * 60)

    # Step 1: Ensure schema is ready
    logger.info("\n📋 Step 1: Ensuring schema is ready...")

    if _test_mysql_reachable():
        logger.info("☁️ MySQL is reachable — using cloud database")
    else:
        logger.info("📴 MySQL unreachable — using local SQLite database")
        os.environ["DISABLE_DB"] = "1"

    try:
        init_db()
    except Exception as e:
        logger.warning(f"init_db raised (may be OK if tables exist): {e}")

    ensure_normalized_degree_column()
    logger.info("✅ Schema ready")

    # Step 2: Fetch all distinct degree values
    logger.info("\n📋 Step 2: Fetching distinct degree values from alumni...")
    conn = get_connection()
    is_sqlite = hasattr(conn, 'execute') and not hasattr(conn, 'cmd_query')

    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT degree FROM alumni WHERE degree IS NOT NULL AND degree != ''")
        rows = cur.fetchall()

        raw_degrees = []
        for row in rows:
            if isinstance(row, dict):
                raw_degrees.append(row['degree'])
            elif hasattr(row, '__getitem__'):
                raw_degrees.append(row[0])

        logger.info(f"Found {len(raw_degrees)} distinct degree values")

        if not raw_degrees:
            logger.info("No degrees to normalize. Migration complete.")
            try: conn.close()
            except Exception: pass
            return

        # Step 3: Normalize and insert canonical entries
        logger.info("\n📋 Step 3: Normalizing degrees and inserting canonical entries...")
        normalized_count = 0
        skipped_count = 0

        for raw in raw_degrees:
            normalized = normalize_degree_deterministic(raw)
            if not normalized:
                skipped_count += 1
                continue

            # Insert canonical entry (idempotent)
            cur = conn.cursor()
            if is_sqlite:
                cur.execute(
                    "INSERT OR IGNORE INTO normalized_degrees (normalized_degree) VALUES (?)",
                    (normalized,)
                )
            else:
                cur.execute(
                    "INSERT IGNORE INTO normalized_degrees (normalized_degree) VALUES (%s)",
                    (normalized,)
                )
            normalized_count += 1

        conn.commit()
        logger.info(f"✅ Processed {normalized_count} degrees, skipped {skipped_count} empty/null")

        # Step 4: Build lookup and update alumni records
        logger.info("\n📋 Step 4: Fetching normalized degree IDs...")
        cur = conn.cursor()
        cur.execute("SELECT id, normalized_degree FROM normalized_degrees")
        norm_rows = cur.fetchall()

        norm_lookup = {}  # normalized_string → id
        for row in norm_rows:
            if isinstance(row, dict):
                norm_lookup[row['normalized_degree']] = row['id']
            else:
                norm_lookup[row[1]] = row[0]

        logger.info(f"Found {len(norm_lookup)} canonical degree entries")

        # Step 5: Update alumni.normalized_degree_id
        logger.info("\n📋 Step 5: Updating alumni records with normalized_degree_id...")
        updated = 0
        no_match = 0

        param = "?" if is_sqlite else "%s"

        for raw in raw_degrees:
            normalized = normalize_degree_deterministic(raw)
            if not normalized:
                continue

            norm_id = norm_lookup.get(normalized)
            if not norm_id:
                no_match += 1
                continue

            # Only update records where normalized_degree_id is NULL (never overwrite)
            cur = conn.cursor()
            cur.execute(f"""
                UPDATE alumni 
                SET normalized_degree_id = {param}
                WHERE degree = {param}
                  AND (normalized_degree_id IS NULL OR normalized_degree_id = 0)
            """, (norm_id, raw))

            if hasattr(cur, 'rowcount'):
                updated += cur.rowcount

        conn.commit()

        # Count how many already had it set
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM alumni WHERE normalized_degree_id IS NOT NULL AND normalized_degree_id != 0")
        total_with_id = cur.fetchone()

        total_set = total_with_id[0] if not isinstance(total_with_id, dict) else total_with_id.get('COUNT(*)', 0)

        # Count total alumni with degrees
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM alumni WHERE degree IS NOT NULL AND degree != ''")
        total_with_degree = cur.fetchone()

        total_deg = total_with_degree[0] if not isinstance(total_with_degree, dict) else total_with_degree.get('COUNT(*)', 0)

        # Step 6: Report
        logger.info("\n" + "=" * 60)
        logger.info("MIGRATION RESULTS")
        logger.info("=" * 60)
        logger.info(f"Total distinct raw degrees:    {len(raw_degrees)}")
        logger.info(f"Canonical degree entries:      {len(norm_lookup)}")
        logger.info(f"Alumni records with degree:    {total_deg}")
        logger.info(f"Alumni records with norm ID:   {total_set}")
        logger.info(f"Coverage:                      {round(total_set/total_deg*100, 1) if total_deg > 0 else 0}%")
        logger.info(f"Records updated this run:      {updated}")
        logger.info(f"No match (empty normalized):   {no_match}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    run_migration()
