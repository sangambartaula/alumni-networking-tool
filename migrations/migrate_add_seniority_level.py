#!/usr/bin/env python
"""
Add seniority_level column to alumni table.

This migration adds a new VARCHAR(20) column to store seniority level classification.
Used in conjunction with seniority_detector.py to classify alumni by experience level.

Seniority levels: Intern, Junior, Mid, Senior, Manager, Director, Executive

Run: python migrations/migrate_add_seniority_level.py
"""

import sys
from pathlib import Path

# Ensure backend is importable
BACKEND_DIR = Path(__file__).resolve().parent.parent / 'backend'
sys.path.insert(0, str(BACKEND_DIR))

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    # Load .env from project root
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(dotenv_path=env_path)
except ImportError:
    logger.warning("dotenv not installed - skipping .env load")


def run_migration():
    """Main migration entry point."""
    logger.info("=" * 60)
    logger.info("SENIORITY LEVEL MIGRATION")
    logger.info("=" * 60)

    try:
        from database import get_connection
    except ImportError as e:
        logger.error(f"Failed to import database module: {e}")
        sys.exit(1)

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Add seniority_level column if it doesn't exist
            try:
                cur.execute("""
                    ALTER TABLE alumni
                    ADD COLUMN seniority_level VARCHAR(20) DEFAULT NULL
                """)
                logger.info("✅ Added seniority_level column to alumni table")
            except Exception as e:
                if "Duplicate column name" in str(e) or "duplicate column name" in str(e).lower():
                    logger.info("✅ seniority_level column already exists")
                else:
                    logger.error(f"❌ Error adding seniority_level column: {e}")
                    raise

            conn.commit()
            logger.info("\n✅ Migration completed successfully")

            # Report statistics
            try:
                cur.execute("SELECT COUNT(*) as total FROM alumni")
                row = cur.fetchone()
                total = row['total'] if isinstance(row, dict) else row[0]
                
                cur.execute("SELECT COUNT(*) as with_seniority FROM alumni WHERE seniority_level IS NOT NULL")
                row = cur.fetchone()
                with_seniority = row['with_seniority'] if isinstance(row, dict) else row[0]
                
                logger.info(f"\n📊 Statistics:")
                logger.info(f"   Total alumni:        {total}")
                logger.info(f"   With seniority level: {with_seniority}")
                if total > 0:
                    pct = round(with_seniority / total * 100, 1)
                    logger.info(f"   Coverage:            {pct}%")
            except Exception as e:
                logger.debug(f"Could not fetch statistics: {e}")

    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        sys.exit(1)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == '__main__':
    run_migration()
