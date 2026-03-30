#!/usr/bin/env python3
"""
Add standardized_major_alt column to the alumni table.

This column stores the secondary major for multi-entry mappings.
Currently only "Computer Science and Engineering" maps to two majors:
  standardized_major     = "Computer Science"
  standardized_major_alt = "Computer Engineering"
"""

import os
import sys
import logging
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "scraper"))

from database import get_connection, init_db

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def _is_sqlite(conn) -> bool:
    return hasattr(conn, "execute") and not hasattr(conn, "cmd_query")


def run_migration() -> None:
    logger.info("Adding standardized_major_alt column to alumni table")

    try:
        init_db()
    except Exception as e:
        logger.warning(f"init_db() warning: {e}")

    conn = get_connection()
    is_sqlite = _is_sqlite(conn)

    try:
        cur = conn.cursor()
        if is_sqlite:
            try:
                cur.execute("ALTER TABLE alumni ADD COLUMN standardized_major_alt TEXT")
                logger.info("Added standardized_major_alt column (SQLite)")
            except Exception as e:
                if "duplicate column" in str(e).lower():
                    logger.info("Column already exists (SQLite)")
                else:
                    raise
        else:
            try:
                cur.execute("ALTER TABLE alumni ADD COLUMN standardized_major_alt VARCHAR(255) DEFAULT NULL")
                logger.info("Added standardized_major_alt column (MySQL)")
            except Exception as e:
                if "Duplicate column name" in str(e):
                    logger.info("Column already exists (MySQL)")
                else:
                    raise
        conn.commit()
        logger.info("Migration complete")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    run_migration()
