#!/usr/bin/env python3
"""
Migration script for Education Schema Refactor.

Can be run standalone to:
  1. Add missing columns to the database (works on MySQL and SQLite)
  2. Copy existing `education` → `school` where school is NULL
  3. Run standardize_degree() / standardize_major() on all rows

Usage:
    python migrations/migrate_education_schema.py
"""

import os
import sys
import logging

# Resolve paths
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backend"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger("migrate_education")


def migrate():
    """Run the education schema migration."""
    from database import ensure_education_columns, get_connection

    # Step 1: Ensure columns exist + migrate education → school
    logger.info("Step 1: Ensuring education columns exist...")
    ensure_education_columns()

    # Step 2: Run normalization on all existing rows
    logger.info("Step 2: Normalizing existing degree/major values...")
    try:
        from degree_normalization import standardize_degree
        from major_normalization import standardize_major
    except ImportError as e:
        logger.error(f"Cannot import normalization modules: {e}")
        return

    conn = get_connection()
    try:
        cur = conn.cursor()

        # Determine placeholder style
        is_sqlite = hasattr(conn, 'execute') and not hasattr(conn, 'cmd_query')
        ph = "?" if is_sqlite else "%s"

        # Fetch rows needing normalization
        cur.execute("""
            SELECT id, degree, major, degree2, major2, degree3, major3
            FROM alumni
        """)
        rows = cur.fetchall()
        updated = 0

        for row in rows:
            if isinstance(row, dict):
                rid = row['id']
                vals = [row.get('degree'), row.get('major'),
                        row.get('degree2'), row.get('major2'),
                        row.get('degree3'), row.get('major3')]
            else:
                rid = row[0]
                vals = list(row[1:])

            degree, major, degree2, major2, degree3, major3 = vals

            std_d = standardize_degree(degree or "") if degree else None
            std_m = standardize_major(major or "") if major else None
            std_d2 = standardize_degree(degree2 or "") if degree2 else None
            std_m2 = standardize_major(major2 or "") if major2 else None
            std_d3 = standardize_degree(degree3 or "") if degree3 else None
            std_m3 = standardize_major(major3 or "") if major3 else None

            cur.execute(f"""
                UPDATE alumni SET
                    standardized_degree = {ph},
                    standardized_major = {ph},
                    standardized_degree2 = {ph},
                    standardized_major2 = {ph},
                    standardized_degree3 = {ph},
                    standardized_major3 = {ph}
                WHERE id = {ph}
            """, (std_d, std_m, std_d2, std_m2, std_d3, std_m3, rid))
            updated += 1

        conn.commit()
        logger.info(f"✅ Normalized {updated} rows")
    except Exception as e:
        logger.error(f"Error during normalization: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    migrate()
