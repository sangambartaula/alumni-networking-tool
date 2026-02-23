"""
Retroactive working_while_studying recomputation.

This script recomputes:
  - alumni.working_while_studying_status
  - alumni.working_while_studying

for all rows in `alumni`, using:
1) Date-based logic first
2) Strict UNT + Graduate Assistant fallback only when date logic is not computable
"""

import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "backend"))

import logging

from database import get_connection, init_db
from working_while_studying_status import (
    recompute_working_while_studying_status,
    status_to_bool,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_migration():
    logger.info("=" * 60)
    logger.info("WORKING WHILE STUDYING RETROACTIVE MIGRATION")
    logger.info("=" * 60)

    try:
        init_db()
    except Exception as exc:
        logger.warning(f"init_db() warning (may be safe): {exc}")

    conn = get_connection()
    updated = 0
    total = 0
    unchanged = 0

    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                """
                SELECT id, grad_year, school, school2, school3,
                       current_job_title, company, job_start_date, job_end_date,
                       exp2_title, exp2_company, exp2_dates,
                       exp3_title, exp3_company, exp3_dates,
                       working_while_studying, working_while_studying_status
                FROM alumni
                """
            )
            rows = cur.fetchall() or []

            total = len(rows)
            logger.info(f"Loaded {total} alumni rows")

            for row in rows:
                new_status = recompute_working_while_studying_status(row)
                new_bool = status_to_bool(new_status)

                existing_status = (row.get("working_while_studying_status") or "").strip().lower()
                existing_bool = row.get("working_while_studying")
                normalized_existing_bool = None if existing_bool is None else bool(existing_bool)

                if existing_status == new_status and normalized_existing_bool == new_bool:
                    unchanged += 1
                    continue

                cur.execute(
                    """
                    UPDATE alumni
                    SET working_while_studying_status = %s,
                        working_while_studying = %s
                    WHERE id = %s
                    """,
                    (new_status, new_bool, row["id"]),
                )
                updated += cur.rowcount

        conn.commit()

        logger.info("-" * 60)
        logger.info(f"Total rows scanned: {total}")
        logger.info(f"Rows updated:      {updated}")
        logger.info(f"Rows unchanged:    {unchanged}")
        logger.info("=" * 60)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    run_migration()
