#!/usr/bin/env python3
"""
Retroactive standardization migration.

Recomputes, for all alumni rows:
- standardized_degree / standardized_degree2 / standardized_degree3
- standardized_major / standardized_major2 / standardized_major3
- normalized_job_title_id (from current_job_title)
- normalized_company_id (from company)

Raw source fields are NOT modified.
"""

import os
import sys
import logging
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend"))
sys.path.insert(0, str(ROOT / "scraper"))

from database import (
    get_connection,
    init_db,
    ensure_normalized_job_title_column,
    ensure_normalized_company_column,
)
from degree_normalization import standardize_degree
from major_normalization import standardize_major
from job_title_normalization import normalize_title_deterministic
from company_normalization import normalize_company_deterministic

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def _is_sqlite(conn) -> bool:
    return hasattr(conn, "execute") and not hasattr(conn, "cmd_query")


def _upsert_lookup(cur, is_sqlite: bool, table: str, column: str, value: str | None) -> int | None:
    if not value or not str(value).strip():
        return None

    text = str(value).strip()
    if is_sqlite:
        cur.execute(f"INSERT OR IGNORE INTO {table} ({column}) VALUES (?)", (text,))
        cur.execute(f"SELECT id FROM {table} WHERE {column} = ?", (text,))
    else:
        cur.execute(
            f"INSERT INTO {table} ({column}) VALUES (%s) "
            f"ON DUPLICATE KEY UPDATE {column}=VALUES({column})",
            (text,),
        )
        cur.execute(f"SELECT id FROM {table} WHERE {column} = %s", (text,))

    row = cur.fetchone()
    if not row:
        return None
    if isinstance(row, dict):
        return row.get("id")
    return row[0]


def run_migration() -> None:
    logger.info("=" * 70)
    logger.info("RETROACTIVE STANDARDIZATION POLICY MIGRATION")
    logger.info("=" * 70)

    try:
        init_db()
    except Exception as e:
        logger.warning(f"init_db() warning: {e}")

    ensure_normalized_job_title_column()
    ensure_normalized_company_column()

    conn = get_connection()
    is_sqlite = _is_sqlite(conn)
    ph = "?" if is_sqlite else "%s"

    updated_rows = 0
    title_links = 0
    company_links = 0

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, degree, degree2, degree3, major, major2, major3, current_job_title, company
            FROM alumni
            """
        )
        rows = cur.fetchall() or []
        logger.info(f"Loaded {len(rows)} alumni rows")

        for idx, row in enumerate(rows, start=1):
            if isinstance(row, dict):
                rid = row.get("id")
                degree, degree2, degree3 = row.get("degree"), row.get("degree2"), row.get("degree3")
                major, major2, major3 = row.get("major"), row.get("major2"), row.get("major3")
                current_title, company = row.get("current_job_title"), row.get("company")
            else:
                rid = row[0]
                degree, degree2, degree3 = row[1], row[2], row[3]
                major, major2, major3 = row[4], row[5], row[6]
                current_title, company = row[7], row[8]

            std_degree = standardize_degree(degree or "")
            std_degree2 = standardize_degree(degree2 or "")
            std_degree3 = standardize_degree(degree3 or "")

            std_major = standardize_major(major or "", current_title or "")
            std_major2 = standardize_major(major2 or "", current_title or "")
            std_major3 = standardize_major(major3 or "", current_title or "")

            norm_title = normalize_title_deterministic(current_title or "")
            norm_company = normalize_company_deterministic(company or "")

            title_id = _upsert_lookup(cur, is_sqlite, "normalized_job_titles", "normalized_title", norm_title)
            company_id = _upsert_lookup(cur, is_sqlite, "normalized_companies", "normalized_company", norm_company)

            if title_id is not None:
                title_links += 1
            if company_id is not None:
                company_links += 1

            cur.execute(
                f"""
                UPDATE alumni
                SET standardized_degree = {ph},
                    standardized_degree2 = {ph},
                    standardized_degree3 = {ph},
                    standardized_major = {ph},
                    standardized_major2 = {ph},
                    standardized_major3 = {ph},
                    normalized_job_title_id = {ph},
                    normalized_company_id = {ph}
                WHERE id = {ph}
                """,
                (
                    std_degree,
                    std_degree2,
                    std_degree3,
                    std_major,
                    std_major2,
                    std_major3,
                    title_id,
                    company_id,
                    rid,
                ),
            )
            updated_rows += 1

            if idx % 100 == 0:
                conn.commit()
                logger.info(f"Processed {idx}/{len(rows)} rows...")

        conn.commit()

        # Optional compaction: keep only referenced normalized rows.
        cur.execute(
            """
            DELETE FROM normalized_job_titles
            WHERE id NOT IN (
                SELECT DISTINCT normalized_job_title_id
                FROM alumni
                WHERE normalized_job_title_id IS NOT NULL
            )
            """
        )
        dropped_titles = cur.rowcount if hasattr(cur, "rowcount") else 0

        cur.execute(
            """
            DELETE FROM normalized_companies
            WHERE id NOT IN (
                SELECT DISTINCT normalized_company_id
                FROM alumni
                WHERE normalized_company_id IS NOT NULL
            )
            """
        )
        dropped_companies = cur.rowcount if hasattr(cur, "rowcount") else 0

        conn.commit()

        logger.info("=" * 70)
        logger.info("MIGRATION COMPLETE")
        logger.info(f"Rows updated:                {updated_rows}")
        logger.info(f"Rows linked to norm titles:  {title_links}")
        logger.info(f"Rows linked to norm company: {company_links}")
        logger.info(f"Dropped unused norm titles:  {dropped_titles}")
        logger.info(f"Dropped unused norm company: {dropped_companies}")
        logger.info("=" * 70)

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    run_migration()
