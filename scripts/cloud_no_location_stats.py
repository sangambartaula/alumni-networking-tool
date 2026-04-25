"""Cloud (MySQL): location missing breakdown + optional random sample.

Buckets (mutually exclusive):
  - location IS NULL
  - non-NULL but TRIM(location) = '' (empty / whitespace-only stored as empty)
  - literal Not Found / not_found (scraper placeholder)

Usage:
  python scripts/cloud_no_location_stats.py
  python scripts/cloud_no_location_stats.py --not-found-urls 10
  python scripts/cloud_no_location_stats.py --not-found-urls 10 --skip 10
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

WHERE = """(
    location IS NULL OR TRIM(location) = ''
    OR LOWER(TRIM(location)) IN ('not found', 'not_found')
)"""


def _empty(val) -> bool:
    return val is None or (isinstance(val, str) and not val.strip())


def main() -> int:
    parser = argparse.ArgumentParser(description="Cloud alumni location stats")
    parser.add_argument(
        "--not-found-urls",
        type=int,
        metavar="N",
        default=0,
        help="Print first N linkedin_url rows where location is Not Found (ordered by id), then exit.",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help="Skip this many Not Found rows (use with --not-found-urls for next pages).",
    )
    args = parser.parse_args()

    load_dotenv(PROJECT_ROOT / ".env")
    kw = {
        "host": os.getenv("MYSQLHOST", "").strip(),
        "user": os.getenv("MYSQLUSER", "").strip(),
        "password": os.getenv("MYSQLPASSWORD", "").strip(),
        "database": os.getenv("MYSQL_DATABASE", "").strip(),
        "port": int(os.getenv("MYSQLPORT", "3306")),
    }
    if not all([kw["host"], kw["user"], kw["database"]]):
        print("Missing MYSQLHOST / MYSQLUSER / MYSQL_DATABASE in .env", file=sys.stderr)
        return 1

    conn = mysql.connector.connect(**kw)
    try:
        if args.not_found_urls > 0:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT linkedin_url FROM alumni
                WHERE LOWER(TRIM(location)) IN ('not found', 'not_found')
                ORDER BY id ASC
                LIMIT %s OFFSET %s
                """,
                (int(args.not_found_urls), int(args.skip)),
            )
            for (url,) in cur.fetchall():
                print(url)
            cur.close()
            return 0

        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT COUNT(*) AS c FROM alumni")
        total = int(cur.fetchone()["c"])

        cur.execute(
            """
            SELECT
              SUM(location IS NULL) AS cnt_null,
              SUM(location IS NOT NULL AND TRIM(location) = '') AS cnt_empty,
              SUM(
                location IS NOT NULL
                AND TRIM(location) <> ''
                AND LOWER(TRIM(location)) IN ('not found', 'not_found')
              ) AS cnt_not_found
            FROM alumni
            """
        )
        b = cur.fetchone()
        c_null = int(b["cnt_null"] or 0)
        c_empty = int(b["cnt_empty"] or 0)
        c_nf = int(b["cnt_not_found"] or 0)
        union = c_null + c_empty + c_nf

        cur.execute(f"SELECT COUNT(*) AS c FROM alumni WHERE {WHERE}")
        n_where = int(cur.fetchone()["c"])

        print(f"Cloud alumni total: {total}\n")
        print("Location buckets (mutually exclusive):")
        print(f"  NULL:              {c_null}")
        print(f"  empty string:      {c_empty}  (non-NULL but blank / whitespace-only)")
        print(f"  'Not Found' text:  {c_nf}")
        print(f"  Sum (should match combined filter): {union}")
        print(f"  WHERE no-usable-location count:      {n_where}")
        if union != n_where:
            print("  NOTE: mismatch means rows match multiple arms of OR filter (should not happen).", file=sys.stderr)

        # --- NULL location: full list + field health ---
        print("\n--- Rows with location IS NULL ---\n")
        cur.execute(
            """
            SELECT id, first_name, last_name, linkedin_url, location,
                   current_job_title, company, degree, major, school, grad_year,
                   headline, created_at, updated_at, scraped_at
            FROM alumni
            WHERE location IS NULL
            ORDER BY id ASC
            """
        )
        null_rows = cur.fetchall() or []
        if not null_rows:
            print("(none)\n")
        else:
            bad_job = sum(
                1
                for r in null_rows
                if _empty(r.get("current_job_title")) and _empty(r.get("company"))
            )
            bad_school = sum(1 for r in null_rows if _empty(r.get("school")))
            print(
                f"Count: {len(null_rows)} | "
                f"both job title and company empty: {bad_job}/{len(null_rows)} | "
                f"school empty: {bad_school}/{len(null_rows)}"
            )
            print()
            for r in null_rows:
                print(
                    f"id={r['id']} | {r.get('first_name') or ''} {r.get('last_name') or ''} | {r.get('linkedin_url')}"
                )
                print(f"  job: {r.get('current_job_title') or '—'} @ {r.get('company') or '—'}")
                print(
                    f"  edu: {r.get('degree') or '—'} | {r.get('major') or '—'} | "
                    f"{r.get('school') or '—'} | grad_year={r.get('grad_year')!r}"
                )
                print(
                    f"  meta: created_at={r.get('created_at')!r} updated_at={r.get('updated_at')!r} "
                    f"scraped_at={r.get('scraped_at')!r}"
                )
                print()

        print("--- Random 10 from combined 'no usable location' set ---\n")
        cur.execute(
            f"""
            SELECT first_name, last_name, linkedin_url, location,
                   current_job_title, company, degree, major, school, grad_year
            FROM alumni WHERE {WHERE}
            ORDER BY RAND()
            LIMIT 10
            """
        )
        rows = cur.fetchall() or []
        for i, r in enumerate(rows, 1):
            print(f"{i}. {r.get('first_name') or ''} {r.get('last_name') or ''}".strip())
            print(f"   {r.get('linkedin_url')}")
            print(f"   location: {r.get('location')!r}")
            print(f"   job: {r.get('current_job_title') or '—'} @ {r.get('company') or '—'}")
            print(
                f"   edu: {r.get('degree') or '—'} | {r.get('major') or '—'} | "
                f"{r.get('school') or '—'} ({r.get('grad_year') or '—'})"
            )
            print()
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
