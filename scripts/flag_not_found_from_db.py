#!/usr/bin/env python3
"""Flag all alumni rows whose location is currently 'Not Found' in cloud DB.

Usage:
  python scripts/flag_not_found_from_db.py
  python scripts/flag_not_found_from_db.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "backend"))
sys.path.insert(0, str(PROJECT_ROOT / "scraper"))

from database import get_connection, normalize_url  # noqa: E402
from settings import FLAGGED_PROFILES_FILE  # noqa: E402

ALIAS_FLAGGED_FILE = PROJECT_ROOT / "scraper" / "output" / "flagged_for_reviews.txt"
REASON = "Location Not Found (backfill from DB)"


def _load_existing_urls(path: Path) -> set[str]:
    urls: set[str] = set()
    if not path.exists():
        return urls
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            existing = line.split("#")[0].strip().rstrip("/")
            if existing:
                urls.add(existing)
    return urls


def _append_unique(path: Path, urls: list[str], reason: str, dry_run: bool) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_existing_urls(path)
    pending = [u for u in urls if u and u not in existing]
    if not pending:
        return 0
    if dry_run:
        return len(pending)
    with open(path, "a", encoding="utf-8") as handle:
        for url in pending:
            handle.write(f"{url} # {reason}\n")
    return len(pending)


def _fetch_not_found_urls() -> list[str]:
    conn = get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                """
                SELECT linkedin_url
                FROM alumni
                WHERE linkedin_url IS NOT NULL
                  AND TRIM(linkedin_url) <> ''
                  AND LOWER(TRIM(location)) = 'not found'
                ORDER BY id ASC
                """
            )
            rows = cur.fetchall() or []
    finally:
        try:
            conn.close()
        except Exception:
            pass

    normalized: list[str] = []
    seen: set[str] = set()
    for row in rows:
        url = normalize_url((row or {}).get("linkedin_url"))
        if not url or url in seen:
            continue
        normalized.append(url)
        seen.add(url)
    return normalized


def main() -> int:
    parser = argparse.ArgumentParser(description="Flag all DB profiles with location 'Not Found'.")
    parser.add_argument("--dry-run", action="store_true", help="Report counts without writing files.")
    args = parser.parse_args()

    urls = _fetch_not_found_urls()
    if not urls:
        print("No DB rows found with location='Not Found'.")
        return 0

    wrote_primary = _append_unique(FLAGGED_PROFILES_FILE, urls, REASON, args.dry_run)
    wrote_alias = _append_unique(ALIAS_FLAGGED_FILE, urls, REASON, args.dry_run)

    mode = "Would append" if args.dry_run else "Appended"
    print(f"{mode} {wrote_primary} URL(s) to {FLAGGED_PROFILES_FILE}")
    print(f"{mode} {wrote_alias} URL(s) to {ALIAS_FLAGGED_FILE}")
    print(f"Total DB Not Found profiles scanned: {len(urls)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
