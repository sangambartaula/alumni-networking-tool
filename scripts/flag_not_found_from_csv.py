#!/usr/bin/env python3
"""Flag all local CSV rows whose location is currently 'Not Found'.

Usage:
  python scripts/flag_not_found_from_csv.py
  python scripts/flag_not_found_from_csv.py --dry-run
  python scripts/flag_not_found_from_csv.py --csv scraper/output/UNT_Alumni_Data.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "backend"))
sys.path.insert(0, str(PROJECT_ROOT / "scraper"))

from database import normalize_url  # noqa: E402
from settings import FLAGGED_PROFILES_FILE  # noqa: E402

DEFAULT_CSV = PROJECT_ROOT / "scraper" / "output" / "UNT_Alumni_Data.csv"
ALIAS_FLAGGED_FILE = PROJECT_ROOT / "scraper" / "output" / "flagged_for_reviews.txt"
REASON = "Location Not Found (backfill from CSV)"


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


def _parse_csv_not_found_urls(csv_path: Path) -> list[str]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    urls: list[str] = []
    seen: set[str] = set()
    with open(csv_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            location = str((row or {}).get("location", "") or "").strip().lower()
            if location != "not found":
                continue
            raw_url = (row or {}).get("linkedin_url") or (row or {}).get("profile_url") or ""
            url = normalize_url(raw_url)
            if not url or url in seen:
                continue
            urls.append(url)
            seen.add(url)
    return urls


def main() -> int:
    parser = argparse.ArgumentParser(description="Flag all CSV profiles with location 'Not Found'.")
    parser.add_argument("--csv", default=str(DEFAULT_CSV), help="Input alumni CSV path.")
    parser.add_argument("--dry-run", action="store_true", help="Report counts without writing files.")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.is_absolute():
        csv_path = PROJECT_ROOT / csv_path

    urls = _parse_csv_not_found_urls(csv_path)
    if not urls:
        print("No CSV rows found with location='Not Found'.")
        return 0

    wrote_primary = _append_unique(FLAGGED_PROFILES_FILE, urls, REASON, args.dry_run)
    wrote_alias = _append_unique(ALIAS_FLAGGED_FILE, urls, REASON, args.dry_run)

    mode = "Would append" if args.dry_run else "Appended"
    print(f"{mode} {wrote_primary} URL(s) to {FLAGGED_PROFILES_FILE}")
    print(f"{mode} {wrote_alias} URL(s) to {ALIAS_FLAGGED_FILE}")
    print(f"Total CSV Not Found profiles scanned: {len(urls)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
