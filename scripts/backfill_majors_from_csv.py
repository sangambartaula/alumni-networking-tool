#!/usr/bin/env python3
"""
Backfill corrupted alumni.major values from UNT_Alumni_Data.csv.

Safety rule:
- Update a DB major field only when current DB value is one of discipline defaults
  (excluding "Other") AND the CSV major for that field is a non-empty, non-junk,
  non-discipline value.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path


DISCIPLINE_DEFAULTS = {
    "Software, Data & AI Engineering",
    "Embedded, Electrical & Hardware Engineering",
    "Mechanical & Energy Engineering",
    "Biomedical Engineering",
    "Materials Science & Manufacturing",
    "Construction & Engineering Management",
}

JUNK_VALUES = {"", "unknown", "not found", "n/a", "na", "none", "null", "nan", "other"}


@dataclass
class RowFix:
    alumni_id: int
    linkedin_url: str
    updates: dict[str, str]


def _norm_url(url: str | None) -> str:
    return (url or "").strip().rstrip("/")


def _safe_major(candidate: str | None) -> str | None:
    value = (candidate or "").strip()
    if not value:
        return None
    if value.lower() in JUNK_VALUES:
        return None
    if value in DISCIPLINE_DEFAULTS:
        return None
    return value


def load_csv_map(csv_path: Path) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = _norm_url(row.get("linkedin_url"))
            if not url:
                continue
            out[url] = {
                "major": (row.get("major") or "").strip(),
                "major2": (row.get("major2") or "").strip(),
                "major3": (row.get("major3") or "").strip(),
            }
    return out


def find_fixes(conn: sqlite3.Connection, csv_map: dict[str, dict[str, str]]) -> tuple[list[RowFix], int]:
    rows = conn.execute(
        """
        SELECT id, linkedin_url, major, major2, major3
        FROM alumni
        """
    ).fetchall()

    fixes: list[RowFix] = []
    csv_missing = 0

    for row in rows:
        alumni_id = int(row[0])
        url = _norm_url(row[1])
        majors = {"major": row[2] or "", "major2": row[3] or "", "major3": row[4] or ""}
        csv_row = csv_map.get(url)
        if not csv_row:
            if any((majors[k] or "").strip() in DISCIPLINE_DEFAULTS for k in ("major", "major2", "major3")):
                csv_missing += 1
            continue

        updates: dict[str, str] = {}
        for field in ("major", "major2", "major3"):
            db_val = (majors[field] or "").strip()
            if db_val not in DISCIPLINE_DEFAULTS:
                continue
            safe = _safe_major(csv_row.get(field))
            if safe:
                updates[field] = safe

        if updates:
            fixes.append(RowFix(alumni_id=alumni_id, linkedin_url=url, updates=updates))

    return fixes, csv_missing


def apply_fixes(conn: sqlite3.Connection, fixes: list[RowFix]) -> int:
    updated_rows = 0
    for fix in fixes:
        set_parts = []
        params: list[str | int] = []
        for field, value in fix.updates.items():
            set_parts.append(f"{field} = ?")
            params.append(value)
        set_parts.append("updated_at = datetime('now')")
        params.append(fix.alumni_id)
        sql = f"UPDATE alumni SET {', '.join(set_parts)} WHERE id = ?"
        cur = conn.execute(sql, params)
        if cur.rowcount:
            updated_rows += 1
    conn.commit()
    return updated_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill corrupted alumni major fields from CSV.")
    parser.add_argument(
        "--csv",
        default="scraper/output/UNT_Alumni_Data.csv",
        help="Path to source CSV (default: scraper/output/UNT_Alumni_Data.csv)",
    )
    parser.add_argument(
        "--db",
        default="backend/alumni_backup.db",
        help="Path to SQLite alumni DB (default: backend/alumni_backup.db)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without writing.")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    db_path = Path(args.db)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    csv_map = load_csv_map(csv_path)
    conn = sqlite3.connect(db_path)
    try:
        fixes, csv_missing = find_fixes(conn, csv_map)
        print(f"candidate_rows_to_fix={len(fixes)}")
        print(f"csv_rows_loaded={len(csv_map)}")
        print(f"rows_with_discipline_major_but_missing_csv={csv_missing}")

        if fixes:
            preview = fixes[:10]
            print("preview_first_10:")
            for f in preview:
                pairs = ", ".join(f"{k}='{v}'" for k, v in f.updates.items())
                print(f"  id={f.alumni_id} {pairs} url={f.linkedin_url}")

        if args.dry_run:
            print("dry_run=true no changes applied")
            return 0

        updated = apply_fixes(conn, fixes)
        print(f"updated_rows={updated}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

