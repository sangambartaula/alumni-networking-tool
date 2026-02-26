#!/usr/bin/env python3
"""
Backfill corrupted alumni major fields from CSV using backend.database.get_connection().

Use this when the app is running with MySQL/SQLite-fallback sync so updates are applied
through the same connection path the backend uses.
"""

from __future__ import annotations

import argparse
import csv
import sys
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


def _norm_url(url: str | None) -> str:
    return (url or "").strip().rstrip("/")


def _is_safe_major(value: str | None) -> bool:
    v = (value or "").strip()
    if not v:
        return False
    if v.lower() in JUNK_VALUES:
        return False
    if v in DISCIPLINE_DEFAULTS:
        return False
    return True


def _load_csv(csv_path: Path) -> dict[str, dict[str, str]]:
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill messed-up major fields from UNT CSV (live DB connection).")
    parser.add_argument("--csv", default="scraper/output/UNT_Alumni_Data.csv", help="Path to UNT CSV")
    parser.add_argument("--dry-run", action="store_true", help="Preview only; do not write.")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    project_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(project_root / "backend"))
    from database import get_connection  # noqa: WPS433 (runtime import is intentional)

    csv_map = _load_csv(csv_path)
    conn = get_connection()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute("SELECT id, linkedin_url, major, major2, major3 FROM alumni")
            rows = cur.fetchall() or []

        updates: list[tuple[list[str], list[object], int, str]] = []
        for row in rows:
            url = _norm_url(row.get("linkedin_url"))
            csv_row = csv_map.get(url)
            if not csv_row:
                continue

            set_parts: list[str] = []
            values: list[object] = []
            for field in ("major", "major2", "major3"):
                db_val = (row.get(field) or "").strip()
                csv_val = (csv_row.get(field) or "").strip()
                if db_val in DISCIPLINE_DEFAULTS and _is_safe_major(csv_val):
                    set_parts.append(f"{field} = %s")
                    values.append(csv_val)

            if set_parts:
                values.append(row["id"])
                updates.append((set_parts, values, row["id"], url))

        print(f"candidate_rows_to_fix={len(updates)}")
        print(f"csv_rows_loaded={len(csv_map)}")
        if updates:
            print("preview_first_10:")
            for set_parts, vals, row_id, url in updates[:10]:
                pairs = ", ".join(
                    f"{part.split('=')[0].strip()}='{vals[idx]}'"
                    for idx, part in enumerate(set_parts)
                )
                print(f"  id={row_id} {pairs} url={url}")

        if args.dry_run:
            print("dry_run=true no changes applied")
            return 0

        updated = 0
        with conn.cursor() as cur:
            for set_parts, vals, _row_id, _url in updates:
                sql = f"UPDATE alumni SET {', '.join(set_parts)}, updated_at = CURRENT_TIMESTAMP WHERE id = %s"
                cur.execute(sql, tuple(vals))
                if cur.rowcount:
                    updated += 1
        conn.commit()

        print(f"updated_rows={updated}")
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())

