#!/usr/bin/env python3
"""
Generate a report of rows where normalized values equal raw values.

Checks:
- standardized_major == major
- normalized_job_title == current_job_title
- normalized_company == company
"""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional


@dataclass
class ReportRow:
    id: int
    linkedin_url: str
    first_name: str
    last_name: str
    major: str
    standardized_major: str
    current_job_title: str
    normalized_job_title: str
    company: str
    normalized_company: str
    major_match_exact: int
    major_match_ci: int
    title_match_exact: int
    title_match_ci: int
    company_match_exact: int
    company_match_ci: int
    any_match_ci: int


REPORT_FIELDS = [
    "id",
    "linkedin_url",
    "first_name",
    "last_name",
    "major",
    "standardized_major",
    "current_job_title",
    "normalized_job_title",
    "company",
    "normalized_company",
    "major_match_exact",
    "major_match_ci",
    "title_match_exact",
    "title_match_ci",
    "company_match_exact",
    "company_match_ci",
    "any_match_ci",
]


def _text(value: Optional[str]) -> str:
    return (value or "").strip()


def _eq_exact(left: Optional[str], right: Optional[str]) -> int:
    a = _text(left)
    b = _text(right)
    return int(bool(a and b and a == b))


def _eq_ci(left: Optional[str], right: Optional[str]) -> int:
    a = _text(left)
    b = _text(right)
    return int(bool(a and b and a.casefold() == b.casefold()))


def _iter_rows(conn: sqlite3.Connection) -> Iterable[ReportRow]:
    query = """
        SELECT
            a.id,
            a.linkedin_url,
            a.first_name,
            a.last_name,
            a.major,
            a.standardized_major,
            a.current_job_title,
            njt.normalized_title AS normalized_job_title,
            a.company,
            nc.normalized_company AS normalized_company
        FROM alumni a
        LEFT JOIN normalized_job_titles njt
            ON a.normalized_job_title_id = njt.id
        LEFT JOIN normalized_companies nc
            ON a.normalized_company_id = nc.id
        ORDER BY a.id
    """
    cur = conn.cursor()
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    for raw in cur.fetchall():
        row = dict(zip(cols, raw))
        major_exact = _eq_exact(row.get("standardized_major"), row.get("major"))
        major_ci = _eq_ci(row.get("standardized_major"), row.get("major"))
        title_exact = _eq_exact(row.get("normalized_job_title"), row.get("current_job_title"))
        title_ci = _eq_ci(row.get("normalized_job_title"), row.get("current_job_title"))
        company_exact = _eq_exact(row.get("normalized_company"), row.get("company"))
        company_ci = _eq_ci(row.get("normalized_company"), row.get("company"))

        yield ReportRow(
            id=row.get("id") or 0,
            linkedin_url=_text(row.get("linkedin_url")),
            first_name=_text(row.get("first_name")),
            last_name=_text(row.get("last_name")),
            major=_text(row.get("major")),
            standardized_major=_text(row.get("standardized_major")),
            current_job_title=_text(row.get("current_job_title")),
            normalized_job_title=_text(row.get("normalized_job_title")),
            company=_text(row.get("company")),
            normalized_company=_text(row.get("normalized_company")),
            major_match_exact=major_exact,
            major_match_ci=major_ci,
            title_match_exact=title_exact,
            title_match_ci=title_ci,
            company_match_exact=company_exact,
            company_match_ci=company_ci,
            any_match_ci=int(bool(major_ci or title_ci or company_ci)),
        )


def _write_csv(path: Path, rows: list[ReportRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def _write_summary(path: Path, rows: list[ReportRow], matched_rows: list[ReportRow]) -> None:
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_rows_scanned": len(rows),
        "rows_with_any_match_ci": len(matched_rows),
        "rows_with_major_match_ci": sum(r.major_match_ci for r in rows),
        "rows_with_title_match_ci": sum(r.title_match_ci for r in rows),
        "rows_with_company_match_ci": sum(r.company_match_ci for r in rows),
        "rows_with_major_match_exact": sum(r.major_match_exact for r in rows),
        "rows_with_title_match_exact": sum(r.title_match_exact for r in rows),
        "rows_with_company_match_exact": sum(r.company_match_exact for r in rows),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate normalization equality report from alumni DB.")
    parser.add_argument(
        "--db-path",
        default=str(Path("backend") / "alumni_backup.db"),
        help="Path to SQLite DB file (default: backend/alumni_backup.db)",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path("scraper") / "output" / "reports"),
        help="Output directory for report files.",
    )
    parser.add_argument(
        "--all-rows",
        action="store_true",
        help="Include all rows in CSV; default is only rows with any case-insensitive match.",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path)
    output_dir = Path(args.output_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"normalization_equality_report_{timestamp}.csv"
    summary_path = output_dir / f"normalization_equality_report_{timestamp}.summary.json"

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        rows = list(_iter_rows(conn))
    finally:
        conn.close()

    matched_rows = [r for r in rows if r.any_match_ci]
    rows_to_write = rows if args.all_rows else matched_rows

    _write_csv(csv_path, rows_to_write)
    _write_summary(summary_path, rows, matched_rows)

    print(f"Scanned rows: {len(rows)}")
    print(f"Rows with any match (case-insensitive): {len(matched_rows)}")
    print(f"CSV report: {csv_path}")
    print(f"Summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
