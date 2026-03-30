"""
Export alumni data from cloud MySQL to canonical scraper CSV schema.

Why this exists:
- Prevent stale local CSV files from overwriting cloud data when seeding.
- Keep a consistent team workflow across multiple devices.

Usage:
  python scripts/export_cloud_alumni_csv.py
  python scripts/export_cloud_alumni_csv.py --output scraper/output/UNT_Alumni_Data.csv
  python scripts/export_cloud_alumni_csv.py --no-backup
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import mysql.connector
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_PATH = ROOT_DIR / "scraper" / "output" / "UNT_Alumni_Data.csv"

# Single source of truth (must match scraper output / database_handler seeding)
import sys

sys.path.insert(0, str(ROOT_DIR / "scraper"))
from config import CSV_COLUMNS  # noqa: E402


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _format_datetime(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return _clean_text(value)


def _canonical_url(value: Any) -> str:
    return _clean_text(value).rstrip("/")


def _normalize_wws(status_value: Any, bool_value: Any) -> str:
    status = _clean_text(status_value).lower()
    if status in {"yes", "no", "currently"}:
        return status
    if bool_value is None:
        return ""
    return "yes" if bool(bool_value) else "no"


def _coerce_grad_year(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    try:
        # Handles "2025", "2025.0", numeric DB values.
        as_float = float(text)
        if as_float.is_integer():
            year = int(as_float)
            if 1900 <= year <= 2100:
                return str(year)
    except ValueError:
        pass
    return text


def _infer_grad_year_from_school_start(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    if "-" in text or "–" in text or "—" in text:
        return ""

    years = re.findall(r"(19\d{2}|20\d{2}|2100)", text)
    if len(years) != 1:
        return ""
    return _coerce_grad_year(years[0])


def _normalize_primary_education_dates(grad_year_value: Any, school_start_value: Any) -> Tuple[str, str]:
    grad_year = _coerce_grad_year(grad_year_value)
    school_start = _clean_text(school_start_value)
    if grad_year:
        return grad_year, school_start

    inferred = _infer_grad_year_from_school_start(school_start)
    if inferred:
        return inferred, ""
    return "", school_start


def _row_rank(row: Dict[str, Any]) -> Tuple[datetime, int]:
    ts = row.get("updated_at") or row.get("scraped_at")
    if isinstance(ts, datetime):
        dt = ts
    else:
        dt = datetime.min
        ts_text = _clean_text(ts)
        if ts_text:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(ts_text, fmt)
                    break
                except ValueError:
                    continue
    try:
        row_id = int(row.get("id") or 0)
    except Exception:
        row_id = 0
    return dt, row_id


def _to_csv_row(row: Dict[str, Any]) -> Dict[str, str]:
    csv_row = {col: "" for col in CSV_COLUMNS}
    grad_year, school_start = _normalize_primary_education_dates(
        row.get("grad_year"),
        row.get("school_start_date"),
    )
    csv_row.update(
        {
            "first": _clean_text(row.get("first_name")),
            "last": _clean_text(row.get("last_name")),
            "linkedin_url": _canonical_url(row.get("linkedin_url")),
            "school": _clean_text(row.get("school")),
            "degree": _clean_text(row.get("degree")),
            "major": _clean_text(row.get("major")),
            "school_start": school_start,
            "grad_year": grad_year,
            "school2": _clean_text(row.get("school2")),
            "degree2": _clean_text(row.get("degree2")),
            "major2": _clean_text(row.get("major2")),
            "school3": _clean_text(row.get("school3")),
            "degree3": _clean_text(row.get("degree3")),
            "major3": _clean_text(row.get("major3")),
            "standardized_degree": _clean_text(row.get("standardized_degree")),
            "standardized_major": _clean_text(row.get("standardized_major")),
            "standardized_major_alt": _clean_text(row.get("standardized_major_alt")),
            "standardized_degree2": _clean_text(row.get("standardized_degree2")),
            "standardized_major2": _clean_text(row.get("standardized_major2")),
            "standardized_degree3": _clean_text(row.get("standardized_degree3")),
            "standardized_major3": _clean_text(row.get("standardized_major3")),
            "discipline": _clean_text(row.get("discipline")),
            "location": _clean_text(row.get("location")),
            "working_while_studying": _normalize_wws(
                row.get("working_while_studying_status"),
                row.get("working_while_studying"),
            ),
            "title": _clean_text(row.get("current_job_title")),
            "company": _clean_text(row.get("company")),
            "job_employment_type": _clean_text(row.get("job_employment_type")),
            "job_start": _clean_text(row.get("job_start_date")),
            "job_end": _clean_text(row.get("job_end_date")),
            "exp_2_title": _clean_text(row.get("exp2_title")),
            "exp_2_company": _clean_text(row.get("exp2_company")),
            "exp_2_dates": _clean_text(row.get("exp2_dates")),
            "exp_2_employment_type": _clean_text(row.get("exp2_employment_type")),
            "exp_3_title": _clean_text(row.get("exp3_title")),
            "exp_3_company": _clean_text(row.get("exp3_company")),
            "exp_3_dates": _clean_text(row.get("exp3_dates")),
            "exp_3_employment_type": _clean_text(row.get("exp3_employment_type")),
            "scraped_at": _format_datetime(row.get("scraped_at")),
            "normalized_job_title": _clean_text(row.get("normalized_job_title")),
            # Not stored as dedicated columns in current DB schema.
            "normalized_exp2_title": "",
            "normalized_exp3_title": "",
            # Experience analysis columns
            "job_1_relevance_score": _clean_text(row.get("job_1_relevance_score")),
            "job_2_relevance_score": _clean_text(row.get("job_2_relevance_score")),
            "job_3_relevance_score": _clean_text(row.get("job_3_relevance_score")),
            "job_1_is_relevant": _clean_text(row.get("job_1_is_relevant")),
            "job_2_is_relevant": _clean_text(row.get("job_2_is_relevant")),
            "job_3_is_relevant": _clean_text(row.get("job_3_is_relevant")),
            "relevant_experience_months": _clean_text(row.get("relevant_experience_months")),
            "seniority_level": _clean_text(row.get("seniority_level")),
        }
    )
    return csv_row


def _build_output_path(path_arg: str) -> Path:
    output_path = Path(path_arg)
    if not output_path.is_absolute():
        output_path = ROOT_DIR / output_path
    return output_path


def export_cloud_alumni_csv(output_path: Path, backup_existing: bool = True) -> int:
    load_dotenv(ROOT_DIR / ".env")

    missing = [k for k in ("MYSQLHOST", "MYSQLUSER", "MYSQLPASSWORD", "MYSQL_DATABASE") if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing DB env vars: {', '.join(missing)}")

    conn = mysql.connector.connect(
        host=os.getenv("MYSQLHOST"),
        user=os.getenv("MYSQLUSER"),
        password=os.getenv("MYSQLPASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        port=int(os.getenv("MYSQLPORT", "3306")),
    )

    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                """
                SELECT a.id,
                       a.first_name,
                       a.last_name,
                       a.linkedin_url,
                       a.school,
                       a.degree,
                       a.major,
                       a.school_start_date,
                       a.grad_year,
                       a.school2,
                       a.degree2,
                       a.major2,
                       a.school3,
                       a.degree3,
                       a.major3,
                       a.standardized_degree,
                       a.standardized_major,
                       a.standardized_major_alt,
                       a.standardized_degree2,
                       a.standardized_major2,
                       a.standardized_degree3,
                       a.standardized_major3,
                       a.discipline,
                       a.location,
                       a.working_while_studying,
                       a.working_while_studying_status,
                       a.current_job_title,
                       a.company,
                       a.job_start_date,
                       a.job_end_date,
                       a.job_employment_type,
                       a.exp2_title,
                       a.exp2_company,
                       a.exp2_dates,
                       a.exp2_employment_type,
                       a.exp3_title,
                       a.exp3_company,
                       a.exp3_dates,
                       a.exp3_employment_type,
                       a.scraped_at,
                       a.updated_at,
                       njt.normalized_title AS normalized_job_title,
                       a.job_1_relevance_score,
                       a.job_2_relevance_score,
                       a.job_3_relevance_score,
                       a.job_1_is_relevant,
                       a.job_2_is_relevant,
                       a.job_3_is_relevant,
                       a.relevant_experience_months,
                       a.seniority_level
                FROM alumni a
                LEFT JOIN normalized_job_titles njt
                  ON a.normalized_job_title_id = njt.id
                ORDER BY a.id ASC
                """
            )
            raw_rows = cur.fetchall() or []
    finally:
        conn.close()

    deduped: Dict[str, Tuple[Tuple[datetime, int], Dict[str, Any]]] = {}
    for row in raw_rows:
        url = _canonical_url(row.get("linkedin_url"))
        if not url:
            continue
        row["linkedin_url"] = url
        rank = _row_rank(row)
        current = deduped.get(url)
        if current is None or rank >= current[0]:
            deduped[url] = (rank, row)

    csv_rows: List[Dict[str, str]] = [_to_csv_row(data[1]) for data in deduped.values()]
    csv_rows.sort(key=lambda r: (r["first"].lower(), r["last"].lower(), r["linkedin_url"].lower()))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if backup_existing and output_path.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = output_path.with_name(f"{output_path.stem}_backup_{timestamp}{output_path.suffix}")
        output_path.replace(backup_path)
        print(f"Backed up previous CSV to: {backup_path}")

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(csv_rows)

    print(f"Wrote {len(csv_rows)} rows to {output_path}")
    return len(csv_rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export cloud alumni DB into canonical UNT_Alumni_Data.csv")
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output CSV path (default: scraper/output/UNT_Alumni_Data.csv)",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Do not create a timestamped backup when output file already exists.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    output = _build_output_path(args.output)
    export_cloud_alumni_csv(output_path=output, backup_existing=not args.no_backup)
