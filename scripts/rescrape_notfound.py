#!/usr/bin/env python3
"""Build review queue entries for cloud DB anomalies and append to flagged_for_review.txt.

Primary target:
- Alumni rows with location == "Not Found"

Also flags rows with title/company normalization anomalies, including:
- Very long normalized job title
- Very long normalized company
- Job title level markers (II, III, Level 2, etc.)
- Normalized company that looks like a job title
- Normalized job title that looks company-like/non-role-like

Supports team split mode:
- python scripts/rescrape_notfound.py 1  -> first half of candidates
- python scripts/rescrape_notfound.py 2  -> second half of candidates
- python scripts/rescrape_notfound.py    -> all candidates
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "backend"))
sys.path.insert(0, str(PROJECT_ROOT / "scraper"))

from database import normalize_url  # noqa: E402
from settings import FLAGGED_PROFILES_FILE  # noqa: E402

NOT_FOUND_VALUES = {"not found", "not_found"}
LEVEL_RE = re.compile(r"\b(level\s*)?(ii|iii|iv|v|vi|[1-9])\b", re.I)
ROLE_RE = re.compile(
    r"\b(engineer|developer|analyst|manager|director|scientist|architect|administrator|consultant|technician|specialist|intern|assistant|officer|coordinator|president|founder|researcher|professor|student)\b",
    re.I,
)
COMPANY_RE = re.compile(
    r"\b(inc|llc|ltd|corp|corporation|company|co\.?|technologies|technology|systems|solutions|university|college|bank|hospital|health|group|partners|associates|laboratories|lab|clinic|pharmacy|institute|center|centre)\b",
    re.I,
)


@dataclass
class Candidate:
    linkedin_url: str
    reasons: list[str]
    alumni_id: int


def _short(value: str, max_len: int = 120) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip())
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "..."


def _is_not_found_location(value: str) -> bool:
    return (value or "").strip().lower() in NOT_FOUND_VALUES


def _connect_cloud_mysql():
    load_dotenv(PROJECT_ROOT / ".env")
    required = {
        "MYSQLHOST": os.getenv("MYSQLHOST", "").strip(),
        "MYSQLUSER": os.getenv("MYSQLUSER", "").strip(),
        "MYSQLPASSWORD": os.getenv("MYSQLPASSWORD", "").strip(),
        "MYSQL_DATABASE": os.getenv("MYSQL_DATABASE", "").strip(),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError("Missing DB env vars: " + ", ".join(missing))

    return mysql.connector.connect(
        host=required["MYSQLHOST"],
        user=required["MYSQLUSER"],
        password=required["MYSQLPASSWORD"],
        database=required["MYSQL_DATABASE"],
        port=int(os.getenv("MYSQLPORT", "3306")),
    )


def _fetch_cloud_rows() -> list[dict]:
    conn = _connect_cloud_mysql()
    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute(
                """
                SELECT a.id,
                       a.linkedin_url,
                       a.location,
                       a.current_job_title,
                       a.company,
                       njt.normalized_title,
                       nc.normalized_company
                FROM alumni a
                LEFT JOIN normalized_job_titles njt
                  ON a.normalized_job_title_id = njt.id
                LEFT JOIN normalized_companies nc
                  ON a.normalized_company_id = nc.id
                WHERE a.linkedin_url IS NOT NULL
                  AND TRIM(a.linkedin_url) <> ''
                ORDER BY a.id ASC
                """
            )
            return cur.fetchall() or []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _build_reasons(row: dict, max_title_len: int, max_company_len: int) -> list[str]:
    reasons: list[str] = []

    location = (row.get("location") or "").strip()
    raw_title = (row.get("current_job_title") or "").strip()
    raw_company = (row.get("company") or "").strip()
    norm_title = (row.get("normalized_title") or "").strip()
    norm_company = (row.get("normalized_company") or "").strip()

    if _is_not_found_location(location):
        reasons.append("Location: Not Found")

    if norm_title and len(norm_title) > max_title_len:
        reasons.append(f"Job Title: {_short(norm_title)} (very long normalized title)")

    if raw_title and LEVEL_RE.search(raw_title):
        reasons.append(f"Job Title: {_short(raw_title)} (has level marker)")
    elif norm_title and LEVEL_RE.search(norm_title):
        reasons.append(f"Job Title: {_short(norm_title)} (normalized title has level marker)")

    if norm_company and len(norm_company) > max_company_len:
        reasons.append(f"Company: {_short(norm_company)} (very long normalized company)")

    if norm_company and ROLE_RE.search(norm_company) and not COMPANY_RE.search(norm_company):
        reasons.append(f"Company: {_short(norm_company)} (looks like a role/title)")

    if norm_title and COMPANY_RE.search(norm_title) and not ROLE_RE.search(norm_title):
        reasons.append(f"Job Title: {_short(norm_title)} (looks company/non-role-like)")

    if raw_company and len(raw_company) > 120:
        reasons.append(f"Company: {_short(raw_company)} (raw company looks like sentence/noise)")

    # Deduplicate while preserving order.
    deduped: list[str] = []
    seen = set()
    for reason in reasons:
        if reason not in seen:
            deduped.append(reason)
            seen.add(reason)
    return deduped


def _build_candidates(rows: list[dict], max_title_len: int, max_company_len: int) -> list[Candidate]:
    candidates: list[Candidate] = []
    by_url: dict[str, Candidate] = {}

    for row in rows:
        url = normalize_url((row or {}).get("linkedin_url"))
        if not url:
            continue
        reasons = _build_reasons(row, max_title_len=max_title_len, max_company_len=max_company_len)
        if not reasons:
            continue

        alumni_id = int((row or {}).get("id") or 0)
        existing = by_url.get(url)
        if not existing:
            by_url[url] = Candidate(linkedin_url=url, reasons=reasons, alumni_id=alumni_id)
            continue

        # Merge reasons if duplicate URL appears more than once.
        merged = list(existing.reasons)
        for reason in reasons:
            if reason not in merged:
                merged.append(reason)
        existing.reasons = merged

    candidates = sorted(by_url.values(), key=lambda c: (c.alumni_id, c.linkedin_url))
    return candidates


def _split_candidates(candidates: list[Candidate], split: int | None) -> list[Candidate]:
    if split not in {1, 2}:
        return candidates
    mid = (len(candidates) + 1) // 2
    if split == 1:
        return candidates[:mid]
    return candidates[mid:]


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


def _append_candidates(path: Path, candidates: list[Candidate], dry_run: bool) -> tuple[int, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_existing_urls(path)

    pending = [c for c in candidates if c.linkedin_url not in existing]
    if dry_run or not pending:
        return len(pending), len(candidates)

    with open(path, "a", encoding="utf-8") as handle:
        for c in pending:
            reason_text = "; ".join(c.reasons)
            handle.write(f"{c.linkedin_url} # {reason_text}\n")

    return len(pending), len(candidates)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flag cloud DB profiles for re-scrape and append to flagged_for_review.txt"
    )
    parser.add_argument(
        "split",
        nargs="?",
        choices=["1", "2"],
        help="Optional team split: 1 = first half, 2 = second half",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show counts without writing files")
    parser.add_argument("--max-title-len", type=int, default=45, help="Threshold for long normalized job titles")
    parser.add_argument("--max-company-len", type=int, default=55, help="Threshold for long normalized companies")
    args = parser.parse_args()

    split = int(args.split) if args.split else None

    rows = _fetch_cloud_rows()
    candidates = _build_candidates(
        rows,
        max_title_len=max(1, int(args.max_title_len)),
        max_company_len=max(1, int(args.max_company_len)),
    )

    if not candidates:
        print("No candidates matched anomaly rules in cloud DB.")
        return 0

    selected = _split_candidates(candidates, split)

    wrote_primary, selected_count = _append_candidates(FLAGGED_PROFILES_FILE, selected, args.dry_run)

    split_label = "all"
    if split == 1:
        split_label = "first half"
    elif split == 2:
        split_label = "second half"

    mode = "Would append" if args.dry_run else "Appended"
    print(f"Cloud rows scanned: {len(rows)}")
    print(f"Total anomaly candidates (before split): {len(candidates)}")
    print(f"Selected set ({split_label}): {selected_count}")
    print(f"{mode} {wrote_primary} URL(s) to {FLAGGED_PROFILES_FILE}")

    # Helpful sample output for teammates.
    sample = selected[:5]
    if sample:
        print("\nSample entries:")
        for item in sample:
            print(f"- {item.linkedin_url} # {'; '.join(item.reasons)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
