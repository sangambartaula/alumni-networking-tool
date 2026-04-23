#!/usr/bin/env python3
"""
Audit alumni data quality and apply safe fixes.

Behavior:
- Scans every row in cloud MySQL alumni table.
- Flags suspicious raw data into flagged_for_review.txt with reason comments.
- Applies safe standardized-field fixes from raw degree/major fields.
- Marks visited_profiles.needs_update=1 for rows that should be re-scraped.

Usage:
  python scripts/audit_data_quality.py
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scraper"))

from backend.database import get_direct_mysql_connection  # noqa: E402
from degree_normalization import extract_hidden_degree, standardize_degree  # noqa: E402
from major_normalization import standardize_major_list  # noqa: E402
from job_title_normalization import normalize_title_deterministic  # noqa: E402


BAD_NAME_TOKENS = {
    "notifications",
    "notification",
    "messages",
    "jobs",
    "home",
    "feed",
    "my network",
    "network",
    "search",
    "linkedin",
    "premium",
    "sign in",
}

PLACEHOLDER_VALUES = {"", "n/a", "na", "none", "null", "-", "--", "unknown", "not found"}

MAX_TITLE_LEN = 140
MAX_DEGREE_LEN = 120
MAX_MAJOR_LEN = 120
MAX_COMPANY_LEN = 120
MAX_HEADLINE_LEN = 260

_GARBAGE_TITLE_RE = re.compile(r"^[\d\s\-_/.,:|#]+$")
_TITLE_NO_LETTERS = re.compile(r"[A-Za-z]")


def _clean(value: object) -> str:
    return str(value or "").strip()


def _norm(value: object) -> str:
    return _clean(value).lower()


def _is_placeholder(value: object) -> bool:
    return _norm(value) in PLACEHOLDER_VALUES


def _likely_bad_name(first_name: object, last_name: object) -> tuple[bool, str]:
    full = f"{_clean(first_name)} {_clean(last_name)}".strip()
    full_lower = full.lower()

    if not full:
        return True, "name_missing"

    if full_lower in BAD_NAME_TOKENS:
        return True, f"name_ui_artifact:{full}"

    for token in BAD_NAME_TOKENS:
        if token in full_lower:
            return True, f"name_contains_ui_artifact:{token}"

    if re.fullmatch(r"\d+", full):
        return True, f"name_numeric_only:{full}"

    if len(full) > 80:
        return True, f"name_too_long:{len(full)}"

    return False, ""


def _suspicious_job_title(title: str) -> tuple[bool, str]:
    t = _clean(title)
    if not t:
        return False, ""
    if _GARBAGE_TITLE_RE.match(t):
        return True, f"job_title_garbage_pattern:{t[:80]}"
    if not _TITLE_NO_LETTERS.search(t):
        return True, f"job_title_no_letters:{t[:80]}"
    if len(t.split()) > 14:
        return True, f"job_title_too_many_tokens:{len(t.split())}"
    return False, ""


def _bad_linkedin_url(linkedin_url: object) -> tuple[bool, str]:
    url = _clean(linkedin_url)
    if not url:
        return True, "linkedin_url_missing"
    if "linkedin.com/in/" not in url.lower():
        return True, "linkedin_url_not_profile_path"
    return False, ""


def _recompute_standardized(raw_degree: object, raw_major: object, raw_title: object) -> tuple[str, str, str | None, str | None, str | None]:
    degree = _clean(raw_degree)
    major = _clean(raw_major)
    title = _clean(raw_title)

    if not degree and major:
        extracted_degree, cleaned_major = extract_hidden_degree(major)
        if extracted_degree:
            degree = _clean(extracted_degree)
            major = _clean(cleaned_major)

    standardized_degree = standardize_degree(degree) if degree else None
    standardized_major = None
    standardized_major_alt = None

    if major:
        majors = standardize_major_list(major, title) or []
        if majors:
            standardized_major = majors[0]
            if len(majors) > 1:
                standardized_major_alt = majors[1]

    return degree, major, standardized_degree, standardized_major, standardized_major_alt


def main() -> None:
    flag_file = PROJECT_ROOT / "flagged_for_review.txt"
    proposal_file = PROJECT_ROOT / "job_title_normalization_proposal.txt"

    conn = get_direct_mysql_connection()
    conn.autocommit = False

    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute("SHOW COLUMNS FROM alumni")
            columns = {row["Field"] for row in cur.fetchall()}

        desired_columns = [
            "id",
            "linkedin_url",
            "first_name",
            "last_name",
            "headline",
            "current_job_title",
            "company",
            "degree",
            "major",
            "degree2",
            "major2",
            "degree3",
            "major3",
            "standardized_degree",
            "standardized_degree2",
            "standardized_degree3",
            "standardized_major",
            "standardized_major2",
            "standardized_major3",
            "standardized_major_alt",
            "normalized_job_title_id",
            "normalized_company_id",
            "updated_at",
        ]
        select_columns = [col for col in desired_columns if col in columns]

        with conn.cursor(dictionary=True) as cur:
            cur.execute(f"SELECT {', '.join(select_columns)} FROM alumni ORDER BY id ASC")
            rows = cur.fetchall() or []

        flagged_lines: list[str] = []
        duplicate_linkedin: dict[str, list[int]] = {}
        name_bucket: dict[str, list[tuple[int, str]]] = {}
        for row in rows:
            url = _clean(row.get("linkedin_url")).rstrip("/").lower()
            if url and "linkedin.com/in/" in url:
                duplicate_linkedin.setdefault(url, []).append(row.get("id"))
            fn = _norm(row.get("first_name"))
            ln = _norm(row.get("last_name"))
            if fn and ln:
                key = f"{fn}|{ln}"
                name_bucket.setdefault(key, []).append(
                    (
                        row.get("id"),
                        _clean(row.get("linkedin_url")),
                        _clean(row.get("first_name")),
                        _clean(row.get("last_name")),
                    )
                )

        for url, ids in sorted(duplicate_linkedin.items()):
            uniq = sorted({i for i in ids if i is not None})
            if len(uniq) <= 1:
                continue
            reason = f"duplicate_linkedin_url:{len(uniq)}_rows"
            for aid in uniq:
                flagged_lines.append(f"id={aid} | name=<see peer ids> | linkedin_url={url} # {reason}")

        for key, entries in sorted(name_bucket.items()):
            urls = {u.rstrip("/").lower() for (_, u, _, _) in entries if _clean(u)}
            if len(urls) <= 1:
                continue
            reason = "near_duplicate_name_different_linkedin"
            for aid, u, fn_disp, ln_disp in entries:
                display_name = f"{fn_disp} {ln_disp}".strip() or "<name>"
                flagged_lines.append(
                    f"id={aid} | name={display_name} | linkedin_url={_clean(u)} # {reason}"
                )

        needs_rescrape_urls: set[str] = set()
        reason_counts: Counter[str] = Counter()
        fixed_column_counts: Counter[str] = Counter()

        fixed_rows = 0

        valid_job_ids: set[int] = set()
        valid_company_ids: set[int] = set()

        if "normalized_job_title_id" in columns:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM normalized_job_titles")
                valid_job_ids = {row[0] for row in cur.fetchall()}

        if "normalized_company_id" in columns:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM normalized_companies")
                valid_company_ids = {row[0] for row in cur.fetchall()}

        for row in rows:
            alumni_id = row.get("id")
            linkedin_url = _clean(row.get("linkedin_url"))
            first_name = _clean(row.get("first_name"))
            last_name = _clean(row.get("last_name"))
            current_job_title = _clean(row.get("current_job_title"))
            company = _clean(row.get("company"))
            headline = _clean(row.get("headline"))

            row_reasons: list[str] = []

            bad_name, bad_name_reason = _likely_bad_name(first_name, last_name)
            if bad_name:
                row_reasons.append(bad_name_reason)
                if linkedin_url:
                    needs_rescrape_urls.add(linkedin_url)

            bad_url, bad_url_reason = _bad_linkedin_url(linkedin_url)
            if bad_url:
                row_reasons.append(bad_url_reason)
                if linkedin_url:
                    needs_rescrape_urls.add(linkedin_url)

            if current_job_title and len(current_job_title) > MAX_TITLE_LEN:
                row_reasons.append(f"job_title_too_long:{len(current_job_title)}")
            bad_title, bad_title_reason = _suspicious_job_title(current_job_title)
            if bad_title:
                row_reasons.append(bad_title_reason)
            if company and len(company) > MAX_COMPANY_LEN:
                row_reasons.append(f"company_too_long:{len(company)}")
            if headline and len(headline) > MAX_HEADLINE_LEN:
                row_reasons.append(f"headline_too_long:{len(headline)}")

            for degree_col in ("degree", "degree2", "degree3"):
                if degree_col not in columns:
                    continue
                value = _clean(row.get(degree_col))
                if value and len(value) > MAX_DEGREE_LEN:
                    row_reasons.append(f"{degree_col}_too_long:{len(value)}")
                if _is_placeholder(value):
                    row_reasons.append(f"{degree_col}_placeholder")

            for major_col in ("major", "major2", "major3"):
                if major_col not in columns:
                    continue
                value = _clean(row.get(major_col))
                if value and len(value) > MAX_MAJOR_LEN:
                    row_reasons.append(f"{major_col}_too_long:{len(value)}")
                if _is_placeholder(value):
                    row_reasons.append(f"{major_col}_placeholder")

            if "normalized_job_title_id" in columns:
                job_id = row.get("normalized_job_title_id")
                if job_id and job_id not in valid_job_ids:
                    row_reasons.append(f"bad_normalized_job_title_id:{job_id}")

            if "normalized_company_id" in columns:
                company_id = row.get("normalized_company_id")
                if company_id and company_id not in valid_company_ids:
                    row_reasons.append(f"bad_normalized_company_id:{company_id}")

            updates: dict[str, object] = {}

            _, _, std_degree, std_major, std_major_alt = _recompute_standardized(
                row.get("degree"),
                row.get("major"),
                current_job_title,
            )

            if "standardized_degree" in columns and (row.get("standardized_degree") or None) != std_degree:
                updates["standardized_degree"] = std_degree
            if "standardized_major" in columns and (row.get("standardized_major") or None) != std_major:
                updates["standardized_major"] = std_major
            if "standardized_major_alt" in columns and (row.get("standardized_major_alt") or None) != std_major_alt:
                updates["standardized_major_alt"] = std_major_alt

            _, _, std_degree2, std_major2, _ = _recompute_standardized(
                row.get("degree2"),
                row.get("major2"),
                current_job_title,
            )
            if "standardized_degree2" in columns and (row.get("standardized_degree2") or None) != std_degree2:
                updates["standardized_degree2"] = std_degree2
            if "standardized_major2" in columns and (row.get("standardized_major2") or None) != std_major2:
                updates["standardized_major2"] = std_major2

            _, _, std_degree3, std_major3, _ = _recompute_standardized(
                row.get("degree3"),
                row.get("major3"),
                current_job_title,
            )
            if "standardized_degree3" in columns and (row.get("standardized_degree3") or None) != std_degree3:
                updates["standardized_degree3"] = std_degree3
            if "standardized_major3" in columns and (row.get("standardized_major3") or None) != std_major3:
                updates["standardized_major3"] = std_major3

            if updates:
                assignments = ", ".join([f"{col} = %s" for col in updates.keys()])
                values = list(updates.values()) + [alumni_id]
                with conn.cursor() as cur:
                    cur.execute(f"UPDATE alumni SET {assignments} WHERE id = %s", values)
                fixed_rows += 1
                for col in updates.keys():
                    fixed_column_counts[col] += 1

            if row_reasons:
                display_name = f"{first_name} {last_name}".strip() or "<missing name>"
                reason_text = ", ".join(sorted(set(row_reasons)))
                flagged_lines.append(
                    f"id={alumni_id} | name={display_name} | linkedin_url={linkedin_url} # {reason_text}"
                )
                for reason in sorted(set(row_reasons)):
                    reason_counts[reason.split(":")[0]] += 1

        conn.commit()

        marked_for_rescrape = 0
        with conn.cursor(dictionary=True) as cur:
            cur.execute("SHOW TABLES LIKE 'visited_profiles'")
            has_visited_profiles = cur.fetchone() is not None

        if has_visited_profiles and needs_rescrape_urls:
            with conn.cursor() as cur:
                for url in sorted(needs_rescrape_urls):
                    if not url:
                        continue
                    normalized = url.rstrip("/")
                    cur.execute(
                        """
                        UPDATE visited_profiles
                        SET needs_update = 1, notes = %s
                        WHERE linkedin_url IN (%s, %s)
                        """,
                        ("Flagged by audit: bad raw profile fields", normalized, normalized + "/"),
                    )
                    marked_for_rescrape += cur.rowcount
            conn.commit()

        report_lines: list[str] = []
        report_lines.append("Data Quality Audit Report (cloud alumni table)")
        report_lines.append("")
        report_lines.append(f"Total alumni rows scanned: {len(rows)}")
        report_lines.append(f"Rows with safe standardized-field fixes applied: {fixed_rows}")
        report_lines.append(f"Rows flagged for manual review/rescrape: {len(flagged_lines)}")
        report_lines.append(f"visited_profiles rows marked needs_update=1: {marked_for_rescrape}")
        report_lines.append("")

        if fixed_column_counts:
            report_lines.append("Standardized field fixes applied:")
            for column, count in sorted(fixed_column_counts.items()):
                report_lines.append(f"- {column}: {count}")
            report_lines.append("")

        if reason_counts:
            report_lines.append("Flag reason counts:")
            for reason, count in reason_counts.most_common():
                report_lines.append(f"- {reason}: {count}")
            report_lines.append("")

        report_lines.append("Flagged rows (one per line):")
        report_lines.append("# format: id | name | linkedin_url # reason(s)")
        report_lines.extend(flagged_lines if flagged_lines else ["<none>"])
        report_lines.append("")
        report_lines.append(
            "Review note: rows flagged for bad raw fields should be re-scraped; "
            "standardized-only issues were auto-corrected above."
        )

        flag_file.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

        raw_titles = sorted({_clean(r.get("current_job_title")) for r in rows if _clean(r.get("current_job_title"))})
        prop_lines: list[str] = [
            "Job title normalization proposal (deterministic normalize_title_deterministic)",
            f"Unique non-empty current_job_title values: {len(raw_titles)}",
            "",
            "Format: Raw Title → Normalized Title",
            "",
        ]
        bucket_to_raw: dict[str, list[str]] = {}
        for raw in raw_titles:
            norm = normalize_title_deterministic(raw) or "(empty)"
            prop_lines.append(f"{raw} → {norm}")
            bucket_to_raw.setdefault(norm, []).append(raw)
        prop_lines.append("")
        prop_lines.append("=== Merge groups (multiple raw titles → same normalized) ===")
        merge_groups = [(n, rs) for n, rs in bucket_to_raw.items() if n != "(empty)" and len(rs) > 1]
        merge_groups.sort(key=lambda x: (-len(x[1]), x[0]))
        if not merge_groups:
            prop_lines.append("<none>")
        else:
            for norm, raws in merge_groups:
                prop_lines.append(f"\n{norm} ({len(raws)} raw variants):")
                for r in sorted(raws):
                    prop_lines.append(f"  - {r}")
        proposal_file.write_text("\n".join(prop_lines) + "\n", encoding="utf-8")

        print(f"SCANNED_ROWS={len(rows)}")
        print(f"FIXED_STANDARDIZED_ROWS={fixed_rows}")
        print(f"FLAGGED_ROWS={len(flagged_lines)}")
        print(f"MARKED_FOR_RESCRAPE={marked_for_rescrape}")
        print(f"REPORT={flag_file}")
        print(f"PROPOSAL={proposal_file}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
