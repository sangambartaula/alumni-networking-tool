#!/usr/bin/env python3
"""
Audit normalized job titles and companies with Groq.

What it does:
1) Loads ALL unique normalized job titles and normalized companies in the DB.
2) Sends them to Groq in chunks for quality review.
3) Produces a text report with flagged entities, reason, and affected profile URLs.

Usage examples:
  python scripts/audit_normalized_entities_with_groq.py --target sqlite
  python scripts/audit_normalized_entities_with_groq.py --target cloud
  python scripts/audit_normalized_entities_with_groq.py --target auto --chunk-size 120
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def _get_groq_client_and_model():
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    model = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant").strip() or "llama-3.1-8b-instant"
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is not set")

    from groq import Groq

    return Groq(api_key=api_key), model


def mysql_config() -> dict:
    return {
        "host": os.getenv("MYSQLHOST"),
        "user": os.getenv("MYSQLUSER"),
        "password": os.getenv("MYSQLPASSWORD"),
        "database": os.getenv("MYSQL_DATABASE"),
        "port": int(os.getenv("MYSQLPORT", "3306")),
    }


def connect_mysql():
    cfg = mysql_config()
    missing = [k for k, v in cfg.items() if k in {"host", "user", "password", "database"} and not v]
    if missing:
        raise RuntimeError(f"Missing MySQL env vars: {', '.join(missing)}")
    return mysql.connector.connect(**cfg)


def connect_sqlite(sqlite_path: Path):
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    return conn


def get_engine(conn) -> str:
    return "mysql" if conn.__class__.__module__.startswith("mysql") else "sqlite"


@dataclass
class EntityRecord:
    entity_id: int
    normalized_value: str
    profile_count: int
    sample_raw_values: list[str]
    profile_urls: list[str]


def _fetch_rows(conn, query_mysql: str, query_sqlite: str, params: tuple = ()):  # noqa: ANN001
    engine = get_engine(conn)
    query = query_mysql if engine == "mysql" else query_sqlite
    cur = conn.cursor(dictionary=True) if engine == "mysql" else conn.cursor()
    try:
        cur.execute(query, params)
        rows = cur.fetchall() or []
        if engine == "mysql":
            return rows
        return [dict(r) for r in rows]
    finally:
        cur.close()


def load_title_entities(conn) -> list[EntityRecord]:
    rows = _fetch_rows(
        conn,
        """
        SELECT njt.id AS entity_id, njt.normalized_title AS normalized_value, COUNT(*) AS profile_count
        FROM normalized_job_titles njt
        JOIN alumni a ON a.normalized_job_title_id = njt.id
        WHERE njt.normalized_title IS NOT NULL AND TRIM(njt.normalized_title) <> ''
        GROUP BY njt.id, njt.normalized_title
        ORDER BY COUNT(*) DESC, njt.normalized_title ASC
        """,
        """
        SELECT njt.id AS entity_id, njt.normalized_title AS normalized_value, COUNT(*) AS profile_count
        FROM normalized_job_titles njt
        JOIN alumni a ON a.normalized_job_title_id = njt.id
        WHERE njt.normalized_title IS NOT NULL AND TRIM(njt.normalized_title) <> ''
        GROUP BY njt.id, njt.normalized_title
        ORDER BY COUNT(*) DESC, njt.normalized_title ASC
        """,
    )

    detail_rows = _fetch_rows(
        conn,
        """
        SELECT a.normalized_job_title_id AS entity_id, a.linkedin_url, a.current_job_title AS raw_value
        FROM alumni a
        WHERE a.normalized_job_title_id IS NOT NULL
        """,
        """
        SELECT a.normalized_job_title_id AS entity_id, a.linkedin_url, a.current_job_title AS raw_value
        FROM alumni a
        WHERE a.normalized_job_title_id IS NOT NULL
        """,
    )

    return _assemble_entities(rows, detail_rows)


def load_company_entities(conn) -> list[EntityRecord]:
    rows = _fetch_rows(
        conn,
        """
        SELECT nc.id AS entity_id, nc.normalized_company AS normalized_value, COUNT(*) AS profile_count
        FROM normalized_companies nc
        JOIN alumni a ON a.normalized_company_id = nc.id
        WHERE nc.normalized_company IS NOT NULL AND TRIM(nc.normalized_company) <> ''
        GROUP BY nc.id, nc.normalized_company
        ORDER BY COUNT(*) DESC, nc.normalized_company ASC
        """,
        """
        SELECT nc.id AS entity_id, nc.normalized_company AS normalized_value, COUNT(*) AS profile_count
        FROM normalized_companies nc
        JOIN alumni a ON a.normalized_company_id = nc.id
        WHERE nc.normalized_company IS NOT NULL AND TRIM(nc.normalized_company) <> ''
        GROUP BY nc.id, nc.normalized_company
        ORDER BY COUNT(*) DESC, nc.normalized_company ASC
        """,
    )

    detail_rows = _fetch_rows(
        conn,
        """
        SELECT a.normalized_company_id AS entity_id, a.linkedin_url, a.company AS raw_value
        FROM alumni a
        WHERE a.normalized_company_id IS NOT NULL
        """,
        """
        SELECT a.normalized_company_id AS entity_id, a.linkedin_url, a.company AS raw_value
        FROM alumni a
        WHERE a.normalized_company_id IS NOT NULL
        """,
    )

    return _assemble_entities(rows, detail_rows)


def _assemble_entities(agg_rows: list[dict], detail_rows: list[dict]) -> list[EntityRecord]:
    raw_values_by_entity = defaultdict(set)
    urls_by_entity = defaultdict(list)

    for row in detail_rows:
        entity_id = row.get("entity_id")
        if entity_id is None:
            continue
        raw = (row.get("raw_value") or "").strip()
        url = (row.get("linkedin_url") or "").strip()
        if raw:
            raw_values_by_entity[entity_id].add(raw)
        if url:
            urls_by_entity[entity_id].append(url)

    entities = []
    for row in agg_rows:
        entity_id = row.get("entity_id")
        entities.append(
            EntityRecord(
                entity_id=entity_id,
                normalized_value=(row.get("normalized_value") or "").strip(),
                profile_count=int(row.get("profile_count") or 0),
                sample_raw_values=sorted(raw_values_by_entity.get(entity_id, set()))[:8],
                profile_urls=sorted(set(urls_by_entity.get(entity_id, []))),
            )
        )
    return entities


def chunked(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def groq_audit_entities(client, model: str, entity_type: str, entities: list[EntityRecord]) -> list[dict]:
    # Compact payload per chunk while keeping enough signal (normalized + raw samples + count).
    payload = [
        {
            "entity_id": e.entity_id,
            "normalized_value": e.normalized_value,
            "profile_count": e.profile_count,
            "sample_raw_values": e.sample_raw_values,
        }
        for e in entities
    ]

    prompt = f"""
You are auditing normalized {entity_type} values extracted from LinkedIn profiles.

Primary objective:
- Minimize false positives.
- Return only clearly invalid entries or clearly broken normalization.
- If uncertain, do NOT flag.

Validity policy:
- VALID means a real-world title/company variant, even if formatting is imperfect.
- INVALID means clearly not a real title/company.
- Only output INVALID issues.

Only mark INVALID when clearly non-entity, for example:
- Dates/durations: "Jan 2022 - May 2025", "2020 - Present"
- Pure locations: "Denton", "Boston", "Dallas-Fort Worth Metroplex"
- Full sentence descriptions instead of an entity name
- Placeholder values: "Company Name", "N/A", "Unknown"
- Obvious garbage/noise strings or malformed text

Job title rules:
- Treat real titles as VALID by default.
- Titles like "Software Engineer II", "Software Engineer IV", "Senior Software Engineer", "Backend Engineer", "AI / ML Engineer", "Biomedical Engineer", "CEO", "Barista", "Tutor", "PhD Candidate", and "Research Fellow" are VALID.
- Do NOT treat roman numerals as invalid formatting.
- Do NOT treat seniority words (Senior, Junior, Intern, Lead) as invalid.
- Seniority is handled elsewhere; do not enforce seniority normalization here.
- If a correction is suggested, base-role simplification is optional only (example: "Software Engineer II" -> "Software Engineer").

Company rules:
- Treat real organizations as VALID by default.
- Well-known companies (Deloitte, Amazon, McKinsey, Meta, etc.) are VALID.
- Full institution names are VALID companies, including universities, research labs, and medical centers.
- Do NOT shorten organization names.
- Do NOT reduce entities to generic root words like "University".
- If value is partial/generic like only "University", flag as INVALID.
- Suggest reconstruction only when confidently derivable from sample_raw_values; otherwise keep suggested_value empty.

Hard constraints:
- Do NOT use subjective criteria like "too specific", "not typical", or "generic noise".
- Do NOT invent categories or fields beyond the required JSON schema.
- Be conservative and prefer false negatives over false positives.

Input JSON array (each item has entity_id, normalized_value, profile_count, sample_raw_values):
{json.dumps(payload, ensure_ascii=True)}

Return STRICT JSON only in this schema:
{{
    "issues": [
        {{
            "entity_id": 123,
            "severity": "high|medium|low",
            "reason": "short explanation",
            "suggested_value": "optional corrected normalized value or empty string"
        }}
    ]
}}
""".strip()

    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": "You are a strict data-quality auditor. Output JSON only.",
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        temperature=0,
        response_format={"type": "json_object"},
        max_tokens=1400,
    )

    content = (response.choices[0].message.content or "").strip()
    parsed = json.loads(content)
    issues = parsed.get("issues", []) if isinstance(parsed, dict) else []

    value_by_id = {e.entity_id: (e.normalized_value or "").strip() for e in entities}

    def _looks_clearly_invalid(value: str) -> bool:
        t = (value or "").strip()
        if not t:
            return True
        low = t.lower()
        if low in {"n/a", "na", "none", "null", "unknown", "company name", "placeholder"}:
            return True
        if re.search(r"https?://|www\.", low):
            return True
        if re.search(r"\b\d{4}\b\s*-\s*(\d{4}|present)\b", low):
            return True
        if re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b", low) and re.search(r"\d{4}", low):
            return True
        if len(t.split()) > 12:
            return True
        return False

    def _is_generic_institution_root(value: str) -> bool:
        low = (value or "").strip().lower()
        return low in {
            "university", "college", "institute", "institution", "school", "hospital", "medical center", "research lab", "lab"
        }

    def _looks_valid_institution(value: str) -> bool:
        low = (value or "").strip().lower()
        return bool(re.search(r"\b(university|college|institute|hospital|medical center|lab|laboratory|research center|school of)\b", low)) and not _is_generic_institution_root(value)

    def _looks_valid_title_variant(value: str) -> bool:
        low = (value or "").strip().lower()
        if not low:
            return False
        if re.search(r"\b(senior|sr\.?|junior|jr\.?|intern|lead|principal|staff)\b", low):
            return True
        if re.search(r"\b(i|ii|iii|iv|v|vi|vii|viii|ix|x)\b", low):
            return True
        if re.search(r"\b(engineer|developer|scientist|analyst|architect|manager|consultant|ceo|cto|coo|cfo|barista|tutor|candidate|fellow)\b", low):
            return True
        if "/" in low:
            return True
        return False

    clean_issues = []
    seen_issue_keys = set()
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        entity_id = issue.get("entity_id")
        if entity_id is None:
            continue
        entity_id = int(entity_id)
        value = value_by_id.get(entity_id, "")

        # Guardrail: suppress common false positives for known-valid title/company patterns.
        if not _looks_clearly_invalid(value):
            if entity_type == "companies" and _looks_valid_institution(value):
                continue
            if entity_type == "job_titles" and _looks_valid_title_variant(value):
                continue
            if entity_type == "companies" and _is_generic_institution_root(value):
                pass

        reason = str(issue.get("reason") or "").strip()
        normalized_reason = re.sub(r"\s+", " ", reason).strip().casefold()
        dedupe_key = (entity_id, normalized_reason)
        if dedupe_key in seen_issue_keys:
            continue
        seen_issue_keys.add(dedupe_key)

        clean_issues.append(
            {
                "entity_id": entity_id,
                "severity": str(issue.get("severity") or "medium").strip().lower(),
                "reason": reason,
                "suggested_value": str(issue.get("suggested_value") or "").strip(),
            }
        )

    return clean_issues


def build_report_section(entity_type: str, entities: list[EntityRecord], issues: list[dict]) -> tuple[str, list[dict]]:
    by_id = {e.entity_id: e for e in entities}

    lines = [f"## {entity_type.upper()} ISSUES", ""]
    enriched = []

    if not issues:
        lines.append("No issues flagged by Groq.")
        lines.append("")
        return "\n".join(lines), enriched

    # High -> medium -> low, then most profiles first.
    severity_rank = {"high": 0, "medium": 1, "low": 2}
    issues_sorted = sorted(
        issues,
        key=lambda x: (severity_rank.get(x.get("severity"), 3), -(by_id.get(x.get("entity_id"), EntityRecord(0, "", 0, [], [])).profile_count)),
    )

    for issue in issues_sorted:
        entity = by_id.get(issue["entity_id"])
        if not entity:
            continue
        urls_preview = entity.profile_urls[:20]
        urls_text = ", ".join(urls_preview) if urls_preview else "[no profile URLs found]"
        if len(entity.profile_urls) > 20:
            urls_text += f" ... (+{len(entity.profile_urls) - 20} more)"

        reason = issue.get("reason") or "No reason provided"
        lines.append(
            f"- {entity.normalized_value} - Profiles: {urls_text} - Reason: {reason}"
        )

        enriched.append(
            {
                "entity_type": entity_type,
                "entity_id": entity.entity_id,
                "normalized_value": entity.normalized_value,
                "profile_count": entity.profile_count,
                "severity": issue.get("severity", "medium"),
                "reason": reason,
                "suggested_value": issue.get("suggested_value", ""),
                "profile_urls": entity.profile_urls,
                "sample_raw_values": entity.sample_raw_values,
            }
        )

    lines.append("")
    return "\n".join(lines), enriched


def run_audit(conn, chunk_size: int, output_txt: Path, output_json: Path):
    client, model = _get_groq_client_and_model()

    title_entities = load_title_entities(conn)
    company_entities = load_company_entities(conn)

    print(f"Loaded {len(title_entities)} normalized titles and {len(company_entities)} normalized companies")

    all_issues = []

    # Titles
    title_issues = []
    for idx, ch in enumerate(chunked(title_entities, chunk_size), start=1):
        print(f"Auditing title chunk {idx} ({len(ch)} entries)...")
        title_issues.extend(groq_audit_entities(client, model, "job_titles", ch))

    # Companies
    company_issues = []
    for idx, ch in enumerate(chunked(company_entities, chunk_size), start=1):
        print(f"Auditing company chunk {idx} ({len(ch)} entries)...")
        company_issues.extend(groq_audit_entities(client, model, "companies", ch))

    title_section, title_enriched = build_report_section("job_titles", title_entities, title_issues)
    company_section, company_enriched = build_report_section("companies", company_entities, company_issues)

    all_issues.extend(title_enriched)
    all_issues.extend(company_enriched)

    ts = datetime.now().isoformat(timespec="seconds")
    txt = []
    txt.append("Normalized Entity Audit Report")
    txt.append("==============================")
    txt.append("")
    txt.append(f"Generated: {ts}")
    txt.append(f"Title entities audited: {len(title_entities)}")
    txt.append(f"Company entities audited: {len(company_entities)}")
    txt.append(f"Total issues flagged: {len(all_issues)}")
    txt.append("")
    txt.append(title_section)
    txt.append(company_section)

    output_txt.parent.mkdir(parents=True, exist_ok=True)
    output_txt.write_text("\n".join(txt), encoding="utf-8")

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(
        json.dumps(
            {
                "generated_at": ts,
                "title_entity_count": len(title_entities),
                "company_entity_count": len(company_entities),
                "issues": all_issues,
            },
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )

    print(f"Report written: {output_txt}")
    print(f"JSON written: {output_json}")


def connect(target: str, sqlite_path: Path):
    if target == "cloud":
        conn = connect_mysql()
        return conn, "cloud"
    if target == "sqlite":
        conn = connect_sqlite(sqlite_path)
        return conn, "sqlite"

    # auto
    try:
        conn = connect_mysql()
        return conn, "cloud"
    except Exception as exc:
        print(f"Cloud connection unavailable ({exc}); falling back to SQLite.")
        conn = connect_sqlite(sqlite_path)
        return conn, "sqlite"


def main():
    parser = argparse.ArgumentParser(description="Groq audit for normalized titles/companies.")
    parser.add_argument("--target", choices=["auto", "cloud", "sqlite"], default="auto")
    parser.add_argument("--sqlite-path", default="backend/alumni_backup.db")
    parser.add_argument("--chunk-size", type=int, default=120)
    parser.add_argument("--output-txt", default="temp/groq_normalized_entity_audit_report.txt")
    parser.add_argument("--output-json", default="temp/groq_normalized_entity_audit_report.json")
    args = parser.parse_args()

    sqlite_path = (ROOT / args.sqlite_path).resolve()
    output_txt = (ROOT / args.output_txt).resolve()
    output_json = (ROOT / args.output_json).resolve()

    conn, mode = connect(args.target, sqlite_path)
    print(f"Connected to: {mode}")

    try:
        run_audit(conn, max(20, args.chunk_size), output_txt, output_json)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
