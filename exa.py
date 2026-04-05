import csv
import json
import os
import sqlite3
import time
import importlib
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv
from exa_py import Exa

try:
    psycopg2 = importlib.import_module("psycopg2")
except ImportError as exc:
    raise RuntimeError(
        "psycopg2 is required for cloud alumni dedupe checks. Install with: pip install psycopg2-binary"
    ) from exc


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"❌ {name} not found in .env file")
    return value


def normalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlsplit(url.strip())
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    path = (parsed.path or "").rstrip("/")
    return urlunsplit((scheme, netloc, path, "", ""))


def safe_text(value) -> str:
    text = str(value).strip() if value is not None else ""
    return text if text else "N/A"


def clean_range(start, end):
    start_text = safe_text(start)
    if start_text == "N/A":
        return "N/A"
    end_text = safe_text(end)
    return f"{start_text} - {('Present' if end_text == 'N/A' else end_text)}"


def job_dates(start, end):
    start_text = safe_text(start)
    if start_text == "N/A":
        return "N/A", "N/A"
    end_text = safe_text(end)
    return start_text, ("Present" if end_text == "N/A" else end_text)


def as_list(value):
    return value if isinstance(value, list) else []


def as_dict(value):
    return value if isinstance(value, dict) else {}


def has_unt_education(output: dict) -> bool:
    schools = []
    for edu in as_list(output.get("education")):
        school = safe_text(as_dict(edu).get("school"))
        if school != "N/A":
            schools.append(school.lower())

    for school in schools:
        if "university of north texas" in school or " unt " in f" {school} ":
            return True
    return False


def fetch_existing_cloud_urls(pg_conn, normalized_urls):
    if not normalized_urls:
        return set()
    sql = """
        SELECT LOWER(REGEXP_REPLACE(linkedin_url, '/+$', '')) AS normalized_url
        FROM alumni
        WHERE LOWER(REGEXP_REPLACE(linkedin_url, '/+$', '')) = ANY(%s)
    """
    with pg_conn.cursor() as cur:
        cur.execute(sql, (list(normalized_urls),))
        return {row[0] for row in cur.fetchall() if row and row[0]}


def parse_target_divisor() -> int:
    raw = os.getenv("EXA_TARGET_DIVISOR", "1").strip()
    try:
        divisor = int(raw)
    except ValueError as exc:
        raise ValueError(f"❌ EXA_TARGET_DIVISOR must be an integer >= 1, got: {raw}") from exc
    if divisor < 1:
        raise ValueError(f"❌ EXA_TARGET_DIVISOR must be >= 1, got: {raw}")
    return divisor


load_dotenv()
EXA_API_KEY = require_env("EXA_API_KEY")
DATABASE_URL = require_env("DATABASE_URL")

exa = Exa(EXA_API_KEY)

CSV_FILE = os.getenv("EXA_CSV_FILE", "EXA_STAGING_OUTPUT.csv")
DB_FILE = os.getenv("EXA_STAGING_DB", "backend/alumni_backup.db")

CSV_COLUMNS = [
    "first",
    "last",
    "linkedin_url",
    "location",
    "headline",
    "discipline",
    "school",
    "degree",
    "major",
    "s1_start",
    "s1_end",
    "s2_school",
    "s2_degree",
    "s3_school",
    "s3_degree",
    "job_title",
    "company",
    "job_start",
    "job_end",
    "exp2_title",
    "exp2_company",
    "exp2_dates",
    "exp3_title",
    "exp3_company",
    "exp3_dates",
]

# 2. Database & Staging Table Setup
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS exa_staging_raw (
        url TEXT PRIMARY KEY,
        json_data TEXT,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
)
conn.commit()

# Load existing URLs to ensure zero double-spending
cursor.execute("SELECT url FROM exa_staging_raw")
seen_urls = {normalize_url(row[0]) for row in cursor.fetchall() if row and row[0]}

pg_conn = psycopg2.connect(DATABASE_URL)
pg_conn.autocommit = True
cloud_seen_cache = set()

# 3. Lean Extraction Schema
output_schema = {
    "type": "object",
    "properties": {
        "first": {"type": "string"},
        "last": {"type": "string"},
        "location": {"type": "string"},
        "headline": {"type": "string", "description": "Professional tagline"},
        "education": {
            "type": "array",
            "maxItems": 3,
            "items": {
                "type": "object",
                "properties": {
                    "school": {"type": "string"},
                    "degree": {"type": "string"},
                    "major": {"type": "string"},
                    "start": {"type": "string"},
                    "end": {"type": "string"},
                },
            },
        },
        "experience": {
            "type": "array",
            "maxItems": 3,
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "company": {"type": "string"},
                    "start": {"type": "string"},
                    "end": {"type": "string"},
                },
            },
        },
    },
}

# 4. Balanced Disciplines & Targets
disciplines = [
    {
        "name": "Software",
        "target": 4000,
        "majors": ["Computer Science", "Cybersecurity", "Artificial Intelligence", "Information Technology"],
    },
    {"name": "Mechanical", "target": 2000, "majors": ["Mechanical Engineering", "Materials Science"]},
    {"name": "Electrical", "target": 1500, "majors": ["Electrical Engineering", "Computer Engineering"]},
    {
        "name": "Construction",
        "target": 1500,
        "majors": ["Construction Management", "Construction Engineering Technology"],
    },
    {"name": "Biomedical", "target": 1000, "majors": ["Biomedical Engineering"]},
]

TARGET_DIVISOR = parse_target_divisor()
for group in disciplines:
    full_target = group["target"]
    reduced_target = max(1, full_target // TARGET_DIVISOR)
    group["full_target"] = full_target
    group["target"] = reduced_target

full_total_target = sum(group["full_target"] for group in disciplines)
effective_total_target = sum(group["target"] for group in disciplines)

# 5. Execution Loop
print(
    f"🚀 Starting run. Full target: {full_total_target} | "
    f"Divisor: {TARGET_DIVISOR} | Effective target: {effective_total_target}. "
    f"Staging to {CSV_FILE}..."
)
csv_has_header = os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0

try:
    for group in disciplines:
        grp_count = 0
        search_mode = "deep" if group["name"] in ["Biomedical", "Construction"] else "auto"

        for major in group["majors"]:
            if grp_count >= group["target"]:
                break

            for year in range(2015, 2027):
                if grp_count >= group["target"]:
                    break

                query = f"site:linkedin.com/in/ \"University of North Texas\" \"{major}\" {year}"
                print(f"🔎 Querying: {major} ({year}) | Mode: {search_mode}")

                try:
                    # Lightweight URL pass first to avoid content-spend on already-known URLs.
                    pre = exa.search(query, type=search_mode, category="people", num_results=25)
                    pre_results = getattr(pre, "results", []) or []
                    candidate_urls = {
                        normalize_url(getattr(item, "url", ""))
                        for item in pre_results
                        if normalize_url(getattr(item, "url", ""))
                    }

                    unknown_urls = [u for u in candidate_urls if u not in seen_urls and u not in cloud_seen_cache]
                    if unknown_urls:
                        cloud_existing = fetch_existing_cloud_urls(pg_conn, unknown_urls)
                        cloud_seen_cache.update(cloud_existing)

                    remaining = [u for u in candidate_urls if u not in seen_urls and u not in cloud_seen_cache]
                    if not remaining:
                        print("   ⏭️ Skipped batch: all candidate URLs already exist in staging/cloud.")
                        time.sleep(0.4)
                        continue

                    res = exa.search_and_contents(
                        query,
                        type=search_mode,
                        category="people",
                        num_results=25,
                        text={"max_characters": 12000},
                        output_schema=output_schema,
                        system_prompt=(
                            "Extract the 3 most recent entries. Use 'Present' for current jobs. "
                            "Use 'N/A' for missing fields."
                        ),
                    )

                    batch_rows = []
                    for p in getattr(res, "results", []) or []:
                        normalized = normalize_url(getattr(p, "url", ""))
                        if not normalized:
                            continue
                        if normalized in seen_urls or normalized in cloud_seen_cache:
                            continue

                        output = as_dict(getattr(p, "output", {}) or {})
                        if not output:
                            continue

                        # UNT validation gate: only persist records with explicit UNT school mention.
                        if not has_unt_education(output):
                            continue

                        edu = [as_dict(x) for x in as_list(output.get("education"))]
                        exp = [as_dict(x) for x in as_list(output.get("experience"))]

                        start1, end1 = job_dates(
                            exp[0].get("start") if exp else None,
                            exp[0].get("end") if exp else None,
                        )

                        row = [
                            safe_text(output.get("first")),
                            safe_text(output.get("last")),
                            normalized,
                            safe_text(output.get("location")),
                            safe_text(output.get("headline")),
                            group["name"],
                            # Edu 1
                            safe_text(edu[0].get("school") if edu else None),
                            safe_text(edu[0].get("degree") if edu else None),
                            safe_text(edu[0].get("major") if edu else None),
                            safe_text(edu[0].get("start") if edu else None),
                            safe_text(edu[0].get("end") if edu else None),
                            # Edu 2 & 3
                            safe_text(edu[1].get("school") if len(edu) > 1 else None),
                            safe_text(edu[1].get("degree") if len(edu) > 1 else None),
                            safe_text(edu[2].get("school") if len(edu) > 2 else None),
                            safe_text(edu[2].get("degree") if len(edu) > 2 else None),
                            # Job 1
                            safe_text(exp[0].get("title") if exp else None),
                            safe_text(exp[0].get("company") if exp else None),
                            start1,
                            end1,
                            # Exp 2
                            safe_text(exp[1].get("title") if len(exp) > 1 else None),
                            safe_text(exp[1].get("company") if len(exp) > 1 else None),
                            clean_range(
                                exp[1].get("start") if len(exp) > 1 else None,
                                exp[1].get("end") if len(exp) > 1 else None,
                            ),
                            # Exp 3
                            safe_text(exp[2].get("title") if len(exp) > 2 else None),
                            safe_text(exp[2].get("company") if len(exp) > 2 else None),
                            clean_range(
                                exp[2].get("start") if len(exp) > 2 else None,
                                exp[2].get("end") if len(exp) > 2 else None,
                            ),
                        ]

                        cursor.execute(
                            "INSERT OR IGNORE INTO exa_staging_raw (url, json_data) VALUES (?, ?)",
                            (normalized, json.dumps(output, ensure_ascii=False)),
                        )

                        if cursor.rowcount > 0:
                            seen_urls.add(normalized)
                            batch_rows.append(row)
                            grp_count += 1
                            if grp_count >= group["target"]:
                                break

                    # Buffered CSV write: one file open per batch.
                    if batch_rows:
                        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
                            writer = csv.writer(f)
                            if not csv_has_header:
                                writer.writerow(CSV_COLUMNS)
                                csv_has_header = True
                            writer.writerows(batch_rows)

                    conn.commit()
                    print(f"   ✅ Batch complete. {group['name']} Progress: {grp_count}/{group['target']}")
                    time.sleep(1.2)

                except Exception as e:
                    print(f"   ⚠️ Error: {e}")
                    time.sleep(5)
finally:
    try:
        conn.close()
    except Exception:
        pass
    try:
        pg_conn.close()
    except Exception:
        pass

print("🏁 Production Run Complete.")