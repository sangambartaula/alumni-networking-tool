import csv
import json
import os
import sqlite3
import time
import importlib
import re
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv
from exa_py import Exa

try:
    mysql_connector = importlib.import_module("mysql.connector")
except ImportError as exc:
    raise RuntimeError(
        "mysql-connector-python is required for cloud alumni writes. Install with: pip install mysql-connector-python"
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


def split_delimited_line(raw, field_names):
    text = safe_text(raw)
    parts = [part.strip() for part in text.split(";;", len(field_names) - 1)]
    if len(parts) < len(field_names):
        parts.extend(["N/A"] * (len(field_names) - len(parts)))
    return {field_names[index]: safe_text(parts[index]) for index in range(len(field_names))}


def parse_delimited_entries(lines, field_names, max_items=3):
    entries = []
    for raw in as_list(lines):
        if len(entries) >= max_items:
            break
        entries.append(split_delimited_line(raw, field_names))
    return entries


def normalize_pipe_segment(raw_segment, expected_fields):
    text = safe_text(raw_segment)
    parts = [part.strip() for part in text.split("|")]
    if len(parts) > expected_fields:
        parts = parts[:expected_fields]
    if len(parts) < expected_fields:
        parts.extend(["N/A"] * (expected_fields - len(parts)))
    return ";;".join(safe_text(part) for part in parts[:expected_fields])


def classify_tail_segment(raw_segment: str) -> str:
    text = safe_text(raw_segment)
    lowered = text.lower()
    if text == "N/A":
        return "unknown"

    education_markers = [
        "university",
        "college",
        "institute",
        "school",
        "bachelor",
        "master",
        "phd",
        "ph.d",
        "degree",
    ]
    experience_markers = [
        " at ",
        "intern",
        "engineer",
        "manager",
        "director",
        "analyst",
        "consultant",
        "assistant",
        "technician",
        "coordinator",
        "developer",
    ]

    has_education = any(marker in lowered for marker in education_markers)
    has_experience = any(marker in lowered for marker in experience_markers)

    if has_education and not has_experience:
        return "education"
    if has_experience and not has_education:
        return "experience"

    # Tie-breaker: most education rows naturally carry 5 pipe fields.
    pipe_count = text.count("|")
    if pipe_count >= 4:
        return "education"
    return "experience"


def parse_summary_output(summary_text: str):
    text = safe_text(summary_text)
    if text == "N/A":
        return {}

    segments = [segment.strip() for segment in text.split(";;")]
    if len(segments) < 4:
        return {}

    tail_segments = [segment.strip() for segment in segments[4:] if segment.strip()]

    education = []
    experience = []
    for segment in tail_segments:
        segment_type = classify_tail_segment(segment)
        if segment_type == "education" and len(education) < 3:
            education.append(normalize_pipe_segment(segment, expected_fields=5))
            continue
        if segment_type == "experience" and len(experience) < 3:
            experience.append(normalize_pipe_segment(segment, expected_fields=4))
            continue

        # Fallback if classification is ambiguous or a bucket is already full.
        if len(education) < 3:
            education.append(normalize_pipe_segment(segment, expected_fields=5))
        elif len(experience) < 3:
            experience.append(normalize_pipe_segment(segment, expected_fields=4))

    while len(education) < 3:
        education.append(normalize_pipe_segment("N/A", expected_fields=5))
    while len(experience) < 3:
        experience.append(normalize_pipe_segment("N/A", expected_fields=4))

    return {
        "first": safe_text(segments[0]),
        "last": safe_text(segments[1]),
        "location": safe_text(segments[2]),
        "headline": safe_text(segments[3]),
        "education": education,
        "experience": experience,
    }


def extract_education_entries(output: dict):
    education_lines = output.get("education")
    if education_lines is not None:
        return parse_delimited_entries(
            education_lines,
            field_names=("school", "degree", "major", "start", "end"),
            max_items=3,
        )

    # Backward-compatible fallback in case old shape appears.
    legacy = []
    for item in as_list(output.get("education"))[:3]:
        d = as_dict(item)
        if not d:
            continue
        legacy.append(
            {
                "school": safe_text(d.get("school")),
                "degree": safe_text(d.get("degree")),
                "major": safe_text(d.get("major")),
                "start": safe_text(d.get("start")),
                "end": safe_text(d.get("end")),
            }
        )
    return legacy


def extract_experience_entries(output: dict):
    experience_lines = output.get("experience")
    if experience_lines is not None:
        return parse_delimited_entries(
            experience_lines,
            field_names=("title", "company", "start", "end"),
            max_items=3,
        )

    # Backward-compatible fallback in case old shape appears.
    legacy = []
    for item in as_list(output.get("experience"))[:3]:
        d = as_dict(item)
        if not d:
            continue
        legacy.append(
            {
                "title": safe_text(d.get("title")),
                "company": safe_text(d.get("company")),
                "start": safe_text(d.get("start")),
                "end": safe_text(d.get("end")),
            }
        )
    return legacy


def raw_mentions_unt(lines) -> bool:
    for raw in as_list(lines):
        text = safe_text(raw).lower()
        if "north texas" in text or " unt " in f" {text} ":
            return True
    return False


def has_unt_education(output: dict) -> bool:
    education_lines = output.get("education")
    if raw_mentions_unt(education_lines):
        return True

    # Backward-compatible fallback for legacy structured output.
    for edu in extract_education_entries(output):
        school = safe_text(edu.get("school")).lower()
        if "north texas" in school or " unt " in f" {school} ":
            return True
    return False


def sanitize_mysql_identifier(name: str) -> str:
    cleaned = (name or "").strip()
    if not cleaned or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cleaned):
        raise ValueError(f"❌ Invalid MySQL table name: {name}")
    return cleaned


def get_mysql_connection():
    return mysql_connector.connect(
        host=require_env("MYSQLHOST"),
        user=require_env("MYSQLUSER"),
        password=require_env("MYSQLPASSWORD"),
        database=require_env("MYSQL_DATABASE"),
        port=int(os.getenv("MYSQLPORT", "3306")),
    )


def ensure_exa_cloud_table(mysql_conn, table_name: str):
    sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id BIGINT NOT NULL AUTO_INCREMENT,
            linkedin_url VARCHAR(512) NOT NULL,
            first_name VARCHAR(255) NOT NULL DEFAULT 'N/A',
            last_name VARCHAR(255) NOT NULL DEFAULT 'N/A',
            location VARCHAR(255) NOT NULL DEFAULT 'N/A',
            headline TEXT,
            discipline VARCHAR(128) NOT NULL DEFAULT 'N/A',
            school VARCHAR(255) NOT NULL DEFAULT 'N/A',
            degree VARCHAR(255) NOT NULL DEFAULT 'N/A',
            major VARCHAR(255) NOT NULL DEFAULT 'N/A',
            job_title VARCHAR(255) NOT NULL DEFAULT 'N/A',
            company VARCHAR(255) NOT NULL DEFAULT 'N/A',
            job_start VARCHAR(64) NOT NULL DEFAULT 'N/A',
            job_end VARCHAR(64) NOT NULL DEFAULT 'N/A',
            raw_json JSON,
            source VARCHAR(64) NOT NULL DEFAULT 'exa_ai',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_exa_ai_linkedin_url (linkedin_url)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    with mysql_conn.cursor() as cur:
        cur.execute(sql)
    mysql_conn.commit()


def fetch_existing_cloud_urls(mysql_conn, table_name: str, normalized_urls):
    if not normalized_urls:
        return set()
    placeholders = ", ".join(["%s"] * len(normalized_urls))
    sql = f"SELECT linkedin_url FROM `{table_name}` WHERE linkedin_url IN ({placeholders})"
    with mysql_conn.cursor() as cur:
        cur.execute(sql, list(normalized_urls))
        return {row[0] for row in cur.fetchall() if row and row[0]}


def insert_cloud_row(mysql_conn, table_name: str, normalized_url: str, output: dict, discipline: str):
    edu = extract_education_entries(output)
    exp = extract_experience_entries(output)
    start1, end1 = job_dates(
        exp[0].get("start") if exp else None,
        exp[0].get("end") if exp else None,
    )

    params = (
        normalized_url,
        safe_text(output.get("first")),
        safe_text(output.get("last")),
        safe_text(output.get("location")),
        safe_text(output.get("headline")),
        safe_text(discipline),
        safe_text(edu[0].get("school") if edu else None),
        safe_text(edu[0].get("degree") if edu else None),
        safe_text(edu[0].get("major") if edu else None),
        safe_text(exp[0].get("title") if exp else None),
        safe_text(exp[0].get("company") if exp else None),
        start1,
        end1,
        json.dumps(output, ensure_ascii=False),
    )

    sql = f"""
        INSERT IGNORE INTO `{table_name}` (
            linkedin_url,
            first_name,
            last_name,
            location,
            headline,
            discipline,
            school,
            degree,
            major,
            job_title,
            company,
            job_start,
            job_end,
            raw_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with mysql_conn.cursor() as cur:
        cur.execute(sql, params)
        inserted = cur.rowcount > 0
    mysql_conn.commit()
    return inserted


def parse_target_divisor() -> int:
    raw = os.getenv("EXA_TARGET_DIVISOR", "1").strip()
    try:
        divisor = int(raw)
    except ValueError as exc:
        raise ValueError(f"❌ EXA_TARGET_DIVISOR must be an integer >= 1, got: {raw}") from exc
    if divisor < 1:
        raise ValueError(f"❌ EXA_TARGET_DIVISOR must be >= 1, got: {raw}")
    return divisor


ENV_FILE = Path(__file__).resolve().with_name(".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)
EXA_API_KEY = require_env("EXA_API_KEY")
EXA_CLOUD_TABLE = sanitize_mysql_identifier(os.getenv("EXA_CLOUD_TABLE", "exa_ai_alumni"))
EXA_DEBUG_FILE = Path(os.getenv("EXA_DEBUG_FILE", "exa_debug.log"))
EXA_DEBUG_PREVIEW_LIMIT = max(1, int(os.getenv("EXA_DEBUG_PREVIEW_LIMIT", "3")))

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

mysql_conn = get_mysql_connection()
ensure_exa_cloud_table(mysql_conn, EXA_CLOUD_TABLE)
cloud_seen_cache = set()

# 3. Summary Query Template
SUMMARY_QUERY = (
    "Extract exactly one line using this format and delimiter rules: "
    "First;;Last;;Location;;Headline;;"
    "Edu1School|Edu1Degree|Edu1Major|Edu1Start|Edu1End;;"
    "Edu2School|Edu2Degree|Edu2Major|Edu2Start|Edu2End;;"
    "Edu3School|Edu3Degree|Edu3Major|Edu3Start|Edu3End;;"
    "Exp1Title|Exp1Company|Exp1Start|Exp1End;;"
    "Exp2Title|Exp2Company|Exp2Start|Exp2End;;"
    "Exp3Title|Exp3Company|Exp3Start|Exp3End. "
    "Use N/A for missing fields. Use Present for current roles."
)

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


def debug_preview_output(query: str, output: dict, preview_index: int):
    first = safe_text(output.get("first"))
    last = safe_text(output.get("last"))
    lines = [
        f"\n=== DEBUG PREVIEW #{preview_index} ===",
        f"Query: {query}",
        f"Name: {first} {last}",
        f"Raw keys: {', '.join(sorted(output.keys()))}",
        "education:",
    ]

    for idx, raw_line in enumerate(as_list(output.get("education")), start=1):
        lines.append(f"  edu[{idx}]: {raw_line}")

    lines.append("experience:")
    for idx, raw_line in enumerate(as_list(output.get("experience")), start=1):
        lines.append(f"  exp[{idx}]: {raw_line}")

    if not as_list(output.get("education")):
        lines.append("  (none)")
    if not as_list(output.get("experience")):
        lines.append("  (none)")

    preview_text = "\n".join(lines) + "\n"
    print(preview_text, end="")
    with open(EXA_DEBUG_FILE, "a", encoding="utf-8") as debug_file:
        debug_file.write(preview_text)


def debug_log_query(query: str, pre_count: int, result_count: int):
    message = (
        f"\n=== QUERY ===\n"
        f"{query}\n"
        f"pre_results={pre_count}\n"
        f"summary_results={result_count}\n"
    )
    print(message, end="")
    with open(EXA_DEBUG_FILE, "a", encoding="utf-8") as debug_file:
        debug_file.write(message)


def debug_log_result(url: str, output: dict, summary_text: str):
    lines = [
        f"URL: {url}",
        f"summary_raw: {safe_text(summary_text)}",
        f"output_keys: {', '.join(sorted(output.keys()))}",
        f"first: {safe_text(output.get('first'))}",
        f"last: {safe_text(output.get('last'))}",
        f"location: {safe_text(output.get('location'))}",
        f"headline: {safe_text(output.get('headline'))}",
        "education:",
    ]

    education_lines = as_list(output.get("education"))
    for idx, raw_line in enumerate(education_lines, start=1):
        lines.append(f"  edu[{idx}]: {raw_line}")
    if not education_lines:
        lines.append("  (none)")

    lines.append("experience:")
    experience_lines = as_list(output.get("experience"))
    for idx, raw_line in enumerate(experience_lines, start=1):
        lines.append(f"  exp[{idx}]: {raw_line}")
    if not experience_lines:
        lines.append("  (none)")

    preview_text = "\n".join(lines) + "\n"
    print(preview_text, end="")
    with open(EXA_DEBUG_FILE, "a", encoding="utf-8") as debug_file:
        debug_file.write(preview_text)

# 5. Execution Loop
EXA_DEBUG_FILE.write_text(
    f"Exa debug session started at {time.strftime('%Y-%m-%d %H:%M:%S')}\n",
    encoding="utf-8",
)
print(
    f"🚀 Starting run. Full target: {full_total_target} | "
    f"Divisor: {TARGET_DIVISOR} | Effective target: {effective_total_target}. "
    f"Staging to {CSV_FILE}. Cloud table: {EXA_CLOUD_TABLE}. Debug log: {EXA_DEBUG_FILE}..."
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
                    preview_count = 0
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
                        cloud_existing = fetch_existing_cloud_urls(mysql_conn, EXA_CLOUD_TABLE, unknown_urls)
                        cloud_seen_cache.update(cloud_existing)

                    remaining = [u for u in candidate_urls if u not in seen_urls and u not in cloud_seen_cache]
                    if not remaining:
                        print("   ⏭️ Skipped batch: all candidate URLs already exist in staging/cloud.")
                        time.sleep(0.4)
                        continue

                    res = exa.search(
                        query,
                        type=search_mode,
                        category="people",
                        num_results=25,
                        contents={"summary": {"query": SUMMARY_QUERY}},
                    )
                    res_results = getattr(res, "results", []) or []
                    debug_log_query(query, len(pre_results), len(res_results))
                    with open(EXA_DEBUG_FILE, "a", encoding="utf-8") as debug_file:
                        debug_file.write(f"summary_results={len(res_results)}\n")

                    batch_rows = []
                    for p in res_results:
                        normalized = normalize_url(getattr(p, "url", ""))
                        if not normalized:
                            continue

                        summary_text = getattr(p, "summary", "")
                        output = parse_summary_output(summary_text)
                        debug_log_result(normalized, output, summary_text)
                        if not output:
                            continue

                        if normalized in seen_urls or normalized in cloud_seen_cache:
                            continue

                        if preview_count < EXA_DEBUG_PREVIEW_LIMIT:
                            preview_count += 1
                            debug_preview_output(query, output, preview_count)

                        if not has_unt_education(output):
                            continue

                        edu = extract_education_entries(output)
                        exp = extract_experience_entries(output)

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

                        cloud_inserted = insert_cloud_row(mysql_conn, EXA_CLOUD_TABLE, normalized, output, group["name"])
                        if cloud_inserted:
                            cloud_seen_cache.add(normalized)

                        if cursor.rowcount > 0 or cloud_inserted:
                            seen_urls.add(normalized)
                            cloud_seen_cache.add(normalized)
                            if cursor.rowcount > 0:
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
        mysql_conn.close()
    except Exception:
        pass

print("🏁 Production Run Complete.")