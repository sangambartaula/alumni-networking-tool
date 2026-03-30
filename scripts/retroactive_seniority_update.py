import sys
import csv
import mysql.connector
import re
from pathlib import Path

# Self-contained logic from seniority_detector.py to avoid sandbox import errors
SENIORITY_PATTERNS = [
    ("Executive", re.compile(
        r'\b(CEO|CTO|CFO|COO|CIO|CMO|CPO|CISO|Chief|President|Founder|Co-Founder|'
        r'Vice\s*President|VP|EVP|SVP)\b', re.IGNORECASE
    )),
    ("Director", re.compile(
        r'\b(Director|Head\s+of|Principal)\b', re.IGNORECASE
    )),
    ("Manager", re.compile(
        r'\b(Manager|Supervisor|Program\s+Manager|Scrum\s+Master)\b', re.IGNORECASE
    )),
    ("Senior", re.compile(
        r'\b(Senior|Sr\.?|Staff|Distinguished|Fellow|Team\s+Lead|Tech\s+Lead|Lead\s+Engineer|'
        r'Engineering\s+Lead|Project\s+Lead|Lead)\b', re.IGNORECASE
    )),
    ("Junior", re.compile(
        r'\b(Junior|Jr\.?|Entry[\s-]?Level|Associate(?!\s+(?:Director|VP|Vice|Manager|Principal))|Apprentice)\b', re.IGNORECASE
    )),
    ("Intern", re.compile(
        r'\b(Intern|Internship|Co-op|Coop|Trainee|Student\s+Worker|'
        r'Student\s+Employee|Research\s+Assistant|Teaching\s+Assistant)\b', re.IGNORECASE
    )),
]

def _merge_seniority_level(seniority: str) -> str:
    s = (seniority or "").strip()
    if s == "Intern": return "Intern"
    if s in {"Junior", "Mid"}: return "Mid"
    if s == "Senior": return "Senior"
    if s == "Manager": return "Manager"
    if s in {"Director", "Executive"}: return "Executive"
    return "Mid"

def detect_seniority(job_title, employment_type=None):
    title = str(job_title).strip() if job_title else ""
    et = (employment_type or "").strip()
    
    et_hint = None
    if et:
        if re.search(r"\b(intern(ship)?|co-?op|trainee|student\s+worker|student\s+employee)\b", et, re.IGNORECASE):
            et_hint = "Intern"
        elif re.search(r"\b(entry[\s-]?level|apprentice(ship)?)\b", et, re.IGNORECASE):
            et_hint = "Junior"

    if not title:
        if et_hint == "Intern": return "Intern"
        if et_hint == "Junior": return "Junior"
        return "Mid"

    for seniority, pattern in SENIORITY_PATTERNS:
        if pattern.search(title): return seniority

    if et_hint == "Intern": return "Intern"
    if et_hint == "Junior": return "Junior"
    return "Mid"

def run_retroactive_update():
    print("=" * 60)
    print("SENIORITY RETROACTIVE UPDATE (MySQL + CSV)")
    print("=" * 60)

    conn = mysql.connector.connect(
        host="ballast.proxy.rlwy.net",
        user="root",
        password="jvLJhcDWxAheXdFXTNrtDDkmLxIstYGe",
        database="linkedinhelper",
        port=37157
    )
    
    url_to_seniority = {}
    updated_db_count = 0

    try:
        with conn.cursor(dictionary=True) as cur:
            cur.execute("SELECT id, linkedin_url, current_job_title, job_employment_type, seniority_level FROM alumni WHERE linkedin_url IS NOT NULL")
            rows = cur.fetchall()
            
            for row in rows:
                url = str(row['linkedin_url']).strip().rstrip('/')
                job_title = row['current_job_title']
                employment_type = row['job_employment_type']
                old_seniority = row['seniority_level']
                
                raw_seniority = detect_seniority(job_title, employment_type)
                new_seniority = _merge_seniority_level(raw_seniority)
                
                url_to_seniority[url] = new_seniority
                
                if str(old_seniority) != str(new_seniority):
                    cur.execute("UPDATE alumni SET seniority_level = %s WHERE id = %s", (new_seniority, row['id']))
                    updated_db_count += 1
            
        conn.commit()
        print(f"✅ Updated {updated_db_count} database rows with new seniority levels.")
    except Exception as e:
        print(f"❌ DB Error: {e}")
    finally:
        conn.close()

    PROJECT_ROOT = Path('/Users/sangambartaula/Documents/GitHub/alumni-networking-tool')
    csv_path = PROJECT_ROOT / 'scraper' / 'output' / 'UNT_Alumni_Data.csv'

    try:
        updated_csv_count = 0
        with open(csv_path, 'r', encoding='utf-8') as fin:
            reader = csv.DictReader(fin)
            fields = reader.fieldnames
            if 'seniority_level' not in fields:
                fields.append('seniority_level')
            csv_rows = list(reader)

        for row in csv_rows:
            url = str(row.get('linkedin_url', '')).strip().rstrip('/')
            if url in url_to_seniority:
                new_sen = url_to_seniority[url]
                if str(row.get('seniority_level')) != str(new_sen):
                    row['seniority_level'] = new_sen
                    updated_csv_count += 1

        with open(csv_path, 'w', encoding='utf-8', newline='') as fout:
            writer = csv.DictWriter(fout, fieldnames=fields)
            writer.writeheader()
            writer.writerows(csv_rows)

        print(f"✅ Updated CSV with {updated_csv_count} new seniority levels.")
    except PermissionError:
        print(f"⚠️ Skipping CSV update due to permissions. (Run this script locally to sync CSV)")
    except Exception as e:
        print(f"❌ CSV Update Error: {e}")

if __name__ == "__main__":
    run_retroactive_update()
