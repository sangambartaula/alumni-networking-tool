import re
from datetime import timedelta, datetime
import pandas as pd
from pathlib import Path
from config import logger

"""Pure functions for text cleaning, date parsing, and logic."""
# --- Constants for Parsing ---
MONTHS_RE = r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
DATE_RANGE_RE = re.compile(
    rf"(?P<start>(?:{MONTHS_RE}\s+\d{{4}})|(?:\d{{4}}))\s*[-–—]\s*(?P<end>(?:Present)|(?:{MONTHS_RE}\s+\d{{4}})|(?:\d{{4}}))",
    re.IGNORECASE
)
YEAR_RANGE_RE = re.compile(r"(\d{4})\s*[-–—]\s*(\d{4}|Present)", re.IGNORECASE)
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
UNT_KEYWORDS = ("unt", "university of north texas", "north texas")

DEGREE_LEVELS = {
    'ph.d': 100, 'phd': 100, 'doctor': 100, 'doctorate': 100, 'd.phil': 100,
    'master': 80, 'ms': 80, 'm.s': 80, 'mba': 80, 'm.b.a': 80, 'ma': 80, 'm.a': 80,
    'bachelor': 60, 'bs': 60, 'b.s': 60, 'ba': 60, 'b.a': 60, 'bba': 60,
    'associate': 40,
}

ENGINEERING_KEYWORDS = (
    'engineering', 'engineer', 'computer science', 'computer engineering',
    'mechanical', 'electrical', 'civil', 'chemical', 'aerospace',
    'software', 'hardware', 'materials', 'industrial', 'manufacturing',
    'biomedical', 'petroleum', 'environmental', 'systems',
    'technology', 'physics', 'mathematics', 'math',
    'data science', 'cybersecurity', 'information technology',
    'electronics', 'robotics', 'mechatronics', 'energy',
)

def clean_job_title(raw_title: str) -> str:
    if not raw_title:
        return ""
    raw = " ".join(raw_title.strip().split())
    
    # Only filter if the ENTIRE string is just an employment type
    banned_exact = {
        "Full-time", "Part-time", "Internship", "Contract", "Temporary",
        "Volunteer", "Apprenticeship", "Self-employed", "Freelance",
        "Remote", "Hybrid", "On-site"
    }
    if raw in banned_exact:
        return ""
    
    # Remove employment type suffixes (e.g., "· Full-time", "· Internship")
    # But DON'T remove if it's part of a compound title like "Summer Internship"
    for bad in banned_exact:
        # Only remove "· BadWord" patterns (LinkedIn suffix style)
        raw = re.sub(rf'\s*·\s*{bad}\b', '', raw, flags=re.I)
    
    return " ".join(raw.split()).strip()

def parse_frequency(frequency_str: str) -> timedelta:
    try:
        parts = frequency_str.strip().lower().split()
        if len(parts) != 2:
            return timedelta(days=180)
        amount = int(parts[0])
        unit = parts[1].rstrip('s')
        if unit == "day": return timedelta(days=amount)
        if unit == "month": return timedelta(days=amount * 30)
        if unit == "year": return timedelta(days=amount * 365)
        return timedelta(days=180)
    except Exception:
        return timedelta(days=180)

def load_names_from_csv(csv_path: Path):
    try:
        df = pd.read_csv(csv_path)
        if 'name' in df.columns:
            return [str(n).strip() for n in df['name'].dropna().unique() if str(n).strip()]
        if 'first_name' in df.columns and 'last_name' in df.columns:
            names = [
                f"{str(r).strip()} {str(l).strip()}".strip()
                for r, l in zip(df['first_name'].fillna(''), df['last_name'].fillna(''))
                if (str(r).strip() or str(l).strip())
            ]
            seen = set()
            uniq = []
            for n in names:
                if n not in seen:
                    seen.add(n)
                    uniq.append(n)
            return uniq
        raise ValueError("Input CSV must contain either 'name' or ('first_name','last_name').")
    except Exception as e:
        logger.error(f"Failed to read names from {csv_path}: {e}")
        return []

# --- Date Logic ---
def month_to_num(m: str) -> int:
    month_map = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
    }
    return month_map.get((m or "").strip().title(), 0)

def parse_date_token(token: str):
    raw = (token or "").strip()
    if not raw:
        return {"raw": "", "is_present": False, "year": None, "month": None, "has_month": False}
    if raw.lower().startswith("present"):
        return {"raw": "Present", "is_present": True, "year": None, "month": None, "has_month": False}
    
    mm = re.match(rf"^(?P<m>{MONTHS_RE})\s+(?P<y>\d{{4}})$", raw, re.I)
    if mm:
        m = mm.group("m").title()
        y = int(mm.group("y"))
        return {"raw": f"{m} {y}", "is_present": False, "year": y, "month": month_to_num(m), "has_month": True}
    
    yy = re.match(r"^(?P<y>\d{4})$", raw)
    if yy:
        y = int(yy.group("y"))
        return {"raw": f"{y}", "is_present": False, "year": y, "month": None, "has_month": False}
    return {"raw": raw, "is_present": False, "year": None, "month": None, "has_month": False}

def parse_date_range_line(line: str):
    if not line: return None, None
    m = DATE_RANGE_RE.search(line)
    if not m: return None, None
    start = parse_date_token(m.group("start"))
    end = parse_date_token(m.group("end"))
    if (not start.get("is_present")) and (start.get("year") is None): return None, None
    if (not end.get("is_present")) and (end.get("year") is None): return None, None
    return start, end

def format_date_for_storage(d: dict) -> str:
    if not d: return ""
    if d.get("is_present"): return "Present"
    y = d.get("year")
    if not y: return ""
    if d.get("has_month") and d.get("month"): return d.get("raw") or ""
    return str(y)

def date_to_comparable(d: dict, bound: str):
    if not d: return None
    if d.get("is_present"): return (9999, 12, "month")
    y = d.get("year")
    if not y: return None
    if d.get("has_month") and d.get("month"): return (y, int(d.get("month")), "month")
    if bound == "start": return (y, 1, "year")
    return (y, 12, "year")

def check_working_while_studying(school_start: dict, school_end: dict, job_start: dict, job_end: dict) -> bool:
    ss = date_to_comparable(school_start, "start")
    se = date_to_comparable(school_end, "end")
    js = date_to_comparable(job_start, "start")
    je = date_to_comparable(job_end, "end")

    if not (ss and se and js and je): return False

    school_month_precise = bool(school_start.get("has_month") and school_end.get("has_month"))
    job_month_precise = bool(job_start.get("has_month") and (job_end.get("has_month") or job_end.get("is_present")))

    # If both have month precision, use full date comparison
    if school_month_precise and job_month_precise:
        # Two ranges [ss, se] and [js, je] overlap if: start1 <= end2 AND start2 <= end1
        return (ss[0], ss[1]) <= (je[0], je[1]) and (js[0], js[1]) <= (se[0], se[1])

    # Year-only comparison: check if year ranges overlap
    # Ranges overlap if: start1 <= end2 AND start2 <= end1
    ss_y, se_y, js_y, je_y = ss[0], se[0], js[0], je[0]
    return ss_y <= je_y and js_y <= se_y