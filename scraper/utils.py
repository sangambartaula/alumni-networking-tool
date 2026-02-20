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
    'certificate': 20, 'cert': 20,
    'high school': 10, 'hs': 10, 'diploma': 10, 'ged': 10
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

def determine_work_study_status(
    school_end: dict,
    job_start: dict,
    job_end: dict,
    is_expected: bool = False,
) -> str:
    """
    Determine whether an alumni worked before, during, or after graduation.

    Logic (per spec):
      1. Identify graduation year from school_end. Year 9999 means "Present/ongoing".
      2. If graduation is marked "Expected" (or school end is_present / 9999) → treat as
         not yet graduated (currently studying).
      3. Compare job_start year with graduation year:
         a. Job start year < grad year → "yes" (Worked While Studying).
         b. Grad year in the future (or Expected) AND job is active → "currently"
            (Currently Working While Studying).
         c. Job start year >= grad year → "no" (Worked After Graduation / no overlap).

    Returns:
        "yes"       – worked while studying (job started before graduation)
        "currently" – actively working while still studying (expected grad or no grad yet)
        "no"        – worked after graduation
        ""          – insufficient data to determine
    """
    CURRENT_YEAR = datetime.now().year

    # --- Get graduation year -------------------------------------------------
    grad_year = None
    still_studying = False  # True if not yet graduated

    if school_end:
        if school_end.get("is_present") or school_end.get("year") == 9999:
            # Ongoing education → not yet graduated
            still_studying = True
        else:
            grad_year = school_end.get("year")

    # Treat "Expected" flag or missing graduation year as not yet graduated
    if is_expected or grad_year is None:
        still_studying = True

    # --- Get job start year --------------------------------------------------
    job_start_year = None
    if job_start and not job_start.get("is_present"):
        job_start_year = job_start.get("year")

    if job_start_year is None:
        return ""  # Can't determine without job start

    # --- Classify ------------------------------------------------------------
    if still_studying:
        # Not yet graduated: any active job = currently working while studying
        job_is_active = job_end is None or job_end.get("is_present") or job_end.get("year") == 9999
        if job_is_active:
            return "currently"
        # Job has ended but they were still in school → "yes"
        job_end_year = job_end.get("year") if job_end else None
        if job_end_year and job_end_year > CURRENT_YEAR:
            return "currently"
        return "yes"

    # grad_year is known
    # Step 4: job started before graduation → worked while studying
    if job_start_year < grad_year:
        return "yes"

    # Step 5: grad year is in the future and job is active → currently working while studying
    if grad_year > CURRENT_YEAR:
        job_is_active = job_end is None or job_end.get("is_present") or (job_end.get("year") or 0) > CURRENT_YEAR
        if job_is_active:
            return "currently"

    # Step 6: job started at or after graduation → worked after graduation
    return "no"


def check_working_while_studying(school_start: dict, school_end: dict, job_start: dict, job_end: dict) -> bool:
    # 1. Get comparable dates (Year, Month)
    ss = date_to_comparable(school_start, "start")
    se = date_to_comparable(school_end, "end")
    js = date_to_comparable(job_start, "start")
    je = date_to_comparable(job_end, "end")

    if not (ss and se and js and je): return False

    # 2. Get Overlap Interval
    # Max of starts
    # Since we have tuples (Year, Month, Precision), we can compare them directly basically.
    # But precision matters.
    # Rules:
    # - If job is PRESENT/Ongoing -> Overlap is TRUE if Job Start <= School End (and School Start <= Present).
    # - If job is FINITE -> Duration must be >= 2 months.
    
    # Normalize to comparable values: (Year * 12 + Month)
    def to_months(d_tuple):
        return d_tuple[0] * 12 + d_tuple[1]

    ss_m = to_months(ss)
    se_m = to_months(se)
    js_m = to_months(js)
    
    is_job_present = job_end.get("is_present")
    je_m = to_months(je) if not is_job_present else 999999 # Far future

    # Check for simple overlap first
    overlap_start = max(ss_m, js_m)
    overlap_end = min(se_m, je_m)

    if overlap_end < overlap_start:
        return False

    # 3. Check Duration/Rules
    
    # If Job is Present (Ongoing), and there is ANY overlap, it counts.
    # Rationale: User said: "if their job shows time to present... we can assume they are in tht job... working while studying is true"
    # Even if overlap is small (e.g. school starts Aug, current time is Aug), Present implies continued intent.
    if is_job_present:
        return True

    # If Job is Finite, require >= 2 months overlap.
    # "Overlap duration" = end - start
    # e.g. May (5) to May (5) -> 5 - 5 = 0 months. (Requires same month start/end?) 
    # Actually, inclusive? 
    # If job is May 2025 - May 2025 (1 mo).
    # If school is May 2025 - May 2029.
    # Overlap is May. 1 month.
    # User said: "graduated May 2025... job start May 2025... false" -> This implies 1 month overlap is FALSE.
    # User said: "overlap for atleast 2 months".
    
    duration = overlap_end - overlap_start
    # If duration >= 2, we are good.
    # e.g. Jan to Mar = 3 - 1 = 2 (Jan, Feb, Mar? No, 3-1 is 2. Jan to March is usually 2 months or 3?)
    # date_to_comparable returns month index (1..12).
    # If overlap is [2025-05, 2025-05] (May to May) -> 5 - 5 = 0.
    # If overlap is [2025-05, 2025-06] (May to Jun) -> 6 - 5 = 1.
    # If overlap is [2025-05, 2025-07] (May to Jul) -> 7 - 5 = 2.
    
    # So strictly: duration >= 2 means effectively 3 distinct months? Or just 2 month gap?
    # User said: "atleast 2 months".
    # Keep it simple: diff >= 2.
    
    return duration >= 2