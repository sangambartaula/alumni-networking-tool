import re
from calendar import monthrange
from datetime import date
from typing import Dict, Optional, Tuple

try:
    from scraper.job_title_normalization import normalize_title_deterministic
except Exception:
    normalize_title_deterministic = None


_UNT_FULL_NAME_RE = re.compile(r"university\s+of\s+north\s+texas", re.IGNORECASE)
_UNT_TOKEN_RE = re.compile(r"\bunt\b", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
_MONTHS_RE = r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
_DATE_RANGE_RE = re.compile(
    rf"(?P<start>(?:{_MONTHS_RE}\.?\s+\d{{4}})|(?:\d{{4}}))\s*[-–—]\s*(?P<end>(?:Present)|(?:{_MONTHS_RE}\.?\s+\d{{4}})|(?:\d{{4}}))",
    re.IGNORECASE,
)
_MONTH_TO_NUM = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def is_unt_school_name(name: str) -> bool:
    if not name:
        return False
    return bool(_UNT_FULL_NAME_RE.search(name) or _UNT_TOKEN_RE.search(name))


def is_unt_employer(raw_company: str) -> bool:
    if not raw_company or not str(raw_company).strip():
        return False
    company = " ".join(str(raw_company).split())
    if _UNT_FULL_NAME_RE.search(company):
        return True
    if re.search(r"^\s*unt\s+", company, re.IGNORECASE):
        return True
    return bool(_UNT_TOKEN_RE.search(company))


def _parse_year(value) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit() and len(text) == 4:
        return int(text)
    match = _YEAR_RE.search(text)
    if match:
        return int(match.group(0))
    return None


def _parse_date_token(value, bound: str) -> Optional[Dict]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower().startswith("present"):
        return {"year": 9999, "month": 12, "is_present": True}

    month_match = re.match(rf"^({_MONTHS_RE})\.?\s+(\d{{4}})$", text, re.IGNORECASE)
    if month_match:
        month = _MONTH_TO_NUM.get(month_match.group(1).lower())
        year = int(month_match.group(2))
        if month:
            return {"year": year, "month": month, "is_present": False}

    year_match = re.match(r"^(\d{4})$", text)
    if year_match:
        year = int(year_match.group(1))
        month = 1 if bound == "start" else 12
        return {"year": year, "month": month, "is_present": False}

    parsed_year = _parse_year(text)
    if parsed_year is None:
        return None
    month = 1 if bound == "start" else 12
    return {"year": parsed_year, "month": month, "is_present": False}


def _parse_date_range_text(range_text) -> Tuple[Optional[Dict], Optional[Dict]]:
    if range_text is None:
        return None, None
    text = str(range_text).strip()
    if not text:
        return None, None
    match = _DATE_RANGE_RE.search(text)
    if not match:
        return None, None
    start_d = _parse_date_token(match.group("start"), "start")
    end_d = _parse_date_token(match.group("end"), "end")
    return start_d, end_d


def _job_start_end_dicts(start_text, end_text) -> Tuple[Optional[Dict], Optional[Dict]]:
    start_d = _parse_date_token(start_text, "start")
    if start_d is None:
        return None, None
    end_d = _parse_date_token(end_text, "end")
    return start_d, end_d


def _has_unt_education(row: Dict) -> bool:
    schools = [row.get("school"), row.get("school2"), row.get("school3")]
    return any(is_unt_school_name(s or "") for s in schools)


def _normalize_title(raw_title: str) -> str:
    if not raw_title or not str(raw_title).strip():
        return ""
    if normalize_title_deterministic is None:
        raw = str(raw_title).strip()
        raw_lower = raw.lower()
        if any(k in raw_lower for k in ("graduate assistant", "research assistant", "teaching assistant", "student assistant")):
            return "Graduate Assistant"
        return raw
    try:
        return normalize_title_deterministic(str(raw_title))
    except Exception:
        return str(raw_title).strip()


def _determine_work_study_status_local(
    school_end: Optional[Dict],
    job_start: Optional[Dict],
    job_end: Optional[Dict],
    is_expected: bool = False,
) -> str:
    today = date.today()

    def _safe_date(y: int, m: int, d: int) -> Optional[date]:
        try:
            return date(int(y), int(m), int(d))
        except Exception:
            return None

    def _effective_grad_date(end_info: Optional[Dict]) -> Tuple[Optional[date], bool]:
        if not end_info or is_expected:
            return None, True
        if end_info.get("is_present") or end_info.get("year") == 9999:
            return None, True
        year = end_info.get("year")
        if not year:
            return None, True
        month = end_info.get("month") or 5
        grad = _safe_date(year, month, 15)
        return grad, grad is None

    def _job_date(d: Optional[Dict], bound: str) -> Optional[date]:
        if d is None:
            return None if bound == "start" else today
        if d.get("is_present") or d.get("year") == 9999:
            return today
        year = d.get("year")
        if not year:
            return None
        month = d.get("month") or (1 if bound == "start" else 12)
        if bound == "start":
            day = 1
        else:
            try:
                day = monthrange(int(year), int(month))[1]
            except Exception:
                day = 28
        return _safe_date(year, month, day)

    grad_date, still_studying = _effective_grad_date(school_end)
    job_start_date = _job_date(job_start, "start")
    if job_start_date is None:
        return ""

    job_end_date = _job_date(job_end, "end")
    job_is_active = job_end is None or job_end.get("is_present") or job_end.get("year") == 9999

    if still_studying:
        if job_is_active:
            return "currently"
        if job_end_date and job_end_date > today:
            return "currently"
        return "yes"

    if grad_date and job_start_date < grad_date:
        return "yes"

    if grad_date and grad_date > today and job_is_active:
        return "currently"

    return "no"


def _has_unt_ga_experience(row: Dict) -> bool:
    experiences = [
        (row.get("current_job_title"), row.get("company")),
        (row.get("exp2_title"), row.get("exp2_company")),
        (row.get("exp3_title"), row.get("exp3_company")),
    ]
    for raw_title, raw_company in experiences:
        if _normalize_title(raw_title) != "Graduate Assistant":
            continue
        if is_unt_employer(raw_company):
            return True
    return False


def recompute_working_while_studying_status(row: Dict) -> str:
    """
    Recompute status using date-based logic first; only if non-computable apply
    strict UNT+Graduate Assistant fallback.
    """
    grad_year = _parse_year(row.get("grad_year"))
    # Backend row currently stores grad_year only; use May 15 fallback semantics.
    school_end = {"year": grad_year, "month": None, "is_present": False} if grad_year else None
    is_expected = False

    wws_priority = {"": 0, "no": 1, "yes": 2, "currently": 3}
    best_status = ""

    date_pairs = []
    date_pairs.append((row.get("job_start_date"), row.get("job_end_date")))
    for range_key in ("exp2_dates", "exp3_dates"):
        range_start, range_end = _parse_date_range_text(row.get(range_key))
        if range_start:
            date_pairs.append((range_start, range_end))

    for start_text, end_text in date_pairs:
        if isinstance(start_text, dict):
            start_d, end_d = start_text, end_text
        else:
            start_d, end_d = _job_start_end_dicts(start_text, end_text)
        if not start_d:
            continue
        status = _determine_work_study_status_local(
            school_end=school_end,
            job_start=start_d,
            job_end=end_d,
            is_expected=is_expected,
        )
        if wws_priority.get(status, 0) > wws_priority.get(best_status, 0):
            best_status = status
            if best_status == "currently":
                break

    if best_status != "":
        return best_status

    if _has_unt_education(row) and _has_unt_ga_experience(row):
        return "yes"

    return "no"


def status_to_bool(status: str) -> Optional[bool]:
    value = (status or "").strip().lower()
    if value in ("yes", "currently"):
        return True
    if value == "no":
        return False
    return None
