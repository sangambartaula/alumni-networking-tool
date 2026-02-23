import re
from datetime import datetime
from typing import Dict, Optional, Tuple

try:
    from scraper.job_title_normalization import normalize_title_deterministic
except Exception:
    normalize_title_deterministic = None


_UNT_FULL_NAME_RE = re.compile(r"university\s+of\s+north\s+texas", re.IGNORECASE)
_UNT_TOKEN_RE = re.compile(r"\bunt\b", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")


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


def _job_start_end_dicts(start_text, end_text) -> Tuple[Optional[Dict], Optional[Dict]]:
    start_year = _parse_year(start_text)
    if start_year is None:
        return None, None

    start_d = {"year": start_year, "is_present": False}
    end_raw = str(end_text).strip().lower() if end_text is not None else ""
    if end_raw.startswith("present"):
        end_d = {"year": 9999, "is_present": True}
    else:
        end_year = _parse_year(end_text)
        end_d = {"year": end_year, "is_present": False} if end_year else None
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
    current_year = datetime.now().year
    grad_year = None
    still_studying = False

    if school_end:
        if school_end.get("is_present") or school_end.get("year") == 9999:
            still_studying = True
        else:
            grad_year = school_end.get("year")

    if is_expected or grad_year is None:
        still_studying = True

    job_start_year = None
    if job_start and not job_start.get("is_present"):
        job_start_year = job_start.get("year")

    if job_start_year is None:
        return ""

    if still_studying:
        job_is_active = job_end is None or job_end.get("is_present") or job_end.get("year") == 9999
        if job_is_active:
            return "currently"
        job_end_year = job_end.get("year") if job_end else None
        if job_end_year and job_end_year > current_year:
            return "currently"
        return "yes"

    if job_start_year < grad_year:
        return "yes"

    if grad_year > current_year:
        job_is_active = job_end is None or job_end.get("is_present") or (job_end.get("year") or 0) > current_year
        if job_is_active:
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
    school_end = {"year": grad_year, "is_present": False} if grad_year else None
    is_expected = False

    wws_priority = {"": 0, "no": 1, "yes": 2, "currently": 3}
    best_status = ""

    date_pairs = [
        (row.get("job_start_date"), row.get("job_end_date")),
        (row.get("exp2_dates"), row.get("exp2_dates")),
        (row.get("exp3_dates"), row.get("exp3_dates")),
    ]

    for start_text, end_text in date_pairs:
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
