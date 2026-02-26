import re
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional


UNT_ALUMNI_STATUS_YES = "yes"
UNT_ALUMNI_STATUS_NO = "no"
UNT_ALUMNI_STATUS_UNKNOWN = "unknown"
UNT_ALUMNI_STATUS_VALUES = {
    UNT_ALUMNI_STATUS_YES,
    UNT_ALUMNI_STATUS_NO,
    UNT_ALUMNI_STATUS_UNKNOWN,
}

_UNT_FULL_NAME_RE = re.compile(r"university\s+of\s+north\s+texas", re.IGNORECASE)
_UNT_TOKEN_RE = re.compile(r"\bunt\b", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")


def is_unt_school_name(name: Any) -> bool:
    if not name:
        return False
    text = str(name).strip()
    if not text:
        return False
    return bool(_UNT_FULL_NAME_RE.search(text) or _UNT_TOKEN_RE.search(text))


def _extract_year(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if 1900 <= value <= 2100 else None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit() and len(text) == 4:
        year = int(text)
        return year if 1900 <= year <= 2100 else None
    match = _YEAR_RE.search(text)
    if not match:
        return None
    year = int(match.group(0))
    return year if 1900 <= year <= 2100 else None


def _parse_end_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    # Support basic ISO datetime values, e.g. 2026-05-14T00:00:00
    if "T" in text:
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError:
            return None

    return None


def _entry_status(entry: Dict[str, Any], today: date) -> str:
    end_date = _parse_end_date(entry.get("end_date"))
    if end_date is not None:
        return UNT_ALUMNI_STATUS_YES if end_date <= today else UNT_ALUMNI_STATUS_NO

    end_year = _extract_year(entry.get("end_year"))
    if end_year is None:
        return UNT_ALUMNI_STATUS_UNKNOWN

    if end_year < today.year:
        return UNT_ALUMNI_STATUS_YES
    if end_year > today.year:
        return UNT_ALUMNI_STATUS_NO

    # Year-only value equal to current year is ambiguous without month/day.
    # We intentionally treat this as unknown for consistency.
    return UNT_ALUMNI_STATUS_UNKNOWN


def compute_unt_alumni_status(unt_education_entries: Iterable[Dict[str, Any]], today: Optional[date] = None) -> str:
    """
    Compute UNT alumni status from UNT education entries only.

    Rule precedence:
      1) If any UNT entry has a future end date/year -> "no" (current student)
      2) Else if any UNT entry has a past end date/year -> "yes"
      3) Else -> "unknown"

    This satisfies mixed-entry edge cases and also aligns with "most recent end
    date/year" behavior when all dated entries are on one side of "today".
    """
    ref_today = today or date.today()
    entries = list(unt_education_entries or [])
    if not entries:
        return UNT_ALUMNI_STATUS_UNKNOWN

    statuses = [_entry_status(entry, ref_today) for entry in entries]
    if UNT_ALUMNI_STATUS_NO in statuses:
        return UNT_ALUMNI_STATUS_NO
    if UNT_ALUMNI_STATUS_YES in statuses:
        return UNT_ALUMNI_STATUS_YES
    return UNT_ALUMNI_STATUS_UNKNOWN


def build_unt_education_entries_from_alumni_row(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build normalized UNT education entries from alumni row fields.
    We only use stored UNT education slots (school/school2/school3) and available
    end-year/date-like hints tied to those entries.
    """
    entries: List[Dict[str, Any]] = []

    schools = [
        ("school", "degree", "major", "grad_year"),
        ("school2", "degree2", "major2", None),
        ("school3", "degree3", "major3", None),
    ]

    for school_key, degree_key, major_key, grad_year_key in schools:
        school = row.get(school_key)
        if not is_unt_school_name(school):
            continue

        explicit_end_year = _extract_year(row.get(grad_year_key)) if grad_year_key else None
        degree_year = _extract_year(row.get(degree_key))
        major_year = _extract_year(row.get(major_key))
        inferred_end_year = explicit_end_year or degree_year or major_year

        entries.append(
            {
                "school": school,
                "end_year": inferred_end_year,
                # Existing schema does not store per-education end date fields;
                # keep this key for forward compatibility.
                "end_date": None,
            }
        )

    return entries


def compute_unt_alumni_status_from_row(row: Dict[str, Any], today: Optional[date] = None) -> str:
    entries = build_unt_education_entries_from_alumni_row(row or {})
    return compute_unt_alumni_status(entries, today=today)

