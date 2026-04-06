import csv
import json
import os
import re
import time
from pathlib import Path

from dotenv import load_dotenv
try:
    from groq import Groq
except ImportError:  # pragma: no cover - optional dependency for local tests
    Groq = None

from scraper.degree_normalization import normalize_degree_deterministic


RAW_CSV_FILE = Path(os.getenv("EXA_RAW_CSV_FILE", "raw_alumni_data.csv"))
FINAL_CSV_FILE = Path(os.getenv("EXA_FINAL_CSV_FILE", "final_alumni_clean.csv"))
GROQ_MODEL = os.getenv("EXA_GROQ_MODEL", os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"))
GROQ_FALLBACK_MODELS = [
    model.strip()
    for model in os.getenv("EXA_GROQ_FALLBACK_MODELS", "").split(",")
    if model.strip()
]
GROQ_TIMEOUT_SECONDS = float(os.getenv("EXA_GROQ_TIMEOUT_SECONDS", "45"))
GROQ_MAX_RETRIES = int(os.getenv("EXA_GROQ_MAX_RETRIES", "3"))
LOG_EVERY_N_ROWS = max(1, int(os.getenv("EXA_GROQ_LOG_EVERY", "10")))

OUTPUT_COLUMNS = [
    "first",
    "last",
    "school_1",
    "degree_1",
    "major_1",
    "school_2",
    "degree_2",
    "major_2",
    "school_3",
    "degree_3",
    "major_3",
    "job_1_title",
    "job_1_company",
    "job_2_title",
    "job_2_company",
    "job_3_title",
    "job_3_company",
    "linkedin_url",
]

SYSTEM_PROMPT = """You are a strict alumni data normalizer.
Return ONLY compact JSON and no extra text.

The user payload is already pre-trimmed into high-signal evidence candidates.
Use ONLY that evidence. Do not invent schools, degrees, majors, jobs, or dates.

Schema:
{
  "first": string,
  "last": string,
  "edu_1": {"school": string, "degree": string, "major": string, "dates": string},
  "edu_2": {"school": string, "degree": string, "major": string, "dates": string},
  "edu_3": {"school": string, "degree": string, "major": string, "dates": string},
  "job_1": {"title": string, "company": string, "dates": string},
  "job_2": {"title": string, "company": string, "dates": string},
  "job_3": {"title": string, "company": string, "dates": string}
}

Rules:
- Extract only evidence-backed facts from the provided candidates.
- Populate edu_1..edu_3 and job_1..job_3 directly; do not return arrays.
- Merge duplicate evidence for the same education/job instead of repeating it.
- The highest degree from University of North Texas (Ph.D. > M.S. > B.S.) must be placed in edu_1.
- All other education records (UNT or non-UNT) should follow in recency/chronology order in edu_2 and edu_3.
- If the evidence says "Computer Science", "Mechanical Engineering", or "Biomedical Engineering", that is the MAJOR. Preserve it even if the degree is unknown.
- Jobs are already pre-ranked newest-first in the payload. Keep that order unless the candidate dates clearly prove otherwise.
- job_1 = current/most recent, job_3 = oldest of the returned three.
- Do NOT skip service/retail roles (e.g., Walmart, Cashier, Pharmacy Technician) if they are most recent.
- Return exactly 3 jobs as job_1..job_3 with title, company, dates.
- For education.major, keep only the primary major and strip GPA, honors, minors, activities, and other extras.
- Fix duplicated or overlapped names. Example: "John John" should become first="John", last="N/A" if no real last name exists.
- Never duplicate the same school+degree+major record across multiple education slots.
- Never duplicate the same title+company record across multiple job slots.
- Use "N/A" for unknown strings.
"""

_METADATA_PATTERNS = [
    re.compile(r"\bconnections\b.*\bfollowers\b", re.IGNORECASE),
    re.compile(r"Company:\s*[0-9,+\- ]+employees", re.IGNORECASE),
    re.compile(r"Founded\s*[0-9]{4}", re.IGNORECASE),
    re.compile(r"\b(?:Privately Held|Public Company|Nonprofit)\b", re.IGNORECASE),
    re.compile(r"Department:\s*.*", re.IGNORECASE),
    re.compile(r"Level:\s*.*", re.IGNORECASE),
    re.compile(r"^Total Experience:.*", re.IGNORECASE),
    re.compile(r"^Issued:\s*.*", re.IGNORECASE),
    re.compile(r"^View Certificate\b.*", re.IGNORECASE),
]

_LOCATION_TAIL_RE = re.compile(
    r"\s+[A-Za-z .'-]+,\s*[A-Za-z .'-]+(?:,\s*[A-Za-z .'-]+)?(?:\s*\([A-Za-z]{2,3}\))?$"
)
_DATE_LINE_RE = re.compile(
    r"(\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+\d{4}\b|\bPresent\b|\b\d{4}\s*-\s*(?:\d{4}|Present)\b)",
    re.IGNORECASE,
)
_DATE_RANGE_RE = re.compile(
    r"(?P<start>(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{4}|\d{4})\s*[-–—]\s*(?P<end>(?:Present|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{4}|\d{4}))",
    re.IGNORECASE,
)
_ADVISOR_RE = re.compile(r"advisor:\s*dr\.", re.IGNORECASE)
_UNT_LINE_RE = re.compile(r"(?:university\s+of\s+north\s+texas|\bunt\b)", re.IGNORECASE)
_EDU_CONTEXT_RE = re.compile(
    r"(?:university\s+of\s+north\s+texas|\bunt\b|\bbachelor\b|\bmaster\b|\bph\.?d\.?\b|\bb\.?s\.?\b|\bm\.?s\.?\b)",
    re.IGNORECASE,
)
_MAJOR_SIGNAL_RE = re.compile(
    r"\b(major(?:ing)?\s+in|pursuing a major in|field of study|computer science|data science|cybersecurity|artificial intelligence|"
    r"mechanical(?: and energy)? engineering|electrical engineering|biomedical engineering|construction management|"
    r"civil engineering|integrated circuit design)\b",
    re.IGNORECASE,
)
_PROFILE_EDU_SIGNAL_RE = re.compile(
    r"(?:university\s+of\s+north\s+texas|\bunt\b).{0,100}\b(?:major|bachelor|master|ph\.?d|student|graduate|engineering|science)\b|"
    r"\b(?:bachelor|master|ph\.?d|major(?:ing)?\s+in|pursuing a major in)\b",
    re.IGNORECASE,
)
_DEGREE_HINT_RE = re.compile(
    r"\b(?:ph\.?\s*d\.?|phd|doctor(?:ate| of philosophy)?|master(?:['’]s|s)?(?: student| degree| of [a-z& ]+)?|m\.?\s*s\.?|"
    r"m\.?\s*eng\.?|mba|bachelor(?:['’]s)?(?: student| degree| of [a-z& ]+)?|b\.?\s*s\.?|b\.?\s*e\.?|associate(?:['’]s)?(?: degree| of [a-z& ]+)?)\b",
    re.IGNORECASE,
)
_GENERIC_MAJOR_TOKENS = {
    "student",
    "graduate",
    "engineering",
    "science",
    "degree",
    "program",
    "coursework",
}
_NOISE_SECTION_TITLES = (
    "activity",
    "licenses",
    "skills",
    "courses",
    "languages",
    "honors",
)
_NOISE_LINE_PREFIXES = (
    "view certificate",
    "view publication",
    "github profile",
    "username:",
    "issued:",
    "activities:",
    "activities and societies:",
    "honors:",
    "coursework:",
)
_EMPLOYMENT_DECORATION_RE = re.compile(r"\((?:current|past)\)", re.IGNORECASE)
_DATE_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
_MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s{2,}", " ", str(text or "").strip())


def _normalize_source_text(text: str) -> str:
    raw = str(text or "")
    if not raw.strip():
        return ""

    raw = raw.replace("\r\n", "\n").replace("\r", "\n").replace("\u2022", " • ")
    raw = re.sub(r"(?<![#\n])(###\s+)", r"\n\1", raw)
    raw = re.sub(r"(?<![#\n])(##\s+)", r"\n\1", raw)
    for marker in ("Total Experience:", "Company:", "Department:", "Level:", "Issued:", "View Certificate"):
        raw = re.sub(rf"(?<!\n){re.escape(marker)}", f"\n{marker}", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    return raw


def _compact_line(line: str) -> str:
    working = _normalize_whitespace(line)
    if not working:
        return ""
    working = working.strip(" |")
    working = re.sub(r"\s*\.\.\.\s*", " ... ", working)
    working = _normalize_whitespace(working)
    return working


def _is_noise_line(line: str) -> bool:
    lowered = line.lower()
    if any(pattern.search(line) for pattern in _METADATA_PATTERNS):
        return True
    if _ADVISOR_RE.search(line):
        return True
    if any(lowered.startswith(prefix) for prefix in _NOISE_LINE_PREFIXES):
        return True
    return False


def split_profile_sections(text: str) -> dict[str, list[str]]:
    sections = {"header": [], "about": [], "education": [], "experience": [], "other": []}
    current_section = "header"

    for raw_line in _normalize_source_text(text).splitlines():
        line = _compact_line(raw_line)
        if not line or _is_noise_line(line):
            continue

        lowered = line.lower()
        if lowered.startswith("## "):
            title = line[3:].strip()
            title_lower = title.lower()
            remainder = ""
            if title_lower.startswith("about"):
                current_section = "about"
                remainder = _compact_line(title[len("about"):].strip(" :-|"))
            elif title_lower.startswith("education"):
                current_section = "education"
                remainder = _compact_line(title[len("education"):].strip(" :-|"))
            elif title_lower.startswith("experience"):
                current_section = "experience"
                remainder = _compact_line(title[len("experience"):].strip(" :-|"))
            elif any(title_lower.startswith(noise) for noise in _NOISE_SECTION_TITLES):
                current_section = "other"
            else:
                current_section = "other"

            if remainder and current_section in {"about", "education", "experience"} and not _is_noise_line(remainder):
                sections[current_section].append(remainder)
            continue

        if not line or _is_noise_line(line):
            continue

        sections.setdefault(current_section, []).append(line)

    return sections


def _split_sentences(text: str) -> list[str]:
    normalized = _normalize_whitespace(text)
    if not normalized:
        return []
    parts = re.split(r"(?<=[.!?])\s+|\s+\.\.\.\s+", normalized)
    return [_compact_line(part) for part in parts if _compact_line(part)]


def _extract_date_range_parts(text: str) -> tuple[str, str]:
    matches = list(_DATE_RANGE_RE.finditer(str(text or "")))
    if not matches:
        return ("N/A", "N/A")
    match = matches[-1]
    return (_compact_line(match.group("start")), _compact_line(match.group("end")))


def _render_date_range(start_date: str, end_date: str) -> str:
    start = safe_text(start_date)
    end = safe_text(end_date)
    if start == "N/A" and end == "N/A":
        return "N/A"
    if start == "N/A":
        return end
    if end == "N/A":
        return start
    return f"{start} - {end}"


def _strip_dates_from_text(text: str) -> str:
    cleaned = _DATE_RANGE_RE.sub("", str(text or ""))
    cleaned = cleaned.replace("•", " ")
    cleaned = _EMPLOYMENT_DECORATION_RE.sub("", cleaned)
    cleaned = re.sub(r"\bCurrent\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b\d+\s+years?(?:\s+and\s+\d+\s+months?)?\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b\d+\s+months?\b", "", cleaned, flags=re.IGNORECASE)
    return _compact_line(cleaned).strip(" -,|")


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    deduped = []
    seen = set()
    for value in values:
        normalized = value.casefold()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(value)
    return deduped


def _chunk_section_lines(lines: list[str]) -> list[list[str]]:
    chunks = []
    current = []
    for line in lines:
        if line.startswith("###"):
            if current:
                chunks.append(current)
            current = [line[3:].strip()]
            continue
        if current:
            current.append(line)
    if current:
        chunks.append(current)
    if not chunks and lines:
        chunks.append(lines[:])
    return chunks


def _canonical_school_name(value: str) -> str:
    school = _compact_line(value)
    if not school:
        return ""
    if is_unt_school(school):
        return "University of North Texas"
    school = re.sub(r"^[Tt]he\s+", "", school)
    school = re.sub(r"\s{2,}", " ", school).strip(" -,|")
    return school


def _extract_school_hint(text: str) -> str:
    content = _strip_dates_from_text(text)
    if not content:
        return "N/A"

    if is_unt_school(content):
        return "University of North Texas"

    for pattern in (
        r"\b(?:at|@)\s+([^|]+)$",
        r"\s-\s+(University of [A-Za-z0-9 '&.-]+)$",
    ):
        match = re.search(pattern, content, flags=re.IGNORECASE)
        if match:
            school = _canonical_school_name(match.group(1))
            return school or "N/A"

    return "N/A"


def _extract_degree_hint(text: str) -> str:
    content = _strip_dates_from_text(text)
    match = _DEGREE_HINT_RE.search(content)
    return _compact_line(match.group(0)) if match else "N/A"


def _clean_major_hint(value: str) -> str:
    cleaned = _compact_line(value)
    cleaned = re.sub(r"\b(?:minor|minors|activities and societies|activities|honors|gpa)\b.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^[,.;:/-]+|[,.;:/-]+$", "", cleaned).strip()
    cleaned = _normalize_whitespace(cleaned)
    return cleaned or "N/A"


def _remove_degree_tokens(text: str) -> str:
    cleaned = _DEGREE_HINT_RE.sub("", text)
    cleaned = re.sub(r"\b(?:student|degree|program|candidate)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^[,.\-:/ ]+|[,.\-:/ ]+$", "", cleaned)
    return _normalize_whitespace(cleaned)


def _strip_profile_name_prefix(text: str, raw_name: str) -> str:
    cleaned = _compact_line(text)
    profile_name = _compact_line((raw_name or "").split("|", 1)[0]).lstrip("# ").strip()
    if not profile_name:
        return cleaned
    for candidate in (profile_name, f"# {profile_name}"):
        if cleaned.lower().startswith(candidate.lower()):
            remainder = cleaned[len(candidate):].strip(" -|")
            return remainder or cleaned
    return cleaned


def _major_match_key(value: str) -> str:
    key = re.sub(r"[^a-z0-9]+", " ", clean_major_field(value).lower()).strip()
    aliases = {
        "cs": "computer science",
        "comp sci": "computer science",
    }
    return aliases.get(key, key)


def _extract_major_hint(text: str, degree_hint: str, school_hint: str) -> str:
    content = _strip_dates_from_text(text)

    direct_patterns = (
        r"\bpursuing a major in\s+([^.|]+)",
        r"\bmajor(?:ing)?\s+in\s+([^.|]+)",
        r"\bmajor:\s*([^.|]+)",
        r"\b(?:b\.?\s*s\.?|bachelor(?:['’]s)?(?: student| of [a-z& ]+)?|m\.?\s*s\.?|master(?:['’]s)?(?: student| of [a-z& ]+)?|ph\.?\s*d\.?)\s+in\s+([^.|]+)",
    )
    for pattern in direct_patterns:
        match = re.search(pattern, content, flags=re.IGNORECASE)
        if match:
            candidate = re.split(r"\s+(?:at|@)\s+|\s+-\s+University\b|\s+-\s+UNT\b", match.group(1), maxsplit=1)[0]
            return _clean_major_hint(candidate)

    before_school = re.split(r"\s+(?:at|@)\s+", content, maxsplit=1, flags=re.IGNORECASE)[0]
    if school_hint != "N/A":
        before_school = before_school.replace(school_hint, "")
    before_school = _remove_degree_tokens(before_school)

    segments = [
        _clean_major_hint(segment)
        for segment in re.split(r"\s*,\s*|\s+\|\s+", before_school)
        if _clean_major_hint(segment) != "N/A"
    ]
    for segment in segments:
        words = [token.casefold() for token in re.findall(r"[A-Za-z]+", segment)]
        if not words:
            continue
        if all(word in _GENERIC_MAJOR_TOKENS for word in words):
            continue
        return segment

    if _MAJOR_SIGNAL_RE.search(content):
        cleaned = _remove_degree_tokens(content)
        cleaned = re.split(r"\s+(?:at|@)\s+", cleaned, maxsplit=1, flags=re.IGNORECASE)[0]
        return _clean_major_hint(cleaned)

    return "N/A"


def _education_candidate_sort_key(candidate: dict) -> tuple[int, int, int, int, int]:
    end_ord = _date_to_ordinal(candidate.get("end_date"))
    start_ord = _date_to_ordinal(candidate.get("start_date"))
    return (
        1 if is_unt_school(candidate.get("school_hint")) else 0,
        int(candidate.get("score", 0)),
        _degree_rank(candidate.get("degree_hint")),
        end_ord[0] * 100 + end_ord[1],
        start_ord[0] * 100 + start_ord[1],
    )


def _job_candidate_sort_key(candidate: dict) -> tuple[int, int, int, int]:
    return _job_recency_key(
        {
            "start_date": candidate.get("start_date"),
            "end_date": candidate.get("end_date"),
        }
    )


def _education_dedupe_key(candidate: dict) -> tuple[str, str, str, str, str]:
    school = _school_dedupe_key(candidate.get("school_hint"))
    degree = degree_to_short_form(candidate.get("degree_hint"))
    major = clean_major_field(candidate.get("major_hint"))
    start = safe_text(candidate.get("start_date"))
    end = safe_text(candidate.get("end_date"))
    if degree == "N/A" and major == "N/A" and start == "N/A" and end == "N/A":
        return (school, "N/A", "N/A", "N/A", "N/A")
    return (school, degree, major, start, end)


def _job_dedupe_key(candidate: dict) -> tuple[str, str, str, str]:
    title = re.sub(r"[^a-z0-9]+", " ", safe_text(candidate.get("title_hint")).lower()).strip()
    company = re.sub(r"[^a-z0-9]+", " ", safe_text(candidate.get("company_hint")).lower()).strip()
    start = safe_text(candidate.get("start_date"))
    end = safe_text(candidate.get("end_date"))
    return (title or "n/a", company or "n/a", start, end)


def _make_education_candidate(source: str, text: str, supporting_lines: list[str] | None = None) -> dict | None:
    evidence_parts = [text] + list(supporting_lines or [])
    evidence_parts = [part for part in evidence_parts if _compact_line(part)]
    if not evidence_parts:
        return None

    evidence = " | ".join(_dedupe_preserve_order([_compact_line(part) for part in evidence_parts]))[:360]
    start_date, end_date = _extract_date_range_parts(evidence)
    school_hint = _extract_school_hint(evidence)
    degree_hint = _extract_degree_hint(evidence)
    major_hint = _extract_major_hint(evidence, degree_hint, school_hint)

    if degree_hint == "N/A" and re.search(r"\bcertificate\b", evidence, flags=re.IGNORECASE):
        return None

    score = 0
    if school_hint != "N/A":
        score += 3
    if degree_hint != "N/A":
        score += 2
    if major_hint != "N/A":
        score += 2
    if end_date != "N/A" or start_date != "N/A":
        score += 1
    if source in {"headline", "about"}:
        score += 1

    return {
        "source": source,
        "school_hint": school_hint,
        "degree_hint": degree_hint,
        "major_hint": major_hint,
        "start_date": start_date,
        "end_date": end_date,
        "dates": _render_date_range(start_date, end_date),
        "evidence": evidence,
        "score": score,
    }


def build_education_candidates(name: str, highlight_text: str) -> list[dict]:
    sections = split_profile_sections(highlight_text)
    candidates = []

    header_lines = [
        line
        for line in sections.get("header", [])[:5]
        if not re.search(r"\bconnections\b|\bfollowers\b", line, flags=re.IGNORECASE)
    ]
    for line in header_lines:
        candidate_line = _strip_profile_name_prefix(line, name)
        if _PROFILE_EDU_SIGNAL_RE.search(candidate_line):
            candidate = _make_education_candidate("headline", candidate_line)
            if candidate:
                candidates.append(candidate)

    about_sentences = _split_sentences(" ".join(sections.get("about", [])))
    for sentence in about_sentences:
        if not _PROFILE_EDU_SIGNAL_RE.search(sentence):
            continue
        candidate = _make_education_candidate("about", sentence)
        if candidate:
            candidates.append(candidate)

    for chunk in _chunk_section_lines(sections.get("education", [])):
        heading = _compact_line(chunk[0]) if chunk else ""
        if not heading:
            continue
        support = [
            _compact_line(line)
            for line in chunk[1:4]
            if _compact_line(line)
            and not any(prefix in line.lower() for prefix in ("activities", "coursework", "honors"))
            and (_DATE_LINE_RE.search(line) or _EDU_CONTEXT_RE.search(line) or " at " in line.lower())
        ]
        candidate = _make_education_candidate("education", heading, support)
        if candidate:
            candidates.append(candidate)

    merged = []
    seen = {}
    for candidate in candidates:
        key = _education_dedupe_key(candidate)
        existing = seen.get(key)
        if existing is None:
            seen[key] = candidate
            merged.append(candidate)
            continue
        if len(candidate.get("evidence", "")) > len(existing.get("evidence", "")):
            existing["evidence"] = candidate["evidence"]
        if candidate.get("score", 0) > existing.get("score", 0):
            existing["score"] = candidate["score"]

    merged.sort(key=_education_candidate_sort_key, reverse=True)
    return merged[:6]


def _extract_job_title_company(text: str) -> tuple[str, str]:
    content = _strip_dates_from_text(text)
    content = _EMPLOYMENT_DECORATION_RE.sub("", content).strip()
    content = re.split(r"\s+\.\.\.\s+", content, maxsplit=1)[0].strip()

    for separator in (" at ", " @ "):
        if separator in content.lower():
            pattern = re.compile(re.escape(separator), re.IGNORECASE)
            parts = pattern.split(content, maxsplit=1)
            if len(parts) == 2:
                title = _compact_line(parts[0]).strip(" -,|")
                company = _compact_line(parts[1]).strip(" -,|")
                if title and company:
                    return (title, company)

    return ("N/A", "N/A")


def _make_experience_candidate(chunk: list[str]) -> dict | None:
    if not chunk:
        return None

    heading = _compact_line(chunk[0])
    if not heading:
        return None

    support = []
    for line in chunk[1:4]:
        compact = _compact_line(line)
        if not compact or _is_noise_line(compact):
            continue
        if compact.lower().startswith(("company:", "department:", "level:")):
            continue
        support.append(compact)

    evidence = " | ".join(_dedupe_preserve_order([heading] + support))[:360]
    title_hint, company_hint = _extract_job_title_company(heading)
    start_date, end_date = _extract_date_range_parts(evidence)

    score = 0
    if title_hint != "N/A":
        score += 2
    if company_hint != "N/A":
        score += 2
    if end_date != "N/A" or start_date != "N/A":
        score += 2

    return {
        "title_hint": title_hint,
        "company_hint": company_hint,
        "start_date": start_date,
        "end_date": end_date,
        "dates": _render_date_range(start_date, end_date),
        "evidence": evidence,
        "score": score,
    }


def build_experience_candidates(highlight_text: str) -> list[dict]:
    sections = split_profile_sections(highlight_text)
    candidates = []

    for chunk in _chunk_section_lines(sections.get("experience", [])):
        candidate = _make_experience_candidate(chunk)
        if not candidate:
            continue
        candidates.append(candidate)

    deduped = []
    seen = set()
    for candidate in sorted(candidates, key=_job_candidate_sort_key, reverse=True):
        key = _job_dedupe_key(candidate)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)

    for idx, candidate in enumerate(deduped, start=1):
        candidate["rank_hint"] = idx

    return deduped[:6]


def build_groq_payload(name: str, highlight_text: str) -> dict:
    sections = split_profile_sections(highlight_text)
    header_lines = [
        line
        for line in sections.get("header", [])[:4]
        if not re.search(r"\bconnections\b|\bfollowers\b", line, flags=re.IGNORECASE)
    ]
    about_signals = [
        sentence
        for sentence in _split_sentences(" ".join(sections.get("about", [])))
        if _PROFILE_EDU_SIGNAL_RE.search(sentence)
    ][:2]

    payload = {
        "profile_name": _compact_line(name),
        "profile_evidence": _dedupe_preserve_order(header_lines + about_signals)[:6],
        "education_candidates": build_education_candidates(name, highlight_text),
        "experience_candidates": build_experience_candidates(highlight_text),
    }
    return payload


def pre_clean_text(text: str) -> str:
    """Debug-friendly renderer for the compact Groq payload."""
    payload = build_groq_payload("", text)
    return json.dumps(payload, ensure_ascii=True, indent=2)


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def clean_doubled(text: str) -> str:
    if not text:
        return ""
    text = text.strip()
    if len(text) < 4:
        return text

    if len(text) % 2 == 0:
        half = len(text) // 2
        if text[:half] == text[half:]:
            return text[:half]

    parts = text.split()
    if len(parts) >= 2 and len(parts) % 2 == 0:
        half = len(parts) // 2
        if parts[:half] == parts[half:]:
            return " ".join(parts[:half])

    return text


def safe_text(value) -> str:
    text = str(value).strip() if value is not None else ""
    return text if text else "N/A"


def parse_json_payload(content: str) -> dict:
    try:
        parsed = json.loads(content)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if not match:
            return {}
        try:
            parsed = json.loads(match.group(0))
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}


def degree_to_short_form(raw_degree: str) -> str:
    normalized = normalize_degree_deterministic(raw_degree or "")
    lowered = normalized.lower()

    if "philosophy" in lowered or "ph.d" in lowered or lowered.startswith("doctor"):
        return "Ph.D."
    if "master" in lowered or lowered.startswith("m.") or lowered == "ms":
        return "M.S."
    if "bachelor" in lowered or lowered.startswith("b.") or lowered == "bs":
        return "B.S."
    if "associate" in lowered:
        return "A.S."

    return safe_text(raw_degree)


def strict_degree_short_form(raw_degree: str) -> str:
    short = degree_to_short_form(raw_degree)
    if short in {"B.S.", "M.S.", "Ph.D."}:
        return short
    return "N/A"


def clean_major_field(raw_major: str) -> str:
    major = safe_text(raw_major)
    if major == "N/A":
        return major

    cleaned = major
    cleaned = re.sub(r"\bGPA\b.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bHonors?\b.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bDean'?s\s+List\b.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bActivities?\b.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bMinor\b.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bMinors\b.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" ,-;|")

    # Keep only the first major-like segment if extra comma/pipe details remain.
    cleaned = re.split(r"\s*[|,;]\s*", cleaned)[0].strip()
    return cleaned if cleaned else "N/A"


def _degree_rank(raw_degree: str) -> int:
    lowered = safe_text(raw_degree).lower()
    if "ph.d" in lowered or "phd" in lowered or "doctor" in lowered:
        return 3
    if "m.s" in lowered or lowered == "ms" or "master" in lowered:
        return 2
    if "b.s" in lowered or lowered == "bs" or "bachelor" in lowered:
        return 1
    return 0


def _education_chrono_key(entry: dict) -> tuple[int, int]:
    # Best-effort ordering using any year token present in entry values.
    blob = " ".join(
        [
            str(entry.get("end") or ""),
            str(entry.get("end_date") or ""),
            str(entry.get("year") or ""),
            str(entry.get("start") or ""),
            str(entry.get("start_date") or ""),
        ]
    )
    years = [int(y) for y in _DATE_YEAR_RE.findall(blob)]
    if not years:
        return (0, 0)
    return (max(years), min(years))


def _school_dedupe_key(raw_school: str) -> str:
    school = safe_text(raw_school).lower()
    if school == "n/a":
        return "n/a"
    if is_unt_school(school):
        return "university of north texas"
    normalized = re.sub(r"[^a-z0-9]+", " ", school)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or school


def _education_entry_dedupe_key(entry: dict) -> tuple[str, str, str, str, str]:
    school_key = _school_dedupe_key(entry.get("school"))
    degree = degree_to_short_form(entry.get("degree"))
    major = clean_major_field(entry.get("major"))
    start = safe_text(entry.get("start") or entry.get("start_date"))
    end = safe_text(entry.get("end") or entry.get("end_date"))
    if degree == "N/A" and major == "N/A" and start == "N/A" and end == "N/A":
        return (school_key, "N/A", "N/A", "N/A", "N/A")
    return (school_key, degree, major, start, end)


def _has_real_education_value(entry: dict) -> bool:
    if not isinstance(entry, dict):
        return False
    return any(
        safe_text(entry.get(field)) != "N/A"
        for field in ("school", "degree", "major", "start", "end", "start_date", "end_date")
    )


def education_records_from_candidates(candidates: list[dict]) -> list[dict]:
    records = []
    for candidate in candidates or []:
        if not isinstance(candidate, dict):
            continue
        if safe_text(candidate.get("school_hint")) == "N/A":
            continue
        records.append(
            {
                "school": safe_text(candidate.get("school_hint")),
                "degree": safe_text(candidate.get("degree_hint")),
                "major": clean_major_field(candidate.get("major_hint")),
                "start": safe_text(candidate.get("start_date")),
                "end": safe_text(candidate.get("end_date")),
            }
        )
    return records


def _education_entries_match(existing: dict, fallback: dict) -> bool:
    school_existing = _school_dedupe_key(existing.get("school"))
    school_fallback = _school_dedupe_key(fallback.get("school"))
    if school_existing == "n/a" or school_fallback == "n/a" or school_existing != school_fallback:
        return False

    degree_existing = _degree_rank(existing.get("degree"))
    degree_fallback = _degree_rank(fallback.get("degree"))
    major_existing = _major_match_key(existing.get("major"))
    major_fallback = _major_match_key(fallback.get("major"))
    end_existing = safe_text(existing.get("end") or existing.get("end_date"))
    end_fallback = safe_text(fallback.get("end") or fallback.get("end_date"))

    if degree_existing and degree_existing == degree_fallback:
        return True
    if major_existing and major_existing == major_fallback and (degree_existing == degree_fallback or degree_existing == 0 or degree_fallback == 0):
        return True
    if end_existing != "N/A" and end_existing == end_fallback and (degree_existing == degree_fallback or degree_existing == 0 or degree_fallback == 0):
        return True
    if degree_existing == 0 and not major_existing:
        return True
    return False


def merge_education_payloads(primary: list[dict], fallback: list[dict]) -> list[dict]:
    merged = []
    for entry in list(primary or []):
        if not isinstance(entry, dict):
            continue
        merged.append(
            {
                "school": safe_text(entry.get("school")),
                "degree": safe_text(entry.get("degree") or entry.get("degree_raw") or entry.get("raw_degree")),
                "major": clean_major_field(entry.get("major") or entry.get("major_raw")),
                "start": safe_text(entry.get("start") or entry.get("start_date")),
                "end": safe_text(entry.get("end") or entry.get("end_date")),
            }
        )

    for fallback_entry in list(fallback or []):
        if not isinstance(fallback_entry, dict) or not _has_real_education_value(fallback_entry):
            continue

        match = next((entry for entry in merged if _education_entries_match(entry, fallback_entry)), None)
        if match is None:
            merged.append(
                {
                    "school": safe_text(fallback_entry.get("school")),
                    "degree": safe_text(fallback_entry.get("degree") or fallback_entry.get("degree_raw") or fallback_entry.get("raw_degree")),
                    "major": clean_major_field(fallback_entry.get("major") or fallback_entry.get("major_raw")),
                    "start": safe_text(fallback_entry.get("start") or fallback_entry.get("start_date")),
                    "end": safe_text(fallback_entry.get("end") or fallback_entry.get("end_date")),
                }
            )
            continue

        for field in ("school", "degree", "major", "start", "end"):
            fallback_value = fallback_entry.get(field)
            if field == "degree":
                fallback_value = fallback_entry.get("degree") or fallback_entry.get("degree_raw") or fallback_entry.get("raw_degree")
            elif field == "major":
                fallback_value = fallback_entry.get("major") or fallback_entry.get("major_raw")
            elif field == "start":
                fallback_value = fallback_entry.get("start") or fallback_entry.get("start_date")
            elif field == "end":
                fallback_value = fallback_entry.get("end") or fallback_entry.get("end_date")

            if safe_text(match.get(field)) == "N/A" and safe_text(fallback_value) != "N/A":
                match[field] = safe_text(fallback_value)

    return merged


def normalize_education(raw_education) -> list[dict]:
    source = raw_education if isinstance(raw_education, list) else []
    cleaned = []
    for item in source:
        if not isinstance(item, dict):
            continue
        start_hint, end_hint = _extract_date_range_parts(item.get("dates") or "")
        school = safe_text(clean_doubled(item.get("school")))
        if is_unt_school(school):
            school = "University of North Texas"
        degree = degree_to_short_form(clean_doubled(item.get("degree") or item.get("degree_raw") or item.get("raw_degree")))
        major = clean_major_field(clean_doubled(item.get("major") or item.get("major_raw")))
        cleaned.append(
            {
                "school": school,
                "degree": degree,
                "major": major,
                "start": safe_text(item.get("start") or item.get("start_date") or start_hint),
                "end": safe_text(item.get("end") or item.get("end_date") or item.get("year") or end_hint),
            }
        )

    # Backward compatibility: if older payload returns single unt_education object.
    if not cleaned and isinstance(raw_education, dict):
        school = safe_text(clean_doubled(raw_education.get("school")))
        if is_unt_school(school):
            school = "University of North Texas"
        degree = degree_to_short_form(clean_doubled(raw_education.get("degree") or raw_education.get("degree_raw") or raw_education.get("raw_degree")))
        major = clean_major_field(clean_doubled(raw_education.get("major") or raw_education.get("major_raw")))
        cleaned = [{"school": school, "degree": degree, "major": major, "start": "N/A", "end": "N/A"}]

    unt_entries = [entry for entry in cleaned if is_unt_school(entry.get("school"))]
    if unt_entries:
        unt_entries_sorted = sorted(
            unt_entries,
            key=lambda e: (_degree_rank(e.get("degree")), _education_chrono_key(e)),
            reverse=True,
        )
        unt_best = unt_entries_sorted[0]
        remaining = [entry for entry in cleaned if entry is not unt_best]
        remaining.sort(key=_education_chrono_key, reverse=True)
        ordered = [unt_best] + remaining
    else:
        ordered = cleaned

    # Dedupe exact repeats while preserving distinct degrees at the same school.
    deduped = []
    seen_keys = set()
    for entry in ordered:
        dedupe_key = _education_entry_dedupe_key(entry)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        deduped.append(entry)

    ordered = deduped

    trimmed = ordered[:3]
    while len(trimmed) < 3:
        trimmed.append({"school": "N/A", "degree": "N/A", "major": "N/A", "start": "N/A", "end": "N/A"})
    return trimmed


def education_payload_from_structured(parsed: dict) -> list[dict]:
    payload = []
    for idx in range(1, 4):
        entry = parsed.get(f"edu_{idx}")
        if not isinstance(entry, dict):
            payload.append({"school": "N/A", "degree": "N/A", "major": "N/A", "dates": "N/A"})
            continue
        start_date, end_date = _extract_date_range_parts(entry.get("dates") or "")
        payload.append(
            {
                "school": entry.get("school"),
                "degree": entry.get("degree"),
                "major": entry.get("major"),
                "start_date": start_date,
                "end_date": end_date,
                "dates": entry.get("dates"),
            }
        )
    return payload


def fix_name_overlap(first: str, last: str, raw_name: str) -> tuple[str, str]:
    first_clean = clean_doubled(safe_text(first))
    last_clean = clean_doubled(safe_text(last))

    if first_clean == "N/A" and last_clean == "N/A":
        tokens = [t for t in clean_doubled(raw_name).split() if t]
        if len(tokens) >= 2:
            return tokens[0], " ".join(tokens[1:])
        if len(tokens) == 1:
            return tokens[0], "N/A"
        return "N/A", "N/A"

    # Only treat as a doubling bug when first and last are exact duplicates.
    if first_clean != "N/A" and last_clean != "N/A" and first_clean.lower() == last_clean.lower():
        tokens = [t for t in clean_doubled(raw_name).split() if t]
        if len(tokens) >= 2:
            return tokens[0], " ".join(tokens[1:])
        return first_clean, "N/A"

    return first_clean, last_clean


def _date_to_ordinal(date_text: str) -> tuple[int, int]:
    text = str(date_text or "").strip()
    if not text or text == "N/A":
        return (0, 0)

    range_start, range_end = _extract_date_range_parts(text)
    if range_end != "N/A":
        text = range_end

    if re.search(r"\bpresent\b", text, flags=re.IGNORECASE):
        return (9999, 12)

    month_match = re.search(
        r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b\s+(19\d{2}|20\d{2})\b",
        text,
        flags=re.IGNORECASE,
    )
    if month_match:
        month_key = month_match.group(1).lower()
        year = int(month_match.group(2))
        month = _MONTH_MAP.get(month_key, 0)
        return (year, month)

    years = _DATE_YEAR_RE.findall(text)
    if years:
        return (int(years[-1]), 0)

    return (0, 0)


def _job_recency_key(job: dict) -> tuple[int, int, int, int]:
    end_ord = _date_to_ordinal(job.get("end_date"))
    start_ord = _date_to_ordinal(job.get("start_date"))
    is_present = 1 if end_ord[0] == 9999 else 0
    return (is_present, end_ord[0], end_ord[1], start_ord[0] * 100 + start_ord[1])


def job_records_from_candidates(candidates: list[dict]) -> list[dict]:
    records = []
    for candidate in candidates or []:
        if not isinstance(candidate, dict):
            continue
        records.append(
            {
                "title": safe_text(candidate.get("title_hint")),
                "company": safe_text(candidate.get("company_hint")),
                "start_date": safe_text(candidate.get("start_date")),
                "end_date": safe_text(candidate.get("end_date")),
            }
        )
    return records


def _normalize_job_key_fragment(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", safe_text(value).lower()).strip()


def _job_entries_match(existing: dict, fallback: dict) -> bool:
    title_existing = _normalize_job_key_fragment(existing.get("title") or existing.get("job_title"))
    title_fallback = _normalize_job_key_fragment(fallback.get("title") or fallback.get("job_title"))
    company_existing = _normalize_job_key_fragment(existing.get("company"))
    company_fallback = _normalize_job_key_fragment(fallback.get("company"))
    start_existing = safe_text(existing.get("start_date"))
    start_fallback = safe_text(fallback.get("start_date"))
    end_existing = safe_text(existing.get("end_date"))
    end_fallback = safe_text(fallback.get("end_date"))

    if title_existing and company_existing and title_existing == title_fallback and company_existing == company_fallback:
        return True
    if title_existing and title_existing == title_fallback and end_existing != "N/A" and end_existing == end_fallback:
        return True
    if company_existing and company_existing == company_fallback and start_existing == start_fallback and end_existing == end_fallback:
        return True
    return False


def merge_job_payloads(primary: list[dict], fallback: list[dict]) -> list[dict]:
    merged = []
    for entry in list(primary or []):
        if not isinstance(entry, dict):
            continue
        merged.append(
            {
                "title": safe_text(entry.get("title") or entry.get("job_title")),
                "company": safe_text(entry.get("company")),
                "start_date": safe_text(entry.get("start_date")),
                "end_date": safe_text(entry.get("end_date")),
            }
        )

    for fallback_entry in list(fallback or []):
        if not isinstance(fallback_entry, dict):
            continue
        if safe_text(fallback_entry.get("title") or fallback_entry.get("job_title")) == "N/A" and safe_text(fallback_entry.get("company")) == "N/A":
            continue

        match = next((entry for entry in merged if _job_entries_match(entry, fallback_entry)), None)
        if match is None:
            merged.append(
                {
                    "title": safe_text(fallback_entry.get("title") or fallback_entry.get("job_title")),
                    "company": safe_text(fallback_entry.get("company")),
                    "start_date": safe_text(fallback_entry.get("start_date")),
                    "end_date": safe_text(fallback_entry.get("end_date")),
                }
            )
            continue

        for field in ("title", "company", "start_date", "end_date"):
            if safe_text(match.get(field)) == "N/A" and safe_text(fallback_entry.get(field)) != "N/A":
                match[field] = safe_text(fallback_entry.get(field))

    return merged


def call_groq_extract(client, name: str, highlight_text: str) -> tuple[dict, dict]:
    payload = build_groq_payload(name, highlight_text)
    has_signal = any(payload.get(key) for key in ("profile_evidence", "education_candidates", "experience_candidates"))
    if not has_signal:
        return {}, payload

    user_prompt = (
        "Raw profile name:\n"
        f"{name}\n\n"
        "Structured evidence payload (JSON):\n"
        f"{json.dumps(payload, ensure_ascii=True)}\n"
    )

    model_candidates = [GROQ_MODEL] + [m for m in GROQ_FALLBACK_MODELS if m and m != GROQ_MODEL]

    for attempt in range(GROQ_MAX_RETRIES):
        try:
            response = None
            last_model_error = None
            for model_name in model_candidates:
                try:
                    response = client.chat.completions.create(
                        model=model_name,
                        temperature=0,
                        max_tokens=700,
                        response_format={"type": "json_object"},
                        timeout=GROQ_TIMEOUT_SECONDS,
                        messages=[
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": user_prompt},
                        ],
                    )
                    if model_name != GROQ_MODEL:
                        print(
                            f"  Using fallback model '{model_name}' for: {name[:60]}",
                            flush=True,
                        )
                    break
                except Exception as model_err:
                    err_text = str(model_err)
                    if "model_not_found" in err_text or "does not exist" in err_text:
                        last_model_error = model_err
                        continue
                    raise

            if response is None and last_model_error is not None:
                raise last_model_error

            content = (response.choices[0].message.content or "").strip()
            parsed = parse_json_payload(content)
            if parsed:
                return parsed, payload
            print(
                f"  Groq parse retry {attempt + 1}/{GROQ_MAX_RETRIES} for: {name[:60]}",
                flush=True,
            )
        except Exception as err:
            print(
                f"  Groq error {attempt + 1}/{GROQ_MAX_RETRIES} for '{name[:60]}': {err}",
                flush=True,
            )

        time.sleep(1.5 * (attempt + 1))

    return {}, payload


def load_existing_urls(final_csv: Path) -> set[str]:
    if not final_csv.exists() or final_csv.stat().st_size == 0:
        return set()
    seen = set()
    with final_csv.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            url = (row.get("linkedin_url") or "").strip()
            if url:
                seen.add(url)
    return seen


def ensure_output_writer(path: Path) -> tuple[csv.writer, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists() and path.stat().st_size > 0

    if file_exists:
        with path.open("r", newline="", encoding="utf-8") as check_handle:
            reader = csv.reader(check_handle)
            existing_header = next(reader, [])
        if existing_header != OUTPUT_COLUMNS:
            backup_path = path.with_suffix(path.suffix + f".bak_{int(time.time())}")
            path.rename(backup_path)
            print(
                f"Detected legacy CSV header. Backed up old file to: {backup_path.name}",
                flush=True,
            )
            file_exists = False

    handle = path.open("a", newline="", encoding="utf-8")
    writer = csv.writer(handle)
    if not file_exists:
        writer.writerow(OUTPUT_COLUMNS)
        handle.flush()
    return writer, handle


def normalize_jobs(raw_jobs) -> list[dict]:
    jobs = raw_jobs if isinstance(raw_jobs, list) else []
    clean_jobs = []
    for item in jobs:
        if not isinstance(item, dict):
            continue
        range_start, range_end = _extract_date_range_parts(item.get("dates") or item.get("date_range") or "")
        start_date = safe_text(item.get("start_date") or item.get("start") or range_start)
        end_date = safe_text(item.get("end_date") or item.get("end") or range_end)
        clean_jobs.append(
            {
                "title": safe_text(clean_doubled(item.get("title") or item.get("job_title"))),
                "company": safe_text(clean_doubled(item.get("company"))),
                "start_date": start_date,
                "end_date": end_date,
            }
        )

    # Law of recency: Job 1 = most recent/current; Job 3 = oldest.
    clean_jobs.sort(key=_job_recency_key, reverse=True)

    # Keep only distinct title+company pairs so repeated roles don't fill slots 2/3.
    deduped_jobs = []
    seen_keys = set()
    seen_pairs = {}
    for job in clean_jobs:
        exact_key = (
            _normalize_job_key_fragment(job.get("title")),
            _normalize_job_key_fragment(job.get("company")),
            safe_text(job.get("start_date")),
            safe_text(job.get("end_date")),
        )
        if exact_key in seen_keys:
            continue
        seen_keys.add(exact_key)

        pair_key = exact_key[:2]
        previous_pair = seen_pairs.get(pair_key)
        if previous_pair and all(value == "N/A" for value in exact_key[2:]):
            continue
        if previous_pair == exact_key[2:]:
            continue

        seen_pairs[pair_key] = exact_key[2:]
        deduped_jobs.append(job)

    clean_jobs = deduped_jobs

    while len(clean_jobs) < 3:
        clean_jobs.append({"title": "N/A", "company": "N/A", "start_date": "N/A", "end_date": "N/A"})

    return clean_jobs[:3]


def jobs_payload_from_structured(parsed: dict) -> list[dict]:
    payload = []
    for idx in range(1, 4):
        entry = parsed.get(f"job_{idx}")
        if not isinstance(entry, dict):
            payload.append({"title": "N/A", "company": "N/A", "dates": "N/A"})
            continue
        start_date, end_date = _extract_date_range_parts(entry.get("dates") or "")
        payload.append(
            {
                "title": entry.get("title"),
                "company": entry.get("company"),
                "start_date": start_date,
                "end_date": end_date,
                "dates": entry.get("dates"),
            }
        )
    return payload


def is_unt_school(school: str) -> bool:
    text = (school or "").strip().lower()
    return "university of north texas" in text or re.search(r"\bunt\b", text) is not None


def main() -> None:
    load_dotenv()
    require_env("GROQ_API_KEY")
    if Groq is None:
        raise ImportError("groq package is required to run exa_groq.py")

    if not RAW_CSV_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {RAW_CSV_FILE}")

    client = Groq(api_key=os.getenv("GROQ_API_KEY", "").strip())

    existing_urls = load_existing_urls(FINAL_CSV_FILE)
    writer, out_handle = ensure_output_writer(FINAL_CSV_FILE)

    processed = 0
    kept = 0
    skipped_non_unt = 0
    skipped_empty_text = 0
    skipped_empty_parse = 0

    input_rows = 0
    with RAW_CSV_FILE.open("r", newline="", encoding="utf-8") as input_handle:
        input_rows = max(0, sum(1 for _ in input_handle) - 1)

    print(
        f"Starting refine stage for {input_rows} rows. "
        f"model={GROQ_MODEL} timeout={GROQ_TIMEOUT_SECONDS}s retries={GROQ_MAX_RETRIES}.",
        flush=True,
    )

    try:
        with RAW_CSV_FILE.open("r", newline="", encoding="utf-8") as input_handle:
            reader = csv.DictReader(input_handle)
            for row in reader:
                processed += 1

                if processed % LOG_EVERY_N_ROWS == 0:
                    print(
                        f"Progress: {processed}/{input_rows} processed | "
                        f"kept={kept} non_unt={skipped_non_unt} "
                        f"empty_text={skipped_empty_text} empty_parse={skipped_empty_parse}",
                        flush=True,
                    )

                linkedin_url = (row.get("url") or "").strip()
                if not linkedin_url or linkedin_url in existing_urls:
                    continue

                name = (row.get("name") or "").strip()
                highlight_text = (row.get("highlight_text") or "").strip()
                if not highlight_text:
                    skipped_empty_text += 1
                    continue

                parsed, payload = call_groq_extract(client, name, highlight_text)
                if not parsed and not payload.get("education_candidates") and not payload.get("experience_candidates"):
                    skipped_empty_parse += 1
                    continue

                education_payload = education_payload_from_structured(parsed)
                if not any((safe_text(e.get("school")) != "N/A" for e in education_payload)):
                    education_payload = parsed.get("education")
                    if not isinstance(education_payload, list):
                        education_payload = parsed.get("unt_education")
                education_payload = merge_education_payloads(
                    education_payload if isinstance(education_payload, list) else [],
                    education_records_from_candidates(payload.get("education_candidates")),
                )
                education = normalize_education(education_payload)

                # Iron gate: school_1 must be UNT.
                school_1 = safe_text(education[0].get("school"))
                if not is_unt_school(school_1):
                    skipped_non_unt += 1
                    continue

                first = parsed.get("first")
                last = parsed.get("last")
                fixed_first, fixed_last = fix_name_overlap(safe_text(first), safe_text(last), name)

                jobs_payload = jobs_payload_from_structured(parsed)
                if not any((safe_text(j.get("title")) != "N/A" for j in jobs_payload)):
                    jobs_payload = parsed.get("jobs")
                jobs_payload = merge_job_payloads(
                    jobs_payload if isinstance(jobs_payload, list) else [],
                    job_records_from_candidates(payload.get("experience_candidates")),
                )
                jobs = normalize_jobs(jobs_payload)

                writer.writerow(
                    [
                        fixed_first,
                        fixed_last,
                        safe_text(education[0].get("school")),
                        strict_degree_short_form(safe_text(education[0].get("degree"))),
                        clean_major_field(safe_text(education[0].get("major"))),
                        safe_text(education[1].get("school")),
                        degree_to_short_form(safe_text(education[1].get("degree"))),
                        clean_major_field(safe_text(education[1].get("major"))),
                        safe_text(education[2].get("school")),
                        degree_to_short_form(safe_text(education[2].get("degree"))),
                        clean_major_field(safe_text(education[2].get("major"))),
                        jobs[0]["title"],
                        jobs[0]["company"],
                        jobs[1]["title"],
                        jobs[1]["company"],
                        jobs[2]["title"],
                        jobs[2]["company"],
                        linkedin_url,
                    ]
                )
                out_handle.flush()

                existing_urls.add(linkedin_url)
                kept += 1

        print(f"Refine stage complete. Processed rows: {processed}")
        print(f"Rows kept (UNT only): {kept}")
        print(f"Rows dropped by iron gate: {skipped_non_unt}")
        print(f"Rows skipped (empty text): {skipped_empty_text}")
        print(f"Rows skipped (parse/retry exhaustion): {skipped_empty_parse}")
        print(f"Final output file: {FINAL_CSV_FILE}")
    finally:
        out_handle.close()


if __name__ == "__main__":
    main()
