import csv
import json
import os
import re
import time
from pathlib import Path

from dotenv import load_dotenv
from groq import Groq

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

SYSTEM_PROMPT = """You are a strict alumni data parser.
Return ONLY compact JSON and no extra text.

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
- Source text is noisy. Extract only evidence-backed facts.
- Populate edu_1..edu_3 and job_1..job_3 directly; do not return arrays.
- Examine all education evidence.
- The highest degree from University of North Texas (Ph.D. > M.S. > B.S.) must be placed in edu_1.
- All other education records (UNT or non-UNT) should follow in recency/chronology order in edu_2 and edu_3.
- You are failing to capture the major for some users. If the text says "Computer Science", "Mechanical Engineering", or "Biomedical Engineering", that is the MAJOR. You must extract it even if it is not on the same line as the degree type.
- Jobs MUST be ranked by recency using date strings (Present, YYYY, or ranges like 2024 - 2025).
- job_1 = current/most recent, job_3 = oldest of the returned three.
- Do NOT skip service/retail roles (e.g., Walmart, Cashier) if they are most recent.
- Return exactly 3 jobs as job_1..job_3 with title, company, dates.
- For education.major, keep only the primary major and strip GPA, honors, minors, activities, and other extras.
- Fix duplicated or overlapped names. Example: "John John" should become first="John", last="N/A" if no real last name exists.
- Use "N/A" for unknown strings.
"""

_JUNK_SECTION_HEADERS = [
    "## About",
    "## Activity",
    "## Licenses & Certifications",
    "## Skills",
    "## Courses",
    "## Languages",
    "## Honors & Awards",
]

_METADATA_PATTERNS = [
    re.compile(r"connections\s*•\s*followers", re.IGNORECASE),
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
_ADVISOR_RE = re.compile(r"advisor:\s*dr\.", re.IGNORECASE)
_UNT_LINE_RE = re.compile(r"(?:university\s+of\s+north\s+texas|\bunt\b)", re.IGNORECASE)
_EDU_CONTEXT_RE = re.compile(
    r"(?:university\s+of\s+north\s+texas|\bunt\b|\bbachelor\b|\bmaster\b|\bph\.?d\.?\b|\bb\.?s\.?\b|\bm\.?s\.?\b)",
    re.IGNORECASE,
)
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


def pre_clean_text(text: str) -> str:
    raw = str(text or "")
    if not raw.strip():
        return ""

    # 1) Remove noisy blocks beginning with known headers until next markdown header.
    for header in _JUNK_SECTION_HEADERS:
        block_re = re.compile(
            rf"{re.escape(header)}\b.*?(?=\n##\s+|\Z)",
            re.IGNORECASE | re.DOTALL,
        )
        raw = block_re.sub("\n", raw)

    # 2) Normalize line structure and remove known noisy metadata lines.
    cleaned_lines = []
    for line in raw.splitlines():
        working = line.strip()
        if not working:
            continue

        # Strip obvious truncation artifacts and repeated separators.
        working = re.sub(r"\s*\.\.\.\s*", " ", working)
        working = re.sub(r"\s{2,}", " ", working).strip()

        if any(pattern.search(working) for pattern in _METADATA_PATTERNS):
            continue

        # Explicitly remove advisor lines that can be misread as person identity.
        if _ADVISOR_RE.search(working):
            continue

        # Strip location tails often attached to headings.
        working = _LOCATION_TAIL_RE.sub("", working).strip(" -,")

        if not working:
            continue

        # Drop low-signal non-profile noise that burns tokens.
        lowered = working.lower()
        if lowered.startswith("##") and (
            "licenses" in lowered
            or "skills" in lowered
            or "activity" in lowered
            or "courses" in lowered
            or "languages" in lowered
            or "honors" in lowered
        ):
            continue
        if lowered.startswith("activity:") or lowered.startswith("github profile"):
            continue

        cleaned_lines.append(working)

    # 3) Strict structured extraction payload:
    # HEADER = first 10 high-signal lines from full cleaned text.
    # EDUCATION = first 10 lines inside education section.
    # EXPERIENCE = first 10 lines inside experience section.
    header_lines = []
    education_lines = []
    experience_lines = []
    current_section = "other"

    for line in cleaned_lines:
        lowered = line.lower()
        if lowered.startswith("## "):
            if "education" in lowered:
                current_section = "education"
            elif "experience" in lowered:
                current_section = "experience"
            else:
                current_section = "other"
            continue

        if len(header_lines) < 10:
            header_lines.append(line)

        if current_section == "education" and len(education_lines) < 10:
            if line.startswith("###") or _DATE_LINE_RE.search(line) or _EDU_CONTEXT_RE.search(line):
                education_lines.append(line)

        if current_section == "experience" and len(experience_lines) < 10:
            if line.startswith("###") or _DATE_LINE_RE.search(line):
                experience_lines.append(line)

        if len(header_lines) >= 10 and len(education_lines) >= 10 and len(experience_lines) >= 10:
            break

    # Fallbacks keep the strict 3-block format while preserving minimum signal.
    if not education_lines:
        education_lines = [
            line
            for line in cleaned_lines
            if (_EDU_CONTEXT_RE.search(line) or _DATE_LINE_RE.search(line) or line.startswith("###"))
        ][:10]
    if not experience_lines:
        experience_lines = [
            line
            for line in cleaned_lines
            if (_DATE_LINE_RE.search(line) or line.startswith("###"))
        ][:10]

    def _render_block(label: str, lines: list[str]) -> str:
        if not lines:
            return f"{label}:\nN/A"
        return f"{label}:\n" + "\n".join(lines[:10])

    compact = "\n\n".join(
        [
            _render_block("HEADER", header_lines[:10]),
            _render_block("EDUCATION", education_lines[:10]),
            _render_block("EXPERIENCE", experience_lines[:10]),
        ]
    )
    compact = re.sub(r"\n{3,}", "\n\n", compact)
    return compact.strip()


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


def normalize_education(raw_education) -> list[dict]:
    source = raw_education if isinstance(raw_education, list) else []
    cleaned = []
    for item in source:
        if not isinstance(item, dict):
            continue
        school = safe_text(clean_doubled(item.get("school")))
        degree = degree_to_short_form(clean_doubled(item.get("degree")))
        major = clean_major_field(clean_doubled(item.get("major")))
        cleaned.append(
            {
                "school": school,
                "degree": degree,
                "major": major,
                "start": safe_text(item.get("start") or item.get("start_date")),
                "end": safe_text(item.get("end") or item.get("end_date") or item.get("year")),
            }
        )

    # Backward compatibility: if older payload returns single unt_education object.
    if not cleaned and isinstance(raw_education, dict):
        school = safe_text(clean_doubled(raw_education.get("school")))
        degree = degree_to_short_form(clean_doubled(raw_education.get("degree")))
        major = clean_major_field(clean_doubled(raw_education.get("major")))
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

    # Strict dedupe by institution: keep first appearance, blank repeated-school slots.
    deduped = []
    seen_school_keys = set()
    for entry in ordered:
        school_key = _school_dedupe_key(entry.get("school"))
        if school_key in seen_school_keys:
            continue
        seen_school_keys.add(school_key)
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
        payload.append(
            {
                "school": entry.get("school"),
                "degree": entry.get("degree"),
                "major": entry.get("major"),
                "end_date": entry.get("dates"),
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


def call_groq_extract(client: Groq, name: str, highlight_text: str) -> dict:
    highlight_text = pre_clean_text(highlight_text)
    if not highlight_text:
        return {}

    user_prompt = (
        "Raw profile name:\n"
        f"{name}\n\n"
        "Structured extraction payload (use HEADER/EDUCATION/EXPERIENCE as evidence):\n"
        f"{highlight_text}\n"
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
                        temperature=0.1,
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
                return parsed
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

    return {}


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
        start_date = safe_text(item.get("start_date") or item.get("start") or item.get("date_range"))
        end_date = safe_text(item.get("end_date") or item.get("end"))
        clean_jobs.append(
            {
                "title": safe_text(clean_doubled(item.get("title"))),
                "company": safe_text(clean_doubled(item.get("company"))),
                "start_date": start_date,
                "end_date": end_date,
            }
        )

    # Law of recency: Job 1 = most recent/current; Job 3 = oldest.
    clean_jobs.sort(key=_job_recency_key, reverse=True)

    # Keep only distinct title+company pairs so repeated roles don't fill slots 2/3.
    deduped_jobs = []
    seen_pairs = set()
    for job in clean_jobs:
        pair = (
            safe_text(job.get("title")).lower(),
            safe_text(job.get("company")).lower(),
        )
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
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
        payload.append(
            {
                "title": entry.get("title"),
                "company": entry.get("company"),
                "end_date": entry.get("dates"),
            }
        )
    return payload


def is_unt_school(school: str) -> bool:
    text = (school or "").strip().lower()
    return "university of north texas" in text or re.search(r"\bunt\b", text) is not None


def main() -> None:
    load_dotenv()
    require_env("GROQ_API_KEY")

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

                parsed = call_groq_extract(client, name, highlight_text)
                if not parsed:
                    skipped_empty_parse += 1
                    continue

                education_payload = education_payload_from_structured(parsed)
                if not any((safe_text(e.get("school")) != "N/A" for e in education_payload)):
                    education_payload = parsed.get("education")
                    if not isinstance(education_payload, list):
                        education_payload = parsed.get("unt_education")

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
