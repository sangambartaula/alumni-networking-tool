import argparse
import csv
import os
import re
import time
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv
from exa_py import Exa


BASE_DIR = Path(__file__).resolve().parent
ARTIFACTS_DIR = BASE_DIR / "artifacts"
RAW_CSV_FILE = Path(os.getenv("EXA_RAW_CSV_FILE", str(ARTIFACTS_DIR / "raw_alumni_data.csv")))
SEEN_URLS_FILE = Path(os.getenv("EXA_SEEN_URLS_FILE", str(ARTIFACTS_DIR / "seen_urls.txt")))
RESULTS_PER_BATCH = int(os.getenv("EXA_RESULTS_PER_BATCH", "50"))
MAX_PAGES_PER_BATCH = int(os.getenv("EXA_MAX_PAGES_PER_BATCH", "250"))
EXA_SEARCH_TYPE = os.getenv("EXA_SEARCH_TYPE", "neural").strip() or "neural"
EXA_HIGHLIGHT_MAX_CHARACTERS = int(os.getenv("EXA_HIGHLIGHT_MAX_CHARACTERS", "2200"))
EXA_MAX_AGE_HOURS = int(os.getenv("EXA_MAX_AGE_HOURS", "1"))
EXA_LIVECRAWL_TIMEOUT_MS = int(os.getenv("EXA_LIVECRAWL_TIMEOUT_MS", "12000"))
EXA_PAGE_DELAY_SECONDS = float(os.getenv("EXA_PAGE_DELAY_SECONDS", "0.3"))
EXA_LOW_YIELD_STOP_RATIO = float(os.getenv("EXA_LOW_YIELD_STOP_RATIO", "0.1"))

HIGHLIGHT_GUIDE_QUERY = (
    "Education at University of North Texas or UNT: school, degree, major, graduation year. "
    "Work experience: company, job title, start and end date. "
    "Return the profile header plus education and experience only. "
    "Exclude skills, activity, licenses, certifications, courses, honors, follower counts, "
    "company-size metadata, and long descriptions."
)

_NOISE_SECTION_TITLES = (
    "activity",
    "licenses",
    "certifications",
    "skills",
    "courses",
    "languages",
    "honors",
    "projects",
)
_NOISE_LINE_PREFIXES = (
    "view certificate",
    "view publication",
    "issued:",
    "github profile",
    "username:",
    "activities:",
    "activities and societies:",
    "honors:",
    "coursework:",
)
_METADATA_PATTERNS = [
    re.compile(r"\bconnections\b.*\bfollowers\b", re.IGNORECASE),
    re.compile(r"Company:\s*[0-9,+\- ]+employees", re.IGNORECASE),
    re.compile(r"Founded\s*[0-9]{4}", re.IGNORECASE),
    re.compile(r"\b(?:Privately Held|Public Company|Nonprofit)\b", re.IGNORECASE),
    re.compile(r"Department:\s*.*", re.IGNORECASE),
    re.compile(r"Level:\s*.*", re.IGNORECASE),
    re.compile(r"^Total Experience:.*", re.IGNORECASE),
]
_DATE_LINE_RE = re.compile(
    r"(\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+\d{4}\b|\bPresent\b|\b\d{4}\s*-\s*(?:\d{4}|Present)\b)",
    re.IGNORECASE,
)
_PROFILE_SIGNAL_RE = re.compile(
    r"(?:university\s+of\s+north\s+texas|\bunt\b|##\s+education|##\s+experience|"
    r"\bbachelor\b|\bmaster\b|\bph\.?d\b|\bcomputer science\b|\bengineering\b)",
    re.IGNORECASE,
)
_ABOUT_SIGNAL_RE = re.compile(
    r"(?:university\s+of\s+north\s+texas|\bunt\b|\bmajor(?:ing)?\s+in\b|"
    r"\bpursuing a major in\b|\bgraduate\b|\bstudent\b|\bbachelor\b|\bmaster\b|\bph\.?d\b)",
    re.IGNORECASE,
)
_UNT_SIGNAL_RE = re.compile(r"(?:university\s+of\s+north\s+texas|\bnorth texas\b|\bunt\b)", re.IGNORECASE)
_DEGREE_SIGNAL_RE = re.compile(r"\b(?:bachelor|master|ph\.?d|b\.?s\.?|m\.?s\.?|doctorate)\b", re.IGNORECASE)
_INLINE_EDU_NOISE_RE = re.compile(
    r"(?:\bHonors?:|\bActivities and societies:|\bActivities:|\bCoursework:).*$",
    re.IGNORECASE,
)
_NAME_JUNK_RE = re.compile(r"(?:linkedin|company|group|jobs|hiring|http|www\.)", re.IGNORECASE)
_EDUCATION_TITLE_SIGNAL_RE = re.compile(
    r"\b(?:degree|bachelor|master|associate|ph\.?d|doctor(?:ate| of philosophy)|b\.?s\.?|m\.?s\.?|"
    r"btech|b\.?tech|basc|b\.?a\.?s\.?c|mba)\b",
    re.IGNORECASE,
)
_NARRATIVE_CUE_RE = re.compile(
    r"\b(?:student at|candidate at|with a\b|passionate\b|leverag(?:e|ing)\b|thrilled\b|currently\b|"
    r"seeking\b|focus(?:ed|ing)\b|experienced\b)\b",
    re.IGNORECASE,
)
# Five required discipline batches.
BATCH_QUERIES = {
    "Digital": (
        'site:linkedin.com/in/ ("University of North Texas" OR UNT) '
        '(alumni OR student OR "bachelor" OR "master" OR "phd") '
        '("computer science" OR "software engineer" OR "software developer" OR '
        '"artificial intelligence" OR "machine learning" OR "data science" OR '
        '"cybersecurity" OR "cloud")'
    ),
    "Hardware": (
        'site:linkedin.com/in/ ("University of North Texas" OR UNT) '
        '(alumni OR student OR "bachelor" OR "master" OR "phd") '
        '("electrical engineer" OR "embedded engineer" OR "hardware engineer" OR '
        '"firmware engineer" OR "vlsi" OR "semiconductor" OR "fpga" OR '
        '"circuit design") '
        '-("computer science")'
    ),
    "Mechanical": (
        'site:linkedin.com/in/ ("University of North Texas" OR UNT) '
        '(alumni OR student OR "bachelor" OR "master" OR "phd") '
        '("mechanical engineer" OR "manufacturing engineer" OR "design engineer" OR '
        '"hvac" OR "thermodynamics" OR "robotics" OR "cad") '
        '-("computer science" OR "construction")'
    ),
    "Construction": (
        'site:linkedin.com/in/ ("University of North Texas" OR UNT) '
        '(alumni OR student OR "bachelor" OR "master" OR "phd") '
        '("construction engineer" OR "construction management" OR "project engineer" OR '
        '"civil engineer" OR "site engineer" OR "bim" OR "revit" OR '
        '"cost engineer") '
        '-("computer science" OR "mechanical")'
    ),
    "Biomedical": (
        'site:linkedin.com/in/ ("University of North Texas" OR UNT) '
        '(alumni OR student OR "bachelor" OR "master" OR "phd") '
        '("biomedical engineer" OR "medical device" OR "clinical engineer" OR '
        '"bioinformatics" OR "biomaterials" OR "medical imaging" OR '
        '"health informatics") '
        '-("mechanical" OR "computer science")'
    ),
}

# Base volume targets per batch family.
BASE_BATCH_TARGETS = {
    "Digital": 4000,
    "Mechanical": 2000,
    "Electrical": 1500,
    "Construction": 1500,
    "Biomedical": 1000,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gather raw UNT alumni profiles from Exa.")
    parser.add_argument("--test", action="store_true", default=False, help="Run at a tiny scale for smoke testing.")
    return parser.parse_args()


def compute_scaled_targets(is_test: bool) -> dict[str, int]:
    if not is_test:
        return {
            "Digital": BASE_BATCH_TARGETS["Digital"],
            "Hardware": BASE_BATCH_TARGETS["Electrical"],
            "Mechanical": BASE_BATCH_TARGETS["Mechanical"],
            "Construction": BASE_BATCH_TARGETS["Construction"],
            "Biomedical": BASE_BATCH_TARGETS["Biomedical"],
        }

    scaled = {}
    for batch_name, base_target in (
        ("Digital", BASE_BATCH_TARGETS["Digital"]),
        ("Hardware", BASE_BATCH_TARGETS["Electrical"]),
        ("Mechanical", BASE_BATCH_TARGETS["Mechanical"]),
        ("Construction", BASE_BATCH_TARGETS["Construction"]),
        ("Biomedical", BASE_BATCH_TARGETS["Biomedical"]),
    ):
        scaled_value = base_target / 1000.0
        if scaled_value in {1.0, 1.5}:
            target = 1
        else:
            target = max(1, int(scaled_value))
        scaled[batch_name] = target
    return scaled


def reset_exa_search_state(exa_api_key: str) -> Exa:
    # Recreate the client once at process start so each run starts from a fresh Exa session state.
    return Exa(exa_api_key)


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def normalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlsplit(url.strip())
    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()
    path = (parsed.path or "").rstrip("/")
    return urlunsplit((scheme, netloc, path, "", ""))


def is_linkedin_profile_url(url: str) -> bool:
    normalized = normalize_url(url)
    if not normalized:
        return False
    parsed = urlsplit(normalized)
    return "linkedin.com" in parsed.netloc and parsed.path.startswith("/in/")


def load_seen_urls(path: Path) -> set[str]:
    if not path.exists():
        return set()
    urls = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            normalized = normalize_url(line.strip())
            if normalized:
                urls.add(normalized)
    return urls


def append_seen_url(path: Path, url: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{url}\n")


def ensure_raw_csv(path: Path) -> tuple[csv.writer, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists() and path.stat().st_size > 0
    handle = path.open("a", newline="", encoding="utf-8")
    writer = csv.writer(handle)
    if not file_exists:
        writer.writerow(["name", "url", "highlight_text", "discipline_category"])
        handle.flush()
    return writer, handle


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s{2,}", " ", str(text or "").strip())


def _normalize_highlight_text(text: str) -> str:
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
    compact = _normalize_whitespace(line)
    if not compact:
        return ""
    compact = compact.strip(" |")
    compact = re.sub(r"\s*\.\.\.\s*", " ... ", compact)
    return _normalize_whitespace(compact)


def _is_metadata_line(line: str) -> bool:
    lowered = line.lower()
    if any(pattern.search(line) for pattern in _METADATA_PATTERNS):
        return True
    if any(lowered.startswith(prefix) for prefix in _NOISE_LINE_PREFIXES):
        return True
    return False


def split_highlight_sections(text: str) -> dict[str, list[str]]:
    sections = {"header": [], "about": [], "education": [], "experience": [], "other": []}
    current_section = "header"

    for raw_line in _normalize_highlight_text(text).splitlines():
        line = _compact_line(raw_line)
        if not line or _is_metadata_line(line):
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

            if remainder and current_section in {"about", "education", "experience"} and not _is_metadata_line(remainder):
                sections[current_section].append(remainder)
            continue

        sections[current_section].append(line)

    return sections


def _split_sentences(text: str) -> list[str]:
    normalized = _normalize_whitespace(text)
    if not normalized:
        return []
    parts = re.split(r"(?<=[.!?])\s+|\s+\.\.\.\s+", normalized)
    return [_compact_line(part) for part in parts if _compact_line(part)]


def _chunk_section_lines(lines: list[str]) -> list[list[str]]:
    chunks = []
    current = []
    for line in lines:
        if line.startswith("###"):
            if current:
                chunks.append(current)
            current = [line]
            continue
        if current:
            current.append(line)
    if current:
        chunks.append(current)
    if not chunks and lines:
        chunks.append(lines[:])
    return chunks


def _heading_body(heading: str) -> str:
    return heading[3:].strip() if heading.startswith("###") else heading.strip()


def _trim_narrative_tail(text: str) -> str:
    compact = _compact_line(text)
    if "..." not in compact:
        return compact
    head, _, tail = compact.partition("...")
    if tail and _NARRATIVE_CUE_RE.search(tail):
        return _compact_line(f"{head} ...")
    return compact


def _is_blank_education_heading(heading: str) -> bool:
    body = _heading_body(heading)
    if not body:
        return True
    lowered = body.lower()
    if not lowered.startswith("at "):
        return False
    return True


def _is_experience_noise_heading(heading: str) -> bool:
    body = _heading_body(heading)
    if not body:
        return True
    lowered = body.lower()
    if lowered.startswith("student at ") and "university of north texas" in lowered:
        return True
    if lowered.startswith("student ") and "university of north texas" in lowered:
        return True
    return False


def _is_valid_education_heading(heading: str) -> bool:
    body = _trim_narrative_tail(_heading_body(heading))
    if not body:
        return False
    lowered = body.lower()
    if " at " not in lowered:
        return False
    if _NARRATIVE_CUE_RE.search(body):
        return False

    subject, _, school = body.partition(" at ")
    if not school.strip():
        return False
    if _EDUCATION_TITLE_SIGNAL_RE.search(subject):
        return True

    if "," in subject or "." in subject:
        return False

    words = re.findall(r"[A-Za-z][A-Za-z&'/.-]*", subject)
    if 1 <= len(words) <= 6:
        return True
    return False


def _clean_section_line(line: str, section_name: str) -> str:
    compact = _compact_line(line)
    if not compact or _is_metadata_line(compact):
        return ""
    if section_name == "education":
        compact = _INLINE_EDU_NOISE_RE.sub("", compact).strip(" ,-;|")
        if not compact or compact.lower().startswith("grade:"):
            return ""
        compact = _trim_narrative_tail(compact)
    if section_name == "experience":
        if compact.lower().startswith("show all "):
            return ""
        if compact.lower().startswith("see all "):
            return ""
    return compact


def _looks_like_entry_heading(line: str, section_name: str) -> bool:
    lowered = line.lower()
    if line.startswith("###"):
        return True
    if section_name == "education":
        return " at " in lowered and (
            _DEGREE_SIGNAL_RE.search(line)
            or "degree" in lowered
            or "btech" in lowered
            or "basc" in lowered
            or "bachelor of technology" in lowered
        )
    if section_name == "experience":
        return " at " in lowered
    return False


def _normalize_section_lines(lines: list[str], section_name: str) -> list[str]:
    normalized = []
    for raw_line in lines:
        compact = _clean_section_line(raw_line, section_name)
        if not compact:
            continue
        if _looks_like_entry_heading(compact, section_name):
            if not compact.startswith("###"):
                compact = f"### {compact}"
        normalized.append(compact)
    return normalized


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    deduped = []
    seen = set()
    for value in values:
        key = value.casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _select_header_lines(lines: list[str]) -> list[str]:
    selected = []
    for line in lines:
        if _is_metadata_line(line):
            continue
        if "," in line and " at " not in line.lower() and "|" not in line and not line.startswith("#"):
            continue
        selected.append(line)
        if len(selected) >= 2:
            break
    return _dedupe_preserve_order(selected)


def _select_about_lines(lines: list[str]) -> list[str]:
    selected = []
    for sentence in _split_sentences(" ".join(lines)):
        if not _ABOUT_SIGNAL_RE.search(sentence):
            continue
        selected.append(sentence)
        if len(selected) >= 2:
            break
    return _dedupe_preserve_order(selected)


def _compact_profile_section(lines: list[str], max_entries: int, section_name: str) -> list[str]:
    compact_entries = []
    normalized_lines = _normalize_section_lines(lines, section_name)
    for chunk in _chunk_section_lines(normalized_lines):
        heading = _compact_line(chunk[0]) if chunk else ""
        if not heading:
            continue
        if section_name == "education" and "certificate" in heading.lower() and not _DEGREE_SIGNAL_RE.search(heading):
            continue
        if section_name == "education" and _is_blank_education_heading(heading):
            continue
        if section_name == "education" and not _is_valid_education_heading(heading):
            continue
        if section_name == "experience" and _is_experience_noise_heading(heading):
            continue

        entry_lines = [heading]
        for line in chunk[1:]:
            compact = _clean_section_line(line, section_name)
            if not compact:
                continue
            if _DATE_LINE_RE.search(compact):
                entry_lines.append(compact)
                break

        compact_entries.append("\n".join(entry_lines))
        if len(compact_entries) >= max_entries:
            break

    return compact_entries


def clean_highlight_text(text: str) -> str:
    sections = split_highlight_sections(text)

    parts = []
    header_lines = _select_header_lines(sections.get("header", []))
    if header_lines:
        parts.extend(header_lines)

    about_lines = _select_about_lines(sections.get("about", []))
    if about_lines:
        parts.append("## About")
        parts.extend(about_lines)

    experience_entries = _compact_profile_section(sections.get("experience", []), max_entries=4, section_name="experience")
    if experience_entries:
        parts.append("## Experience")
        parts.extend(experience_entries)

    education_entries = _compact_profile_section(sections.get("education", []), max_entries=4, section_name="education")
    if education_entries:
        parts.append("## Education")
        parts.extend(education_entries)

    cleaned = "\n\n".join(_dedupe_preserve_order(parts)).strip()
    if len(cleaned) > EXA_HIGHLIGHT_MAX_CHARACTERS:
        cleaned = cleaned[:EXA_HIGHLIGHT_MAX_CHARACTERS].rsplit("\n", 1)[0].strip()
    return cleaned


def highlight_has_profile_signal(text: str) -> bool:
    compact = str(text or "").strip()
    if not compact:
        return False
    if not _PROFILE_SIGNAL_RE.search(compact):
        return False
    return "## Experience" in compact or "## Education" in compact or _DATE_LINE_RE.search(compact) is not None


def is_valid_name(name: str) -> bool:
    compact = _normalize_whitespace(name)
    if len(compact) < 3:
        return False
    if _NAME_JUNK_RE.search(compact):
        return False
    alpha_tokens = re.findall(r"[A-Za-z][A-Za-z'.-]*", compact.split("|", 1)[0])
    return len(alpha_tokens) >= 2


def is_valid_unt_profile(name: str, highlight_text: str) -> bool:
    compact = str(highlight_text or "").strip()
    if not is_valid_name(name):
        return False
    if not compact:
        return False
    if not _UNT_SIGNAL_RE.search(compact):
        return False
    return highlight_has_profile_signal(compact)


def extract_highlight_text(result) -> str:
    highlights = getattr(result, "highlights", None) or []
    raw_text = ""
    if isinstance(highlights, list):
        raw_text = "\n".join(str(part).strip() for part in highlights if str(part).strip()).strip()
    else:
        raw_text = str(highlights).strip()
    return clean_highlight_text(raw_text)


def search_people_with_highlights(exa: Exa, query: str, page_size: int):
    base_kwargs = {
        "type": EXA_SEARCH_TYPE,
        "category": "people",
        "num_results": page_size,
        "include_domains": ["linkedin.com"],
        "highlights": {
            "query": HIGHLIGHT_GUIDE_QUERY,
            "max_characters": EXA_HIGHLIGHT_MAX_CHARACTERS,
        },
        "text": False,
        "summary": False,
    }

    attempts = (
        {**base_kwargs, "max_age_hours": EXA_MAX_AGE_HOURS, "livecrawl_timeout": EXA_LIVECRAWL_TIMEOUT_MS},
        {**base_kwargs, "maxAgeHours": EXA_MAX_AGE_HOURS, "livecrawlTimeout": EXA_LIVECRAWL_TIMEOUT_MS},
        {**base_kwargs, "livecrawl": "preferred"},
    )

    last_error = None
    for kwargs in attempts:
        try:
            return exa.search_and_contents(query, **kwargs)
        except TypeError as err:
            last_error = err
            continue

    if last_error is not None:
        raise last_error
    return exa.search_and_contents(query, **base_kwargs)


def gather_batch(
    exa: Exa,
    batch_name: str,
    query: str,
    batch_target: int,
    seen_urls: set[str],
    writer: csv.writer,
    handle,
) -> int:
    inserted = 0
    page_number = 0
    skipped_low_signal = 0

    while inserted < batch_target and page_number < MAX_PAGES_PER_BATCH:
        page_number += 1
        remaining = batch_target - inserted
        page_size = min(RESULTS_PER_BATCH, max(1, remaining))

        response = search_people_with_highlights(exa, query, page_size)

        results = getattr(response, "results", []) or []
        if not results:
            break

        new_in_page = 0
        for item in results:
            if inserted >= batch_target:
                break

            url = normalize_url(getattr(item, "url", ""))
            if not is_linkedin_profile_url(url):
                continue
            if url in seen_urls:
                continue

            highlight_text = extract_highlight_text(item)
            name = str(getattr(item, "title", "") or "").strip()
            if not is_valid_unt_profile(name, highlight_text):
                skipped_low_signal += 1
                continue

            writer.writerow([name, url, highlight_text, batch_name])
            handle.flush()

            append_seen_url(SEEN_URLS_FILE, url)
            seen_urls.add(url)
            inserted += 1
            new_in_page += 1

        print(
            f"    Page {page_number}: {new_in_page} new rows ({inserted}/{batch_target}), "
            f"low-signal skipped={skipped_low_signal}"
        )

        # Stop when a full page returns no unseen links, which indicates no more usable results.
        if new_in_page == 0:
            break
        if page_number > 3 and new_in_page < max(1, int(page_size * EXA_LOW_YIELD_STOP_RATIO)):
            print(f"    Low yield ({new_in_page}/{page_size}), stopping batch early")
            break
        if EXA_PAGE_DELAY_SECONDS > 0:
            time.sleep(EXA_PAGE_DELAY_SECONDS)

    return inserted


def main() -> None:
    args = parse_args()
    load_dotenv()
    exa_api_key = require_env("EXA_API_KEY")

    seen_urls = load_seen_urls(SEEN_URLS_FILE)
    exa = reset_exa_search_state(exa_api_key)
    batch_targets = compute_scaled_targets(args.test)

    writer, handle = ensure_raw_csv(RAW_CSV_FILE)
    try:
        print(
            f"Starting gather stage. test_mode={args.test}. "
            f"Existing seen URLs: {len(seen_urls)}"
        )
        print(f"Targets: {batch_targets}")
        total_inserted = 0

        for batch_name, query in BATCH_QUERIES.items():
            target = batch_targets.get(batch_name, 0)
            if target <= 0:
                print(f"Skipping batch: {batch_name} (target={target})")
                continue

            print(f"Querying batch: {batch_name} (target={target})")
            try:
                inserted = gather_batch(
                    exa,
                    batch_name,
                    query,
                    target,
                    seen_urls,
                    writer,
                    handle,
                )
                total_inserted += inserted
                print(f"  Added {inserted} new profiles from {batch_name}")
            except Exception as err:
                print(f"  Skipped {batch_name} due to error: {err}")
                time.sleep(1.5)

        print(f"Gather stage complete. New rows appended: {total_inserted}")
        print(f"Raw output file: {RAW_CSV_FILE}")
    finally:
        handle.close()


if __name__ == "__main__":
    main()
