"""
Groq API integration for extracting job experiences from LinkedIn HTML.

Uses shared Groq client infrastructure from groq_client.py.
"""

import re
from config import logger
from groq_client import (
    _get_client, is_groq_available, GROQ_MODEL, BS4_AVAILABLE,
    SCRAPER_DEBUG_HTML, save_debug_html, parse_groq_json_response,
    _clean_doubled, parse_groq_date
)

if BS4_AVAILABLE:
    from bs4 import BeautifulSoup


CLOUD_JOB_MAX_LEN = 255
_BARE_EMPLOYMENT_TYPES = {
    "internship",
    "part-time",
    "full-time",
    "contract",
    "seasonal",
    "temporary",
    "apprenticeship",
    "self-employed",
    "freelance",
}


def _looks_like_location_fragment(fragment: str) -> bool:
    """Heuristic for trailing location fragments like ', Hyderabad' or ' - Austin'."""
    if not fragment:
        return False
    frag = fragment.strip()
    if not frag:
        return False
    if re.search(r"\d", frag):
        return False

    low = frag.lower()
    if re.search(
        r"\b(inc|llc|ltd|corp|company|co|technologies|technology|systems|solutions|group|university|college|school)\b",
        low,
        re.I,
    ):
        return False

    words = re.findall(r"[A-Za-z][A-Za-z'.&-]*", frag)
    return 1 <= len(words) <= 4


def _strip_trailing_location_fragment(text: str) -> str:
    """Strip trailing location suffix when separated by comma/dash."""
    t = (text or "").strip()
    if not t:
        return ""
    for sep in [",", " - ", " – ", " — "]:
        if sep in t:
            head, tail = t.rsplit(sep, 1)
            if _looks_like_location_fragment(tail):
                return head.strip()
    return t


def _canonical_entity_text(text: str) -> str:
    """Normalize strings for lightweight title/company collision checks."""
    normalized = re.sub(r"[^a-z0-9]+", " ", (text or "").casefold())
    return re.sub(r"\s+", " ", normalized).strip()


def _is_company_title_collision(title: str, company: str) -> bool:
    """True when title and company resolve to the same label."""
    title_key = _canonical_entity_text(title)
    company_key = _canonical_entity_text(company)
    if not title_key or not company_key:
        return False
    return title_key == company_key


def _job_entry_exceeds_cloud_limit(job: dict, max_len: int = CLOUD_JOB_MAX_LEN) -> bool:
    if not isinstance(job, dict):
        return False
    fields = [
        str(job.get("job_title") or ""),
        str(job.get("company") or ""),
        str(job.get("employment_type") or ""),
        str(job.get("start_date") or ""),
        str(job.get("end_date") or ""),
    ]
    return any(len(value) > max_len for value in fields)


def _normalize_job_text(value: str) -> str:
    text = _clean_doubled((value or "").strip())
    text = _strip_trailing_location_fragment(text)
    text = re.sub(
        r'\s+(?:Full-time|Part-time|Contract|Internship|Self-employed|Freelance|Seasonal|Temporary|Apprenticeship)$',
        '',
        text,
        flags=re.IGNORECASE,
    ).strip()
    return text


def _is_bare_employment_type(title: str) -> bool:
    return (title or "").strip().casefold() in _BARE_EMPLOYMENT_TYPES


# Strip common LinkedIn level prefixes from titles for cleaner storage (Sr./Jr./Associate, etc.).
# Deliberately excludes Staff/Principal/Lead — those are part of role identity for many tracks.
_TITLE_LEVEL_PREFIX = re.compile(
    r"^(?:senior|sr\.?|junior|jr\.?|associate|entry[\s-]?level|assistant|asst\.?)\s+",
    re.IGNORECASE,
)


def strip_seniority_prefixes_from_title(title: str) -> str:
    """Remove stacked level prefixes (e.g. 'Senior Senior Analyst' -> 'Analyst')."""
    t = (title or "").strip()
    if not t:
        return ""
    prev = None
    while prev != t:
        prev = t
        t = _TITLE_LEVEL_PREFIX.sub("", t).strip()
    return t


def _html_to_structured_text(html: str, profile_name: str = "unknown") -> str:
    """
    Convert experience section HTML into clean structured text for the LLM.
    
    This is dramatically more reliable than sending raw HTML:
    - No markup confusion
    - No JavaScript hallucination
    - Fewer tokens
    - Cleaner extraction
    """
    if not BS4_AVAILABLE:
        # Fallback: strip tags with regex
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:5000]
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Remove noise elements
    for tag in soup.find_all(['script', 'style', 'svg', 'img', 'iframe', 'noscript']):
        tag.decompose()
    for tag in soup.find_all(class_=lambda x: x and any(c in x for c in ['visually-hidden', 'inline-show-more-text', 'artdeco-button__icon'])):
        tag.decompose()
    
    # Remove diamond-icon skill/tag entries from sub-components
    for sub_comp in soup.select('.pvs-entity__sub-components'):
        for container in sub_comp.find_all('div', class_=lambda c: c and 't-14' in c and 't-normal' in c and 't-black' in c and 't-black--light' not in c):
            if container.find('strong'):
                li_parent = container.find_parent('li')
                if li_parent:
                    li_parent.decompose()
                else:
                    container.decompose()
    
    # Remove skill lines like "Teaching, Computer Science and +1 skill"
    for tag in soup.find_all(["strong", "span"]):
        txt = tag.get_text()
        if ("skill" in txt.lower() and "+" in txt) or ("skills" in txt.lower() and ("+" in txt or "," in txt)):
            parent = tag.find_parent("div", class_="display-flex")
            if parent:
                parent.decompose()
            else:
                tag.decompose()
    
    # Now extract structured entries from the experience list items
    entries = []
    
    # Find top-level experience list items
    experience_items = soup.select('li.pvs-list__paged-list-item') or soup.select('li[class*="pvs-list"]') or soup.find_all('li')
    
    for item in experience_items:
        text = item.get_text(separator=' | ', strip=True)
        # Clean up excessive pipes and whitespace
        text = re.sub(r'\|\s*\|', '|', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        if text and len(text) > 10:
            entries.append(text)
    
    if not entries:
        # Fallback: just get all text with line breaks
        text = soup.get_text(separator='\n', strip=True)
        text = re.sub(r'\n{3,}', '\n\n', text)
        structured = text.strip()
    else:
        structured = '\n---\n'.join(entries)
    
    # Save debug output if enabled
    save_debug_html(structured, profile_name, "experience")
    
    return structured.strip()


def clean_html_for_llm(html: str, profile_name: str = "unknown") -> str:
    """
    Clean HTML to reduce tokens while preserving structure and text.
    DEPRECATED: Use _html_to_structured_text() instead for Groq extraction.
    Kept for backward compatibility with any other callers.
    """
    if not BS4_AVAILABLE:
        html = re.sub(r'\s+', ' ', html)
        html = re.sub(r'<svg[^>]*>.*?</svg>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<img[^>]*>', '', html, flags=re.IGNORECASE)
        return html.strip()
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # 1. Remove non-content tags (script, style, svg, img, iframe, noscript)
    for tag in soup.find_all(['script', 'style', 'svg', 'img', 'iframe', 'noscript']):
        tag.decompose()
        
    # 3. Simplify attributes (keep class/aria-label)
    keep_attrs = {'aria-label', 'class'}
    for tag in soup.find_all(True):
        attrs_to_remove = [attr for attr in tag.attrs if attr not in keep_attrs]
        for attr in attrs_to_remove:
            del tag[attr]
            
        if 'class' in tag.attrs:
            useful_classes = [c for c in tag['class'] if any(x in c for c in ['pvs-', 't-', 'inline-show-more-text', 'visually-hidden'])]
            if useful_classes:
                tag['class'] = useful_classes
    
    for tag in soup.find_all(class_=lambda x: x and any(c in x for c in ['visually-hidden', 'inline-show-more-text', 'artdeco-button__icon'])):
        tag.decompose()

    for tag in soup.find_all(["strong", "span"]):
        txt = tag.get_text()
        if ("skill" in txt.lower() and "+" in txt) or "skills" in txt.lower():
             if "+" in txt or "," in txt:
                parent = tag.find_parent("div", class_="display-flex")
                if parent:
                    parent.decompose()
                else:
                    tag.decompose()

    for sub_comp in soup.select('.pvs-entity__sub-components'):
        for container in sub_comp.find_all('div', class_=lambda c: c and 't-14' in c and 't-normal' in c and 't-black' in c and 't-black--light' not in c):
            if container.find('strong'):
                li_parent = container.find_parent('li')
                if li_parent:
                    li_parent.decompose()
                else:
                    container.decompose()

    for _ in range(3):
        for tag in soup.find_all():
            if len(tag.get_text(strip=True)) == 0 and not tag.find_all():
                tag.decompose()
    
    cleaned = str(soup)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = re.sub(r'>\s*<', '> <', cleaned)
    
    if len(cleaned) < 50 and len(html) > 100:
        logger.warning(f"    ⚠️ HTML Cleaning removed too much! Fallback to basic regex.")
        return re.sub(r'<[^>]+>', ' ', html).strip()[:5000]
    
    save_debug_html(cleaned, profile_name, "experience_html")
    
    return cleaned.strip()


def extract_experiences_with_groq(experience_html: str, max_jobs: int = 3, profile_name: str = "unknown") -> list:
    """
    Extract job experiences from LinkedIn experience section HTML using Groq.

    Returns:
        (list of dicts with job_title, company, employment_type, start_date, end_date), token_count
    """
    client = _get_client()
    if not client:
        logger.warning("⚠️ Groq not available, skipping LLM extraction")
        return [], 0

    original_len = len(experience_html or "")
    structured_text = _html_to_structured_text(experience_html or "", profile_name)
    if not structured_text:
        logger.warning("⚠️ Experience section text is empty after cleaning; skipping Groq extraction")
        return [], 0

    text_len = len(structured_text)
    reduction = round((1 - text_len / original_len) * 100) if original_len > 0 else 0
    logger.debug(f"Experience HTML → text: {original_len:,} → {text_len:,} chars ({reduction}% reduction)")

    return extract_experiences_with_groq_from_text(
        structured_text, max_jobs=max_jobs, profile_name=profile_name
    )


def extract_experiences_with_groq_from_text(
    structured_text: str,
    max_jobs: int = 3,
    profile_name: str = "unknown",
) -> tuple:
    """
    Same as extract_experiences_with_groq but accepts pre-built text (e.g. DB backfill).
    Each job dict: job_title, company, employment_type, start_date, end_date
    """
    client = _get_client()
    if not client:
        logger.warning("⚠️ Groq not available, skipping LLM extraction")
        return [], 0

    structured_text = (structured_text or "").strip()
    if not structured_text:
        logger.warning("⚠️ Experience structured text is empty; skipping Groq extraction")
        return [], 0

    try:
        save_debug_html(structured_text, f"{profile_name}_from_text", "experience")
    except Exception:
        pass

    groq_max = max_jobs + 2

    prompt = f"""Extract the {groq_max} most recent jobs from this LinkedIn experience data.

For each job return:
- company: The employer name ONLY (no employment type). If the line is "Deloitte · Full-time", company is "Deloitte".
- employment_type: The token after " · " on that same line when present
  (e.g. Full-time, Part-time, Contract, Internship, Seasonal, Apprenticeship, Self-employed, Freelance).
  Use "" if the type is not visible.
- job_title: The position/role only. Do not repeat the company name. Do not use bare employment-type words as the title.
- start_date: "Mon YYYY" when month is visible; "YYYY" only if month is truly missing
- end_date: "Mon YYYY", "Present" if current, or "YYYY" when month is not available

Rules:
- Keep subtitle lines in the source (employment type, location, date) — they disambiguate roles.
- Extract ALL jobs found. Match each title to its company and dates — never mix across jobs.
- GROUPED ROLES under one company: each sub-role is its own job entry with the same company (and usually same employment_type line).
- If a line only says "Internship" / "Full-time" under a parent role, fold into the real title (e.g. "... Intern").
- Remove trailing location fragments from company/job_title after comma or dash when clearly a city/region.
- Skip chip lines like "Python, SQL and +3 skills" (skill pills), but DO NOT strip real job descriptions that clarify the role.
- DO NOT return bare employment types (Internship, Part-time, Full-time, Contract) as the sole job_title.
- Order by most recent first.

Validation rules:
- Return at most {groq_max} jobs.
- Every job must be a real role/company pair. If the title or company is missing, return nothing for that row.
- Never use a school, degree, or location as a company or title.
- Never use a company name as the title, and never use the title as the company.
- Never guess missing titles, companies, or dates.
- If a row is ambiguous, omit it rather than guessing.
- Use empty strings for missing optional values, not "N/A".

Return ONLY JSON:
{{"jobs": [{{"company":"...","employment_type":"...","job_title":"...","start_date":"...","end_date":"..."}}]}}

If no jobs: {{"jobs": []}}

Data:
{structured_text}
"""

    try:
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a strict LinkedIn experience extraction engine. "
                        "Output valid JSON only. No prose, no markdown, no code fences, no extra keys. "
                        "Return {\"jobs\": []} if the data is ambiguous or no real jobs are present. "
                        "Never guess missing titles, companies, or dates."
                    )
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=768,
            response_format={"type": "json_object"}
        )
        
        result_text = response.choices[0].message.content.strip()
        _tokens = getattr(response, 'usage', None)
        token_count = _tokens.total_tokens if _tokens else 0
        
        # Parse JSON response
        parsed = parse_groq_json_response(result_text)
        if parsed is None:
            return [], 0
        
        # Handle json_object mode wrapping (Groq may return {"jobs": [...]} instead of [...])
        if isinstance(parsed, dict):
            # Find the array value in the dict
            jobs = None
            for key, val in parsed.items():
                if isinstance(val, list):
                    jobs = val
                    break
            if jobs is None:
                # Groq returned a single job object instead of an array — wrap it
                if parsed.get("job_title") or parsed.get("company"):
                    logger.debug("Groq returned single job object, wrapping in array")
                    jobs = [parsed]
                else:
                    logger.warning("Groq returned JSON object with no job data")
                    return [], 0
        elif isinstance(parsed, list):
            jobs = parsed
        else:
            logger.warning("Groq returned unexpected JSON type")
            return [], 0
        
        valid_jobs = []
        skill_pattern = re.compile(r'.*and \+\d+ skills?$', re.IGNORECASE)
        type_pattern = re.compile(r'^(Internship|Part-time|Full-time|Contract|Seasonal|Temporary|Self-employed|Freelance|Apprenticeship)$', re.IGNORECASE)
        hallucinated_pattern = re.compile(r'^Role\s*\d+$', re.IGNORECASE)
        
        for job in jobs[:groq_max]:
            if not isinstance(job, dict):
                continue
            
            title = (job.get("job_title") or "").strip()
            company = (job.get("company") or "").strip()
            employment_type = (job.get("employment_type") or "").strip()
            start = (job.get("start_date") or "").strip()
            end = (job.get("end_date") or "").strip()
            
            if not title or not company:
                continue

            if _job_entry_exceeds_cloud_limit(job):
                logger.warning(
                    "Dropping oversized Groq experience entry for %s (%s @ %s)",
                    profile_name,
                    title[:60],
                    company[:60],
                )
                continue
            
            # Filter out skill lines
            if skill_pattern.search(title) or ("skills" in title.lower() and "+" in title):
                logger.debug(f"Skipping skill line: {title}")
                continue
            
            # Filter out standalone employment types as titles
            if type_pattern.match(title):
                logger.debug(f"Skipping employment type title: {title}")
                continue

            if _is_bare_employment_type(title):
                logger.debug(f"Skipping bare employment-type title: {title}")
                continue
            
            # Filter out hallucinated role names
            if hallucinated_pattern.match(title):
                logger.debug(f"Skipping hallucinated role name: {title} @ {company}")
                continue

            # Clean up doubled text and trailing location/employment fragments.
            title = _normalize_job_text(title)
            company = _normalize_job_text(company)

            # Strip trailing employment type suffixes that Groq sometimes
            # appends without the · separator
            # (e.g. "UNT College of Engineering Part-time" → "UNT College of Engineering")
            title = strip_seniority_prefixes_from_title(title)

            if _is_company_title_collision(title, company):
                logger.debug(f"Skipping title/company collision: {title} @ {company}")
                continue

            if not title or not company:
                continue
                    
            # Filter out duration strings in Company field
            duration_pattern = re.compile(r'\b(\d+\s+yrs?|\d+\s+mos?|Full-time|Part-time|Contract|Internship)\b', re.IGNORECASE)
            if duration_pattern.search(company) and len(company) < 30:
                 if "·" in company or re.search(r'\d+\s+yrs?', company):
                     logger.warning(f"    ⚠️ Suspicious company name (looks like duration): {company}")
                     company = "" 

            if not title or not company:
                continue

            # Deduplication: only skip if same company AND same title AND same dates
            is_duplicate = False
            for existing in valid_jobs:
                same_company = existing["company"].lower().strip() == company.lower().strip()
                if not same_company:
                    continue
                
                same_title = (
                    existing["job_title"].lower().strip() == title.lower().strip()
                    or existing["job_title"].lower().strip() in title.lower().strip()
                    or title.lower().strip() in existing["job_title"].lower().strip()
                )
                same_dates = (
                    existing.get("start_date", "").lower().strip() == start.lower().strip()
                    and existing.get("end_date", "").lower().strip() == end.lower().strip()
                )
                
                # Only duplicate if same company + same title + same dates
                if same_title and same_dates:
                    logger.debug(f"Skipping duplicate job: {title} @ {company}")
                    is_duplicate = True
                    break
                    
            if is_duplicate:
                continue

            valid_jobs.append({
                "job_title": title,
                "company": company,
                "employment_type": employment_type,
                "start_date": start,
                "end_date": end,
            })
        
        # Sort jobs by date (Most recent first)
        valid_jobs.sort(key=_job_sort_key, reverse=True)

        # Trim to requested max after dedup and sorting
        valid_jobs = valid_jobs[:max_jobs]

        if valid_jobs:
            logger.debug(f"Groq extracted {len(valid_jobs)} job(s)")
            for i, job in enumerate(valid_jobs):
                et = job.get("employment_type") or ""
                logger.debug(
                    f"  Job {i+1}: {job['job_title']} @ {job['company']}"
                    f"{' [' + et + ']' if et else ''} ({job['start_date']} - {job['end_date']})"
                )
        
        return valid_jobs, token_count
        
    except Exception as e:
        logger.error(f"Groq extraction failed: {e}")
        return [], 0


def _job_sort_key(job):
    """Sort key for jobs — most recent first."""
    end_date = job.get('end_date', '')
    end_info = parse_groq_date(end_date)
    if not end_info:
        end_y, end_m = 0, 0
    else:
        end_y = end_info.get('year', 0)
        end_m = end_info.get('month', 0) or 0
        if end_info.get('is_present'):
            end_y = 9999
            end_m = 13

    start_date = job.get('start_date', '')
    start_info = parse_groq_date(start_date)
    if not start_info:
        start_y, start_m = 0, 0
    else:
        start_y = start_info.get('year', 0)
        start_m = start_info.get('month', 0) or 0

    return (end_y, end_m, start_y, start_m)
