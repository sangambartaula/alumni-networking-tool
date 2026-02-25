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
    
    Args:
        experience_html: The innerHTML of the experience section
        max_jobs: Maximum number of jobs to extract (default 3)
        profile_name: Name of the profile (for debug file naming)
    
    Returns:
        List of dicts with keys: job_title, company, start_date, end_date
        Returns empty list if extraction fails
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
    
    # Ask Groq for extra jobs to buffer against duplicates being filtered out
    groq_max = max_jobs + 2
    
    # Build a clear, simple prompt (no HTML instructions needed anymore)
    prompt = f"""Extract the {groq_max} most recent jobs from this LinkedIn experience data.

For each job return:
- company: The employer name
- job_title: The position/role  
- start_date: Format "Mon YYYY" (e.g. "Jan 2024") or just "YYYY"
- end_date: Format "Mon YYYY" or "Present" if current

Rules:
- Extract ALL jobs found, even if there is only 1
- Each job has a title, a company, and a date range. They appear near each other in the text. CAREFULLY match each title to its own company and dates — never mix a title from one job with a company from a different job.
- A company name often has " · " followed by an employment type (e.g. "CloudFactory · Full-time"). Extract ONLY the text before " · " as the company name.
- INCLUDE roles like "Member", "Volunteer", "Fellow", "Extern" — these are valid job titles
- INCLUDE academic jobs (Teaching Assistant, Research Assistant, etc.)
- INCLUDE multiple roles at the same company as separate entries with their own dates
- If someone held an internship then got hired full-time at the same company, include BOTH as separate entries
- If a sub-role only says "Internship" or "Full-time" but has a parent title, append "Intern" to the parent title (e.g. "Assistant Project Manager Intern")
- DO NOT invent role names. Never output "Role 1", "Role 2", etc.
- DO NOT return bare employment types (Internship, Part-time, Full-time) as the job_title
- IGNORE skill tags, metadata, and duration strings like "1 yr 5 mos"
- Order by most recent first

Return ONLY a JSON object with a "jobs" key:
{{"jobs": [{{"company":"...","job_title":"...","start_date":"...","end_date":"..."}}]}}

If no jobs found return: {{"jobs": []}}

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
                        "You are a data extraction engine. You must output valid JSON only. "
                        "No explanations, no code, no markdown, no JavaScript. "
                        "If extraction fails, return [] exactly."
                    )
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=1024,
            response_format={"type": "json_object"}
        )
        
        result_text = response.choices[0].message.content.strip()
        _tokens = getattr(response, 'usage', None)
        token_count = _tokens.total_tokens if _tokens else 0
        
        # Parse JSON response
        parsed = parse_groq_json_response(result_text)
        if parsed is None:
            return []
        
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
                    return []
        elif isinstance(parsed, list):
            jobs = parsed
        else:
            logger.warning("Groq returned unexpected JSON type")
            return []
        
        valid_jobs = []
        skill_pattern = re.compile(r'.*and \+\d+ skills?$', re.IGNORECASE)
        type_pattern = re.compile(r'^(Internship|Part-time|Full-time|Contract|Seasonal|Temporary|Self-employed|Freelance)$', re.IGNORECASE)
        hallucinated_pattern = re.compile(r'^Role\s*\d+$', re.IGNORECASE)
        
        for job in jobs[:groq_max]:
            if not isinstance(job, dict):
                continue
            
            title = (job.get("job_title") or "").strip()
            company = (job.get("company") or "").strip()
            start = (job.get("start_date") or "").strip()
            end = (job.get("end_date") or "").strip()
            
            if not title or not company:
                continue
            
            # Filter out skill lines
            if skill_pattern.search(title) or ("skills" in title.lower() and "+" in title):
                logger.debug(f"Skipping skill line: {title}")
                continue
            
            # Filter out standalone employment types as titles
            if type_pattern.match(title):
                logger.debug(f"Skipping employment type title: {title}")
                continue
            
            # Filter out hallucinated role names
            if hallucinated_pattern.match(title):
                logger.debug(f"Skipping hallucinated role name: {title} @ {company}")
                continue

            # Clean up doubled text (e.g. "EngineerEngineer" -> "Engineer")
            title = _clean_doubled(title)
            company = _clean_doubled(company)
                    
            # Filter out duration strings in Company field
            duration_pattern = re.compile(r'\b(\d+\s+yrs?|\d+\s+mos?|Full-time|Part-time|Contract|Internship)\b', re.IGNORECASE)
            if duration_pattern.search(company) and len(company) < 30:
                 if "·" in company or re.search(r'\d+\s+yrs?', company):
                     logger.warning(f"    ⚠️ Suspicious company name (looks like duration): {company}")
                     company = "" 

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
                "start_date": start,
                "end_date": end
            })
        
        # Sort jobs by date (Most recent first)
        valid_jobs.sort(key=_job_sort_key, reverse=True)

        # Trim to requested max after dedup and sorting
        valid_jobs = valid_jobs[:max_jobs]

        if valid_jobs:
            logger.debug(f"Groq extracted {len(valid_jobs)} job(s)")
            for i, job in enumerate(valid_jobs):
                logger.debug(f"  Job {i+1}: {job['job_title']} @ {job['company']} ({job['start_date']} - {job['end_date']})")
        
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
