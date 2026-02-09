"""
Groq API integration for extracting job experiences from LinkedIn HTML.

Uses Groq's free API tier with Llama 3.1 model:
- 30 requests/minute
- 14.4K requests/day (8b model) or 1K RPD (70b model)
- Model: llama-3.1-8b-instant (default, high volume)
"""

import os
import json
import re
from pathlib import Path
from datetime import datetime
from config import logger

# Try to import groq and BeautifulSoup
try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False
    logger.warning("⚠️ groq not installed. Run: pip install groq")

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

# Configuration
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_ENABLED = os.getenv("USE_GROQ", "true").lower() == "true"
# Model choice: "llama-3.1-8b-instant" (14.4K RPD) or "llama-3.3-70b-versatile" (1K RPD, better quality)
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
# Debug: save HTML to file for inspection
DEBUG_SAVE_HTML = os.getenv("DEBUG_SAVE_HTML", "false").lower() == "true"
DEBUG_HTML_DIR = Path(__file__).parent / "output" / "debug_html"

# Initialize the client once
_client = None

def _get_client():
    """Lazy initialization of Groq client."""
    global _client
    if _client is None and GROQ_AVAILABLE and GROQ_API_KEY:
        try:
            _client = Groq(api_key=GROQ_API_KEY)
            logger.info(f"✓ Groq API initialized (model: {GROQ_MODEL})")
        except Exception as e:
            logger.error(f"Failed to initialize Groq: {e}")
    return _client

def is_groq_available():
    """Check if Groq API is available and configured."""
    return GROQ_AVAILABLE and GROQ_ENABLED and bool(GROQ_API_KEY)

def clean_html_for_llm(html: str, profile_name: str = "unknown") -> str:
    """
    Clean HTML to reduce tokens while preserving structure and text.
    """
    if not BS4_AVAILABLE:
        # Fallback: just strip some obvious things with regex
        html = re.sub(r'\s+', ' ', html)
        html = re.sub(r'<svg[^>]*>.*?</svg>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<img[^>]*>', '', html, flags=re.IGNORECASE)
        return html.strip()
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # 1. Remove non-content tags (script, style, svg, img, iframe, noscript)
    for tag in soup.find_all(['script', 'style', 'svg', 'img', 'iframe', 'noscript']):
        tag.decompose()
        
    # 2. Remove hidden elements? (Be careful not to remove containers)
    # Only remove if it's a leaf node or small text?
    # For now, let's skip aggressive hidden removal if it's causing issues.
    # Instead, let's just remove visually-hidden if it's explicitly marked?
    # (Commented out to be safe for now, as 100% reduction suggests we lost root content)
    # for tag in soup.find_all(attrs={"aria-hidden": "true"}):
    #     tag.decompose()
    
    # 3. Simplify attributes (keep class/aria-label)
    keep_attrs = {'aria-label', 'class'}
    for tag in soup.find_all(True):
        attrs_to_remove = [attr for attr in tag.attrs if attr not in keep_attrs]
        for attr in attrs_to_remove:
            del tag[attr]
            
        # Keep useful layout classes
        if 'class' in tag.attrs:
            # Keep structural classes (pvs-entity, etc) and Typography (t-bold, etc)
            useful_classes = [c for c in tag['class'] if any(x in c for x in ['pvs-', 't-', 'inline-show-more-text', 'visually-hidden'])]
            if useful_classes:
                tag['class'] = useful_classes
    
    # NEW: Remove specific noise classes (Description, Visually Hidden duplicates)
    for tag in soup.find_all(class_=lambda x: x and any(c in x for c in ['visually-hidden', 'inline-show-more-text', 'artdeco-button__icon'])):
        tag.decompose()

    # NEW: Remove Skill sections
    # Look for elements containing "skills" inside typical containers
    for tag in soup.find_all(["strong", "span"]):
        txt = tag.get_text()
        # Case insensitive check for "and +X skill" or "and +X skills"
        if ("skill" in txt.lower() and "+" in txt) or "skills" in txt.lower():
             # Verify it looks like a skill list: usually has comma or "+"
             if "+" in txt or "," in txt:
                # Likely "Python and +X skills" - remove the parent container
                parent = tag.find_parent("div", class_="display-flex")
                if parent:
                    parent.decompose()
                else:
                    tag.decompose()

    # NEW: Remove diamond-icon skill/tag entries from sub-components
    # These appear as <strong> text (e.g., "Teaching", "Problem Solving") inside
    # .t-14.t-normal.t-black divs nested within pvs-entity__sub-components.
    # Groq mistakes these for job titles, wasting extraction slots.
    for sub_comp in soup.select('.pvs-entity__sub-components'):
        for container in sub_comp.find_all('div', class_=lambda c: c and 't-14' in c and 't-normal' in c and 't-black' in c and 't-black--light' not in c):
            if container.find('strong'):
                li_parent = container.find_parent('li')
                if li_parent:
                    li_parent.decompose()
                else:
                    container.decompose()

    # 4. Remove empty tags (recursive) - ONLY IF truly empty (no text, no children)
    # We loop multiple times to clean nested empty tags
    for _ in range(3):
        for tag in soup.find_all():
            if len(tag.get_text(strip=True)) == 0 and not tag.find_all(): # No text, no children
                tag.decompose()
    
    # Get cleaned HTML
    cleaned = str(soup)
    
    # 5. Fix whitespace merging (Prevent "WordWord")
    # Replace newlines/tabs with space
    cleaned = re.sub(r'\s+', ' ', cleaned)
    # Add space between tags instead of removing it
    cleaned = re.sub(r'>\s*<', '> <', cleaned)
    
    # 6. Safety Check: If we reduced to 0, return original (stripped)
    if len(cleaned) < 50 and len(html) > 100:
        logger.warning(f"    ⚠️ HTML Cleaning removed too much! Fallback to basic regex.")
        return re.sub(r'<[^>]+>', ' ', html).strip()[:5000] # Just return text if desperate?
        # Or return less processed HTML
        # return html 
    
    # Save to debug file if enabled
    if DEBUG_SAVE_HTML:
        DEBUG_HTML_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r'[^\w\-]', '_', profile_name)[:50]
        timestamp = datetime.now().strftime("%H%M%S")
        debug_file = DEBUG_HTML_DIR / f"{safe_name}_{timestamp}.html"
        debug_file.write_text(cleaned, encoding='utf-8')
        logger.info(f"    📄 Saved debug HTML to: {debug_file.name}")
    
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
        return []
    
    # Clean HTML to reduce tokens
    cleaned_html = clean_html_for_llm(experience_html, profile_name)
    original_len = len(experience_html)
    cleaned_len = len(cleaned_html)
    reduction = round((1 - cleaned_len / original_len) * 100) if original_len > 0 else 0
    logger.info(f"    📉 HTML cleaned: {original_len:,} → {cleaned_len:,} chars ({reduction}% reduction)")
    
    # Ask Groq for extra jobs to buffer against duplicates/skills being filtered out
    groq_max = max_jobs + 2
    
    # Build a clear, simple prompt
    prompt = f"""I have provided you with the HTML of a LinkedIn Profile's Experience Section. 
Extract their {groq_max} most recent jobs (if any).

For each job, I need:
- company: The employer's name (e.g., "Wells Fargo", "Chewy", "Target")
- job_title: The position/role (e.g., "Software Engineer", "Bank Teller")
- start_date: When they started (format: "Mon YYYY" like "Jan 2024" or just "2024")
- end_date: When they ended (format: "Mon YYYY" or "Present" if current)

IMPORTANT:
- Only extract ACTUAL JOBS where someone was employed
- INCLUDE academic employment like "Teaching Assistant", "Research Assistant", "Lecturer", "Instructor", "Postdoc". These are valid jobs. Even at "University of North Texas".
- INCLUDE nested roles (multiple roles at same company). Treat each nested role as a separate job entry.
- DO NOT exclude a job just because it is at a university.
- DO NOT include education entries (Student, Candidate) unless they have a job title.
- Order by most recent first (current job first)
- IGNORE metadata lines like "Skills: Python, Java...", "Teaching, Computer Science and +1 skill", or anything starting with a diamond symbol. These are NOT job titles.
- DO NOT extract employment types (Internship, Part-time, Full-time, Contract, Seasonal) as the job_title. If the only title available is "Internship", look for the actual role (e.g., "Software Engineer Intern"). If none, skip it.

Return ONLY a JSON array. Do NOT return valid javascript or any other code. Just the raw JSON.
[{{"company": "...", "job_title": "...", "start_date": "...", "end_date": "..."}}]

If no jobs found, return: []

HTML:
{cleaned_html}
"""

    try:
        response = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=1024
        )
        
        result_text = response.choices[0].message.content.strip()
        
        # Extract JSON from response (handle markdown code blocks)
        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0].strip()
        elif "```" in result_text:
            result_text = result_text.split("```")[1].split("```")[0].strip()
        
        # Clean up common JS artifacts if model hallucinates code
        if result_text.startswith("const ") or result_text.startswith("let ") or result_text.startswith("var "):
            # Try to find the array bracket
            start_idx = result_text.find("[")
            if start_idx != -1:
                result_text = result_text[start_idx:]
        
        # Parse JSON
        try:
            jobs = json.loads(result_text)
        except json.JSONDecodeError:
            # Fallback: try to find array in text
            import re
            match = re.search(r'\[.*\]', result_text, re.DOTALL)
            if match:
                try:
                    jobs = json.loads(match.group(0))
                except:
                    logger.warning(f"⚠️ Failed to parse extracted JSON: {result_text[:100]}...")
                    return []
            else:
                logger.warning(f"⚠️ Groq returned invalid JSON: {result_text[:100]}...")
                return []
        
        if not isinstance(jobs, list):
            logger.warning("⚠️ Groq returned non-list response")
            return []
        
        valid_jobs = []
        import re
        skill_pattern = re.compile(r'.*and \+\d+ skills?$', re.IGNORECASE)
        # Regex for standalone employment types (case insensitive)
        type_pattern = re.compile(r'^(Internship|Part-time|Full-time|Contract|Seasonal|Temporary|Self-employed|Freelance)$', re.IGNORECASE)
        
        for job in jobs[:groq_max]:
            if isinstance(job, dict) and (job.get("job_title") or job.get("company")):
                title = job.get("job_title", "").strip()
                company = job.get("company", "").strip()
                
                # Filter out obvious skill lines
                if skill_pattern.search(title) or "skills" in title.lower() and "+" in title:
                    logger.info(f"    🗑️ Skipping skill line: {title}")
                    continue
                
                # Filter out standalone employment types as titles
                if type_pattern.match(title):
                    logger.info(f"    🗑️ Skipping employment type title: {title}")
                    continue

                # Clean up doubled text (e.g. "EngineerEngineer" -> "Engineer")
                # Also handles "Engineer Engineer"
                def clean_doubled(text):
                    if not text or len(text) < 4: return text
                    # Exact duplication "WordWord"
                    if len(text) % 2 == 0:
                        mid = len(text) // 2
                        if text[:mid] == text[mid:]:
                            return text[:mid]
                    # Duplication with space "Word Word"
                    # Check if text consists of two identical halves separated by space
                    parts = text.split()
                    if len(parts) >= 2 and len(parts) % 2 == 0:
                        half = len(parts) // 2
                        if parts[:half] == parts[half:]:
                           return " ".join(parts[:half])
                    
                    # Heuristic: Check if end of string repeats start of string
                    # e.g. "Senior Network EngineerSenior Network Engineer"
                    # We already checked exact half split.
                    return text

                title = clean_doubled(title)
                company = clean_doubled(company)
                        
                # Filter out duration strings in Company field
                duration_pattern = re.compile(r'\b(\d+\s+yrs?|\d+\s+mos?|Full-time|Part-time|Contract|Internship)\b', re.IGNORECASE)
                if duration_pattern.search(company) and len(company) < 30:
                     if "·" in company or re.search(r'\d+\s+yrs?', company):
                         logger.warning(f"    ⚠️ Suspicious company name (looks like duration): {company}")
                         company = "" 

                # Deduplication check against existing valid_jobs
                is_duplicate = False
                for existing in valid_jobs:
                    # If Company matches AND (Title matches OR Start Date matches)
                    if existing["company"].lower() == company.lower():
                        if existing["job_title"].lower() == title.lower() or \
                           (existing["start_date"] == job.get("start_date", "").strip() and job.get("start_date")):
                            logger.info(f"    🗑️ Skipping duplicate job: {title} @ {company}")
                            is_duplicate = True
                            break
                            
                if is_duplicate: continue

                valid_jobs.append({
                    "job_title": title,
                    "company": company,
                    "start_date": job.get("start_date", "").strip(),
                    "end_date": job.get("end_date", "").strip()
                })
        
        # Sort jobs by date (Most recent first) to fix ordering issues
        def job_sort_key(job):
            # Parse End Date
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

            # Parse Start Date
            start_date = job.get('start_date', '')
            start_info = parse_groq_date(start_date)
            if not start_info:
                start_y, start_m = 0, 0
            else:
                start_y = start_info.get('year', 0)
                start_m = start_info.get('month', 0) or 0

            return (end_y, end_m, start_y, start_m)

        valid_jobs.sort(key=job_sort_key, reverse=True)

        # Trim to requested max after dedup and sorting
        valid_jobs = valid_jobs[:max_jobs]

        if valid_jobs:
            logger.info(f"    ✓ Groq extracted {len(valid_jobs)} job(s)")
            for i, job in enumerate(valid_jobs):
                logger.info(f"      Job {i+1}: {job['job_title']} @ {job['company']} ({job['start_date']} - {job['end_date']})")
        
        return valid_jobs
        
    except json.JSONDecodeError as e:
        logger.warning(f"⚠️ Groq returned invalid JSON: {e}")
        logger.warning(f"    Raw text: {result_text[:200]}...")
        return []
    except Exception as e:
        logger.error(f"⚠️ Groq extraction failed: {e}")
        return []


def parse_groq_date(date_str: str) -> dict:
    """
    Parse a date string like "Oct 2024" or "Present" into the format used by the scraper.
    
    Returns:
        dict with keys: year (int), month (int or None), is_present (bool)
    """
    if not date_str:
        return None
    
    date_str = date_str.strip()
    
    # Check for "Present"
    if date_str.lower() == "present":
        return {"year": 9999, "month": 12, "is_present": True}
    
    # Month mapping
    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }
    
    # Try "Mon YYYY" format
    match = re.match(r'([A-Za-z]{3})\s*(\d{4})', date_str)
    if match:
        month_str, year_str = match.groups()
        month = month_map.get(month_str.lower(), None)
        return {"year": int(year_str), "month": month, "is_present": False}
    
    # Try "YYYY" format
    match = re.match(r'^(\d{4})$', date_str)
    if match:
        return {"year": int(match.group(1)), "month": None, "is_present": False}
    
    return None
