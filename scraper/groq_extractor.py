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
    Clean HTML to reduce tokens while preserving important content.
    
    Removes:
    - All attributes except aria-label (sometimes has useful text)
    - Script and style tags
    - SVG and image elements
    - Empty elements
    - Excessive whitespace
    
    This can reduce HTML size by 60-80%, significantly cutting token usage.
    """
    if not BS4_AVAILABLE:
        # Fallback: just strip some obvious things with regex
        html = re.sub(r'\s+', ' ', html)
        html = re.sub(r'<svg[^>]*>.*?</svg>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<img[^>]*>', '', html, flags=re.IGNORECASE)
        return html.strip()
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # Remove script, style, svg, img elements
    for tag in soup.find_all(['script', 'style', 'svg', 'img', 'button', 'iframe']):
        tag.decompose()
    
    # Remove all attributes except a few useful ones
    keep_attrs = {'aria-label', 'class'}
    for tag in soup.find_all(True):
        attrs_to_remove = [attr for attr in tag.attrs if attr not in keep_attrs]
        for attr in attrs_to_remove:
            del tag[attr]
        # Simplify classes - keep only potentially useful ones
        if 'class' in tag.attrs:
            useful_classes = [c for c in tag['class'] if any(x in c for x in ['t-bold', 't-14', 't-normal', 'pvs-entity'])]
            if useful_classes:
                tag['class'] = useful_classes
            else:
                del tag['class']
    
    # Get cleaned HTML
    cleaned = str(soup)
    
    # Collapse whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = re.sub(r'>\s+<', '><', cleaned)
    
    # Save to debug file if enabled
    if DEBUG_SAVE_HTML:
        DEBUG_HTML_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r'[^\w\-]', '_', profile_name)[:50]
        debug_file = DEBUG_HTML_DIR / f"{safe_name}.html"
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
    
    # Build a clear, simple prompt
    prompt = f"""I have provided you with the HTML of a LinkedIn Profile's Experience Section. 
Extract their {max_jobs} most recent jobs (if any).

For each job, I need:
- company: The employer's name (e.g., "Wells Fargo", "Chewy", "Target")
- job_title: The position/role (e.g., "Software Engineer", "Bank Teller")
- start_date: When they started (format: "Mon YYYY" like "Jan 2024" or just "2024")
- end_date: When they ended (format: "Mon YYYY" or "Present" if current)

IMPORTANT:
- Only extract ACTUAL JOBS where someone was employed
- DO NOT include education or schools unless their title matches a real job. Not Student or something similar. Certifications, projects, etc. aren't included but Lecturers, TA's etc. are.
- Order by most recent first (current job first)

Return ONLY a JSON array, no other text:
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
        
        # Parse JSON
        jobs = json.loads(result_text)
        
        if not isinstance(jobs, list):
            logger.warning("⚠️ Groq returned non-list response")
            return []
        
        # Validate and clean results
        valid_jobs = []
        for job in jobs[:max_jobs]:
            if isinstance(job, dict) and (job.get("job_title") or job.get("company")):
                valid_jobs.append({
                    "job_title": job.get("job_title", "").strip(),
                    "company": job.get("company", "").strip(),
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
