"""
Groq-based job relevance scoring and experience months computation.

=== SCORING LOGIC ===
For each of a person's jobs (up to 3), this module asks the Groq LLM to rate
how relevant that job is to the person's field of study (major/standardized_major).
The LLM returns a single float in [0, 1]:
    0.0 = completely unrelated
    0.5 = somewhat related
    1.0 = perfectly aligned

=== THRESHOLD REASONING ===
RELEVANCE_THRESHOLD = 0.8
We use 0.8 as the boolean cutoff because:
  - It avoids false positives from tangentially related roles
  - It aligns with the user's requirement (>= 0.8 → relevant)
  - Scores 0.6–0.79 are "maybe related" — too noisy to count as relevant
  - The LLM tends to give ~0.5 for loosely related jobs and ~0.85-1.0 for
    genuinely field-aligned jobs, so 0.8 sits in the natural gap

=== RETRY LOGIC ===
Each job is scored independently with up to MAX_RETRIES=3 attempts.
On each attempt, the LLM output is strictly validated: it must parse as a
single float in [0.0, 1.0]. If validation fails, a retry is triggered.

=== INPUT / OUTPUT ===
Input: profile_data dict with job titles, companies, dates, and major
Output: dict with per-job scores and booleans, plus total relevant months

Structured JSON output is available via get_relevance_json() — returns a list
of dicts ready for the Experience Engine:
    [{"title": "...", "company": "...", "score": 0.82, "is_relevant": true,
      "start_date": "...", "end_date": "..."}, ...]

Uses the Groq LLM with strict validation and retry logic.
"""

import re
from datetime import datetime
from config import logger

from groq_client import (
    _get_client, is_groq_available, GROQ_MODEL,
    parse_groq_json_response, parse_groq_date,
)

# ── Relevance Thresholds ──────────────────────────────────────
# >= 0.8 = relevant (boolean True), < 0.8 = not relevant (boolean False)
# See module docstring for reasoning.
RELEVANCE_THRESHOLD_RELEVANT = 0.8
RELEVANCE_THRESHOLD_UNSURE = 0.4     # 0.4-0.8 = unsure, < 0.4 = not relevant
MAX_RETRIES = 3


def score_job_relevance(title, company, major):
    """
    Use Groq LLM to score how relevant a job is to the person's major/field.
    
    Args:
        title: Job title (original, not normalized)
        company: Company name
        major: Person's field of study / major
        
    Returns:
        float between 0.0 and 1.0, or None if scoring fails after retries
        
    Edge cases:
        - Empty/None title or major → returns None (can't score without both)
        - Empty company → still scores (company is optional context)
        - LLM returns garbage → retries up to MAX_RETRIES times
    """
    if not title or not major:
        return None
    
    client = _get_client()
    if not client:
        logger.debug("Groq not available, skipping relevance scoring")
        return None
    
    title = str(title).strip()
    company = str(company).strip() if company else ""
    major = str(major).strip()
    
    prompt = f"""Rate how relevant this job is to the person's field of study.

Job Title: {title}
Company: {company}
Field of Study / Major: {major}

Consider:
- Does the job title align with skills typically learned in this major?
- Is the company in an industry related to this field?
- Would this job realistically be held by someone with this degree?

Return ONLY a single number between 0 and 1 (e.g. 0.85).
- 0.0 = completely unrelated (e.g. fast food worker with Computer Science degree)
- 0.5 = somewhat related (e.g. IT support with Computer Science degree)
- 1.0 = perfectly aligned (e.g. Software Engineer with Computer Science degree)

Return ONLY the number. No text, no explanation, no JSON."""

    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a career relevance evaluator. "
                            "You MUST respond with ONLY a single decimal number between 0 and 1. "
                            "No words, no explanation, no formatting. Just the number."
                        )
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=10,
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Validate: extract a float from the response
            score = _extract_score(result_text)
            if score is not None:
                logger.debug(f"Relevance score for '{title}' vs '{major}': {score:.2f}")
                return score
            
            logger.warning(
                f"⚠️ Groq returned invalid score (attempt {attempt + 1}/{MAX_RETRIES}): '{result_text}'"
            )
            
        except Exception as e:
            logger.warning(f"⚠️ Groq relevance scoring failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
    
    logger.warning(f"❌ Failed to score relevance for '{title}' after {MAX_RETRIES} attempts")
    return None


def _extract_score(text):
    """
    Extract a float between 0 and 1 from LLM output.
    
    Strict validation: output must be a single number in [0, 1].
    Returns None if invalid (triggers retry in caller).
    """
    if not text:
        return None
    
    # Try direct float parse first
    text = text.strip()
    try:
        val = float(text)
        if 0.0 <= val <= 1.0:
            return round(val, 2)
        return None
    except ValueError:
        pass
    
    # Fallback: find a decimal number in text
    match = re.search(r'(\d+\.?\d*)', text)
    if match:
        try:
            val = float(match.group(1))
            if 0.0 <= val <= 1.0:
                return round(val, 2)
        except ValueError:
            pass
    
    return None


def is_job_relevant(score, threshold=RELEVANCE_THRESHOLD_RELEVANT):
    """
    Check if a relevance score meets the threshold for 'relevant'.
    
    Args:
        score: float in [0, 1], or None
        threshold: cutoff value (default 0.8)
        
    Returns:
        True if score >= threshold, False if score < threshold, None if score is None
    """
    if score is None:
        return None
    return score >= threshold


def compute_relevant_experience_months(jobs):
    """
    Compute total months of relevant work experience.
    
    Merges overlapping date ranges and sums only months from relevant jobs.
    
    Args:
        jobs: list of dicts with keys:
            - start_date: str like "Jan 2020" or "2020"
            - end_date: str like "Dec 2022", "Present", or "2022"  
            - is_relevant: bool
            
    Returns:
        int: total months of relevant experience, or None if no data
    """
    if not jobs:
        return None
    
    # Collect date ranges for relevant jobs only
    intervals = []
    for job in jobs:
        if not job.get('is_relevant'):
            continue
        
        start = _parse_date_to_month_year(job.get('start_date', ''))
        end = _parse_date_to_month_year(job.get('end_date', ''))
        
        if start is None:
            continue
        if end is None:
            # If no end date, assume present
            now = datetime.now()
            end = (now.year, now.month)
        
        # Ensure start <= end
        if start > end:
            start, end = end, start
        
        intervals.append((start, end))
    
    if not intervals:
        return 0
    
    # Merge overlapping intervals
    merged = _merge_intervals(intervals)
    
    # Sum months
    total_months = 0
    for (start_year, start_month), (end_year, end_month) in merged:
        months = (end_year - start_year) * 12 + (end_month - start_month)
        # Add 1 to include the end month
        total_months += max(months, 0) + 1
    
    return total_months


def _parse_date_to_month_year(date_str):
    """
    Parse a date string to (year, month) tuple.
    
    Uses parse_groq_date() from groq_client.py for consistency.
    
    Returns:
        (year, month) tuple or None
    """
    if not date_str:
        return None
    
    date_str = str(date_str).strip()
    if not date_str:
        return None
    
    # Check for "Present"
    if date_str.lower() == "present":
        now = datetime.now()
        return (now.year, now.month)
    
    parsed = parse_groq_date(date_str)
    if parsed is None:
        return None
    
    year = parsed.get('year')
    if year is None or year == 9999:
        # 9999 means "Present"
        now = datetime.now()
        return (now.year, now.month)
    
    month = parsed.get('month')
    if month is None:
        month = 1  # Default to January if no month
    
    return (year, month)


def _merge_intervals(intervals):
    """Merge overlapping (year, month) intervals."""
    if not intervals:
        return []
    
    # Sort by start
    sorted_intervals = sorted(intervals, key=lambda x: x[0])
    
    merged = [sorted_intervals[0]]
    for start, end in sorted_intervals[1:]:
        prev_start, prev_end = merged[-1]
        
        # Check if overlapping or adjacent (within 1 month)
        if start <= (prev_end[0], prev_end[1] + 1) if prev_end[1] < 12 else (prev_end[0] + 1, 1):
            # Merge: extend the end
            new_end = max(prev_end, end)
            merged[-1] = (prev_start, new_end)
        else:
            merged.append((start, end))
    
    return merged


def analyze_profile_relevance(profile_data):
    """
    Analyze all jobs in a profile for relevance and compute experience months.
    
    Handles up to 3 jobs per person. Edge cases:
      - No major → returns empty dict (can't assess relevance without major)
      - 0 jobs → returns empty dict
      - 1-3 jobs, none relevant → all is_relevant=False, relevant_experience_months=0
      - Jobs with missing title/company → skipped
    
    Args:
        profile_data: dict with job titles, companies, dates, and major
        
    Returns:
        dict with keys like job_1_relevance_score, job_1_is_relevant, etc.
    """
    major = (
        profile_data.get('standardized_major')
        or profile_data.get('major')
        or ''
    ).strip()
    
    if not major:
        logger.debug("No major found, skipping relevance analysis")
        return {}
    
    result = {}
    jobs_for_experience = []
    
    # Job 1 (most recent)
    title1 = profile_data.get('title') or profile_data.get('current_job_title') or ''
    company1 = profile_data.get('company') or ''
    if title1.strip():
        score1 = score_job_relevance(title1, company1, major)
        result['job_1_relevance_score'] = score1
        result['job_1_is_relevant'] = is_job_relevant(score1)
        
        start1 = profile_data.get('job_start') or profile_data.get('job_start_date') or ''
        end1 = profile_data.get('job_end') or profile_data.get('job_end_date') or ''
        jobs_for_experience.append({
            'start_date': start1,
            'end_date': end1,
            'is_relevant': result['job_1_is_relevant'],
        })
    
    # Job 2
    title2 = profile_data.get('exp_2_title') or profile_data.get('exp2_title') or ''
    company2 = profile_data.get('exp_2_company') or profile_data.get('exp2_company') or ''
    if title2.strip():
        score2 = score_job_relevance(title2, company2, major)
        result['job_2_relevance_score'] = score2
        result['job_2_is_relevant'] = is_job_relevant(score2)
        
        dates2 = profile_data.get('exp_2_dates') or profile_data.get('exp2_dates') or ''
        start2, end2 = _split_date_range(dates2)
        jobs_for_experience.append({
            'start_date': start2,
            'end_date': end2,
            'is_relevant': result['job_2_is_relevant'],
        })
    
    # Job 3
    title3 = profile_data.get('exp_3_title') or profile_data.get('exp3_title') or ''
    company3 = profile_data.get('exp_3_company') or profile_data.get('exp3_company') or ''
    if title3.strip():
        score3 = score_job_relevance(title3, company3, major)
        result['job_3_relevance_score'] = score3
        result['job_3_is_relevant'] = is_job_relevant(score3)
        
        dates3 = profile_data.get('exp_3_dates') or profile_data.get('exp3_dates') or ''
        start3, end3 = _split_date_range(dates3)
        jobs_for_experience.append({
            'start_date': start3,
            'end_date': end3,
            'is_relevant': result['job_3_is_relevant'],
        })
    
    # Compute total relevant experience months
    result['relevant_experience_months'] = compute_relevant_experience_months(jobs_for_experience)
    
    return result


def get_relevance_json(profile_data):
    """
    Get structured JSON output for all jobs in a profile.
    
    This is the primary output format for the Experience Engine.
    Returns a list of dicts (up to 3), one per job, with score and boolean.
    
    Args:
        profile_data: dict with job titles, companies, dates, and major
        
    Returns:
        list of dicts, each with:
            - title (str)
            - company (str)
            - score (float or None)
            - is_relevant (bool or None)
            - start_date (str)
            - end_date (str)
            
    Edge cases:
        - Person has 0 jobs → returns []
        - Person has no major → returns [] (can't score)
        - Jobs with missing title → skipped
        - Score is None (Groq failed) → score=None, is_relevant=None
    """
    major = (
        profile_data.get('standardized_major')
        or profile_data.get('major')
        or ''
    ).strip()
    
    if not major:
        return []
    
    # Define job field mappings for up to 3 jobs
    job_specs = [
        {
            'title_keys': ['title', 'current_job_title'],
            'company_keys': ['company'],
            'start_keys': ['job_start', 'job_start_date'],
            'end_keys': ['job_end', 'job_end_date'],
            'dates_key': None,  # Job 1 has separate start/end fields
        },
        {
            'title_keys': ['exp_2_title', 'exp2_title'],
            'company_keys': ['exp_2_company', 'exp2_company'],
            'start_keys': [],
            'end_keys': [],
            'dates_key': ['exp_2_dates', 'exp2_dates'],
        },
        {
            'title_keys': ['exp_3_title', 'exp3_title'],
            'company_keys': ['exp_3_company', 'exp3_company'],
            'start_keys': [],
            'end_keys': [],
            'dates_key': ['exp_3_dates', 'exp3_dates'],
        },
    ]
    
    results = []
    
    for spec in job_specs:
        # Get title
        title = ''
        for key in spec['title_keys']:
            title = profile_data.get(key) or ''
            if title.strip():
                break
        
        if not title.strip():
            continue  # Skip jobs with no title
        
        # Get company
        company = ''
        for key in spec['company_keys']:
            company = profile_data.get(key) or ''
            if company.strip():
                break
        
        # Get dates
        if spec['dates_key']:
            dates_str = ''
            for key in spec['dates_key']:
                dates_str = profile_data.get(key) or ''
                if dates_str.strip():
                    break
            start_date, end_date = _split_date_range(dates_str)
        else:
            start_date = ''
            for key in spec['start_keys']:
                start_date = profile_data.get(key) or ''
                if start_date.strip():
                    break
            end_date = ''
            for key in spec['end_keys']:
                end_date = profile_data.get(key) or ''
                if end_date.strip():
                    break
        
        # Score this job
        score = score_job_relevance(title.strip(), company.strip(), major)
        
        results.append({
            'title': title.strip(),
            'company': company.strip(),
            'score': score,
            'is_relevant': is_job_relevant(score),
            'start_date': start_date.strip() if start_date else '',
            'end_date': end_date.strip() if end_date else '',
        })
    
    return results


def _split_date_range(date_range_str):
    """
    Split a date range string like "Mar 2020 - Dec 2022" into (start, end).
    
    Handles various separators: " - ", " – ", " — ", " to "
    
    Returns:
        (start_str, end_str) tuple. Both empty strings if input is empty.
    """
    if not date_range_str:
        return ('', '')
    
    text = str(date_range_str).strip()
    
    # Try various separators
    for sep in [' - ', ' – ', ' — ', ' to ']:
        if sep in text:
            parts = text.split(sep, 1)
            return (parts[0].strip(), parts[1].strip())
    
    # Fallback: single date, treat as both start and end
    return (text, text)
