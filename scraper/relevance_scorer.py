"""
Groq-based job relevance scoring and experience months computation.

=== CONTEXT ===
This tool serves College of Engineering alumni. The main goal is to
count **professional post-college career experience** while filtering
obvious non-career / high-school-level jobs (cashier, retail associate,
warehouse picker, fast food, etc.).

Any legitimate professional career job should be counted as relevant —
not just STEM/engineering roles. Directors, managers, analysts, designers,
architects, consultants, etc. are ALL relevant. Engineering and STEM titles
get a slight boost but are NOT required for relevance.

=== SCORING LOGIC ===
Order: **junk check → LLM (non-junk only) → additive boosts → floors → clamp [0,1]**

1) **Junk titles** (cashier, retail associate, etc.): score **0.10**, no LLM.
   Professional-sounding titles are never treated as junk.

2) **LLM**: Groq returns the base career-level score in [0, 1]. If Groq is
   unavailable or fails, there is no base score unless a **floor** applies.

3) **Heuristic boosts** (only when LLM returned a score):
   - Engineering/STEM-style title → **+0.05**
   - Non-empty major matches STEM/engineering patterns → **+0.05**

4) **Floors** (minimum on the boosted score):
   - TA / RA / grad assistant / peer tutor patterns → minimum **0.65**
   - Engineering-style title with **no major** stored → minimum **0.60**
   - Professional title (director, manager, VP, etc.) → minimum **0.60**

5) **Clamp** the final value to **[0, 1]**.

=== THRESHOLD ===
RELEVANCE_THRESHOLD_RELEVANT = 0.45
is_relevant is True when score >= 0.45. This keeps all professional-level
careers while excluding obvious HS-level service jobs (typically <= 0.15).

=== RETRY LOGIC ===
Each job is scored independently with up to MAX_RETRIES=3 attempts.

=== INPUT / OUTPUT ===
Structured JSON via get_relevance_json() for the Experience Engine:
    [{"title": "...", "company": "...", "score": 0.82, "is_relevant": true,
      "start_date": "...", "end_date": "..."}, ...]

Uses the Groq LLM with strict validation and retry logic.
"""

import re
from datetime import datetime
from config import logger

from groq_client import _get_client, is_groq_available, GROQ_MODEL, parse_groq_date, apply_groq_retry_delay

apply_groq_retry_delay()

# ── Relevance Thresholds ──────────────────────────────────────
# >= 0.45 = relevant (boolean True), < 0.45 = not relevant (boolean False)
RELEVANCE_THRESHOLD_RELEVANT = 0.45
RELEVANCE_THRESHOLD_UNSURE = 0.25  # narrative band
MAX_RETRIES = 3

# LLM context when major is absent from the profile
DEFAULT_MAJOR_CONTEXT_FOR_LLM = (
    "Not listed — assume a university graduate; any professional-level "
    "career counts as relevant. Only exclude obvious non-career service jobs."
)

_JUNK_TITLE = re.compile(
    r'\b('
    r'cashier|barista|(?:fast[\s-]?food|line)\s+cook|fry\s+cook'
    r'|server\b|waiter|waitress|host(?:ess)?|busboy|bartender'
    r'|dishwasher|janitor|custodian|housekeeper|landscaper'
    r'|security\s+guard|parking\s+attendant'
    r'|retail\s+associate|stock(?:er| clerk)?|stocker\b|merchandiser'
    r'|warehouse\s+associate|package\s+handler|picker\b|packer\b'
    r'|forklift|material\s+handler'
    r'|delivery\s+driver|(?:uber|lyft|doordash|grubhub)\s+driver'
    r'|crew\s+member|sandwich\s+artist'
    r'|personal\s+shopper|front\s+desk\s+staff'
    r'|courtesy\s+clerk|bagger\b'
    r')\b',
    re.I,
)

# Professional-level titles that are always relevant regardless of major
_PROFESSIONAL_TITLE = re.compile(
    r'\b('
    r'director|manager|president|vice\s+president|\bvp\b'
    r'|\bceo\b|\bcto\b|\bcoo\b|\bcfo\b|\bcmo\b|\bcio\b'
    r'|architect|consultant|advisor|analyst|strategist'
    r'|designer|product\s+(?:manager|owner|designer)'
    r'|project\s+manager|program\s+manager|scrum\s+master'
    r'|recruiter|talent|human\s+resources|\bhr\b'
    r'|accountant|auditor|controller|financial'
    r'|attorney|lawyer|counsel|paralegal'
    r'|nurse|physician|pharmacist|therapist|clinician'
    r'|professor|lecturer|instructor|teacher'
    r'|founder|co-?founder|entrepreneur|owner'
    r'|solutions?\s+(?:architect|engineer|consultant)'
    r'|operations|procurement|supply\s+chain|logistics'
    r')\b',
    re.I,
)

# Technical / engineering-ish titles (College of Engineering oriented)
_ENGINEERING_TITLE = re.compile(
    r'\b('
    r'engineer(?:ing)?'
    r'|developer|programmer'
    r'|\b(?:swe|sde|sre)\b|devops|technologist'
    r'|(?:software|systems|network|security|cloud|data|solutions|platform|'
    r'infrastructure|machine[\s-]learning|ml)\s+architect'
    r'|(?:data|research|applied|staff)\s+scientist'
    r'|research\s+engineer'
    r'|(?:hardware|firmware|embedded|pcb|asic)\b'
    r'|(?:qa|quality|validation|test(?:ing)?)\s+engineer'
    r'|automation\s+engineer'
    r'|(?:field|applications|manufacturing|process|plant|sales)\s+engineer'
    r'|(?:mechanical|electrical|civil|chemical|software|systems)\s+engineer'
    r'|(?:it|technical|technology)\s+(?:analyst|specialist|consultant)'
    r'|lab\s+(?:technician|engineer|tech)|r\s*&\s*d\b'
    r')\b',
    re.I,
)

_STEM_MAJOR = re.compile(
    r'\b('
    r'engineering|engineer\b'
    r'|computer|software|electrical|mechanical|civil|chemical'
    r'|aerospace|biomedical|materials|industrial|systems'
    r'|physics|mathematics|statistics|informatics|technology\b'
    r'|data\s+science|\bcs\b|\bce\b|\bece\b'
    r')\b',
    re.I,
)

_TA_TITLE = re.compile(
    r'\b('
    r'teaching\s+assistant|research\s+assistant|graduate\s+assistant'
    r'|instructional\s+assistant|academic\s+assistant|learning\s+assistant'
    r'|(?:under)?graduate\s+researcher|student\s+researcher'
    r'|peer\s+tutor|\bta\b|\bra\b'
    r')\b',
    re.I,
)

_SCORE_CAP_JUNK = 0.10
_FLOOR_TA = 0.65
_FLOOR_ENG_NO_MAJOR_CONTEXT = 0.60
_FLOOR_PROFESSIONAL = 0.60
_BOOST_ENGINEERING_TITLE = 0.05
_BOOST_STEM_MAJOR_MATCH = 0.05


def _is_obviously_non_career_title(title: str) -> bool:
    t = (title or "").strip()
    if not t:
        return False
    if _ENGINEERING_TITLE.search(t):
        return False
    if _PROFESSIONAL_TITLE.search(t):
        return False
    return bool(_JUNK_TITLE.search(t))


def apply_relevance_adjustments(title, company, major, llm_score):
    """
    Apply boosts and floors to the LLM base score (non-junk titles only).

    Order: additive boosts on LLM base → floors → single clamp to [0, 1].
    Boosts apply only when ``llm_score`` is not None.

    Args:
        title: job title
        company: company name (unused; kept for API stability)
        major: stored major string (may be empty)
        llm_score: float from Groq or None if unavailable / invalid

    Returns:
        float in [0, 1], or None if there is no LLM score and no floor applies
    """
    _ = company
    title = (title or "").strip()
    major = (major or "").strip()
    m_lower = major.lower()

    if _is_obviously_non_career_title(title):
        if llm_score is None:
            return round(_SCORE_CAP_JUNK, 2)
        return round(min(float(llm_score), _SCORE_CAP_JUNK + 0.05), 2)

    if llm_score is not None:
        score = float(llm_score)
        if _ENGINEERING_TITLE.search(title):
            score += _BOOST_ENGINEERING_TITLE
        if major and _STEM_MAJOR.search(m_lower):
            score += _BOOST_STEM_MAJOR_MATCH
    else:
        score = 0.0

    t_lower = title.lower()
    if _TA_TITLE.search(t_lower):
        score = max(score, _FLOOR_TA)
    if (not major) and _ENGINEERING_TITLE.search(title):
        score = max(score, _FLOOR_ENG_NO_MAJOR_CONTEXT)
    if _PROFESSIONAL_TITLE.search(title):
        score = max(score, _FLOOR_PROFESSIONAL)

    score = min(1.0, max(0.0, score))

    if llm_score is None and score == 0.0:
        return None

    return round(score, 2)


def _llm_score_job_relevance(title, company, major_for_prompt):
    """
    Call Groq and return a validated float in [0, 1], or None.
    major_for_prompt must be non-empty (use DEFAULT_MAJOR_CONTEXT_FOR_LLM if needed).
    """
    client = _get_client()
    if not client:
        return None

    company = str(company).strip() if company else ""
    major_for_prompt = str(major_for_prompt).strip()

    prompt = f"""Rate whether this is a legitimate professional career-level job.

Job Title: {title}
Company: {company}
Field of Study / Major: {major_for_prompt}

Scoring guide:
- 0.0-0.15 = Obvious non-career / high-school-level job (cashier, fast food, retail associate, warehouse picker, crew member)
- 0.3-0.5 = Entry-level or loosely professional (customer service, office assistant, front desk)
- 0.5-0.7 = Legitimate professional career not directly in their field (operations manager, recruiter, accountant, product designer)
- 0.7-0.85 = Professional career in a broadly related area (technical sales, project manager for engineering firm, data analyst)
- 0.85-1.0 = Directly aligned professional role (software engineer with CS degree, mechanical engineer with ME degree)

IMPORTANT:
- ANY director, VP, manager, architect, consultant, analyst, designer, or executive role is AT LEAST 0.6
- Engineering/STEM roles score 0.85+
- Student internships in professional fields score 0.6+
- Only score below 0.3 for obvious non-career service/retail jobs

Return ONLY a single number between 0 and 1 (e.g. 0.75).
Return ONLY the number. No text, no explanation, no JSON."""

    for attempt in range(MAX_RETRIES):
        try:
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a career-level evaluator for university graduates. "
                            "You distinguish professional careers from non-career service jobs. "
                            "You MUST respond with ONLY a single decimal number between 0 and 1. "
                            "No words, no explanation, no formatting. Just the number."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
                max_tokens=10,
            )

            result_text = response.choices[0].message.content.strip()
            score = _extract_score(result_text)
            if score is not None:
                logger.debug(
                    "Relevance score for '%s' vs '%s': %.2f", title, major_for_prompt, score
                )
                return score

            logger.warning(
                "⚠️ Groq returned invalid score (attempt %s/%s): '%s'",
                attempt + 1,
                MAX_RETRIES,
                result_text,
            )

        except Exception as e:
            logger.warning(
                "⚠️ Groq relevance scoring failed (attempt %s/%s): %s",
                attempt + 1,
                MAX_RETRIES,
                e,
            )

    logger.warning("❌ Failed to score relevance for '%s' after %s attempts", title, MAX_RETRIES)
    return None


def score_job_relevance(title, company, major):
    """
    Score how relevant a job is to the person's major / CoE cohort.

    Args:
        title: Job title (original, not normalized)
        company: Company name
        major: Person's field of study / major (may be empty — LLM uses default context)

    Returns:
        float between 0.0 and 1.0, or None if no title or no usable signal

    Edge cases:
        - Empty/None title → None
        - Empty major → still scored (default alumni context + heuristic floors)
        - Obvious service/retail titles → low fixed score (no Groq call)
        - LLM failure → TA / engineering-without-major floors may still yield a score
    """
    if not title or not str(title).strip():
        return None

    title = str(title).strip()
    company = str(company).strip() if company else ""
    major_stored = str(major).strip() if major else ""

    if _is_obviously_non_career_title(title):
        return round(_SCORE_CAP_JUNK, 2)

    major_for_prompt = major_stored if major_stored else DEFAULT_MAJOR_CONTEXT_FOR_LLM
    llm_score = _llm_score_job_relevance(title, company, major_for_prompt)
    return apply_relevance_adjustments(title, company, major_stored, llm_score)


def _extract_score(text):
    """
    Extract a float between 0 and 1 from LLM output.

    Strict validation: output must be a single number in [0, 1].
    Returns None if invalid (triggers retry in caller).
    """
    if not text:
        return None

    text = text.strip()
    try:
        val = float(text)
        if 0.0 <= val <= 1.0:
            return round(val, 2)
        return None
    except ValueError:
        pass

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
        threshold: cutoff value (default RELEVANCE_THRESHOLD_RELEVANT, 0.6)

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

    intervals = []
    for job in jobs:
        if not job.get('is_relevant'):
            continue

        start = _parse_date_to_month_year(job.get('start_date', ''))
        end = _parse_date_to_month_year(job.get('end_date', ''))

        if start is None:
            continue
        if end is None:
            now = datetime.now()
            end = (now.year, now.month)

        if start > end:
            start, end = end, start

        intervals.append((start, end))

    if not intervals:
        return 0

    merged = _merge_intervals(intervals)

    total_months = 0
    for (start_year, start_month), (end_year, end_month) in merged:
        months = (end_year - start_year) * 12 + (end_month - start_month)
        total_months += max(months, 0) + 1

    return total_months


def _parse_date_to_month_year(date_str):
    """Parse a date string to (year, month) tuple using parse_groq_date()."""
    if not date_str:
        return None

    date_str = str(date_str).strip()
    if not date_str:
        return None

    if date_str.lower() == "present":
        now = datetime.now()
        return (now.year, now.month)

    parsed = parse_groq_date(date_str)
    if parsed is None:
        return None

    year = parsed.get('year')
    if year is None or year == 9999:
        now = datetime.now()
        return (now.year, now.month)

    month = parsed.get('month')
    if month is None:
        month = 1

    return (year, month)


def _merge_intervals(intervals):
    """Merge overlapping or adjacent (year, month) intervals."""
    if not intervals:
        return []

    sorted_intervals = sorted(intervals, key=lambda x: x[0])

    def _to_months(ym):
        return ym[0] * 12 + ym[1]

    merged = [sorted_intervals[0]]
    for start, end in sorted_intervals[1:]:
        prev_start, prev_end = merged[-1]

        if _to_months(start) <= _to_months(prev_end) + 1:
            new_end = max(prev_end, end)
            merged[-1] = (prev_start, new_end)
        else:
            merged.append((start, end))

    return merged


def analyze_profile_relevance(profile_data):
    """
    Analyze all jobs in a profile for relevance and compute experience months.

    Handles up to 3 jobs per person. Major may be missing — engineering titles
    still receive heuristic scores and LLM context defaults to CoE/STEM.

    Edge cases:
      - No jobs → empty dict
      - No major → still scores jobs when titles exist
      - 1–3 jobs, none relevant → all is_relevant=False, relevant_experience_months=0
    """
    major = (
        profile_data.get('standardized_major')
        or profile_data.get('major')
        or ''
    ).strip()

    result = {}
    jobs_for_experience = []

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

    if not result:
        return {}

    result['relevant_experience_months'] = compute_relevant_experience_months(jobs_for_experience)

    return result


def get_relevance_json(profile_data):
    """
    Structured JSON for the Experience Engine (up to 3 jobs).

    Major may be missing; jobs are still scored with default alumni context.
    """
    major = (
        profile_data.get('standardized_major')
        or profile_data.get('major')
        or ''
    ).strip()

    job_specs = [
        {
            'title_keys': ['title', 'current_job_title'],
            'company_keys': ['company'],
            'start_keys': ['job_start', 'job_start_date'],
            'end_keys': ['job_end', 'job_end_date'],
            'dates_key': None,
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
        title = ''
        for key in spec['title_keys']:
            title = profile_data.get(key) or ''
            if title.strip():
                break

        if not title.strip():
            continue

        company = ''
        for key in spec['company_keys']:
            company = profile_data.get(key) or ''
            if company.strip():
                break

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
    """
    if not date_range_str:
        return ('', '')

    text = str(date_range_str).strip()

    for sep in [' - ', ' – ', ' — ', ' to ']:
        if sep in text:
            parts = text.split(sep, 1)
            return (parts[0].strip(), parts[1].strip())

    return (text, text)
