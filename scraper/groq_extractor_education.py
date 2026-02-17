"""
Groq API integration for extracting education entries from LinkedIn HTML.

Uses shared Groq client infrastructure from groq_client.py.
Returns structured education data for local processing (UNT filtering, sorting, normalization).
"""

import re
from config import logger
from groq_client import (
    _get_client, is_groq_available, GROQ_MODEL, BS4_AVAILABLE,
    save_debug_html, parse_groq_json_response, strip_noise_elements,
    _clean_doubled
)

if BS4_AVAILABLE:
    from bs4 import BeautifulSoup


def _education_html_to_structured_text(html: str, profile_name: str = "unknown") -> str:
    """
    Convert education section HTML into clean structured text for the LLM.

    Follows the same philosophy as the experience extractor:
    - Remove all markup noise
    - Pipe-separated text per entry separated by ---
    - Save debug output when enabled
    """
    if not BS4_AVAILABLE:
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:5000]

    soup = BeautifulSoup(html, 'html.parser')

    # Remove noise elements (shared utility)
    strip_noise_elements(soup)

    # Remove activity-related sections (they show in education area sometimes)
    for tag in soup.find_all(class_=lambda x: x and any(
        c in x for c in ['activities-societies', 'pv-shared-text-with-see-more']
    )):
        tag.decompose()

    # Extract structured entries from education list items
    entries = []

    education_items = (
        soup.select('li.pvs-list__paged-list-item')
        or soup.select('li[class*="pvs-list"]')
        or soup.find_all('li')
    )

    for item in education_items:
        text = item.get_text(separator=' | ', strip=True)
        text = re.sub(r'\|\s*\|', '|', text)
        text = re.sub(r'\s+', ' ', text).strip()

        if text and len(text) > 10:
            entries.append(text)

    if not entries:
        text = soup.get_text(separator='\n', strip=True)
        text = re.sub(r'\n{3,}', '\n\n', text)
        structured = text.strip()
    else:
        structured = '\n---\n'.join(entries)

    # Save debug output
    save_debug_html(structured, profile_name, "education")

    return structured.strip()


def extract_education_with_groq(education_html: str, profile_name: str = "unknown") -> list:
    """
    Extract education entries from LinkedIn education section HTML using Groq.

    Args:
        education_html: The innerHTML of the education section.
        profile_name: Name of the profile (for debug file naming).

    Returns:
        List of dicts with keys: school, raw_degree, start_date, end_date.
        Returns empty list if extraction fails.

    Note:
        This function does NOT filter for UNT, sort, or normalize.
        All post-processing is done locally by the caller.
    """
    client = _get_client()
    if not client:
        logger.warning("⚠️ Groq not available, skipping education LLM extraction")
        return []

    structured_text = _education_html_to_structured_text(education_html, profile_name)
    original_len = len(education_html)
    text_len = len(structured_text)
    reduction = round((1 - text_len / original_len) * 100) if original_len > 0 else 0
    logger.info(f"    📉 Education HTML → text: {original_len:,} → {text_len:,} chars ({reduction}% reduction)")

    prompt = f"""Extract ALL education entries from this LinkedIn education data.

For each education entry return:
- school: The full school/university name
- raw_degree: The full degree text (e.g. "Bachelor of Science in Computer Science", "Master of Business Administration")
- start_date: Format "YYYY" or "Mon YYYY" if available
- end_date: Format "YYYY" or "Mon YYYY" if available

Rules:
- Extract every school listed, even if there is only 1
- If the degree field is missing or blank, set raw_degree to ""
- If start_date or end_date are missing, set them to ""
- DO NOT invent data — if something is not present in the text, leave it blank
- DO NOT split a combined degree string (e.g. "Bachelor of Science, Computer Science") into separate entries
- IGNORE activity/society lists, grades, and descriptions
- Each entry should represent ONE school attendance

Return ONLY a JSON object with an "education" key:
{{"education": [{{"school":"...","raw_degree":"...","start_date":"...","end_date":"..."}}]}}

If no education found return: {{"education": []}}

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
                        "If extraction fails, return {\"education\": []} exactly."
                    )
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=1024,
            response_format={"type": "json_object"}
        )

        result_text = response.choices[0].message.content.strip()

        parsed = parse_groq_json_response(result_text)
        if parsed is None:
            return []

        # Extract education array from response
        if isinstance(parsed, dict):
            education = None
            for key, val in parsed.items():
                if isinstance(val, list):
                    education = val
                    break
            if education is None:
                # Single object
                if parsed.get("school"):
                    education = [parsed]
                else:
                    logger.warning("⚠️ Groq returned JSON object with no education data")
                    return []
        elif isinstance(parsed, list):
            education = parsed
        else:
            logger.warning("⚠️ Groq returned unexpected JSON type for education")
            return []

        # Validate and clean each entry
        valid_entries = []
        for entry in education:
            if not isinstance(entry, dict):
                continue

            school = (entry.get("school") or "").strip()
            raw_degree = (entry.get("raw_degree") or "").strip()
            start_date = (entry.get("start_date") or "").strip()
            end_date = (entry.get("end_date") or "").strip()

            if not school:
                continue

            # Clean doubled text
            school = _clean_doubled(school)
            raw_degree = _clean_doubled(raw_degree)

            # Deduplicate: same school + same degree
            is_dup = False
            for existing in valid_entries:
                if (existing["school"].lower() == school.lower()
                        and existing["raw_degree"].lower() == raw_degree.lower()):
                    is_dup = True
                    break
            if is_dup:
                logger.info(f"    🗑️ Skipping duplicate education: {school}")
                continue

            valid_entries.append({
                "school": school,
                "raw_degree": raw_degree,
                "start_date": start_date,
                "end_date": end_date
            })

        if valid_entries:
            logger.info(f"    ✓ Groq extracted {len(valid_entries)} education entry/entries")
            for i, e in enumerate(valid_entries):
                logger.info(f"      Edu {i+1}: {e['school']} — {e['raw_degree']} ({e['start_date']}–{e['end_date']})")

        return valid_entries

    except Exception as e:
        logger.error(f"⚠️ Groq education extraction failed: {e}")
        return []
