"""
Groq API integration for extracting education entries from LinkedIn HTML.

Uses shared Groq client infrastructure from groq_client.py.
Returns structured education data for local processing (UNT filtering, sorting, normalization).
"""

import re
from config import logger
from groq_client import (
    _get_client, is_groq_available, GROQ_MODEL, BS4_AVAILABLE,
    save_debug_html, parse_groq_json_response,
    _clean_doubled
)

if BS4_AVAILABLE:
    from bs4 import BeautifulSoup


def _education_html_to_structured_text(html: str, profile_name: str = "unknown", relaxed: bool = False) -> str:
    """
    Convert education section HTML into clean structured text for the LLM.

    Follows the same philosophy as the experience extractor:
    - Remove all markup noise
    - Pipe-separated text per entry separated by ---
    - Save debug output when enabled
    
    Args:
        relaxed (bool): If True, skips aggressive cleaning (removing visually-hidden, sub-components)
                       to prevent data loss in edge cases.
    """
    if not BS4_AVAILABLE:
        text = re.sub(r'<[^>]+>', ' ', html)
        text = re.sub(r'\s+', ' ', text).strip()
        return text[:5000]

    soup = BeautifulSoup(html, 'html.parser')

    # 1. Remove script/style/media (same as experience extractor)
    for tag in soup.find_all(['script', 'style', 'svg', 'img', 'iframe', 'noscript']):
        tag.decompose()

    # 2. Remove visually-hidden and button-icon elements
    #    BUT *unwrap* inline-show-more-text instead of decomposing it,
    #    because LinkedIn wraps school name / degree / dates inside these divs.
    #    Conditional: Only do this if NOT relaxed.
    if not relaxed:
        for tag in soup.find_all(class_=lambda x: x and any(
            c in x for c in ['visually-hidden', 'artdeco-button__icon']
        )):
            tag.decompose()
            
    for tag in soup.find_all(class_=lambda x: x and 'inline-show-more-text' in x):
        tag.unwrap()  # keeps inner content, removes the wrapping tag

    # 3. Remove diamond-icon skill/tag entries from sub-components
    #    (same logic as the experience extractor)
    #    Conditional: Only do this if NOT relaxed.
    if not relaxed:
        for sub_comp in soup.select('.pvs-entity__sub-components'):
            for container in sub_comp.find_all('div', class_=lambda c: c and 't-14' in c and 't-normal' in c and 't-black' in c and 't-black--light' not in c):
                if container.find('strong'):
                    li_parent = container.find_parent('li')
                    if li_parent:
                        li_parent.decompose()
                    else:
                        container.decompose()

    # 4. Remove skill lines like "Civil Engineering, Problem Solving and +4 skills"
    for tag in soup.find_all(["strong", "span"]):
        txt = tag.get_text()
        if ("skill" in txt.lower() and "+" in txt) or ("skills" in txt.lower() and ("+" in txt or "," in txt)):
            parent = tag.find_parent("div", class_="display-flex")
            if parent:
                parent.decompose()
            else:
                tag.decompose()

    # 5. Remove activity-related sections (they show in education area sometimes)
    for tag in soup.find_all(class_=lambda x: x and any(
        c in x for c in ['activities-societies', 'pv-shared-text-with-see-more']
    )):
        tag.decompose()

    # Extract structured entries from education list items
    entries = []

    # Prioritize 'artdeco-list__item' which is the standard container for main profile items.
    # Previous logic prioritized 'pvs-list' which matched CHILDREN items (details) in some profiles, 
    # causing the parent item (with School Name) to be ignored.
    education_items = (
        soup.select('li.artdeco-list__item')
        or soup.select('li.pvs-list__paged-list-item')
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
    mode_suffix = "_relaxed" if relaxed else ""
    save_debug_html(structured, profile_name + mode_suffix, "education")

    # Save debug HTML to see what's being passed after cleaning (for diagnosis)
    try:
        import os
        debug_dir = "scraper/output/debug_html"
        if not os.path.exists(debug_dir): os.makedirs(debug_dir)
        debug_path = f"{debug_dir}/{profile_name.replace(' ', '_')}_education_cleaned{mode_suffix}.html"
        with open(debug_path, "w", encoding="utf-8") as f:
             f.write(str(soup))
    except Exception:
        pass

    return structured.strip()


def extract_education_with_groq(education_html: str, profile_name: str = "unknown") -> list:
    """
    Extract education entries from LinkedIn education section HTML using Groq.

    Args:
        education_html: The innerHTML of the education section.
        profile_name: Name of the profile (for debug file naming).

    Returns:
        List of dicts with keys: school, degree_raw, major_raw, start_date, end_date.
        Returns empty list if extraction fails.

    Note:
        This function does NOT filter for UNT, sort, or normalize.
        All post-processing is done locally by the caller.
    """
    client = _get_client()
    if not client:
        logger.warning("⚠️ Groq not available, skipping education LLM extraction")
        return []

    original_len = len(education_html)
    
    # Adaptive Cleaning Logic:
    # 1. Try standard cleaning (removes noise).
    # 2. Check if we lost the target university (UNT) keywords found in raw JSON.
    # 3. If lost, retry with relaxed cleaning.
    
    unt_keywords = ["university of north texas", "north texas", "unt"]
    raw_lower = education_html.lower()
    has_unt_raw = any(k in raw_lower for k in unt_keywords)
    
    # Try Standard
    structured_text = _education_html_to_structured_text(education_html, profile_name, relaxed=False)
    
    # Check if we messed up
    should_retry = False
    retry_reason = ""

    # 1. Check for lost UNT keywords
    if has_unt_raw:
        text_lower = structured_text.lower()
        if not any(k in text_lower for k in unt_keywords):
            should_retry = True
            retry_reason = "Loss of UNT keywords"

    # 2. Check for over-aggressive reduction (e.g. >99% loss on large input)
    if not should_retry and original_len > 1000:
        ratio = len(structured_text) / original_len
        if ratio < 0.01:  # Less than 1% preserved
            should_retry = True
            retry_reason = f"Extreme reduction ({original_len} -> {len(structured_text)} chars)"

    if should_retry:
        logger.warning(f"    ⚠️ Standard cleaning failed: {retry_reason}. Retrying with RELAXED cleaning for {profile_name}...")
        structured_text = _education_html_to_structured_text(education_html, profile_name, relaxed=True)
        
        # Log result of retry
        if has_unt_raw:
            if any(k in structured_text.lower() for k in unt_keywords):
                    logger.info("      ✓ Relaxed cleaning recovered UNT keywords.")
            else:
                    logger.warning("      ❌ Relaxed cleaning still missed UNT keywords (check debug HTML).")
        else:
            logger.info("      ✓ Retried with relaxed cleaning.")

    text_len = len(structured_text)
    reduction = round((1 - text_len / original_len) * 100) if original_len > 0 else 0
    logger.info(f"    📉 Education HTML → text: {original_len:,} → {text_len:,} chars ({reduction}% reduction)")

    prompt = f"""Extract ALL education entries from this LinkedIn education data.

For each education entry return:
- school: The full school/university name
- degree_raw: ONLY the degree type (e.g. "Bachelor of Science", "Master of Arts", "PhD"). Do NOT include the field of study here.
- major_raw: ONLY the field of study / major (e.g. "Computer Science", "Electrical Engineering"). Do NOT include the degree type here.
- start_year: Format "YYYY" or "Mon YYYY" if available
- end_year: Format "YYYY" or "Mon YYYY" if available

Rules:
- SPLIT the degree type from the major. For example:
  "Bachelor of Science in Computer Science" → degree_raw: "Bachelor of Science", major_raw: "Computer Science"
  "Master of Science - MS, Computational Science" → degree_raw: "Master of Science", major_raw: "Computational Science"
  "BS, Mechanical Engineering" → degree_raw: "BS", major_raw: "Mechanical Engineering"
- Extract every school listed, even if there is only 1
- If the degree field is missing or blank, set degree_raw to ""
- If the major field is missing or blank, set major_raw to ""
- If start_year or end_year are missing, set them to ""
- DO NOT invent data — if something is not present in the text, leave it blank
- IGNORE activity/society lists, grades, and descriptions
- Each entry should represent ONE school attendance
- If a single entry mentions a degree and then repeats it or a similar one in the description (e.g. "Masters in X" ... "MS in Y"), treat it as ONE degree. DO NOT split into two entries unless the dates are distinct.
- If duplicate information appears (e.g. parent item + child detail item), merge them into one entry.

Return ONLY a JSON object with an "education" key:
{{"education": [{{"school":"...","degree_raw":"...","major_raw":"...","start_year":"...","end_year":"..."}}]}}

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
            degree_raw = (entry.get("degree_raw") or entry.get("raw_degree") or "").strip()
            major_raw = (entry.get("major_raw") or "").strip()
            start_year = (entry.get("start_year") or entry.get("start_date") or "").strip()
            end_year = (entry.get("end_year") or entry.get("end_date") or "").strip()

            if not school:
                continue

            # Clean doubled text
            school = _clean_doubled(school)
            degree_raw = _clean_doubled(degree_raw)
            major_raw = _clean_doubled(major_raw)

            # Deduplicate: same school + same degree + same major
            is_dup = False
            for existing in valid_entries:
                if (existing["school"].lower() == school.lower()
                        and existing["degree_raw"].lower() == degree_raw.lower()
                        and existing["major_raw"].lower() == major_raw.lower()):
                    is_dup = True
                    break
            if is_dup:
                logger.info(f"    🗑️ Skipping duplicate education: {school}")
                continue

            valid_entries.append({
                "school": school,
                "degree_raw": degree_raw,
                "major_raw": major_raw,
                "start_date": start_year,
                "end_date": end_year,
            })

        if valid_entries:
            logger.info(f"    ✓ Groq extracted {len(valid_entries)} education entry/entries")
            for i, e in enumerate(valid_entries):
                logger.info(f"      Edu {i+1}: {e['school']} — {e['degree_raw']} / {e['major_raw']} ({e['start_date']}–{e['end_date']})")

        return valid_entries

    except Exception as e:
        logger.error(f"⚠️ Groq education extraction failed: {e}")
        return []
