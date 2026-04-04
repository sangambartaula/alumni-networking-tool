# Data Processing & AI Engines

This document outlines the artificial intelligence and data transformation pipelines used by the alumni scraper.

## Geocoding Fallback Rules

Primary geocoding uses OpenStreetMap/Nominatim via `backend/geocoding.py`.

When geocoding returns `unknown_location` during scrape persistence:

- The scraper can optionally run a one-time Groq normalization pass (`GEOCODE_USE_GROQ_FALLBACK=true`).
- Groq is asked to normalize location text into a geocodable format (for example `Austin, Texas, United States`).
- If Groq returns a normalized location, geocoding is retried once with that value.
- If Groq returns `unknown`, the scraper does not guess and clears `location` to null for that profile payload.
- If Groq is unavailable, the original location is preserved and the profile is tracked as unknown geocode.

This behavior prevents vague region labels from being force-mapped to incorrect coordinates while still rescuing obvious cases like metro-area strings.

# UNT Alumni Networking Tool - Normalization and Classification Rules

This document defines how raw scraped/profile data is standardized for filtering, analytics, and display.

## 1. Scope

These rules cover:

- Job title normalization
- Company normalization
- Degree normalization
- Major normalization
- Discipline inference
- Alumni status classification
- Working-while-studying classification
- Entity classification used during parsing (company vs job title vs location)

## 2. Job Title Normalization

Primary code paths:

- `scraper/job_title_normalization.py`
- `scraper/scraper.py` (`_apply_experience_display_normalization`)

Rules:

- Cleanup first:
- collapse whitespace
- strip trailing location fragments (for example `", Austin"` or `" - Dallas"`)
- strip trailing punctuation
- remove regional markers
- Deterministic multi-pass mapping:
- exact lookup in `TITLE_MAP`
- heuristic for `soft*` prefix => `Software Engineer`
- retry after removing level suffixes (`II`, `III`, etc.)
- retry after removing parenthetical qualifiers
- retry after removing seniority prefixes/suffixes
- If no deterministic map hit:
- keep a cleaned version of the title (with seniority stripped when possible)
- then run compaction buckets (for example multiple software-like terms => `Software Engineer`)
- Noise handling:
- obvious non-title values (for example `unknown`, `n/a`, date-like strings) collapse to empty

LLM behavior:

- `normalize_title_with_groq()` is available and guarded by `GROQ_API_KEY`.
- `get_or_create_normalized_title(..., use_groq=True)` only calls Groq if deterministic normalization is a passthrough.
- Current scraper display normalization uses deterministic normalization directly.

Examples:

- `Senior Software Engineer II` -> `Software Engineer`
- `Graduate Research Assistant` -> `Researcher` or `Graduate Assistant` bucket depending on mapped value
- `N/A` -> `""`

## 3. Company Normalization

Primary code paths:

- `scraper/company_normalization.py`
- `scraper/scraper.py` (`_apply_experience_display_normalization`)

Rules:

- Cleanup first:
- collapse whitespace
- strip trailing location fragments
- strip legal suffixes (for example `Inc`, `LLC`, `Ltd`, `Corp`) when non-essential
- strip trailing punctuation
- Deterministic mapping:
- exact lookup in `COMPANY_MAP` (case-insensitive)
- retry with extra suffix stripping
- fallback to cleaned company string if no map hit
- DB-aware normalization:
- `get_or_create_normalized_company()` checks existing normalized rows case-insensitively
- if deterministic result is passthrough and `use_groq=True`, it can call Groq
- result is upserted to `normalized_companies`

LLM behavior:

- `normalize_company_with_groq()` is available and guarded by `GROQ_API_KEY`.
- invalid LLM outputs like `unknown` or `other` are rejected and fall back to deterministic output.
- current scraper display normalization uses deterministic normalization directly.

Examples:

- `University of North Texas` and `UNT` -> `University of North Texas`
- `Acme Inc., Dallas` -> `Acme`

## 4. Degree Normalization

Primary code path:

- `scraper/degree_normalization.py`

There are two outputs:

- Canonical degree string from `normalize_degree_deterministic()`
- Grouped degree label from `standardize_degree()`

Canonical rules:

- exact map match first (for example `BS`, `PhD`, `MBA`)
- prefix extraction before `in`, comma, or dash
- broad pattern scan
- if unknown, return cleaned original string

Grouped rules (`Associate`, `Bachelors`, `Masters`, `Doctorate`, `Other`):

- null/empty/unknown-like values => `Other`
- map known canonical degrees to grouped labels
- keyword fallback patterns
- UNT-specific heuristic: engineering-major-like text in degree field can map to `Bachelors`
- default => `Other`

Examples:

- `BS in Computer Science` -> canonical `Bachelor of Science` -> grouped `Bachelors`
- `Certificate in Data Analytics` -> canonical cleaned string, grouped `Other`

## 5. Major Normalization

Primary code path:

- `scraper/major_normalization.py`

Degree-agnostic normalized majors list (v2):

- Artificial Intelligence
- Biomedical Engineering
- Computer Engineering
- Computer Science
- Construction Engineering Technology
- Construction Management
- Cybersecurity
- Data Engineering
- Electrical Engineering
- Engineering Management
- Geographic Information Systems + Computer Science
- Information Technology
- Materials Science and Engineering
- Mechanical and Energy Engineering
- Mechanical Engineering Technology
- Semiconductor Manufacturing Engineering
- Other

Rules:

- output must be one of `UNT_ALLOWED_MAJORS` or `Other`
- strip minor/concentration/certificate noise before matching
- then apply (AI-first):
  - Groq major classification first when enabled and configured:
    - `MAJOR_USE_GROQ_FALLBACK=1` (default enabled)
    - `GROQ_API_KEY` present
  - deterministic fallback when Groq is unavailable, fails, or returns unusable output:
    - exact alias map (`_EXACT_MAJOR_MAP`)
    - direct canonical match
    - ordered regex patterns (`_MAJOR_PATTERNS`)
- if unresolved or invalid => `Other`

Multi-entry mapping (CS&E):

- "Computer Science and Engineering" is the only raw major that maps to two canonical majors:
  `standardized_major = "Computer Science"` and `standardized_major_alt = "Computer Engineering"`
- `standardize_major_list()` returns a list (length 2 for CS&E, length 1 for all others)
- `standardize_major()` remains backward-compatible and returns only the primary entry
- In the dashboard filter: selecting CS or CE alone shows all matching entries (including dual-mapped CS&E);
  selecting both CS and CE together shows only entries mapped to both (i.e., CS&E entries)

"Other" fallback:

- Any raw major text that cannot be resolved through Groq-first classification plus deterministic fallback
  is mapped to "Other"
- "Other" entries are still stored in the database and displayed but excluded from major filter checkboxes

LLM contract:

- model returns `major_id`
- `0` means `Other`
- non-integer or out-of-range values are coerced to `Other`

## 6. Discipline Inference

Primary code path:

- `scraper/discipline_classification.py`

Allowed output categories:

- `Software, Data, AI & Cybersecurity`
- `Embedded, Electrical & Hardware Engineering`
- `Mechanical Engineering & Manufacturing`
- `Biomedical Engineering`
- `Construction & Engineering Management`
- `Other`

Precedence order:

1. Highest-ranked UNT major
2. Other UNT majors
3. Non-UNT majors
4. Current job title
5. Older job titles
6. Headline

Additional guards:

- non-engineering kill-list can force `Other`
- generic degree labels avoid forcing false positives
- classification is AI-first for each non-empty source text when Groq is configured
- deterministic rules are used as fallback when Groq is unavailable/fails/returns `Unknown`

## 7. Alumni Status (UNT Alumni vs Not Yet Alumni vs Unknown)

Primary code path:

- `backend/unt_alumni_status.py`

Rules:

- only UNT education entries are considered
- if explicit end date exists:
- `end_date <= today` => `yes`
- `end_date > today` => `no`
- if only end year exists:
- past year => `yes`
- future year => `no`
- current year (year-only) => assume May 15 cutoff
- `today >= May 15` => `yes`
- `today < May 15` => `no`
- no usable year/date => `unknown`

Multi-entry precedence:

- if any UNT entry is `no`, overall is `no`
- else if any UNT entry is `yes`, overall is `yes`
- else `unknown`

## 8. Working While Studying

Primary code paths:

- `scraper/utils.py` (`determine_work_study_status`)
- `scraper/scraper.py` (`_apply_missing_dates_unt_ga_fallback`)
- `backend/working_while_studying_status.py`

Status values:

- `yes`
- `no`
- `currently`
- `""` (insufficient date info in scraper stage)

Date-based logic:

- use month precision when available for both school end and job dates
- if graduation year exists but month is missing, effective graduation date defaults to May 15 of that year
- if still studying (expected grad / present / missing grad year) and job is active => `currently`
- if job started before effective graduation date => `yes`
- else => `no`

Missing-date strict fallback:

- only applies when computed status is `""`
- requires UNT education + standardized `Graduate Assistant` title + UNT employer
- returns `yes` when all conditions match, otherwise `no`

Backend normalization:

- `status_to_bool`: `yes/currently => True`, `no => False`, else `None`

## 9. Entity Classification Used in Parsing

Primary code path:

- `scraper/entity_classifier.py`

Tiered order:

1. Location quick-check (for obvious location formats)
2. Curated DB lookup (`companies.json`)
3. spaCy NER
4. Regex fallback heuristics

Output labels:

- `company`
- `job_title`
- `location`
- `university`
- `unknown`

This is primarily used when parsing noisy LinkedIn experience blocks.

## 10. Unknown/Null Conventions

Normalization conventions are intentionally strict:

- Degree/Major/Discipline unresolved values map to `Other` (or `unknown` where that status is explicitly modeled)
- Alumni status unresolved values map to `unknown`
- Job/company normalization may keep cleaned passthrough values rather than forcing `Other`
- LLM outputs that are malformed or invalid always fall back to deterministic logic

## 11. Test References

Core behavior is exercised in:

- `backend/tests/test_unt_alumni_status.py`
- `backend/tests/test_unt_alumni_filter_endpoints.py`
- `tests/test_backend_filter_api.py`
- `backend/tests/test_working_while_studying_status.py`
- `tests/test_working_while_studying_fallback.py`
- `backend/tests/test_work_while_studying.py`
- `backend/tests/test_discipline_classification.py`
- `tests/test_degree_normalization.py`
- `tests/test_entity_classifier.py`
- `tests/test_groq_prompt_hardening.py`
- `tests/test_groq_imports.py`
- `tests/test_groq_extractor_experience_regression.py`

If rules are changed, update this file and the corresponding tests in the same PR.


---

# Relevance Engine

The Relevance Engine scores whether each job (up to 3) is a **legitimate professional career** versus a non-career / high-school-level service job. It uses the **Groq LLM** plus **heuristic floors and boosts**.

**Philosophy**: Any professional post-college career counts as relevant — not just STEM/engineering roles. Directors, managers, analysts, consultants, designers, etc. are ALL relevant. Engineering and STEM titles get a slight boost. Only obvious non-career service jobs (cashier, crew member, retail associate, etc.) are excluded.

## Scoring Logic

For each job, we send a prompt to the Groq API with:

- **Job title** (original, not normalized)
- **Company name** (optional context)
- **Major / field of study** (from `standardized_major` or `major`, or a default "university graduate" prompt if missing)

The LLM returns a **base** career-level score in `[0.0, 1.0]`. Processing order:

1. **Junk check** — Titles like cashier, crew member, retail associate, etc. → **0.10** and **no LLM call** (unless the title matches engineering/technical or professional patterns).
2. **LLM** — Base score for all non-junk titles.
3. **Additive boosts** (only if the LLM returned a score):
   - Engineering/STEM-style title → **+0.05**
   - Stored major matches STEM/engineering patterns → **+0.05**
4. **Floors** (apply as a minimum on the boosted score):
   - TA / RA / peer tutor (and similar) → **0.65**
   - Engineering-style title with **no major** stored → **0.60**
   - Professional title (director, manager, VP, analyst, consultant, etc.) → **0.60**
5. **Clamp** the result to `[0, 1]`.

### LLM Scoring Guide

The prompt instructs the LLM to score on this scale:

| Score Range | Meaning |
|------------|---------|
| 0.0 – 0.15 | Non-career / HS-level service job (cashier, fast food, retail, crew member) |
| 0.3 – 0.5 | Entry-level or loosely professional (customer service, office assistant) |
| 0.5 – 0.7 | Professional career not directly in their field (operations manager, recruiter) |
| 0.7 – 0.85 | Professional career in a broadly related area (technical sales, project manager) |
| 0.85 – 1.0 | Directly aligned professional role (software engineer with CS degree) |

### Retry & Validation

- **Up to 3 attempts** per job (`MAX_RETRIES = 3`)
- Output is **strictly validated**: must parse as a single float in `[0, 1]`
- If the LLM fails, **floors** may still set a score for TA/RA, engineering, or professional titles; otherwise the job score is `None` and `is_relevant` is `None`

## Threshold

```
RELEVANCE_THRESHOLD_RELEVANT = 0.45
```

**Why 0.45?**

- Captures all professional-level careers while still excluding obvious HS-level service jobs (typically scored ≤ ~0.15)
- Professional title floors (0.60) and TA/RA floors (0.65) always pass the bar
- Entry-level professional roles scoring ~0.5 from the LLM are included
- Configurable: change `RELEVANCE_THRESHOLD_RELEVANT` in `scraper/relevance_scorer.py`

| Score Range | Typical meaning     | `is_relevant` |
|------------|---------------------|---------------|
| 0.0 – 0.44 | Not counted (service/retail/HS-level) | `false` |
| 0.45 – 1.0 | Professional career | `true` |

## Heuristic Regex Patterns

### Junk Titles (auto-score 0.10, no LLM call)
Cashier, barista, fast food cook, server, waiter, busboy, bartender, dishwasher, janitor, landscaper, security guard, retail associate, stocker, warehouse associate, package handler, delivery driver, crew member, sandwich artist, personal shopper, front desk staff, etc.

### Professional Titles (floor 0.60, never treated as junk)
Director, manager, president, VP, CxO roles, architect, consultant, advisor, analyst, strategist, designer, product manager, project manager, recruiter, accountant, auditor, attorney, nurse, professor, founder, operations, procurement, supply chain, logistics, etc.

### Engineering/STEM Titles (floor 0.60 when no major, +0.05 boost)
Engineer, developer, programmer, SWE/SDE/SRE, DevOps, architect (tech), data scientist, QA engineer, automation engineer, field/manufacturing/process engineer, IT analyst/specialist, lab technician, R&D, etc.

## Input / Output

### Input

A `profile_data` dict with these fields (all optional — missing fields are handled gracefully):

| Field | Source | Example |
|-------|--------|---------|
| `title` / `current_job_title` | Job 1 title | "Software Engineer" |
| `company` | Job 1 company | "Google" |
| `job_start` / `job_start_date` | Job 1 start | "Jan 2020" |
| `job_end` / `job_end_date` | Job 1 end | "Present" |
| `exp_2_title` | Job 2 title | "Data Analyst" |
| `exp_2_company` | Job 2 company | "Meta" |
| `exp_2_dates` | Job 2 date range | "Mar 2018 - Dec 2019" |
| `exp_3_title` | Job 3 title | "Intern" |
| `exp_3_company` | Job 3 company | "Startup Inc" |
| `exp_3_dates` | Job 3 date range | "Jun 2017 - Aug 2017" |
| `standardized_major` / `major` | Person's field | "Computer Science" |

### Output — Flat Dict (`analyze_profile_relevance`)

```python
{
    "job_1_relevance_score": 0.92,
    "job_1_is_relevant": True,
    "job_2_relevance_score": 0.65,
    "job_2_is_relevant": True,
    "job_3_relevance_score": 0.85,
    "job_3_is_relevant": True,
    "relevant_experience_months": 36
}
```

### Output — Structured JSON (`get_relevance_json`)

```json
[
    {"title": "Software Engineer", "company": "Google", "score": 0.92, "is_relevant": true, "start_date": "Jan 2020", "end_date": "Present"},
    {"title": "Operations Manager", "company": "Acme Corp", "score": 0.65, "is_relevant": true, "start_date": "Mar 2018", "end_date": "Dec 2019"},
    {"title": "Intern", "company": "Startup Inc", "score": 0.85, "is_relevant": true, "start_date": "Jun 2017", "end_date": "Aug 2017"}
]
```

## Edge Cases

| Scenario | Behavior |
|----------|----------|
| Person has 0 jobs | Returns empty dict / empty list |
| Person has no major | Jobs **still scored** (default prompt + heuristic floors for professional/engineering titles) |
| 1–3 jobs, none relevant | All `is_relevant=false`, `relevant_experience_months=0` |
| Job with missing title | Skipped entirely |
| Job with missing company | Still scored (company is optional context) |
| Groq fails after 3 retries | Heuristic floors may still set a score; else `score=None` |
| Groq unavailable | Heuristic floors only (professional → 0.60, engineering → 0.60); else `None` |

## CSV Columns

Added to `UNT_Alumni_Data.csv`:

- `job_1_relevance_score`, `job_2_relevance_score`, `job_3_relevance_score`
- `job_1_is_relevant`, `job_2_is_relevant`, `job_3_is_relevant`
- `relevant_experience_months`

## Database Columns

Added to `alumni` table (MySQL and SQLite):

- `job_1_relevance_score` (FLOAT / REAL)
- `job_2_relevance_score` (FLOAT / REAL)
- `job_3_relevance_score` (FLOAT / REAL)
- `job_1_is_relevant` (BOOLEAN / INTEGER)
- `job_2_is_relevant` (BOOLEAN / INTEGER)
- `job_3_is_relevant` (BOOLEAN / INTEGER)
- `relevant_experience_months` (INT / INTEGER)
- `seniority_level` (VARCHAR(20) / TEXT)

## Integration with Experience Engine

The structured output from `get_relevance_json()` feeds directly into the Experience Engine. Each entry includes all fields needed to compute weighted experience:

- `score` — raw relevance weight
- `is_relevant` — boolean filter
- `start_date` / `end_date` — for duration computation

## Retroactive Computation

### Full Re-scoring (calls Groq LLM)

```bash
# Re-score all alumni relevance + compute experience months
python scripts/backfill_experience_analysis.py --force
```

### Experience Months Only (no Groq, date arithmetic only)

```bash
# Dry run — show computed values without writing
python scripts/compute_experience_months.py --dry-run

# Normal run — update only alumni with missing experience months
python scripts/compute_experience_months.py

# Force — re-process all alumni (even those already computed)
python scripts/compute_experience_months.py --force
```

### Date Handling

| Scenario | Behavior |
|----------|----------|
| Missing end date | Treated as "Present" (current month) |
| Missing start date | Job skipped |
| Overlapping date ranges | Merged before summing |
| Same-month start/end | Counted as 1 month |
| Year-only date (e.g. "2020") | Defaults to January |
| Missing/insufficient date signals after relevance | Stored as unknown (`NULL`) in recompute workflows |

### Frontend Experience Filter

The alumni directory sidebar includes a "Relevant Experience" range filter. Enter min/max years of relevant experience.

- **Backend**: `exp_min` and `exp_max` query params (in months) on `/api/alumni`
- **Frontend**: `#expMin` / `#expMax` number inputs converted to months (years × 12)
- **Unknown handling**: unknown (`NULL`) experience is excluded by default from range filters
- **Optional include**: set `include_unknown_experience=1` (via sidebar checkbox) to include unknown profiles


---

# Seniority Level Classification Guide

## Overview

The alumni networking tool now automatically classifies each alumni profile into a **seniority level** bucket based on their job titles. This feature helps analysts and recruiters quickly identify professionals at specific experience levels.

## Seniority Level Buckets

The system classifies profiles into these buckets (used by the API filter UI):

| Level | Description | Examples |
|-------|-------------|----------|
| **Intern** | Student/early career during studies | Intern, Co-op, Research Assistant, Teaching Assistant, Trainee |
| **Mid** | Early-to-mid professional | Software Engineer, Data Analyst, Entry-Level Analyst |
| **Senior** | Experienced professional | Senior Engineer, Staff Engineer, Distinguished Engineer, Fellow |
| **Manager** | People/program/project management roles | Engineering Manager, Program Manager, Project Manager, Supervisor, Scrum Master |
| **Executive** | Director+ and leadership/executive roles | Director of Engineering, Head of Product, VP, CxO, Founder |

## How It Works

### Classification Logic

1. **Primary Source**: The **most recent job title** (from current position)
2. **Supporting Context**: Additional job titles (exp_2_title, exp_3_title) used only if unclear
3. **Keyword Matching**: Pattern matching on job titles to identify seniority indicators
4. **Validation Layer**: Relevant experience months used for light validation, not main classifier

### Key Rules

- **Title-based, not experience-based**: A person titled "Senior Engineer" is classified as Senior regardless of months employed
- **Original titles only**: Uses non-normalized job titles (preserves seniority keywords like "Senior", "Lead", etc.)
- **Non-destructive flagging**: If a profile looks inconsistent (e.g., "Senior" but only 8 months experience), it's flagged for review but the seniority classification is retained
- **Default to Mid**: If no seniority indicators found in the title, defaults to "Mid"

## Setup & Population

### 1. Ensure Column Exists

Run the migration to add the seniority_level column:

```bash
python migrations/migrate_add_seniority_level.py
```

This creates the `seniority_level VARCHAR(20) DEFAULT NULL` column in the alumni table.

### 2. Database Initialization

The column is automatically created during database initialization. Check [backend/database.py](../backend/database.py) - the `ensure_experience_analysis_columns()` function handles this.

### 3. Population

For new profiles (as they're scraped), seniority is automatically detected and stored.

To backfill existing profiles:

```bash
python scripts/backfill_seniority_levels.py
```

This script:
- Reads all alumni records from the database
- Analyzes each job title to determine seniority level
- Updates the seniority_level column
- Flags mismatches for manual review
- Reports coverage statistics

## Flagging for Manual Review

When inconsistencies are detected (e.g., "Senior" title but only 11 months of relevant experience), the profile is flagged in:

```
scraper/output/flagged_for_review.txt
```

Format:
```
https://linkedin.com/in/username # Seniority mismatch: Senior but only 11 months relevant experience (expected >= 12 months)
```

### Mismatch Thresholds

The stored/API label is now `Intern/Mid/Senior/Manager/Executive`.

| Seniority (stored bucket) | Min Experience used for flagging | Notes |
|----------------------------|-------------------------------------|-------|
| Senior | 12 months | Flags if less than 12 months |
| Manager | 18 months | Flags if less than 18 months |
| Executive | 36+ months depending on title tier | Director/executive leadership expectations |
| Intern/Mid | N/A | Never flagged on experience |

### Important: Flagging Does NOT Block Processing

- Profiles are never blocked or dropped due to mismatches
- Flagging is purely for auditing and manual review
- The seniority_level is always stored and used in the system
- Analysts can review flagged profiles later and adjust if needed

## API Integration

### List Alumni with Seniority

**GET /api/alumni**

Returns alumni list with seniority_level included:

```json
{
  "success": true,
  "items": [
    {
      "id": 123,
      "name": "Jane Smith",
      "current_job_title": "Senior Software Engineer",
      "company": "Tech Corp",
      "grad_year": 2018,
      "major": "Computer Science",
      "seniority_level": "Senior",
      ...
    }
  ],
  "total": 500,
  "limit": 250,
  "offset": 0
}
```

### Get Single Alumni Detail

**GET /api/alumni/<id>**

Returns full alumni record including seniority_level:

```json
{
  "success": true,
  "alumni": {
    "id": 123,
    "name": "Jane Smith",
    "current_job_title": "Senior Software Engineer",
    "company": "Tech Corp",
    "seniority_level": "Senior",
    ...
  }
}
```

## CSV Export

The **UNT_Alumni_Data.csv** includes the `seniority_level` column:

```csv
first,last,linkedin_url,title,company,...,seniority_level
Jane,Smith,https://linkedin.com/in/jane-smith,...,Senior Software Engineer,Tech Corp,...,Senior
```

## Database Schema

Column added to the `alumni` table:

```sql
seniority_level VARCHAR(20) DEFAULT NULL
```

Possible values:
- `Intern`
- `Mid`
- `Senior`
- `Manager`
- `Executive`
- `NULL` (if not yet determined)

## Code Integration Points

### Scraper Pipeline

1. **database_handler.py** - Calls `_run_experience_analysis_on_profile()` after profile save
2. **seniority_detector.py** - Main seniority detection logic
3. **database.py** - `seed_alumni_data()` reads seniority_level from CSV and inserts into DB

### Backend API

- **app.py** - `/api/alumni` and `/api/alumni/<id>` endpoints now include seniority_level
- **backend.database.py** - SELECT queries include a.seniority_level column

## Testing

Run the comprehensive seniority detection tests:

```bash
python tests/test_seniority_detection.py
```

Tests verify:
- Correct classification for 40+ job title patterns
- Mismatch flagging thresholds
- All seniority levels properly detected

## Troubleshooting

### Profiles Not Getting Seniority Level

1. Check if column exists:
   ```sql
   DESCRIBE alumni;
   -- Should show `seniority_level` column
   ```

2. Run migration if column missing:
   ```bash
   python migrations/migrate_add_seniority_level.py
   ```

3. Backfill existing records:
   ```bash
   python scripts/backfill_seniority_levels.py
   ```

### Incorrect Classifications

The system uses keyword matching, so unusual titles might not classify correctly. For example:
- "Lead Software Engineer" may classify as Senior (lead/staff style) rather than Manager when no direct management keywords are present
- Generic titles like "Engineer Specialist" default to Mid

These cases can be manually reviewed in `flagged_for_review.txt`.

### Mismatch Flags Building Up

Review the `scraper/output/flagged_for_review.txt` file periodically. If flags are valid:
1. Manually update the seniority_level in the database
2. Or adjust the regex patterns in seniority_detector.py

If flags are false positives, they can be safely ignored.

## Future Enhancements

Potential improvements:
- Machine learning model for title classification
- Industry-specific seniority patterns
- Custom company role mappings
- Manual override capability in the UI
- Seniority trend analysis (career progression tracking)

## Related Files

- [scraper/seniority_detector.py](../scraper/seniority_detector.py) - Main detection logic
- [backend/database.py](../backend/database.py) - Database schema and seeding
- [backend/app.py](../backend/app.py) - API endpoints
- [migrations/migrate_add_seniority_level.py](../migrations/migrate_add_seniority_level.py) - Schema migration
- [scripts/backfill_seniority_levels.py](../scripts/backfill_seniority_levels.py) - Backfill script
- [tests/test_seniority_detection.py](../tests/test_seniority_detection.py) - Test suite


---

