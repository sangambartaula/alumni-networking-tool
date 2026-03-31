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
- then apply:
  - exact alias map (`_EXACT_MAJOR_MAP`)
  - direct canonical match
  - ordered regex patterns (`_MAJOR_PATTERNS`)
  - optional Groq fallback controlled by:
    - `MAJOR_USE_GROQ_FALLBACK` (default enabled)
    - `GROQ_API_KEY` (must be present)
- if unresolved or invalid => `Other`

Multi-entry mapping (CS&E):

- "Computer Science and Engineering" is the only raw major that maps to two canonical majors:
  `standardized_major = "Computer Science"` and `standardized_major_alt = "Computer Engineering"`
- `standardize_major_list()` returns a list (length 2 for CS&E, length 1 for all others)
- `standardize_major()` remains backward-compatible and returns only the primary entry
- In the dashboard filter: selecting CS or CE alone shows all matching entries (including dual-mapped CS&E);
  selecting both CS and CE together shows only entries mapped to both (i.e., CS&E entries)

"Other" fallback:

- Any raw major text that cannot be resolved through aliases, regex patterns, or LLM fallback
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

- `Software, Data & AI Engineering`
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
- LLM inference is only attempted when engineering signals are present

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
