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
