# Relevance Engine

The Relevance Engine scores how relevant each job (up to 3) is to a person's field of study, using the **Groq LLM**.

## Scoring Logic

For each job, we send a prompt to the Groq API with:
- **Job title** (original, not normalized)
- **Company name** (optional context)
- **Major / field of study** (from `standardized_major` or `major`)

The LLM returns a single float in `[0.0, 1.0]`:
- `0.0` = completely unrelated (e.g., fast food worker with a CS degree)
- `0.5` = somewhat related (e.g., IT support with a CS degree)
- `1.0` = perfectly aligned (e.g., Software Engineer with a CS degree)

### Retry & Validation
- **Up to 3 attempts** per job (`MAX_RETRIES = 3`)
- Output is **strictly validated**: must parse as a single float in `[0, 1]`
- If all attempts fail, score is `None` and `is_relevant` is `None`

## Threshold

```
RELEVANCE_THRESHOLD_RELEVANT = 0.8
```

**Why 0.8?**
- Avoids false positives from tangentially related roles
- LLM output tends to cluster: ~0.5 for loosely related, ~0.85–1.0 for genuinely aligned
- The 0.8 cutoff sits in the natural gap between "maybe" and "yes"
- Configurable: change `RELEVANCE_THRESHOLD_RELEVANT` in `scraper/relevance_scorer.py`

| Score Range | Meaning          | `is_relevant` |
|------------|------------------|---------------|
| 0.0 – 0.39  | Not related       | `false`       |
| 0.4 – 0.79  | Somewhat related  | `false`       |
| 0.80 – 1.0  | Relevant          | `true`        |

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
    "job_2_relevance_score": 0.45,
    "job_2_is_relevant": False,
    "job_3_relevance_score": 0.85,
    "job_3_is_relevant": True,
    "relevant_experience_months": 36
}
```

### Output — Structured JSON (`get_relevance_json`)

```json
[
    {"title": "Software Engineer", "company": "Google", "score": 0.92, "is_relevant": true, "start_date": "Jan 2020", "end_date": "Present"},
    {"title": "Data Analyst", "company": "Meta", "score": 0.45, "is_relevant": false, "start_date": "Mar 2018", "end_date": "Dec 2019"},
    {"title": "Intern", "company": "Startup Inc", "score": 0.85, "is_relevant": true, "start_date": "Jun 2017", "end_date": "Aug 2017"}
]
```

## Edge Cases

| Scenario | Behavior |
|----------|----------|
| Person has 0 jobs | Returns empty dict / empty list |
| Person has no major | Returns empty dict / empty list |
| 1–3 jobs, none relevant | All `is_relevant=false`, `relevant_experience_months=0` |
| Job with missing title | Skipped entirely |
| Job with missing company | Still scored (company is optional context) |
| Groq fails after 3 retries | `score=None`, `is_relevant=None` |
| Groq unavailable | All scores are `None` |

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

The `scripts/compute_experience_months.py` script computes `relevant_experience_months` for alumni who already have `job_X_is_relevant` flags but missing experience months. **It does NOT re-call Groq** — it only does date arithmetic.

### Usage

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

### Frontend Experience Filter

The alumni directory sidebar includes a "Relevant Experience" range filter (0–30+ years). Setting max to 30 displays the `30+` range.

- **Backend**: `exp_min` and `exp_max` query params (in months) on `/api/alumni`
- **Frontend**: `#expMin` / `#expMax` number inputs converted to months (years × 12)
