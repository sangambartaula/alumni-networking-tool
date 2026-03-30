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
| **Executive** | Management / director / C-suite | Engineering Manager, Director of Engineering, VP, CEO |

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

The stored label is merged into `Intern/Mid/Senior/Executive`. For auditing, the system still
uses the fine-grained internal categories (Manager/Director/Executive) to decide how to flag
low `relevant_experience_months` (it does not change the final bucket).

| Seniority (stored bucket) | Min Experience used for flagging | Notes |
|----------------------------|-------------------------------------|-------|
| Senior | 12 months | Flags if less than 12 months |
| Executive | varies (manager vs director vs executive) | Uses fine-grained expected minimums |
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
- `Junior`
- `Mid`
- `Senior`
- `Manager`
- `Director`
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
- "Lead Software Engineer" without "Engineer" immediately after "Lead" may not match the Manager pattern
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
