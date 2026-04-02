# UNT Alumni Networking Tool - User Guide

This guide is for staff users who need to run and use the dashboard and scraper day-to-day.

## 1. Start the System

1. Activate the virtual environment.
2. Start the dashboard:

```bash
python backend/app.py
```

3. Open the URL shown in terminal output (usually `http://127.0.0.1:5000`).

If this is your first setup, complete [SETUP.md](SETUP.md) first.

## 2. Run the Scraper

Run:

```bash
python scraper/main.py
```

Behavior:

- Cookie-first login is used when possible.
- If cookies are not valid, account login is used.
- Experience and education extraction uses Groq LLM when available, with parser fallbacks if unavailable.

Recommended:

- Use randomized delays (30-120 seconds per profile).
- Stop runs if LinkedIn shows checkpoint, verification, or repeated rate limiting.

## 3. Use Alumni Filters in the Dashboard

In alumni list and analytics/heatmap views, use `UNT Alumni Status`:

- `Yes`: likely graduated from UNT.
- `No`: likely still studying (future or not-yet-reached graduation point).
- `Unknown`: UNT attendance found but no reliable graduation signal.

Current-year class handling:

- For year-only graduation values (example: `Class of 2026`), the system assumes a May 15 graduation cutoff by default.
- Before May 15 of that year: status resolves to `No`.
- On/after May 15 of that year: status resolves to `Yes`.

### Alumni Directory Filters (Current)

- Search by name, role, and company.
- Multi-select filters: Location, Job Title, Company, Seniority, Engineering Discipline, Major, Degree.
- Engineering Discipline categories used in filters:
	- `Software, Data, AI & Cybersecurity`
	- `Embedded, Electrical & Hardware Engineering`
	- `Mechanical Engineering & Manufacturing`
	- `Biomedical Engineering`
	- `Construction & Engineering Management`
- If both Engineering Discipline and Major are selected, choose matching logic:
	- `Match Both` (AND): returns profiles matching both filter types.
	- `Match Either` (OR): returns profiles matching either filter type.
- Graduation year range filter (inclusive from/to, e.g., 2023 to 2025 includes 2023, 2024, and 2025).
- Working-while-studying and UNT alumni status filters.
- Relevant experience range (years) filter with an option to include unknown experience profiles.

### Validation Behavior

- Min/max filters enforce integer-only input.
- Decimal points and non-numeric input are blocked and show an inline warning.
- If min > max, filter application is blocked and an inline warning is shown.
- Directory graduation year range uses inclusive bounds (`from` and `to`).

## 4. Use Working-While-Studying Signals

Use the `Working While Studying` filter:

- `Yes`: evidence of job overlap during UNT attendance window.
- `No`: no evidence of overlap.
- `Currently` may appear in scraper-side status values when profile indicates ongoing study/work overlap.
- Month precision is used when available for job and graduation dates.
- If graduation month is missing but graduation year exists, the system uses a May 15 cutoff for overlap checks.

The backend supports both boolean and status-string representations and normalizes them for filtering.

## 5. Analytics and Heatmap Filters

### Analytics

- Supports filter panel controls for:
	- Hidden locations and companies
	- UNT alumni status
	- Graduation year range
	- Major, Degree, Seniority
- Degree and seniority selections are shown as removable active tags.
- The graduation line chart has its own view-range controls with integer/range validation.
- Analytics supports PDF export (full dashboard or selected diagrams) via the Download PDF action.

### Heatmap

- Supports filter panel controls for:
	- Hidden locations and companies
	- UNT alumni status
	- Graduation year range
	- Major, Degree, Seniority
- Degree and seniority selections are shown as removable active tags.
- Integer-only and min/max validation is applied to graduation year range input.
- Heatmap supports both 2D and 3D modes and can open pre-filtered from Analytics year ranges.

## 6. Scraper GUI Highlights

- Browse button for selecting LinkedIn `Connections.csv` in Connections mode.
- Help dialog for LinkedIn data export steps.
- Manage Flags dialog for review workflows.
- Delay presets and custom anti-ban timing controls.
- Delay validation blocks invalid values (non-integer, negative, or max < min).
- Auto-save of key GUI settings to `.env` on close.
- Per-profile persistence is now immediate during scraping:
	- Always saved locally to CSV.
	- Also written to local SQLite backup.
	- Cloud DB is attempted per profile when reachable (`DISABLE_DB=0`).
- Cloud fallback guard:
	- If cloud upload fails for 5 consecutive profiles in one run, cloud attempts are disabled for that run.
	- End-of-run warning is logged and local backup continues.
- Automatic geocoding runs during scraping for each profile location when resolvable.
- If some profile locations cannot be geocoded during a run, scraping continues (no crash) and an end-of-run warning is shown with a reminder to run optional geocode backfill later.
- `Backfill Geocode (Optional)` button remains useful for older records and repair runs.
- Built app path resolution improvements for running from `dist` bundles on macOS/Windows.
- Stop action uses terminate then forced kill fallback so scraper subprocesses do not hang.

## 7. Seniority Buckets

Current UI/API buckets are:

- Intern
- Mid
- Senior
- Manager
- Executive

## 8. Troubleshooting

- Missing data in filters: confirm scraper output is being imported into the active DB.
- LLM extraction not active: confirm `GROQ_API_KEY` is configured.
- Alumni status looks wrong: verify school is UNT-related and graduation year/date fields are present.
- For full environment setup and DB details, use [SETUP.md](SETUP.md).


<!-- Extracted from TESTING.md -->
# Testing Guide

## Purpose

This document summarizes how to run the main automated tests for the UNT Alumni
Networking Tool and highlights the current high-value coverage areas for the
Dean's Office dashboard.

## Core Commands

Run targeted backend/API coverage:

```bash
pytest -q backend/tests/test_sprint_white_black_box.py
pytest -q tests/test_backend_filter_api.py
```

Run the full regression suite:

```bash
pytest -q
```

## Current Coverage

Current automated coverage includes:

- graduation year range validation
- seniority filter validation
- years-of-experience edge cases
- major filter parsing for comma-containing values
- discipline classification and search behavior
- alumni filter API behavior
- general regression coverage across backend and scraper modules

## Notes

- Some tests validate backend logic directly.
- Some tests validate API behavior from the request/response side.
- A live database connectivity test may skip when MySQL is not configured or
  reachable in the current environment.
