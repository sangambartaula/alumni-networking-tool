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

## 4. Use Working-While-Studying Signals

Use the `Working While Studying` filter:

- `Yes`: evidence of job overlap during UNT attendance window.
- `No`: no evidence of overlap.
- `Currently` may appear in scraper-side status values when profile indicates ongoing study/work overlap.
- Month precision is used when available for job and graduation dates.
- If graduation month is missing but graduation year exists, the system uses a May 15 cutoff for overlap checks.

The backend supports both boolean and status-string representations and normalizes them for filtering.

## 5. Troubleshooting

- Missing data in filters: confirm scraper output is being imported into the active DB.
- LLM extraction not active: confirm `GROQ_API_KEY` is configured.
- Alumni status looks wrong: verify school is UNT-related and graduation year/date fields are present.
- For full environment setup and DB details, use [SETUP.md](SETUP.md).
