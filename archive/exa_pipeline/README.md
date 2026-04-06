## Exa Alumni Prototype

This folder contains an archived prototype for building a UNT engineering alumni dataset with Exa and Groq.

Status: archived, not production-ready.

Why it was archived:
- Gather speed was good, but extraction accuracy did not meet the bar for sponsor-facing use.
- Exa highlight quality was inconsistent on grouped roles, current-job titles, and some education rows.
- The active scraper remains the source of truth because it is materially more accurate.

What is here:
- `exa.py`: Exa gather-stage prototype.
- `exa_groq.py`: Exa-to-Groq normalization prototype.
- `tests/`: regression tests that were used while tuning the pipeline.
- `artifacts/`: local output directory for raw and cleaned prototype files.

Notes:
- The scripts default to writing under `archive/exa_pipeline/artifacts/`.
- Archived tests are intentionally outside the main `tests/` folder so they do not run with the active suite.
- `exa-py` was removed from root dependencies because this prototype is no longer part of the live app.
