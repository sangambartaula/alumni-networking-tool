# UNT Alumni Networking Tool

The UNT Alumni Networking Tool helps staff find, review, and organize alumni outreach data in one place.

This README is for everyday users. Technical setup, architecture, and maintenance details are documented in separate technical guides.

## What You Can Do

- Search and filter alumni by name, role, company, location, major, degree, seniority, graduation range, and more.
- Review alumni status and working-while-studying signals.
- Use analytics and heatmap views for trend analysis.
- Save private outreach notes and bookmarks.
- Run scraper workflows from terminal or desktop GUI.

## Quick Start

If your environment is already set up:

1. Start the dashboard:

```bash
python backend/app.py
```

2. Open the URL shown in terminal output (usually `http://127.0.0.1:5000`).

3. Optional: run scraper tools:

```bash
python scraper/main.py
python scraper_gui.py
```

If this is your first time, follow [SETUP.md](SETUP.md) first.

## Access and Security

- Access is restricted to approved users via whitelist controls.
- Users can authenticate with email/password or LinkedIn OAuth.
- Admin users can manage access and user roles.

See [docs/AUTH.md](docs/AUTH.md) and [docs/SECURITY.md](docs/SECURITY.md) for technical security details.

## Documentation Map

User-oriented docs:

- [README.md](README.md) - Product overview and daily entry points
- [USER_GUIDE.md](USER_GUIDE.md) - Day-to-day usage workflows

Technical docs:

- [SETUP.md](SETUP.md) - Environment setup, deployment, and operations
- [TESTING.md](TESTING.md) - Test commands and coverage notes
- [NORMALIZATION_RULES.md](NORMALIZATION_RULES.md) - Data normalization behavior
- [SENIORITY_LEVEL_FEATURE.md](SENIORITY_LEVEL_FEATURE.md) - Seniority classification details
- [RELEVANCE_ENGINE.md](RELEVANCE_ENGINE.md) - Relevance scoring details
- [docs/AUTH.md](docs/AUTH.md) - Authentication internals
- [docs/SECURITY.md](docs/SECURITY.md) - Security controls and policy

## Important Notes

- LinkedIn can impose anti-automation checks and rate limits.
- Scraper activity should be run by authorized operators only.
- For multi-staff production use, operate a single centrally hosted UNT instance with shared MySQL.
