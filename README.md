# UNT Alumni Networking Tool

## 1. Executive Summary

The UNT Alumni Networking Tool helps the College of Engineering organize alumni outreach through a single internal dashboard. It centralizes alumni profile data and outreach workflows so staff can find relevant contacts faster and follow up more consistently.

The project is still in active development and is expected to be finalized in about two months.

## 2. What Staff Can Do

- Search alumni records.
- Filter alumni by major and graduation year.
- View an alumni location heatmap.
- Add private outreach notes.
- Bookmark and track alumni contacts.

## 3. How Data Is Updated

- The scraper collects alumni data from LinkedIn.
- Alumni profile data is shared across authorized staff users.
- Notes are private per staff account.
- The scraper should only be run by authorized staff.

### Multi-Staff Usage Model

- The system should run as one centrally hosted UNT instance.
- Alumni data is shared across all authorized users.
- Notes remain private to each individual staff account.

## 4. Running the Dashboard (If Already Installed)

```bash
python backend/app.py
```

Default URL:

```text
http://127.0.0.1:5000
```

The terminal startup output also shows the active URL. Use the terminal output as the source of truth.

## 5. Running the Scraper (Simple Version)

```bash
python scraper/main.py
```

The scraper uses cookie-first login. If a valid saved session cookie exists, it reuses that session. If not, it falls back to account login and refreshes cookies.

### Recommended Operating Guidance

- Delays must be randomized to mimic human behavior.
- Recommended delay range: 30-120 seconds per profile.
- Safe or cautious delay range: 120-600 seconds per profile.
- Delays below 15-45 seconds are not recommended for normal operation.
- Recommended session cap: 100 profiles or 3 hours, whichever comes first.
- Stop immediately for LinkedIn verification or checkpoint prompts.
- Stop immediately for repeated rate-limit signals.
- Stop immediately for repeated authwall or login loops.
- Stop immediately for unusual spikes in scrape failures.

## 6. Important Limitations

- LinkedIn can enforce verification checks and rate limits.
- Aggressive scraper settings increase challenge and lock risk.
- A valid LinkedIn session is required.
- This is an internal UNT tool only.

## 7. Link to Setup Guide

For full installation instructions, see SETUP.md.

[SETUP.md](SETUP.md)
