# UNT Alumni Networking Tool

## 1. Executive Summary

The UNT Alumni Networking Tool helps the College of Engineering organize alumni outreach through a single internal dashboard. It centralizes alumni profile data and outreach workflows so staff can find relevant contacts faster and follow up more consistently.

The project is still in active development and is expected to be finalized in about two months.

## 2. What Staff Can Do

- Search alumni records.
- Filter alumni by major, graduation year, UNT alumni status (yes/no/unknown), and working-while-studying status.
- View alumni seniority levels (Intern, Junior, Mid, Senior, Manager, Director, Executive).
- View an analytics dashboard with charts for graduation year trends, degree distribution, major breakdown, and more.
  - Filter the graduation year chart by a custom **From–To year range**.
  - Click **Show Heatmap** to open the location heatmap pre-filtered to the same graduation year range.
- View an alumni location heatmap with continent filters, 2D/3D toggle, and search by city/country.
  - When opened from the Analytics page with a year range, the heatmap automatically filters to show only alumni who graduated in that range and displays a dismissible banner with a "Back to Analytics" link.
- Add private outreach notes.
- Bookmark and track alumni contacts.

### Alumni Status Rules

UNT alumni status is computed from UNT education data using this logic:

- If an explicit graduation date is available (month/day/year), use that exact date.
- If only a graduation year is available, assume a default graduation date of May 15 for that year.
- If the effective graduation date has passed, classify as `yes` (alumni).
- If the effective graduation date is in the future, classify as `no` (not yet alumni).
- If no usable graduation year/date is available, classify as `unknown`.

## 3. How Data Is Updated

- The scraper collects alumni data from LinkedIn.
- Scraping uses Groq LLM extraction for experience/education parsing when available, with fallbacks for resilience.
- Alumni profile data is shared across authorized staff users.
- Notes are private per staff account.
- The scraper should only be run by authorized staff.

### Multi-Staff Usage Model

- The system should run as one centrally hosted UNT instance.
- Alumni data is shared across all authorized users.
- Notes remain private to each individual staff account.

## 4. Client Desktop Setup (Dean's Office)

This tool can run entirely on its own as a Native Desktop App (Mac or Windows), but it requires a **one-time** initial setup before it works forever.

**Prerequisites:**
You must have [Python](https://www.python.org/downloads/) installed on your computer.

**One-Time Setup:**
1. Download this entire project folder and place it somewhere permanent on your computer (e.g., your Desktop or Documents folder).
2. Open your project folder and double-click the setup script for your operating system:
   - **Mac Users**: double-click `build_mac_app.command` (if it says you lack permissions, open Terminal and run `chmod +x build_mac_app.command`).
   - **Windows Users**: double-click `build_windows_app.bat`.
3. Wait for the terminal window to finish downloading the internal requirements and building your App.
4. Once it finishes, look inside the **`dist/`** folder. You will find your permanent **UNT Alumni Scraper** Desktop App!

You only ever have to do this once! From now on, whenever you need to scrape data, just double-click that `.app` or `.exe` file! 
*(Note: Keep the app loosely inside the project folder so it can securely access your saved data).*

---

## 5. Running the Dashboard (If Already Installed)

```bash
python backend/app.py
```

Default URL:

```text
http://127.0.0.1:5000
```

The terminal startup output also shows the active URL. Use the terminal output as the source of truth.

## 5. Running the Scraper GUI (Desktop App)

A visual desktop application is available to configure and run the scraper without modifying `.env` files manually. 

### How to Build the App (First Time Only)
Because the scraper requires web drivers and specific libraries to run efficiently, you can bundle it into a standalone app so you can just double click it like any other program on your machine.

**Mac Users:**
1. Open your project folder.
2. Double-click the file named `build_mac_app.command` (if needed, run `chmod +x build_mac_app.command` in the terminal first).
3. Wait for the terminal to finish compiling. Inside the newly created `dist/` folder, you will find `UNT Alumni Scraper.app`. You can double click this file forever!

**Windows Users:**
1. Open your project folder.
2. Double-click the file named `build_windows_app.bat`.
3. Wait for compilation. Inside the `dist/` folder, you will find `UNT Alumni Scraper.exe`. Double click this anytime to open the scraper!

*Note: For the App to work optimally, ensure it remains in the same parent folder/directory alongside your `venv` folder.*

---

## 6. Running the Scraper via Terminal (Command Line Version)

```bash
python scraper/main.py
```

The scraper uses cookie-first login. If a valid saved session cookie exists, it reuses that session. If not, it falls back to account login and refreshes cookies.

### How to Export Connections.csv from LinkedIn

If you run the scraper in **Connections Mode**, you must provide your LinkedIn connections archive.
1. Visit: [https://www.linkedin.com/mypreferences/d/download-my-data](https://www.linkedin.com/mypreferences/d/download-my-data)
2. Select **Download larger data archive**, including connections, verifications, contacts, account history, and information we infer about you based on your profile and activity.
3. When available, download the data and extract the `.zip` file natively on your computer.
4. Open the extracted folder and locate the **`Connections.csv`** file. This includes all your connections with first name, last name, URLs, etc.
5. Inside the newly updated Scraper Desktop application, select **Connections** Mode, click the **Browse** button, and select this downloaded `.csv` file. You no longer need to move it around your drive!

### Discipline-Targeted UNT Search (`SEARCH_DISCIPLINES`)

When `SCRAPER_MODE=search`, you can optionally target one or more discipline buckets with:

- `SEARCH_DISCIPLINES=software`
- `SEARCH_DISCIPLINES=software,mechanical`
- `SEARCH_DISCIPLINES=embedded,construction`
- `SEARCH_DISCIPLINES=biomedical,materials`

Accepted values (case-insensitive, comma-separated, whitespace allowed):
- `software`
- `embedded`
- `mechanical`
- `construction`
- `biomedical`
- `materials`

Behavior:
- If `SEARCH_DISCIPLINES` is missing or empty, scraper behavior stays the same as normal/default search mode.
- Unknown values are ignored with a warning.
- If all provided values are invalid, scraper falls back to normal/default search mode.
- If multiple values are provided, they run sequentially in one scraper run.
- This mode starts from the canonical UNT people search page (`https://www.linkedin.com/search/results/people/?schoolFilter=%5B%226464%22%5D`), types the discipline keyword bucket into the page search bar, submits with Enter, then continues with the normal result-processing and profile-scraping flow.

Visited history behavior:
- After a profile is successfully saved, its normalized LinkedIn URL is now persisted to visited history immediately during the run (including DB update), to reduce duplicates across concurrent scraper sessions.

### Device-Safe Team Workflow (Recommended)

If teammates scrape from different machines, refresh the local CSV from cloud DB first:

```bash
python scripts/export_cloud_alumni_csv.py
```

This writes canonical `scraper/output/UNT_Alumni_Data.csv` from the shared cloud database and prevents stale local CSV data from being re-seeded into DB.

Important:
- Do not run `backend/database.py` with an old local CSV.
- Keep `SEED_ON_STARTUP=0` unless you intentionally want to seed from CSV.

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

## 8. User Guide / Tutorial

For day-to-day run and usage instructions (filters, alumni status behavior, working-while-studying behavior), see:

[USER_GUIDE.md](USER_GUIDE.md)

## 9. Data Rules Reference

For detailed normalization/classification logic (job titles, companies, majors, degrees, discipline inference, alumni status, and working-while-studying), see:

[NORMALIZATION_RULES.md](NORMALIZATION_RULES.md)

## 10. Seniority Level Classification

For information about the automatic seniority level classification feature, flagging logic, and how to populate existing records, see:

[SENIORITY_LEVEL_FEATURE.md](SENIORITY_LEVEL_FEATURE.md)

## 11. Relevance Engine & Experience Scoring

The Relevance Engine scores each alumni job as a **professional career** vs a **non-career service job**, using Groq LLM + heuristic rules. Professional roles (director, manager, analyst, engineer, etc.) are scored as relevant; HS-level jobs (cashier, crew member, etc.) are excluded. For full details, scoring logic, thresholds, and retroactive computation commands, see:

[RELEVANCE_ENGINE.md](RELEVANCE_ENGINE.md)
