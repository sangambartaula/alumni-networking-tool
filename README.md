# UNT Alumni Networking Tool

## 1. Executive Summary

The UNT Alumni Networking Tool helps the College of Engineering organize alumni outreach through a single internal dashboard. It centralizes alumni profile data and outreach workflows so staff can find relevant contacts faster and follow up more consistently.

The project is still in active development and is expected to be finalized in about two months.

## 2. What Staff Can Do

- Search alumni records.
- Filter alumni by major, degree, seniority, graduation year, and location.
- View alumni seniority levels (Intern, Mid, Senior, Manager, Executive).
- Use analytics dashboards to understand alumni trends.
- Use the heatmap to view where alumni are located.
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

## 5. Running the Scraper via Terminal (Command Line Version)

```bash
python scraper/main.py
```

The scraper uses cookie-first login. If a valid saved session cookie exists, it reuses that session. If not, it falls back to account login and refreshes cookies.

For detailed scraper GUI and operating instructions, use [USER_GUIDE.md](USER_GUIDE.md) and [SETUP.md](SETUP.md).

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

For full technical details used by developers, see:

[RELEVANCE_ENGINE.md](RELEVANCE_ENGINE.md)
