# Alumni Networking Tool

A web-based application designed to help the College of Engineering connect with students and alumni using LinkedIn data. The tool includes a powerful scraper to extract alumni information and provides an interface for exploring alumni profiles, networking opportunities, and fostering engagement between current students and graduates.

---

## Features

### LinkedIn Alumni Scraper
- **Automated Profile Scraping** — Selenium-based scraper with anti-bot defense layer
- **Groq AI Extraction** — LLM-powered extraction for experience and education data
- **Search Resume Checkpointing** — Search mode resumes from the last saved page (fresh for up to 7 days by default)
- **Multiple Scraping Modes:**
  - **Search Mode** — Iterates through UNT alumni search results
  - **Names Mode** — Searches specific names from a CSV file
  - **Connections Mode** — Scrapes from your LinkedIn connections
  - **Review Mode** — Re-scrapes flagged profiles; detects dead/removed URLs
  - **Update Mode** — Refreshes outdated profiles based on configurable frequency

### Data Extraction
- **Profile Information:** Name, headline, location
- **Work Experience:** Up to 3 jobs with company, title, and date ranges (Groq AI primary, CSS fallback)
- **Education:** School, degree/major, graduation year, school start date (Groq AI primary, CSS fallback)
- **Degree Normalization:** Deterministic mapping of raw degree strings to standardized forms (e.g. "B.S." → "Bachelor of Science")
- **Working While Studying Detection:** Automatically determines if alumni worked during school
- **Smart Entity Classification:** Tiered system using database lookup, spaCy NER, and regex to accurately distinguish job titles from company names
- **Engineering Discipline Classification:** Smart categorization of alumni into 7 engineering disciplines based on Job Title, Degree, and Headline priority

### Data Management
- **CSV Output** — All scraped data saved to `scraper/output/UNT_Alumni_Data.csv`
- **MySQL Database** — Persistent storage with full profile data
- **SQLite Fallback** — Automatic local backup when cloud DB is unreachable
- **Visited Profile Tracking** — Prevents duplicate scraping across sessions
- **Flagged Profile Review** — Re-scrape specific profiles to fix data issues
- **Dead URL Detection** — Identifies removed/changed LinkedIn profiles during review mode
- **Profile Blocklist** — Permanently blocks fake/placeholder profiles from being scraped or saved
- **Smart Duplicate Handling** — New data overwrites old when re-scraped
- **CSV Data Cleanup** — Utility to fix swapped job titles/companies in existing data

### Web Application
- **Alumni Search** - Find alumni by name, graduation year, degree, or department
- **Profile Insights** - View LinkedIn profiles, career paths, and current positions
- **Interactive Dashboard** - Visualize alumni distribution by location, industry, and role
- **Alumni Location Heatmap** - Interactive map showing alumni distribution worldwide
- **Secure Data Storage** - MySQL database with geocoded coordinates
- **Access Control** - Restricted to UNT faculty/staff (unt.edu emails) with whitelist support

---

## Working While Studying Logic

`working_while_studying` is computed with this order:

1. Date-based logic first (existing behavior): compare graduation year/date context with job start/end context.
2. Missing-date fallback (strict): only used when date-based status is not computable.
3. Fallback returns `yes` only when all are true:
   - At least one UNT education entry exists (`University of North Texas` or `UNT`)
   - At least one experience normalizes to `Graduate Assistant`
   - That experience employer is UNT (`University of North Texas`, `UNT Libraries`, etc.; `HUNT` does not match)
4. Otherwise fallback returns `no`.

### Retroactive backfill for existing records

If you already have rows in the database from older logic, run:

```bash
python migrations/migrate_working_while_studying.py
```

This recomputes both:
- `alumni.working_while_studying_status`
- `alumni.working_while_studying`

---

## Engineering Discipline Classification

The system automatically classifies alumni into one of 7 categories.

**Priority Logic:** Job Title > Degree > Headline
(e.g., A "Lead Software Engineer" with a "Computer Engineering" degree is classified as "Software" because their current job is the source of truth.)

**Categories:**
1. Software, Data & AI Engineering
2. Embedded, Electrical & Hardware Engineering
3. Mechanical & Energy Engineering
4. Biomedical Engineering
5. Materials Science & Manufacturing
6. Construction & Engineering Management
7. Unknown

**Auto-Inference:**
New records imported via CSV are automatically classified. The system uses an ordered keyword matching algorithm to ensure accurate categorization (e.g., "Embedded Systems" takes precedence over generic "Systems").

**Retroactive Updates:**
To re-classify existing alumni records (e.g., after updating logic):
```bash
python backend/backfill_disciplines.py
```

---

## Quick Start

### Prerequisites
- Python 3.10+
- Google Chrome browser
- LinkedIn account
- MySQL database (for web app features)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/sangambartaula/alumni-networking-tool
   cd alumni-networking-tool
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   venv\Scripts\activate     # Windows
   source venv/bin/activate  # macOS/Linux
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables:**
   Create a `.env` file in the project root. See the [Setup Guide](SETUP_GUIDE.md) for a full template and instructions on getting API keys.

5. **Run the application:**
   ```bash
   python backend/app.py
   ```

For a complete walkthrough including LinkedIn API setup, see [SETUP_GUIDE.md](SETUP_GUIDE.md).

---

## LinkedIn Login Setup

For LinkedIn login to work, you must create your own LinkedIn App on the [LinkedIn Developer Portal](https://www.linkedin.com/developers/). This will generate a **Client ID** and **Client Secret**. Update the `.env` file with these credentials to enable authentication.

---

## Scraper Modes

### Search Mode
Iterates through LinkedIn's UNT alumni search results.
```
SCRAPER_MODE=search
```

Search mode automatically checkpoints the current results page in local SQLite state and resumes from that page on the next run when the checkpoint is still recent.

```
SCRAPE_RESUME_MAX_AGE_DAYS=7
```

If no results are found on a page, the checkpoint resets to page `1` for the next run.

### Names Mode
Searches for specific people from an input CSV file.
```
SCRAPER_MODE=names
INPUT_CSV=engineering_graduates.csv
```

### Connections Mode
Scrapes alumni from your LinkedIn connections.
```
SCRAPER_MODE=connections
CONNECTIONS_CSV=connections.csv
```

### Review Mode
Re-scrapes profiles listed in `scraper/output/flagged_for_review.txt`.
```
SCRAPER_MODE=review
```
Or: Add URLs to `flagged_for_review.txt` and the scraper will prompt you on startup.

**Dead URL Detection:** During review mode, the scraper detects when a LinkedIn profile returns "This page doesn't exist". At the end of the session, all dead URLs are listed and you're prompted to remove them from the database and history files:
```
============================================================
⚠️  3 DEAD / REMOVED PROFILES DETECTED:
============================================================
  💀 https://linkedin.com/in/someone
  💀 https://linkedin.com/in/anotherone
============================================================

Remove these profiles from database & history? [y/N]: y
✅ Dead profiles cleaned from all data sources.
```

### Update Mode
Automatically detects profiles older than `UPDATE_FREQUENCY` and prompts to re-scrape.

---

## Flagging Profiles for Review

To fix bad data in existing profiles:

1. Add LinkedIn URLs to `scraper/output/flagged_for_review.txt` (one per line):
   ```
   https://www.linkedin.com/in/john-doe-123
   https://www.linkedin.com/in/jane-smith-456
   # This is a comment (ignored)
   ```

2. Run the scraper - it will detect flagged profiles and prompt you:
   ```
   ==================================================
   Found 2 profiles flagged for review.
   ==================================================
   >>> Run REVIEW mode to re-scrape them? (y/n): y
   ```

3. Successfully scraped profiles are removed from the file; failed ones remain for retry.

---

## Groq AI Extraction

The scraper uses [Groq](https://console.groq.com/) LLM to extract structured data from LinkedIn HTML, with CSS-based extraction as a fallback.

### How It Works
1. LinkedIn profile HTML is cleaned and structured
2. Sent to Groq (default model: `llama-3.1-8b-instant`, configurable via `GROQ_MODEL`)
3. Response is parsed as JSON and validated
4. Falls back to CSS selectors if Groq is unavailable or returns invalid data

### Modules
| Module | Purpose |
|--------|--------|
| `groq_client.py` | Shared client, API key handling, JSON parsing, debug HTML saving |
| `groq_extractor_experience.py` | Experience extraction (up to 3 jobs) |
| `groq_extractor_education.py` | Education extraction (degree, school, dates) |

### Configuration
```bash
# .env
GROQ_API_KEY=gsk_...         # Get from https://console.groq.com/keys
USE_GROQ=true                # Enable/disable Groq (falls back to CSS)
GROQ_MODEL=llama-3.1-8b-instant
SCRAPER_DEBUG_HTML=true      # Save raw HTML for debugging
```

---

## Degree Normalization

Raw degree strings from LinkedIn are normalized to standardized forms using a deterministic mapping.

| Raw Input | Normalized |
|-----------|------------|
| B.S. | Bachelor of Science |
| Master of Science in Computer Science | Master of Science |
| Ph.D. | Doctor of Philosophy |
| MBA | Master of Business Administration |

### Retroactive Migration
To normalize degrees for existing alumni records:
```bash
python migrations/migrate_normalize_degrees.py
```

---

## Profile Blocklist

Fake or placeholder LinkedIn profiles can be permanently blocked. Blocked profiles are:
- Skipped during scraping (all modes)
- Rejected by `save_profile_to_csv`
- Never saved to the database

To add a profile to the blocklist, add its LinkedIn slug to `BLOCKED_PROFILE_SLUGS` in `scraper/config.py`:
```python
BLOCKED_PROFILE_SLUGS = {
    "davidmartinez",
    "emilybrown",
    "johnsmith",
    # Add more slugs here...
}
```

---

## Automated Data Pipeline

After any scraping operation completes, the tool automatically:

1. **Syncs CSV to Database** — Imports/updates all profiles from `UNT_Alumni_Data.csv` to MySQL
2. **Updates Visited Profiles** — Ensures all alumni are tracked in the visited profiles table
3. **Geocodes New Locations** — Converts location strings to latitude/longitude for the heatmap

This automation runs in the `finally` block of `main.py`, so it happens even if you stop the scraper early.

### Manual Sync

If you need to run the sync manually (e.g., after importing data from another source):

```bash
cd backend

# Sync CSV to database (also runs migrations and stats)
python database.py

# Geocode missing locations
python geocoding.py
```

### CSV Data Cleanup

If you notice swapped job titles and company names in the CSV:

```bash
# Fix swapped entries and normalize text
python scraper/fix_csv_data.py
```

This script:
- Normalizes text (removes newlines, special characters)
- Auto-detects and fixes swapped job_title/company fields
- Applies known fixes for specific profiles
- Creates a backup before making changes
- **Interactive Data Validation**:
  ```bash
  # Check data against known companies/universities and train the classifier
  python scraper/check_data.py
  ```
  Scans `UNT_Alumni_Data.csv` and prompts you to verify new companies, universities, or job titles. This updates `scraper/data/companies.json` to improve future classification accuracy.

---

## Output Data

### CSV Columns
| Column | Description |
|--------|-------------|
| name | Full name |
| headline | LinkedIn headline |
| location | City, State, Country |
| job_title | Current/latest job title |
| company | Current/latest company |
| job_start_date | Job start date |
| job_end_date | Job end date |
| exp2_title, exp2_company, exp2_dates | Second experience |
| exp3_title, exp3_company, exp3_dates | Third experience |
| education | School name (UNT) |
| major | Degree/field of study |
| school_start_date | When they started school |
| graduation_year | Expected or actual graduation |
| working_while_studying | "yes", "currently", or "no" (stored status + boolean flag in DB) |
| profile_url | LinkedIn profile URL |
| scraped_at | Timestamp of scrape |

---

## Project Structure

```
alumni-networking-tool/
├── backend/
│   ├── app.py                    # Flask web application
│   ├── database.py               # Database models and migrations
│   ├── degree_normalization.py   # Deterministic degree normalization
│   ├── job_title_normalization.py # Deterministic job title normalization
│   ├── backfill_disciplines.py   # Engineering discipline classification
│   ├── sqlite_fallback.py        # SQLite offline fallback system
│   ├── geocoding.py              # Location geocoding service
│   └── alumni_backup.db          # Local SQLite backup (auto-generated)
│
├── frontend/
│   ├── alumni.html               # Alumni profile page
│   ├── heatmap.html              # Alumni location heatmap
│   └── index.html                # Main landing page
│
├── scraper/
│   ├── main.py                   # Main scraper entry point
│   ├── scraper.py                # LinkedIn scraping logic
│   ├── config.py                 # Configuration, constants, and blocklist
│   ├── groq_client.py            # Shared Groq LLM client infrastructure
│   ├── groq_extractor_experience.py # AI experience extraction
│   ├── groq_extractor_education.py  # AI education extraction
│   ├── utils.py                  # Utility functions
│   ├── database_handler.py       # CSV and history management
│   ├── entity_classifier.py      # Job title/company classification
│   ├── fix_csv_data.py           # CSV cleanup utility
│   ├── defense/                  # Anti-bot defense layer
│   │   ├── navigator.py          # Safe navigation with health checks
│   │   ├── backoff.py            # Exponential backoff controller
│   │   └── page_health.py        # Page health verification
│   ├── data/
│   │   └── companies.json        # Curated company/university database
│   └── output/
│       ├── UNT_Alumni_Data.csv        # Scraped data
│       ├── flagged_for_review.txt     # Profiles to re-scrape
│       └── visited_history.csv        # Visited profile tracking
│
├── migrations/
│   ├── migrate_normalize_titles.py   # Retroactive job title normalization
│   ├── migrate_normalize_degrees.py  # Retroactive degree normalization
│   └── migrate_working_while_studying.py  # Retroactive working-while-studying recompute
│
├── tests/
│   ├── test_degree_normalization.py  # Degree normalization tests
│   ├── test_groq_imports.py          # Groq module import tests
│   └── test_scraper_logic.py         # Scraper logic tests
│
├── .env                    # Environment variables (not in git)
├── .env.example            # Example environment file
├── requirements.txt        # Python dependencies
├── README.md               # This file
├── SETUP_GUIDE.md          # Detailed setup tutorial
└── GETTING_STARTED.md      # Getting started guide
```

---

## Testing

Run the test suite:
```bash
# Run full suite (root + backend tests)
./venv/bin/pytest -q

# Run entity classifier tests specifically
./venv/bin/pytest -q tests/test_entity_classifier.py
```

Tests cover:
- API route validation (including alumni filters)
- Database + SQLite fallback behavior
- Scraper data extraction logic
- Groq module imports and refactoring validation
- Degree normalization (exact matches, abbreviations, prefixes, edge cases)
- Entity classification (job titles, companies, locations, universities)
- Text normalization (newlines, special characters)
- Search resume state and dead URL cleanup safety
- Working-while-studying fallback and UNT matching rules

---

## Heatmap Feature

### Setup
```bash
# 1. Geocode all alumni locations
cd backend
python geocoding.py

# 2. Start the app
python app.py
# Open: http://localhost:5000/heatmap
```

### Features
- Interactive Leaflet map with zoom and pan
- Color-coded clustering (blue = low, red = high density)
- Clickable locations showing alumni count and sample profiles
- Real-time statistics

---

## SQLite Fallback (Offline Mode)

The application includes a local SQLite database backup that ensures the app continues to work when the cloud MySQL database is unreachable—perfect for demos or network issues.

### How It Works

1. **On Startup:** Tries to connect to cloud MySQL
   - If reachable → syncs a copy to local `alumni_backup.db`
   - If unreachable → uses the existing local SQLite backup

2. **In Offline Mode:** 
   - All queries use the local SQLite database
   - Background thread silently retries cloud connection every 30 seconds
   - Any changes are recorded locally for later sync

3. **On Reconnection:**
   - Local changes are pushed to cloud (with smart merge)
   - Cloud updates are pulled to local
   - Conflicting changes → cloud wins (source of truth)

### Testing SQLite Fallback

```bash
cd backend
python sqlite_fallback.py
```

This runs built-in tests and shows:
- Connection status (online/offline)
- Data sync status
- Table row counts
- Test results (WAL mode, table existence, etc.)

### Configuration

Add to `.env`:
```bash
# Enable SQLite fallback (default: enabled)
USE_SQLITE_FALLBACK=1

# Disable all DB operations (dev mode only)
DISABLE_DB=0
```

### API Endpoint

Check fallback status via API:
```
GET /api/fallback-status
```

Returns:
```json
{
  "success": true,
  "enabled": true,
  "is_offline": false,
  "last_cloud_sync": "2024-01-26T20:00:00+00:00",
  "pending_changes": 0,
  "discarded_changes": 0,
  "table_counts": {"alumni": 70, "users": 5, ...}
}
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| LINKEDIN_EMAIL | LinkedIn login email | Required |
| LINKEDIN_PASSWORD | LinkedIn login password | Required |
| SCRAPER_MODE | search, names, connections, review | names |
| SCRAPE_RESUME_MAX_AGE_DAYS | Max age (days) for search page checkpoint resume | 7 |
| UPDATE_FREQUENCY | How often to re-scrape profiles | 6 months |
| HEADLESS | Run Chrome without UI | false |
| TESTING | Enable shorter delays for testing | false |
| USE_COOKIES | Reuse cached LinkedIn cookies before credential login | false |
| INPUT_CSV | CSV file with names (names mode) | engineering_graduates.csv |
| CONNECTIONS_CSV | CSV of connections (connections mode) | connections.csv |
| GROQ_API_KEY | Groq API key for AI extraction | — |
| USE_GROQ | Enable Groq LLM extraction | true |
| GROQ_MODEL | Groq model used for LLM extraction/classification | llama-3.1-8b-instant |
| SCRAPER_DEBUG_HTML | Save raw HTML for debugging | false |
| USE_SQLITE_FALLBACK | Enable local SQLite backup | 1 (enabled) |
| DISABLE_DB | Disable all database operations | 0 (disabled) |
| FLAG_MISSING_GRAD_YEAR | Flag profile if grad year is missing | false |
| FLAG_MISSING_DEGREE | Flag profile if degree/major is missing | false |
| FLAG_MISSING_EXPERIENCE_DATA | Flag if job title/company is inconsistent | true |

---

## License

This project is for educational purposes. Use responsibly and in accordance with LinkedIn's Terms of Service.
