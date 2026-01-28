# Alumni Networking Tool

A web-based application designed to help the College of Engineering connect with students and alumni using LinkedIn data. The tool includes a powerful scraper to extract alumni information and provides an interface for exploring alumni profiles, networking opportunities, and fostering engagement between current students and graduates.

---

## Features

### LinkedIn Alumni Scraper
- **Automated Profile Scraping** - Selenium-based scraper with anti-bot measures
- **Multiple Scraping Modes:**
  - **Search Mode** - Iterates through UNT alumni search results
  - **Names Mode** - Searches specific names from a CSV file
  - **Connections Mode** - Scrapes from your LinkedIn connections
  - **Review Mode** - Re-scrapes flagged profiles to fix data issues
  - **Update Mode** - Refreshes outdated profiles based on configurable frequency

### Data Extraction
- **Profile Information:** Name, headline, location
- **Work Experience:** Up to 3 jobs with company, title, and date ranges
- **Education:** School, degree/major, graduation year, school start date
- **Working While Studying Detection:** Automatically determines if alumni worked during school
- **Smart Entity Classification:** Tiered system using database lookup, spaCy NER, and regex to accurately distinguish job titles from company names

### Data Management
- **CSV Output** - All scraped data saved to `scraper/output/UNT_Alumni_Data.csv`
- **MySQL Database** - Persistent storage with full profile data
- **Visited Profile Tracking** - Prevents duplicate scraping across sessions
- **Flagged Profile Review** - Re-scrape specific profiles to fix data issues
- **Smart Duplicate Handling** - New data overwrites old when re-scraped
- **CSV Data Cleanup** - Utility to fix swapped job titles/companies in existing data

### Web Application
- **Alumni Search** - Find alumni by name, graduation year, degree, or department
- **Profile Insights** - View LinkedIn profiles, career paths, and current positions
- **Interactive Dashboard** - Visualize alumni distribution by location, industry, and role
- **Alumni Location Heatmap** - Interactive map showing alumni distribution worldwide
- **Secure Data Storage** - MySQL database with geocoded coordinates

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
   Create a `.env` file in the project root (see `.env.example`):
   ```
   LINKEDIN_EMAIL=your_email@example.com
   LINKEDIN_PASSWORD=your_password
   SCRAPER_MODE=search
   UPDATE_FREQUENCY=6 months
   ```

5. **Run the scraper:**
   ```bash
   cd scraper
   python main.py
   ```

For detailed step-by-step instructions, see [GETTING_STARTED.md](GETTING_STARTED.md).

---

## Scraper Modes

### Search Mode (Default)
Iterates through LinkedIn's UNT alumni search results.
```
SCRAPER_MODE=search
```

### Names Mode
Searches for specific people from an input CSV file.
```
SCRAPER_MODE=names
INPUT_CSV=backend/engineering_graduate.csv
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

## Automated Data Pipeline

After any scraping operation completes, the tool automatically:

1. **Syncs CSV to Database** - Imports/updates all profiles from `UNT_Alumni_Data.csv` to MySQL
2. **Updates Visited Profiles** - Ensures all alumni are tracked in the visited profiles table
3. **Geocodes New Locations** - Converts location strings to latitude/longitude for the heatmap

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
| working_while_studying | "yes" or "no" |
| profile_url | LinkedIn profile URL |
| scraped_at | Timestamp of scrape |

---

## Project Structure

```
alumni-networking-tool/
├── backend/
│   ├── app.py              # Flask web application
│   ├── database.py         # Database models and migrations
│   ├── sqlite_fallback.py  # SQLite offline fallback system
│   ├── geocoding.py        # Location geocoding service
│   └── alumni_backup.db    # Local SQLite backup (auto-generated)
│
├── frontend/
│   ├── alumni.html         # Alumni profile page
│   ├── heatmap.html        # Alumni location heatmap
│   └── index.html          # Main landing page
│
├── scraper/
│   ├── main.py             # Main scraper entry point
│   ├── scraper.py          # LinkedIn scraping logic
│   ├── config.py           # Configuration and constants
│   ├── utils.py            # Utility functions
│   ├── database_handler.py # CSV and history management
│   ├── entity_classifier.py # Job title/company classification
│   ├── fix_csv_data.py     # CSV cleanup utility
│   ├── data/
│   │   └── companies.json  # Curated company/university database
│   └── output/
│       ├── UNT_Alumni_Data.csv      # Scraped data
│       └── flagged_for_review.txt   # Profiles to re-scrape
│
├── .env                    # Environment variables (not in git)
├── .env.example            # Example environment file
├── requirements.txt        # Python dependencies
├── README.md               # This file
└── GETTING_STARTED.md      # Detailed setup tutorial
```

---

## Testing

Run the test suite:
```bash
# Run all tests
pytest tests/ -v

# Run entity classifier tests specifically
pytest tests/test_entity_classifier.py -v
```

Tests cover:
- LinkedIn OAuth login flow (mocked)
- API route validation
- Database connectivity
- Scraper data extraction logic
- Entity classification (job titles, companies, locations, universities)
- Text normalization (newlines, special characters)

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
| SCRAPER_MODE | search, names, connections, review | search |
| UPDATE_FREQUENCY | How often to re-scrape profiles | 6 months |
| HEADLESS | Run Chrome without UI | false |
| TESTING | Enable shorter delays for testing | false |
| INPUT_CSV | CSV file with names (names mode) | - |
| CONNECTIONS_CSV | CSV of connections (connections mode) | connections.csv |
| USE_SQLITE_FALLBACK | Enable local SQLite backup | 1 (enabled) |
| DISABLE_DB | Disable all database operations | 0 (disabled) |
| FLAG_MISSING_GRAD_YEAR | Flag profile if grad year is missing | false |
| FLAG_MISSING_DEGREE | Flag profile if degree/major is missing | false |
| FLAG_MISSING_EXPERIENCE_DATA | Flag if job title/company is inconsistent | true |

---

## License

This project is for educational purposes. Use responsibly and in accordance with LinkedIn's Terms of Service.
