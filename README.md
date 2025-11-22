# Alumni Networking Tool

A web-based application designed to help the College of Engineering connect with students and alumni using LinkedIn data. The tool includes a powerful scraper to extract alumni information and provides an interface for exploring alumni profiles, networking opportunities, and fostering engagement between current students and graduates.

---

##  Features

- **LinkedIn Alumni Scraper:** Automated scraping of UNT alumni profiles with anti-bot measures  
- **Data Timestamping:** Track when profiles were first scraped and last updated  
- **Automatic Profile Updates:** Intelligently re-scrape outdated profiles based on configurable frequency (e.g., every 6 months)  
- **Alumni Search:** Find alumni by name, graduation year, degree, or department  
- **Profile Insights:** View LinkedIn profiles, career paths, and current positions  
- **Networking:** Connect students with alumni for mentorship, internships, and professional guidance  
- **Interactive Dashboard:** Visualize alumni distribution by location, industry, and role  
- **Alumni Location Heatmap:** 🗺️ Interactive map showing alumni distribution worldwide with geocoded coordinates and location clustering  
- **Secure Data Storage:** All scraped data stored in MySQL database with tracking and MySQL connection details  

---

## Getting Started

### Prerequisites

- Python 3.10+  
- LinkedIn account (for cookie authentication)  
- Chrome/Chromium browser (for Selenium scraper)  
- Google Chrome WebDriver (chromedriver)  

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/sangambartaula/alumni-networking-tool
   cd alumni-networking-tool

2. **Create a virtual environment**
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate     # Windows

3. **Install Dependencies**
pip install -r requirements.txt

4. **Environment Variables**
Create a .env file in the project root and add LinkedIn credentials, scraper options, and database settings (see .env.example).

5. **Run the application (or scraper):**

   ```bash
   python app.py                # Run the Flask web app
   # or
   python scraper/linkedin_scraper.py  # Run the LinkedIn scraper directly

Open your browser at:
http://localhost:5000

## Project Structure

```bash
alumni-networking-tool/
│
├── .gitignore                     # Git ignore rules
├── README.md                      # Project documentation
├── requirements.txt               # Python dependencies
│
├── backend/                       # Backend code
│   ├── app.py                     # Main Flask application
│   ├── database.py                # Database connection and models with timestamp tracking
│   ├── geocoding.py               # Location geocoding service
│   └── tests/                     # Unit and integration tests
│       ├── __init__.py
│       ├── conftest.py
│       ├── test_linkedin.py       # Tests for scraper and integration
│       └── test_smoke.py          # Basic functional and sanity tests
│
├── frontend/                      # Frontend interface
│   ├── assets/                    # Images, icons, and other static files
│   ├── alumni_style.css           # Stylesheet for alumni pages
│   ├── alumni.html                # Alumni profile page
│   ├── heatmap.html               # 🗺️ Alumni location heatmap page (NEW)
│   ├── heatmap.js                 # 🗺️ Heatmap logic and interactions (NEW)
│   ├── heatmap_style.css          # 🗺️ Heatmap styling (NEW)
│   ├── app.js                     # Frontend JavaScript logic
│   └── index.html                 # Main landing page
│
└── scraper/                       # LinkedIn scraper module
    ├── linkedin_scraper.py        # Selenium-based scraper
    ├── playwright_scraper.py      # Playwright-based scraper (alternative)
    ├── parsers.py                 # Parsing logic for extracted data
   ├── backend/engineering_graduate.csv        # Input CSV of names produced by the PDF reader (ignored in Git)
    ├── linkedin_cookies.json      # Exported LinkedIn cookies (ignored in Git)
    └── output/                    # Generated data from scraper
        └── UNT_Alumni_Data.csv    # Scraped alumni data output (ignored in Git)

##  Scraper Features

### Data Tracking
- **Scraped At:** Timestamp of when each profile was first scraped
- **Last Updated:** Timestamp of the most recent profile update
- **Update Frequency:** Configurable interval (set `UPDATE_FREQUENCY` in `.env`, e.g., "6 months", "1 year")

### Smart Update Mode
On startup, the scraper checks for outdated profiles:
- Shows count of profiles that haven't been updated in the specified frequency
- Prompts user to re-scrape outdated profiles
- Only updates existing profiles if user opts in
- Preserves original scrape time while updating profile data

### Fallback Mode
- If no names CSV is provided, automatically defaults to general UNT alumni search
- Continues scraping instead of failing silently

---

##  Scraper Quick Start

### 1) Requirements
- `.env` at **repo root** with:
  - `LINKEDIN_EMAIL=...`
  - `LINKEDIN_PASSWORD=...`
  - `UPDATE_FREQUENCY=6 months` (configurable)
- Google Chrome installed
- `chromedriver` installed: `brew install chromedriver`

### 2) Run the scraper
From the `scraper` folder:
```bash
cd scraper
python3 linkedin_scraper.py

##  Testing and Quality Assurance

The project includes automated and manual tests to ensure backend reliability, scraper stability, and API correctness.

### Automated Tests
- **Pytest-based testing suite** (`backend/tests/`) covers:
  - LinkedIn OAuth login flow (mocked using `responses`)
  - API route validation for alumni data fetching and notes
  - Environment variable and app configuration verification
- **Database Health Check:** `test_db.py` ensures MySQL connectivity, table access, and query performance.
- **Scraper Validation:** `test_scraper.py` uses mock HTML to verify data extraction logic without hitting LinkedIn servers.

Run all tests:
```bash
cd backend
pytest -q

##  Testing Environment Setup

1. Copy `.env.example` to `.env` and configure database + LinkedIn test credentials.  
2. Activate your virtual environment:
   ```bash
   source venv/bin/activate

---

## 🗺️ Alumni Location Heatmap (NEW)

### Quick Start
```bash
# 1. Geocode all alumni locations (one-time setup)
cd backend
python geocoding.py

# 2. Start the app and visit the heatmap
cd ..
python backend/app.py
# Open: http://localhost:5000/heatmap
```

### Features
- **Interactive Leaflet Map** - Zoom, pan, and explore
- **Color-coded Clustering** - Blue (low) to Red (high) alumni density
- **Clickable Locations** - View alumni count and sample profiles per location
- **Real-time Statistics** - Total alumni and unique locations
- **Responsive Design** - Works on desktop, tablet, and mobile

### How It Works
1. **Geocoding Service** (`backend/geocoding.py`) converts location strings like "Denton, Texas, United States" into latitude/longitude coordinates using the free **Nominatim** API
2. **API Endpoint** (`GET /api/heatmap`) returns aggregated location data with alumni counts
3. **Frontend** renders an interactive Leaflet-based heatmap with clustered markers





