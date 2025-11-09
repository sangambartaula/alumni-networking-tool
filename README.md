# Alumni Networking Tool

A web-based application designed to help the College of Engineering connect with students and alumni using LinkedIn data. The tool includes a powerful scraper to extract alumni information and provides an interface for exploring alumni profiles, networking opportunities, and fostering engagement between current students and graduates.

---

##  Features

- **LinkedIn Alumni Scraper:** Automated scraping of UNT alumni profiles with anti-bot measures  
- **Alumni Search:** Find alumni by name, graduation year, degree, or department  
- **Profile Insights:** View LinkedIn profiles, career paths, and current positions  
- **Networking:** Connect students with alumni for mentorship, internships, and professional guidance  
- **Interactive Dashboard:** Visualize alumni distribution by location, industry, and role  
- **Secure Data Storage:** All scraped data stored locally in CSV format  

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
│   ├── database.py                # Database connection and models
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

##  Scraper Quick Check (local)

Use these helper commands to validate your local scraper setup without changing any scraper code.

### 1) Requirements
- `.env` at **repo root** with:
  - `LINKEDIN_EMAIL=...`
  - `LINKEDIN_PASSWORD=...`
- Google Chrome installed (or run Selenium headless if supported)
- `chromedriver` installed (Homebrew): `brew install chromedriver`

### 2) Run the scraper (short test)
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



