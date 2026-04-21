# UNT Alumni Networking Tool - Deployment and User Guide

## 1. Overview
The UNT Alumni Networking Tool helps the College of Engineering staff find, review, and organize alumni outreach data in one place. It centralizes alumni discovery, provides robust filtering (by engineering discipline, role, major, graduation range, etc.), and supports analytics for strategic reporting. This document provides step-by-step guidelines for installing, setting up, and running the dashboard and scraper day-to-day.

## 2. Assumptions
- The deployment environment supports Python 3.10 or newer.
- Git is installed on the host machine.
- A modern browser (Google Chrome) is available for both dashboard interactions and automated scraper sessions.
- For production shared deployment, a MySQL 8.0+ server is provisioned and network-accessible.
- The staff executing the deployment have basic command-line knowledge to activate a virtual environment and execute python scripts.

## 3. Dependencies
### External APIs and Services
- **LinkedIn OAuth**: Required for website login integration for staff authentication. If you wish to not use LinkedIn for login at all, you can skip this step.
- **Groq API**: Required for AI-powered experiential data extraction and relevance-scoring (using open-weight LLMs). It is highly recommended to use Groq API for this purpose as it is extremely fast and cheap. Without this, you may sacrifice the quality of the data.
- **OpenStreetMap / Nominatim**: Used internally for non-authenticated location geocoding. It is free to use and does not require any API key.

### Python Libraries
Key dependencies (installed via `pip install -r requirements.txt`) include:
- `Flask` / `Werkzeug`: Web application server and routing.
- `PyQt6`: Frontend GUI framework for the desktop Scraper application.
- `mysql-connector-python`: Database driver for production MySQL interactions.
- `selenium` / `undetected-chromedriver`: Headless browser interactions for LinkedIn profile scraping.
- `groq`: Client for connecting to the Groq LLM API.
- `python-dotenv`: Environment variable and configuration management.
Additional utilities include `pandas`, `pytest`, `Pillow`, etc.

## 4. Constraints
- **LinkedIn Rate Limits**: The scraping component is strictly constrained by LinkedIn's anti-automation checks. Without a Sales Navigator account, routine scraping must be heavily throttled (default safety target: ~50 profiles/day maximum). Repeated warnings or checkpoints require multi-day cooldowns to prevent account bans.
- **Production Architecture**: The system is designed to run as a **single centrally hosted instance** utilizing a shared MySQL database to ensure data parity across all staff members. Multiple disconnected staff instances will cause severe data fragmentation and overwrite conflicts.
- **Geocoding API**: Nominatim is a free geocoding service and is heavily rate-limited; large manual backfills should be run slowly or scheduled during off-hours.
- **Strict AI Extraction Requirements**: To prevent data corruption seen on different operating systems (macOS vs. Windows rendering inconsistencies), the system does **not** rely on CSS fallbacks for profile scraping when Groq is available. If LinkedIn's HTML structure changes and Groq fails to find data, the fallback is explicitly short-circuited.
- **Console UTF-8 Requirement**: Native execution on Windows devices has been stabilized by forcing all internal components to use `encoding="utf-8"`, alleviating issues where log writes and console outputs crashed the backend processing flow.

## 5. Description of Deployment Artifacts
If the project is compiled or bundled using the provided builder scripts (`build_windows_app.bat` or `build_mac_app.command`), the final deployment artifact resides in the `dist` directory. The structure of the primary repository artifacts is as follows:

```
alumni-networking-tool/
├── backend/            # Web application, API server (Flask), and filters
├── frontend/           # Web UI assets (HTML, CSS, Vanilla JS)
├── scraper/            # Data extraction engine, normalizers, and relevancy rules
├── migrations/         # Database schema update scripts
├── scripts/            # Standalone utility/maintenance scripts
├── tests/              # Regression and backend tests
├── dist/               # Contains the packaged standalone app artifacts once built
├── requirements.txt    # Python dependency mapping
└── .env                # Runtime environment configuration (credentials and keys)
```

## 6. Data Creation
The system utilizes automated data initialization and seeding. Upon the first execution of `python backend/database.py`, the following schema creations occur automatically:
- The schemas for `alumni`, `users`, `scraper_activity`, and tracking tables are created.
- The `passwords`, `seniority_level` and `roles` schema migrations are applied.
- The database connection verifies that the table rules follow utf8mb4 encoding rules.
- The authentication schema is synchronized, and the default admin accounts are securely seeded.
- **Alumni Data Seeding**: Crucially, the system ships with a locally verified dataset mapped in `scraper/output/UNT_Alumni_Data.csv`. When the database initializes, it automatically imports all pre-scraped legacy data directly into the DB. Therefore, **there is no need for external DB dump or explicit import scripts**—the codebase self-hydrates.
- *No manual SQL inputs are required other than initially creating a blank database and granting a user permissions.*

## 7. Admin Credentials
The system comes with the following pre-configured admin accounts mapping to following faculty members. These users possess unrestricted system, scrape, and API endpoints capabilities:
- `paul.krueger@unt.edu`
- `seifollah.nasrazadani@unt.edu`

To grant elevated permissions to additional staff members post-deployment, you can easily add them via the Users tab in the Web App settings once logged in.

**Note:** For the initial environment variables (`.env`), you must securely generate and supply string credentials for `SECRET_KEY` (Flask sessions), `LINKEDIN_CLIENT_SECRET`, `MYSQLPASSWORD`, and `GROQ_API_KEY`.

**Important Auth Clarification:** No default admin plaintext passwords are generated or distributed. Default admin emails are seeded with `admin` role, and admin access is granted when the user signs in with LinkedIn and LinkedIn returns a matching authorized email. Password-based self-registration is disabled when LinkedIn OAuth is configured to prevent pre-claiming another user's email.

## 8. Deployment Process

### First-Time Setup

**Primary Method: Scraper GUI Integration**
The easiest way to initialize the application is to rely on the included builder scripts and the Scraper GUI application. The GUI contains a dedicated Settings interface that installs project dependencies, manages all environment variables, and initializes your database automatically.

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/sangambartaula/alumni-networking-tool
   cd alumni-networking-tool
   ```
2. **Build the Application:**
   Instead of manually configuring a Python environment, run the dedicated system builder:
   - *(Windows)* Double-click `build_windows_app.bat` (or run `.\build_windows_app.bat`).
   - *(macOS)* Double-click `build_mac_app.command` or run `bash build_mac_app.command`.
3. **Configure Settings via GUI:**
   - Once the build succeeds, launch the resulting standalone app located in the `dist/` folder.
   - Navigate to the **Settings** tab. Here, you can paste in your API credentials, map the database host, and easily click **Install Dependencies** or **Initialize Database** without touching the terminal!

**Fallback Method: Manual CLI Configuration**  
If you prefer running from source or encounter build errors, you can bypass the GUI and perform the manual CLI setup:
1. **Create Virtual Environment:** *(Windows)* `python -m venv venv` and `.\venv\Scripts\Activate.ps1` | *(macOS)* `python3 -m venv venv` and `source venv/bin/activate`
2. **Install Dependencies:** `pip install -r requirements.txt`
3. **Configure Environment:** Copy `.env.example` to `.env` and populate your keys manually.
4. **Initialize Database:** Run `python backend/database.py`.

### Start the Central System (Day-to-day)
1. **Activate the Virtual Environment** (as shown above).
2. **Start the Dashboard:**
   ```bash
   python backend/app.py
   ```
   *Open the URL shown in terminal output (usually `http://127.0.0.1:5000`) in your browser.*

### Run the Scraper (Day-to-day)

**Primary Method: Desktop Application**
For easiest day-to-day use, launch the bundled application you created during the first-time setup:
1. Navigate to the `dist/` folder.
2. Double-click the `Alumni Scraper App` executable.
**IMPORTANT:** Do NOT move the app executable out of the `dist/` folder! It relies on its position within the project folder structure to access the `.env` configuration and backend scripts correctly.

**Fallback Method: CLI Launch**
If you wish to bypass the built app, you can launch the GUI interface directly via CLI from the project root:
```bash
python scraper_gui.py
```
*(Or manually configure your `.env` and run `python scraper/main.py` for headless terminal execution)*.

- **Available Modes**: `search` (new alumni via UNT pages), `review` (repair incomplete records), `update` (refresh existing out-of-date profiles), and `connections` (import from a LinkedIn data export).
- Always use the GUI to manage delay configurations (30-120 seconds recommended) to gracefully spoof human activity and avoid triggering LinkedIn captcha restrictions.
