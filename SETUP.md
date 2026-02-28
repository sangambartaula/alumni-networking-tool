# UNT Alumni Networking Tool - Setup Guide

This guide is for UNT IT staff and future technical maintainers setting up the project from scratch.

## 1. System Requirements

- Python 3.10 or newer
- Git
- Google Chrome (latest stable)
- MySQL 8.0 or newer for production shared deployment
- SQLite fallback support for local or demo use

## 2. Clone Repo

```bash
git clone https://github.com/sangambartaula/alumni-networking-tool
cd alumni-networking-tool
```

## 3. Create Virtual Environment

Windows PowerShell:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

macOS or Linux:

```bash
python3 -m venv venv
source venv/bin/activate
```

## 4. Install Requirements

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## 5. Configure `.env`

1. Copy the template:

```bash
copy .env.example .env
```

2. Update values in `.env`.

Template sections:

```env
# LinkedIn Credentials (used when cookie session is not reusable)
LINKEDIN_EMAIL=your-email@example.com
LINKEDIN_PASSWORD=your-password

# Data source and scraper input/output
DATA_SOURCE=csv
INPUT_CSV=backend/engineering_graduate.csv
OUTPUT_CSV=UNT_Alumni_Data.csv
RESULTS_PER_SEARCH=15

# Scraper behavior
USE_COOKIES=true
LINKEDIN_COOKIES_PATH=linkedin_cookies.json
HEADLESS=false
RATE_LIMIT_SECONDS=2
SCROLL_PAUSE_TIME=2
SCRAPER_MODE=search
UPDATE_FREQUENCY=6 months

# LinkedIn OAuth for website login
LINKEDIN_CLIENT_ID=your-client-id
LINKEDIN_CLIENT_SECRET=your-client-secret
LINKEDIN_REDIRECT_URI=http://127.0.0.1:5000/auth/linkedin/callback

# Flask session secret
SECRET_KEY=replace-with-a-long-random-secret

# MySQL connection (required for production shared deployment)
MYSQLHOST=your-mysql-host
MYSQLUSER=your-mysql-user
MYSQLPASSWORD=your-mysql-password
MYSQL_DATABASE=your-database-name
MYSQLPORT=3306

# SQLite fallback and DB mode controls
USE_SQLITE_FALLBACK=1
DISABLE_DB=0

# Optional scraper testing and review flags
TESTING=false
FLAG_MISSING_GRAD_YEAR=false
FLAG_MISSING_DEGREE=false
FLAG_MISSING_EXPERIENCE_DATA=true
```

What each block controls:

- LinkedIn Credentials: scraper login fallback when cookie session expires.
- Data source and scraper input/output: input source and output file behavior.
- Scraper behavior: cookie use, headless mode, timing controls, scraper mode.
- LinkedIn OAuth: website login integration for staff authentication.
- Flask session secret: app session security.
- MySQL connection: required settings for shared production database access.
- SQLite fallback and DB mode controls: local fallback behavior and DB disable mode.
- Optional testing and review flags: speed and profile flagging behavior.

## 6. LinkedIn OAuth Setup

1. Open the LinkedIn Developer Portal: https://www.linkedin.com/developers/apps
2. Create a LinkedIn app under the approved UNT owner.
3. Fill out app details (name, logo, company page, privacy policy, contact email).
4. Enable the LinkedIn sign-in product required by this project.
5. In Auth settings, add this redirect URL exactly:
   `http://127.0.0.1:5000/auth/linkedin/callback`
6. Copy the generated Client ID and Client Secret into `.env`.
7. Save the app settings and test login at `/login/linkedin` after starting the app.

## 7. Deployment Mode Selection

### Local or Demo (SQLite fallback)

Use this mode for local development, demos, or temporary offline scenarios.

- Set `USE_SQLITE_FALLBACK=1`.
- Set `DISABLE_DB=1` for demo-only runs without active MySQL writes.
- Keep this mode non-production.

Limitations:

- Not a shared system of record for multiple staff.
- Data may diverge from production MySQL state.
- Should not be used as the long-term hosted deployment model.

### Production Shared Deployment (MySQL required)

Use this mode for real multi-staff operation.

- Run one centrally hosted UNT instance.
- MySQL is required as the shared source of truth.
- Set `DISABLE_DB=0`.
- Provide valid `MYSQLHOST`, `MYSQLUSER`, `MYSQLPASSWORD`, `MYSQL_DATABASE`, and `MYSQLPORT`.
- `USE_SQLITE_FALLBACK=1` may stay enabled for resiliency, but MySQL remains primary.

## 8. MySQL Setup (Production Path)

If a database and user do not already exist, create them with DBA-approved credentials.

```sql
CREATE DATABASE alumni_networking
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

CREATE USER 'alumni_app'@'%' IDENTIFIED BY 'replace-with-secure-password';
GRANT ALL PRIVILEGES ON alumni_networking.* TO 'alumni_app'@'%';
FLUSH PRIVILEGES;
```

Then map those values into `.env`:

- `MYSQLHOST=<host>`
- `MYSQLUSER=<user>`
- `MYSQLPASSWORD=<password>`
- `MYSQL_DATABASE=<database>`
- `MYSQLPORT=<port>`

## 9. Initialize Database

```bash
python backend/database.py
```

This creates and verifies required tables, runs data seeding and maintenance logic, and reports initialization stats.

## 10. Run Application

```bash
python backend/app.py
```

Open the URL printed by the terminal at startup. Default local URL is usually:

```text
http://127.0.0.1:5000
```

Optional scraper run:

```bash
python scraper/main.py
```

## 11. Production Deployment Notes

- Run a single centrally hosted UNT application instance.
- Use one shared MySQL database for all staff users.
- Do not run multiple disconnected production instances.
- Restrict scraper runs to authorized operators and scheduled windows.
- Monitor logs for verification or rate-limit events and pause scraper activity when triggered.
