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
# LinkedIn scraper fallback credentials (optional)
LINKEDIN_EMAIL=your-email@example.com
LINKEDIN_PASSWORD=your-password

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
MYSQLPORT=37157

# AI key
GROQ_API_KEY=your-groq-key
```

What each block controls:

- LinkedIn scraper fallback credentials: used only when a valid LinkedIn session cookie is unavailable.
- LinkedIn OAuth: website login integration for staff authentication.
- Flask session secret: **CRITICAL** app session security. Must be a long, random string in production (e.g. `openssl rand -hex 32`).
- MySQL connection: required settings for shared production database access.
- AI key: enables Groq-powered extraction and relevance-related features.

Important: several scraper defaults now live in `scraper/config.py` as code-level constants rather than `.env` keys (for example scraper mode, default delay windows, GUI stop limits, and discipline defaults).

Also note: the scraper GUI may write optional runtime keys into `.env` (for example `HEADLESS`, `USE_COOKIES`, `SCRAPER_DEBUG_HTML`, `SCRAPE_RESUME_MAX_AGE_DAYS`, `USE_GROQ`) when you save settings. These are valid operational overrides, but not required in a minimal fresh template.

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

These two keys are optional backend toggles and can be added to `.env` only when needed.

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

The authentication schema (adding `password_hash`, `role`, etc.) and the seeding of the 7 default admin accounts will run **automatically** when the app starts. If you wish to trigger this manually at any time, run:

```bash
python scripts/seed_admins.py
```

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

## 11. Scraper GUI Setup and Use

The desktop GUI (`scraper_gui.py`) supports:

- Scraper mode selection (`search`, `review`, `connections`)
- Scraper update mode (`update`) for refreshing existing alumni records
- Connections CSV browsing with file picker
- Review flag management dialog
- Delay presets and custom delay ranges
- Auto-save of GUI settings into `.env`
- Database upload/geocode action from GUI
- Mode-specific help/info panel and mode-aware `?` guidance

Update mode behavior:

- Selects already-scraped alumni that are due based on `UPDATE_FREQUENCY`.
- Processes queue in ascending `last_updated` order (oldest first).

Run locally:

```bash
python scraper_gui.py
```

### 11.1 One-Time Scraper GUI Setup Checklist

Use this checklist once per machine (or after recreating the virtual environment):

1. Activate the project virtual environment.
2. Ensure dependencies are installed:

```bash
pip install -r requirements.txt
```

3. Launch the GUI:

```bash
python scraper_gui.py
```

4. On first GUI launch:
- Enter LinkedIn email/password.
- Click `Refresh Status` in Preflight Status.
- If any setup-needed warning appears, click `Install Dependencies` and wait for completion.

5. Select scraper mode and delays, then run a short test scrape.

6. Optional build verification:
- Windows: `build_windows_app.bat`
- macOS: `build_mac_app.command`

If the virtual environment is recreated later, repeat this checklist.

Built desktop apps from `build_mac_app.command` and `build_windows_app.bat` include path-resolution fixes so worker subprocesses can find project scripts when launched from packaged `dist` outputs.

## 12. Production Deployment Notes

- Run a single centrally hosted UNT application instance.
- Use one shared MySQL database for all staff users.
- Do not run multiple disconnected production instances.
- Restrict scraper runs to authorized operators and scheduled windows.
- Monitor logs for verification or rate-limit events and pause scraper activity when triggered.

## 13. API Validation and Error Contract

For developer-facing API clients, numeric filters are validated strictly and return HTTP 400 on invalid inputs.

Structured validation errors use this shape:

```json
{
  "error": {
    "code": "validation_error",
    "message": "exp_min cannot be greater than exp_max.",
    "field": "exp_min"
  }
}
```

Current strict numeric validation includes:

- `/api/alumni`
  - `exp_min`, `exp_max`: optional non-negative integers; if both exist then `exp_min <= exp_max`
  - `grad_year`: repeated/comma-separated integer list; invalid values return HTTP 400
- `/api/heatmap`
  - `grad_year_from`, `grad_year_to`: optional non-negative integers; if both exist then `grad_year_from <= grad_year_to`

Notes for client implementations:

- Do not assume invalid values are ignored; requests now fail fast with HTTP 400.
- Surface `error.message` directly in UI when possible.
- Use `error.field` to map server-side validation feedback to the correct form input.

## 14. Authentication and Authorization Technical Notes

The backend supports both email/password and LinkedIn OAuth login.

Core flow behavior:

1. Login attempts are rate-limited.
2. Email must pass whitelist checks (`authorized_emails`).
3. Session is established only for authorized users.
4. User permissions are enforced by role (`user` or `admin`).

Admin-only operations are protected server-side and are not trusted from UI state alone.

If LinkedIn OAuth is disabled in deployment, remove OAuth credentials from `.env` and rely on email/password access paths.

## 15. Security Practices

- Passwords are stored using bcrypt hashes (never plaintext).
- Password policy is enforced for minimum complexity.
- Session integrity depends on a strong `SECRET_KEY`.
- Authorization is enforced with explicit whitelist and role checks.
- Database operations use parameterized queries for SQL injection protection.

## 16. Testing Workflow

Run targeted backend/API tests:

```bash
pytest -q backend/tests/test_sprint_white_black_box.py
pytest -q tests/test_backend_filter_api.py
```

Run full regression suite:

```bash
pytest -q
```

Current high-value automated coverage includes:

- graduation year range validation
- seniority filter validation
- years-of-experience edge cases
- major/discipline parsing and normalization behavior
- backend alumni filter API correctness
- scraper/backend regression scenarios

## 17. Terminal Interactivity Notes

When launched from the GUI, scraper runs support interactive stdin via the **Terminal Input** box under Console Output.

- Use that input box to answer runtime prompts (for example dead-profile cleanup confirmation in review mode).
- GUI stop controls still support graceful/immediate stop signals programmatically.
