# Alumni Networking Tool — Setup Guide

This guide will help you set up the project from scratch, including Python installation, environment configuration, and obtaining the necessary LinkedIn API credentials.

---

## 1. System Requirements

Before starting, ensure you have the following installed:

- **Python 3.10+**: [Download Python](https://www.python.org/downloads/)
  - *Note: During installation on Windows, check "Add Python to PATH".*
- **Google Chrome**: The scraper requires the Chrome browser.
- **Git**: [Download Git](https://git-scm.com/downloads)

---

## 2. Installation

1. **Clone the repository:**
   Open a terminal/command prompt and run:
   ```bash
   git clone https://github.com/sangambartaula/alumni-networking-tool
   cd alumni-networking-tool
   ```

2. **Create a Virtual Environment (Recommended):**
   Isolates project dependencies from your system Python.
   ```bash
   # Windows
   python -m venv venv
   .\venv\Scripts\activate

   # macOS/Linux
   python3 -m venv venv
   source venv/bin/activate
   ```
   *(You should see `(venv)` appear in your terminal prompt)*

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Install Spacy Language Model:**
   Used for parsing names and locations.
   ```bash
   python -m spacy download en_core_web_sm
   ```

---

## 3. Configuration (.env)

The project uses a `.env` file to store sensitive credentials.

1. **Create the file:**
   Duplicate the `.env.example` file and rename it to `.env`, or create a new file named `.env` in the project root.

2. **Fill in the variables:**
   Copy the following template and fill in your details:

   ```ini
   # --- Scraper Credentials ---
   # Used by the selenium bot to scrape profiles
   LINKEDIN_EMAIL=your_email@example.com
   LINKEDIN_PASSWORD=your_password
   
   # --- Scraper Settings ---
   SCRAPER_MODE=search
   UPDATE_FREQUENCY=6 months
   HEADLESS=false
   
   # --- Web App Login (OAuth) ---
   # See "How to Get LinkedIn API Keys" below
   LINKEDIN_CLIENT_ID=your_client_id
   LINKEDIN_CLIENT_SECRET=your_client_secret
   LINKEDIN_REDIRECT_URI=http://127.0.0.1:5000/auth/linkedin/callback
   
   # --- Database ---
   # Leave as-is to use the built-in SQLite offline database
   USE_SQLITE_FALLBACK=1
   
   # Optional: MySQL Credentials (if deploying to cloud)
   # MYSQLHOST=...
   # MYSQLUSER=...
   # MYSQLPASSWORD=...
   # MYSQL_DATABASE=...
   
   # --- Flask Security ---
   # Generate a random string for session security
   SECRET_KEY=change_this_to_something_random
   ```

---

## 4. How to Get LinkedIn API Keys

To verify users and allow them to "Sign in with LinkedIn", you need to create an app on the LinkedIn Developer Portal.

1. **Go to LinkedIn Developers:**
   Visit [https://www.linkedin.com/developers/apps/new](https://www.linkedin.com/developers/apps/new) and log in.

2. **Create an App:**
   - **App Name**: e.g., "Alumni Networking Tool (Local)"
   - **LinkedIn Page**: You'll need to link it to a company page (you can create a dummy page if needed).
   - **Privacy Policy URL**: Use `http://127.0.0.1:5000/privacy` (or any placeholder for local dev).
   - Upload a logo (any image works).
   - Check the API Terms box and click **Create App**.

3. **Request Access (Products):**
   - Go to the **Products** tab.
   - Find **"Sign In with LinkedIn using OpenID Connect"**.
   - Click **Request Access**. (It's usually approved instantly).

4. **Get Credentials:**
   - Go to the **Auth** tab.
   - Copy the **Client ID** and **Client Secret**.
   - Paste them into your `.env` file:
     ```ini
     LINKEDIN_CLIENT_ID=copied_client_id
     LINKEDIN_CLIENT_SECRET=copied_client_secret
     ```

5. **Configure Redirect URI:**
   - Still in the **Auth** tab, look for **"OAuth 2.0 settings"**.
   - Under **Authorized redirect URLs for your app**, click **Edit**.
   - Add this exact URL:
     ```
     http://127.0.0.1:5000/auth/linkedin/callback
     ```
   - Click **Update**.

---

## 5. Running the Application

1. **Start the Backend Server:**
   Ensure your virtual environment is activated (`(venv)`), then run:
   ```bash
   python backend/app.py
   ```
   You should see: `Running on http://127.0.0.1:5000`

2. **Start the Frontend:**
   Open your browser and navigate to:
   [http://127.0.0.1:5000](http://127.0.0.1:5000)

3. **Running the Scraper (Optional):**
   To start collecting data:
   ```bash
   cd scraper
   python main.py
   ```

---

## Troubleshooting

- **"ModuleNotFoundError"**: Make sure you ran `pip install -r requirements.txt` *after* activating your virtual environment.
- **Login fails**: Check that your `LINKEDIN_REDIRECT_URI` in `.env` matches exactly what you entered in the LinkedIn Developer Portal.
- **Database errors**: Ensure `USE_SQLITE_FALLBACK=1` is set in your `.env` if you don't have a MySQL server.
