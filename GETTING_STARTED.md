# Getting Started Guide

This guide walks you through setting up and using the Alumni Networking Tool, step by step. No programming experience required.

---

## Table of Contents

1. [What You Need Before Starting](#what-you-need-before-starting)
2. [Step 1: Download the Project](#step-1-download-the-project)
3. [Step 2: Install Python](#step-2-install-python)
4. [Step 3: Set Up the Project](#step-3-set-up-the-project)
5. [Step 4: Configure Your Settings](#step-4-configure-your-settings)
6. [Step 5: Run the Scraper](#step-5-run-the-scraper)
7. [Understanding the Output](#understanding-the-output)
8. [Common Tasks](#common-tasks)
9. [Troubleshooting](#troubleshooting)

---

## What You Need Before Starting

Before you begin, make sure you have:

- [ ] A Windows, Mac, or Linux computer
- [ ] Google Chrome browser installed
- [ ] A LinkedIn account (email and password)
- [ ] Internet connection

---

## Step 1: Download the Project

### Option A: Download as ZIP (Easiest)

1. Go to the project page on GitHub
2. Click the green "Code" button
3. Select "Download ZIP"
4. Extract the ZIP file to a folder you'll remember (like your Desktop or Documents)

### Option B: Clone with Git

If you have Git installed, open a terminal and run:
```
git clone https://github.com/sangambartaula/alumni-networking-tool
```

---

## Step 2: Install Python

### Check if Python is Already Installed

1. Open a terminal/command prompt:
   - **Windows:** Press `Windows + R`, type `cmd`, press Enter
   - **Mac:** Open "Terminal" from Applications > Utilities
   
2. Type this and press Enter:
   ```
   python --version
   ```
   
3. If you see "Python 3.10" or higher, skip to Step 3. Otherwise, continue below.

### Installing Python

1. Go to [python.org/downloads](https://www.python.org/downloads/)
2. Click "Download Python 3.x.x" (the latest version)
3. Run the downloaded installer
4. **IMPORTANT:** Check the box that says "Add Python to PATH"
5. Click "Install Now"
6. Wait for installation to complete

---

## Step 3: Set Up the Project

### Open a Terminal in the Project Folder

**Windows:**
1. Open File Explorer and navigate to the project folder
2. Click in the address bar at the top
3. Type `cmd` and press Enter

**Mac:**
1. Open Terminal
2. Type `cd ` (with a space after)
3. Drag the project folder into the terminal
4. Press Enter

### Create a Virtual Environment

A virtual environment keeps this project's files separate from your computer's other Python programs.

Type these commands one at a time, pressing Enter after each:

```
python -m venv venv
```

Then activate it:

**Windows:**
```
venv\Scripts\activate
```

**Mac/Linux:**
```
source venv/bin/activate
```

You should see `(venv)` appear at the start of your terminal line. This means the virtual environment is active.

### Install Required Packages

With the virtual environment active, run:
```
pip install -r requirements.txt
```

Wait for all packages to download and install. This may take a few minutes.

---

## Step 4: Configure Your Settings

### Create Your Configuration File

1. In the project folder, find the file named `.env.example`
2. Make a copy of it and rename the copy to `.env` (just `.env`, nothing else)
3. Open `.env` with a text editor (like Notepad or TextEdit)

### Edit Your Settings

Replace the placeholder values with your actual information:

```
# Your LinkedIn login credentials
LINKEDIN_EMAIL=your_actual_email@example.com
LINKEDIN_PASSWORD=your_actual_password

# Scraper settings (you can leave these as default)
SCRAPER_MODE=search
HEADLESS=false
TESTING=false
UPDATE_FREQUENCY=6 months

# Review Flags (Customize what gets flagged for manual review)
FLAG_MISSING_GRAD_YEAR=false
FLAG_MISSING_DEGREE=false
FLAG_MISSING_EXPERIENCE_DATA=true
```

**Important:** 
- Use your real LinkedIn email and password
- Keep `HEADLESS=false` so you can see what the scraper is doing
- Set `TESTING=true` if you want faster scraping (but higher chance of LinkedIn blocking)

### Save and Close

Save the file and close your text editor.

---

## Step 5: Run the Scraper

### Navigate to the Scraper Folder

In your terminal (with the venv active), type:
```
cd scraper
```

### Start the Scraper

Type:
```
python main.py
```

### What Happens Next

1. **Chrome Opens:** You'll see a Chrome browser window open automatically
2. **Login:** The scraper will log into LinkedIn using your credentials
3. **Scraping Begins:** The scraper will start visiting alumni profiles
4. **Progress Updates:** You'll see messages in the terminal showing what's happening

### Stopping the Scraper

- **Graceful Stop:** Type `exit` in the terminal and press Enter. The scraper will finish the current profile and stop safely.
- **Immediate Stop:** Type `force exit` or press `Ctrl+C` to stop immediately.

---

## Understanding the Output

### Where is the Data?

After the scraper runs, your data is saved in:
```
scraper/output/UNT_Alumni_Data.csv
```

You can open this file with:
- Microsoft Excel
- Google Sheets
- Any spreadsheet program

### What Data is Collected

For each alumni profile:
- Name and headline
- Location (city, state, country)
- Current job and company
- Up to 3 work experiences
- Education (school, degree, graduation year)
- Whether they worked while studying

---

## Common Tasks

### Re-scraping Specific Profiles

If some profiles have wrong data and you want to re-scrape them:

1. Open the file `scraper/output/flagged_for_review.txt`
2. Add the LinkedIn URLs you want to re-scrape (one per line):
   ```
   https://www.linkedin.com/in/john-doe-123
   https://www.linkedin.com/in/jane-smith-456
   ```
3. Save the file
4. Run the scraper: `python main.py`
5. When prompted about flagged profiles, type `y` and press Enter

### Improving Data Quality (Training the Scraper)

As you scrape more profiles, you'll encounter new companies and job titles. To teach the scraper about them:

1. Run the data validation tool:
   ```bash
   python scraper/check_data.py
   ```
2. The script checks your `UNT_Alumni_Data.csv` for any values not in its database.
3. For each unknown item, it asks you to classify it:
   - **Add to 'companies'**: If it's a company name
   - **Add to 'universities'**: If it's a school/university
   - **Add to 'job_titles'**: If it's a valid job title
   - **Skip**: If it's junk data or you're unsure
4. Changes are saved to `scraper/data/companies.json`, improving accuracy for all future scrapes.

### Running in Different Modes

Edit your `.env` file to change the `SCRAPER_MODE`:

- **search** - Find UNT alumni through LinkedIn search (default)
- **names** - Search for specific people from a CSV file
- **connections** - Scrape your LinkedIn connections
- **review** - Only re-scrape flagged profiles

### Viewing the Web Dashboard

1. Make sure the database is set up (ask your administrator)
2. In the terminal, go to the backend folder: `cd backend`
3. Run the web app: `python app.py`
4. Open your browser to: `http://localhost:5000`

---

## Troubleshooting

### "Python is not recognized"

Python isn't installed correctly or not added to PATH.
- Reinstall Python and make sure to check "Add Python to PATH"
- Restart your terminal/command prompt after installation

### "No module named X"

A required package is missing.
- Make sure your virtual environment is active (you see `(venv)`)
- Run `pip install -r requirements.txt` again

### Chrome Doesn't Open

- Make sure Google Chrome is installed
- The scraper uses Chrome's automated mode, which should work automatically

### LinkedIn Login Fails

- Double-check your email and password in the `.env` file
- Make sure there are no extra spaces
- LinkedIn may require you to verify your identity - check your email

### "Rate Limited" or Slow Performance

LinkedIn may have detected automated activity.
- Stop the scraper for a few hours
- Set `TESTING=false` in your `.env` file for slower, safer scraping
- Use the scraper during off-peak hours

### Profiles Show Wrong Data

- Add the profile URLs to `flagged_for_review.txt`
- Run the scraper and choose Review mode
- The profiles will be re-scraped with the latest extraction logic

### Database Connection Fails (Demo Mode)

If the cloud MySQL database is unreachable:

1. **SQLite Fallback Activates Automatically**
   - The app detects the error and switches to local SQLite
   - You'll see a log: "📴 Switching to offline mode (SQLite fallback)"
   - All features continue to work using cached data

2. **Testing the Fallback**
   ```bash
   cd backend
   python sqlite_fallback.py
   ```
   This shows the fallback status and runs tests.

3. **Checking Fallback Status**
   - Visit `http://localhost:5000/api/fallback-status` to see current mode
   - Or check logs for "is_offline: true/false"

4. **When Cloud Returns**
   - The app automatically syncs local changes to the cloud
   - Any conflicting changes favor the cloud (source of truth)
   - Discarded local changes are logged for review

---

## Getting Help

If you encounter issues not covered here:

1. Check the terminal for error messages
2. Make sure all steps were followed correctly
3. Contact the project maintainer with:
   - What you were trying to do
   - The exact error message
   - Your operating system (Windows/Mac/Linux)

You can reach me at sangambartaula@my.unt.edu for any questions or issues.
---

## Next Steps

Once you're comfortable with the basics:

- Explore the web dashboard to visualize alumni data
- Set up the database for persistent storage
- Use the heatmap to see alumni locations worldwide
- Automate regular scraping with scheduled tasks

Good luck, and happy networking!
