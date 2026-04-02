# UNT Alumni Networking Tool

## 1. Executive Summary

The UNT Alumni Networking Tool helps the College of Engineering organize alumni outreach through a single internal dashboard. It centralizes alumni profile data and outreach workflows so staff can find relevant contacts faster and follow up more consistently.

The project is still in active development and is expected to be finalized in about two months.

## 2. What Staff Can Do

- Search alumni records.
- Filter alumni by major, degree, seniority, graduation year range, and location.
- View alumni seniority levels (Intern, Mid, Senior, Manager, Executive).
- Use analytics dashboards to understand alumni trends.
- Use the heatmap to view where alumni are located.
- Add private outreach notes.
- Bookmark and track alumni contacts.

## 3. Account Access and Authentication

Staff have two ways to log into the alumni dashboard safely:
1. **Email & Password**: You can register an account directly if your email is whitelisted.
2. **LinkedIn OAuth**: You can use your LinkedIn account.

If you initially sign up with LinkedIn, you can easily create an Email/Password credential later by visiting the **Settings** menu inside the app. Check the `docs/AUTH.md` document for deeper technical flows.

### Whiteness and Security
Access is strictly managed via an email allow-list (whitelist). Even if a user connects via LinkedIn, access is instantly denied if the email address is not actively marked as authorized. 

### User Roles
The tool features two distinct roles:
- **User**: Can search alumni, view analytics, use the heatmap, track bookmarks, and add notes.
- **Admin**: Has all User privileges, plus the ability to manage other users securely from their Settings menu (e.g. adding new users to the whitelist, assigning rows, performing forced password resets, and deleting user access).

## 4. How Data Is Updated

- The scraper collects alumni data from LinkedIn.
- Alumni profile data is shared across authorized staff users.
- Notes are private per staff account.
- The scraper should only be run by authorized staff.

### Multi-Staff Usage Model

- The system should run as one centrally hosted UNT instance.
- Alumni data is shared across all authorized users.
- Notes remain private to each individual staff account.

## 5. Setup and Installation

All first-time setup and installation steps are maintained in [SETUP.md](SETUP.md).

Use README as the project overview, and use setup/user docs for operational details.

## 6. Running the Dashboard

```bash
python backend/app.py
```

Default URL:

```text
http://127.0.0.1:5000
```

The terminal startup output also shows the active URL. Use the terminal output as the source of truth.

## 6. Running the Scraper via Terminal

```bash
python scraper/main.py
```

The scraper uses cookie-first login. If a valid saved session cookie exists, it reuses that session. If not, it falls back to account login and refreshes cookies.

For detailed scraper GUI and operating instructions, use [USER_GUIDE.md](USER_GUIDE.md) and [SETUP.md](SETUP.md).

## 8. Documentation Index

- Setup and installation: [SETUP.md](SETUP.md)
- Day-to-day usage: [USER_GUIDE.md](USER_GUIDE.md)
- Authentication and Login: [docs/AUTH.md](docs/AUTH.md)
- Security Model: [docs/SECURITY.md](docs/SECURITY.md)
- Testing and regression commands: [TESTING.md](TESTING.md)
- Data rules and normalization: [NORMALIZATION_RULES.md](NORMALIZATION_RULES.md)
- Seniority classification details: [SENIORITY_LEVEL_FEATURE.md](SENIORITY_LEVEL_FEATURE.md)
- Relevance engine details: [RELEVANCE_ENGINE.md](RELEVANCE_ENGINE.md)

## 9. Important Limitations

- LinkedIn can enforce verification checks and rate limits.
- Aggressive scraper settings increase challenge and lock risk.
- A valid LinkedIn session is required.
- This is an internal UNT tool only.
