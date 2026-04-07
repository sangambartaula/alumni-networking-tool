# UNT Alumni Networking Tool

The UNT Alumni Networking Tool helps the College of Engineering staff find, review, and organize alumni outreach data in one place.

This README is a leadership-facing overview. Daily operations are in [USER_GUIDE.md](USER_GUIDE.md), technical implementation is in [SETUP.md](SETUP.md), and data logic is documented in [DATA_RULES.md](DATA_RULES.md).

## What The Tool Does

- Centralizes alumni discovery for outreach planning.
- Provides filtering by role, company, location, major, degree, graduation range, and seniority.
- Supports analytics and heatmap views for strategic reporting.
- Standardizes profile data so trends are comparable over time.
- Enforces controlled user access for institutional governance.

## Key Functionality

### Alumni Discovery

Staff can identify target alumni groups quickly using academic and professional filters.

### Outreach Readiness

Users can review profile context, store internal notes, and organize candidate lists.

### Leadership Insights

Analytics and geographic views help leadership identify concentration areas and trend patterns.

### Data Consistency

Normalization and classification rules keep majors, degrees, titles, and scoring consistent.

### Access and Security

Only approved users can access the platform; administrative actions are role-gated.

## Audience Routing

- Dean and leadership: high-level outcomes and value
- Staff operators: [USER_GUIDE.md](USER_GUIDE.md)
- Engineers and maintainers: [SETUP.md](SETUP.md)
- Data and analytics stakeholders: [DATA_RULES.md](DATA_RULES.md)

## Documentation Map

- [README.md](README.md): Executive overview
- [USER_GUIDE.md](USER_GUIDE.md): Day-to-day staff workflow
- [SETUP.md](SETUP.md): One-time engineering setup, authentication/security, testing workflow
- [DATA_RULES.md](DATA_RULES.md): Normalization, seniority, relevance, and design rationale

## Important Notes

- LinkedIn data is collected via authorized scraping of UNT search results and Connections exports (only LinkedIn OAuth touches the official API), so expect anti-automation checks and rate limits.
- Without LinkedIn Sales Navigator, routine scraping should stay conservative: about 50 profiles/day is the default safety target in this tool, and roughly 60/day should be treated as an upper bound.
- If LinkedIn shows a warning, checkpoint, unusual activity notice, or rate limit, stop scraping for a few days. Light normal LinkedIn use is okay during cooldown, but avoid visiting lots of profiles.
- Scraper activity should be run by authorized operators only.
- For multi-staff production use, operate a single centrally hosted UNT instance with shared MySQL.
