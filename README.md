# UNT Alumni Networking Tool

The UNT Alumni Networking Tool helps the College of Engineering staff find, review, and organize alumni outreach data in one place.

This README is a leadership-facing overview. Deployment details and daily operations instructions are in [DEPLOYMENT.md](DEPLOYMENT.md), and automated data transformation logic is documented in [DATA_PIPELINE.md](DATA_PIPELINE.md).

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
- Staff operators and maintainers: [DEPLOYMENT.md](DEPLOYMENT.md)
- Data and analytics stakeholders: [DATA_PIPELINE.md](DATA_PIPELINE.md)

## Documentation Map

- [README.md](README.md): Executive overview
- [DEPLOYMENT.md](DEPLOYMENT.md): Step-by-step guidelines for system deployment, administration credentials, and daily operations workflow.
- [DATA_PIPELINE.md](DATA_PIPELINE.md): Data creation, AI relevance, normalization, seniority, and design rationale.

## Important Operational Notes

- **Data Sourcing Constraints**: Profile data is gathered via automated scraping of LinkedIn search results and Connections exports. As a result, the system is subject to LinkedIn's anti-automation checks and rate limiting.
- **Scraping Limits**: If operating without a LinkedIn Sales Navigator account, keep scraping activity conservative. The tool defaults to a safety target of ~50 profiles per day. Exceeding 60 profiles daily drastically increases the risk of account restrictions.
- **Handling Warnings**: If LinkedIn issues a warning, triggers a verification checkpoint, or restricts your account, **stop automated scraping immediately for several days**. When you resume, reduce your daily scraping capacity. Light, manual browsing is acceptable during this cooldown, but avoid bulk profile visits.
- **Production Architecture**: For team-wide operation, you must deploy a single, centrally-hosted application connected to a shared MySQL database. Isolated local databases will result in conflicting team data.
