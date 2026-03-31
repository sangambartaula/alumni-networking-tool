# Testing Guide

## Purpose

This document summarizes how to run the main automated tests for the UNT Alumni
Networking Tool and highlights the current high-value coverage areas for the
Dean's Office dashboard.

## Core Commands

Run targeted backend/API coverage:

```bash
pytest -q backend/tests/test_sprint_white_black_box.py
pytest -q tests/test_backend_filter_api.py
```

Run the full regression suite:

```bash
pytest -q
```

## Current Coverage

Current automated coverage includes:

- graduation year range validation
- seniority filter validation
- years-of-experience edge cases
- major filter parsing for comma-containing values
- discipline classification and search behavior
- alumni filter API behavior
- general regression coverage across backend and scraper modules

## Notes

- Some tests validate backend logic directly.
- Some tests validate API behavior from the request/response side.
- A live database connectivity test may skip when MySQL is not configured or
  reachable in the current environment.
