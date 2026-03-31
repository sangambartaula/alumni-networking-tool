# Testing Guide

## Purpose

This document summarizes how to run the main automated tests for the UNT Alumni
Networking Tool and highlights the Sprint 6 coverage areas.

## Core Commands

Run the targeted Sprint 6 coverage:

```bash
pytest -q backend/tests/test_sprint_white_black_box.py
pytest -q tests/test_backend_filter_api.py
```

Run the full regression suite:

```bash
pytest -q
```

## Sprint 6 Coverage

Sprint 6 testing focuses on:

- graduation year range validation
- seniority filter validation
- years-of-experience edge cases
- major filter parsing for comma-containing values
- discipline classification and search behavior

## Notes

- Some tests validate backend logic directly.
- Some tests validate API behavior from the request/response side.
- A live database connectivity test may skip when MySQL is not configured or
  reachable in the current environment.
