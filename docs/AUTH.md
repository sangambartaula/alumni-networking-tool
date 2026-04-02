# Authentication & User Management

This document details the architecture and technical design of the authentication and user management system for the UNT Alumni Networking Tool.

## Overview

The authentication system is a hybrid approach that supports both **email/password login** and **LinkedIn OAuth**. It features:

1. **Role-Based Access Control (RBAC):** Users are assigned either an `admin` or `user` role.
2. **Whitelist Registration:** All accounts, whether created via email/password or LinkedIn OAuth, MUST exist in the `authorized_emails` whitelist.
3. **Password Security:** Strict password policy (10+ characters, mixed case, numbers, special characters) enforced with `bcrypt` hashing.
4. **Rate Limiting:** In-memory rate limiting to protect against brute-force attacks.

## Authentication Flows

### Email/Password Login
1. User submits email and password via `/login` form on `index.html`.
2. Backend rate-limits the request via `check_rate_limit(email)`.
3. Backend verifies the email exists in the `authorized_emails` whitelist.
4. Backend retrieves user from `users` table. If `password_hash` is not set but `auth_type` is `linkedin_only`, an instruction to use LinkedIn is returned.
5. `bcrypt.checkpw()` verifies the password.
6. A Flask session is established with `user_email`, `user_role`, and `must_change_password` flags.

### LinkedIn OAuth Login
1. User clicks "Sign in with LinkedIn" on `index.html`.
2. OAuth flows complete and return the user profile.
3. Backend extracts the email and verifies it against the `authorized_emails` whitelist.
4. If approved, backend performs an "Upsert" to the `users` table.
   - If a new user is created, `auth_type` defaults to `linkedin_only`.
   - If an existing user has a `password_hash`, `auth_type` is preserved as `both`.
5. A Flask session is established storing both `linkedin_profile` and `user_email`.

### Whitelist Registration
1. A user attempts to register from `register.html`.
2. Email is checked against `authorized_emails`. Unapproved emails are rejected immediately.
3. Password logic checks for complexity required by `validate_password_policy()`.
4. Password hashed using `bcrypt` and stored in DB. Role defaults to `user`.

## Role-Based Permissions

Users have a `role` of either `user` (default) or `admin`.

| Feature | `user` | `admin` |
| --- | --- | --- |
| Access Dashboard | ✅ | ✅ |
| Change Own Password | ✅ | ✅ |
| View Scraper Stats | ✅ | ✅ |
| View Admins UI | ❌ | ✅ |
| List All Users | ❌ | ✅ |
| Add User & Whitelist | ❌ | ✅ |
| Delete User | ❌ | ✅ |
| Change User Roles | ❌ | ✅ |
| Force Password Reset| ❌ | ✅ |

These permissions are enforced on backend endpoints using the `@admin_required` decorator.

## Session Management

Flask provides a secure, signed cookie-based session implementation. Access control decorators enforce session presence:

- `login_required`: Checks for `session['user_email']` or `session['linkedin_token']`, redirects to `/login` if missing. Furthermore, intercepts and forces redirection to `/change-password` if `session['must_change_password']` is true.
- `api_login_required`: Same verification as above, but returns HTTP 401 instead of a redirect.

## Removing LinkedIn OAuth

If the College of Engineering decides to completely remove LinkedIn OAuth:
1. Delete the `CLIENT_ID` and `CLIENT_SECRET` out of `.env` or configuration. The frontend login page will automatically hide the LinkedIn button.
2. Ensure all active users have used the "Create Password" flow in Settings so they have an `email_password` auth type.
3. Remove the LinkedIn routes from `app.py`.
