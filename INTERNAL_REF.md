# INTERNAL_REF

## Purpose
This document is an internal architecture reference for the backend Blueprint refactor.
It records where functionality from the historical monolithic `backend/app.py` moved,
which compatibility surfaces remain in place, and what residual risks are known.

## Scope and Baseline
- Pre-refactor baseline used for comparison: `backend/app.py` at commit `8c8666f`.
- Current architecture baseline: modular backend under `backend/routes/`, `backend/middleware.py`, `backend/utils.py`, and slim `backend/app.py` bootstrap.

## Runtime Boot Sequence
1. `backend/app.py` constructs Flask app with `static_folder=../frontend/public`.
2. `Config.apply(app)` loads environment and applies runtime flags.
3. Optional access-log filtering is enabled via `configure_werkzeug_access_logging()`.
4. Blueprints are registered in this order:
   - `core_bp`
   - `auth_bp`
   - `alumni_bp`
   - `analytics_bp`
   - `interaction_bp`
   - `admin_bp`
   - `scraper_bp`
5. Global error handlers are registered for 404 and 500.

Note: all blueprints are currently mounted with no `url_prefix`, preserving legacy route paths exactly.

## Module Responsibilities
- `backend/app.py`
  - Application bootstrap only.
  - Blueprint registration.
  - Error handlers.
  - Compatibility exports for legacy tests/patch points.
- `backend/config.py`
  - Environment loading and app config hydration.
  - Secret key policy (strict in production).
- `backend/middleware.py`
  - Auth/session helpers and decorators.
  - Authorized-domain/whitelist gate.
  - User ID resolution from session.
  - HTTP access-log suppression filter for oversized URLs.
- `backend/utils.py`
  - Generic helpers intended to avoid route-module coupling.
  - Shared list parsing and ranking helper.
- `backend/routes/core_routes.py`
  - Root/static page routes.
- `backend/routes/auth_routes.py`
  - Login/register/password lifecycle.
  - LinkedIn OAuth entry and callback.
- `backend/routes/alumni_routes.py`
  - Alumni listing/filter/detail/update APIs.
  - Alumni and events pages/assets.
  - Alumni filter options and major list APIs.
- `backend/routes/analytics_routes.py`
  - Heatmap/analytics pages/assets.
  - `/api/heatmap` processing and caching.
  - Geo-clustering helpers.
- `backend/routes/interaction_routes.py`
  - Bookmark/connect interactions.
  - Notes CRUD and note-summary API.
- `backend/routes/admin_routes.py`
  - Admin user management APIs.
  - Authorized-emails whitelist APIs.
- `backend/routes/scraper_routes.py`
  - Scraper activity accountability API.

## Legacy Monolith -> Current Module Mapping

### Core app/config/auth helpers
- `_is_production_mode` -> `Config.is_production_mode` (`backend/config.py`)
- `_configure_secret_key` -> `Config.configure_secret_key` (`backend/config.py`)
- `is_authorized_user` -> `middleware.is_authorized_user`
- `_is_logged_in` -> `middleware._is_logged_in`
- `_get_session_email` -> `middleware._get_session_email`
- `get_current_user_id` -> `middleware.get_current_user_id`
- `login_required` -> `middleware.login_required`
- `api_login_required` -> `middleware.api_login_required`
- `admin_required` -> `middleware.admin_required`

### Shared parsing/normalization utilities
- `_get_or_create_normalized_entity_id` -> `utils._get_or_create_normalized_entity_id`
- `_parse_int_list_param` -> `utils.parse_int_list_param`
- `_rank_filter_option_counts` -> `utils.rank_filter_option_counts`

### Alumni-domain logic moved from monolith
- `_validation_error` -> `routes/alumni_routes.py:_validation_error`
- `_parse_multi_value_param` -> `routes/alumni_routes.py:_parse_multi_value_param`
- `_parse_optional_non_negative_int` -> `routes/alumni_routes.py:_parse_optional_non_negative_int`
- `_validate_min_max` -> `routes/alumni_routes.py:_validate_min_max`
- `_parse_unt_alumni_status_filter` -> `routes/alumni_routes.py:_parse_unt_alumni_status_filter`
- `classify_seniority_bucket` -> `routes/alumni_routes.py:classify_seniority_bucket`
- `_normalize_requested_discipline` -> `routes/alumni_routes.py:_normalize_requested_discipline`
- Route `/api/alumni` -> `routes/alumni_routes.py:api_get_alumni`
- Route `/api/alumni/filter` -> `routes/alumni_routes.py:api_alumni_filter_alias`
- Route `/api/alumni/<id>` GET -> `routes/alumni_routes.py:api_get_alumni_detail`
- Route `/api/alumni/<id>` PUT -> `routes/alumni_routes.py:api_update_alumni`
- Route `/api/alumni/majors` -> `routes/alumni_routes.py:api_get_majors`
- Route `/api/alumni/filter-options` -> `routes/alumni_routes.py:api_filter_options`

### Analytics/heatmap logic moved from monolith
- `classify_degree` -> `routes/analytics_routes.py:classify_degree`
- `get_continent` -> `routes/analytics_routes.py:get_continent`
- Route `/api/heatmap` -> `routes/analytics_routes.py:get_heatmap_data`
- Route `/api/geocode` -> `routes/analytics_routes.py:api_geocode`
- `search_location_candidates` compatibility hook -> app module export + `routes/analytics_routes.py` fallback import
- Page routes `/heatmap`, `/analytics`, `/heatmap.js`, `/heatmap_style.css` -> `routes/analytics_routes.py`

### Auth and admin route migration
- All `/api/auth/*` and LinkedIn routes -> `routes/auth_routes.py`
- Login/register/change-password pages + logout/access-denied -> `routes/auth_routes.py`
- All `/api/admin/users*` -> `routes/admin_routes.py`
- `/api/authorized-emails` GET/POST/DELETE -> `routes/admin_routes.py`

### Interaction and notes route migration
- `/api/interaction` POST/DELETE -> `routes/interaction_routes.py`
- `/api/user-interactions` GET -> `routes/interaction_routes.py`
- `/api/notes` GET (list-all) -> `routes/interaction_routes.py:get_all_notes`
- `/api/notes/<alumni_id>` GET/POST/DELETE -> `routes/interaction_routes.py`
- `/api/notes/summary` GET -> `routes/interaction_routes.py`

### Scraper accountability migration
- `_resolve_scraper_display_name` -> `routes/scraper_routes.py:_resolve_scraper_display_name`
- `/api/scraper-activity` -> `routes/scraper_routes.py:get_scraper_activity_api`

### Static/core route migration
- `/`, `/about`, `/alumni_style.css`, `/app.js`, `/assets/<path>` -> `routes/core_routes.py`
- `/api/fallback-status` -> `routes/core_routes.py:get_fallback_status_api`

## Compatibility Surfaces Kept in `backend/app.py`
The following imports are intentionally exposed at module scope to preserve older tests and monkeypatch patterns:
- `get_connection` (from `database`)
- `get_current_user_id` (from `middleware`)
- `search_location_candidates` (from `geocoding`)
- `_heatmap_cache` (re-exported from `routes.analytics_routes`)
- `_resolve_scraper_display_name` (re-exported from `routes.scraper_routes`)

## Dependency Graph (High-level)
- `app.py`
  - imports `Config` and middleware auth/session helpers
  - imports all blueprints
  - imports selected compatibility symbols (`database.get_connection`, `_heatmap_cache`, `_resolve_scraper_display_name`)
- `routes/*`
  - primarily import `database` functions and middleware decorators/helpers
  - some route modules import helper functions from peer route modules (analytics imports parse helpers from alumni routes)
- `middleware.py`
  - imports `database.get_connection` and `database.get_user_by_email`
- `utils.py`
  - no Flask app import; pure helper module

## Circular Import Safeguards
Current pattern for route modules that need app-level symbols:
- Route modules call `importlib.import_module("app")` inside helper `_app_mod()`.
- Access to `get_connection` and `get_current_user_id` is deferred until request-time.

Why this exists:
- Avoids hard cyclic import during startup when app imports blueprints and blueprints need app symbols.

Trade-off:
- Route modules are still coupled to the module name `app` and compatibility exports.
- A future app-factory migration should replace `_app_mod()` calls with direct imports from stable service modules.

## Global State and Caching
- `_heatmap_cache` in `routes/analytics_routes.py`
  - in-memory dict.
  - TTL: 60 seconds.
  - cache key: `continent|unt_alumni_status` when no grad-year/seniority filters are applied.
  - currently process-local (not shared across workers).

Operational note:
- In multi-process deployment, cache divergence is expected and acceptable for current use.

## Known Legacy Endpoints: Current Status
Legacy routes present in pre-refactor `backend/app.py` but not currently found in active route blueprints:
- `/profile_modal.js`
- `/profile_modal.css`
- `/profile_modal_test.js`

Restored during parity pass:
- `/api/geocode` (now in `routes/analytics_routes.py`)
- `/api/fallback-status` (now in `routes/core_routes.py`)
- `/api/notes` list-all variant (now in `routes/interaction_routes.py`)

Impact surface:
- Frontend still references `/api/geocode` (example: `frontend/public/heatmap_dual.js`) and is now served by analytics routes.
- Tests that monkeypatch geocode-search behavior can still patch through the backend app module compatibility export.

Recommended follow-up:
1. Re-introduce these endpoints in a dedicated blueprint (for example `routes/system_routes.py` or `routes/geocode_routes.py`) if still required by UI/tests.
2. If intentionally retired, remove call sites and update tests/docs to avoid silent drift.

## Database Layer Boundary
- `database.py` remains a lower-level data/service module.
- Route modules depend on `database.py`; `database.py` does not import route modules.
- This preserves one-way dependency flow for request handlers -> data access.

## Import Integrity Observations
- Blueprint registration and imports in `backend/app.py` are internally consistent.
- Scraper-side importability remains intact after Groq retry consolidation (`scraper/groq_retry_patch.py` removed and callers updated).
- `backend/database.py` remains route-agnostic (no blueprint imports).

## Risk Register
- Medium: route-helper sharing (`analytics_routes` importing parsing helpers from `alumni_routes`) creates cross-domain coupling.
- Medium: dynamic `_app_mod()` import pattern is resilient but not ideal for static analysis or packaging.
- Medium: missing legacy endpoints may cause runtime regressions in specific UI paths/tests not in the recent targeted regression slice.
- Low: in-memory heatmap cache is intentionally non-distributed.

## Suggested Next Refactor Steps
1. Introduce a small `backend/services/` layer for shared logic currently duplicated or cross-imported between route modules.
2. Move request-agnostic parsing/classification helpers out of route modules into `utils` or service modules.
3. Replace `_app_mod()` indirection by importing stable provider functions from a dedicated module (`backend/providers.py` or similar).
4. Decide endpoint strategy for legacy routes (`/api/geocode`, `/api/fallback-status`, profile modal assets, `/api/notes` aggregate) and either restore or formally deprecate.
4. Decide endpoint strategy for remaining legacy profile modal assets and either restore or formally deprecate.

## Verification Notes
This reference was compiled by comparing current source declarations and the pre-refactor symbol inventory from commit `8c8666f`.
