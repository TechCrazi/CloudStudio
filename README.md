# CloudStudio

CloudStudio is a single, unified control plane for multi-cloud pricing, billing, storage operations, tagging, cloud metrics, live health, and security posture.

- One `.env` file.
- One SQLite database (`./data/cloudstudio.db`).

## Current integrated modules

CloudStudio mounts all modules in-process and exposes them through one unified UI:

- `Dashboard` (cross-cloud summary)
- `Storage` (embedded CloudStorageStudio functionality)
- `IP Address` (global IP mapping/resolution)
- `Pricing` (embedded CloudPriceStudio calculator)
- `Billing` (provider/account totals, resource-type drilldown, export, trend, tag filtering)
- `Billing Backfill` (historical pull controls/status)
- `Tags` (resource tag viewer/editor with cloud sync where supported)
- `Utilization`
- `Live View`
- `Security`
- `Vendors`
- `API Docs`

## Architecture

- Backend: Node.js + Express
- UI: static SPA (`public/index.html`, `public/app.js`, `public/styles.css`)
- DB: `better-sqlite3`, single master DB (`./data/cloudstudio.db`)
- Auth: login session + users stored in same DB (`users`, `user_state`)
- Services mounted by `src/services.js`:
  - `src/modules/storage/src/server.js`
  - `src/modules/pricing/server.js`
  - `src/modules/ip-address/server.js`
  - `src/modules/billing/server.js`
  - `src/modules/tag/server.js`

## Project layout

```text
CloudStudio/
  public/                   # Unified CloudStudio shell UI
  src/
    server.js               # Main API + auth + module orchestration
    db.js                   # Shared DB schema and data access
    auth.js                 # Login/session + user store
    connectors/billing.js   # Azure/AWS/GCP/Rackspace billing pulls
    modules/
      storage/              # Embedded storage engine
      pricing/              # Embedded pricing engine
      billing/              # Billing API module
      tag/                  # Tag API module
      ip-address/           # IP map module
  data/
    cloudstudio.db          # Master database
```

## Prerequisites

- Node.js `>=18.18.0`

## Quick start

```bash
npm install
cp .env.example .env
npm start
```

Optional dev mode:

```bash
npm run dev
```

Open:

- App login: `http://localhost:9090/login`
- App shell: `http://localhost:9090`

Note: direct module routes under `/apps/*` are intentionally redirected to `/` so users stay in one unified shell.

## Core behavior implemented

- Active screen persists across browser refresh (`localStorage` state).
- Billing scope/preset/collapse state persists across refresh.
- Drawer includes live `Service Modules` health indicators.
- Vendor credentials are encrypted at rest before DB write.
- Unified private APIs are auth-protected; public APIs require `x-api-key`.

## Environment model (single `.env`)

CloudStudio uses one env file at repo root for all modules.

### Required baseline

- `PORT` or `CLOUDSTUDIO_PORT`
- `CLOUDSTUDIO_SECRET_KEY`
- `APP_AUTH_USERS` (or `APP_LOGIN_USER` + `APP_LOGIN_PASSWORD`)
- `CLOUDSTUDIO_DB_FILE` / `SQLITE_PATH` (defaults to `./data/cloudstudio.db`)

### Branding

- `CLOUDSTUDIO_BRAND_LOGIN` controls login page brand (`Company Name|INITIALS`)
- `CLOUDSTUDIO_BRAND_MAIN` controls main app header brand (`Company Name|INITIALS`)
- JSON is also supported:
  - `{"name":"My Company","initials":"MC"}`

### Azure (billing + tagging + storage)

- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_TENANT_ID`
- Vendor-level Azure credentials can also be set from the UI (recommended for multi-subscription setups).

### AWS (multi-account)

- `AWS_ACCOUNTS_JSON` supports multiple accounts and is used to auto-create billing vendors when missing.
- `AWS_PRICING_ACCOUNT_ID` chooses which account from `AWS_ACCOUNTS_JSON` is used for pricing API pulls.

Example:

```json
[
  {
    "accountId": "123456789012",
    "displayName": "Prod",
    "accessKeyId": "AKIA...",
    "secretAccessKey": "..."
  },
  {
    "accountId": "210987654321",
    "displayName": "Dev",
    "profile": "dev-profile"
  }
]
```

### Cloud Metrics (Azure + AWS + Rackspace)

- `CLOUD_METRICS_SYNC_ENABLED=true`
- `CLOUD_METRICS_SYNC_INTERVAL_MS=300000` (default 5 minutes)
- `CLOUD_METRICS_SYNC_RUN_ON_STARTUP=true`
- Provider toggles:
  - `CLOUD_METRICS_AZURE=true|false`
  - `CLOUD_METRICS_AWS=true|false`
  - `CLOUD_METRICS_RACKSPACE=true|false`
- Azure tuning:
  - `CLOUD_METRICS_AZURE_CONCURRENCY`
  - `CLOUD_METRICS_AZURE_MAX_RESOURCES`
  - `CLOUD_METRICS_AZURE_LOOKBACK_MINUTES`
- AWS tuning:
  - `CLOUD_METRICS_AWS_CONCURRENCY`
  - `CLOUD_METRICS_AWS_MAX_RETRIES`
  - `CLOUD_METRICS_AWS_LOOKBACK_MINUTES`
  - `CLOUD_METRICS_AWS_MAX_METRICS_PER_ACCOUNT`
  - `CLOUD_METRICS_AWS_NAMESPACES` (optional, CSV or JSON array)
  - `CLOUD_METRICS_AWS_REGIONS` (optional, CSV or JSON array)
- Rackspace tuning:
  - `CLOUD_METRICS_RACKSPACE_CONCURRENCY`
  - `CLOUD_METRICS_RACKSPACE_PAGE_SIZE`
  - `CLOUD_METRICS_RACKSPACE_MAX_ENTITIES`
  - `CLOUD_METRICS_RACKSPACE_MAX_METRICS_PER_CHECK`
  - `CLOUD_METRICS_RACKSPACE_LOOKBACK_MINUTES`
  - `CLOUD_METRICS_RACKSPACE_PLOT_POINTS`
  - `CLOUD_METRICS_RACKSPACE_TIMEOUT_MS`
  - `CLOUD_METRICS_RACKSPACE_MAX_RETRIES`

Notes:

- If `CLOUD_METRICS_AWS_REGIONS` is not set, CloudStudio uses account-defined regions from `AWS_ACCOUNTS_JSON` (`metricRegions`/`cloudWatchRegion`/`region` and `efsRegions` fallback).
- Rackspace cloud metrics use Rackspace Cloud Monitoring (`rax:monitor`) endpoints for entities/checks/metric plots.
- Scheduled/startup cloud-metrics sync runs for all configured and enabled supported providers (`azure`, `aws`, `rackspace`).

### Rackspace (multi-account)

- `RACKSPACE_ACCOUNTS_JSON` supports multiple Rackspace accounts.
- Fallback single-account vars:
  - `RACKSPACE_USERNAME`
  - `RACKSPACE_API_KEY`
  - `RACKSPACE_ACCOUNT_ID`
  - `RACKSPACE_BILLING_BASE_URL`
  - `RACKSPACE_IDENTITY_URL`

Example:

```json
[
  {
    "accountId": "030-34972734591",
    "displayName": "ELLKAY LLC (USD)",
    "username": "your-rackspace-user",
    "apiKey": "your-rackspace-api-key"
  }
]
```

### Billing scheduler/backfill controls

- `BILLING_AUTO_SYNC_ENABLED=true`
- `BILLING_AUTO_SYNC_INTERVAL_MS=86400000` (24h)
- `BILLING_AUTO_SYNC_RUN_ON_STARTUP=true`
- `BILLING_AUTO_SYNC_ONLY_MISSING=true`
- `BILLING_AUTO_SYNC_LOOKBACK_MONTHS=24`
- `BILLING_STARTUP_BACKFILL_MISSING_ENABLED=true`
- `BILLING_STARTUP_BACKFILL_MISSING_MONTHS=12`
- `RACKSPACE_REQUIRE_DETAIL_CSV=true` (strict mode for Rackspace device-level data)

### DB backup scheduler (S3-compatible)

CloudStudio can take SQLite backups and upload them to any S3-compatible target (Wasabi, AWS S3, and compatible endpoints).

- `DB_BACKUP_ENABLED=true`
- `DB_BACKUP_BUCKET=your-wasabi-bucket`
- `DB_BACKUP_PREFIX=cloudstudio-db-backups`
- `DB_BACKUP_REGION=us-east-1`
- `DB_BACKUP_ENDPOINT=https://s3.wasabisys.com`
- `DB_BACKUP_ACCESS_KEY_ID=...` (falls back to `WASABI_ACCESS_KEY`)
- `DB_BACKUP_SECRET_ACCESS_KEY=...` (falls back to `WASABI_SECRET_KEY`)
- `DB_BACKUP_INTERVAL_MS=3600000` (hourly)
- `DB_BACKUP_RUN_ON_STARTUP=true`
- `DB_BACKUP_RETENTION_DAYS=15`
- `DB_BACKUP_APPLY_LIFECYCLE=true`
- `DB_BACKUP_LIFECYCLE_RULE_ID=cloudstudio-db-backup-retention`
- `DB_BACKUP_TMP_DIR=./data/tmp-backups`

Behavior:

- Backups are created from SQLite using `better-sqlite3` backup API for consistency.
- Uploaded object keys use timestamped filenames under `DB_BACKUP_PREFIX`.
- Lifecycle retention is enforced by creating/updating one lifecycle rule for the configured prefix.
- If lifecycle API permissions are missing, backup upload still succeeds and logs a lifecycle warning.
- Config precedence is env-first: if a `DB_BACKUP_*` env var is set, it overrides Admin UI values; missing env vars fall back to saved Admin UI config.
- Admin UI path: `Admin -> Backup`.

## Billing module details (current behavior)

- Providers supported for pull: `azure`, `aws`, `gcp`, `rackspace`.
- `gcp` currently uses manual fallback amount (`manualMonthlyCost`) unless export integration is added.
- Billing nav is dynamic from configured providers/accounts.
- Resource table supports:
  - expand/collapse row details
  - provider filter
  - bill-type filter
  - tag filter
  - sort modes
- Exports:
  - Top-level CSV export includes both snapshot rows and resource-detail rows.
  - Resource-level export is available per resource type.
- Auto-sync:
  - Runs every 24h by default.
  - Tracks status at `/api/billing/auto-sync-status`.
- Missing-history protection:
  - Startup missing-month check can auto-backfill last 12 months (configurable).
  - Missing-only mode avoids unnecessary provider API calls.
- Duplicate protection:
  - billing snapshots are de-duped by vendor/provider/period unique index.

## Rackspace billing specifics

- Pull path uses Rackspace billing APIs + invoice detail CSV (`/invoices/{id}/detail`) for line-item/device-level data.
- Historical API window is limited (last ~2 years by provider API behavior).
- If detailed CSV is unavailable and `RACKSPACE_REQUIRE_DETAIL_CSV=true`, sync fails fast to avoid summary-only data drift.
- Manual CSV ingest endpoint is available:
  - `POST /api/billing/rackspace/import-csv`
  - supports `csvText` or `filePath` payload.

## Tag module behavior

- `GET /api/tags`, `POST /api/tags/resolve`, `PUT /api/tags`.
- Cloud sync support:
  - Azure: attempts ARM tag read/write for taggable resource IDs.
  - AWS: uses Resource Groups Tagging API.
- For unsupported/untaggable resources or cloud errors, tags are still saved locally in `resource_tags` so data is not lost.

## API overview

### Auth/session

- `GET /api/auth/me`
- `POST /api/auth/login`
- `POST /api/auth/logout`

### Unified private APIs

- `GET /api/services`
- `GET /api/platform/overview`
- `GET /api/platform/cloud-metrics/providers`
- `GET /api/platform/cloud-metrics`
- `POST /api/platform/cloud-metrics/sync`
- `GET /api/platform/live`
- `GET /api/platform/security`
- `POST /api/platform/pricing/compare`
- `GET /api/vendors`
- `POST /api/vendors`
- `PUT /api/vendors/:vendorId`
- `DELETE /api/vendors/:vendorId`
- `POST /api/vendors/:vendorId/test`
- `GET /api/vendors/export`
- `GET /api/vendors/:vendorId/export`
- `POST /api/vendors/import`
- `POST /api/vendors/:vendorId/import`
- `GET /api/ip-address`, `POST /api/ip-address/import`, `GET /api/ip-address/resolve`, `GET /api/ip-address/export/csv`
- `GET /api/billing`
- `GET /api/billing/accounts`
- `POST /api/billing/sync`
- `POST /api/billing/backfill`
- `POST /api/billing/backfill/ensure`
- `GET /api/billing/backfill/status`
- `GET /api/billing/auto-sync-status`
- `GET /api/billing/export/csv`
- `POST /api/billing/rackspace/import-csv`
- `GET /api/tags`
- `POST /api/tags/resolve`
- `PUT /api/tags`
- `GET /api/admin/db-backup/config` (admin only)
- `PUT /api/admin/db-backup/config` (admin only)
- `GET /api/admin/db-backup/status` (admin only)
- `POST /api/admin/db-backup/run` (admin only)

### Public APIs (API key required)

- `GET /api/public/overview`
- `GET /api/public/billing`
- `GET /api/public/cloud-metrics`
- `GET /api/public/cloud-metrics/resources`
- `GET /api/public/security`

Generate keys via:

- `POST /api/keys`
- `GET /api/keys`
- `PATCH /api/keys/:keyId`

## Database (single source of truth)

Primary tables in `./data/cloudstudio.db`:

- `vendors`
- `billing_snapshots`
- `resource_tags`
- `ip_aliases`
- `api_keys`
- `sync_runs`
- `users`
- `user_state`

## Security notes

- Set a strong `CLOUDSTUDIO_SECRET_KEY` in production.
- Do not commit `.env`.
- API keys are shown once at creation; store in a secret manager.
