# CloudStudio

CloudStudio is a single, unified control plane for multi-cloud pricing, billing, storage operations, tagging, cloud metrics, live health, and security posture.

- One `.env` file.
- One SQLite database (`./data/cloudstudio.db`).

## Current integrated modules

CloudStudio mounts all modules in-process and exposes them through one unified UI:

- `Dashboard` (cross-cloud summary)
- `Pricing` (embedded CloudPriceStudio calculator)
- `Billing` (provider/account totals, resource-type drilldown, export, trend, tag filtering)
- `Billing Backfill` (historical pull controls/status)
- `Storage` (embedded CloudStorageStudio functionality)
- `Live View (VSAx)` (real-time VSAx device health table)
- `Security`
- `IP Address` (global IP mapping/resolution + discovery/inherit flow)
- `Cloud Metrics` (Azure/AWS/Rackspace metrics catalog)
- `Cloud Database` (Azure SQL/AWS RDS inventory + health snapshot view)
- `Grafana-Cloud` (daily ingest usage + billed usage sync, stored in DB)
- `Firewall` (CheckMK-backed firewall + VPN status)
- `Tags` (resource tag viewer/editor with cloud sync where supported)
- `Admin` (vendor onboarding, app config, users, API keys, backup)
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
  - `src/modules/grafana-cloud/server.js`
  - `src/modules/firewall/server.js`
- Cloud Database routes are mounted from:
  - `src/modules/cloud-database/server.js`

## Project layout

```text
CloudStudio/
  public/                   # Unified CloudStudio shell UI
  src/
    server.js               # Main API + auth + module orchestration
    db.js                   # Shared DB schema and data access
    auth.js                 # Login/session + user store
    connectors/billing.js   # Azure/AWS/GCP/SendGrid/Rackspace/Wasabi/Grafana billing pulls
    modules/
      storage/              # Embedded storage engine
      pricing/              # Embedded pricing engine
      billing/              # Billing API module
      tag/                  # Tag API module
      ip-address/           # IP map module
      grafana-cloud/        # Grafana billed usage + daily ingest module
      firewall/             # CheckMK firewall/VPN module
      cloud-database/       # Cloud DB provider view API
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
- Runtime App Config is managed in Admin UI and encrypted at rest.
- Unified private APIs are auth-protected; public APIs require `x-api-key`.
- AWS account resolution is vendor-first (Admin -> Vendor onboarding), with env-based AWS keys/JSON used only as fallback.

## Environment model (minimal `.env` + DB runtime config)

CloudStudio now uses a minimal bootstrap `.env` and stores most runtime/module settings in DB via Admin UI.

### Minimal required env

- `CLOUDSTUDIO_PORT` (or `PORT`)
- `CLOUDSTUDIO_SECRET_KEY`
- `CLOUDSTUDIO_DB_FILE` (defaults to `./data/cloudstudio.db`)
- `APP_AUTH_USERS` (bootstrap admin user)

### Runtime settings in DB (Admin)

Admin path: `Admin -> App config`.

- Branding (login + main header)
- Runtime env overrides (`KEY=value`) for module/provider tuning
- Saved encrypted in `app_settings` (ciphertext payload)
- Applied in-memory without app restart for root scheduler/runtime settings

### Configuration key reference

- In app (Admin): `Admin -> App config -> Open App Config Keys Reference`
- Direct URL: `/docs/app-config-keys.md`
- Repo document: `docs/APP_CONFIG_KEYS.md`

When env/config keys change:

```bash
npm run docs:app-config-keys
```

Then commit both:

- `docs/APP_CONFIG_KEYS.md`
- `public/docs/app-config-keys.md`
- `README.md` (if section paths or behavior changed)

### Provider credential precedence (important)

- AWS integrations (storage, pricing, billing label mapping, cloud metrics) now resolve accounts from **Vendor onboarding first**.
- Legacy `AWS_ACCOUNTS_JSON` / direct `AWS_*` env values are still supported as fallback for bootstrap/migration.
- Wasabi integrations still support `WASABI_ACCOUNTS_JSON` and direct keys; prefer storing provider credentials in `Admin -> Vendor onboarding` for long-term management.

### Import/export app config

Admin can export/import one unencrypted JSON bundle:

- `runtimeConfig` (branding + env overrides)
- `dbBackupConfig` (including credentials)
- `vendors` (including credentials)

On import, sensitive values are re-encrypted before DB write.

### DB backup scheduler (S3-compatible)

Admin path: `Admin -> Backup`.

- Supports Wasabi/AWS/any S3-compatible endpoint
- SQLite backup API + gzip compression + manifest upload
- Local queue retry when S3 target is unavailable
- Lifecycle retention support
- Env-first precedence for `DB_BACKUP_*`; missing values fall back to saved Admin config

## Billing module details (current behavior)

- Providers supported for pull: `azure`, `aws`, `gcp`, `sendgrid`, `rackspace`, `wasabi`, `wasabi-main`, `grafana-cloud`.
- `gcp` currently uses manual fallback amount (`manualMonthlyCost`) unless export integration is added.
- `grafana-cloud` supports usage-based billing plus flex commit amortization (`usage`, `amortized`, `max` reporting modes).
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
- `GET /api/platform/grafana-cloud/vendors`
- `GET /api/platform/grafana-cloud/status`
- `GET /api/platform/grafana-cloud/daily-ingest`
- `POST /api/platform/grafana-cloud/sync`
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
- `GET /api/admin/app-config` (admin only)
- `PUT /api/admin/app-config` (admin only)
- `GET /api/admin/app-config/export` (admin only)
- `POST /api/admin/app-config/import` (admin only)

### Public APIs (API key required)

- `GET /api/public/overview`
- `GET /api/public/billing`
- `GET /api/public/cloud-metrics`
- `GET /api/public/cloud-metrics/resources`
- `GET /api/public/grafana-cloud/vendors`
- `GET /api/public/grafana-cloud/status`
- `GET /api/public/grafana-cloud/daily-ingest`
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
- `app_settings`
- `grafana_cloud_ingest_daily`
- `users`
- `user_state`

## Security notes

- Set a strong `CLOUDSTUDIO_SECRET_KEY` in production.
- Do not commit `.env`.
- API keys are shown once at creation; store in a secret manager.
