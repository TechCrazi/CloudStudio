# App Config Keys Reference

This document defines what each CloudStudio configuration key is used for.

- Generated: 2026-02-22T01:38:25.145Z
- Source: `src/**/*.js` (`process.env.*` references)
- Generator: `scripts/generate-app-config-keys-doc.js`

## Maintenance Rule

When new env/config keys are added or changed:

1. Update key behavior in code.
2. Run `npm run docs:app-config-keys`.
3. Verify this file changed as expected.
4. Verify the README section **Configuration key reference** still points here.

## Minimal Bootstrap .env Keys

These keys should stay in `.env` even when App Config runtime overrides are used:

| Key | Level | Purpose |
| --- | --- | --- |
| `CLOUDSTUDIO_PORT` | Recommended | Main HTTP port for CloudStudio (`PORT` fallback supported). |
| `CLOUDSTUDIO_SECRET_KEY` | Required | Master secret used to encrypt/decrypt sensitive DB settings and secrets. |
| `CLOUDSTUDIO_DB_FILE` | Recommended | Path to the SQLite database file. |
| `APP_AUTH_USERS` | Required | Bootstrap `username:password` list for initial local users. |
| `APP_ADMIN_USERS` | Optional | Comma-separated bootstrap admin usernames. |
| `CLOUDSTUDIO_BRAND_LOGIN` | Optional | Default login-screen brand label/initials (`Name|INIT`). |
| `CLOUDSTUDIO_BRAND_MAIN` | Optional | Default main-shell brand label/initials (`Name|INIT`). |
| `CLOUDSTUDIO_SINGLE_ENV` | Optional | Sets unified single-env mode marker. |

## Runtime Override Notes

- Runtime overrides are edited in **Admin -> App config** and stored encrypted in DB.
- Not all keys are runtime-override eligible. Blocked bootstrap keys are kept env-driven.
- Some deep module settings may still require module restart for full effect.
- Provider precedence:
  - AWS account config resolves from **Vendor onboarding first**; `AWS_ACCOUNTS_JSON` is fallback-only.
  - Wasabi should be configured in Vendor onboarding; `WASABI_ACCOUNTS_JSON` remains supported for fallback/migration.

## Full Key Reference

| Key | Scope | Purpose | Primary usage(s) |
| --- | --- | --- | --- |
| `ACCOUNT_SYNC_CONCURRENCY` | Runtime override (Admin -> App config) | Account sync/concurrency setting. | `src/modules/storage/src/server.js:94` |
| `APP_ADMIN_USERS` | Bootstrap `.env` (restart required) | Bootstrap admin users list. | `src/auth.js:55`, `src/modules/pricing/server.js:1009` |
| `APP_AUTH_USERS` | Bootstrap `.env` (restart required) | Bootstrap credentials used for initial user seeding. | `src/auth.js:445`, `src/modules/pricing/server.js:1397` |
| `APP_LOGIN_PASSWORD` | Bootstrap `.env` (restart required) | Legacy single-user bootstrap password. | `src/auth.js:446`, `src/auth.js:449`, `src/auth.js:477` +2 more |
| `APP_LOGIN_USER` | Bootstrap `.env` (restart required) | Legacy single-user bootstrap username. | `src/auth.js:446`, `src/auth.js:448`, `src/auth.js:476` +2 more |
| `AUTH_DATA_DIR` | Runtime override (Admin -> App config) | Auth state storage directory. | `src/modules/pricing/server.js:114`, `src/modules/pricing/server.js:118`, `src/server.js:672` +1 more |
| `AUTH_DB_FILE` | Bootstrap `.env` (restart required) | Auth DB file path (forced to unified DB when not explicitly set). | `src/modules/pricing/server.js:115`, `src/modules/pricing/server.js:121`, `src/server.js:667` +1 more |
| `AUTH_SESSION_TTL_MS` | Runtime override (Admin -> App config) | Auth/session TTL override in milliseconds. | `src/modules/pricing/server.js:132` |
| `AUTH_STATE_DIR` | Runtime override (Admin -> App config) | User-state directory for auth module. | `src/modules/pricing/server.js:127`, `src/server.js:677`, `src/server.js:679` |
| `AUTH_STATE_MAX_BYTES` | Runtime override (Admin -> App config) | Maximum per-user persisted UI/auth state payload size. | `src/modules/pricing/server.js:138` |
| `AUTH_USERS_FILE` | Runtime override (Admin -> App config) | Legacy auth users JSON file path. | `src/modules/pricing/server.js:124` |
| `AWS_ACCESS_KEY_ID` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:410`, `src/modules/pricing/server.js:3407`, `src/modules/pricing/server.js:3445` |
| `AWS_ACCOUNT_SYNC_CONCURRENCY` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:112` |
| `AWS_ACCOUNTS_JSON` | Runtime override (Admin -> App config) | Legacy/fallback AWS account list JSON. Vendor onboarding is primary for AWS account discovery. | `src/aws-account-config.js:390`, `src/modules/billing/server.js:2326` |
| `AWS_API_MAX_CONCURRENCY` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/aws.js:24` |
| `AWS_API_MAX_RETRIES` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/aws.js:26` |
| `AWS_API_MIN_INTERVAL_MS` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/aws.js:25` |
| `AWS_BUCKET_SYNC_CONCURRENCY` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:113` |
| `AWS_CACHE_TTL_HOURS` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:111` |
| `AWS_DEEP_SCAN_DEFAULT` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:114` |
| `AWS_DEEP_SCAN_MAX_PAGES_PER_BUCKET` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/aws.js:27` |
| `AWS_DEFAULT_ACCESS_KEY_ID` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:410` |
| `AWS_DEFAULT_ACCOUNT_ID` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:408` |
| `AWS_DEFAULT_ACCOUNT_LABEL` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:409` |
| `AWS_DEFAULT_CLOUDWATCH_REGION` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:184`, `src/aws-account-config.js:347`, `src/aws-account-config.js:415` +2 more |
| `AWS_DEFAULT_EFS_REGIONS` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:191`, `src/aws-account-config.js:349`, `src/aws-account-config.js:421` +2 more |
| `AWS_DEFAULT_FORCE_PATH_STYLE` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:423` |
| `AWS_DEFAULT_REGION` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:173`, `src/aws-account-config.js:343`, `src/aws-account-config.js:414` +4 more |
| `AWS_DEFAULT_REQUEST_METRICS_ENABLED` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:424` |
| `AWS_DEFAULT_S3_ENDPOINT` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:422` |
| `AWS_DEFAULT_SECRET_ACCESS_KEY` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:411` |
| `AWS_DEFAULT_SESSION_TOKEN` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:412` |
| `AWS_PRICING_ACCOUNT_ID` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/pricing/server.js:3370` |
| `AWS_PRICING_AS_OF_DATE` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:187` |
| `AWS_PRICING_BYTES_PER_GB` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:188` |
| `AWS_PRICING_CURRENCY` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:184` |
| `AWS_PRICING_DAYS_IN_MONTH` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:189` |
| `AWS_PRICING_EFS_STANDARD_GB_MONTH` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:196` |
| `AWS_PRICING_REGION_LABEL` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:185` |
| `AWS_PRICING_S3_EGRESS_FREE_GB` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:192` |
| `AWS_PRICING_S3_EGRESS_GB` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:191` |
| `AWS_PRICING_S3_REQUEST_LABEL` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:195` |
| `AWS_PRICING_S3_REQUEST_UNIT_PRICE` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:194` |
| `AWS_PRICING_S3_REQUEST_UNIT_SIZE` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:193` |
| `AWS_PRICING_S3_STANDARD_GB_MONTH` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:190` |
| `AWS_PRICING_SOURCE_URL` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:186` |
| `AWS_PROFILE` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:413`, `src/connectors/billing.js:2836`, `src/connectors/billing.js:2839` +5 more |
| `AWS_REQUEST_METRICS_DEFAULT` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:115` |
| `AWS_SECRET_ACCESS_KEY` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:411`, `src/modules/pricing/server.js:3445` |
| `AWS_SECURITY_SCAN_DEFAULT` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:116` |
| `AWS_SESSION_TOKEN` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/aws-account-config.js:412` |
| `AWS_SYNC_INTERVAL_HOURS` | Runtime override (Admin -> App config) | AWS integration setting (inventory, pricing, billing, or tagging). | `src/modules/storage/src/server.js:110` |
| `AZURE_API_BASE_BACKOFF_MS` | Runtime override (Admin -> App config) | Azure integration setting (inventory, pricing, billing, or metrics). | `src/modules/storage/src/azure.js:12` |
| `AZURE_API_MAX_BACKOFF_MS` | Runtime override (Admin -> App config) | Azure integration setting (inventory, pricing, billing, or metrics). | `src/modules/storage/src/azure.js:13` |
| `AZURE_API_MAX_CONCURRENCY` | Runtime override (Admin -> App config) | Azure integration setting (inventory, pricing, billing, or metrics). | `src/modules/storage/src/azure.js:9`, `src/modules/storage/src/server.js:3279` |
| `AZURE_API_MAX_RETRIES` | Runtime override (Admin -> App config) | Azure integration setting (inventory, pricing, billing, or metrics). | `src/modules/storage/src/azure.js:11` |
| `AZURE_API_MIN_INTERVAL_MS` | Runtime override (Admin -> App config) | Azure integration setting (inventory, pricing, billing, or metrics). | `src/modules/storage/src/azure.js:10`, `src/modules/storage/src/server.js:3280` |
| `AZURE_CLIENT_ID` | Runtime override (Admin -> App config) | Azure integration setting (inventory, pricing, billing, or metrics). | `src/modules/storage/src/server.js:256`, `src/server.js:2142` |
| `AZURE_CLIENT_SECRET` | Runtime override (Admin -> App config) | Azure integration setting (inventory, pricing, billing, or metrics). | `src/modules/storage/src/server.js:257`, `src/server.js:2143` |
| `AZURE_METRICS_SYNC_INTERVAL_HOURS` | Runtime override (Admin -> App config) | Azure integration setting (inventory, pricing, billing, or metrics). | `src/modules/storage/src/server.js:93` |
| `AZURE_TENANT_ID` | Runtime override (Admin -> App config) | Azure integration setting (inventory, pricing, billing, or metrics). | `src/modules/storage/src/server.js:255`, `src/server.js:2141` |
| `BILLING_AUTO_SYNC_ENABLED` | Runtime override (Admin -> App config) | Billing module sync/backfill behavior setting. | `src/server.js:433` |
| `BILLING_AUTO_SYNC_INTERVAL_MS` | Runtime override (Admin -> App config) | Billing module sync/backfill behavior setting. | `src/server.js:430` |
| `BILLING_AUTO_SYNC_LOOKBACK_MONTHS` | Runtime override (Admin -> App config) | Billing module sync/backfill behavior setting. | `src/server.js:443` |
| `BILLING_AUTO_SYNC_MISSING_DELAY_MS` | Runtime override (Admin -> App config) | Billing module sync/backfill behavior setting. | `src/server.js:447` |
| `BILLING_AUTO_SYNC_ONLY_MISSING` | Runtime override (Admin -> App config) | Billing module sync/backfill behavior setting. | `src/server.js:439` |
| `BILLING_AUTO_SYNC_RUN_ON_STARTUP` | Runtime override (Admin -> App config) | Billing module sync/backfill behavior setting. | `src/server.js:436` |
| `BILLING_BACKFILL_DEFAULT_MONTHS` | Runtime override (Admin -> App config) | Billing module sync/backfill behavior setting. | `src/modules/billing/server.js:31` |
| `BILLING_BACKFILL_MAX_MONTHS` | Runtime override (Admin -> App config) | Billing module sync/backfill behavior setting. | `src/modules/billing/server.js:28` |
| `BILLING_STARTUP_BACKFILL_DELAY_MS` | Runtime override (Admin -> App config) | Billing module sync/backfill behavior setting. | `src/server.js:458` |
| `BILLING_STARTUP_BACKFILL_MISSING_ENABLED` | Runtime override (Admin -> App config) | Billing module sync/backfill behavior setting. | `src/server.js:450` |
| `BILLING_STARTUP_BACKFILL_MISSING_MONTHS` | Runtime override (Admin -> App config) | Billing module sync/backfill behavior setting. | `src/server.js:454` |
| `CACHE_TTL_MINUTES` | Runtime override (Admin -> App config) | Cache retention behavior setting. | `src/modules/storage/src/server.js:90` |
| `CHECKMK_API_KEY` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:246` |
| `CHECKMK_AUTH_SCHEME` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:247` |
| `CHECKMK_AUTOMATION_KEY` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:246` |
| `CHECKMK_BASE_URL` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:245` |
| `CHECKMK_CF_CLIENT_ID` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:255` |
| `CHECKMK_CF_CLIENT_SECRET` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:256` |
| `CHECKMK_ENABLED` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:251` |
| `CHECKMK_FIREWALL_HOSTS` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:248` |
| `CHECKMK_HOSTS` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:248` |
| `CHECKMK_SERVICE_FILTER_REGEX` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:262` |
| `CHECKMK_SYNC_INTERVAL_MS` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:258` |
| `CHECKMK_SYNC_RUN_ON_STARTUP` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:260` |
| `CHECKMK_SYNC_STARTUP_DELAY_MS` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:261` |
| `CHECKMK_TIMEOUT_MS` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:259` |
| `CHECKMK_URL` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:245` |
| `CLOUD_METRICS_AWS` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:469` |
| `CLOUD_METRICS_AWS_CONCURRENCY` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:512` |
| `CLOUD_METRICS_AWS_LOOKBACK_MINUTES` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:516` |
| `CLOUD_METRICS_AWS_MAX_METRICS_PER_ACCOUNT` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:520` |
| `CLOUD_METRICS_AWS_MAX_RETRIES` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:508` |
| `CLOUD_METRICS_AWS_NAMESPACES` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:522` |
| `CLOUD_METRICS_AWS_REGIONS` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/aws-account-config.js:201`, `src/aws-account-config.js:354`, `src/aws-account-config.js:417` +1 more |
| `CLOUD_METRICS_AZURE` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:468` |
| `CLOUD_METRICS_AZURE_CONCURRENCY` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:488` |
| `CLOUD_METRICS_AZURE_LOOKBACK_MINUTES` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:496` |
| `CLOUD_METRICS_AZURE_MAX_RESOURCES` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:492` |
| `CLOUD_METRICS_AZURE_MAX_RETRIES` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:480` |
| `CLOUD_METRICS_AZURE_METRIC_BATCH_DELAY_MS` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:504` |
| `CLOUD_METRICS_AZURE_METRIC_BATCH_SIZE` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:500` |
| `CLOUD_METRICS_AZURE_TIMEOUT_MS` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:484` |
| `CLOUD_METRICS_RACKSPACE` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:470` |
| `CLOUD_METRICS_RACKSPACE_CONCURRENCY` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:534` |
| `CLOUD_METRICS_RACKSPACE_LOOKBACK_MINUTES` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:550` |
| `CLOUD_METRICS_RACKSPACE_MAX_ENTITIES` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:542` |
| `CLOUD_METRICS_RACKSPACE_MAX_METRICS_PER_CHECK` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:546` |
| `CLOUD_METRICS_RACKSPACE_MAX_RETRIES` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:526` |
| `CLOUD_METRICS_RACKSPACE_PAGE_SIZE` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:538` |
| `CLOUD_METRICS_RACKSPACE_PLOT_POINTS` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:554` |
| `CLOUD_METRICS_RACKSPACE_TIMEOUT_MS` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:530` |
| `CLOUD_METRICS_SYNC_ENABLED` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:466` |
| `CLOUD_METRICS_SYNC_INTERVAL_MS` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:463` |
| `CLOUD_METRICS_SYNC_RUN_ON_STARTUP` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:472` |
| `CLOUD_METRICS_SYNC_STARTUP_DELAY_MS` | Runtime override (Admin -> App config) | Cloud metrics scheduler/collector setting. | `src/server.js:476` |
| `CLOUDFLARE_ACCESS_CLIENT_ID` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:255` |
| `CLOUDFLARE_ACCESS_CLIENT_SECRET` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/firewall/server.js:256` |
| `CLOUDSTUDIO_BILLING_DIR` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/services.js:36` |
| `CLOUDSTUDIO_BRAND_LOGIN` | Bootstrap `.env` (restart required) | Login page branding value. | `src/server.js:367`, `src/server.js:418`, `src/server.js:924` +1 more |
| `CLOUDSTUDIO_BRAND_MAIN` | Bootstrap `.env` (restart required) | Main shell branding value. | `src/server.js:371`, `src/server.js:422`, `src/server.js:925` +1 more |
| `CLOUDSTUDIO_DB_FILE` | Bootstrap `.env` (restart required) | SQLite database location. | `src/db.js:7` |
| `CLOUDSTUDIO_FIREWALL_DIR` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/services.js:54` |
| `CLOUDSTUDIO_GRAFANA_CLOUD_DIR` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/services.js:48` |
| `CLOUDSTUDIO_IP_ADDRESS_DIR` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/services.js:30` |
| `CLOUDSTUDIO_PORT` | Bootstrap `.env` (restart required) | Main HTTP listener port for the CloudStudio server. | `src/server.js:57` |
| `CLOUDSTUDIO_PRICE_DIR` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/services.js:24` |
| `CLOUDSTUDIO_SECRET_KEY` | Bootstrap `.env` (restart required) | Master encryption key for sensitive values (runtime config, backup config, vendor credentials). | `src/crypto.js:3` |
| `CLOUDSTUDIO_SESSION_COOKIE_NAME` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/auth.js:4` |
| `CLOUDSTUDIO_SESSION_TTL_MS` | Bootstrap/internal env | Session lifetime override in milliseconds. | `src/auth.js:7` |
| `CLOUDSTUDIO_SINGLE_ENV` | Bootstrap/internal env | Unified single-environment mode marker. | `src/modules/pricing/server.js:22`, `src/server.js:682`, `src/server.js:683` |
| `CLOUDSTUDIO_STORAGE_DIR` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/services.js:18` |
| `CLOUDSTUDIO_TAG_DIR` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/services.js:42` |
| `CONTAINER_SYNC_CONCURRENCY` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/storage/src/server.js:95` |
| `GCP_API_KEY` | Runtime override (Admin -> App config) | GCP integration/pricing setting. | `src/modules/pricing/server.js:3452` |
| `GCP_BILLING_ACCOUNT_ID` | Runtime override (Admin -> App config) | GCP integration/pricing setting. | `src/server.js:2502` |
| `GCP_PRICING_API_KEY` | Runtime override (Admin -> App config) | GCP integration/pricing setting. | `src/modules/pricing/server.js:3451` |
| `GCP_PROJECT_ID` | Runtime override (Admin -> App config) | GCP integration/pricing setting. | `src/server.js:2502` |
| `GOOGLE_API_KEY` | Runtime override (Admin -> App config) | Google API integration setting. | `src/modules/pricing/server.js:3453` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Runtime override (Admin -> App config) | Google API integration setting. | `src/server.js:2502` |
| `GRAFANA_CLOUD_ACCESS_POLICY_TOKEN` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:3445`, `src/modules/billing/server.js:2695` |
| `GRAFANA_CLOUD_ACCOUNT_ID` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/modules/billing/server.js:2691` |
| `GRAFANA_CLOUD_ACCOUNT_LABEL` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/modules/billing/server.js:2693` |
| `GRAFANA_CLOUD_ACCOUNTS_JSON` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/modules/billing/server.js:2676` |
| `GRAFANA_CLOUD_API_BASE_URL` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:3482`, `src/modules/billing/server.js:2698` |
| `GRAFANA_CLOUD_API_KEY` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:3447`, `src/modules/billing/server.js:2697` |
| `GRAFANA_CLOUD_API_TOKEN` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:3446`, `src/modules/billing/server.js:2696` |
| `GRAFANA_CLOUD_BILLING_MODEL` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:1579`, `src/modules/billing/server.js:2700` |
| `GRAFANA_CLOUD_CURRENCY` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:3530`, `src/modules/billing/server.js:2699` |
| `GRAFANA_CLOUD_FLEX_COMMIT_AMOUNT` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:1594`, `src/modules/billing/server.js:2702` |
| `GRAFANA_CLOUD_FLEX_COMMIT_AUTO_RENEW` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:1619`, `src/modules/billing/server.js:2707` |
| `GRAFANA_CLOUD_FLEX_COMMIT_CURRENCY` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:1623`, `src/modules/billing/server.js:2703` |
| `GRAFANA_CLOUD_FLEX_COMMIT_ENABLED` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:1587`, `src/modules/billing/server.js:2701` |
| `GRAFANA_CLOUD_FLEX_COMMIT_REPORTING_MODE` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:1607`, `src/modules/billing/server.js:2706` |
| `GRAFANA_CLOUD_FLEX_COMMIT_START_DATE` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:1597`, `src/modules/billing/server.js:2704` |
| `GRAFANA_CLOUD_FLEX_COMMIT_YEARS` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:1603`, `src/modules/billing/server.js:2705` |
| `GRAFANA_CLOUD_INGEST_LOOKBACK_MONTHS` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/modules/grafana-cloud/server.js:130`, `src/modules/grafana-cloud/server.js:579` |
| `GRAFANA_CLOUD_INGEST_STARTUP_DELAY_MS` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/modules/grafana-cloud/server.js:606` |
| `GRAFANA_CLOUD_INGEST_SYNC_ENABLED` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/modules/grafana-cloud/server.js:574` |
| `GRAFANA_CLOUD_INGEST_SYNC_INTERVAL_MS` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/modules/grafana-cloud/server.js:577` |
| `GRAFANA_CLOUD_INGEST_SYNC_RUN_ON_STARTUP` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/modules/grafana-cloud/server.js:604` |
| `GRAFANA_CLOUD_ORG_SLUG` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:3464`, `src/modules/billing/server.js:2691`, `src/modules/billing/server.js:2692` |
| `GRAFANA_CLOUD_STACK_ACCESS_POLICY_TOKEN` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:3454` |
| `GRAFANA_CLOUD_STACK_API_TOKEN` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:3455` |
| `GRAFANA_CLOUD_STACK_TOKEN` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:3456` |
| `GRAFANA_CLOUD_STACK_URL` | Runtime override (Admin -> App config) | Grafana-Cloud billing/ingest integration setting. | `src/connectors/billing.js:3489` |
| `HTTP_PROXY` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/storage/src/proxy.js:20` |
| `HTTPS_PROXY` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/storage/src/proxy.js:20` |
| `LOG_ENABLE_DEBUG` | Runtime override (Admin -> App config) | Server logging behavior setting. | `src/modules/storage/src/logger.js:17` |
| `LOG_ENABLE_ERROR` | Runtime override (Admin -> App config) | Server logging behavior setting. | `src/modules/storage/src/logger.js:20` |
| `LOG_ENABLE_INFO` | Runtime override (Admin -> App config) | Server logging behavior setting. | `src/modules/storage/src/logger.js:18` |
| `LOG_ENABLE_WARN` | Runtime override (Admin -> App config) | Server logging behavior setting. | `src/modules/storage/src/logger.js:19` |
| `LOG_HTTP_DEBUG` | Runtime override (Admin -> App config) | Server logging behavior setting. | `src/modules/storage/src/logger.js:24` |
| `LOG_HTTP_REQUESTS` | Runtime override (Admin -> App config) | Server logging behavior setting. | `src/modules/storage/src/logger.js:23` |
| `LOG_INCLUDE_TIMESTAMP` | Runtime override (Admin -> App config) | Server logging behavior setting. | `src/modules/storage/src/logger.js:21` |
| `LOG_JSON` | Runtime override (Admin -> App config) | Server logging behavior setting. | `src/modules/storage/src/logger.js:22` |
| `LOG_MAX_VALUE_LENGTH` | Runtime override (Admin -> App config) | Server logging behavior setting. | `src/modules/storage/src/logger.js:25` |
| `METRICS_CACHE_TTL_MINUTES` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/storage/src/server.js:92` |
| `NODE_ENV` | Bootstrap/internal env | Node runtime mode (`development`/`production`). | `src/auth.js:558`, `src/auth.js:568`, `src/modules/pricing/server.js:1608` +2 more |
| `PORT` | Bootstrap `.env` (restart required) | Fallback HTTP port when `CLOUDSTUDIO_PORT` is not set. | `src/modules/pricing/server.js:83`, `src/modules/storage/src/server.js:89`, `src/server.js:57` |
| `PRICING_ARM_REGION_NAME` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:102` |
| `PRICING_AS_OF_DATE` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:151` |
| `PRICING_BYTES_PER_GB` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:152` |
| `PRICING_CACHE_CONCURRENCY` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/pricing/server.js:91` |
| `PRICING_CACHE_REFRESH_INTERVAL_MS` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/pricing/server.js:96` |
| `PRICING_CACHE_WARMUP` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/pricing/server.js:88` |
| `PRICING_CURRENCY` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:148` |
| `PRICING_DAYS_IN_MONTH` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:153` |
| `PRICING_EGRESS_PRODUCT_NAME` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:105` |
| `PRICING_EGRESS_TIER0_GB` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:160` |
| `PRICING_EGRESS_TIER1_GB` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:161` |
| `PRICING_EGRESS_TIER2_GB` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:162` |
| `PRICING_EGRESS_TIER3_GB` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:163` |
| `PRICING_EGRESS_TIER4_GB` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:164` |
| `PRICING_EGRESS_TIER5_GB` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:165` |
| `PRICING_INGRESS_GB` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:167` |
| `PRICING_REGION_LABEL` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:149` |
| `PRICING_SOURCE_URL` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:150` |
| `PRICING_STORAGE_HOT_LRS_TIER1_GB_MONTH` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:155` |
| `PRICING_STORAGE_HOT_LRS_TIER2_GB_MONTH` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:156` |
| `PRICING_STORAGE_HOT_LRS_TIER3_GB_MONTH` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:157` |
| `PRICING_STORAGE_PRODUCT_NAME` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:103` |
| `PRICING_STORAGE_SKU_NAME` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:104` |
| `PRICING_SYNC_INTERVAL_HOURS` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:99` |
| `PRICING_TRANSACTION_LABEL` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:170` |
| `PRICING_TRANSACTION_UNIT_PRICE` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:169` |
| `PRICING_TRANSACTION_UNIT_SIZE` | Runtime override (Admin -> App config) | Pricing module cache or cost-model assumption setting. | `src/modules/storage/src/server.js:168` |
| `PULL_ALL_JOB_HISTORY_LIMIT` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/storage/src/server.js:98` |
| `RACKSPACE_ACCOUNT_ID` | Runtime override (Admin -> App config) | Rackspace billing/metrics integration setting. | `src/connectors/billing.js:2439`, `src/modules/billing/server.js:2842`, `src/server.js:2414` |
| `RACKSPACE_ACCOUNT_LABEL` | Runtime override (Admin -> App config) | Rackspace billing/metrics integration setting. | `src/modules/billing/server.js:2843`, `src/server.js:2370`, `src/server.js:2416` |
| `RACKSPACE_ACCOUNT_NAME` | Runtime override (Admin -> App config) | Rackspace billing/metrics integration setting. | `src/modules/billing/server.js:2843`, `src/server.js:2416` |
| `RACKSPACE_ACCOUNT_NUMBER` | Runtime override (Admin -> App config) | Rackspace billing/metrics integration setting. | `src/server.js:2357`, `src/server.js:2414`, `src/server.js:2415` |
| `RACKSPACE_ACCOUNTS_JSON` | Runtime override (Admin -> App config) | Rackspace billing/metrics integration setting. | `src/modules/billing/server.js:2827`, `src/server.js:2396` |
| `RACKSPACE_API_KEY` | Runtime override (Admin -> App config) | Rackspace billing/metrics integration setting. | `src/connectors/billing.js:2367`, `src/modules/billing/server.js:2845`, `src/server.js:2418` |
| `RACKSPACE_BASE_URL` | Runtime override (Admin -> App config) | Rackspace billing/metrics integration setting. | `src/connectors/billing.js:2418`, `src/modules/billing/server.js:2846` |
| `RACKSPACE_BILLING_BASE_URL` | Runtime override (Admin -> App config) | Rackspace billing/metrics integration setting. | `src/connectors/billing.js:2414`, `src/modules/billing/server.js:2846` |
| `RACKSPACE_IDENTITY_URL` | Runtime override (Admin -> App config) | Rackspace billing/metrics integration setting. | `src/connectors/billing.js:2374`, `src/modules/billing/server.js:2847`, `src/server.js:2378` +1 more |
| `RACKSPACE_REGION` | Runtime override (Admin -> App config) | Rackspace billing/metrics integration setting. | `src/connectors/billing.js:2371`, `src/modules/billing/server.js:2848`, `src/server.js:2381` +1 more |
| `RACKSPACE_REQUIRE_DETAIL_CSV` | Runtime override (Admin -> App config) | Rackspace billing/metrics integration setting. | `src/connectors/billing.js:2423` |
| `RACKSPACE_USERNAME` | Runtime override (Admin -> App config) | Rackspace billing/metrics integration setting. | `src/connectors/billing.js:2361`, `src/modules/billing/server.js:2844`, `src/modules/billing/server.js:4271` +1 more |
| `SECURITY_CACHE_TTL_MINUTES` | Runtime override (Admin -> App config) | Security scan/cache behavior setting. | `src/modules/storage/src/server.js:91` |
| `SENDGRID_ACCOUNT_LABEL` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/billing/server.js:2540` |
| `SENDGRID_BILLING_ACCOUNT_ID` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/connectors/billing.js:3306`, `src/modules/billing/server.js:2539` |
| `SENDGRID_BILLING_ACCOUNT_LABEL` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/connectors/billing.js:3312`, `src/modules/billing/server.js:2540` |
| `SENDGRID_BILLING_ACCOUNTS_JSON` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/billing/server.js:2524` |
| `SENDGRID_BILLING_API_KEY` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/connectors/billing.js:3288`, `src/modules/billing/server.js:2541` |
| `SENDGRID_BILLING_CURRENCY` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/connectors/billing.js:3319`, `src/modules/billing/server.js:2495`, `src/modules/billing/server.js:2544` |
| `SENDGRID_BILLING_ENDPOINT` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/connectors/billing.js:3299`, `src/modules/billing/server.js:2542` |
| `SENDGRID_BILLING_MONTH_OFFSET` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/connectors/billing.js:3315`, `src/modules/billing/server.js:2493`, `src/modules/billing/server.js:2543` |
| `SQLITE_PATH` | Bootstrap `.env` (restart required) | Shared SQLite DB path used by integrated modules. | `src/modules/storage/src/db.js:6`, `src/server.js:662`, `src/server.js:664` |
| `UI_PULL_ALL_CONCURRENCY` | Bootstrap/internal env | CloudStudio runtime/configuration key. See source usage for exact behavior. | `src/modules/storage/src/server.js:96` |
| `VSAX_API_MAX_CONCURRENCY` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:2` |
| `VSAX_API_MAX_RETRIES` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:4` |
| `VSAX_API_MIN_INTERVAL_MS` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:3` |
| `VSAX_API_TOKEN_ID` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:223` |
| `VSAX_API_TOKEN_SECRET` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:224` |
| `VSAX_ASSET_FILTER` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:230` |
| `VSAX_BASE_URL` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:222` |
| `VSAX_CACHE_TTL_HOURS` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/server.js:126` |
| `VSAX_DISK_VALUE_UNIT` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:586` |
| `VSAX_GROUP_FILTERS` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:220` |
| `VSAX_GROUP_SYNC_CONCURRENCY` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/server.js:127` |
| `VSAX_GROUPS` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:220` |
| `VSAX_INCLUDE` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:225` |
| `VSAX_MAX_PAGES` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:229` |
| `VSAX_PAGE_SIZE` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:228` |
| `VSAX_PRICING_AS_OF_DATE` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/server.js:202` |
| `VSAX_PRICING_BYTES_PER_TB` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/server.js:203` |
| `VSAX_PRICING_CURRENCY` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/server.js:200` |
| `VSAX_PRICING_DAYS_IN_MONTH` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/server.js:204` |
| `VSAX_PRICING_SOURCE_URL` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/server.js:201` |
| `VSAX_PRICING_STORAGE_TB_MONTH` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/server.js:205` |
| `VSAX_SERVER_URL` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:222` |
| `VSAX_SYNC_INTERVAL_HOURS` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/server.js:122` |
| `VSAX_SYNC_INTERVAL_MINUTES` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/server.js:118` |
| `VSAX_TOKEN_ID` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:223` |
| `VSAX_TOKEN_SECRET` | Runtime override (Admin -> App config) | VSAx integration setting (inventory/live view/price compare source). | `src/modules/storage/src/vsax.js:224` |
| `WASABI_ACCESS_KEY` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/wasabi.js:224` |
| `WASABI_ACCOUNT_ID` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/billing/server.js:3208`, `src/modules/storage/src/wasabi.js:230` |
| `WASABI_ACCOUNT_LABEL` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/billing/server.js:3047`, `src/modules/billing/server.js:3141`, `src/modules/billing/server.js:3209` +1 more |
| `WASABI_ACCOUNT_SYNC_CONCURRENCY` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/server.js:108` |
| `WASABI_ACCOUNTS_JSON` | Runtime override (Admin -> App config) | Legacy/fallback Wasabi account list JSON (WACM/main/inventory/pricing/billing). Prefer Vendor onboarding. | `src/modules/storage/src/wasabi.js:213`, `src/server.js:2453` |
| `WASABI_API_BASE_BACKOFF_MS` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/wasabi.js:13` |
| `WASABI_API_MAX_BACKOFF_MS` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/wasabi.js:14` |
| `WASABI_API_MAX_CONCURRENCY` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/wasabi.js:10` |
| `WASABI_API_MAX_RETRIES` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/wasabi.js:12` |
| `WASABI_API_MIN_INTERVAL_MS` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/wasabi.js:11` |
| `WASABI_BUCKET_SYNC_CONCURRENCY` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/server.js:109` |
| `WASABI_CACHE_TTL_HOURS` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/server.js:107` |
| `WASABI_MAIN_ACCOUNT_ID` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/billing/server.js:3208` |
| `WASABI_MAIN_ACCOUNT_LABEL` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/billing/server.js:3140`, `src/modules/billing/server.js:3209` |
| `WASABI_MAIN_ACCOUNTS_JSON` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/billing/server.js:3193` |
| `WASABI_MAIN_ACTIVE_STORAGE_TB_MONTH` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2094`, `src/modules/billing/server.js:3219` |
| `WASABI_MAIN_API_CALLS_PER_MILLION` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2118`, `src/modules/billing/server.js:3222` |
| `WASABI_MAIN_CONTROL_USAGE_ENDPOINT` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:3080`, `src/modules/billing/server.js:3216` |
| `WASABI_MAIN_CURRENCY` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2128`, `src/modules/billing/server.js:3217` |
| `WASABI_MAIN_DAYS_IN_MONTH` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2085`, `src/modules/billing/server.js:3218` |
| `WASABI_MAIN_DELETED_STORAGE_TB_MONTH` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2103`, `src/modules/billing/server.js:3220` |
| `WASABI_MAIN_EGRESS_TB_COST` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2126`, `src/modules/billing/server.js:3224` |
| `WASABI_MAIN_INCLUDE_ALL_ACCOUNTS` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:3093`, `src/modules/billing/server.js:3225` |
| `WASABI_MAIN_INCLUDE_SUB_ACCOUNTS` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:3097`, `src/modules/billing/server.js:3226` |
| `WASABI_MAIN_INGRESS_TB_COST` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2122`, `src/modules/billing/server.js:3223` |
| `WASABI_MAIN_MAX_PAGES` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:3090` |
| `WASABI_MAIN_MIN_BILLABLE_TB` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2111`, `src/modules/billing/server.js:3221` |
| `WASABI_MAIN_PAGE_SIZE` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:3086` |
| `WASABI_MAIN_USAGE_ENDPOINT` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:3070`, `src/modules/billing/server.js:3213` |
| `WASABI_MAIN_WACM_API_KEY` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:3055`, `src/modules/billing/server.js:3211` |
| `WASABI_MAIN_WACM_USERNAME` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:3047`, `src/modules/billing/server.js:3210` |
| `WASABI_PRICING_AS_OF_DATE` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/server.js:176` |
| `WASABI_PRICING_BYTES_PER_TB` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/server.js:177` |
| `WASABI_PRICING_CURRENCY` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/server.js:174` |
| `WASABI_PRICING_DAYS_IN_MONTH` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2086`, `src/modules/storage/src/server.js:178` |
| `WASABI_PRICING_MIN_BILLABLE_TB` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2112`, `src/modules/storage/src/server.js:180` |
| `WASABI_PRICING_SOURCE_URL` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/server.js:134` |
| `WASABI_PRICING_STORAGE_TB_MONTH` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2095`, `src/modules/storage/src/server.js:179` |
| `WASABI_PRICING_SYNC_INTERVAL_HOURS` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/server.js:130` |
| `WASABI_REGION` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/wasabi.js:234` |
| `WASABI_S3_ENDPOINT` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/wasabi.js:235` |
| `WASABI_SECRET_KEY` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/wasabi.js:225` |
| `WASABI_STATS_ENDPOINT` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/wasabi.js:236` |
| `WASABI_SYNC_INTERVAL_HOURS` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/storage/src/server.js:106` |
| `WASABI_WACM_ACCOUNT_ID` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/billing/server.js:3046` |
| `WASABI_WACM_ACCOUNT_LABEL` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/billing/server.js:3047` |
| `WASABI_WACM_ACCOUNTS_JSON` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/modules/billing/server.js:3031` |
| `WASABI_WACM_API_KEY` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2916`, `src/connectors/billing.js:3056`, `src/modules/billing/server.js:3049` +1 more |
| `WASABI_WACM_CURRENCY` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2939` |
| `WASABI_WACM_ENDPOINT` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2927`, `src/connectors/billing.js:3072`, `src/modules/billing/server.js:3050` +1 more |
| `WASABI_WACM_MAX_PAGES` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2937` |
| `WASABI_WACM_PAGE_SIZE` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2933` |
| `WASABI_WACM_USAGE_ENDPOINT` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:3071`, `src/modules/billing/server.js:3214` |
| `WASABI_WACM_USERNAME` | Runtime override (Admin -> App config) | Wasabi integration setting (WACM/main/inventory/pricing/billing). | `src/connectors/billing.js:2909`, `src/connectors/billing.js:3048`, `src/modules/billing/server.js:3048` +1 more |
