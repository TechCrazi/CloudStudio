#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const ROOT_DIR = path.resolve(__dirname, '..');
const SRC_DIR = path.join(ROOT_DIR, 'src');
const OUTPUT_DIR = path.join(ROOT_DIR, 'docs');
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'APP_CONFIG_KEYS.md');
const PUBLIC_OUTPUT_DIR = path.join(ROOT_DIR, 'public', 'docs');
const PUBLIC_OUTPUT_FILE = path.join(PUBLIC_OUTPUT_DIR, 'app-config-keys.md');

const PROCESS_ENV_DOT_RE = /process\.env\.([A-Z0-9_]+)/g;
const PROCESS_ENV_BRACKET_RE = /process\.env\[['"]([A-Z0-9_]+)['"]\]/g;

const RUNTIME_ENV_OVERRIDE_BLOCKLIST = new Set([
  'PORT',
  'CLOUDSTUDIO_PORT',
  'CLOUDSTUDIO_SECRET_KEY',
  'CLOUDSTUDIO_DB_FILE',
  'CLOUDSTUDIO_BRAND_LOGIN',
  'CLOUDSTUDIO_BRAND_MAIN',
  'SQLITE_PATH',
  'AUTH_DB_FILE',
  'APP_AUTH_USERS',
  'APP_LOGIN_USER',
  'APP_LOGIN_PASSWORD',
  'APP_ADMIN_USERS'
]);

const RUNTIME_ENV_SEED_PREFIXES = [
  'AWS_',
  'AZURE_',
  'GCP_',
  'GOOGLE_',
  'RACKSPACE_',
  'GRAFANA_',
  'WASABI_',
  'BILLING_',
  'CLOUD_METRICS_',
  'PRICING_',
  'VSAX_',
  'DB_BACKUP_',
  'LOG_',
  'CACHE_',
  'SECURITY_',
  'ACCOUNT_',
  'AUTH_'
];

const BOOTSTRAP_KEY_DOCS = [
  ['CLOUDSTUDIO_PORT', 'Main HTTP port for CloudStudio (`PORT` fallback supported).', 'Recommended'],
  ['CLOUDSTUDIO_SECRET_KEY', 'Master secret used to encrypt/decrypt sensitive DB settings and secrets.', 'Required'],
  ['CLOUDSTUDIO_DB_FILE', 'Path to the SQLite database file.', 'Recommended'],
  ['APP_AUTH_USERS', 'Bootstrap `username:password` list for initial local users.', 'Required'],
  ['APP_ADMIN_USERS', 'Comma-separated bootstrap admin usernames.', 'Optional'],
  ['CLOUDSTUDIO_BRAND_LOGIN', 'Default login-screen brand label/initials (`Name|INIT`).', 'Optional'],
  ['CLOUDSTUDIO_BRAND_MAIN', 'Default main-shell brand label/initials (`Name|INIT`).', 'Optional'],
  ['CLOUDSTUDIO_SINGLE_ENV', 'Sets unified single-env mode marker.', 'Optional']
];

const EXPLICIT_PURPOSE = {
  PORT: 'Fallback HTTP port when `CLOUDSTUDIO_PORT` is not set.',
  NODE_ENV: 'Node runtime mode (`development`/`production`).',
  CLOUDSTUDIO_PORT: 'Main HTTP listener port for the CloudStudio server.',
  CLOUDSTUDIO_SECRET_KEY: 'Master encryption key for sensitive values (runtime config, backup config, vendor credentials).',
  CLOUDSTUDIO_DB_FILE: 'SQLite database location.',
  CLOUDSTUDIO_SINGLE_ENV: 'Unified single-environment mode marker.',
  APP_AUTH_USERS: 'Bootstrap credentials used for initial user seeding.',
  APP_ADMIN_USERS: 'Bootstrap admin users list.',
  APP_LOGIN_USER: 'Legacy single-user bootstrap username.',
  APP_LOGIN_PASSWORD: 'Legacy single-user bootstrap password.',
  CLOUDSTUDIO_BRAND_LOGIN: 'Login page branding value.',
  CLOUDSTUDIO_BRAND_MAIN: 'Main shell branding value.',
  CLOUDSTUDIO_SESSION_TTL_MS: 'Session lifetime override in milliseconds.',
  AUTH_SESSION_TTL_MS: 'Auth/session TTL override in milliseconds.',
  AUTH_STATE_MAX_BYTES: 'Maximum per-user persisted UI/auth state payload size.',
  AUTH_DATA_DIR: 'Auth state storage directory.',
  AUTH_STATE_DIR: 'User-state directory for auth module.',
  AUTH_DB_FILE: 'Auth DB file path (forced to unified DB when not explicitly set).',
  AUTH_USERS_FILE: 'Legacy auth users JSON file path.',
  SQLITE_PATH: 'Shared SQLite DB path used by integrated modules.',
  DB_BACKUP_ENABLED: 'Enable/disable S3-compatible DB backup scheduler.',
  DB_BACKUP_BUCKET: 'Target bucket for DB backups.',
  DB_BACKUP_PREFIX: 'Object prefix path for backups in target bucket.',
  DB_BACKUP_REGION: 'S3 region for backup target.',
  DB_BACKUP_ENDPOINT: 'S3-compatible endpoint URL.',
  DB_BACKUP_ACCESS_KEY_ID: 'S3 access key ID for backup target.',
  DB_BACKUP_SECRET_ACCESS_KEY: 'S3 secret key for backup target.',
  DB_BACKUP_SESSION_TOKEN: 'Optional S3 session token.',
  DB_BACKUP_INTERVAL_MS: 'Backup schedule interval in milliseconds.',
  DB_BACKUP_RETENTION_DAYS: 'Retention period used for lifecycle policy.',
  DB_BACKUP_APPLY_LIFECYCLE: 'Whether lifecycle policy should be auto-applied.',
  DB_BACKUP_COMPRESS: 'Enable compressed backup artifact creation.',
  DB_BACKUP_VERIFY_QUICK_CHECK: 'Enable quick restore validation after backup generation.'
};

const PREFIX_PURPOSE = [
  ['AWS_', 'AWS integration setting (inventory, pricing, billing, or tagging).'],
  ['AZURE_', 'Azure integration setting (inventory, pricing, billing, or metrics).'],
  ['GCP_', 'GCP integration/pricing setting.'],
  ['GOOGLE_', 'Google API integration setting.'],
  ['RACKSPACE_', 'Rackspace billing/metrics integration setting.'],
  ['GRAFANA_', 'Grafana-Cloud billing/ingest integration setting.'],
  ['WASABI_', 'Wasabi integration setting (WACM/main/inventory/pricing/billing).'],
  ['VSAX_', 'VSAx integration setting (inventory/live view/price compare source).'],
  ['BILLING_', 'Billing module sync/backfill behavior setting.'],
  ['CLOUD_METRICS_', 'Cloud metrics scheduler/collector setting.'],
  ['PRICING_', 'Pricing module cache or cost-model assumption setting.'],
  ['LOG_', 'Server logging behavior setting.'],
  ['CACHE_', 'Cache retention behavior setting.'],
  ['SECURITY_', 'Security scan/cache behavior setting.'],
  ['ACCOUNT_', 'Account sync/concurrency setting.'],
  ['AUTH_', 'Auth/session/state behavior setting.'],
  ['DB_BACKUP_', 'DB backup scheduler/target setting.']
];

function walkFiles(dir, output = []) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      walkFiles(fullPath, output);
      continue;
    }
    if (entry.isFile() && fullPath.endsWith('.js')) {
      output.push(fullPath);
    }
  }
  return output;
}

function addOccurrence(map, key, filePath, lineNumber) {
  const existing = map.get(key) || [];
  existing.push({
    file: path.relative(ROOT_DIR, filePath).replace(/\\/g, '/'),
    line: lineNumber
  });
  map.set(key, existing);
}

function extractEnvKeysFromFile(filePath, map) {
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split(/\r?\n/);
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    let match = null;

    PROCESS_ENV_DOT_RE.lastIndex = 0;
    while ((match = PROCESS_ENV_DOT_RE.exec(line)) !== null) {
      addOccurrence(map, match[1], filePath, index + 1);
    }

    PROCESS_ENV_BRACKET_RE.lastIndex = 0;
    while ((match = PROCESS_ENV_BRACKET_RE.exec(line)) !== null) {
      addOccurrence(map, match[1], filePath, index + 1);
    }
  }
}

function dedupeOccurrences(items) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const id = `${item.file}:${item.line}`;
    if (seen.has(id)) {
      continue;
    }
    seen.add(id);
    out.push(item);
  }
  return out.sort((left, right) => {
    if (left.file === right.file) {
      return left.line - right.line;
    }
    return left.file.localeCompare(right.file);
  });
}

function toUsageSummary(items, maxItems = 3) {
  if (!items.length) {
    return '-';
  }
  const shown = items.slice(0, maxItems).map((item) => `\`${item.file}:${item.line}\``);
  const extra = items.length > maxItems ? ` +${items.length - maxItems} more` : '';
  return `${shown.join(', ')}${extra}`;
}

function inferScope(key) {
  if (RUNTIME_ENV_OVERRIDE_BLOCKLIST.has(key)) {
    return 'Bootstrap `.env` (restart required)';
  }
  const runtimeEligible = RUNTIME_ENV_SEED_PREFIXES.some((prefix) => key.startsWith(prefix));
  if (runtimeEligible) {
    return 'Runtime override (Admin -> App config)';
  }
  return 'Bootstrap/internal env';
}

function inferPurpose(key) {
  if (EXPLICIT_PURPOSE[key]) {
    return EXPLICIT_PURPOSE[key];
  }
  for (const [prefix, text] of PREFIX_PURPOSE) {
    if (key.startsWith(prefix)) {
      return text;
    }
  }
  return 'CloudStudio runtime/configuration key. See source usage for exact behavior.';
}

function renderMarkdown(envMap) {
  const nowIso = new Date().toISOString();
  const allKeys = [...envMap.keys()].sort((left, right) => left.localeCompare(right));
  const rows = allKeys.map((key) => {
    const occurrences = dedupeOccurrences(envMap.get(key) || []);
    const scope = inferScope(key);
    const purpose = inferPurpose(key);
    const usage = toUsageSummary(occurrences);
    return `| \`${key}\` | ${scope} | ${purpose} | ${usage} |`;
  });

  const bootstrapRows = BOOTSTRAP_KEY_DOCS.map(
    ([key, purpose, level]) => `| \`${key}\` | ${level} | ${purpose} |`
  ).join('\n');

  return `# App Config Keys Reference

This document defines what each CloudStudio configuration key is used for.

- Generated: ${nowIso}
- Source: \`src/**/*.js\` (\`process.env.*\` references)
- Generator: \`scripts/generate-app-config-keys-doc.js\`

## Maintenance Rule

When new env/config keys are added or changed:

1. Update key behavior in code.
2. Run \`npm run docs:app-config-keys\`.
3. Verify this file changed as expected.
4. Verify the README section **Configuration key reference** still points here.

## Minimal Bootstrap .env Keys

These keys should stay in \`.env\` even when App Config runtime overrides are used:

| Key | Level | Purpose |
| --- | --- | --- |
${bootstrapRows}

## Runtime Override Notes

- Runtime overrides are edited in **Admin -> App config** and stored encrypted in DB.
- Not all keys are runtime-override eligible. Blocked bootstrap keys are kept env-driven.
- Some deep module settings may still require module restart for full effect.

## Full Key Reference

| Key | Scope | Purpose | Primary usage(s) |
| --- | --- | --- | --- |
${rows.join('\n')}
`;
}

function main() {
  const files = walkFiles(SRC_DIR);
  const envMap = new Map();
  for (const filePath of files) {
    extractEnvKeysFromFile(filePath, envMap);
  }

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  fs.mkdirSync(PUBLIC_OUTPUT_DIR, { recursive: true });
  const markdown = renderMarkdown(envMap);
  fs.writeFileSync(OUTPUT_FILE, markdown, 'utf8');
  fs.writeFileSync(PUBLIC_OUTPUT_FILE, markdown, 'utf8');
  process.stdout.write(
    `Generated ${path.relative(ROOT_DIR, OUTPUT_FILE)} and ${path.relative(ROOT_DIR, PUBLIC_OUTPUT_FILE)} with ${envMap.size} keys.\n`
  );
}

main();
