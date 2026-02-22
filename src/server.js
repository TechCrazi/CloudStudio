require('dotenv').config();

const fs = require('fs');
const crypto = require('crypto');
const path = require('path');
const express = require('express');
const MarkdownIt = require('markdown-it');
const { CloudWatchClient, ListMetricsCommand, GetMetricStatisticsCommand } = require('@aws-sdk/client-cloudwatch');

const {
  dbFile,
  listVendors,
  getVendorById,
  upsertVendor,
  deleteVendor,
  insertApiKey,
  listApiKeys,
  setApiKeyActive,
  deleteApiKey,
  findApiKeyByHash,
  touchApiKeyUsage,
  getAppSetting,
  upsertAppSetting,
  replaceCloudMetricsLatest,
  listCloudMetricsLatest,
  summarizeCloudMetricsProviders
} = require('./db');
const {
  initializeAuth,
  VIEW_KEYS,
  normalizeAllowedViewsInput,
  authSessionMiddleware,
  hasViewAccess,
  authenticateCredentials,
  toPublicUser,
  listUsers,
  createUser,
  updateUser,
  deleteUser,
  countAdminUsers,
  loginUser,
  logoutRequest
} = require('./auth');
const { encryptJson, decryptJson, generateApiKey, hashApiKey, maskApiKey } = require('./crypto');
const { getAwsAccountConfigs } = require('./aws-account-config');
const { createManagedServices } = require('./services');
const { createDbBackupScheduler } = require('./db-backup');
const { registerCloudMetricsRoutes } = require('./modules/cloud-metrics/server');
const {
  registerCloudDatabaseRoutes,
  normalizeCloudDatabaseProvider,
  buildCloudDatabaseViewFromCloudMetrics
} = require('./modules/cloud-database/server');

const app = express();
app.set('etag', false);
const port = Number.parseInt(process.env.PORT || process.env.CLOUDSTUDIO_PORT || '9090', 10);
const publicDir = path.resolve(__dirname, '..', 'public');
const docsDir = path.join(publicDir, 'docs');
const markdown = new MarkdownIt({
  html: false,
  linkify: true,
  typographer: true
});
const INTERNAL_API_TOKEN_HEADER = 'x-cloudstudio-internal-token';
const internalApiToken = crypto.randomBytes(24).toString('hex');
const FALSE_ENV_VALUES = new Set(['0', 'false', 'no', 'off']);

function parseEnvStringList(input, options = {}) {
  const lowercase = Boolean(options.lowercase);
  const raw = String(input || '').trim();
  if (!raw) {
    return [];
  }

  let values = [];
  if (raw.startsWith('[') && raw.endsWith(']')) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        values = parsed;
      }
    } catch (_error) {
      values = [];
    }
  }

  if (!values.length) {
    values = raw.split(',');
  }

  return values
    .map((item) =>
      String(item || '')
        .trim()
        .replace(/^[\[\]"']+/, '')
        .replace(/[\[\]"']+$/, '')
        .trim()
    )
    .filter(Boolean)
    .map((item) => (lowercase ? item.toLowerCase() : item));
}

function normalizeJsonEnvValue(rawValue) {
  const text = String(rawValue || '').trim();
  if (!text) {
    return '';
  }

  if (
    (text.startsWith("'") && text.endsWith("'") && text.length >= 2) ||
    (text.startsWith('"') && text.endsWith('"') && text.length >= 2)
  ) {
    return text.slice(1, -1).trim();
  }

  return text;
}

function parseBooleanEnv(value, defaultValue = true) {
  const text = String(value === undefined || value === null ? '' : value)
    .trim()
    .toLowerCase();
  if (!text) {
    return Boolean(defaultValue);
  }
  return !FALSE_ENV_VALUES.has(text);
}

function deriveBrandInitials(name, fallback = 'CS') {
  const rawName = String(name || '').trim();
  if (!rawName) {
    return fallback;
  }
  const tokens = rawName.toUpperCase().match(/[A-Z0-9]+/g) || [];
  if (!tokens.length) {
    return fallback;
  }
  if (tokens.length === 1) {
    return tokens[0].slice(0, 4) || fallback;
  }
  return (
    tokens
      .slice(0, 4)
      .map((token) => token.charAt(0))
      .join('')
      .slice(0, 4) || fallback
  );
}

function parseBrandingEnv(value, defaults = {}) {
  const fallbackName = String(defaults.name || 'CloudStudio').trim() || 'CloudStudio';
  const fallbackInitials = String(defaults.initials || '').trim() || deriveBrandInitials(fallbackName, 'CS');
  const raw = String(value || '').trim();
  if (!raw) {
    return {
      name: fallbackName,
      initials: fallbackInitials
    };
  }

  let parsedName = '';
  let parsedInitials = '';
  if (raw.startsWith('{') && raw.endsWith('}')) {
    try {
      const payload = JSON.parse(raw);
      parsedName = String(payload?.name || '').trim();
      parsedInitials = String(payload?.initials || '').trim();
    } catch (_error) {
      parsedName = '';
      parsedInitials = '';
    }
  }

  if (!parsedName && !parsedInitials) {
    const [namePart, initialsPart] = raw.split('|', 2);
    parsedName = String(namePart || '').trim();
    parsedInitials = String(initialsPart || '').trim();
  }

  const name = parsedName || fallbackName;
  const rawInitials = parsedInitials || deriveBrandInitials(name, fallbackInitials);
  const initials = String(rawInitials)
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')
    .slice(0, 8);
  return {
    name,
    initials: initials || fallbackInitials
  };
}

function extractMarkdownTitle(markdownSource = '', fallback = 'CloudStudio Docs') {
  const source = String(markdownSource || '');
  const match = source.match(/^\s*#\s+(.+?)\s*$/m);
  if (!match) {
    return fallback;
  }
  return String(match[1] || '').trim() || fallback;
}

function escapeHtml(text) {
  return String(text || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function resolveDocsFilePath(requestedPath = '') {
  const decoded = decodeURIComponent(String(requestedPath || '').trim());
  const trimmed = decoded.replace(/^\/+/, '');
  if (!trimmed) {
    return null;
  }
  const normalized = path.normalize(trimmed);
  if (!normalized || normalized.startsWith('..') || path.isAbsolute(normalized)) {
    return null;
  }
  const absolutePath = path.join(docsDir, normalized);
  const docsRoot = docsDir.endsWith(path.sep) ? docsDir : `${docsDir}${path.sep}`;
  if (absolutePath !== docsDir && !absolutePath.startsWith(docsRoot)) {
    return null;
  }
  return absolutePath;
}

function renderMarkdownPage({ title, relativePath, html }) {
  const safeTitle = escapeHtml(title || 'CloudStudio Docs');
  const safePath = escapeHtml(relativePath || '');
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${safeTitle}</title>
    <style>
      :root {
        color-scheme: dark;
      }
      body {
        margin: 0;
        font-family: Sora, Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
        background: radial-gradient(circle at 10% -30%, rgba(54, 115, 176, 0.26), rgba(6, 11, 16, 0.94) 48%), #070d14;
        color: #e6eef7;
        line-height: 1.6;
      }
      .page {
        max-width: 1180px;
        margin: 0 auto;
        padding: 22px 20px 42px;
      }
      .topbar {
        position: sticky;
        top: 0;
        z-index: 10;
        backdrop-filter: blur(8px);
        background: linear-gradient(180deg, rgba(7, 13, 20, 0.95), rgba(7, 13, 20, 0.78));
        border-bottom: 1px solid rgba(119, 152, 188, 0.24);
      }
      .topbar .row {
        max-width: 1180px;
        margin: 0 auto;
        padding: 10px 20px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
      }
      .brand {
        font-size: 13px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #9ab4cd;
      }
      .path {
        font-size: 12px;
        color: #87a9c8;
      }
      a {
        color: #7ec8ff;
      }
      a:hover {
        color: #a7ddff;
      }
      .doc {
        border: 1px solid rgba(125, 158, 194, 0.22);
        border-radius: 14px;
        background: rgba(10, 17, 25, 0.78);
        box-shadow: 0 20px 46px rgba(0, 0, 0, 0.36);
        padding: 24px;
      }
      h1, h2, h3, h4, h5, h6 {
        line-height: 1.3;
        margin-top: 1.2em;
        margin-bottom: 0.45em;
      }
      h1:first-child {
        margin-top: 0;
      }
      code {
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        font-size: 0.92em;
        background: rgba(78, 113, 153, 0.16);
        border: 1px solid rgba(130, 162, 196, 0.18);
        border-radius: 6px;
        padding: 0.08em 0.34em;
      }
      pre {
        overflow-x: auto;
        padding: 12px;
        border-radius: 10px;
        background: rgba(8, 14, 22, 0.92);
        border: 1px solid rgba(130, 162, 196, 0.24);
      }
      pre code {
        background: transparent;
        border: none;
        padding: 0;
      }
      table {
        width: 100%;
        border-collapse: collapse;
        margin: 12px 0;
        font-size: 13px;
      }
      th, td {
        border: 1px solid rgba(125, 158, 194, 0.22);
        padding: 8px 10px;
        vertical-align: top;
      }
      th {
        background: rgba(59, 101, 148, 0.28);
        text-align: left;
      }
      blockquote {
        margin: 10px 0;
        border-left: 3px solid rgba(126, 200, 255, 0.7);
        padding: 0 0 0 12px;
        color: #b9ccdf;
      }
      hr {
        border: 0;
        border-top: 1px solid rgba(125, 158, 194, 0.22);
        margin: 18px 0;
      }
    </style>
  </head>
  <body>
    <div class="topbar">
      <div class="row">
        <div class="brand">CloudStudio Docs</div>
        <div><a href="/">Back to app</a></div>
      </div>
    </div>
    <main class="page">
      <div class="path">${safePath}</div>
      <article class="doc">${html}</article>
    </main>
  </body>
</html>`;
}

let APP_BRANDING = Object.freeze({
  login: parseBrandingEnv(process.env.CLOUDSTUDIO_BRAND_LOGIN, {
    name: 'CloudStudio',
    initials: 'CS'
  }),
  main: parseBrandingEnv(process.env.CLOUDSTUDIO_BRAND_MAIN, {
    name: 'CloudStudio',
    initials: 'CS'
  })
});

let BILLING_AUTO_SYNC_INTERVAL_MS = 24 * 60 * 60 * 1000;
let BILLING_AUTO_SYNC_ENABLED = true;
let BILLING_AUTO_SYNC_STARTUP_RUN = true;
let BILLING_AUTO_SYNC_ONLY_MISSING = true;
let BILLING_AUTO_SYNC_LOOKBACK_MONTHS = 24;
let BILLING_AUTO_SYNC_MISSING_DELAY_MS = 1200;
let BILLING_STARTUP_MISSING_BACKFILL_ENABLED = true;
let BILLING_STARTUP_MISSING_BACKFILL_MONTHS = 12;
let BILLING_STARTUP_MISSING_BACKFILL_DELAY_MS = 1200;

let CLOUD_METRICS_SYNC_INTERVAL_MS = 5 * 60 * 1000;
let CLOUD_METRICS_SYNC_ENABLED = true;
let CLOUD_METRICS_AZURE_ENABLED = true;
let CLOUD_METRICS_AWS_ENABLED = true;
let CLOUD_METRICS_RACKSPACE_ENABLED = true;
let CLOUD_METRICS_SYNC_STARTUP_RUN = true;
let CLOUD_METRICS_SYNC_STARTUP_DELAY_MS = 15_000;
let CLOUD_METRICS_AZURE_MAX_RETRIES = 4;
let CLOUD_METRICS_AZURE_TIMEOUT_MS = 30_000;
let CLOUD_METRICS_AZURE_CONCURRENCY = 6;
let CLOUD_METRICS_AZURE_MAX_RESOURCES = 5000;
let CLOUD_METRICS_AZURE_LOOKBACK_MINUTES = 15;
let CLOUD_METRICS_AZURE_METRIC_BATCH_SIZE = 20;
let CLOUD_METRICS_AZURE_METRIC_BATCH_DELAY_MS = 0;
let CLOUD_METRICS_AWS_MAX_RETRIES = 4;
let CLOUD_METRICS_AWS_CONCURRENCY = 6;
let CLOUD_METRICS_AWS_LOOKBACK_MINUTES = 15;
let CLOUD_METRICS_AWS_MAX_METRICS_PER_ACCOUNT = 5000;
let CLOUD_METRICS_AWS_NAMESPACES = [];
let CLOUD_METRICS_AWS_REGIONS = [];
let CLOUD_METRICS_RACKSPACE_MAX_RETRIES = 3;
let CLOUD_METRICS_RACKSPACE_TIMEOUT_MS = 30_000;
let CLOUD_METRICS_RACKSPACE_CONCURRENCY = 4;
let CLOUD_METRICS_RACKSPACE_PAGE_SIZE = 100;
let CLOUD_METRICS_RACKSPACE_MAX_ENTITIES = 5000;
let CLOUD_METRICS_RACKSPACE_MAX_METRICS_PER_CHECK = 50;
let CLOUD_METRICS_RACKSPACE_LOOKBACK_MINUTES = 60;
let CLOUD_METRICS_RACKSPACE_PLOT_POINTS = 30;

function refreshRuntimeTunablesFromEnv() {
  APP_BRANDING = Object.freeze({
    login: parseBrandingEnv(process.env.CLOUDSTUDIO_BRAND_LOGIN, {
      name: 'CloudStudio',
      initials: 'CS'
    }),
    main: parseBrandingEnv(process.env.CLOUDSTUDIO_BRAND_MAIN, {
      name: 'CloudStudio',
      initials: 'CS'
    })
  });

  BILLING_AUTO_SYNC_INTERVAL_MS = Math.max(
    60 * 60 * 1000,
    Number(process.env.BILLING_AUTO_SYNC_INTERVAL_MS || 24 * 60 * 60 * 1000)
  );
  BILLING_AUTO_SYNC_ENABLED = !['0', 'false', 'no', 'off'].includes(
    String(process.env.BILLING_AUTO_SYNC_ENABLED || 'true').trim().toLowerCase()
  );
  BILLING_AUTO_SYNC_STARTUP_RUN = !['0', 'false', 'no', 'off'].includes(
    String(process.env.BILLING_AUTO_SYNC_RUN_ON_STARTUP || 'true').trim().toLowerCase()
  );
  BILLING_AUTO_SYNC_ONLY_MISSING = !['0', 'false', 'no', 'off'].includes(
    String(process.env.BILLING_AUTO_SYNC_ONLY_MISSING || 'true').trim().toLowerCase()
  );
  BILLING_AUTO_SYNC_LOOKBACK_MONTHS = Math.max(
    1,
    Math.min(120, Number(process.env.BILLING_AUTO_SYNC_LOOKBACK_MONTHS || 24))
  );
  BILLING_AUTO_SYNC_MISSING_DELAY_MS = Math.max(
    0,
    Math.min(12_000, Number(process.env.BILLING_AUTO_SYNC_MISSING_DELAY_MS || 1200))
  );
  BILLING_STARTUP_MISSING_BACKFILL_ENABLED = !['0', 'false', 'no', 'off'].includes(
    String(process.env.BILLING_STARTUP_BACKFILL_MISSING_ENABLED || 'true').trim().toLowerCase()
  );
  BILLING_STARTUP_MISSING_BACKFILL_MONTHS = Math.max(
    1,
    Math.min(120, Number(process.env.BILLING_STARTUP_BACKFILL_MISSING_MONTHS || 12))
  );
  BILLING_STARTUP_MISSING_BACKFILL_DELAY_MS = Math.max(
    0,
    Math.min(12_000, Number(process.env.BILLING_STARTUP_BACKFILL_DELAY_MS || 1200))
  );

  CLOUD_METRICS_SYNC_INTERVAL_MS = Math.max(
    60_000,
    Number(process.env.CLOUD_METRICS_SYNC_INTERVAL_MS || 5 * 60 * 1000)
  );
  CLOUD_METRICS_SYNC_ENABLED = !['0', 'false', 'no', 'off'].includes(
    String(process.env.CLOUD_METRICS_SYNC_ENABLED || 'true').trim().toLowerCase()
  );
  CLOUD_METRICS_AZURE_ENABLED = parseBooleanEnv(process.env.CLOUD_METRICS_AZURE, true);
  CLOUD_METRICS_AWS_ENABLED = parseBooleanEnv(process.env.CLOUD_METRICS_AWS, true);
  CLOUD_METRICS_RACKSPACE_ENABLED = parseBooleanEnv(process.env.CLOUD_METRICS_RACKSPACE, true);
  CLOUD_METRICS_SYNC_STARTUP_RUN = !['0', 'false', 'no', 'off'].includes(
    String(process.env.CLOUD_METRICS_SYNC_RUN_ON_STARTUP || 'true').trim().toLowerCase()
  );
  CLOUD_METRICS_SYNC_STARTUP_DELAY_MS = Math.max(
    0,
    Math.min(10 * 60 * 1000, Number(process.env.CLOUD_METRICS_SYNC_STARTUP_DELAY_MS || 15_000))
  );
  CLOUD_METRICS_AZURE_MAX_RETRIES = Math.max(
    0,
    Math.min(10, Number(process.env.CLOUD_METRICS_AZURE_MAX_RETRIES || 4))
  );
  CLOUD_METRICS_AZURE_TIMEOUT_MS = Math.max(
    5_000,
    Math.min(120_000, Number(process.env.CLOUD_METRICS_AZURE_TIMEOUT_MS || 30_000))
  );
  CLOUD_METRICS_AZURE_CONCURRENCY = Math.max(
    1,
    Math.min(24, Number(process.env.CLOUD_METRICS_AZURE_CONCURRENCY || 6))
  );
  CLOUD_METRICS_AZURE_MAX_RESOURCES = Math.max(
    1,
    Math.min(10_000, Number(process.env.CLOUD_METRICS_AZURE_MAX_RESOURCES || 5000))
  );
  CLOUD_METRICS_AZURE_LOOKBACK_MINUTES = Math.max(
    5,
    Math.min(180, Number(process.env.CLOUD_METRICS_AZURE_LOOKBACK_MINUTES || 15))
  );
  CLOUD_METRICS_AZURE_METRIC_BATCH_SIZE = Math.max(
    1,
    Math.min(40, Number(process.env.CLOUD_METRICS_AZURE_METRIC_BATCH_SIZE || 20))
  );
  CLOUD_METRICS_AZURE_METRIC_BATCH_DELAY_MS = Math.max(
    0,
    Math.min(5_000, Number(process.env.CLOUD_METRICS_AZURE_METRIC_BATCH_DELAY_MS || 0))
  );
  CLOUD_METRICS_AWS_MAX_RETRIES = Math.max(
    0,
    Math.min(10, Number(process.env.CLOUD_METRICS_AWS_MAX_RETRIES || 4))
  );
  CLOUD_METRICS_AWS_CONCURRENCY = Math.max(
    1,
    Math.min(24, Number(process.env.CLOUD_METRICS_AWS_CONCURRENCY || 6))
  );
  CLOUD_METRICS_AWS_LOOKBACK_MINUTES = Math.max(
    5,
    Math.min(180, Number(process.env.CLOUD_METRICS_AWS_LOOKBACK_MINUTES || 15))
  );
  CLOUD_METRICS_AWS_MAX_METRICS_PER_ACCOUNT = Math.max(
    100,
    Math.min(50_000, Number(process.env.CLOUD_METRICS_AWS_MAX_METRICS_PER_ACCOUNT || 5000))
  );
  CLOUD_METRICS_AWS_NAMESPACES = parseEnvStringList(process.env.CLOUD_METRICS_AWS_NAMESPACES || '');
  CLOUD_METRICS_AWS_REGIONS = parseEnvStringList(process.env.CLOUD_METRICS_AWS_REGIONS || '', { lowercase: true });
  CLOUD_METRICS_RACKSPACE_MAX_RETRIES = Math.max(
    0,
    Math.min(10, Number(process.env.CLOUD_METRICS_RACKSPACE_MAX_RETRIES || 3))
  );
  CLOUD_METRICS_RACKSPACE_TIMEOUT_MS = Math.max(
    5_000,
    Math.min(120_000, Number(process.env.CLOUD_METRICS_RACKSPACE_TIMEOUT_MS || 30_000))
  );
  CLOUD_METRICS_RACKSPACE_CONCURRENCY = Math.max(
    1,
    Math.min(16, Number(process.env.CLOUD_METRICS_RACKSPACE_CONCURRENCY || 4))
  );
  CLOUD_METRICS_RACKSPACE_PAGE_SIZE = Math.max(
    10,
    Math.min(1000, Number(process.env.CLOUD_METRICS_RACKSPACE_PAGE_SIZE || 100))
  );
  CLOUD_METRICS_RACKSPACE_MAX_ENTITIES = Math.max(
    1,
    Math.min(50_000, Number(process.env.CLOUD_METRICS_RACKSPACE_MAX_ENTITIES || 5000))
  );
  CLOUD_METRICS_RACKSPACE_MAX_METRICS_PER_CHECK = Math.max(
    1,
    Math.min(200, Number(process.env.CLOUD_METRICS_RACKSPACE_MAX_METRICS_PER_CHECK || 50))
  );
  CLOUD_METRICS_RACKSPACE_LOOKBACK_MINUTES = Math.max(
    5,
    Math.min(1440, Number(process.env.CLOUD_METRICS_RACKSPACE_LOOKBACK_MINUTES || 60))
  );
  CLOUD_METRICS_RACKSPACE_PLOT_POINTS = Math.max(
    5,
    Math.min(240, Number(process.env.CLOUD_METRICS_RACKSPACE_PLOT_POINTS || 30))
  );
}

refreshRuntimeTunablesFromEnv();
let billingAutoSyncTimer = null;
let cloudMetricsSyncTimer = null;
const billingAutoSyncState = {
  enabled: BILLING_AUTO_SYNC_ENABLED,
  intervalMs: BILLING_AUTO_SYNC_INTERVAL_MS,
  running: false,
  nextRunAt: null,
  lastStartedAt: null,
  lastFinishedAt: null,
  lastStatus: null,
  lastError: null,
  lastPeriodStart: null,
  lastPeriodEnd: null,
  lastSummary: null,
  lastRunId: null
};
const cloudMetricsSyncState = {
  enabled: CLOUD_METRICS_SYNC_ENABLED,
  intervalMs: CLOUD_METRICS_SYNC_INTERVAL_MS,
  running: false,
  nextRunAt: null,
  lastStartedAt: null,
  lastFinishedAt: null,
  lastStatus: null,
  lastError: null,
  lastProvider: null,
  lastSummary: null
};

function syncRuntimeStateFlags() {
  billingAutoSyncState.enabled = BILLING_AUTO_SYNC_ENABLED;
  billingAutoSyncState.intervalMs = BILLING_AUTO_SYNC_INTERVAL_MS;
  cloudMetricsSyncState.enabled = CLOUD_METRICS_SYNC_ENABLED;
  cloudMetricsSyncState.intervalMs = CLOUD_METRICS_SYNC_INTERVAL_MS;
}

syncRuntimeStateFlags();

const APP_VIEW_SET = new Set(VIEW_KEYS);
const VIEW_RULES = Object.freeze([
  { prefix: '/apps/storage', view: 'storage' },
  { prefix: '/apps/pricing', view: 'pricing' },
  { prefix: '/apps/ip-address', view: 'ip-address' },
  { prefix: '/apps/billing', view: 'billing' },
  { prefix: '/apps/tag', view: 'tags' },
  { prefix: '/apps/grafana-cloud', view: 'grafana-cloud' },
  { prefix: '/apps/firewall', view: 'firewall' },
  { prefix: '/api/platform/overview', view: 'dashboard' },
  { prefix: '/api/platform/cloud-metrics', view: 'cloud-metrics' },
  { prefix: '/api/platform/cloud-database', view: 'cloud-database' },
  { prefix: '/api/platform/grafana-cloud', view: 'grafana-cloud' },
  { prefix: '/api/platform/firewall', view: 'firewall' },
  { prefix: '/api/platform/vpn', view: 'vpn' },
  { prefix: '/api/platform/utilization', view: 'cloud-metrics' },
  { prefix: '/api/platform/live', view: 'live' },
  { prefix: '/api/platform/security', view: 'security' },
  { prefix: '/api/platform/pricing', view: 'pricing' },
  { prefix: '/api/ip-address', view: 'ip-address' },
  { prefix: '/api/ip-map', view: 'ip-address' },
  { prefix: '/api/vendors', view: 'vendors' },
  { prefix: '/api/billing', view: 'billing' },
  { prefix: '/api/tags', view: 'tags' },
  { prefix: '/api/keys', view: 'admin-api-keys' },
  { prefix: '/api/admin/app-config', view: 'admin-settings' },
  { prefix: '/api/admin/db-backup', view: 'admin-backup' },
  { prefix: '/api/admin/users', view: 'admin-users' }
]);
const VIEW_LABELS = Object.freeze({
  dashboard: 'Dashboard',
  storage: 'Storage',
  'storage-unified': 'Storage: Unified',
  'storage-azure': 'Storage: Azure',
  'storage-aws': 'Storage: AWS',
  'storage-gcp': 'Storage: GCP',
  'storage-wasabi': 'Storage: Wasabi',
  'storage-vsax': 'Storage: VSAx',
  'storage-other': 'Storage: Other',
  'ip-address': 'IP Address',
  pricing: 'Pricing',
  billing: 'Billing',
  tags: 'Tags',
  'cloud-metrics': 'Cloud Metrics',
  'cloud-database': 'Cloud Database',
  'grafana-cloud': 'Grafana-Cloud',
  live: 'Live View (VSAx)',
  firewall: 'Firewall',
  vpn: 'VPN',
  security: 'Security',
  vendors: 'Vendor onboarding',
  'admin-settings': 'App config',
  'admin-users': 'User access',
  'admin-api-keys': 'API keys',
  'admin-backup': 'Backup',
  apidocs: 'API Docs'
});

function ensureUnifiedEnvDefaults() {
  const sharedDataDir = path.resolve(__dirname, '..', 'data');
  const defaultAuthDataDir = path.join(sharedDataDir, 'auth');
  const defaultAuthStateDir = path.join(defaultAuthDataDir, 'user-state');

  fs.mkdirSync(sharedDataDir, { recursive: true });

  const explicitSqlitePath = String(process.env.SQLITE_PATH || '').trim();
  if (!explicitSqlitePath) {
    process.env.SQLITE_PATH = dbFile;
  }

  const explicitAuthDb = String(process.env.AUTH_DB_FILE || '').trim();
  if (!explicitAuthDb) {
    process.env.AUTH_DB_FILE = dbFile;
  }

  const explicitAuthDataDir = String(process.env.AUTH_DATA_DIR || '').trim();
  if (!explicitAuthDataDir) {
    process.env.AUTH_DATA_DIR = defaultAuthDataDir;
  }

  const explicitAuthStateDir = String(process.env.AUTH_STATE_DIR || '').trim();
  if (!explicitAuthStateDir) {
    process.env.AUTH_STATE_DIR = defaultAuthStateDir;
  }

  if (process.env.CLOUDSTUDIO_SINGLE_ENV === undefined) {
    process.env.CLOUDSTUDIO_SINGLE_ENV = 'true';
  }
}

ensureUnifiedEnvDefaults();
const APP_RUNTIME_CONFIG_SETTING_KEY = 'app_runtime_config_v1';
const DB_BACKUP_UI_SETTING_KEY = 'db_backup_ui_config';
const APP_CONFIG_SCHEMA_VERSION = 'cloudstudio.app-config.v1';
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
const RUNTIME_ENV_SEED_PREFIXES = Object.freeze([
  'AWS_',
  'AZURE_',
  'GCP_',
  'GOOGLE_',
  'RACKSPACE_',
  'CHECKMK_',
  'CLOUDFLARE_',
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
]);
const RUNTIME_ENV_KEY_PATTERN = /^[A-Z][A-Z0-9_]{1,127}$/;
const STRICT_JSON_ENV_OVERRIDE_RULES = Object.freeze({
  AWS_ACCOUNTS_JSON: 'array',
  WASABI_ACCOUNTS_JSON: 'array'
});
const ENV_UNSET_SENTINEL = Symbol('env-unset');
const runtimeEnvOriginalValues = new Map();
let runtimeOverrideKeys = new Set();
let appRuntimeConfig = null;

function normalizeRuntimeEnvKey(keyRaw) {
  const key = String(keyRaw || '')
    .trim()
    .toUpperCase();
  if (!key || !RUNTIME_ENV_KEY_PATTERN.test(key) || RUNTIME_ENV_OVERRIDE_BLOCKLIST.has(key)) {
    return '';
  }
  return key;
}

function normalizeRuntimeEnvOverrides(input = {}) {
  const source = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const normalized = {};
  for (const [rawKey, rawValue] of Object.entries(source)) {
    const key = normalizeRuntimeEnvKey(rawKey);
    if (!key) {
      continue;
    }
    const value = String(rawValue === null || rawValue === undefined ? '' : rawValue).trim();
    if (!value) {
      continue;
    }
    normalized[key] = value;
  }
  return Object.fromEntries(Object.entries(normalized).sort(([left], [right]) => left.localeCompare(right)));
}

function isValidRuntimeJsonOverrideValue(key, value) {
  const mode = STRICT_JSON_ENV_OVERRIDE_RULES[String(key || '').trim().toUpperCase()];
  if (!mode) {
    return true;
  }

  const normalized = normalizeJsonEnvValue(value);
  if (!normalized) {
    return false;
  }

  try {
    const parsed = JSON.parse(normalized);
    if (mode === 'array') {
      return Array.isArray(parsed);
    }
    return parsed !== null && typeof parsed === 'object';
  } catch (_error) {
    return false;
  }
}

function normalizeRuntimeJsonOverrideValue(key, value) {
  const mode = STRICT_JSON_ENV_OVERRIDE_RULES[String(key || '').trim().toUpperCase()];
  if (!mode) {
    return String(value === null || value === undefined ? '' : value).trim();
  }

  const normalized = normalizeJsonEnvValue(value);
  if (!normalized) {
    return '';
  }

  let parsed;
  try {
    parsed = JSON.parse(normalized);
  } catch (_error) {
    return '';
  }

  if (mode === 'array' && !Array.isArray(parsed)) {
    return '';
  }
  if (mode !== 'array' && (parsed === null || typeof parsed !== 'object')) {
    return '';
  }

  return JSON.stringify(parsed);
}

function sanitizeRuntimeEnvOverrides(nextOverrides = {}, previousOverrides = {}) {
  const sanitized = { ...(nextOverrides && typeof nextOverrides === 'object' ? nextOverrides : {}) };
  const prior = previousOverrides && typeof previousOverrides === 'object' ? previousOverrides : {};

  function resolveStrictJsonFallbackValue(key) {
    const candidates = [];
    const priorValue = String(prior[key] || '').trim();
    if (priorValue) {
      candidates.push(priorValue);
    }

    const currentEnvValue = String(process.env[key] || '').trim();
    if (currentEnvValue) {
      candidates.push(currentEnvValue);
    }

    if (runtimeEnvOriginalValues.has(key)) {
      const originalValue = runtimeEnvOriginalValues.get(key);
      if (originalValue !== undefined && originalValue !== null && originalValue !== ENV_UNSET_SENTINEL) {
        const normalizedOriginalValue = String(originalValue).trim();
        if (normalizedOriginalValue) {
          candidates.push(normalizedOriginalValue);
        }
      }
    }

    for (const candidate of candidates) {
      if (!isValidRuntimeJsonOverrideValue(key, candidate)) {
        continue;
      }
      const normalized = normalizeRuntimeJsonOverrideValue(key, candidate);
      if (normalized) {
        return normalized;
      }
    }
    return '';
  }

  for (const key of Object.keys(STRICT_JSON_ENV_OVERRIDE_RULES)) {
    if (!Object.prototype.hasOwnProperty.call(sanitized, key)) {
      const fallback = resolveStrictJsonFallbackValue(key);
      if (fallback) {
        sanitized[key] = fallback;
      }
      continue;
    }

    if (isValidRuntimeJsonOverrideValue(key, sanitized[key])) {
      const normalizedCurrent = normalizeRuntimeJsonOverrideValue(key, sanitized[key]);
      if (normalizedCurrent) {
        sanitized[key] = normalizedCurrent;
      }
      continue;
    }

    const fallback = resolveStrictJsonFallbackValue(key);
    if (fallback) {
      sanitized[key] = fallback;
      console.warn(`[app-config] Ignoring invalid runtime override for ${key}; preserved last valid value.`);
      continue;
    }

    delete sanitized[key];
    console.warn(`[app-config] Ignoring invalid runtime override for ${key}; no valid fallback available.`);
  }

  return Object.fromEntries(Object.entries(sanitized).sort(([left], [right]) => left.localeCompare(right)));
}

function shouldSeedRuntimeEnvOverrideKey(keyRaw) {
  const key = String(keyRaw || '').trim().toUpperCase();
  if (!key || RUNTIME_ENV_OVERRIDE_BLOCKLIST.has(key)) {
    return false;
  }
  return RUNTIME_ENV_SEED_PREFIXES.some((prefix) => key.startsWith(prefix));
}

function buildSeedRuntimeEnvOverridesFromProcessEnv() {
  const overrides = {};
  for (const [key, rawValue] of Object.entries(process.env || {})) {
    if (!shouldSeedRuntimeEnvOverrideKey(key)) {
      continue;
    }
    const value = String(rawValue || '').trim();
    if (!value) {
      continue;
    }
    overrides[key] = value;
  }
  return normalizeRuntimeEnvOverrides(overrides);
}

function normalizeBrandingConfig(input, fallback = {}) {
  const source = input && typeof input === 'object' ? input : {};
  const fallbackName = String(fallback.name || 'CloudStudio').trim() || 'CloudStudio';
  const fallbackInitials = String(fallback.initials || '').trim() || deriveBrandInitials(fallbackName, 'CS');
  const name = String(source.name || '').trim() || fallbackName;
  const initials = String(source.initials || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')
    .slice(0, 8);
  return {
    name,
    initials: initials || deriveBrandInitials(name, fallbackInitials)
  };
}

function buildDefaultRuntimeConfigFromEnv() {
  return {
    branding: {
      login: parseBrandingEnv(process.env.CLOUDSTUDIO_BRAND_LOGIN, { name: 'CloudStudio', initials: 'CS' }),
      main: parseBrandingEnv(process.env.CLOUDSTUDIO_BRAND_MAIN, { name: 'CloudStudio', initials: 'CS' })
    },
    envOverrides: {}
  };
}

function normalizeRuntimeConfig(input = {}, existing = {}) {
  const source = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const prior = existing && typeof existing === 'object' && !Array.isArray(existing) ? existing : {};
  const defaults = buildDefaultRuntimeConfigFromEnv();
  const priorBranding = prior.branding && typeof prior.branding === 'object' ? prior.branding : {};
  const sourceBranding = source.branding && typeof source.branding === 'object' ? source.branding : {};
  const sourceEnvRaw =
    source.envOverrides && typeof source.envOverrides === 'object' && !Array.isArray(source.envOverrides)
      ? source.envOverrides
      : source.env && typeof source.env === 'object' && !Array.isArray(source.env)
        ? source.env
        : null;
  let envOverrides =
    sourceEnvRaw !== null
      ? normalizeRuntimeEnvOverrides(sourceEnvRaw)
      : normalizeRuntimeEnvOverrides(prior.envOverrides || defaults.envOverrides);

  envOverrides = sanitizeRuntimeEnvOverrides(envOverrides, prior.envOverrides || defaults.envOverrides || {});

  return {
    branding: {
      login: normalizeBrandingConfig(sourceBranding.login, priorBranding.login || defaults.branding.login),
      main: normalizeBrandingConfig(sourceBranding.main, priorBranding.main || defaults.branding.main)
    },
    envOverrides
  };
}

function applyRuntimeEnvOverrides(overrides = {}) {
  const next = normalizeRuntimeEnvOverrides(overrides);
  const nextKeys = new Set(Object.keys(next));

  for (const key of runtimeOverrideKeys) {
    if (nextKeys.has(key)) {
      continue;
    }
    const original = runtimeEnvOriginalValues.get(key);
    if (original === undefined || original === ENV_UNSET_SENTINEL) {
      delete process.env[key];
    } else {
      process.env[key] = original;
    }
  }

  for (const [key, value] of Object.entries(next)) {
    if (!runtimeEnvOriginalValues.has(key)) {
      if (Object.prototype.hasOwnProperty.call(process.env, key)) {
        runtimeEnvOriginalValues.set(key, process.env[key]);
      } else {
        runtimeEnvOriginalValues.set(key, ENV_UNSET_SENTINEL);
      }
    }
    process.env[key] = value;
  }

  runtimeOverrideKeys = nextKeys;
  return next;
}

function loadStoredRuntimeConfig() {
  const row = getAppSetting(APP_RUNTIME_CONFIG_SETTING_KEY);
  if (!row?.value || typeof row.value !== 'object') {
    return null;
  }
  const encrypted = String(row.value?.ciphertext || '').trim();
  let payload = row.value;
  if (encrypted) {
    payload = decryptJson(encrypted);
  }
  if (!payload || typeof payload !== 'object') {
    return null;
  }
  return normalizeRuntimeConfig(payload, {});
}

function saveStoredRuntimeConfig(input = {}, existing = {}) {
  const normalized = normalizeRuntimeConfig(input, existing);
  const ciphertext = encryptJson(normalized);
  if (!ciphertext) {
    throw new Error('Could not encrypt app runtime config.');
  }
  upsertAppSetting(APP_RUNTIME_CONFIG_SETTING_KEY, { ciphertext });
  return normalized;
}

function toPublicRuntimeConfig(input = {}) {
  return normalizeRuntimeConfig(input, {});
}

function applyRuntimeConfig(input = {}, options = {}) {
  const existing = appRuntimeConfig || buildDefaultRuntimeConfigFromEnv();
  const normalized = normalizeRuntimeConfig(input, existing);
  const appliedEnvOverrides = applyRuntimeEnvOverrides(normalized.envOverrides);
  process.env.CLOUDSTUDIO_BRAND_LOGIN = JSON.stringify(normalized.branding.login);
  process.env.CLOUDSTUDIO_BRAND_MAIN = JSON.stringify(normalized.branding.main);
  refreshRuntimeTunablesFromEnv();
  syncRuntimeStateFlags();
  appRuntimeConfig = {
    ...normalized,
    envOverrides: appliedEnvOverrides
  };

  if (options.reloadSchedulers) {
    stopBillingAutoSync();
    startBillingAutoSync();
    stopCloudMetricsSync();
    startCloudMetricsSync();
    if (dbBackupScheduler && typeof dbBackupScheduler.reloadConfig === 'function') {
      dbBackupScheduler.reloadConfig(options.reason || 'runtime-config-update');
    }
  }

  return appRuntimeConfig;
}

const storedRuntimeConfig = loadStoredRuntimeConfig();
let bootstrapRuntimeConfig = storedRuntimeConfig || null;
if (!bootstrapRuntimeConfig) {
  bootstrapRuntimeConfig = normalizeRuntimeConfig(
    {
      branding: buildDefaultRuntimeConfigFromEnv().branding,
      envOverrides: buildSeedRuntimeEnvOverridesFromProcessEnv()
    },
    {}
  );
  saveStoredRuntimeConfig(bootstrapRuntimeConfig, {});
}
appRuntimeConfig = applyRuntimeConfig(bootstrapRuntimeConfig, {
  reloadSchedulers: false
});
initializeAuth();

function parseBooleanConfig(value, fallback = false) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  const text = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(text)) {
    return true;
  }
  if (['0', 'false', 'no', 'off'].includes(text)) {
    return false;
  }
  return fallback;
}

function parseIntegerConfig(value, fallback, min = null, max = null) {
  const parsed = Number.parseInt(String(value ?? '').trim(), 10);
  let output = Number.isFinite(parsed) ? parsed : fallback;
  if (Number.isFinite(min)) {
    output = Math.max(min, output);
  }
  if (Number.isFinite(max)) {
    output = Math.min(max, output);
  }
  return output;
}

function normalizeBackupPrefix(value) {
  return String(value || '')
    .trim()
    .replace(/^\/+/, '')
    .replace(/\/+$/, '');
}

function hasOwn(obj, key) {
  return Object.prototype.hasOwnProperty.call(obj || {}, key);
}

function normalizeDbBackupUiConfig(input = {}, existing = {}) {
  const source = input && typeof input === 'object' ? input : {};
  const prior = existing && typeof existing === 'object' ? existing : {};

  const readString = (key, fallback = '') => {
    if (hasOwn(source, key)) {
      return String(source[key] || '').trim();
    }
    if (hasOwn(prior, key)) {
      return String(prior[key] || '').trim();
    }
    return String(fallback || '').trim();
  };

  const readBoolean = (key, fallback = false) => {
    if (hasOwn(source, key)) {
      return parseBooleanConfig(source[key], fallback);
    }
    if (hasOwn(prior, key)) {
      return parseBooleanConfig(prior[key], fallback);
    }
    return fallback;
  };

  const readInteger = (key, fallback, min = null, max = null) => {
    if (hasOwn(source, key)) {
      return parseIntegerConfig(source[key], fallback, min, max);
    }
    if (hasOwn(prior, key)) {
      return parseIntegerConfig(prior[key], fallback, min, max);
    }
    return parseIntegerConfig(fallback, fallback, min, max);
  };

  let accessKeyId = readString('accessKeyId', '');
  if (readBoolean('clearAccessKeyId', false)) {
    accessKeyId = '';
  }

  let secretAccessKey = String(prior.secretAccessKey || '').trim();
  if (hasOwn(source, 'secretAccessKey')) {
    const nextSecret = String(source.secretAccessKey || '').trim();
    if (nextSecret) {
      secretAccessKey = nextSecret;
    } else if (readBoolean('clearSecretAccessKey', false)) {
      secretAccessKey = '';
    }
  }

  let sessionToken = String(prior.sessionToken || '').trim();
  if (hasOwn(source, 'sessionToken')) {
    const nextToken = String(source.sessionToken || '').trim();
    if (nextToken) {
      sessionToken = nextToken;
    } else if (readBoolean('clearSessionToken', false)) {
      sessionToken = '';
    }
  }

  let lifecycleRuleId = readString('lifecycleRuleId', 'cloudstudio-db-backup-retention').slice(0, 255);
  if (!lifecycleRuleId) {
    lifecycleRuleId = 'cloudstudio-db-backup-retention';
  }

  return {
    enabled: readBoolean('enabled', false),
    bucket: readString('bucket', ''),
    prefix: normalizeBackupPrefix(readString('prefix', 'cloudstudio-db-backups')),
    region: readString('region', 'us-east-1') || 'us-east-1',
    endpoint: readString('endpoint', 'https://s3.wasabisys.com') || 'https://s3.wasabisys.com',
    accessKeyId,
    secretAccessKey,
    sessionToken,
    forcePathStyle: readBoolean('forcePathStyle', false),
    intervalMs: readInteger('intervalMs', 60 * 60 * 1000, 60 * 1000, 7 * 24 * 60 * 60 * 1000),
    runOnStartup: readBoolean('runOnStartup', true),
    retentionDays: readInteger('retentionDays', 15, 1, 3650),
    applyLifecycle: readBoolean('applyLifecycle', true),
    lifecycleRuleId,
    tempDir: readString('tempDir', './data/tmp-backups') || './data/tmp-backups'
  };
}

function loadStoredDbBackupUiConfig() {
  const row = getAppSetting(DB_BACKUP_UI_SETTING_KEY);
  if (!row?.value || typeof row.value !== 'object') {
    return null;
  }

  const encrypted = String(row.value?.ciphertext || '').trim();
  let payload = row.value;
  if (encrypted) {
    payload = decryptJson(encrypted);
  }
  if (!payload || typeof payload !== 'object') {
    return null;
  }
  return normalizeDbBackupUiConfig(payload, {});
}

function saveStoredDbBackupUiConfig(input = {}) {
  const normalized = normalizeDbBackupUiConfig(input, {});
  const ciphertext = encryptJson(normalized);
  if (!ciphertext) {
    throw new Error('Could not encrypt DB backup settings.');
  }
  upsertAppSetting(DB_BACKUP_UI_SETTING_KEY, { ciphertext });
  return normalized;
}

function toPublicDbBackupUiConfig(input = {}) {
  const normalized = normalizeDbBackupUiConfig(input, {});
  return {
    ...normalized,
    accessKeyId: normalized.accessKeyId || '',
    secretAccessKey: '',
    sessionToken: '',
    hasAccessKeyId: Boolean(normalized.accessKeyId),
    hasSecretAccessKey: Boolean(normalized.secretAccessKey),
    hasSessionToken: Boolean(normalized.sessionToken)
  };
}

let dbBackupUiConfig = loadStoredDbBackupUiConfig() || normalizeDbBackupUiConfig({}, {});

const managedServices = createManagedServices({ rootDir: path.resolve(__dirname, '..') });
const dbBackupScheduler = createDbBackupScheduler({
  logger: console,
  getUiConfig: () => dbBackupUiConfig
});

app.use(express.json({ limit: '5mb' }));
app.use(express.urlencoded({ extended: false }));

app.use((req, _res, next) => {
  req.requestStartedAt = Date.now();
  next();
});

app.use((req, res, next) => {
  res.on('finish', () => {
    const durationMs = Date.now() - req.requestStartedAt;
    const message = `[cloudstudio] ${req.method} ${req.originalUrl} ${res.statusCode} ${durationMs}ms`;
    if (res.statusCode >= 500) {
      console.error(message);
    } else {
      console.log(message);
    }
  });
  next();
});

function normalizeProvider(value) {
  const v = String(value || '').trim().toLowerCase();
  if (v === 'wasabi-wacm') {
    return 'wasabi';
  }
  if (
    [
      'azure',
      'aws',
      'gcp',
      'grafana-cloud',
      'sendgrid',
      'rackspace',
      'private',
      'wasabi',
      'wasabi-main',
      'vsax',
      'other'
    ].includes(v)
  ) {
    return v;
  }
  return 'other';
}

function normalizeCloudType(value) {
  return String(value || '').trim().toLowerCase() === 'private' ? 'private' : 'public';
}

function parseOptionalBoolean(value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'number') {
    return value !== 0;
  }
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) {
    return false;
  }
  return null;
}

function getVendorHiddenFlag(input = {}) {
  const hiddenDirect = parseOptionalBoolean(input.hidden);
  if (hiddenDirect !== null) {
    return hiddenDirect;
  }
  const metadata = input?.metadata && typeof input.metadata === 'object' && !Array.isArray(input.metadata) ? input.metadata : null;
  if (!metadata) {
    return null;
  }
  return parseOptionalBoolean(metadata.hidden);
}

function isVendorHidden(vendor = {}) {
  return getVendorHiddenFlag(vendor) === true;
}

function parseVendorPayload(body = {}) {
  return {
    id: body.id || null,
    name: String(body.name || '').trim(),
    provider: normalizeProvider(body.provider),
    cloudType: normalizeCloudType(body.cloudType),
    authMethod: String(body.authMethod || '').trim() || 'unknown',
    subscriptionId: body.subscriptionId ? String(body.subscriptionId).trim() : null,
    accountId: body.accountId ? String(body.accountId).trim() : null,
    projectId: body.projectId ? String(body.projectId).trim() : null,
    metadata: body.metadata && typeof body.metadata === 'object' ? body.metadata : {},
    hidden: getVendorHiddenFlag(body),
    credentials: body.credentials && typeof body.credentials === 'object' ? body.credentials : null
  };
}

function toPublicVendor(vendor) {
  if (!vendor) {
    return null;
  }

  return {
    id: vendor.id,
    name: vendor.name,
    provider: vendor.provider,
    cloudType: vendor.cloudType,
    authMethod: vendor.authMethod,
    subscriptionId: vendor.subscriptionId,
    accountId: vendor.accountId,
    projectId: vendor.projectId,
    hasCredentials: Boolean(vendor.credentialsEncrypted),
    hidden: isVendorHidden(vendor),
    metadata: vendor.metadata || {},
    createdAt: vendor.createdAt,
    updatedAt: vendor.updatedAt
  };
}

function buildVendorImportFingerprint(input = {}) {
  const provider = normalizeProvider(input.provider);
  const accountId = String(input.accountId || '').trim().toLowerCase();
  const projectId = String(input.projectId || '').trim().toLowerCase();
  const name = String(input.name || '').trim().toLowerCase();
  const subscriptionId = String(input.subscriptionId || '').trim().toLowerCase();
  return [provider, accountId, projectId, subscriptionId, name].join('|');
}

function toVendorExportRecord(vendor) {
  if (!vendor) {
    return null;
  }
  let credentials = null;
  if (vendor.credentialsEncrypted) {
    try {
      credentials = decryptJson(vendor.credentialsEncrypted);
    } catch (_error) {
      credentials = null;
    }
  }
  return {
    id: vendor.id,
    name: vendor.name,
    provider: vendor.provider,
    cloudType: vendor.cloudType,
    authMethod: vendor.authMethod,
    subscriptionId: vendor.subscriptionId,
    accountId: vendor.accountId,
    projectId: vendor.projectId,
    metadata: vendor.metadata || {},
    credentials,
    createdAt: vendor.createdAt,
    updatedAt: vendor.updatedAt
  };
}

function normalizeVendorImportEntries(payload) {
  let source = payload;
  if (source && typeof source === 'object' && !Array.isArray(source) && Object.prototype.hasOwnProperty.call(source, 'payload')) {
    source = source.payload;
  }

  if (Array.isArray(source)) {
    return source;
  }
  if (source && typeof source === 'object' && Array.isArray(source.vendors)) {
    return source.vendors;
  }
  if (source && typeof source === 'object' && source.vendor && typeof source.vendor === 'object') {
    return [source.vendor];
  }
  if (source && typeof source === 'object') {
    return [source];
  }
  return [];
}

function buildAppConfigExportPayload() {
  const vendors = listVendors()
    .map((vendor) => toVendorExportRecord(getVendorById(vendor.id)))
    .filter(Boolean);
  const runtimeConfig = normalizeRuntimeConfig(appRuntimeConfig || buildDefaultRuntimeConfigFromEnv(), {});
  const backupConfig = normalizeDbBackupUiConfig(loadStoredDbBackupUiConfig() || dbBackupUiConfig || {}, {});
  return {
    schemaVersion: APP_CONFIG_SCHEMA_VERSION,
    exportedAt: new Date().toISOString(),
    runtimeConfig,
    dbBackupConfig: backupConfig,
    vendors,
    metadata: {
      source: 'CloudStudio',
      version: 'v1'
    }
  };
}

function normalizeAppConfigImportPayload(payload = {}) {
  let source = payload;
  if (
    source &&
    typeof source === 'object' &&
    !Array.isArray(source) &&
    Object.prototype.hasOwnProperty.call(source, 'payload')
  ) {
    source = source.payload;
  }
  if (!source || typeof source !== 'object' || Array.isArray(source)) {
    return {
      runtimeConfig: null,
      dbBackupConfig: null,
      vendors: []
    };
  }

  const runtimeConfig =
    source.runtimeConfig && typeof source.runtimeConfig === 'object' && !Array.isArray(source.runtimeConfig)
      ? source.runtimeConfig
      : null;
  const dbBackupConfig =
    source.dbBackupConfig && typeof source.dbBackupConfig === 'object' && !Array.isArray(source.dbBackupConfig)
      ? source.dbBackupConfig
      : null;
  const vendors = normalizeVendorImportEntries(source.vendors || source.vendorConfig || source.vendorConfigs || []);

  return {
    runtimeConfig,
    dbBackupConfig,
    vendors
  };
}

function requireApiKey(req, res, next) {
  const raw = req.headers['x-api-key'] || req.query.apiKey;
  const apiKey = String(raw || '').trim();
  if (!apiKey) {
    return res.status(401).json({ error: 'API key required via x-api-key header.' });
  }

  const keyHash = hashApiKey(apiKey);
  const record = findApiKeyByHash(keyHash);
  if (!record || !record.isActive) {
    return res.status(401).json({ error: 'Invalid API key.' });
  }

  touchApiKeyUsage(record.id);
  req.apiKeyRecord = record;
  return next();
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 40_000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      ...options,
      headers: {
        'content-type': 'application/json',
        ...(options.headers || {})
      },
      signal: controller.signal
    });

    const text = await response.text();
    let payload = null;
    try {
      payload = text ? JSON.parse(text) : null;
    } catch (_error) {
      payload = { raw: text };
    }

    if (!response.ok) {
      const error = new Error(payload?.error || `Request failed (${response.status})`);
      error.statusCode = response.status;
      error.payload = payload;
      throw error;
    }

    return payload;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchResponseWithTimeout(url, options = {}, timeoutMs = 40_000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...options,
      headers: {
        ...(options.headers || {})
      },
      signal: controller.signal
    });
  } finally {
    clearTimeout(timeout);
  }
}

function getStorageBaseUrl() {
  return managedServices.getServiceBaseUrl('storage');
}

function getPriceBaseUrl() {
  return managedServices.getServiceBaseUrl('price');
}

function getIpAddressBaseUrl() {
  return managedServices.getServiceBaseUrl('ipAddress');
}

function getBillingBaseUrl() {
  return managedServices.getServiceBaseUrl('billing');
}

function getTagBaseUrl() {
  return managedServices.getServiceBaseUrl('tag');
}

function getGrafanaCloudBaseUrl() {
  return managedServices.getServiceBaseUrl('grafanaCloud');
}

function getFirewallBaseUrl() {
  return managedServices.getServiceBaseUrl('firewall');
}

async function fetchStorage(pathname, options, timeoutMs) {
  const basePath = getStorageBaseUrl();
  const requestOptions = options || {};
  return fetchJsonWithTimeout(
    `http://127.0.0.1:${port}${basePath}${pathname}`,
    {
      ...requestOptions,
      headers: {
        [INTERNAL_API_TOKEN_HEADER]: internalApiToken,
        ...(requestOptions.headers || {})
      }
    },
    timeoutMs
  );
}

async function fetchStorageRaw(pathname, options, timeoutMs) {
  const basePath = getStorageBaseUrl();
  const requestOptions = options || {};
  return fetchResponseWithTimeout(
    `http://127.0.0.1:${port}${basePath}${pathname}`,
    {
      ...requestOptions,
      headers: {
        [INTERNAL_API_TOKEN_HEADER]: internalApiToken,
        ...(requestOptions.headers || {})
      }
    },
    timeoutMs
  );
}

async function fetchPrice(pathname, options, timeoutMs) {
  const basePath = getPriceBaseUrl();
  const requestOptions = options || {};
  return fetchJsonWithTimeout(
    `http://127.0.0.1:${port}${basePath}${pathname}`,
    {
      ...requestOptions,
      headers: {
        [INTERNAL_API_TOKEN_HEADER]: internalApiToken,
        ...(requestOptions.headers || {})
      }
    },
    timeoutMs
  );
}

async function fetchIpAddress(pathname, options, timeoutMs) {
  const basePath = getIpAddressBaseUrl();
  const requestOptions = options || {};
  return fetchJsonWithTimeout(
    `http://127.0.0.1:${port}${basePath}${pathname}`,
    {
      ...requestOptions,
      headers: {
        [INTERNAL_API_TOKEN_HEADER]: internalApiToken,
        ...(requestOptions.headers || {})
      }
    },
    timeoutMs
  );
}

async function fetchIpAddressRaw(pathname, options, timeoutMs) {
  const basePath = getIpAddressBaseUrl();
  const requestOptions = options || {};
  return fetchResponseWithTimeout(
    `http://127.0.0.1:${port}${basePath}${pathname}`,
    {
      ...requestOptions,
      headers: {
        [INTERNAL_API_TOKEN_HEADER]: internalApiToken,
        ...(requestOptions.headers || {})
      }
    },
    timeoutMs
  );
}

async function fetchBilling(pathname, options, timeoutMs) {
  const basePath = getBillingBaseUrl();
  const requestOptions = options || {};
  return fetchJsonWithTimeout(
    `http://127.0.0.1:${port}${basePath}${pathname}`,
    {
      ...requestOptions,
      headers: {
        [INTERNAL_API_TOKEN_HEADER]: internalApiToken,
        ...(requestOptions.headers || {})
      }
    },
    timeoutMs
  );
}

async function fetchBillingRaw(pathname, options, timeoutMs) {
  const basePath = getBillingBaseUrl();
  const requestOptions = options || {};
  return fetchResponseWithTimeout(
    `http://127.0.0.1:${port}${basePath}${pathname}`,
    {
      ...requestOptions,
      headers: {
        [INTERNAL_API_TOKEN_HEADER]: internalApiToken,
        ...(requestOptions.headers || {})
      }
    },
    timeoutMs
  );
}

async function fetchTag(pathname, options, timeoutMs) {
  const basePath = getTagBaseUrl();
  const requestOptions = options || {};
  return fetchJsonWithTimeout(
    `http://127.0.0.1:${port}${basePath}${pathname}`,
    {
      ...requestOptions,
      headers: {
        [INTERNAL_API_TOKEN_HEADER]: internalApiToken,
        ...(requestOptions.headers || {})
      }
    },
    timeoutMs
  );
}

async function fetchGrafanaCloud(pathname, options, timeoutMs) {
  const basePath = getGrafanaCloudBaseUrl();
  const requestOptions = options || {};
  return fetchJsonWithTimeout(
    `http://127.0.0.1:${port}${basePath}${pathname}`,
    {
      ...requestOptions,
      headers: {
        [INTERNAL_API_TOKEN_HEADER]: internalApiToken,
        ...(requestOptions.headers || {})
      }
    },
    timeoutMs
  );
}

async function fetchFirewall(pathname, options, timeoutMs) {
  const basePath = getFirewallBaseUrl();
  const requestOptions = options || {};
  return fetchJsonWithTimeout(
    `http://127.0.0.1:${port}${basePath}${pathname}`,
    {
      ...requestOptions,
      headers: {
        [INTERNAL_API_TOKEN_HEADER]: internalApiToken,
        ...(requestOptions.headers || {})
      }
    },
    timeoutMs
  );
}

function sumNumbers(values) {
  return values.reduce((sum, value) => {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? sum + numeric : sum;
  }, 0);
}

function parseStringValues(raw) {
  if (raw === null || raw === undefined || raw === '') {
    return [];
  }

  const values = Array.isArray(raw) ? raw : [raw];
  const normalized = [];
  for (const value of values) {
    const parts = String(value || '')
      .split(',')
      .map((part) => String(part || '').trim())
      .filter(Boolean);
    normalized.push(...parts);
  }
  return Array.from(new Set(normalized));
}

function toIsoNow() {
  return new Date().toISOString();
}

function getLastMonthRange(referenceDate = new Date()) {
  const year = referenceDate.getUTCFullYear();
  const month = referenceDate.getUTCMonth();
  const monthStart = new Date(Date.UTC(year, month, 1));
  const lastMonthStart = new Date(Date.UTC(monthStart.getUTCFullYear(), monthStart.getUTCMonth() - 1, 1));
  const lastMonthEnd = new Date(Date.UTC(monthStart.getUTCFullYear(), monthStart.getUTCMonth(), 0));
  return {
    periodStart: lastMonthStart.toISOString().slice(0, 10),
    periodEnd: lastMonthEnd.toISOString().slice(0, 10)
  };
}

function setBillingAutoSyncNextRun(referenceTimeMs = Date.now()) {
  billingAutoSyncState.nextRunAt = new Date(referenceTimeMs + BILLING_AUTO_SYNC_INTERVAL_MS).toISOString();
}

function getBillingAutoSyncStatus() {
  return {
    ...billingAutoSyncState,
    intervalHours: Math.round((BILLING_AUTO_SYNC_INTERVAL_MS / (60 * 60 * 1000)) * 100) / 100
  };
}

async function runBillingAutoSync(trigger = 'scheduled') {
  if (!BILLING_AUTO_SYNC_ENABLED || billingAutoSyncState.running) {
    return;
  }

  const range = getLastMonthRange();
  billingAutoSyncState.running = true;
  billingAutoSyncState.lastStartedAt = toIsoNow();
  billingAutoSyncState.lastError = null;
  billingAutoSyncState.lastStatus = 'running';
  billingAutoSyncState.lastPeriodStart = range.periodStart;
  billingAutoSyncState.lastPeriodEnd = range.periodEnd;

  try {
    if (BILLING_AUTO_SYNC_ONLY_MISSING) {
      const payload = await fetchBilling(
        '/api/billing/backfill/ensure',
        {
          method: 'POST',
          body: JSON.stringify({
            lookbackMonths: BILLING_AUTO_SYNC_LOOKBACK_MONTHS,
            onlyMissing: true,
            delayMs: BILLING_AUTO_SYNC_MISSING_DELAY_MS
          })
        },
        180_000
      );
      billingAutoSyncState.lastFinishedAt = toIsoNow();
      billingAutoSyncState.lastStatus = 'ok';
      billingAutoSyncState.lastError = null;
      billingAutoSyncState.lastRunId = payload?.status?.jobId || null;
      billingAutoSyncState.lastSummary = {
        mode: 'missing-only',
        started: Boolean(payload?.started),
        reason: payload?.reason || null,
        lookbackMonths: payload?.coverage?.lookbackMonths || BILLING_AUTO_SYNC_LOOKBACK_MONTHS,
        missingVendorMonths: payload?.coverage?.missingVendorMonths ?? null,
        vendorCount: payload?.coverage?.vendorCount ?? null
      };
      console.log('[cloudstudio] Billing auto-sync completed (missing-only mode)', {
        trigger,
        lookbackMonths: BILLING_AUTO_SYNC_LOOKBACK_MONTHS,
        summary: billingAutoSyncState.lastSummary
      });
    } else {
      const payload = await fetchBilling(
        '/api/billing/sync',
        {
          method: 'POST',
          body: JSON.stringify({
            periodStart: range.periodStart,
            periodEnd: range.periodEnd
          })
        },
        180_000
      );
      billingAutoSyncState.lastFinishedAt = toIsoNow();
      billingAutoSyncState.lastStatus = 'ok';
      billingAutoSyncState.lastError = null;
      billingAutoSyncState.lastRunId = payload?.runId || null;
      billingAutoSyncState.lastSummary = payload?.summary || null;
      console.log('[cloudstudio] Billing auto-sync completed', {
        trigger,
        periodStart: range.periodStart,
        periodEnd: range.periodEnd,
        summary: payload?.summary || null
      });
    }
  } catch (error) {
    billingAutoSyncState.lastFinishedAt = toIsoNow();
    billingAutoSyncState.lastStatus = 'error';
    billingAutoSyncState.lastError = error?.message || 'Billing auto-sync failed.';
    billingAutoSyncState.lastRunId = null;
    billingAutoSyncState.lastSummary = null;
    console.error('[cloudstudio] Billing auto-sync failed', {
      trigger,
      periodStart: range.periodStart,
      periodEnd: range.periodEnd,
      message: billingAutoSyncState.lastError
    });
  } finally {
    billingAutoSyncState.running = false;
    setBillingAutoSyncNextRun(Date.now());
  }
}

function startBillingAutoSync() {
  if (!BILLING_AUTO_SYNC_ENABLED) {
    billingAutoSyncState.nextRunAt = null;
    return;
  }
  if (billingAutoSyncTimer) {
    return;
  }

  setBillingAutoSyncNextRun(Date.now());
  billingAutoSyncTimer = setInterval(() => {
    void runBillingAutoSync('scheduled');
  }, BILLING_AUTO_SYNC_INTERVAL_MS);

  if (BILLING_AUTO_SYNC_STARTUP_RUN) {
    setTimeout(async () => {
      let startupBackfillRunning = false;
      if (BILLING_STARTUP_MISSING_BACKFILL_ENABLED) {
        try {
          const ensurePayload = await fetchBilling(
            '/api/billing/backfill/ensure',
            {
              method: 'POST',
              body: JSON.stringify({
                lookbackMonths: BILLING_STARTUP_MISSING_BACKFILL_MONTHS,
                onlyMissing: true,
                delayMs: BILLING_STARTUP_MISSING_BACKFILL_DELAY_MS
              })
            },
            180_000
          );
          startupBackfillRunning = Boolean(ensurePayload?.started || ensurePayload?.status?.running);
          if (startupBackfillRunning) {
            console.log('[cloudstudio] Startup billing backfill started for missing historical months', {
              lookbackMonths: BILLING_STARTUP_MISSING_BACKFILL_MONTHS,
              missingVendorMonths: ensurePayload?.coverage?.missingVendorMonths || null
            });
          } else {
            console.log('[cloudstudio] Startup billing backfill check completed', {
              lookbackMonths: BILLING_STARTUP_MISSING_BACKFILL_MONTHS,
              missingVendorMonths: ensurePayload?.coverage?.missingVendorMonths || 0
            });
          }
        } catch (error) {
          console.error('[cloudstudio] Startup billing backfill check failed', {
            message: error?.message || String(error)
          });
        }
      }

      if (!startupBackfillRunning) {
        void runBillingAutoSync('startup');
      }
    }, 15_000);
  }
}

function stopBillingAutoSync() {
  if (billingAutoSyncTimer) {
    clearInterval(billingAutoSyncTimer);
    billingAutoSyncTimer = null;
  }
  billingAutoSyncState.nextRunAt = null;
}

const CLOUD_METRIC_PROVIDER_ORDER = ['azure', 'aws', 'gcp', 'rackspace', 'wasabi', 'private', 'other'];
const CLOUD_METRIC_PROVIDER_LABELS = Object.freeze({
  azure: 'Azure',
  aws: 'AWS',
  gcp: 'GCP',
  rackspace: 'Rackspace',
  wasabi: 'Wasabi',
  private: 'Private',
  other: 'Other'
});
const CLOUD_METRIC_RESOURCE_TYPE_LABELS = Object.freeze({
  'microsoft.compute/virtualmachines': 'Virtual Machines',
  'microsoft.compute/disks': 'Managed Disks',
  'microsoft.compute/snapshots': 'Snapshots',
  'microsoft.compute/availabilitysets': 'Availability Sets',
  'microsoft.storage/storageaccounts': 'Storage Accounts',
  'microsoft.sql/servers': 'SQL Servers',
  'microsoft.sql/servers/databases': 'SQL Databases',
  'microsoft.sql/managedinstances': 'SQL Managed Instances',
  'microsoft.sql/managedinstances/databases': 'Managed Instance Databases',
  'microsoft.web/sites': 'App Services',
  'microsoft.web/serverfarms': 'App Service Plans',
  'microsoft.web/sites/functions': 'Function Apps',
  'microsoft.containerregistry/registries': 'Container Registries',
  'microsoft.containerservice/managedclusters': 'AKS Clusters',
  'microsoft.network/virtualnetworks': 'Virtual Networks',
  'microsoft.network/loadbalancers': 'Load Balancers',
  'microsoft.network/applicationgateways': 'Application Gateways',
  'microsoft.network/networksecuritygroups': 'Network Security Groups',
  'microsoft.network/azurefirewalls': 'Azure Firewalls',
  'microsoft.network/publicipaddresses': 'Public IP Addresses',
  'microsoft.network/bastionhosts': 'Bastion Hosts',
  'microsoft.insights/components': 'Application Insights',
  'microsoft.insights/workbooks': 'Workbooks',
  'microsoft.operationalinsights/workspaces': 'Log Analytics Workspaces',
  'microsoft.recoveryservices/vaults': 'Recovery Services Vaults',
  'microsoft.servicebus/namespaces': 'Service Bus Namespaces',
  'microsoft.cognitiveservices/accounts': 'Cognitive Services Accounts',
  'microsoft.keyvault/vaults': 'Key Vaults',
  'microsoft.cache/redis': 'Azure Cache for Redis',
  'microsoft.dbformysql/flexibleservers': 'MySQL Flexible Servers',
  'microsoft.dbforpostgresql/flexibleservers': 'PostgreSQL Flexible Servers',
  'microsoft.documentdb/databaseaccounts': 'Cosmos DB Accounts',
  'microsoft.kusto/clusters': 'Data Explorer Clusters',
  'aws/ec2/instanceid': 'EC2 Instances',
  'aws/rds/dbinstanceidentifier': 'RDS Instances',
  'aws/rds/dbclusteridentifier': 'RDS Clusters',
  'aws/s3/bucketname': 'S3 Buckets',
  'aws/lambda/functionname': 'Lambda Functions',
  'aws/applicationelb/loadbalancer': 'Application Load Balancers',
  'aws/networkelb/loadbalancer': 'Network Load Balancers',
  'aws/apigateway/apiname': 'API Gateway APIs',
  'aws/dynamodb/tablename': 'DynamoDB Tables',
  'aws/ecs/clustername': 'ECS Clusters',
  'aws/ecs/servicename': 'ECS Services',
  'aws/es/domainname': 'OpenSearch Domains',
  'aws/route53/hostedzoneid': 'Route53 Hosted Zones',
  'aws/cloudfront/distributionid': 'CloudFront Distributions',
  'rackspace/monitoring/entity': 'Monitoring Entities'
});
const AWS_CLOUD_METRIC_PRIMARY_DIMENSION_PRIORITY = Object.freeze([
  'InstanceId',
  'DBInstanceIdentifier',
  'DBClusterIdentifier',
  'LoadBalancer',
  'TargetGroup',
  'BucketName',
  'FunctionName',
  'TableName',
  'QueueName',
  'TopicName',
  'StreamName',
  'VolumeId',
  'FileSystemId',
  'ClusterName',
  'ServiceName',
  'ApiName',
  'DomainName',
  'DistributionId',
  'HostedZoneId',
  'RepositoryName',
  'TransitGateway',
  'NatGatewayId',
  'VpnId',
  'VpcId',
  'SubnetId',
  'NetworkInterfaceId',
  'AutoScalingGroupName'
]);

function toTitleWords(text) {
  const normalized = String(text || '')
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ');
  if (!normalized) {
    return '';
  }
  return normalized
    .split(/\s+/)
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1).toLowerCase())
    .join(' ');
}

function humanizeCloudMetricResourceType(typeValue) {
  const normalized = String(typeValue || '')
    .trim()
    .toLowerCase();
  if (!normalized) {
    return 'Unknown';
  }
  const parts = normalized.split('/').filter(Boolean);
  if (!parts.length) {
    return normalized;
  }
  const provider = parts.shift() || '';
  const providerLabel = toTitleWords(provider.replace(/^microsoft\./, '').replace(/\./g, ' '));
  const tail = parts.map((part) => toTitleWords(part)).filter(Boolean);
  return [providerLabel, ...tail].filter(Boolean).join(' / ');
}

const azureCloudMetricsTokenCache = {
  accessToken: null,
  expiresAtMs: 0
};

function waitMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseRetryAfterMs(rawValue) {
  const text = String(rawValue || '').trim();
  if (!text) {
    return 0;
  }
  const seconds = Number.parseInt(text, 10);
  if (Number.isFinite(seconds)) {
    return Math.max(0, seconds * 1000);
  }
  const dateMs = Date.parse(text);
  if (Number.isFinite(dateMs)) {
    return Math.max(0, dateMs - Date.now());
  }
  return 0;
}

function shouldRetryAzureStatus(statusCode) {
  return [429, 500, 502, 503, 504].includes(Number(statusCode));
}

async function fetchResponseTextWithTimeout(url, options = {}, timeoutMs = CLOUD_METRICS_AZURE_TIMEOUT_MS) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    const text = await response.text();
    return { response, text };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchAzureJsonWithRetry(url, options = {}, context = 'Azure request') {
  let attempt = 0;
  while (true) {
    let response;
    let text;
    try {
      const result = await fetchResponseTextWithTimeout(url, options, CLOUD_METRICS_AZURE_TIMEOUT_MS);
      response = result.response;
      text = result.text;
    } catch (error) {
      if (attempt >= CLOUD_METRICS_AZURE_MAX_RETRIES) {
        throw error;
      }
      const backoff = Math.min(8_000, 500 * 2 ** attempt + Math.floor(Math.random() * 250));
      await waitMs(backoff);
      attempt += 1;
      continue;
    }

    if (response.ok) {
      if (!text) {
        return {};
      }
      try {
        return JSON.parse(text);
      } catch (_error) {
        const parseError = new Error(`${context} returned a non-JSON response.`);
        parseError.statusCode = response.status;
        throw parseError;
      }
    }

    if (attempt < CLOUD_METRICS_AZURE_MAX_RETRIES && shouldRetryAzureStatus(response.status)) {
      const retryAfterMs = parseRetryAfterMs(response.headers.get('retry-after'));
      const backoff = retryAfterMs > 0 ? retryAfterMs : Math.min(10_000, 600 * 2 ** attempt + Math.floor(Math.random() * 300));
      await waitMs(backoff);
      attempt += 1;
      continue;
    }

    const error = new Error(`${context} failed (${response.status}): ${String(text || '').slice(0, 600)}`);
    error.statusCode = response.status;
    throw error;
  }
}

function getAzureMetricCredentials() {
  return {
    tenantId: String(process.env.AZURE_TENANT_ID || '').trim(),
    clientId: String(process.env.AZURE_CLIENT_ID || '').trim(),
    clientSecret: String(process.env.AZURE_CLIENT_SECRET || '').trim()
  };
}

function isAzureCloudMetricsConfigured() {
  const creds = getAzureMetricCredentials();
  return Boolean(creds.tenantId && creds.clientId && creds.clientSecret);
}

function isAzureCloudMetricsAvailable() {
  return CLOUD_METRICS_AZURE_ENABLED && isAzureCloudMetricsConfigured();
}

async function getAzureCloudMetricsToken() {
  const creds = getAzureMetricCredentials();
  if (!creds.tenantId || !creds.clientId || !creds.clientSecret) {
    throw new Error('Azure metrics sync requires AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET.');
  }

  if (azureCloudMetricsTokenCache.accessToken && azureCloudMetricsTokenCache.expiresAtMs > Date.now() + 120_000) {
    return azureCloudMetricsTokenCache.accessToken;
  }

  const tokenUrl = `https://login.microsoftonline.com/${encodeURIComponent(creds.tenantId)}/oauth2/v2.0/token`;
  const body = new URLSearchParams({
    client_id: creds.clientId,
    client_secret: creds.clientSecret,
    grant_type: 'client_credentials',
    scope: 'https://management.azure.com/.default'
  });

  const payload = await fetchAzureJsonWithRetry(
    tokenUrl,
    {
      method: 'POST',
      headers: {
        'content-type': 'application/x-www-form-urlencoded'
      },
      body
    },
    'Azure token request'
  );

  const token = String(payload?.access_token || '').trim();
  if (!token) {
    throw new Error('Azure token response missing access_token.');
  }
  const expiresInSec = Number(payload?.expires_in);
  azureCloudMetricsTokenCache.accessToken = token;
  azureCloudMetricsTokenCache.expiresAtMs = Date.now() + (Number.isFinite(expiresInSec) ? expiresInSec : 3600) * 1000;
  return token;
}

function normalizeCloudMetricProvider(value, fallback = 'azure') {
  const normalized = normalizeProvider(value);
  if (normalized && CLOUD_METRIC_PROVIDER_ORDER.includes(normalized)) {
    return normalized;
  }
  const defaultValue = normalizeProvider(fallback);
  return CLOUD_METRIC_PROVIDER_ORDER.includes(defaultValue) ? defaultValue : 'azure';
}

function normalizeAwsCloudMetricsRegion(value, fallback = 'us-east-1') {
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  if (normalized) {
    return normalized;
  }
  return String(fallback || 'us-east-1')
    .trim()
    .toLowerCase() || 'us-east-1';
}

function normalizeAwsCloudMetricsRegionList(input, fallbackRegion = 'us-east-1') {
  const values = Array.isArray(input) ? input : parseEnvStringList(input, { lowercase: true });
  const normalized = values
    .map((item) => normalizeAwsCloudMetricsRegion(item))
    .filter(Boolean);
  if (!normalized.length) {
    return [normalizeAwsCloudMetricsRegion(fallbackRegion, 'us-east-1')];
  }
  return Array.from(new Set(normalized));
}

function normalizeAwsCloudMetricsAccount(rawAccount = {}, index = 0) {
  if (!rawAccount || typeof rawAccount !== 'object') {
    return null;
  }

  const accessKeyId = String(
    rawAccount.accessKeyId || rawAccount.accessKey || rawAccount.access_key || rawAccount.awsAccessKeyId || ''
  ).trim();
  const secretAccessKey = String(
    rawAccount.secretAccessKey || rawAccount.secretKey || rawAccount.secret_key || rawAccount.awsSecretAccessKey || ''
  ).trim();
  const sessionToken = String(rawAccount.sessionToken || rawAccount.session_token || rawAccount.token || '').trim();
  if (!accessKeyId || !secretAccessKey) {
    return null;
  }

  const accountId = String(
    rawAccount.accountId || rawAccount.id || rawAccount.account || rawAccount.name || `aws-${index + 1}`
  ).trim();
  const displayName = String(rawAccount.displayName || rawAccount.label || rawAccount.name || accountId || 'AWS').trim();
  const region = normalizeAwsCloudMetricsRegion(rawAccount.region || process.env.AWS_DEFAULT_REGION || 'us-east-1');
  const cloudWatchRegion = normalizeAwsCloudMetricsRegion(
    rawAccount.cloudWatchRegion ||
      rawAccount.cloudwatchRegion ||
      process.env.AWS_DEFAULT_CLOUDWATCH_REGION ||
      region,
    region
  );
  const efsRegions = normalizeAwsCloudMetricsRegionList(
    rawAccount.efsRegions || rawAccount.efsRegion || process.env.AWS_DEFAULT_EFS_REGIONS || '',
    cloudWatchRegion
  );
  const metricRegions = normalizeAwsCloudMetricsRegionList(
    rawAccount.metricRegions ||
      rawAccount.cloudWatchRegions ||
      rawAccount.cloudwatchRegions ||
      rawAccount.regions ||
      rawAccount.monitoringRegions ||
      rawAccount.monitoringRegion ||
      rawAccount.cloudMetricsRegions ||
      rawAccount.cloudMetricsRegion ||
      rawAccount.efsRegions ||
      '',
    cloudWatchRegion
  );

  return {
    accountId,
    displayName: displayName || accountId,
    accessKeyId,
    secretAccessKey,
    sessionToken: sessionToken || null,
    region,
    cloudWatchRegion,
    metricRegions,
    efsRegions
  };
}

function parseAwsAccountsFromEnv() {
  const accounts = getAwsAccountConfigs({
    includeEnvFallback: true,
    includeProfileOnly: false
  })
    .map((row, index) => normalizeAwsCloudMetricsAccount(row, index))
    .filter(Boolean);

  const deduped = new Map();
  for (const account of accounts) {
    const key = `${String(account.accountId || '').trim().toLowerCase()}|${String(account.accessKeyId || '').trim().toLowerCase()}`;
    if (!key || deduped.has(key)) {
      continue;
    }
    deduped.set(key, account);
  }
  return Array.from(deduped.values());
}

function isAwsCloudMetricsConfigured() {
  return parseAwsAccountsFromEnv().length > 0;
}

function isAwsCloudMetricsAvailable() {
  return CLOUD_METRICS_AWS_ENABLED && isAwsCloudMetricsConfigured();
}

function normalizeRackspaceMetricsAccount(rawAccount = {}, index = 0) {
  if (!rawAccount || typeof rawAccount !== 'object') {
    return null;
  }

  const credentials =
    rawAccount.credentials && typeof rawAccount.credentials === 'object' && !Array.isArray(rawAccount.credentials)
      ? rawAccount.credentials
      : {};
  const username = String(
    rawAccount.username || rawAccount.userName || rawAccount.user || credentials.username || credentials.userName || credentials.user || ''
  ).trim();
  const apiKey = String(
    rawAccount.apiKey || rawAccount.api_key || rawAccount.apikey || credentials.apiKey || credentials.api_key || credentials.apikey || ''
  ).trim();
  if (!username || !apiKey) {
    return null;
  }

  const accountId = String(
    rawAccount.accountId ||
      rawAccount.account_id ||
      rawAccount.accountNumber ||
      rawAccount.account_number ||
      rawAccount.ran ||
      rawAccount.RAN ||
      credentials.accountId ||
      credentials.account_id ||
      credentials.accountNumber ||
      credentials.account_number ||
      credentials.ran ||
      credentials.RAN ||
      `rackspace-${index + 1}`
  ).trim();
  const accountNumber = String(
    rawAccount.accountNumber ||
      rawAccount.account_number ||
      rawAccount.ran ||
      rawAccount.RAN ||
      credentials.accountNumber ||
      credentials.account_number ||
      credentials.ran ||
      credentials.RAN ||
      process.env.RACKSPACE_ACCOUNT_NUMBER ||
      ''
  ).trim();
  const displayName = String(
    rawAccount.displayName ||
      rawAccount.accountName ||
      rawAccount.account_label ||
      rawAccount.label ||
      rawAccount.name ||
      credentials.displayName ||
      credentials.accountName ||
      credentials.label ||
      credentials.name ||
      process.env.RACKSPACE_ACCOUNT_LABEL ||
      accountId ||
      accountNumber ||
      'Rackspace'
  ).trim();
  const identityUrl = String(
    rawAccount.identityUrl ||
      credentials.identityUrl ||
      process.env.RACKSPACE_IDENTITY_URL ||
      'https://identity.api.rackspacecloud.com/v2.0/tokens'
  ).trim();
  const region = String(rawAccount.region || credentials.region || process.env.RACKSPACE_REGION || '').trim().toUpperCase();

  return {
    accountId: accountId || accountNumber || `rackspace-${index + 1}`,
    accountNumber: accountNumber || null,
    displayName: displayName || accountId || accountNumber || `Rackspace ${index + 1}`,
    username,
    apiKey,
    identityUrl,
    region: region || null
  };
}

function parseRackspaceMetricsAccountsFromEnv() {
  const accounts = [];
  const rawJson = String(process.env.RACKSPACE_ACCOUNTS_JSON || '').trim();
  if (rawJson) {
    try {
      const parsed = JSON.parse(rawJson);
      const rows = Array.isArray(parsed) ? parsed : parsed && typeof parsed === 'object' ? Object.values(parsed) : [];
      for (const [index, row] of rows.entries()) {
        const normalized = normalizeRackspaceMetricsAccount(row, index);
        if (normalized) {
          accounts.push(normalized);
        }
      }
    } catch (_error) {
      // Ignore invalid JSON and continue with fallback env fields.
    }
  }

  const fallback = normalizeRackspaceMetricsAccount(
    {
      accountId: process.env.RACKSPACE_ACCOUNT_ID || process.env.RACKSPACE_ACCOUNT_NUMBER || 'rackspace-default',
      accountNumber: process.env.RACKSPACE_ACCOUNT_NUMBER || null,
      displayName: process.env.RACKSPACE_ACCOUNT_LABEL || process.env.RACKSPACE_ACCOUNT_NAME || 'Rackspace',
      username: process.env.RACKSPACE_USERNAME || '',
      apiKey: process.env.RACKSPACE_API_KEY || '',
      identityUrl: process.env.RACKSPACE_IDENTITY_URL || '',
      region: process.env.RACKSPACE_REGION || ''
    },
    accounts.length
  );
  if (fallback) {
    accounts.push(fallback);
  }

  const deduped = new Map();
  for (const account of accounts) {
    const dedupeKey = `${String(account.accountId || account.accountNumber || '')
      .trim()
      .toLowerCase()}|${String(account.username || '')
      .trim()
      .toLowerCase()}`;
    if (!dedupeKey || deduped.has(dedupeKey)) {
      continue;
    }
    deduped.set(dedupeKey, account);
  }

  return Array.from(deduped.values());
}

function isRackspaceCloudMetricsConfigured() {
  return parseRackspaceMetricsAccountsFromEnv().length > 0;
}

function isRackspaceCloudMetricsAvailable() {
  return CLOUD_METRICS_RACKSPACE_ENABLED && isRackspaceCloudMetricsConfigured();
}

function parseWasabiAccountsFromEnv() {
  const raw = normalizeJsonEnvValue(process.env.WASABI_ACCOUNTS_JSON || '');
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed;
    }
    return [];
  } catch (_error) {
    return [];
  }
}

function isCloudMetricsProviderEnabled(providerValue) {
  const provider = normalizeProvider(providerValue);
  if (provider === 'azure') {
    return CLOUD_METRICS_AZURE_ENABLED;
  }
  if (provider === 'aws') {
    return CLOUD_METRICS_AWS_ENABLED;
  }
  if (provider === 'rackspace') {
    return CLOUD_METRICS_RACKSPACE_ENABLED;
  }
  return true;
}

function getConfiguredCloudMetricProviderSet() {
  const set = new Set();
  const vendors = listVendors();
  for (const vendor of vendors) {
    const provider = normalizeProvider(vendor?.provider);
    if (provider && isCloudMetricsProviderEnabled(provider)) {
      set.add(provider);
    }
  }

  if (isAzureCloudMetricsAvailable()) {
    set.add('azure');
  }
  if (isAwsCloudMetricsAvailable()) {
    set.add('aws');
  }
  if (parseWasabiAccountsFromEnv().length > 0) {
    set.add('wasabi');
  }
  if (
    String(process.env.GCP_PROJECT_ID || process.env.GCP_BILLING_ACCOUNT_ID || process.env.GOOGLE_APPLICATION_CREDENTIALS || '')
      .trim()
      .length > 0
  ) {
    set.add('gcp');
  }
  if (isRackspaceCloudMetricsAvailable()) {
    set.add('rackspace');
  }

  return set;
}

function buildCloudMetricsProviderCatalog() {
  const configured = getConfiguredCloudMetricProviderSet();
  const summaryRows = summarizeCloudMetricsProviders();
  const summaryByProvider = new Map(
    summaryRows.map((row) => [normalizeCloudMetricProvider(row.provider), row])
  );

  const ordered = [];
  const seen = new Set();
  for (const provider of CLOUD_METRIC_PROVIDER_ORDER) {
    if (configured.has(provider) || summaryByProvider.has(provider)) {
      ordered.push(provider);
      seen.add(provider);
    }
  }
  for (const row of summaryRows) {
    const provider = normalizeCloudMetricProvider(row.provider);
    if (!seen.has(provider)) {
      ordered.push(provider);
      seen.add(provider);
    }
  }
  if (!ordered.length) {
    ordered.push('azure');
  }

  const supportedProviders = new Set(
    ['azure', 'aws', 'rackspace'].filter((provider) => {
      if (provider === 'azure') {
        return CLOUD_METRICS_AZURE_ENABLED;
      }
      if (provider === 'aws') {
        return CLOUD_METRICS_AWS_ENABLED;
      }
      if (provider === 'rackspace') {
        return CLOUD_METRICS_RACKSPACE_ENABLED;
      }
      return false;
    })
  );
  return ordered.map((provider) => {
    const summary = summaryByProvider.get(provider);
    const supported = supportedProviders.has(provider);
    return {
      id: provider,
      label: CLOUD_METRIC_PROVIDER_LABELS[provider] || provider.toUpperCase(),
      configured: configured.has(provider),
      supported,
      resourceCount: Number(summary?.resourceCount || 0),
      lastSyncAt: summary?.lastSyncAt || null
    };
  });
}

function getDefaultCloudMetricsProvider() {
  const providers = buildCloudMetricsProviderCatalog();
  const configuredSupported = providers.find((provider) => provider.supported && provider.configured);
  if (configuredSupported) {
    return configuredSupported.id;
  }
  const firstSupported = providers.find((provider) => provider.supported);
  if (firstSupported) {
    return firstSupported.id;
  }
  return 'azure';
}

function setCloudMetricsNextRun(referenceTimeMs = Date.now()) {
  cloudMetricsSyncState.nextRunAt = new Date(referenceTimeMs + CLOUD_METRICS_SYNC_INTERVAL_MS).toISOString();
}

function getCloudMetricsSyncStatus() {
  return {
    ...cloudMetricsSyncState,
    intervalMinutes: Math.round((CLOUD_METRICS_SYNC_INTERVAL_MS / 60_000) * 100) / 100
  };
}

function parseResourceGroupFromResourceId(resourceId) {
  const text = String(resourceId || '').trim();
  if (!text) {
    return null;
  }
  const match = text.match(/\/resourceGroups\/([^/]+)/i);
  return match ? match[1] : null;
}

async function listAzureMetricSubscriptions(armToken) {
  const subscriptions = [];
  let nextLink = 'https://management.azure.com/subscriptions?api-version=2022-12-01';
  while (nextLink) {
    const payload = await fetchAzureJsonWithRetry(
      nextLink,
      {
        headers: {
          Authorization: `Bearer ${armToken}`
        }
      },
      'List Azure subscriptions'
    );
    const rows = Array.isArray(payload?.value) ? payload.value : [];
    for (const row of rows) {
      const subscriptionId = String(row?.subscriptionId || '').trim();
      if (!subscriptionId) {
        continue;
      }
      subscriptions.push({
        subscriptionId,
        displayName: String(row?.displayName || subscriptionId).trim() || subscriptionId,
        state: String(row?.state || '').trim()
      });
    }
    nextLink = payload?.nextLink || null;
  }
  return subscriptions;
}

function normalizeAzureMetricResourceType(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function extractAzureVmSize(...values) {
  for (const value of values) {
    const text = String(value || '').trim();
    if (text) {
      return text;
    }
  }
  return null;
}

function isAzureMetricResourceCandidate(resource = {}) {
  const resourceId = String(resource?.resourceId || '').trim();
  if (!resourceId) {
    return false;
  }
  const type = normalizeAzureMetricResourceType(resource?.resourceType);
  if (!type) {
    return false;
  }
  const ignoredPrefixes = ['microsoft.authorization/', 'microsoft.resources/'];
  if (ignoredPrefixes.some((prefix) => type.startsWith(prefix))) {
    return false;
  }
  const ignoredTypes = new Set([
    'microsoft.classiccompute/domainnames',
    'microsoft.classicnetwork/reservedips',
    'microsoft.classicstorage/storageaccounts',
    'microsoft.compute/virtualmachines/extensions',
    'microsoft.network/networkwatchers/flowlogs',
    'microsoft.network/networkwatchers/connectionmonitors',
    'microsoft.insights/diagnosticsettings',
    'microsoft.resources/tags',
    'microsoft.resources/deployments'
  ]);
  return !ignoredTypes.has(type);
}

async function listAzureResourcesViaResourceGraph(armToken, subscriptionIds = []) {
  const result = [];
  let skipToken = null;
  do {
    const payload = await fetchAzureJsonWithRetry(
      'https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2022-10-01',
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${armToken}`,
          'content-type': 'application/json'
        },
        body: JSON.stringify({
          subscriptions: subscriptionIds,
          query:
            "Resources | project id, name, type, location, resourceGroup, subscriptionId, kind, skuName=tostring(sku.name), vmSize=tostring(properties.hardwareProfile.vmSize) | order by id asc",
          options: {
            $top: 1000,
            resultFormat: 'objectArray',
            ...(skipToken ? { $skipToken: skipToken } : {})
          }
        })
      },
      'Azure Resource Graph query'
    );
    const rows = Array.isArray(payload?.data) ? payload.data : [];
    for (const row of rows) {
      const resourceId = String(row?.id || '').trim();
      if (!resourceId) {
        continue;
      }
      result.push({
        resourceId,
        resourceName: String(row?.name || '').trim() || resourceId.split('/').slice(-1)[0] || resourceId,
        resourceType: normalizeAzureMetricResourceType(row?.type),
        subscriptionId: String(row?.subscriptionId || '').trim(),
        location: String(row?.location || '').trim() || null,
        resourceGroup: String(row?.resourceGroup || '').trim() || parseResourceGroupFromResourceId(resourceId),
        kind: String(row?.kind || '').trim() || null,
        skuName: String(row?.skuName || '').trim() || null,
        vmSize: extractAzureVmSize(row?.vmSize, row?.skuName)
      });
    }
    skipToken = payload?.$skipToken || payload?.skipToken || null;
  } while (skipToken);
  return result;
}

async function listAzureResourcesViaSubscriptionApi(armToken, subscriptionIds = []) {
  const resources = [];
  for (const subscriptionId of subscriptionIds) {
    let nextLink = `https://management.azure.com/subscriptions/${encodeURIComponent(
      subscriptionId
    )}/resources?api-version=2021-04-01`;
    while (nextLink) {
      const payload = await fetchAzureJsonWithRetry(
        nextLink,
        {
          headers: {
            Authorization: `Bearer ${armToken}`
          }
        },
        `List Azure resources (${subscriptionId})`
      );
      const rows = Array.isArray(payload?.value) ? payload.value : [];
      for (const row of rows) {
        const resourceId = String(row?.id || '').trim();
        if (!resourceId) {
          continue;
        }
        resources.push({
          resourceId,
          resourceName: String(row?.name || '').trim() || resourceId.split('/').slice(-1)[0] || resourceId,
          resourceType: normalizeAzureMetricResourceType(row?.type),
          subscriptionId,
          location: String(row?.location || '').trim() || null,
          resourceGroup:
            String(row?.resourceGroup || '').trim() ||
            parseResourceGroupFromResourceId(resourceId) ||
            null,
          kind: String(row?.kind || '').trim() || null,
          skuName: String(row?.sku?.name || '').trim() || null,
          vmSize: extractAzureVmSize(row?.properties?.hardwareProfile?.vmSize, row?.sku?.name)
        });
      }
      nextLink = payload?.nextLink || null;
    }
  }
  return resources;
}

async function listAzureResourcesForCloudMetrics(armToken) {
  const subscriptions = (await listAzureMetricSubscriptions(armToken)).filter((row) => {
    const state = String(row?.state || '').trim().toLowerCase();
    return !state || state === 'enabled';
  });
  const subscriptionIds = subscriptions.map((row) => row.subscriptionId).filter(Boolean);
  if (!subscriptionIds.length) {
    return {
      subscriptions,
      resources: []
    };
  }

  let resources = [];
  try {
    resources = await listAzureResourcesViaResourceGraph(armToken, subscriptionIds);
  } catch (error) {
    console.warn('[cloudstudio] Azure Resource Graph listing failed. Falling back to subscription API.', {
      message: error?.message || String(error)
    });
    resources = await listAzureResourcesViaSubscriptionApi(armToken, subscriptionIds);
  }

  const deduped = new Map();
  for (const rawResource of resources) {
    const resource = {
      ...rawResource,
      resourceType: normalizeAzureMetricResourceType(rawResource?.resourceType)
    };
    if (!isAzureMetricResourceCandidate(resource)) {
      continue;
    }
    const key = String(resource?.resourceId || '').trim().toLowerCase();
    if (!key) {
      continue;
    }
    deduped.set(key, resource);
  }

  const allResources = Array.from(deduped.values()).sort((left, right) => {
    const typeCompare = String(left.resourceType || '').localeCompare(String(right.resourceType || ''));
    if (typeCompare !== 0) {
      return typeCompare;
    }
    return String(left.resourceName || '').localeCompare(String(right.resourceName || ''));
  });
  const limitedResources = allResources.slice(0, CLOUD_METRICS_AZURE_MAX_RESOURCES);

  return {
    subscriptions,
    resources: limitedResources,
    truncated: allResources.length > limitedResources.length,
    totalDiscovered: allResources.length
  };
}

async function listAzureMetricDefinitions(armToken, resourceId) {
  const endpoint = `https://management.azure.com${resourceId}/providers/microsoft.insights/metricDefinitions?api-version=2018-01-01`;
  try {
    const payload = await fetchAzureJsonWithRetry(
      endpoint,
      {
        headers: {
          Authorization: `Bearer ${armToken}`
        }
      },
      `Azure metric definitions (${resourceId})`
    );
    return Array.isArray(payload?.value) ? payload.value : [];
  } catch (error) {
    if ([400, 403, 404].includes(Number(error?.statusCode))) {
      return [];
    }
    throw error;
  }
}

function toFiniteNumberOrNull(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function chunkItems(items = [], chunkSize = 20) {
  const list = Array.isArray(items) ? items : [];
  const size = Math.max(1, Number(chunkSize) || 1);
  const output = [];
  for (let index = 0; index < list.length; index += size) {
    output.push(list.slice(index, index + size));
  }
  return output;
}

function normalizeAzureMetricNamespace(value) {
  const text = String(value || '').trim();
  return text ? text : null;
}

function listAzureMetricQueryGroups(definitions = []) {
  const groupMap = new Map();
  for (const definition of definitions) {
    const metricName = String(definition?.name?.value || definition?.name?.localizedValue || '').trim();
    if (!metricName) {
      continue;
    }
    const namespace = normalizeAzureMetricNamespace(definition?.namespace);
    const groupKey = namespace ? namespace.toLowerCase() : '';
    const group = groupMap.get(groupKey) || {
      namespace,
      names: []
    };
    if (!group.names.includes(metricName)) {
      group.names.push(metricName);
    }
    groupMap.set(groupKey, group);
  }

  return Array.from(groupMap.values()).map((group) => ({
    namespace: group.namespace,
    names: group.names.sort((left, right) => left.localeCompare(right))
  }));
}

function getLatestMetricSample(metricRecord = {}) {
  const timeseries = Array.isArray(metricRecord?.timeseries) ? metricRecord.timeseries : [];
  let best = null;
  for (const series of timeseries) {
    const points = Array.isArray(series?.data) ? series.data : [];
    for (const point of points) {
      const average = toFiniteNumberOrNull(point?.average);
      const minimum = toFiniteNumberOrNull(point?.minimum);
      const maximum = toFiniteNumberOrNull(point?.maximum);
      const total = toFiniteNumberOrNull(point?.total);
      const count = toFiniteNumberOrNull(point?.count);
      const value = average ?? total ?? maximum ?? minimum;
      if (value === null) {
        continue;
      }
      const timestamp = String(point?.timeStamp || point?.timestamp || '').trim() || null;
      const comparableTime = timestamp ? Date.parse(timestamp) : NaN;
      if (!best) {
        best = { value, average, minimum, maximum, total, count, timestamp, comparableTime };
        continue;
      }
      const bestTime = Number.isFinite(best.comparableTime) ? best.comparableTime : -Infinity;
      const nextTime = Number.isFinite(comparableTime) ? comparableTime : bestTime + 1;
      if (nextTime >= bestTime) {
        best = { value, average, minimum, maximum, total, count, timestamp, comparableTime: nextTime };
      }
    }
  }
  if (!best) {
    return null;
  }
  return {
    value: best.value,
    average: best.average,
    minimum: best.minimum,
    maximum: best.maximum,
    total: best.total,
    count: best.count,
    timestamp: best.timestamp
  };
}

function shouldReplaceMetricRecord(current = null, next = null) {
  if (!next) {
    return false;
  }
  if (!current) {
    return true;
  }
  const currentTs = Date.parse(String(current?.timestamp || '').trim());
  const nextTs = Date.parse(String(next?.timestamp || '').trim());
  if (Number.isFinite(nextTs) && Number.isFinite(currentTs)) {
    return nextTs >= currentTs;
  }
  if (Number.isFinite(nextTs)) {
    return true;
  }
  if (Number.isFinite(currentTs)) {
    return false;
  }
  return true;
}

async function fetchAzureMetricBatchPayload(
  armToken,
  resource,
  metricNames = [],
  startAt,
  endAt,
  metricNamespace = null
) {
  const buildParams = (namespace) => {
    const params = new URLSearchParams({
      'api-version': '2023-10-01',
      timespan: `${startAt.toISOString()}/${endAt.toISOString()}`,
      interval: 'PT1M',
      metricnames: metricNames.join(','),
      aggregation: 'Average,Minimum,Maximum,Total',
      AutoAdjustTimegrain: 'true'
    });
    if (namespace) {
      params.set('metricnamespace', namespace);
    }
    return params;
  };

  const runRequest = async (namespace) => {
    const params = buildParams(namespace);
    const endpoint = `https://management.azure.com${resource.resourceId}/providers/microsoft.insights/metrics?${params.toString()}`;
    return fetchAzureJsonWithRetry(
      endpoint,
      {
        headers: {
          Authorization: `Bearer ${armToken}`
        }
      },
      `Azure metrics (${resource.resourceName})`
    );
  };

  try {
    return await runRequest(metricNamespace);
  } catch (error) {
    if (metricNamespace && [400, 404].includes(Number(error?.statusCode))) {
      return runRequest(null);
    }
    throw error;
  }
}

async function fetchAzureMetricsForResource(armToken, resource, definitions = []) {
  const queryGroups = listAzureMetricQueryGroups(definitions);
  if (!queryGroups.length) {
    return {
      metrics: [],
      warnings: []
    };
  }
  const endAt = new Date();
  const startAt = new Date(endAt.getTime() - CLOUD_METRICS_AZURE_LOOKBACK_MINUTES * 60 * 1000);
  const warnings = [];
  const metricMap = new Map();

  for (const group of queryGroups) {
    const batches = chunkItems(group.names, CLOUD_METRICS_AZURE_METRIC_BATCH_SIZE);
    for (const batch of batches) {
      let payload;
      try {
        payload = await fetchAzureMetricBatchPayload(
          armToken,
          resource,
          batch,
          startAt,
          endAt,
          group.namespace
        );
      } catch (error) {
        warnings.push(error?.message || `Failed to read metrics: ${batch.join(', ')}`);
        continue;
      }
      const values = Array.isArray(payload?.value) ? payload.value : [];
      for (const metric of values) {
        const latest = getLatestMetricSample(metric);
        if (!latest) {
          continue;
        }
        const name = String(metric?.name?.localizedValue || metric?.name?.value || '').trim() || 'Metric';
        const namespace = normalizeAzureMetricNamespace(metric?.namespace || group.namespace);
        const key = `${String(namespace || '').toLowerCase()}::${name.toLowerCase()}`;
        const candidate = {
          name,
          namespace,
          unit: String(metric?.unit || '').trim() || null,
          value: latest.value,
          average: latest.average,
          minimum: latest.minimum,
          maximum: latest.maximum,
          total: latest.total,
          count: latest.count,
          timestamp: latest.timestamp
        };
        const existing = metricMap.get(key);
        if (shouldReplaceMetricRecord(existing, candidate)) {
          metricMap.set(key, candidate);
        }
      }
      if (CLOUD_METRICS_AZURE_METRIC_BATCH_DELAY_MS > 0) {
        await waitMs(CLOUD_METRICS_AZURE_METRIC_BATCH_DELAY_MS);
      }
    }
  }

  const metrics = Array.from(metricMap.values()).sort((left, right) => {
    const namespaceLeft = String(left?.namespace || '').toLowerCase();
    const namespaceRight = String(right?.namespace || '').toLowerCase();
    if (namespaceLeft !== namespaceRight) {
      return namespaceLeft.localeCompare(namespaceRight);
    }
    return String(left?.name || '').localeCompare(String(right?.name || ''));
  });

  return {
    metrics,
    warnings
  };
}

async function mapWithConcurrency(items, concurrency, task) {
  const safeItems = Array.isArray(items) ? items : [];
  const safeConcurrency = Math.max(1, Number(concurrency) || 1);
  const output = new Array(safeItems.length);
  let cursor = 0;

  async function worker() {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= safeItems.length) {
        return;
      }
      output[index] = await task(safeItems[index], index);
    }
  }

  const workers = Array.from({ length: Math.min(safeConcurrency, safeItems.length || 1) }, () => worker());
  await Promise.all(workers);
  return output;
}

async function listAzureVmSizesForLocation(armToken, subscriptionId, location) {
  if (!subscriptionId || !location) {
    return [];
  }
  const endpoint = `https://management.azure.com/subscriptions/${encodeURIComponent(
    subscriptionId
  )}/providers/Microsoft.Compute/locations/${encodeURIComponent(location)}/vmSizes?api-version=2023-09-01`;
  try {
    const payload = await fetchAzureJsonWithRetry(
      endpoint,
      {
        headers: {
          Authorization: `Bearer ${armToken}`
        }
      },
      `Azure VM size catalog (${subscriptionId}/${location})`
    );
    return Array.isArray(payload?.value) ? payload.value : [];
  } catch (error) {
    if ([400, 403, 404].includes(Number(error?.statusCode))) {
      return [];
    }
    throw error;
  }
}

async function buildAzureVmMemoryCatalog(armToken, resources = []) {
  const vmTargets = new Map();
  for (const resource of resources) {
    const resourceType = normalizeAzureMetricResourceType(resource?.resourceType);
    if (resourceType !== 'microsoft.compute/virtualmachines') {
      continue;
    }
    const subscriptionId = String(resource?.subscriptionId || '').trim();
    const location = String(resource?.location || '').trim().toLowerCase();
    if (!subscriptionId || !location) {
      continue;
    }
    const key = `${subscriptionId}|${location}`;
    vmTargets.set(key, {
      subscriptionId,
      location
    });
  }
  const targets = Array.from(vmTargets.values());
  if (!targets.length) {
    return new Map();
  }

  const catalog = new Map();
  await mapWithConcurrency(targets, Math.min(4, CLOUD_METRICS_AZURE_CONCURRENCY), async (target) => {
    const sizeRows = await listAzureVmSizesForLocation(armToken, target.subscriptionId, target.location);
    for (const row of sizeRows) {
      const vmSize = String(row?.name || '').trim().toLowerCase();
      const memoryMb = Number(row?.memoryInMB);
      if (!vmSize || !Number.isFinite(memoryMb) || memoryMb <= 0) {
        continue;
      }
      const key = `${target.subscriptionId}|${target.location}|${vmSize}`;
      catalog.set(key, memoryMb);
    }
  });
  return catalog;
}

function appendAzureVmMemoryMetric(resource = {}, metrics = [], vmMemoryCatalog = new Map(), fetchedAt = null) {
  const typeKey = normalizeAzureMetricResourceType(resource?.resourceType);
  if (typeKey !== 'microsoft.compute/virtualmachines') {
    return Array.isArray(metrics) ? metrics : [];
  }
  const existingMetrics = Array.isArray(metrics) ? [...metrics] : [];
  const hasMemoryMetric = existingMetrics.some((metric) => {
    const metricName = String(metric?.name || '').trim().toLowerCase();
    return metricName.includes('memory') || metricName.includes('ram');
  });
  if (hasMemoryMetric) {
    return existingMetrics;
  }

  const subscriptionId = String(resource?.subscriptionId || '').trim();
  const location = String(resource?.location || '').trim().toLowerCase();
  const vmSize = String(resource?.vmSize || '').trim().toLowerCase();
  if (!subscriptionId || !location || !vmSize) {
    return existingMetrics;
  }
  const catalogKey = `${subscriptionId}|${location}|${vmSize}`;
  const memoryMb = Number(vmMemoryCatalog.get(catalogKey));
  if (!Number.isFinite(memoryMb) || memoryMb <= 0) {
    return existingMetrics;
  }
  const memoryBytes = memoryMb * 1024 * 1024;
  existingMetrics.push({
    name: 'Provisioned VM Memory',
    namespace: 'CloudStudio.Synthetic',
    unit: 'Bytes',
    value: memoryBytes,
    average: memoryBytes,
    minimum: memoryBytes,
    maximum: memoryBytes,
    total: memoryBytes,
    count: null,
    timestamp: fetchedAt || toIsoNow()
  });
  return existingMetrics;
}

async function collectAzureCloudMetricsResources() {
  const armToken = await getAzureCloudMetricsToken();
  const discovered = await listAzureResourcesForCloudMetrics(armToken);
  const resources = Array.isArray(discovered?.resources) ? discovered.resources : [];
  const now = toIsoNow();
  const definitionCache = new Map();
  const vmMemoryCatalog = await buildAzureVmMemoryCatalog(armToken, resources);
  let metricsErrorCount = 0;
  let definitionErrorCount = 0;

  const rows = await mapWithConcurrency(resources, CLOUD_METRICS_AZURE_CONCURRENCY, async (resource) => {
    const typeKey = String(resource?.resourceType || '').trim().toLowerCase() || 'unknown';
    let definitions = definitionCache.get(typeKey);
    let definitionError = null;
    if (!definitions) {
      try {
        definitions = await listAzureMetricDefinitions(armToken, resource.resourceId);
      } catch (error) {
        definitions = [];
        definitionError = error?.message || 'Metric definition lookup failed.';
        definitionErrorCount += 1;
      }
      definitionCache.set(typeKey, definitions);
    } else if (!Array.isArray(definitions)) {
      definitions = [];
    }

    let metrics = [];
    const metricWarnings = [];
    let metricError = definitionError;
    try {
      if (definitions.length) {
        const metricResult = await fetchAzureMetricsForResource(armToken, resource, definitions);
        metrics = Array.isArray(metricResult?.metrics) ? metricResult.metrics : [];
        if (Array.isArray(metricResult?.warnings) && metricResult.warnings.length) {
          metricWarnings.push(...metricResult.warnings.slice(0, 5));
        }
      }
    } catch (error) {
      metricError = error?.message || 'Metric read failed.';
      metricsErrorCount += 1;
    }
    metrics = appendAzureVmMemoryMetric(resource, metrics, vmMemoryCatalog, now);
    if (!metricError && metricWarnings.length) {
      metricError = metricWarnings[0];
      metricsErrorCount += 1;
    }

    const metricNamespaces = Array.from(
      new Set(
        (Array.isArray(definitions) ? definitions : [])
          .map((definition) => normalizeAzureMetricNamespace(definition?.namespace))
          .filter(Boolean)
      )
    );

    return {
      provider: 'azure',
      resourceId: resource.resourceId,
      accountId: resource.subscriptionId,
      subscriptionId: resource.subscriptionId,
      resourceGroup: resource.resourceGroup || null,
      location: resource.location || null,
      resourceType: typeKey,
      resourceName: resource.resourceName,
      metrics,
      fetchedAt: now,
      metadata: {
        kind: resource?.kind || null,
        skuName: resource?.skuName || null,
        vmSize: resource?.vmSize || null,
        metricNamespaces: metricNamespaces.slice(0, 32),
        definitionCount: Array.isArray(definitions) ? definitions.length : 0,
        metricCount: metrics.length,
        warnings: metricWarnings,
        error: metricError
      }
    };
  });

  return {
    rows,
    summary: {
      subscriptions: Array.isArray(discovered?.subscriptions) ? discovered.subscriptions.length : 0,
      resourcesDiscovered: Number(discovered?.totalDiscovered || resources.length),
      resourcesProcessed: rows.length,
      truncated: Boolean(discovered?.truncated),
      metricsErrorCount,
      definitionErrorCount
    }
  };
}

async function syncAzureCloudMetrics(trigger = 'manual') {
  const fetchedAt = toIsoNow();
  const collection = await collectAzureCloudMetricsResources();
  const replaceResult = replaceCloudMetricsLatest('azure', collection.rows, {
    fetchedAt,
    syncToken: crypto.randomUUID()
  });

  return {
    provider: 'azure',
    trigger,
    fetchedAt,
    upsertedCount: replaceResult.upsertedCount,
    staleDeletedCount: replaceResult.staleDeletedCount,
    ...collection.summary
  };
}

function normalizeAwsMetricNamespace(value) {
  return String(value || '').trim() || 'AWS/Unknown';
}

function normalizeAwsMetricDimensions(dimensions = []) {
  const rows = Array.isArray(dimensions) ? dimensions : [];
  return rows
    .map((dimension) => ({
      name: String(dimension?.Name || dimension?.name || '').trim(),
      value: String(dimension?.Value || dimension?.value || '').trim()
    }))
    .filter((dimension) => dimension.name && dimension.value)
    .sort((left, right) => {
      const nameCompare = left.name.localeCompare(right.name);
      if (nameCompare !== 0) {
        return nameCompare;
      }
      return left.value.localeCompare(right.value);
    });
}

function buildAwsMetricDefinitionKey(metric = {}) {
  const namespace = normalizeAwsMetricNamespace(metric?.namespace);
  const metricName = String(metric?.metricName || '').trim();
  const dimensions = normalizeAwsMetricDimensions(metric?.dimensions);
  const dimsKey = dimensions.map((dimension) => `${dimension.name}=${dimension.value}`).join('|');
  return `${namespace}|${metricName}|${dimsKey}`;
}

function selectAwsPrimaryDimension(dimensions = []) {
  const safeDimensions = normalizeAwsMetricDimensions(dimensions);
  if (!safeDimensions.length) {
    return null;
  }
  const byName = new Map(safeDimensions.map((dimension) => [dimension.name.toLowerCase(), dimension]));
  for (const name of AWS_CLOUD_METRIC_PRIMARY_DIMENSION_PRIORITY) {
    const found = byName.get(String(name || '').trim().toLowerCase());
    if (found) {
      return found;
    }
  }
  return safeDimensions[0];
}

function buildAwsMetricResourceType(namespaceRaw, primaryDimension = null) {
  const namespace = normalizeAwsMetricNamespace(namespaceRaw)
    .replace(/^aws\//i, '')
    .replace(/[^a-z0-9]+/gi, '-')
    .toLowerCase();
  const dimensionPart = String(primaryDimension?.name || 'resource')
    .trim()
    .replace(/[^a-z0-9]+/gi, '-')
    .toLowerCase();
  return `aws/${namespace || 'service'}/${dimensionPart || 'resource'}`;
}

function buildAwsMetricDisplayName(metricDefinition = {}, primaryDimension = null) {
  const metricName = String(metricDefinition?.metricName || 'Metric').trim() || 'Metric';
  const dimensions = normalizeAwsMetricDimensions(metricDefinition?.dimensions);
  const extras = dimensions
    .filter((dimension) => {
      if (!primaryDimension) {
        return true;
      }
      return !(dimension.name === primaryDimension.name && dimension.value === primaryDimension.value);
    })
    .slice(0, 2)
    .map((dimension) => `${dimension.name}=${dimension.value}`);
  if (!extras.length) {
    return metricName;
  }
  return `${metricName} [${extras.join(', ')}]`.slice(0, 256);
}

function buildAwsCloudMetricResourceDescriptor(metricDefinition = {}, account = {}, region = 'us-east-1') {
  const namespace = normalizeAwsMetricNamespace(metricDefinition?.namespace);
  const metricName = String(metricDefinition?.metricName || '').trim() || 'Metric';
  const dimensions = normalizeAwsMetricDimensions(metricDefinition?.dimensions);
  const primaryDimension = selectAwsPrimaryDimension(dimensions);
  const primaryLabel = primaryDimension?.name || 'MetricName';
  const primaryValue = primaryDimension?.value || metricName;
  const accountId = String(account?.accountId || 'aws').trim() || 'aws';
  const resourceType = buildAwsMetricResourceType(namespace, primaryDimension);
  const resourceId = `aws://${accountId}/${region}/${namespace}/${primaryLabel}/${primaryValue}`;
  return {
    resourceId,
    resourceName: primaryValue,
    resourceType,
    accountId,
    location: region,
    namespace,
    primaryDimension,
    metricDisplayName: buildAwsMetricDisplayName(metricDefinition, primaryDimension)
  };
}

function resolveAwsMetricRegionsForAccount(account = {}) {
  if (CLOUD_METRICS_AWS_REGIONS.length) {
    return Array.from(new Set(CLOUD_METRICS_AWS_REGIONS));
  }
  const fallbackRegion = account?.cloudWatchRegion || account?.region || 'us-east-1';
  const primaryRegions = normalizeAwsCloudMetricsRegionList(account?.metricRegions || '', fallbackRegion);
  const efsRegions = normalizeAwsCloudMetricsRegionList(
    account?.efsRegions || process.env.AWS_DEFAULT_EFS_REGIONS || '',
    fallbackRegion
  );
  return Array.from(new Set([...primaryRegions, ...efsRegions]));
}

function createAwsCloudWatchClient(account = {}, region = 'us-east-1') {
  const normalizedRegion = normalizeAwsCloudMetricsRegion(region, account?.cloudWatchRegion || account?.region || 'us-east-1');
  const credentials = {
    accessKeyId: String(account?.accessKeyId || '').trim(),
    secretAccessKey: String(account?.secretAccessKey || '').trim(),
    ...(String(account?.sessionToken || '').trim() ? { sessionToken: String(account.sessionToken).trim() } : {})
  };
  return new CloudWatchClient({
    region: normalizedRegion,
    credentials,
    maxAttempts: Math.max(1, CLOUD_METRICS_AWS_MAX_RETRIES + 1)
  });
}

async function listAwsMetricDefinitions(cloudWatchClient, options = {}) {
  const maxMetrics = Math.max(1, Number(options.maxMetrics || CLOUD_METRICS_AWS_MAX_METRICS_PER_ACCOUNT));
  const namespaces = Array.isArray(options.namespaces) && options.namespaces.length ? options.namespaces : [null];
  const metricMap = new Map();

  for (const namespace of namespaces) {
    let nextToken = undefined;
    while (true) {
      const command = new ListMetricsCommand({
        ...(namespace ? { Namespace: namespace } : {}),
        ...(nextToken ? { NextToken: nextToken } : {})
      });
      const payload = await cloudWatchClient.send(command);
      const rows = Array.isArray(payload?.Metrics) ? payload.Metrics : [];
      for (const metric of rows) {
        const definition = {
          namespace: normalizeAwsMetricNamespace(metric?.Namespace || namespace || null),
          metricName: String(metric?.MetricName || '').trim(),
          dimensions: normalizeAwsMetricDimensions(metric?.Dimensions || [])
        };
        if (!definition.metricName) {
          continue;
        }
        const key = buildAwsMetricDefinitionKey(definition);
        if (!metricMap.has(key)) {
          metricMap.set(key, definition);
        }
        if (metricMap.size >= maxMetrics) {
          return Array.from(metricMap.values());
        }
      }
      nextToken = payload?.NextToken || undefined;
      if (!nextToken) {
        break;
      }
    }
  }

  return Array.from(metricMap.values());
}

function getLatestAwsDatapoint(datapoints = []) {
  const rows = Array.isArray(datapoints) ? datapoints : [];
  let best = null;
  for (const row of rows) {
    const timestampText = String(row?.Timestamp || row?.timestamp || '').trim();
    if (!timestampText) {
      continue;
    }
    const timestampMs = Date.parse(timestampText);
    if (!Number.isFinite(timestampMs)) {
      continue;
    }
    if (!best || timestampMs >= best.timestampMs) {
      best = {
        timestampMs,
        timestamp: new Date(timestampMs).toISOString(),
        average: toFiniteNumberOrNull(row?.Average),
        minimum: toFiniteNumberOrNull(row?.Minimum),
        maximum: toFiniteNumberOrNull(row?.Maximum),
        total: toFiniteNumberOrNull(row?.Sum),
        count: toFiniteNumberOrNull(row?.SampleCount),
        unit: String(row?.Unit || '').trim() || null
      };
    }
  }
  if (!best) {
    return null;
  }
  const value = best.average ?? best.total ?? best.maximum ?? best.minimum ?? best.count;
  if (value === null) {
    return null;
  }
  return {
    ...best,
    value
  };
}

async function fetchAwsMetricLatestValue(cloudWatchClient, metricDefinition = {}) {
  const metricName = String(metricDefinition?.metricName || '').trim();
  const namespace = normalizeAwsMetricNamespace(metricDefinition?.namespace);
  if (!metricName) {
    return null;
  }

  const endAt = new Date();
  const startAt = new Date(endAt.getTime() - CLOUD_METRICS_AWS_LOOKBACK_MINUTES * 60 * 1000);
  const periodSeconds = Math.max(60, Math.min(300, Math.floor((CLOUD_METRICS_AWS_LOOKBACK_MINUTES * 60) / 5) * 60 || 60));

  const payload = await cloudWatchClient.send(
    new GetMetricStatisticsCommand({
      Namespace: namespace,
      MetricName: metricName,
      Dimensions: normalizeAwsMetricDimensions(metricDefinition?.dimensions).map((dimension) => ({
        Name: dimension.name,
        Value: dimension.value
      })),
      StartTime: startAt,
      EndTime: endAt,
      Period: periodSeconds,
      Statistics: ['Average', 'Minimum', 'Maximum', 'Sum', 'SampleCount']
    })
  );

  const latest = getLatestAwsDatapoint(payload?.Datapoints || []);
  if (!latest) {
    return null;
  }
  return {
    name: buildAwsMetricDisplayName(metricDefinition, selectAwsPrimaryDimension(metricDefinition?.dimensions || [])),
    namespace,
    unit: latest.unit,
    value: latest.value,
    average: latest.average,
    minimum: latest.minimum,
    maximum: latest.maximum,
    total: latest.total,
    count: latest.count,
    timestamp: latest.timestamp
  };
}

async function collectAwsCloudMetricsResources() {
  const accounts = parseAwsAccountsFromEnv();
  const fetchedAt = toIsoNow();
  const rows = [];
  let totalDefinitions = 0;
  let metricsErrorCount = 0;
  let discoveryErrorCount = 0;
  let regionCount = 0;
  let successfulRegionCount = 0;

  for (const account of accounts) {
    const regions = resolveAwsMetricRegionsForAccount(account);
    for (const region of regions) {
      regionCount += 1;
      const cloudWatchClient = createAwsCloudWatchClient(account, region);
      try {
        const metricDefinitions = await listAwsMetricDefinitions(cloudWatchClient, {
          maxMetrics: CLOUD_METRICS_AWS_MAX_METRICS_PER_ACCOUNT,
          namespaces: CLOUD_METRICS_AWS_NAMESPACES.length ? CLOUD_METRICS_AWS_NAMESPACES : null
        });
        successfulRegionCount += 1;
        totalDefinitions += metricDefinitions.length;

        const rowMap = new Map();
        for (const metricDefinition of metricDefinitions) {
          const descriptor = buildAwsCloudMetricResourceDescriptor(metricDefinition, account, region);
          const existing =
            rowMap.get(descriptor.resourceId) ||
            {
              provider: 'aws',
              resourceId: descriptor.resourceId,
              accountId: descriptor.accountId,
              subscriptionId: null,
              resourceGroup: null,
              location: descriptor.location,
              resourceType: descriptor.resourceType,
              resourceName: descriptor.resourceName,
              metrics: [],
              fetchedAt,
              metadata: {
                accountName: account.displayName,
                namespace: descriptor.namespace,
                metricNamespaces: [descriptor.namespace],
                definitionCount: 0,
                metricCount: 0,
                warnings: [],
                error: null
              },
              __metricDefinitions: []
            };

          existing.__metricDefinitions.push({
            ...metricDefinition,
            __displayName: descriptor.metricDisplayName,
            __namespace: descriptor.namespace,
            __primaryDimension: descriptor.primaryDimension
          });
          existing.metadata.definitionCount += 1;
          if (!existing.metadata.metricNamespaces.includes(descriptor.namespace)) {
            existing.metadata.metricNamespaces.push(descriptor.namespace);
          }
          rowMap.set(descriptor.resourceId, existing);
        }

        const tasks = [];
        for (const row of rowMap.values()) {
          for (const metricDefinition of row.__metricDefinitions) {
            tasks.push({
              row,
              metricDefinition
            });
          }
        }

        const results = await mapWithConcurrency(tasks, CLOUD_METRICS_AWS_CONCURRENCY, async (task) => {
          try {
            const latest = await fetchAwsMetricLatestValue(cloudWatchClient, task.metricDefinition);
            if (!latest) {
              return { task, latest: null, error: null };
            }
            return {
              task,
              latest: {
                ...latest,
                name: String(task.metricDefinition.__displayName || latest.name || 'Metric').trim().slice(0, 256)
              },
              error: null
            };
          } catch (error) {
            return {
              task,
              latest: null,
              error: error?.message || 'AWS metric read failed.'
            };
          }
        });

        for (const result of results) {
          if (!result?.task?.row) {
            continue;
          }
          if (result.error) {
            metricsErrorCount += 1;
            const warnings = Array.isArray(result.task.row.metadata.warnings)
              ? result.task.row.metadata.warnings
              : [];
            if (warnings.length < 5) {
              warnings.push(result.error);
            }
            result.task.row.metadata.warnings = warnings;
            if (!result.task.row.metadata.error) {
              result.task.row.metadata.error = result.error;
            }
            continue;
          }
          if (result.latest) {
            result.task.row.metrics.push(result.latest);
          }
        }

        for (const row of rowMap.values()) {
          delete row.__metricDefinitions;
          row.metrics.sort((left, right) => String(left?.name || '').localeCompare(String(right?.name || '')));
          row.metadata.metricCount = row.metrics.length;
          row.metadata.metricNamespaces = Array.isArray(row.metadata.metricNamespaces)
            ? row.metadata.metricNamespaces.slice(0, 32)
            : [];
          rows.push(row);
        }
      } catch (error) {
        discoveryErrorCount += 1;
        rows.push({
          provider: 'aws',
          resourceId: `aws://${account.accountId}/${region}/account/summary`,
          accountId: account.accountId,
          subscriptionId: null,
          resourceGroup: null,
          location: region,
          resourceType: 'aws/account/summary',
          resourceName: account.displayName || account.accountId,
          metrics: [],
          fetchedAt,
          metadata: {
            accountName: account.displayName,
            metricNamespaces: [],
            definitionCount: 0,
            metricCount: 0,
            warnings: [],
            error: error?.message || 'AWS metric discovery failed.'
          }
        });
      } finally {
        try {
          cloudWatchClient.destroy();
        } catch (_error) {
          // Ignore destroy errors.
        }
      }
    }
  }

  return {
    rows,
    summary: {
      accounts: accounts.length,
      regions: regionCount,
      successfulRegions: successfulRegionCount,
      resourcesProcessed: rows.length,
      metricDefinitions: totalDefinitions,
      metricsErrorCount,
      discoveryErrorCount,
      truncated: rows.length >= CLOUD_METRICS_AWS_MAX_METRICS_PER_ACCOUNT
    }
  };
}

async function syncAwsCloudMetrics(trigger = 'manual') {
  const fetchedAt = toIsoNow();
  const collection = await collectAwsCloudMetricsResources();
  if (Number(collection?.summary?.regions || 0) > 0 && Number(collection?.summary?.successfulRegions || 0) === 0) {
    const discoveryErrorCount = Number(collection?.summary?.discoveryErrorCount || 0);
    throw new Error(
      `AWS cloud metrics discovery failed for all configured regions (${discoveryErrorCount} region error${
        discoveryErrorCount === 1 ? '' : 's'
      }).`
    );
  }
  const replaceResult = replaceCloudMetricsLatest('aws', collection.rows, {
    fetchedAt,
    syncToken: crypto.randomUUID()
  });

  return {
    provider: 'aws',
    trigger,
    fetchedAt,
    upsertedCount: replaceResult.upsertedCount,
    staleDeletedCount: replaceResult.staleDeletedCount,
    ...collection.summary
  };
}

function shouldRetryRackspaceStatus(statusCode) {
  return [429, 500, 502, 503, 504].includes(Number(statusCode));
}

async function fetchRackspaceJsonWithRetry(url, options = {}, context = 'Rackspace request') {
  let attempt = 0;
  while (true) {
    let response;
    let text;
    try {
      const result = await fetchResponseTextWithTimeout(url, options, CLOUD_METRICS_RACKSPACE_TIMEOUT_MS);
      response = result.response;
      text = result.text;
    } catch (error) {
      if (attempt >= CLOUD_METRICS_RACKSPACE_MAX_RETRIES) {
        throw error;
      }
      const backoff = Math.min(8_000, 500 * 2 ** attempt + Math.floor(Math.random() * 250));
      await waitMs(backoff);
      attempt += 1;
      continue;
    }

    if (response.ok) {
      if (!text) {
        return {};
      }
      try {
        return JSON.parse(text);
      } catch (_error) {
        const parseError = new Error(`${context} returned a non-JSON response.`);
        parseError.statusCode = response.status;
        throw parseError;
      }
    }

    if (attempt < CLOUD_METRICS_RACKSPACE_MAX_RETRIES && shouldRetryRackspaceStatus(response.status)) {
      const retryAfterMs = parseRetryAfterMs(response.headers.get('retry-after'));
      const backoff = retryAfterMs > 0 ? retryAfterMs : Math.min(10_000, 700 * 2 ** attempt + Math.floor(Math.random() * 350));
      await waitMs(backoff);
      attempt += 1;
      continue;
    }

    const error = new Error(`${context} failed (${response.status}): ${String(text || '').slice(0, 600)}`);
    error.statusCode = response.status;
    throw error;
  }
}

function findRackspaceServiceEndpoint(serviceCatalog = [], serviceTypes = [], preferredRegion = null) {
  const targetTypes = new Set(
    (Array.isArray(serviceTypes) ? serviceTypes : [])
      .map((item) => String(item || '').trim().toLowerCase())
      .filter(Boolean)
  );
  const entries = Array.isArray(serviceCatalog) ? serviceCatalog : [];
  const service = entries.find((row) => {
    const type = String(row?.type || '').trim().toLowerCase();
    return targetTypes.has(type);
  });
  if (!service) {
    return null;
  }
  const endpoints = Array.isArray(service?.endpoints) ? service.endpoints : [];
  if (!endpoints.length) {
    return null;
  }
  const preferred = String(preferredRegion || '').trim().toUpperCase();
  if (preferred) {
    const regional = endpoints.find((endpoint) => String(endpoint?.region || '').trim().toUpperCase() === preferred);
    if (regional?.publicURL) {
      return String(regional.publicURL).trim();
    }
  }
  const fallbackEndpoint = endpoints.find((endpoint) => String(endpoint?.publicURL || '').trim()) || null;
  return fallbackEndpoint ? String(fallbackEndpoint.publicURL).trim() : null;
}

function normalizeRackspaceTenantId(value) {
  const text = String(value || '').trim();
  if (!text) {
    return null;
  }
  if (text.includes(':')) {
    return text;
  }
  return `hybrid:${text}`;
}

function resolveRackspaceMonitorBaseUrl(account = {}, tokenPayload = {}) {
  const serviceCatalog = Array.isArray(tokenPayload?.access?.serviceCatalog) ? tokenPayload.access.serviceCatalog : [];
  const catalogUrl = findRackspaceServiceEndpoint(serviceCatalog, ['rax:monitor'], account?.region);
  if (catalogUrl) {
    return String(catalogUrl).trim().replace(/\/+$/g, '');
  }
  const tenantId =
    normalizeRackspaceTenantId(tokenPayload?.access?.token?.tenant?.id) ||
    normalizeRackspaceTenantId(account?.accountNumber) ||
    normalizeRackspaceTenantId(account?.accountId) ||
    'hybrid:unknown';
  return `https://monitoring.api.rackspacecloud.com/v1.0/${tenantId}`;
}

function buildRackspaceMonitoringUrl(baseUrl, pathSuffix = '', query = {}) {
  const root = String(baseUrl || '').trim().replace(/\/+$/g, '');
  const suffix = String(pathSuffix || '').trim().replace(/^\/+/g, '');
  const url = new URL(`${root}/${suffix}`);
  for (const [key, value] of Object.entries(query || {})) {
    if (value === undefined || value === null || value === '') {
      continue;
    }
    url.searchParams.set(key, String(value));
  }
  return url.toString();
}

async function fetchRackspaceMetricsAuth(account = {}) {
  const identityUrl = String(account?.identityUrl || 'https://identity.api.rackspacecloud.com/v2.0/tokens')
    .trim()
    .replace(/\/+$/g, '');
  const payload = await fetchRackspaceJsonWithRetry(
    identityUrl,
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        auth: {
          'RAX-KSKEY:apiKeyCredentials': {
            username: account.username,
            apiKey: account.apiKey
          }
        }
      })
    },
    `Rackspace token request (${account.displayName || account.accountId || 'account'})`
  );

  const token = String(payload?.access?.token?.id || '').trim();
  if (!token) {
    throw new Error('Rackspace token response missing access token.');
  }
  return {
    token,
    tokenPayload: payload,
    monitorBaseUrl: resolveRackspaceMonitorBaseUrl(account, payload)
  };
}

function extractMarkerFromRackspaceMetadata(metadata = {}) {
  const marker = String(metadata?.next_marker || metadata?.nextMarker || '').trim();
  if (marker) {
    return marker;
  }
  const nextHref = String(metadata?.next_href || metadata?.nextHref || '').trim();
  if (!nextHref) {
    return null;
  }
  try {
    const url = new URL(nextHref);
    const hrefMarker = String(url.searchParams.get('marker') || '').trim();
    return hrefMarker || null;
  } catch (_error) {
    return null;
  }
}

async function listRackspaceMonitoringEntities(monitorBaseUrl, authToken) {
  const entities = [];
  let marker = null;
  let pageCount = 0;
  while (entities.length < CLOUD_METRICS_RACKSPACE_MAX_ENTITIES) {
    const url = buildRackspaceMonitoringUrl(monitorBaseUrl, 'entities', {
      limit: CLOUD_METRICS_RACKSPACE_PAGE_SIZE,
      ...(marker ? { marker } : {})
    });
    const payload = await fetchRackspaceJsonWithRetry(
      url,
      {
        headers: {
          'x-auth-token': authToken,
          accept: 'application/json'
        }
      },
      'Rackspace monitoring entities request'
    );
    const rows = Array.isArray(payload?.values) ? payload.values : [];
    if (!rows.length) {
      break;
    }
    for (const row of rows) {
      if (entities.length >= CLOUD_METRICS_RACKSPACE_MAX_ENTITIES) {
        break;
      }
      const entityId = String(row?.id || '').trim();
      if (!entityId) {
        continue;
      }
      entities.push({
        entityId,
        resourceName: String(row?.label || row?.id || '').trim() || entityId,
        uri: String(row?.uri || '').trim() || null,
        managed: Boolean(row?.managed),
        createdAt: Number.isFinite(Number(row?.created_at))
          ? new Date(Number(row.created_at)).toISOString()
          : null,
        updatedAt: Number.isFinite(Number(row?.updated_at))
          ? new Date(Number(row.updated_at)).toISOString()
          : null
      });
    }
    pageCount += 1;
    if (pageCount > 500) {
      break;
    }
    marker = extractMarkerFromRackspaceMetadata(payload?.metadata || {});
    if (!marker) {
      break;
    }
  }
  return entities;
}

async function listRackspaceChecksForEntity(monitorBaseUrl, authToken, entityId) {
  const url = buildRackspaceMonitoringUrl(monitorBaseUrl, `entities/${encodeURIComponent(entityId)}/checks`);
  try {
    const payload = await fetchRackspaceJsonWithRetry(
      url,
      {
        headers: {
          'x-auth-token': authToken,
          accept: 'application/json'
        }
      },
      `Rackspace checks request (${entityId})`
    );
    return Array.isArray(payload?.values) ? payload.values : [];
  } catch (error) {
    if ([403, 404].includes(Number(error?.statusCode))) {
      return [];
    }
    throw error;
  }
}

async function listRackspaceCheckMetricDefinitions(monitorBaseUrl, authToken, entityId, checkId) {
  const url = buildRackspaceMonitoringUrl(
    monitorBaseUrl,
    `entities/${encodeURIComponent(entityId)}/checks/${encodeURIComponent(checkId)}/metrics`
  );
  try {
    const payload = await fetchRackspaceJsonWithRetry(
      url,
      {
        headers: {
          'x-auth-token': authToken,
          accept: 'application/json'
        }
      },
      `Rackspace metric definitions request (${entityId}/${checkId})`
    );
    const rows = Array.isArray(payload?.values) ? payload.values : [];
    return rows
      .map((row) => ({
        name: String(row?.name || '').trim(),
        unit: String(row?.unit || '').trim() || null
      }))
      .filter((row) => row.name);
  } catch (error) {
    if ([403, 404].includes(Number(error?.statusCode))) {
      return [];
    }
    throw error;
  }
}

function getLatestRackspaceMetricSample(values = []) {
  const rows = Array.isArray(values) ? values : [];
  let best = null;
  for (const row of rows) {
    const timestampRaw = row?.timestamp;
    let timestampMs = NaN;
    if (Number.isFinite(Number(timestampRaw))) {
      timestampMs = Number(timestampRaw);
    } else {
      timestampMs = Date.parse(String(timestampRaw || '').trim());
    }
    if (!Number.isFinite(timestampMs)) {
      continue;
    }
    const average = toFiniteNumberOrNull(row?.average);
    const minimum = toFiniteNumberOrNull(row?.minimum ?? row?.min);
    const maximum = toFiniteNumberOrNull(row?.maximum ?? row?.max);
    const total = toFiniteNumberOrNull(row?.total ?? row?.sum);
    const count = toFiniteNumberOrNull(row?.numPoints ?? row?.count ?? row?.sampleCount);
    const value = average ?? maximum ?? minimum ?? total ?? count;
    if (value === null) {
      continue;
    }
    if (!best || timestampMs >= best.timestampMs) {
      best = {
        timestampMs,
        timestamp: new Date(timestampMs).toISOString(),
        value,
        average,
        minimum,
        maximum,
        total,
        count
      };
    }
  }
  return best;
}

async function fetchRackspaceMetricPlot(monitorBaseUrl, authToken, entityId, checkId, metricName) {
  const endAtMs = Date.now();
  const startAtMs = endAtMs - CLOUD_METRICS_RACKSPACE_LOOKBACK_MINUTES * 60 * 1000;
  const url = buildRackspaceMonitoringUrl(
    monitorBaseUrl,
    `entities/${encodeURIComponent(entityId)}/checks/${encodeURIComponent(checkId)}/metrics/${encodeURIComponent(metricName)}/plot`,
    {
      from: startAtMs,
      to: endAtMs,
      points: CLOUD_METRICS_RACKSPACE_PLOT_POINTS
    }
  );
  try {
    const payload = await fetchRackspaceJsonWithRetry(
      url,
      {
        headers: {
          'x-auth-token': authToken,
          accept: 'application/json'
        }
      },
      `Rackspace metric plot request (${entityId}/${checkId}/${metricName})`
    );
    const latest = getLatestRackspaceMetricSample(payload?.values || []);
    if (!latest) {
      return null;
    }
    return latest;
  } catch (error) {
    if ([403, 404].includes(Number(error?.statusCode))) {
      return null;
    }
    throw error;
  }
}

function normalizeRackspaceMetricNamespace(checkType) {
  const text = String(checkType || '').trim();
  return text ? `Rackspace.${text}` : 'Rackspace.Monitoring';
}

async function collectRackspaceCloudMetricsResources() {
  const accounts = parseRackspaceMetricsAccountsFromEnv();
  const fetchedAt = toIsoNow();
  const rows = [];
  let entityCount = 0;
  let checkCount = 0;
  let metricDefinitionCount = 0;
  let metricErrorCount = 0;
  let discoveryErrorCount = 0;
  let successfulAccountCount = 0;

  for (const account of accounts) {
    try {
      const authResult = await fetchRackspaceMetricsAuth(account);
      const monitorBaseUrl = authResult.monitorBaseUrl;
      const entities = await listRackspaceMonitoringEntities(monitorBaseUrl, authResult.token);
      successfulAccountCount += 1;
      entityCount += entities.length;

      const entityRows = await mapWithConcurrency(entities, CLOUD_METRICS_RACKSPACE_CONCURRENCY, async (entity) => {
        const entityWarnings = [];
        let checks = [];
        try {
          checks = await listRackspaceChecksForEntity(monitorBaseUrl, authResult.token, entity.entityId);
        } catch (error) {
          const message = error?.message || 'Rackspace check list request failed.';
          entityWarnings.push(message);
        }

        checkCount += checks.length;
        const metrics = [];
        await mapWithConcurrency(checks, Math.min(4, CLOUD_METRICS_RACKSPACE_CONCURRENCY), async (check) => {
          const checkId = String(check?.id || '').trim();
          if (!checkId) {
            return;
          }
          let definitions = [];
          try {
            definitions = await listRackspaceCheckMetricDefinitions(
              monitorBaseUrl,
              authResult.token,
              entity.entityId,
              checkId
            );
          } catch (error) {
            entityWarnings.push(error?.message || `Metric definition lookup failed for check ${checkId}.`);
            metricErrorCount += 1;
            return;
          }
          if (!definitions.length) {
            return;
          }

          const selectedDefinitions = definitions.slice(0, CLOUD_METRICS_RACKSPACE_MAX_METRICS_PER_CHECK);
          metricDefinitionCount += selectedDefinitions.length;
          const checkLabel = String(check?.label || check?.type || checkId).trim() || checkId;
          const checkType = String(check?.type || '').trim() || null;
          const metricResults = await mapWithConcurrency(
            selectedDefinitions,
            Math.min(6, CLOUD_METRICS_RACKSPACE_CONCURRENCY),
            async (definition) => {
              try {
                const latest = await fetchRackspaceMetricPlot(
                  monitorBaseUrl,
                  authResult.token,
                  entity.entityId,
                  checkId,
                  definition.name
                );
                return {
                  definition,
                  latest,
                  error: null
                };
              } catch (error) {
                return {
                  definition,
                  latest: null,
                  error: error?.message || `Metric plot request failed for ${definition.name}.`
                };
              }
            }
          );

          for (const result of metricResults) {
            if (result?.error) {
              metricErrorCount += 1;
              if (entityWarnings.length < 8) {
                entityWarnings.push(result.error);
              }
              continue;
            }
            if (!result?.latest) {
              continue;
            }
            metrics.push({
              name: `${checkLabel}: ${result.definition.name}`.slice(0, 256),
              namespace: normalizeRackspaceMetricNamespace(checkType),
              unit: result.definition.unit || null,
              value: result.latest.value,
              average: result.latest.average,
              minimum: result.latest.minimum,
              maximum: result.latest.maximum,
              total: result.latest.total,
              count: result.latest.count,
              timestamp: result.latest.timestamp
            });
          }
        });

        const accountRef = String(account.accountNumber || account.accountId || 'rackspace').trim();
        return {
          provider: 'rackspace',
          resourceId: `rackspace://${accountRef}/entity/${entity.entityId}`,
          accountId: accountRef,
          subscriptionId: null,
          resourceGroup: null,
          location: account.region || null,
          resourceType: 'rackspace/monitoring/entity',
          resourceName: entity.resourceName,
          metrics: metrics.sort((left, right) => String(left?.name || '').localeCompare(String(right?.name || ''))),
          fetchedAt,
          metadata: {
            accountName: account.displayName,
            entityId: entity.entityId,
            entityUri: entity.uri || null,
            managed: entity.managed,
            createdAt: entity.createdAt,
            updatedAt: entity.updatedAt,
            checkCount: checks.length,
            metricCount: metrics.length,
            warnings: entityWarnings.slice(0, 8),
            error: entityWarnings.length ? entityWarnings[0] : null
          }
        };
      });

      rows.push(...entityRows);
    } catch (error) {
      discoveryErrorCount += 1;
      const accountRef = String(account.accountNumber || account.accountId || 'rackspace').trim();
      rows.push({
        provider: 'rackspace',
        resourceId: `rackspace://${accountRef}/account/summary`,
        accountId: accountRef,
        subscriptionId: null,
        resourceGroup: null,
        location: account.region || null,
        resourceType: 'rackspace/account/summary',
        resourceName: account.displayName || accountRef,
        metrics: [],
        fetchedAt,
        metadata: {
          accountName: account.displayName || accountRef,
          metricCount: 0,
          warnings: [],
          error: error?.message || 'Rackspace metric discovery failed.'
        }
      });
    }
  }

  return {
    rows,
    summary: {
      accounts: accounts.length,
      successfulAccounts: successfulAccountCount,
      resourcesProcessed: rows.length,
      entities: entityCount,
      checks: checkCount,
      metricDefinitions: metricDefinitionCount,
      metricsErrorCount: metricErrorCount,
      discoveryErrorCount
    }
  };
}

async function syncRackspaceCloudMetrics(trigger = 'manual') {
  const fetchedAt = toIsoNow();
  const collection = await collectRackspaceCloudMetricsResources();
  if (Number(collection?.summary?.accounts || 0) > 0 && Number(collection?.summary?.successfulAccounts || 0) === 0) {
    const discoveryErrorCount = Number(collection?.summary?.discoveryErrorCount || 0);
    throw new Error(
      `Rackspace cloud metrics discovery failed for all configured accounts (${discoveryErrorCount} account error${
        discoveryErrorCount === 1 ? '' : 's'
      }).`
    );
  }
  const replaceResult = replaceCloudMetricsLatest('rackspace', collection.rows, {
    fetchedAt,
    syncToken: crypto.randomUUID()
  });
  return {
    provider: 'rackspace',
    trigger,
    fetchedAt,
    upsertedCount: replaceResult.upsertedCount,
    staleDeletedCount: replaceResult.staleDeletedCount,
    ...collection.summary
  };
}

function getCloudMetricsSyncProviders() {
  const providers = [];
  if (isAzureCloudMetricsAvailable()) {
    providers.push('azure');
  }
  if (isAwsCloudMetricsAvailable()) {
    providers.push('aws');
  }
  if (isRackspaceCloudMetricsAvailable()) {
    providers.push('rackspace');
  }
  return providers;
}

async function runCloudMetricsSync(trigger = 'scheduled', options = {}) {
  const fallbackProvider = trigger === 'manual' ? getDefaultCloudMetricsProvider() : 'all';
  const requestedRaw = String(options.provider || fallbackProvider).trim().toLowerCase();
  const requestedProvider =
    requestedRaw === 'all' ? 'all' : normalizeCloudMetricProvider(requestedRaw, getDefaultCloudMetricsProvider());
  const providersToSync = requestedProvider === 'all' ? getCloudMetricsSyncProviders() : [requestedProvider];

  if (!providersToSync.length) {
    return {
      started: false,
      reason: 'Cloud metrics credentials are not configured.'
    };
  }
  if (cloudMetricsSyncState.running) {
    return {
      started: false,
      reason: 'Cloud metrics sync is already running.'
    };
  }
  if (trigger !== 'manual' && !CLOUD_METRICS_SYNC_ENABLED) {
    return {
      started: false,
      reason: 'Cloud metrics auto-sync is disabled.'
    };
  }
  const unsupportedProvider = providersToSync.find((provider) => !['azure', 'aws', 'rackspace'].includes(provider));
  if (unsupportedProvider) {
    return {
      started: false,
      reason: `Cloud metrics sync is not implemented for provider "${unsupportedProvider}".`
    };
  }
  const missingProvider = providersToSync.find((provider) => {
    if (provider === 'azure') {
      return !isAzureCloudMetricsAvailable();
    }
    if (provider === 'aws') {
      return !isAwsCloudMetricsAvailable();
    }
    if (provider === 'rackspace') {
      return !isRackspaceCloudMetricsAvailable();
    }
    return true;
  });
  if (missingProvider) {
    cloudMetricsSyncState.lastStatus = 'skipped';
    cloudMetricsSyncState.lastError = `${String(
      missingProvider
    ).toUpperCase()} cloud metrics are disabled or credentials are not configured.`;
    cloudMetricsSyncState.lastFinishedAt = toIsoNow();
    cloudMetricsSyncState.lastProvider = missingProvider;
    return {
      started: false,
      reason: cloudMetricsSyncState.lastError
    };
  }

  cloudMetricsSyncState.running = true;
  cloudMetricsSyncState.lastStatus = 'running';
  cloudMetricsSyncState.lastError = null;
  cloudMetricsSyncState.lastStartedAt = toIsoNow();
  cloudMetricsSyncState.lastProvider = providersToSync.join(',');

  try {
    const summaries = [];
    for (const provider of providersToSync) {
      if (provider === 'azure') {
        summaries.push(await syncAzureCloudMetrics(trigger));
      } else if (provider === 'aws') {
        summaries.push(await syncAwsCloudMetrics(trigger));
      } else if (provider === 'rackspace') {
        summaries.push(await syncRackspaceCloudMetrics(trigger));
      }
    }
    const summary = summaries.length === 1 ? summaries[0] : { providers: summaries.map((row) => row.provider), summaries };
    cloudMetricsSyncState.lastStatus = 'ok';
    cloudMetricsSyncState.lastError = null;
    cloudMetricsSyncState.lastSummary = summary;
    return {
      started: true,
      summary
    };
  } catch (error) {
    cloudMetricsSyncState.lastStatus = 'error';
    cloudMetricsSyncState.lastError = error?.message || 'Cloud metrics sync failed.';
    cloudMetricsSyncState.lastSummary = null;
    throw error;
  } finally {
    cloudMetricsSyncState.running = false;
    cloudMetricsSyncState.lastFinishedAt = toIsoNow();
    if (CLOUD_METRICS_SYNC_ENABLED) {
      setCloudMetricsNextRun(Date.now());
    } else {
      cloudMetricsSyncState.nextRunAt = null;
    }
  }
}

function startCloudMetricsSync() {
  if (!CLOUD_METRICS_SYNC_ENABLED) {
    cloudMetricsSyncState.nextRunAt = null;
    return;
  }
  if (cloudMetricsSyncTimer) {
    return;
  }

  setCloudMetricsNextRun(Date.now());
  cloudMetricsSyncTimer = setInterval(() => {
    void runCloudMetricsSync('scheduled', { provider: 'all' }).catch((error) => {
      console.error('[cloudstudio] Cloud metrics scheduled sync failed', {
        message: error?.message || String(error)
      });
    });
  }, CLOUD_METRICS_SYNC_INTERVAL_MS);

  if (CLOUD_METRICS_SYNC_STARTUP_RUN) {
    setTimeout(() => {
      void runCloudMetricsSync('startup', { provider: 'all' }).catch((error) => {
        console.error('[cloudstudio] Cloud metrics startup sync failed', {
          message: error?.message || String(error)
        });
      });
    }, CLOUD_METRICS_SYNC_STARTUP_DELAY_MS);
  }
}

function stopCloudMetricsSync() {
  if (cloudMetricsSyncTimer) {
    clearInterval(cloudMetricsSyncTimer);
    cloudMetricsSyncTimer = null;
  }
  cloudMetricsSyncState.nextRunAt = null;
}

function getCloudMetricsServiceHealth() {
  const configuredProviders = getCloudMetricsSyncProviders();
  const configured = configuredProviders.length > 0;
  return {
    name: 'Cloud Metrics',
    enabled: CLOUD_METRICS_SYNC_ENABLED,
    configured,
    configuredProviders,
    running: cloudMetricsSyncState.running,
    nextRunAt: cloudMetricsSyncState.nextRunAt,
    lastStartedAt: cloudMetricsSyncState.lastStartedAt,
    lastFinishedAt: cloudMetricsSyncState.lastFinishedAt,
    lastStatus: cloudMetricsSyncState.lastStatus,
    lastError: cloudMetricsSyncState.lastError,
    intervalMinutes: Math.round((CLOUD_METRICS_SYNC_INTERVAL_MS / 60_000) * 100) / 100
  };
}

function normalizeCloudMetricResourceType(value) {
  const text = String(value || '')
    .trim()
    .toLowerCase();
  if (!text) {
    return 'unknown';
  }
  return text;
}

function getCloudMetricResourceTypeLabel(value) {
  const normalized = normalizeCloudMetricResourceType(value);
  return CLOUD_METRIC_RESOURCE_TYPE_LABELS[normalized] || humanizeCloudMetricResourceType(normalized);
}

function groupCloudMetricRows(rows = []) {
  const typeMap = new Map();
  for (const row of rows) {
    const typeKey = normalizeCloudMetricResourceType(row?.resourceType);
    const group =
      typeMap.get(typeKey) ||
      {
        resourceType: typeKey,
        resourceTypeLabel: getCloudMetricResourceTypeLabel(typeKey),
        resourceCount: 0,
        metricCount: 0,
        lastSyncAt: null,
        resources: []
      };
    const metrics = Array.isArray(row?.metrics) ? row.metrics : [];
    const fetchedAt = row?.fetchedAt || row?.updatedAt || null;

    group.resourceCount += 1;
    group.metricCount += metrics.length;
    if (fetchedAt && (!group.lastSyncAt || fetchedAt > group.lastSyncAt)) {
      group.lastSyncAt = fetchedAt;
    }

    group.resources.push({
      resourceId: row?.resourceId || null,
      resourceName: row?.resourceName || row?.resourceId || 'Resource',
      accountId: row?.accountId || row?.subscriptionId || null,
      subscriptionId: row?.subscriptionId || null,
      resourceGroup: row?.resourceGroup || null,
      location: row?.location || null,
      resourceType: typeKey,
      metrics,
      metricCount: metrics.length,
      fetchedAt,
      metadata: row?.metadata && typeof row.metadata === 'object' ? row.metadata : {}
    });

    typeMap.set(typeKey, group);
  }

  const groups = Array.from(typeMap.values());
  groups.forEach((group) => {
    group.resources.sort((left, right) =>
      String(left?.resourceName || left?.resourceId || '').localeCompare(String(right?.resourceName || right?.resourceId || ''))
    );
  });

  groups.sort((left, right) => {
    if (right.resourceCount !== left.resourceCount) {
      return right.resourceCount - left.resourceCount;
    }
    return String(left.resourceTypeLabel || left.resourceType).localeCompare(String(right.resourceTypeLabel || right.resourceType));
  });

  return groups;
}

function buildCloudMetricsView(providerRaw = 'azure') {
  const providers = buildCloudMetricsProviderCatalog();
  const availableIds = new Set(providers.map((item) => item.id));
  const preferredProvider = normalizeCloudMetricProvider(providerRaw, providers[0]?.id || 'azure');
  const provider = availableIds.has(preferredProvider) ? preferredProvider : providers[0]?.id || 'azure';

  const rows = listCloudMetricsLatest({
    provider,
    limit: 25_000
  });
  const typeGroups = groupCloudMetricRows(rows);
  const totalMetrics = typeGroups.reduce((sum, group) => sum + Number(group.metricCount || 0), 0);
  const lastSyncAt = rows.reduce((latest, row) => {
    const fetchedAt = String(row?.fetchedAt || '').trim();
    if (!fetchedAt) {
      return latest;
    }
    if (!latest || fetchedAt > latest) {
      return fetchedAt;
    }
    return latest;
  }, null);

  return {
    generatedAt: toIsoNow(),
    provider,
    providerLabel: CLOUD_METRIC_PROVIDER_LABELS[provider] || provider.toUpperCase(),
    providers,
    totalResources: rows.length,
    totalMetrics,
    lastSyncAt,
    typeGroups,
    syncStatus: getCloudMetricsSyncStatus()
  };
}

function normalizeCloudDatabaseSyncProvider(providerRaw = 'all') {
  const provider = normalizeCloudDatabaseProvider(providerRaw, 'all');
  if (provider === 'all') {
    return 'all';
  }
  if (provider === 'azure' || provider === 'aws' || provider === 'rackspace') {
    return provider;
  }
  return 'all';
}

function buildCloudDatabaseView(providerRaw = 'all') {
  const provider = normalizeCloudDatabaseProvider(providerRaw, 'all');
  const rows = listCloudMetricsLatest({
    limit: 25_000
  });
  const payload = buildCloudDatabaseViewFromCloudMetrics(rows, {
    provider,
    generatedAt: toIsoNow()
  });
  return {
    ...payload,
    syncStatus: getCloudMetricsSyncStatus()
  };
}

function buildForwardQueryString(query = {}) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query || {})) {
    if (value === undefined || value === null || value === '') {
      continue;
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        if (item === undefined || item === null || item === '') {
          continue;
        }
        params.append(key, String(item));
      }
      continue;
    }
    params.set(key, String(value));
  }
  return params.toString();
}

function createSecurityFindings(azureSecurity = [], awsBucketsByAccount = []) {
  const findings = [];

  for (const item of azureSecurity) {
    if (!item) {
      continue;
    }

    const accountName = item.account_name || item.accountId || item.account_id || 'Azure storage account';

    if (String(item.public_network_access || '').toLowerCase() !== 'disabled') {
      findings.push({
        provider: 'azure',
        severity: 'medium',
        resourceId: item.account_id || item.accountId || null,
        resourceName: accountName,
        rule: 'Public network access',
        status: 'open',
        detail: `Public network access is set to ${item.public_network_access || 'Unknown'}.`,
        observedAt: item.last_security_scan_at || null
      });
    }

    if (Number(item.allow_blob_public_access) === 1 || item.allow_blob_public_access === true) {
      findings.push({
        provider: 'azure',
        severity: 'high',
        resourceId: item.account_id || item.accountId || null,
        resourceName: accountName,
        rule: 'Allow Blob Public Access',
        status: 'open',
        detail: 'Blob public access is enabled.',
        observedAt: item.last_security_scan_at || null
      });
    }

    if (String(item.minimum_tls_version || '').toLowerCase() && String(item.minimum_tls_version).toLowerCase() !== 'tls1_2') {
      findings.push({
        provider: 'azure',
        severity: 'medium',
        resourceId: item.account_id || item.accountId || null,
        resourceName: accountName,
        rule: 'Minimum TLS Version',
        status: 'open',
        detail: `Minimum TLS version is ${item.minimum_tls_version}.`,
        observedAt: item.last_security_scan_at || null
      });
    }
  }

  for (const account of awsBucketsByAccount) {
    for (const bucket of account.buckets || []) {
      const accountId = account.accountId;
      const bucketName = bucket.bucket_name || bucket.name || 'S3 bucket';

      if (bucket.policy_is_public === true || bucket.policy_is_public === 1) {
        findings.push({
          provider: 'aws',
          severity: 'high',
          resourceId: bucket.bucket_name || bucket.name || null,
          resourceName: bucketName,
          accountId,
          rule: 'Public bucket policy',
          status: 'open',
          detail: 'Bucket policy is public.',
          observedAt: bucket.last_security_scan_at || bucket.last_sync_at || null
        });
      }

      if (bucket.encryption_enabled === false || bucket.encryption_enabled === 0) {
        findings.push({
          provider: 'aws',
          severity: 'high',
          resourceId: bucket.bucket_name || bucket.name || null,
          resourceName: bucketName,
          accountId,
          rule: 'Encryption at rest',
          status: 'open',
          detail: 'Default bucket encryption is disabled.',
          observedAt: bucket.last_security_scan_at || bucket.last_sync_at || null
        });
      }

      if (bucket.public_access_block_enabled === false || bucket.public_access_block_enabled === 0) {
        findings.push({
          provider: 'aws',
          severity: 'medium',
          resourceId: bucket.bucket_name || bucket.name || null,
          resourceName: bucketName,
          accountId,
          rule: 'Public access block',
          status: 'open',
          detail: 'Public access block is not fully enabled.',
          observedAt: bucket.last_security_scan_at || bucket.last_sync_at || null
        });
      }
    }
  }

  return findings;
}

async function loadPlatformData(options = {}) {
  const includeDetails = Boolean(options.includeDetails);
  const onlyVsax = Boolean(options.onlyVsax);
  const requestedVsaxGroupNames = parseStringValues(options.vsaxGroupNames || options.groupNames || []);
  const refreshVsax = Boolean(options.refreshVsax);

  if (onlyVsax) {
    const vsaxGroupsPath = refreshVsax ? '/api/vsax/groups?refreshCatalog=true' : '/api/vsax/groups';
    const vsaxPayload = await fetchStorage(vsaxGroupsPath).catch(() => ({ groups: [] }));

    const vsaxGroups = Array.isArray(vsaxPayload?.groups)
      ? vsaxPayload.groups
      : [];

    const vsaxAvailableGroups = Array.isArray(vsaxPayload?.availableGroups)
      ? vsaxPayload.availableGroups
          .map((item) => String(item?.group_name || item || '').trim())
          .filter(Boolean)
      : vsaxGroups
          .map((item) => String(item?.group_name || '').trim())
          .filter(Boolean);

    const vsaxSelectedGroups = requestedVsaxGroupNames.length
      ? requestedVsaxGroupNames
      : Array.from(new Set(vsaxAvailableGroups));

    const syncGroupNames = vsaxSelectedGroups.length ? vsaxSelectedGroups : vsaxAvailableGroups;
    if (refreshVsax && syncGroupNames.length) {
      await fetchStorage(
        '/api/vsax/sync',
        {
          method: 'POST',
          body: JSON.stringify({
            groupNames: syncGroupNames,
            force: true
          })
        },
        120_000
      ).catch(() => null);
    }

    const vsaxDevicesQuery = buildForwardQueryString({
      ...(syncGroupNames.length ? { groupNames: syncGroupNames } : {}),
      useSelected: false
    });
    const vsaxDevicesPath = vsaxDevicesQuery ? `/api/vsax/devices?${vsaxDevicesQuery}` : '/api/vsax/devices?useSelected=false';
    const vsaxDevicesPayload = await fetchStorage(vsaxDevicesPath).catch(() => ({ devices: [] }));
    const vsaxDevices = Array.isArray(vsaxDevicesPayload?.devices)
      ? vsaxDevicesPayload.devices
      : [];

    return {
      generatedAt: toIsoNow(),
      azureAccounts: [],
      azureSecurity: [],
      awsAccounts: [],
      awsBucketsByAccount: [],
      awsEfsByAccount: [],
      wasabiAccounts: [],
      wasabiBucketsByAccount: [],
      vsaxGroups,
      vsaxAvailableGroups,
      vsaxSelectedGroups,
      vsaxDevices
    };
  }

  const vsaxDevicesPath = '/api/vsax/devices';

  const [
    azureAccountsPayload,
    awsAccountsPayload,
    wasabiPayload,
    vsaxPayload,
    vsaxDevicesPayload
  ] = await Promise.all([
    fetchStorage('/api/storage-accounts').catch(() => ({ storageAccounts: [] })),
    fetchStorage('/api/aws/accounts').catch(() => ({ accounts: [] })),
    fetchStorage('/api/wasabi/accounts').catch(() => ({ accounts: [] })),
    fetchStorage('/api/vsax/groups').catch(() => ({ groups: [] })),
    fetchStorage(vsaxDevicesPath).catch(() => ({ devices: [] }))
  ]);

  const azureAccounts = Array.isArray(azureAccountsPayload?.storageAccounts)
    ? azureAccountsPayload.storageAccounts
    : [];

  const awsAccounts = Array.isArray(awsAccountsPayload?.accounts)
    ? awsAccountsPayload.accounts
    : [];

  const wasabiAccounts = Array.isArray(wasabiPayload?.accounts)
    ? wasabiPayload.accounts
    : [];

  const vsaxGroups = Array.isArray(vsaxPayload?.groups)
    ? vsaxPayload.groups
    : [];

  const vsaxAvailableGroups = Array.isArray(vsaxPayload?.availableGroups)
    ? vsaxPayload.availableGroups
        .map((item) => String(item?.group_name || item || '').trim())
        .filter(Boolean)
    : vsaxGroups
        .map((item) => String(item?.group_name || '').trim())
        .filter(Boolean);

  const vsaxSelectedGroups = Array.isArray(vsaxPayload?.selectedGroupNames)
    ? vsaxPayload.selectedGroupNames.map((item) => String(item || '').trim()).filter(Boolean)
    : [];

  const vsaxDevices = Array.isArray(vsaxDevicesPayload?.devices)
    ? vsaxDevicesPayload.devices
    : [];

  const azureSecurity = azureAccounts.length
    ? (
        await fetchStorage('/api/security/list', {
          method: 'POST',
          body: JSON.stringify({ accountIds: azureAccounts.map((account) => account.account_id) })
        }).catch(() => ({ security: [] }))
      ).security || []
    : [];

  const awsBucketsByAccount = includeDetails
    ? await Promise.all(
        awsAccounts.map(async (account) => {
          const accountId = String(account.account_id || '').trim();
          if (!accountId) {
            return { accountId: null, buckets: [] };
          }
          const payload = await fetchStorage(`/api/aws/buckets?accountId=${encodeURIComponent(accountId)}`).catch(() => ({ buckets: [] }));
          return {
            accountId,
            buckets: Array.isArray(payload?.buckets) ? payload.buckets : []
          };
        })
      )
    : [];

  const awsEfsByAccount = includeDetails
    ? await Promise.all(
        awsAccounts.map(async (account) => {
          const accountId = String(account.account_id || '').trim();
          if (!accountId) {
            return { accountId: null, fileSystems: [] };
          }
          const payload = await fetchStorage(`/api/aws/efs?accountId=${encodeURIComponent(accountId)}`).catch(() => ({ fileSystems: [] }));
          return {
            accountId,
            fileSystems: Array.isArray(payload?.fileSystems) ? payload.fileSystems : []
          };
        })
      )
    : [];

  const wasabiBucketsByAccount = includeDetails
    ? await Promise.all(
        wasabiAccounts.map(async (account) => {
          const accountId = String(account.account_id || '').trim();
          if (!accountId) {
            return { accountId: null, buckets: [] };
          }
          const payload = await fetchStorage(`/api/wasabi/buckets?accountId=${encodeURIComponent(accountId)}`).catch(() => ({ buckets: [] }));
          return {
            accountId,
            buckets: Array.isArray(payload?.buckets) ? payload.buckets : []
          };
        })
      )
    : [];

  return {
    generatedAt: toIsoNow(),
    azureAccounts,
    azureSecurity,
    awsAccounts,
    awsBucketsByAccount,
    awsEfsByAccount,
    wasabiAccounts,
    wasabiBucketsByAccount,
    vsaxGroups,
    vsaxAvailableGroups,
    vsaxSelectedGroups,
    vsaxDevices
  };
}

function buildOverviewFromPlatformData(data) {
  const azureAccounts = data.azureAccounts || [];
  const awsAccounts = data.awsAccounts || [];
  const wasabiAccounts = data.wasabiAccounts || [];
  const vsaxGroups = data.vsaxGroups || [];

  const providerRows = [
    {
      provider: 'azure',
      accountCount: azureAccounts.length,
      resourceCount: sumNumbers(azureAccounts.map((item) => item.container_count)),
      storageBytes: sumNumbers(azureAccounts.map((item) => item.metrics_used_capacity_bytes || item.total_size_bytes)),
      egressBytes24h: sumNumbers(azureAccounts.map((item) => item.metrics_egress_bytes_24h)),
      ingressBytes24h: sumNumbers(azureAccounts.map((item) => item.metrics_ingress_bytes_24h)),
      transactions24h: sumNumbers(azureAccounts.map((item) => item.metrics_transactions_24h)),
      lastSyncAt: azureAccounts.map((item) => item.metrics_last_scan_at || item.last_size_scan_at).filter(Boolean).sort().slice(-1)[0] || null
    },
    {
      provider: 'aws',
      accountCount: awsAccounts.length,
      resourceCount: sumNumbers(awsAccounts.map((item) => item.bucket_count)),
      storageBytes: sumNumbers(awsAccounts.map((item) => item.total_usage_bytes)),
      egressBytes24h: sumNumbers(awsAccounts.map((item) => item.total_egress_bytes_24h)),
      ingressBytes24h: sumNumbers(awsAccounts.map((item) => item.total_ingress_bytes_24h)),
      transactions24h: sumNumbers(awsAccounts.map((item) => item.total_transactions_24h)),
      lastSyncAt: awsAccounts.map((item) => item.last_sync_at || item.last_bucket_sync_at).filter(Boolean).sort().slice(-1)[0] || null
    },
    {
      provider: 'wasabi',
      accountCount: wasabiAccounts.length,
      resourceCount: sumNumbers(wasabiAccounts.map((item) => item.bucket_count)),
      storageBytes: sumNumbers(wasabiAccounts.map((item) => item.total_usage_bytes)),
      egressBytes24h: 0,
      ingressBytes24h: 0,
      transactions24h: 0,
      lastSyncAt: wasabiAccounts.map((item) => item.last_sync_at).filter(Boolean).sort().slice(-1)[0] || null
    },
    {
      provider: 'private',
      accountCount: vsaxGroups.length,
      resourceCount: sumNumbers(vsaxGroups.map((item) => item.device_count || item.disk_count)),
      storageBytes: sumNumbers(vsaxGroups.map((item) => item.total_used_bytes || 0)),
      egressBytes24h: 0,
      ingressBytes24h: 0,
      transactions24h: 0,
      lastSyncAt: vsaxGroups.map((item) => item.last_sync_at).filter(Boolean).sort().slice(-1)[0] || null
    }
  ];

  const totals = {
    accountCount: sumNumbers(providerRows.map((row) => row.accountCount)),
    resourceCount: sumNumbers(providerRows.map((row) => row.resourceCount)),
    storageBytes: sumNumbers(providerRows.map((row) => row.storageBytes)),
    egressBytes24h: sumNumbers(providerRows.map((row) => row.egressBytes24h)),
    ingressBytes24h: sumNumbers(providerRows.map((row) => row.ingressBytes24h)),
    transactions24h: sumNumbers(providerRows.map((row) => row.transactions24h))
  };

  return {
    generatedAt: data.generatedAt,
    providers: providerRows,
    totals
  };
}

async function buildLiveView(options = {}) {
  const requestedGroupNames = parseStringValues(options.groupNames || []);
  const refreshVsax = Boolean(options.refreshVsax);
  const data = await loadPlatformData({
    includeDetails: false,
    onlyVsax: true,
    vsaxGroupNames: requestedGroupNames,
    refreshVsax
  });
  const rows = [];

  for (const device of data.vsaxDevices || []) {
    const diskTotalBytes = Number(device.disk_total_bytes);
    const diskUsedBytes = Number(device.disk_used_bytes);
    const diskUsagePercent =
      Number.isFinite(diskTotalBytes) && diskTotalBytes > 0 && Number.isFinite(diskUsedBytes)
        ? (diskUsedBytes / diskTotalBytes) * 100
        : null;

    rows.push({
      provider: 'vsax',
      resourceName: device.device_name || device.device_id || 'Unknown device',
      resourceType: 'vsax-device',
      groupName: String(device.group_name || '').trim() || null,
      status: device.last_error ? 'warning' : 'healthy',
      accountId: device.group_name || null,
      updatedAt: device.last_sync_at || null,
      note: device.last_error || null,
      cpuUsagePercent: toFiniteNumberOrNull(device.cpu_usage),
      memoryUsagePercent: toFiniteNumberOrNull(device.memory_usage),
      diskUsagePercent: toFiniteNumberOrNull(diskUsagePercent),
      internalIp: String(device.internal_ip || '').trim() || null,
      publicIp: String(device.public_ip || '').trim() || null
    });
  }

  rows.sort((left, right) => {
    const leftGroup = String(left?.groupName || '');
    const rightGroup = String(right?.groupName || '');
    const groupCmp = leftGroup.localeCompare(rightGroup, undefined, { sensitivity: 'base', numeric: true });
    if (groupCmp !== 0) {
      return groupCmp;
    }
    return String(left?.resourceName || '').localeCompare(String(right?.resourceName || ''), undefined, {
      sensitivity: 'base',
      numeric: true
    });
  });

  const availableGroups = Array.from(new Set((data.vsaxAvailableGroups || []).map((item) => String(item || '').trim()).filter(Boolean))).sort(
    (left, right) =>
      String(left || '').localeCompare(String(right || ''), undefined, {
        sensitivity: 'base',
        numeric: true
      })
  );

  const selectedGroupNames = requestedGroupNames.length
    ? requestedGroupNames
    : availableGroups;

  return {
    generatedAt: data.generatedAt,
    totalRows: rows.length,
    availableGroups,
    selectedGroupNames,
    rows
  };
}

async function buildSecurityView() {
  const data = await loadPlatformData({ includeDetails: true });
  const findings = createSecurityFindings(data.azureSecurity || [], data.awsBucketsByAccount || []);

  const summary = {
    totalFindings: findings.length,
    critical: findings.filter((item) => item.severity === 'critical').length,
    high: findings.filter((item) => item.severity === 'high').length,
    medium: findings.filter((item) => item.severity === 'medium').length,
    low: findings.filter((item) => item.severity === 'low').length
  };

  return {
    generatedAt: data.generatedAt,
    summary,
    findings
  };
}

async function buildOverviewView() {
  const data = await loadPlatformData({ includeDetails: false });
  const overview = buildOverviewFromPlatformData(data);
  let billingSummary = [];
  try {
    const billingPayload = await fetchBilling('/api/billing?limit=1');
    billingSummary = Array.isArray(billingPayload?.summary) ? billingPayload.summary : [];
  } catch (_error) {
    billingSummary = [];
  }
  return {
    ...overview,
    billing: {
      summary: billingSummary
    },
    services: {
      ...managedServices.getStatus(),
      cloudMetrics: getCloudMetricsServiceHealth()
    },
    appUrls: {
      storage: getStorageBaseUrl(),
      pricing: getPriceBaseUrl(),
      billing: getBillingBaseUrl(),
      tag: getTagBaseUrl(),
      grafanaCloud: getGrafanaCloudBaseUrl(),
      firewall: getFirewallBaseUrl(),
      vpn: getFirewallBaseUrl(),
      cloudMetrics: '/api/platform/cloud-metrics',
      cloudDatabase: '/api/platform/cloud-database'
    }
  };
}

function shouldBypassAuth(req) {
  if (req.path === '/login' || req.path === '/favicon.ico') {
    return true;
  }
  if (req.path === '/docs' || req.path === '/docs/' || req.path.startsWith('/docs/')) {
    return true;
  }
  if (req.path === '/api' || req.path === '/api/') {
    return true;
  }
  if (req.path.startsWith('/api/auth/')) {
    return true;
  }
  if (req.path === '/api/health') {
    return true;
  }
  if (req.path === '/api/public' || req.path.startsWith('/api/public/')) {
    return true;
  }
  return false;
}

function isApiLikePath(reqPath) {
  return reqPath.startsWith('/api/') || reqPath.includes('/api/');
}

function normalizeViewName(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) {
    return '';
  }
  if (raw === 'ipmap') {
    return 'ip-address';
  }
  if (raw === 'utilization' || raw === 'cloudmetrics') {
    return 'cloud-metrics';
  }
  if (raw === 'clouddatabase') {
    return 'cloud-database';
  }
  if (raw === 'grafanacloud') {
    return 'grafana-cloud';
  }
  if (raw === 'billing-backfill') {
    return 'billing';
  }
  if (raw === 'storageunified') {
    return 'storage-unified';
  }
  if (raw === 'storageazure') {
    return 'storage-azure';
  }
  if (raw === 'storageaws') {
    return 'storage-aws';
  }
  if (raw === 'storagegcp') {
    return 'storage-gcp';
  }
  if (raw === 'storagewasabi') {
    return 'storage-wasabi';
  }
  if (raw === 'storagevsax') {
    return 'storage-vsax';
  }
  if (raw === 'storageother') {
    return 'storage-other';
  }
  return raw;
}

function hasRoutePrefix(pathname, prefix) {
  return pathname === prefix || pathname.startsWith(`${prefix}/`);
}

function resolveViewRule(reqPath) {
  const pathname = String(reqPath || '').trim();
  if (!pathname) {
    return null;
  }
  for (const rule of VIEW_RULES) {
    if (hasRoutePrefix(pathname, rule.prefix)) {
      return rule;
    }
  }
  return null;
}

function hasUserViewAccess(user, viewNameRaw) {
  const viewName = normalizeViewName(viewNameRaw);
  if (!viewName || !APP_VIEW_SET.has(viewName)) {
    return false;
  }
  if (viewName === 'storage') {
    if (hasViewAccess(user, 'storage')) {
      return true;
    }
    return [
      'storage-unified',
      'storage-azure',
      'storage-aws',
      'storage-gcp',
      'storage-wasabi',
      'storage-vsax',
      'storage-other'
    ].some((providerView) => hasViewAccess(user, providerView));
  }
  return hasViewAccess(user, viewName);
}

function isReadOnlyMethod(methodRaw) {
  const method = String(methodRaw || '').trim().toUpperCase();
  return method === 'GET' || method === 'HEAD' || method === 'OPTIONS';
}

function canUseStorageIpDependency(user, req) {
  if (!user || !isReadOnlyMethod(req?.method)) {
    return false;
  }
  if (!hasUserViewAccess(user, 'storage')) {
    return false;
  }
  const reqPath = String(req?.path || '').trim();
  if (!reqPath) {
    return false;
  }
  return (
    reqPath === '/api/ip-address' ||
    reqPath === '/api/ip-address/resolve' ||
    reqPath === '/api/ip-map' ||
    reqPath === '/api/ip-map/resolve'
  );
}

function canUseBillingTagReadDependency(user, req) {
  if (!user) {
    return false;
  }
  if (!hasUserViewAccess(user, 'billing')) {
    return false;
  }
  const method = String(req?.method || '').trim().toUpperCase();
  if (method !== 'POST') {
    return false;
  }
  const reqPath = String(req?.path || '').trim();
  return reqPath === '/api/tags/resolve';
}

function requireAdmin(req, res, next) {
  if (!req.authUser) {
    return res.status(401).json({ error: 'Authentication required.' });
  }
  if (!req.authUser.isAdmin) {
    return res.status(403).json({ error: 'Admin access required.' });
  }
  return next();
}

function buildViewCatalog() {
  return VIEW_KEYS.map((id) => ({
    id,
    label: VIEW_LABELS[id] || id
  }));
}

function parseAllowedViewsPayload(raw, options = {}) {
  const isAdmin = Boolean(options.isAdmin);
  const values = normalizeAllowedViewsInput(raw);
  if (isAdmin) {
    return [...VIEW_KEYS];
  }
  const adminOnlyViews = new Set(['vendors', 'admin-settings', 'admin-users', 'admin-api-keys', 'admin-backup']);
  const nonAdmin = values.filter((view) => !adminOnlyViews.has(view));
  if (!nonAdmin.length) {
    return null;
  }
  return nonAdmin;
}

function isInternalApiRequest(req) {
  const token = String(req.headers[INTERNAL_API_TOKEN_HEADER] || '').trim();
  return token.length > 0 && token === internalApiToken;
}

app.use(authSessionMiddleware);

app.get('/login', (req, res) => {
  if (req.authUser) {
    return res.redirect('/');
  }
  return res.sendFile(path.join(publicDir, 'login.html'));
});

app.get(['/api', '/api/'], (_req, res) => {
  return res.sendFile(path.join(publicDir, 'api-docs.html'));
});

app.get('/api/auth/me', (req, res) => {
  return res.json({
    authenticated: Boolean(req.authUser),
    user: toPublicUser(req.authUser),
    branding: APP_BRANDING
  });
});

app.post('/api/auth/login', (req, res) => {
  const result = authenticateCredentials(req.body?.username, req.body?.password);
  if (!result.ok) {
    return res.status(401).json({ error: result.reason });
  }

  loginUser(res, result.user);
  return res.json({
    authenticated: true,
    user: toPublicUser(result.user)
  });
});

app.post('/api/auth/logout', (req, res) => {
  logoutRequest(req, res);
  return res.json({
    authenticated: false,
    user: null
  });
});

app.get('/api/admin/app-config', requireAdmin, (_req, res) => {
  return res.json({
    schemaVersion: APP_CONFIG_SCHEMA_VERSION,
    runtimeConfig: toPublicRuntimeConfig(appRuntimeConfig || buildDefaultRuntimeConfigFromEnv()),
    dbBackupConfig: toPublicDbBackupUiConfig(dbBackupUiConfig || {}),
    billingAutoSync: getBillingAutoSyncStatus(),
    cloudMetrics: getCloudMetricsServiceHealth(),
    dbBackup: dbBackupScheduler.getStatus()
  });
});

app.put('/api/admin/app-config', requireAdmin, (req, res) => {
  const rawInput =
    req.body && typeof req.body === 'object' && req.body.runtimeConfig && typeof req.body.runtimeConfig === 'object'
      ? req.body.runtimeConfig
      : req.body && typeof req.body === 'object' && req.body.config && typeof req.body.config === 'object'
        ? req.body.config
        : req.body || {};
  const merged = normalizeRuntimeConfig(rawInput, appRuntimeConfig || buildDefaultRuntimeConfigFromEnv());
  const saved = saveStoredRuntimeConfig(merged, appRuntimeConfig || buildDefaultRuntimeConfigFromEnv());
  applyRuntimeConfig(saved, {
    reloadSchedulers: true,
    reason: 'admin-app-config-save'
  });

  return res.json({
    ok: true,
    runtimeConfig: toPublicRuntimeConfig(appRuntimeConfig || saved),
    billingAutoSync: getBillingAutoSyncStatus(),
    cloudMetrics: getCloudMetricsServiceHealth(),
    dbBackup: dbBackupScheduler.getStatus()
  });
});

app.get('/api/admin/app-config/export', requireAdmin, (_req, res) => {
  return res.json(buildAppConfigExportPayload());
});

app.post('/api/admin/app-config/import', requireAdmin, async (req, res) => {
  const parsed = normalizeAppConfigImportPayload(req.body || {});
  let runtimeApplied = false;
  let backupApplied = false;
  let vendorsImported = null;

  if (parsed.runtimeConfig) {
    const saved = saveStoredRuntimeConfig(parsed.runtimeConfig, appRuntimeConfig || buildDefaultRuntimeConfigFromEnv());
    applyRuntimeConfig(saved, {
      reloadSchedulers: true,
      reason: 'admin-app-config-import'
    });
    runtimeApplied = true;
  }

  if (parsed.dbBackupConfig) {
    const merged = normalizeDbBackupUiConfig(parsed.dbBackupConfig, dbBackupUiConfig || {});
    dbBackupUiConfig = saveStoredDbBackupUiConfig(merged);
    dbBackupScheduler.reloadConfig('admin-app-config-import');
    backupApplied = true;
  }

  if (parsed.vendors.length) {
    vendorsImported = await fetchJsonWithTimeout(
      `http://127.0.0.1:${port}/api/vendors/import`,
      {
        method: 'POST',
        headers: {
          [INTERNAL_API_TOKEN_HEADER]: internalApiToken
        },
        body: JSON.stringify({
          vendors: parsed.vendors
        })
      },
      180_000
    );
  }

  return res.json({
    ok: true,
    runtimeApplied,
    backupApplied,
    vendorImport: vendorsImported,
    runtimeConfig: toPublicRuntimeConfig(appRuntimeConfig || buildDefaultRuntimeConfigFromEnv()),
    dbBackupConfig: toPublicDbBackupUiConfig(dbBackupUiConfig || {}),
    billingAutoSync: getBillingAutoSyncStatus(),
    cloudMetrics: getCloudMetricsServiceHealth(),
    dbBackup: dbBackupScheduler.getStatus()
  });
});

app.get('/api/admin/users', requireAdmin, (_req, res) => {
  const users = listUsers().map((user) => toPublicUser(user));
  return res.json({
    users,
    viewCatalog: buildViewCatalog()
  });
});

app.post('/api/admin/users', requireAdmin, (req, res) => {
  const username = String(req.body?.username || '').trim().toLowerCase();
  const password = String(req.body?.password || '');
  const isAdmin = Boolean(req.body?.isAdmin);
  if (!username || !password) {
    return res.status(400).json({ error: 'Username and password are required.' });
  }

  const allowedViews = parseAllowedViewsPayload(req.body?.allowedViews, { isAdmin });
  if (!isAdmin && (!allowedViews || !allowedViews.length)) {
    return res.status(400).json({ error: 'Select at least one tab permission.' });
  }

  const result = createUser(username, password, {
    isAdmin,
    allowedViews
  });
  if (!result.ok) {
    return res.status(400).json({ error: result.reason || 'Could not create user.' });
  }
  return res.status(201).json({
    user: toPublicUser(result.user)
  });
});

app.put('/api/admin/users/:username', requireAdmin, (req, res) => {
  const targetUsername = String(req.params.username || '').trim().toLowerCase();
  if (!targetUsername) {
    return res.status(400).json({ error: 'username is required.' });
  }

  const users = listUsers();
  const target = users.find((user) => String(user.username || '').trim().toLowerCase() === targetUsername);
  if (!target) {
    return res.status(404).json({ error: 'User not found.' });
  }

  const isAdminProvided = Object.prototype.hasOwnProperty.call(req.body || {}, 'isAdmin');
  const nextIsAdmin = isAdminProvided ? Boolean(req.body?.isAdmin) : undefined;
  const effectiveIsAdmin = isAdminProvided ? nextIsAdmin : Boolean(target.isAdmin);
  const allowedViewsProvided = Object.prototype.hasOwnProperty.call(req.body || {}, 'allowedViews');
  const allowedViews = allowedViewsProvided
    ? parseAllowedViewsPayload(req.body?.allowedViews, { isAdmin: effectiveIsAdmin })
    : undefined;

  if (allowedViewsProvided && !effectiveIsAdmin && (!allowedViews || !allowedViews.length)) {
    return res.status(400).json({ error: 'Select at least one tab permission.' });
  }

  if (targetUsername === String(req.authUser?.username || '').trim().toLowerCase() && nextIsAdmin === false) {
    return res.status(400).json({ error: 'You cannot remove admin access from your own account.' });
  }

  if (nextIsAdmin === false && target.isAdmin && countAdminUsers() <= 1) {
    return res.status(400).json({ error: 'At least one admin user is required.' });
  }

  const updates = {};
  if (isAdminProvided) {
    updates.isAdmin = nextIsAdmin;
  }
  if (allowedViewsProvided) {
    updates.allowedViews = allowedViews;
  }
  if (Object.prototype.hasOwnProperty.call(req.body || {}, 'password')) {
    updates.password = String(req.body?.password || '');
  }

  const result = updateUser(targetUsername, updates);
  if (!result.ok) {
    const statusCode = result.reason === 'User not found.' ? 404 : 400;
    return res.status(statusCode).json({ error: result.reason || 'Could not update user.' });
  }
  return res.json({
    user: toPublicUser(result.user)
  });
});

app.delete('/api/admin/users/:username', requireAdmin, (req, res) => {
  const targetUsername = String(req.params.username || '').trim().toLowerCase();
  if (!targetUsername) {
    return res.status(400).json({ error: 'username is required.' });
  }
  const currentUsername = String(req.authUser?.username || '').trim().toLowerCase();
  if (targetUsername === currentUsername) {
    return res.status(400).json({ error: 'You cannot delete your own account.' });
  }

  const users = listUsers();
  const target = users.find((user) => String(user.username || '').trim().toLowerCase() === targetUsername);
  if (!target) {
    return res.status(404).json({ error: 'User not found.' });
  }
  if (target.isAdmin && countAdminUsers() <= 1) {
    return res.status(400).json({ error: 'At least one admin user is required.' });
  }

  const deleted = deleteUser(targetUsername);
  if (!deleted) {
    return res.status(404).json({ error: 'User not found.' });
  }
  return res.json({ ok: true });
});

app.get('/api/admin/db-backup/status', requireAdmin, (_req, res) => {
  return res.json({
    status: dbBackupScheduler.getStatus()
  });
});

app.get('/api/admin/db-backup/config', requireAdmin, (_req, res) => {
  return res.json({
    uiConfig: toPublicDbBackupUiConfig(dbBackupUiConfig || {}),
    status: dbBackupScheduler.getStatus()
  });
});

app.put('/api/admin/db-backup/config', requireAdmin, (req, res) => {
  const rawInput =
    req.body && typeof req.body === 'object' && req.body.config && typeof req.body.config === 'object'
      ? req.body.config
      : req.body || {};
  const merged = normalizeDbBackupUiConfig(rawInput, dbBackupUiConfig || {});
  dbBackupUiConfig = saveStoredDbBackupUiConfig(merged);
  const status = dbBackupScheduler.reloadConfig('admin-ui-config-update');
  return res.json({
    ok: true,
    uiConfig: toPublicDbBackupUiConfig(dbBackupUiConfig || {}),
    status
  });
});

app.post('/api/admin/db-backup/run', requireAdmin, async (_req, res) => {
  dbBackupScheduler.reloadConfig('manual-run');
  const result = await dbBackupScheduler.runBackupNow();
  if (!result?.ok && !result?.skipped) {
    return res.status(500).json({
      error: result?.error || 'DB backup failed.',
      result
    });
  }
  return res.json({
    ok: Boolean(result?.ok),
    skipped: Boolean(result?.skipped),
    result,
    status: dbBackupScheduler.getStatus()
  });
});

app.use((req, res, next) => {
  if (isInternalApiRequest(req)) {
    return next();
  }

  if (shouldBypassAuth(req)) {
    return next();
  }
  if (req.authUser) {
    const viewRule = resolveViewRule(req.path);
    if (viewRule?.view === 'ip-address' && !isReadOnlyMethod(req.method) && !req.authUser.isAdmin) {
      if (isApiLikePath(req.path)) {
        return res.status(403).json({
          error: 'IP address updates require admin access.'
        });
      }
      return res.status(403).send('Access denied.');
    }
    if (!viewRule || hasUserViewAccess(req.authUser, viewRule.view)) {
      return next();
    }
    if (viewRule.view === 'ip-address' && canUseStorageIpDependency(req.authUser, req)) {
      return next();
    }
    if (viewRule.view === 'tags' && canUseBillingTagReadDependency(req.authUser, req)) {
      return next();
    }
    if (isApiLikePath(req.path)) {
      return res.status(403).json({
        error: `Access denied for ${viewRule.view} view.`
      });
    }
    return res.status(403).send('Access denied.');
  }

  if (isApiLikePath(req.path)) {
    return res.status(401).json({ error: 'Authentication required.' });
  }
  return res.redirect('/login');
});

app.get(
  [
    '/apps/storage',
    '/apps/storage/',
    '/apps/pricing',
    '/apps/pricing/',
    '/apps/ip-address',
    '/apps/ip-address/',
    '/apps/billing',
    '/apps/billing/',
    '/apps/tag',
    '/apps/tag/',
    '/apps/cloud-database',
    '/apps/cloud-database/',
    '/apps/grafana-cloud',
    '/apps/grafana-cloud/',
    '/apps/firewall',
    '/apps/firewall/'
  ],
  (_req, res) => {
    res.redirect('/');
  }
);

managedServices.mountAll(app);

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    now: toIsoNow(),
    dbFile,
    services: managedServices.getStatus(),
    cloudMetrics: getCloudMetricsServiceHealth(),
    dbBackup: dbBackupScheduler.getStatus()
  });
});

app.get('/api/services', (_req, res) => {
  res.json({
    services: {
      ...managedServices.getStatus(),
      cloudMetrics: getCloudMetricsServiceHealth()
    },
    cloudMetrics: getCloudMetricsServiceHealth(),
    dbBackup: dbBackupScheduler.getStatus(),
    appUrls: {
      storage: getStorageBaseUrl(),
      pricing: getPriceBaseUrl(),
      ipAddress: getIpAddressBaseUrl(),
      billing: getBillingBaseUrl(),
      tag: getTagBaseUrl(),
      grafanaCloud: getGrafanaCloudBaseUrl(),
      firewall: getFirewallBaseUrl(),
      vpn: getFirewallBaseUrl(),
      cloudMetrics: '/api/platform/cloud-metrics',
      cloudDatabase: '/api/platform/cloud-database'
    }
  });
});

async function handleIpAddressList(_req, res, next) {
  try {
    const payload = await fetchIpAddress('/api/aliases');
    const aliases = Array.isArray(payload?.aliases) ? payload.aliases : [];
    res.json({
      aliases,
      total: aliases.length
    });
  } catch (error) {
    next(error);
  }
}

async function handleIpAddressImport(req, res, next) {
  try {
    const body = req.body || {};
    const rows = Array.isArray(body.rows) ? body.rows : [];
    const payload = await fetchIpAddress('/api/aliases/import', {
      method: 'POST',
      body: JSON.stringify({
        rows,
        replace: Boolean(body.replace)
      })
    });
    res.json(payload);
  } catch (error) {
    next(error);
  }
}

async function handleIpAddressResolve(req, res, next) {
  try {
    const rawIps = String(req.query.ip || req.query.ips || '').trim();
    const query = rawIps ? `?ips=${encodeURIComponent(rawIps)}` : '';
    const payload = await fetchIpAddress(`/api/resolve${query}`);
    res.json(payload);
  } catch (error) {
    next(error);
  }
}

async function handleIpAddressDiscoveryUnmapped(req, res, next) {
  try {
    const rawLimit = String(req.query.limit || '').trim();
    const query = rawLimit ? `?limit=${encodeURIComponent(rawLimit)}` : '';
    const payload = await fetchIpAddress(`/api/discovery/unmapped${query}`);
    res.json(payload);
  } catch (error) {
    next(error);
  }
}

async function handleIpAddressDiscoveryInherit(req, res, next) {
  try {
    const body = req.body || {};
    const payload = await fetchIpAddress('/api/discovery/inherit', {
      method: 'POST',
      body: JSON.stringify({
        ipAddress: body.ipAddress,
        serverName: body.serverName
      })
    });
    res.json(payload);
  } catch (error) {
    next(error);
  }
}

async function handleIpAddressExportCsv(_req, res, next) {
  try {
    const upstream = await fetchIpAddressRaw('/api/export/csv');

    if (!upstream.ok) {
      const raw = await upstream.text().catch(() => '');
      let message = `Request failed (${upstream.status})`;
      if (raw) {
        try {
          const parsed = JSON.parse(raw);
          if (parsed?.error) {
            message = parsed.error;
          }
        } catch (_error) {
          message = raw;
        }
      }
      return res.status(upstream.status).json({ error: message });
    }

    const disposition = upstream.headers.get('content-disposition') || 'attachment; filename="ip-address.csv"';
    const contentType = upstream.headers.get('content-type') || 'text/csv; charset=utf-8';
    const csv = await upstream.text();

    res.setHeader('content-disposition', disposition);
    res.setHeader('content-type', contentType);
    return res.send(csv);
  } catch (error) {
    next(error);
  }
}

app.get('/api/ip-address', handleIpAddressList);
app.post('/api/ip-address/import', handleIpAddressImport);
app.get('/api/ip-address/resolve', handleIpAddressResolve);
app.get('/api/ip-address/discovery/unmapped', handleIpAddressDiscoveryUnmapped);
app.post('/api/ip-address/discovery/inherit', handleIpAddressDiscoveryInherit);
app.get('/api/ip-address/export/csv', handleIpAddressExportCsv);

// Backward-compatible aliases.
app.get('/api/ip-map', handleIpAddressList);
app.post('/api/ip-map/import', handleIpAddressImport);
app.get('/api/ip-map/resolve', handleIpAddressResolve);
app.get('/api/ip-map/discovery/unmapped', handleIpAddressDiscoveryUnmapped);
app.post('/api/ip-map/discovery/inherit', handleIpAddressDiscoveryInherit);
app.get('/api/ip-map/export/csv', handleIpAddressExportCsv);

app.get('/api/vendors', (_req, res) => {
  const vendors = listVendors().map((vendor) => ({
    ...vendor,
    hidden: isVendorHidden(vendor)
  }));
  res.json({
    vendors,
    total: vendors.length
  });
});

app.get('/api/vendors/export', (_req, res) => {
  const vendors = listVendors()
    .map((vendor) => toVendorExportRecord(getVendorById(vendor.id)))
    .filter(Boolean);
  return res.json({
    schemaVersion: 'cloudstudio.vendor-config.v1',
    exportedAt: new Date().toISOString(),
    total: vendors.length,
    vendors
  });
});

app.get('/api/vendors/:vendorId/export', (req, res) => {
  const vendorId = String(req.params.vendorId || '').trim();
  if (!vendorId) {
    return res.status(400).json({ error: 'vendorId is required.' });
  }
  const vendor = getVendorById(vendorId);
  if (!vendor) {
    return res.status(404).json({ error: 'Vendor not found.' });
  }
  return res.json({
    schemaVersion: 'cloudstudio.vendor-config.v1',
    exportedAt: new Date().toISOString(),
    vendor: toVendorExportRecord(vendor)
  });
});

app.post('/api/vendors/import', (req, res) => {
  const entries = normalizeVendorImportEntries(req.body || {});
  if (!entries.length) {
    return res.status(400).json({ error: 'No vendor configs found in import payload.' });
  }

  const existingPublic = listVendors();
  const existingById = new Map(existingPublic.map((vendor) => [vendor.id, vendor]));
  const existingByFingerprint = new Map(
    existingPublic.map((vendor) => [buildVendorImportFingerprint(vendor), vendor])
  );

  const imported = [];
  const errors = [];
  let created = 0;
  let updated = 0;

  for (let index = 0; index < entries.length; index += 1) {
    const raw = entries[index];
    const source = raw && typeof raw === 'object' ? raw : {};
    const payload = parseVendorPayload(source);
    if (!payload.name) {
      errors.push({
        index,
        error: 'Vendor name is required.'
      });
      continue;
    }

    const candidateId = String(source.id || payload.id || '').trim();
    const fingerprint = buildVendorImportFingerprint(payload);
    const existingMatched =
      (candidateId && existingById.get(candidateId)) ||
      existingByFingerprint.get(fingerprint) ||
      null;

    const targetId = existingMatched?.id || candidateId || undefined;
    const existingRecord = targetId ? getVendorById(targetId) : null;
    const metadata = {
      ...(existingRecord?.metadata && typeof existingRecord.metadata === 'object' ? existingRecord.metadata : {}),
      ...(payload.metadata && typeof payload.metadata === 'object' ? payload.metadata : {})
    };
    if (payload.hidden !== null) {
      metadata.hidden = payload.hidden;
    }
    const credentialsEncrypted =
      source.credentials && typeof source.credentials === 'object'
        ? encryptJson(source.credentials)
        : typeof source.credentialsEncrypted === 'string' && source.credentialsEncrypted.trim()
          ? source.credentialsEncrypted.trim()
          : existingRecord?.credentialsEncrypted || null;

    const saved = upsertVendor({
      id: targetId,
      name: payload.name,
      provider: payload.provider,
      cloudType: payload.cloudType,
      authMethod: payload.authMethod,
      subscriptionId: payload.subscriptionId,
      accountId: payload.accountId,
      projectId: payload.projectId,
      metadata,
      credentialsEncrypted
    });

    if (existingMatched || existingRecord) {
      updated += 1;
    } else {
      created += 1;
    }

    const publicVendor = toPublicVendor(saved);
    imported.push(publicVendor);
    existingById.set(publicVendor.id, publicVendor);
    existingByFingerprint.set(buildVendorImportFingerprint(publicVendor), publicVendor);
  }

  return res.json({
    ok: errors.length === 0,
    totalReceived: entries.length,
    importedCount: imported.length,
    createdCount: created,
    updatedCount: updated,
    failedCount: errors.length,
    errors,
    vendors: imported
  });
});

app.post('/api/vendors/:vendorId/import', (req, res) => {
  const vendorId = String(req.params.vendorId || '').trim();
  if (!vendorId) {
    return res.status(400).json({ error: 'vendorId is required.' });
  }

  const existing = getVendorById(vendorId);
  if (!existing) {
    return res.status(404).json({ error: 'Vendor not found.' });
  }

  const entries = normalizeVendorImportEntries(req.body || {});
  if (!entries.length) {
    return res.status(400).json({ error: 'No vendor config found in import payload.' });
  }
  const source = entries[0] && typeof entries[0] === 'object' ? entries[0] : {};
  const payload = parseVendorPayload(source);

  const metadata = {
    ...(existing.metadata && typeof existing.metadata === 'object' ? existing.metadata : {}),
    ...(payload.metadata && typeof payload.metadata === 'object' ? payload.metadata : {})
  };
  if (payload.hidden !== null) {
    metadata.hidden = payload.hidden;
  }
  const credentialsEncrypted =
    source.credentials && typeof source.credentials === 'object'
      ? encryptJson(source.credentials)
      : typeof source.credentialsEncrypted === 'string' && source.credentialsEncrypted.trim()
        ? source.credentialsEncrypted.trim()
        : existing.credentialsEncrypted;

  const saved = upsertVendor({
    id: vendorId,
    name: payload.name || existing.name,
    provider: payload.provider || existing.provider,
    cloudType: payload.cloudType || existing.cloudType,
    authMethod: payload.authMethod || existing.authMethod,
    subscriptionId: payload.subscriptionId !== null ? payload.subscriptionId : existing.subscriptionId,
    accountId: payload.accountId !== null ? payload.accountId : existing.accountId,
    projectId: payload.projectId !== null ? payload.projectId : existing.projectId,
    metadata,
    credentialsEncrypted
  });

  return res.json({
    ok: true,
    vendor: toPublicVendor(saved)
  });
});

app.post('/api/vendors', (req, res) => {
  const payload = parseVendorPayload(req.body || {});
  if (!payload.name) {
    return res.status(400).json({ error: 'Vendor name is required.' });
  }
  const metadata = payload.metadata && typeof payload.metadata === 'object' ? { ...payload.metadata } : {};
  if (payload.hidden !== null) {
    metadata.hidden = payload.hidden;
  }

  const saved = upsertVendor({
    ...payload,
    metadata,
    credentialsEncrypted: payload.credentials ? encryptJson(payload.credentials) : null
  });

  return res.status(201).json({ vendor: toPublicVendor(saved) });
});

app.put('/api/vendors/:vendorId', (req, res) => {
  const vendorId = String(req.params.vendorId || '').trim();
  if (!vendorId) {
    return res.status(400).json({ error: 'vendorId is required.' });
  }

  const existing = getVendorById(vendorId);
  if (!existing) {
    return res.status(404).json({ error: 'Vendor not found.' });
  }

  const payload = parseVendorPayload(req.body || {});
  const metadata = {
    ...(existing.metadata && typeof existing.metadata === 'object' ? existing.metadata : {}),
    ...(payload.metadata && typeof payload.metadata === 'object' ? payload.metadata : {})
  };
  if (payload.hidden !== null) {
    metadata.hidden = payload.hidden;
  }
  const updated = upsertVendor({
    id: vendorId,
    name: payload.name || existing.name,
    provider: payload.provider || existing.provider,
    cloudType: payload.cloudType || existing.cloudType,
    authMethod: payload.authMethod || existing.authMethod,
    subscriptionId: payload.subscriptionId !== null ? payload.subscriptionId : existing.subscriptionId,
    accountId: payload.accountId !== null ? payload.accountId : existing.accountId,
    projectId: payload.projectId !== null ? payload.projectId : existing.projectId,
    metadata,
    credentialsEncrypted:
      payload.credentials && typeof payload.credentials === 'object'
        ? encryptJson(payload.credentials)
        : existing.credentialsEncrypted
  });

  return res.json({ vendor: toPublicVendor(updated) });
});

app.delete('/api/vendors/:vendorId', (req, res) => {
  const vendorId = String(req.params.vendorId || '').trim();
  if (!vendorId) {
    return res.status(400).json({ error: 'vendorId is required.' });
  }

  const deleted = deleteVendor(vendorId);
  if (!deleted) {
    return res.status(404).json({ error: 'Vendor not found.' });
  }

  return res.json({ ok: true });
});

app.post('/api/vendors/:vendorId/test', async (req, res) => {
  try {
    const vendorId = String(req.params.vendorId || '').trim();
    if (!vendorId) {
      return res.status(400).json({ error: 'vendorId is required.' });
    }

    const payload = await fetchBilling(`/api/vendors/${encodeURIComponent(vendorId)}/test`, {
      method: 'POST',
      body: JSON.stringify(req.body || {})
    });
    return res.json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({
      ok: false,
      error: error?.message || 'Vendor test failed.'
    });
  }
});

app.get('/api/billing', async (req, res) => {
  try {
    const queryString = buildForwardQueryString(req.query || {});
    const suffix = queryString ? `?${queryString}` : '';
    const payload = await fetchBilling(`/api/billing${suffix}`);
    return res.json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'Billing request failed.' });
  }
});

app.get('/api/billing/accounts', async (req, res) => {
  try {
    const queryString = buildForwardQueryString(req.query || {});
    const suffix = queryString ? `?${queryString}` : '';
    const payload = await fetchBilling(`/api/billing/accounts${suffix}`);
    return res.json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'Billing accounts request failed.' });
  }
});

app.get('/api/billing/budgets/accounts', async (req, res) => {
  try {
    const queryString = buildForwardQueryString(req.query || {});
    const suffix = queryString ? `?${queryString}` : '';
    const payload = await fetchBilling(`/api/billing/budgets/accounts${suffix}`);
    return res.json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'Billing account budgets request failed.' });
  }
});

app.post('/api/billing/budgets/accounts/save', async (req, res) => {
  try {
    const payload = await fetchBilling('/api/billing/budgets/accounts/save', {
      method: 'POST',
      body: JSON.stringify(req.body || {})
    });
    return res.json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'Billing account budget save failed.' });
  }
});

app.post('/api/billing/sync', async (req, res) => {
  try {
    const payload = await fetchBilling('/api/billing/sync', {
      method: 'POST',
      body: JSON.stringify(req.body || {})
    }, 120_000);
    return res.json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'Billing sync failed.' });
  }
});

app.post('/api/billing/rackspace/import-csv', async (req, res) => {
  try {
    const payload = await fetchBilling(
      '/api/billing/rackspace/import-csv',
      {
        method: 'POST',
        body: JSON.stringify(req.body || {})
      },
      120_000
    );
    return res.json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({
      error: error?.message || 'Rackspace CSV import failed.'
    });
  }
});

app.get('/api/billing/auto-sync-status', (_req, res) => {
  return res.json(getBillingAutoSyncStatus());
});

app.get('/api/billing/backfill/status', async (_req, res) => {
  try {
    const payload = await fetchBilling('/api/billing/backfill/status');
    return res.json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'Billing backfill status failed.' });
  }
});

app.post('/api/billing/backfill/ensure', async (req, res) => {
  try {
    const payload = await fetchBilling(
      '/api/billing/backfill/ensure',
      {
        method: 'POST',
        body: JSON.stringify(req.body || {})
      },
      120_000
    );
    return res.status(payload?.started ? 202 : 200).json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'Billing backfill ensure failed.' });
  }
});

app.post('/api/billing/backfill', async (req, res) => {
  try {
    const payload = await fetchBilling(
      '/api/billing/backfill',
      {
        method: 'POST',
        body: JSON.stringify(req.body || {})
      },
      120_000
    );
    return res.status(202).json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'Billing backfill start failed.' });
  }
});

app.get('/api/billing/export/csv', async (req, res) => {
  try {
    const queryString = buildForwardQueryString(req.query || {});
    const suffix = queryString ? `?${queryString}` : '';
    const upstream = await fetchBillingRaw(`/api/billing/export/csv${suffix}`);

    if (!upstream.ok) {
      const raw = await upstream.text().catch(() => '');
      let message = `Request failed (${upstream.status})`;
      if (raw) {
        try {
          const parsed = JSON.parse(raw);
          if (parsed?.error) {
            message = parsed.error;
          }
        } catch (_error) {
          message = raw;
        }
      }
      return res.status(upstream.status).json({ error: message });
    }

    const disposition = upstream.headers.get('content-disposition') || 'attachment; filename="billing-export.csv"';
    const contentType = upstream.headers.get('content-type') || 'text/csv; charset=utf-8';
    const csv = await upstream.text();

    res.setHeader('content-disposition', disposition);
    res.setHeader('content-type', contentType);
    return res.send(csv);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'Billing CSV export failed.' });
  }
});

app.get('/api/tags', async (req, res) => {
  try {
    const queryString = buildForwardQueryString(req.query || {});
    const suffix = queryString ? `?${queryString}` : '';
    const payload = await fetchTag(`/api/tags${suffix}`);
    return res.json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'Tags request failed.' });
  }
});

app.post('/api/tags/sync/aws', async (req, res) => {
  try {
    const payload = await fetchTag('/api/tags/sync/aws', {
      method: 'POST',
      body: JSON.stringify(req.body || {})
    }, 120_000);
    return res.json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'AWS tag sync failed.' });
  }
});

app.post('/api/tags/resolve', async (req, res) => {
  try {
    const payload = await fetchTag('/api/tags/resolve', {
      method: 'POST',
      body: JSON.stringify(req.body || {})
    }, 90_000);
    return res.json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'Tag resolve failed.' });
  }
});

app.put('/api/tags', async (req, res) => {
  try {
    const payload = await fetchTag('/api/tags', {
      method: 'PUT',
      body: JSON.stringify(req.body || {})
    }, 90_000);
    return res.json(payload);
  } catch (error) {
    return res.status(error?.statusCode || 500).json({ error: error?.message || 'Tag update failed.' });
  }
});

app.get('/api/keys', (_req, res) => {
  const keys = listApiKeys();
  return res.json({ keys, total: keys.length });
});

app.post('/api/keys', (req, res) => {
  const name = String(req.body?.name || '').trim() || 'Integration key';
  const plaintext = generateApiKey();
  const record = insertApiKey({
    name,
    keyHash: hashApiKey(plaintext),
    keyPrefix: plaintext.slice(0, 12)
  });

  return res.status(201).json({
    key: {
      ...record,
      apiKey: plaintext,
      masked: maskApiKey(plaintext)
    }
  });
});

app.patch('/api/keys/:keyId', (req, res) => {
  const keyId = String(req.params.keyId || '').trim();
  const isActive = Boolean(req.body?.isActive);
  const changed = setApiKeyActive(keyId, isActive);
  if (!changed) {
    return res.status(404).json({ error: 'API key not found.' });
  }

  return res.json({ ok: true });
});

app.delete('/api/keys/:keyId', (req, res) => {
  const keyId = String(req.params.keyId || '').trim();
  if (!keyId) {
    return res.status(400).json({ error: 'keyId is required.' });
  }
  const deleted = deleteApiKey(keyId);
  if (!deleted) {
    return res.status(404).json({ error: 'API key not found.' });
  }
  return res.json({ ok: true });
});

app.get('/api/platform/overview', async (_req, res, next) => {
  try {
    const data = await buildOverviewView();
    res.json(data);
  } catch (error) {
    next(error);
  }
});

registerCloudMetricsRoutes(app, {
  includePlatformRoutes: true,
  includePublicRoutes: false,
  normalizeProvider: normalizeCloudMetricProvider,
  getDefaultProvider: () => getDefaultCloudMetricsProvider(),
  buildView: (provider) => buildCloudMetricsView(provider),
  runSync: async (provider) => runCloudMetricsSync('manual', { provider })
});

registerCloudDatabaseRoutes(app, {
  includePlatformRoutes: true,
  includePublicRoutes: false,
  getDefaultProvider: () => 'all',
  buildView: (provider) => buildCloudDatabaseView(provider),
  runSync: async (provider) => runCloudMetricsSync('manual', { provider: normalizeCloudDatabaseSyncProvider(provider) })
});

app.get('/api/platform/live', async (req, res, next) => {
  try {
    const groupNames = parseStringValues(req.query?.groupNames ?? req.query?.groupName ?? []);
    const refresh = parseBooleanConfig(req.query?.refresh, false);
    const data = await buildLiveView({
      groupNames,
      refreshVsax: refresh
    });
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.get('/api/platform/security', async (_req, res, next) => {
  try {
    const data = await buildSecurityView();
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.get('/api/platform/pricing/options', async (_req, res, next) => {
  try {
    const payload = await fetchPrice('/api/options');
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.post('/api/platform/pricing/compare', async (req, res, next) => {
  try {
    const body = req.body || {};
    const payload = await fetchPrice('/api/compare', {
      method: 'POST',
      body: JSON.stringify(body)
    }, 90_000);

    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get('/api/platform/grafana-cloud/vendors', async (_req, res, next) => {
  try {
    const payload = await fetchGrafanaCloud('/api/vendors');
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get('/api/platform/grafana-cloud/status', async (_req, res, next) => {
  try {
    const payload = await fetchGrafanaCloud('/api/status');
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get('/api/platform/grafana-cloud/daily-ingest', async (req, res, next) => {
  try {
    const queryString = buildForwardQueryString(req.query || {});
    const suffix = queryString ? `?${queryString}` : '';
    const payload = await fetchGrafanaCloud(`/api/daily-ingest${suffix}`);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.post('/api/platform/grafana-cloud/sync', async (req, res, next) => {
  try {
    const payload = await fetchGrafanaCloud('/api/sync', {
      method: 'POST',
      body: JSON.stringify(req.body || {})
    }, 120_000);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get('/api/platform/firewall/status', async (_req, res, next) => {
  try {
    const payload = await fetchFirewall('/api/status');
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get('/api/platform/firewall', async (req, res, next) => {
  try {
    const queryString = buildForwardQueryString(req.query || {});
    const suffix = queryString ? `?${queryString}` : '';
    const payload = await fetchFirewall(`/api/firewalls${suffix}`);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.post('/api/platform/firewall/sync', async (req, res, next) => {
  try {
    const payload = await fetchFirewall(
      '/api/sync',
      {
        method: 'POST',
        body: JSON.stringify(req.body || {})
      },
      180_000
    );
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get('/api/platform/vpn', async (req, res, next) => {
  try {
    const queryString = buildForwardQueryString(req.query || {});
    const suffix = queryString ? `?${queryString}` : '';
    const payload = await fetchFirewall(`/api/vpn${suffix}`);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.put('/api/platform/vpn/metadata', async (req, res, next) => {
  try {
    const payload = await fetchFirewall('/api/vpn/metadata', {
      method: 'PUT',
      body: JSON.stringify(req.body || {})
    });
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.use('/api/public', requireApiKey);

app.get('/api/public/overview', async (_req, res, next) => {
  try {
    res.json(await buildOverviewView());
  } catch (error) {
    next(error);
  }
});

app.get('/api/public/billing', async (req, res, next) => {
  try {
    const queryString = buildForwardQueryString(req.query || {});
    const suffix = queryString ? `?${queryString}` : '';
    const payload = await fetchBilling(`/api/billing${suffix}`);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get('/api/public/grafana-cloud/vendors', async (_req, res, next) => {
  try {
    const payload = await fetchGrafanaCloud('/api/vendors');
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get('/api/public/grafana-cloud/status', async (_req, res, next) => {
  try {
    const payload = await fetchGrafanaCloud('/api/status');
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get('/api/public/grafana-cloud/daily-ingest', async (req, res, next) => {
  try {
    const queryString = buildForwardQueryString(req.query || {});
    const suffix = queryString ? `?${queryString}` : '';
    const payload = await fetchGrafanaCloud(`/api/daily-ingest${suffix}`);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get('/api/public/firewall', async (req, res, next) => {
  try {
    const queryString = buildForwardQueryString(req.query || {});
    const suffix = queryString ? `?${queryString}` : '';
    const payload = await fetchFirewall(`/api/firewalls${suffix}`);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get('/api/public/vpn', async (req, res, next) => {
  try {
    const queryString = buildForwardQueryString(req.query || {});
    const suffix = queryString ? `?${queryString}` : '';
    const payload = await fetchFirewall(`/api/vpn${suffix}`);
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

registerCloudMetricsRoutes(app, {
  includePlatformRoutes: false,
  includePublicRoutes: true,
  normalizeProvider: normalizeCloudMetricProvider,
  getDefaultProvider: () => getDefaultCloudMetricsProvider(),
  buildView: (provider) => buildCloudMetricsView(provider),
  runSync: async (provider) => runCloudMetricsSync('manual', { provider })
});

registerCloudDatabaseRoutes(app, {
  includePlatformRoutes: false,
  includePublicRoutes: true,
  getDefaultProvider: () => 'all',
  buildView: (provider) => buildCloudDatabaseView(provider),
  runSync: async (provider) => runCloudMetricsSync('manual', { provider: normalizeCloudDatabaseSyncProvider(provider) })
});

app.get('/api/public/security', async (_req, res, next) => {
  try {
    res.json(await buildSecurityView());
  } catch (error) {
    next(error);
  }
});

app.get(['/docs', '/docs/'], (_req, res) => {
  let files = [];
  try {
    files = fs
      .readdirSync(docsDir, { withFileTypes: true })
      .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith('.md'))
      .map((entry) => entry.name)
      .sort((a, b) => a.localeCompare(b));
  } catch (_error) {
    files = [];
  }

  const content = `
<h1>CloudStudio Docs</h1>
<p>Select a document:</p>
${
  files.length
    ? `<ul>${files
        .map((name) => `<li><a href="/docs/${encodeURIComponent(name)}">${escapeHtml(name)}</a></li>`)
        .join('')}</ul>`
    : '<p>No docs found.</p>'
}
  `.trim();

  return res.status(200).type('html').send(
    renderMarkdownPage({
      title: 'CloudStudio Docs',
      relativePath: '/docs',
      html: content
    })
  );
});

app.get(/^\/docs\/(.+\.md)$/i, (req, res, next) => {
  const requestedPath = req.params && req.params[0] ? String(req.params[0]) : '';
  const absolutePath = resolveDocsFilePath(requestedPath);
  if (!absolutePath) {
    return res.status(400).send('Invalid docs path.');
  }

  let stat = null;
  try {
    stat = fs.statSync(absolutePath);
  } catch (_error) {
    stat = null;
  }
  if (!stat || !stat.isFile()) {
    return next();
  }

  let markdownSource = '';
  try {
    markdownSource = fs.readFileSync(absolutePath, 'utf8');
  } catch (error) {
    return res.status(500).send(error?.message || 'Failed to load markdown file.');
  }

  const title = extractMarkdownTitle(markdownSource, path.basename(absolutePath));
  const html = markdown.render(markdownSource);
  return res
    .status(200)
    .type('html')
    .send(
      renderMarkdownPage({
        title,
        relativePath: `/docs/${requestedPath}`,
        html
      })
    );
});

app.use(express.static(publicDir));

app.use((req, res, next) => {
  if (req.path.startsWith('/api/')) {
    return next();
  }
  return res.sendFile(path.join(publicDir, 'index.html'));
});

app.use((error, req, res, _next) => {
  const status = error.statusCode || 500;
  const payload = {
    error: error?.message || 'Unexpected server error.'
  };
  if (error.payload && process.env.NODE_ENV !== 'production') {
    payload.details = error.payload;
  }

  if (status >= 500) {
    console.error('[cloudstudio:error]', {
      method: req.method,
      path: req.originalUrl,
      status,
      message: payload.error,
      stack: error?.stack || null
    });
  }

  res.status(status).json(payload);
});

async function start() {
  await managedServices.startAll();
  startBillingAutoSync();
  startCloudMetricsSync();
  dbBackupScheduler.start();

  const server = app.listen(port, () => {
    const services = managedServices.getStatus();
    console.log(`CloudStudio listening on http://localhost:${port}`);
    console.log('[cloudstudio] Managed services:', JSON.stringify(services, null, 2));
  });

  const shutdown = () => {
    stopBillingAutoSync();
    stopCloudMetricsSync();
    dbBackupScheduler.stop();
    managedServices.stopAll();
    server.close(() => {
      process.exit(0);
    });
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

start().catch((error) => {
  console.error('[cloudstudio] Failed to start', error);
  process.exit(1);
});
