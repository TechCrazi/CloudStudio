const fs = require('fs');
const path = require('path');
const { randomUUID } = require('crypto');
const Database = require('better-sqlite3');

const DEFAULT_DB_FILE = path.resolve(__dirname, '..', 'data', 'cloudstudio.db');
const dbFile = path.resolve(process.env.CLOUDSTUDIO_DB_FILE || DEFAULT_DB_FILE);

fs.mkdirSync(path.dirname(dbFile), { recursive: true });

const db = new Database(dbFile);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS vendors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    provider TEXT NOT NULL,
    cloud_type TEXT NOT NULL,
    auth_method TEXT NOT NULL,
    subscription_id TEXT,
    account_id TEXT,
    project_id TEXT,
    credentials_encrypted TEXT,
    metadata_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS billing_snapshots (
    id TEXT PRIMARY KEY,
    vendor_id TEXT,
    provider TEXT NOT NULL,
    period_start TEXT NOT NULL,
    period_end TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount REAL NOT NULL,
    source TEXT NOT NULL,
    raw_json TEXT,
    pulled_at TEXT NOT NULL,
    FOREIGN KEY (vendor_id) REFERENCES vendors(id) ON DELETE SET NULL
  );

  CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    key_prefix TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    last_used_at TEXT
  );

  CREATE TABLE IF NOT EXISTS sync_runs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    details_json TEXT
  );

  CREATE TABLE IF NOT EXISTS ip_aliases (
    ip_address TEXT PRIMARY KEY,
    server_name TEXT NOT NULL,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS resource_tags (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    resource_ref TEXT NOT NULL,
    vendor_id TEXT,
    account_id TEXT,
    tags_json TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'local',
    synced_at TEXT,
    updated_at TEXT NOT NULL,
    UNIQUE(provider, resource_ref),
    FOREIGN KEY (vendor_id) REFERENCES vendors(id) ON DELETE SET NULL
  );

  CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value_json TEXT,
    updated_at TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS cloud_metrics_latest (
    provider TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    account_id TEXT,
    subscription_id TEXT,
    resource_group TEXT,
    location TEXT,
    resource_type TEXT NOT NULL,
    resource_name TEXT,
    metrics_json TEXT NOT NULL,
    metadata_json TEXT,
    fetched_at TEXT NOT NULL,
    sync_token TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (provider, resource_id)
  );

  CREATE TABLE IF NOT EXISTS billing_budget_plans (
    id TEXT PRIMARY KEY,
    target_type TEXT NOT NULL,
    target_provider TEXT,
    target_id TEXT NOT NULL,
    target_key TEXT,
    target_label TEXT,
    vendor_id TEXT,
    account_id TEXT,
    budget_year INTEGER NOT NULL,
    budget_month INTEGER,
    currency TEXT NOT NULL,
    amount REAL NOT NULL,
    metadata_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (vendor_id) REFERENCES vendors(id) ON DELETE CASCADE
  );

  CREATE INDEX IF NOT EXISTS idx_vendors_provider ON vendors(provider);
  CREATE INDEX IF NOT EXISTS idx_billing_provider_period ON billing_snapshots(provider, period_start, period_end);
  CREATE INDEX IF NOT EXISTS idx_billing_vendor_period ON billing_snapshots(vendor_id, period_start, period_end);
  CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(is_active);
  CREATE INDEX IF NOT EXISTS idx_ip_aliases_name ON ip_aliases(server_name);
  CREATE INDEX IF NOT EXISTS idx_resource_tags_provider ON resource_tags(provider);
  CREATE INDEX IF NOT EXISTS idx_resource_tags_vendor ON resource_tags(vendor_id);
  CREATE INDEX IF NOT EXISTS idx_cloud_metrics_provider ON cloud_metrics_latest(provider);
  CREATE INDEX IF NOT EXISTS idx_cloud_metrics_provider_type ON cloud_metrics_latest(provider, resource_type);
  CREATE INDEX IF NOT EXISTS idx_billing_budget_type_year ON billing_budget_plans(target_type, budget_year);
  CREATE INDEX IF NOT EXISTS idx_billing_budget_vendor_year ON billing_budget_plans(vendor_id, budget_year);
  CREATE INDEX IF NOT EXISTS idx_billing_budget_provider_year ON billing_budget_plans(target_provider, budget_year);
`);

db.exec(`
  DELETE FROM billing_snapshots
  WHERE rowid NOT IN (
    SELECT MAX(rowid)
    FROM billing_snapshots
    GROUP BY COALESCE(vendor_id, ''), provider, period_start, period_end
  );
`);

db.exec(`
  CREATE UNIQUE INDEX IF NOT EXISTS idx_billing_vendor_provider_period_unique
  ON billing_snapshots(COALESCE(vendor_id, ''), provider, period_start, period_end);
`);

db.exec(`
  DELETE FROM billing_budget_plans
  WHERE rowid NOT IN (
    SELECT MAX(rowid)
    FROM billing_budget_plans
    GROUP BY
      target_type,
      COALESCE(target_provider, ''),
      target_id,
      COALESCE(target_key, ''),
      budget_year,
      COALESCE(budget_month, 0),
      currency
  );
`);

db.exec(`
  CREATE UNIQUE INDEX IF NOT EXISTS idx_billing_budget_plan_unique
  ON billing_budget_plans(
    target_type,
    COALESCE(target_provider, ''),
    target_id,
    COALESCE(target_key, ''),
    budget_year,
    COALESCE(budget_month, 0),
    currency
  );
`);

const upsertIpAliasStmt = db.prepare(`
  INSERT INTO ip_aliases (ip_address, server_name, updated_at)
  VALUES (?, ?, ?)
  ON CONFLICT(ip_address) DO UPDATE SET
    server_name=excluded.server_name,
    updated_at=excluded.updated_at
`);

const deleteAllIpAliasesStmt = db.prepare('DELETE FROM ip_aliases');

function nowIso() {
  return new Date().toISOString();
}

function normalizeIpAddress(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function safeJsonParse(raw, fallback) {
  if (!raw) {
    return fallback;
  }
  try {
    return JSON.parse(raw);
  } catch (_error) {
    return fallback;
  }
}

function normalizeCloudType(value) {
  return value === 'private' ? 'private' : 'public';
}

function normalizeProvider(value) {
  const v = String(value || '').trim().toLowerCase();
  if (v === 'wasabi-wacm') {
    return 'wasabi';
  }
  if (['azure', 'aws', 'gcp', 'rackspace', 'private', 'wasabi', 'wasabi-main', 'vsax', 'other'].includes(v)) {
    return v;
  }
  return 'other';
}

function normalizeTagKey(value) {
  return String(value || '')
    .trim()
    .slice(0, 256);
}

function normalizeTagValue(value) {
  return String(value === null || value === undefined ? '' : value)
    .trim()
    .slice(0, 2048);
}

function normalizeTagsObject(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    return {};
  }
  const tags = {};
  for (const [rawKey, rawValue] of Object.entries(input)) {
    const key = normalizeTagKey(rawKey);
    if (!key) {
      continue;
    }
    tags[key] = normalizeTagValue(rawValue);
  }
  return tags;
}

function normalizeBudgetTargetType(value) {
  const v = String(value || '').trim().toLowerCase();
  if (v === 'org' || v === 'product' || v === 'account') {
    return v;
  }
  return 'account';
}

function normalizeBudgetYear(value, fallback = new Date().getUTCFullYear()) {
  const numeric = Number(value);
  if (!Number.isInteger(numeric) || numeric < 1970 || numeric > 9999) {
    return Number(fallback);
  }
  return numeric;
}

function normalizeBudgetMonth(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  const numeric = Number(value);
  if (!Number.isInteger(numeric) || numeric < 1 || numeric > 12) {
    return null;
  }
  return numeric;
}

function listVendors() {
  const rows = db.prepare(`
    SELECT id, name, provider, cloud_type, auth_method, subscription_id, account_id, project_id,
           credentials_encrypted, metadata_json, created_at, updated_at
    FROM vendors
    ORDER BY provider, name COLLATE NOCASE
  `).all();

  return rows.map((row) => ({
    id: row.id,
    name: row.name,
    provider: row.provider,
    cloudType: row.cloud_type,
    authMethod: row.auth_method,
    subscriptionId: row.subscription_id,
    accountId: row.account_id,
    projectId: row.project_id,
    hasCredentials: Boolean(row.credentials_encrypted),
    metadata: safeJsonParse(row.metadata_json, {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at
  }));
}

function getVendorById(vendorId) {
  const row = db.prepare(`
    SELECT id, name, provider, cloud_type, auth_method, subscription_id, account_id, project_id,
           credentials_encrypted, metadata_json, created_at, updated_at
    FROM vendors
    WHERE id = ?
  `).get(vendorId);

  if (!row) {
    return null;
  }

  return {
    id: row.id,
    name: row.name,
    provider: row.provider,
    cloudType: row.cloud_type,
    authMethod: row.auth_method,
    subscriptionId: row.subscription_id,
    accountId: row.account_id,
    projectId: row.project_id,
    credentialsEncrypted: row.credentials_encrypted,
    metadata: safeJsonParse(row.metadata_json, {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

function upsertVendor(input) {
  const now = new Date().toISOString();
  const id = input.id || randomUUID();
  const existing = getVendorById(id);

  const payload = {
    id,
    name: String(input.name || '').trim() || 'Unnamed vendor',
    provider: normalizeProvider(input.provider),
    cloud_type: normalizeCloudType(input.cloudType),
    auth_method: String(input.authMethod || '').trim() || 'unknown',
    subscription_id: input.subscriptionId ? String(input.subscriptionId).trim() : null,
    account_id: input.accountId ? String(input.accountId).trim() : null,
    project_id: input.projectId ? String(input.projectId).trim() : null,
    credentials_encrypted:
      input.credentialsEncrypted !== undefined
        ? input.credentialsEncrypted || null
        : existing?.credentialsEncrypted || null,
    metadata_json: JSON.stringify(input.metadata && typeof input.metadata === 'object' ? input.metadata : {}),
    created_at: existing?.createdAt || now,
    updated_at: now
  };

  db.prepare(`
    INSERT INTO vendors (
      id, name, provider, cloud_type, auth_method, subscription_id, account_id,
      project_id, credentials_encrypted, metadata_json, created_at, updated_at
    ) VALUES (
      @id, @name, @provider, @cloud_type, @auth_method, @subscription_id, @account_id,
      @project_id, @credentials_encrypted, @metadata_json, @created_at, @updated_at
    )
    ON CONFLICT(id) DO UPDATE SET
      name=excluded.name,
      provider=excluded.provider,
      cloud_type=excluded.cloud_type,
      auth_method=excluded.auth_method,
      subscription_id=excluded.subscription_id,
      account_id=excluded.account_id,
      project_id=excluded.project_id,
      credentials_encrypted=excluded.credentials_encrypted,
      metadata_json=excluded.metadata_json,
      updated_at=excluded.updated_at
  `).run(payload);

  return getVendorById(id);
}

function deleteVendor(vendorId) {
  const info = db.prepare('DELETE FROM vendors WHERE id = ?').run(vendorId);
  return info.changes > 0;
}

function insertBillingSnapshot(input) {
  const now = new Date().toISOString();
  const id = input.id || randomUUID();
  const provider = normalizeProvider(input.provider);

  db.prepare(`
    INSERT OR REPLACE INTO billing_snapshots (
      id, vendor_id, provider, period_start, period_end, currency, amount, source, raw_json, pulled_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    id,
    input.vendorId || null,
    provider,
    input.periodStart,
    input.periodEnd,
    String(input.currency || 'USD').trim().toUpperCase(),
    Number(input.amount || 0),
    String(input.source || 'manual').trim() || 'manual',
    input.raw ? JSON.stringify(input.raw) : null,
    input.pulledAt || now
  );

  return getBillingSnapshotById(id);
}

function deleteBillingSnapshotsForVendorPeriod(input = {}) {
  const vendorId = String(input.vendorId || '').trim();
  const provider = normalizeProvider(input.provider);
  const periodStart = String(input.periodStart || '').trim();
  const periodEnd = String(input.periodEnd || '').trim();

  if (!vendorId || !periodStart || !periodEnd) {
    return 0;
  }

  const info = db.prepare(`
    DELETE FROM billing_snapshots
    WHERE vendor_id = ? AND provider = ? AND period_start = ? AND period_end = ?
  `).run(vendorId, provider, periodStart, periodEnd);

  return Number(info?.changes || 0);
}

function getBillingSnapshotById(snapshotId) {
  const row = db.prepare(`
    SELECT id, vendor_id, provider, period_start, period_end, currency, amount, source, raw_json, pulled_at
    FROM billing_snapshots
    WHERE id = ?
  `).get(snapshotId);

  if (!row) {
    return null;
  }

  return {
    id: row.id,
    vendorId: row.vendor_id,
    provider: row.provider,
    periodStart: row.period_start,
    periodEnd: row.period_end,
    currency: row.currency,
    amount: row.amount,
    source: row.source,
    raw: safeJsonParse(row.raw_json, null),
    pulledAt: row.pulled_at
  };
}

function listBillingSnapshots(filters = {}) {
  const where = [];
  const args = [];

  if (filters.provider) {
    where.push('provider = ?');
    args.push(normalizeProvider(filters.provider));
  }
  if (filters.vendorId) {
    where.push('vendor_id = ?');
    args.push(filters.vendorId);
  }
  if (filters.periodStart) {
    where.push('period_start >= ?');
    args.push(filters.periodStart);
  }
  if (filters.periodEnd) {
    where.push('period_end <= ?');
    args.push(filters.periodEnd);
  }

  const limit = Number.isFinite(Number(filters.limit))
    ? Math.min(500, Math.max(1, Number(filters.limit)))
    : 200;

  const query = `
    SELECT id, vendor_id, provider, period_start, period_end, currency, amount, source, raw_json, pulled_at
    FROM billing_snapshots
    ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
    ORDER BY pulled_at DESC
    LIMIT ${limit}
  `;

  const rows = db.prepare(query).all(...args);
  return rows.map((row) => ({
    id: row.id,
    vendorId: row.vendor_id,
    provider: row.provider,
    periodStart: row.period_start,
    periodEnd: row.period_end,
    currency: row.currency,
    amount: row.amount,
    source: row.source,
    raw: safeJsonParse(row.raw_json, null),
    pulledAt: row.pulled_at
  }));
}

function summarizeBillingByProvider(filters = {}) {
  const where = [];
  const args = [];

  if (filters.periodStart) {
    where.push('period_start >= ?');
    args.push(filters.periodStart);
  }
  if (filters.periodEnd) {
    where.push('period_end <= ?');
    args.push(filters.periodEnd);
  }

  const rows = db.prepare(`
    SELECT provider, currency, SUM(amount) AS total_amount, COUNT(*) AS snapshot_count
    FROM billing_snapshots
    ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
    GROUP BY provider, currency
    ORDER BY provider
  `).all(...args);

  return rows.map((row) => ({
    provider: row.provider,
    currency: row.currency,
    totalAmount: Number(row.total_amount || 0),
    snapshotCount: Number(row.snapshot_count || 0)
  }));
}

function listBillingBudgetPlans(filters = {}) {
  const where = [];
  const args = [];

  if (filters.targetType) {
    where.push('target_type = ?');
    args.push(normalizeBudgetTargetType(filters.targetType));
  }
  if (filters.targetProvider) {
    where.push('target_provider = ?');
    args.push(normalizeProvider(filters.targetProvider));
  }
  if (filters.targetId) {
    where.push('target_id = ?');
    args.push(String(filters.targetId || '').trim());
  }
  if (filters.budgetYear !== undefined && filters.budgetYear !== null && filters.budgetYear !== '') {
    where.push('budget_year = ?');
    args.push(normalizeBudgetYear(filters.budgetYear));
  } else {
    if (filters.budgetYearStart !== undefined && filters.budgetYearStart !== null && filters.budgetYearStart !== '') {
      where.push('budget_year >= ?');
      args.push(normalizeBudgetYear(filters.budgetYearStart));
    }
    if (filters.budgetYearEnd !== undefined && filters.budgetYearEnd !== null && filters.budgetYearEnd !== '') {
      where.push('budget_year <= ?');
      args.push(normalizeBudgetYear(filters.budgetYearEnd));
    }
  }

  const limit = Number.isFinite(Number(filters.limit))
    ? Math.min(50_000, Math.max(1, Number(filters.limit)))
    : 20_000;

  const rows = db
    .prepare(
      `
      SELECT
        id,
        target_type,
        target_provider,
        target_id,
        target_key,
        target_label,
        vendor_id,
        account_id,
        budget_year,
        budget_month,
        currency,
        amount,
        metadata_json,
        created_at,
        updated_at
      FROM billing_budget_plans
      ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
      ORDER BY target_type, target_provider, target_id, budget_year, budget_month, currency
      LIMIT ${limit}
    `
    )
    .all(...args);

  return rows.map((row) => ({
    id: row.id,
    targetType: row.target_type,
    targetProvider: row.target_provider,
    targetId: row.target_id,
    targetKey: row.target_key,
    targetLabel: row.target_label,
    vendorId: row.vendor_id,
    accountId: row.account_id,
    budgetYear: Number(row.budget_year),
    budgetMonth: row.budget_month === null || row.budget_month === undefined ? null : Number(row.budget_month),
    currency: String(row.currency || 'USD').trim().toUpperCase() || 'USD',
    amount: Number(row.amount || 0),
    metadata: safeJsonParse(row.metadata_json, {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at
  }));
}

function replaceBillingBudgetPlans(input = {}) {
  const targetType = normalizeBudgetTargetType(input.targetType || 'account');
  const budgetYear = normalizeBudgetYear(input.budgetYear);
  const entries = Array.isArray(input.entries) ? input.entries : [];
  const now = nowIso();

  const deleteStmt = db.prepare(`
    DELETE FROM billing_budget_plans
    WHERE target_type = ? AND budget_year = ?
  `);
  const insertStmt = db.prepare(`
    INSERT OR REPLACE INTO billing_budget_plans (
      id,
      target_type,
      target_provider,
      target_id,
      target_key,
      target_label,
      vendor_id,
      account_id,
      budget_year,
      budget_month,
      currency,
      amount,
      metadata_json,
      created_at,
      updated_at
    ) VALUES (
      @id,
      @target_type,
      @target_provider,
      @target_id,
      @target_key,
      @target_label,
      @vendor_id,
      @account_id,
      @budget_year,
      @budget_month,
      @currency,
      @amount,
      @metadata_json,
      @created_at,
      @updated_at
    )
  `);

  let inserted = 0;
  const tx = db.transaction(() => {
    deleteStmt.run(targetType, budgetYear);
    for (const entry of entries) {
      if (!entry || typeof entry !== 'object') {
        continue;
      }
      const targetId = String(entry.targetId || '').trim();
      if (!targetId) {
        continue;
      }
      const amount = Number(entry.amount);
      if (!Number.isFinite(amount) || amount < 0) {
        continue;
      }
      insertStmt.run({
        id: randomUUID(),
        target_type: targetType,
        target_provider: entry.targetProvider ? normalizeProvider(entry.targetProvider) : null,
        target_id: targetId,
        target_key: entry.targetKey ? String(entry.targetKey).trim().slice(0, 512) : null,
        target_label: entry.targetLabel ? String(entry.targetLabel).trim().slice(0, 512) : null,
        vendor_id: entry.vendorId ? String(entry.vendorId).trim().slice(0, 128) : null,
        account_id: entry.accountId ? String(entry.accountId).trim().slice(0, 512) : null,
        budget_year: budgetYear,
        budget_month: normalizeBudgetMonth(entry.budgetMonth),
        currency: String(entry.currency || 'USD').trim().toUpperCase() || 'USD',
        amount,
        metadata_json: JSON.stringify(entry.metadata && typeof entry.metadata === 'object' ? entry.metadata : {}),
        created_at: now,
        updated_at: now
      });
      inserted += 1;
    }
  });

  tx();

  return {
    targetType,
    budgetYear,
    inserted
  };
}

function upsertIpAddressAliases(rows = [], options = {}) {
  const entries = Array.isArray(rows) ? rows : [];
  const replace = Boolean(options?.replace);
  const updatedAt = nowIso();

  const tx = db.transaction((items, shouldReplace, timestamp) => {
    if (shouldReplace) {
      deleteAllIpAliasesStmt.run();
    }

    for (const row of items) {
      const ipAddress = normalizeIpAddress(row?.ipAddress || row?.ip_address || row?.ip || '');
      const serverName = String(row?.serverName || row?.server_name || '').trim();
      if (!ipAddress || !serverName) {
        continue;
      }
      upsertIpAliasStmt.run(ipAddress, serverName, timestamp);
    }
  });

  tx(entries, replace, updatedAt);
}

function listIpAddressAliases() {
  return db
    .prepare(
      `
      SELECT ip_address, server_name, updated_at
      FROM ip_aliases
      ORDER BY ip_address
    `
    )
    .all();
}

function resolveIpAddressAliases(ipAddresses = []) {
  const requested = Array.isArray(ipAddresses) ? ipAddresses : [];
  const normalized = requested.map((item) => normalizeIpAddress(item)).filter(Boolean);
  if (!normalized.length) {
    return [];
  }

  const unique = [...new Set(normalized)];
  const placeholders = unique.map(() => '?').join(',');
  const rows = db
    .prepare(
      `
      SELECT ip_address, server_name
      FROM ip_aliases
      WHERE ip_address IN (${placeholders})
    `
    )
    .all(...unique);

  const lookup = new Map(rows.map((row) => [String(row.ip_address || '').toLowerCase(), row.server_name || null]));
  return normalized.map((ipAddress) => ({
    ipAddress,
    serverName: lookup.get(ipAddress) || null
  }));
}

function insertApiKey(input) {
  const now = new Date().toISOString();
  const id = randomUUID();
  db.prepare(`
    INSERT INTO api_keys (id, name, key_hash, key_prefix, is_active, created_at, last_used_at)
    VALUES (?, ?, ?, ?, 1, ?, NULL)
  `).run(id, input.name, input.keyHash, input.keyPrefix, now);

  return {
    id,
    name: input.name,
    keyPrefix: input.keyPrefix,
    isActive: true,
    createdAt: now,
    lastUsedAt: null
  };
}

function listApiKeys() {
  const rows = db.prepare(`
    SELECT id, name, key_prefix, is_active, created_at, last_used_at
    FROM api_keys
    ORDER BY created_at DESC
  `).all();

  return rows.map((row) => ({
    id: row.id,
    name: row.name,
    keyPrefix: row.key_prefix,
    isActive: Number(row.is_active) === 1,
    createdAt: row.created_at,
    lastUsedAt: row.last_used_at
  }));
}

function setApiKeyActive(keyId, isActive) {
  const info = db.prepare(`
    UPDATE api_keys
    SET is_active = ?
    WHERE id = ?
  `).run(isActive ? 1 : 0, keyId);
  return info.changes > 0;
}

function deleteApiKey(keyId) {
  const id = String(keyId || '').trim();
  if (!id) {
    return false;
  }
  const info = db.prepare('DELETE FROM api_keys WHERE id = ?').run(id);
  return info.changes > 0;
}

function findApiKeyByHash(keyHash) {
  const row = db.prepare(`
    SELECT id, name, key_prefix, is_active, created_at, last_used_at
    FROM api_keys
    WHERE key_hash = ?
    LIMIT 1
  `).get(keyHash);

  if (!row) {
    return null;
  }

  return {
    id: row.id,
    name: row.name,
    keyPrefix: row.key_prefix,
    isActive: Number(row.is_active) === 1,
    createdAt: row.created_at,
    lastUsedAt: row.last_used_at
  };
}

function touchApiKeyUsage(keyId) {
  db.prepare('UPDATE api_keys SET last_used_at = ? WHERE id = ?').run(new Date().toISOString(), keyId);
}

function insertSyncRun(input) {
  const id = randomUUID();
  const now = new Date().toISOString();
  db.prepare(`
    INSERT INTO sync_runs (id, kind, status, started_at, finished_at, details_json)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(id, input.kind, input.status || 'started', input.startedAt || now, input.finishedAt || null, input.details ? JSON.stringify(input.details) : null);
  return id;
}

function finishSyncRun(runId, status, details) {
  db.prepare(`
    UPDATE sync_runs
    SET status = ?, finished_at = ?, details_json = ?
    WHERE id = ?
  `).run(status, new Date().toISOString(), details ? JSON.stringify(details) : null, runId);
}

function getResourceTag(provider, resourceRef) {
  const normalizedProvider = normalizeProvider(provider);
  const normalizedRef = String(resourceRef || '').trim();
  if (!normalizedRef) {
    return null;
  }

  const row = db
    .prepare(
      `
      SELECT id, provider, resource_ref, vendor_id, account_id, tags_json, source, synced_at, updated_at
      FROM resource_tags
      WHERE provider = ? AND resource_ref = ?
      LIMIT 1
    `
    )
    .get(normalizedProvider, normalizedRef);

  if (!row) {
    return null;
  }

  return {
    id: row.id,
    provider: row.provider,
    resourceRef: row.resource_ref,
    vendorId: row.vendor_id,
    accountId: row.account_id,
    tags: safeJsonParse(row.tags_json, {}),
    source: row.source || 'local',
    syncedAt: row.synced_at || null,
    updatedAt: row.updated_at
  };
}

function upsertResourceTag(input = {}) {
  const provider = normalizeProvider(input.provider);
  const resourceRef = String(input.resourceRef || '').trim();
  if (!resourceRef) {
    return null;
  }
  const now = nowIso();
  const existing = getResourceTag(provider, resourceRef);
  const id = existing?.id || randomUUID();
  const tags = normalizeTagsObject(input.tags || {});
  const vendorId = input.vendorId ? String(input.vendorId).trim() : existing?.vendorId || null;
  const accountId = input.accountId ? String(input.accountId).trim() : existing?.accountId || null;
  const source = String(input.source || existing?.source || 'local').trim() || 'local';
  const syncedAt = input.syncedAt !== undefined ? (input.syncedAt || null) : existing?.syncedAt || null;

  db.prepare(
    `
    INSERT INTO resource_tags (id, provider, resource_ref, vendor_id, account_id, tags_json, source, synced_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(provider, resource_ref) DO UPDATE SET
      vendor_id = excluded.vendor_id,
      account_id = excluded.account_id,
      tags_json = excluded.tags_json,
      source = excluded.source,
      synced_at = excluded.synced_at,
      updated_at = excluded.updated_at
  `
  ).run(
    id,
    provider,
    resourceRef,
    vendorId,
    accountId,
    JSON.stringify(tags),
    source,
    syncedAt,
    now
  );

  return getResourceTag(provider, resourceRef);
}

function listResourceTags(filters = {}) {
  const where = [];
  const args = [];

  if (filters.provider) {
    where.push('provider = ?');
    args.push(normalizeProvider(filters.provider));
  }
  if (filters.vendorId) {
    where.push('vendor_id = ?');
    args.push(String(filters.vendorId).trim());
  }
  if (filters.accountId) {
    where.push('account_id = ?');
    args.push(String(filters.accountId).trim());
  }
  if (filters.resourceRef) {
    where.push('resource_ref = ?');
    args.push(String(filters.resourceRef).trim());
  }

  const limit = Number.isFinite(Number(filters.limit))
    ? Math.min(5000, Math.max(1, Number(filters.limit)))
    : 1000;

  const rows = db
    .prepare(
      `
      SELECT id, provider, resource_ref, vendor_id, account_id, tags_json, source, synced_at, updated_at
      FROM resource_tags
      ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
      ORDER BY updated_at DESC
      LIMIT ${limit}
    `
    )
    .all(...args);

  return rows.map((row) => ({
    id: row.id,
    provider: row.provider,
    resourceRef: row.resource_ref,
    vendorId: row.vendor_id,
    accountId: row.account_id,
    tags: safeJsonParse(row.tags_json, {}),
    source: row.source || 'local',
    syncedAt: row.synced_at || null,
    updatedAt: row.updated_at
  }));
}

function normalizeCloudMetricsResourceType(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .slice(0, 256) || 'unknown';
}

function normalizeCloudMetricsResourceId(value) {
  return String(value || '')
    .trim()
    .slice(0, 2048);
}

function normalizeCloudMetricArray(input) {
  if (!Array.isArray(input)) {
    return [];
  }
  return input
    .map((metric) => {
      if (!metric || typeof metric !== 'object') {
        return null;
      }
      const name = String(metric.name || metric.metric || '').trim().slice(0, 256);
      if (!name) {
        return null;
      }
      const value = Number(metric.value);
      const average = Number(metric.average);
      const minimum = Number(metric.minimum);
      const maximum = Number(metric.maximum);
      const total = Number(metric.total);
      const count = Number(metric.count);
      return {
        name,
        namespace: String(metric.namespace || metric.metricNamespace || '').trim().slice(0, 256) || null,
        unit: String(metric.unit || '').trim().slice(0, 64) || null,
        value: Number.isFinite(value) ? value : null,
        average: Number.isFinite(average) ? average : null,
        minimum: Number.isFinite(minimum) ? minimum : null,
        maximum: Number.isFinite(maximum) ? maximum : null,
        total: Number.isFinite(total) ? total : null,
        count: Number.isFinite(count) ? count : null,
        timestamp: metric.timestamp ? String(metric.timestamp).trim().slice(0, 64) : null
      };
    })
    .filter(Boolean);
}

function replaceCloudMetricsLatest(providerRaw, rows = [], options = {}) {
  const provider = normalizeProvider(providerRaw);
  const fetchedAt = String(options.fetchedAt || nowIso()).trim() || nowIso();
  const syncToken = String(options.syncToken || randomUUID()).trim() || randomUUID();
  const now = nowIso();
  const entries = Array.isArray(rows) ? rows : [];

  const upsertStmt = db.prepare(`
    INSERT INTO cloud_metrics_latest (
      provider,
      resource_id,
      account_id,
      subscription_id,
      resource_group,
      location,
      resource_type,
      resource_name,
      metrics_json,
      metadata_json,
      fetched_at,
      sync_token,
      created_at,
      updated_at
    ) VALUES (
      @provider,
      @resource_id,
      @account_id,
      @subscription_id,
      @resource_group,
      @location,
      @resource_type,
      @resource_name,
      @metrics_json,
      @metadata_json,
      @fetched_at,
      @sync_token,
      @created_at,
      @updated_at
    )
    ON CONFLICT(provider, resource_id) DO UPDATE SET
      account_id = excluded.account_id,
      subscription_id = excluded.subscription_id,
      resource_group = excluded.resource_group,
      location = excluded.location,
      resource_type = excluded.resource_type,
      resource_name = excluded.resource_name,
      metrics_json = excluded.metrics_json,
      metadata_json = excluded.metadata_json,
      fetched_at = excluded.fetched_at,
      sync_token = excluded.sync_token,
      updated_at = excluded.updated_at
  `);

  const deleteStaleStmt = db.prepare(`
    DELETE FROM cloud_metrics_latest
    WHERE provider = ? AND sync_token != ?
  `);

  const deleteAllStmt = db.prepare(`
    DELETE FROM cloud_metrics_latest
    WHERE provider = ?
  `);

  let upsertedCount = 0;
  let staleDeletedCount = 0;

  const tx = db.transaction(() => {
    for (const row of entries) {
      const resourceId = normalizeCloudMetricsResourceId(row?.resourceId || row?.resource_id);
      if (!resourceId) {
        continue;
      }
      const metrics = normalizeCloudMetricArray(row?.metrics);
      const metadata = row?.metadata && typeof row.metadata === 'object' ? row.metadata : {};
      upsertStmt.run({
        provider,
        resource_id: resourceId,
        account_id: row?.accountId ? String(row.accountId).trim().slice(0, 512) : null,
        subscription_id: row?.subscriptionId ? String(row.subscriptionId).trim().slice(0, 512) : null,
        resource_group: row?.resourceGroup ? String(row.resourceGroup).trim().slice(0, 512) : null,
        location: row?.location ? String(row.location).trim().slice(0, 256) : null,
        resource_type: normalizeCloudMetricsResourceType(row?.resourceType || row?.resource_type),
        resource_name: row?.resourceName ? String(row.resourceName).trim().slice(0, 512) : null,
        metrics_json: JSON.stringify(metrics),
        metadata_json: JSON.stringify(metadata),
        fetched_at: row?.fetchedAt ? String(row.fetchedAt).trim().slice(0, 64) : fetchedAt,
        sync_token: syncToken,
        created_at: now,
        updated_at: now
      });
      upsertedCount += 1;
    }

    if (upsertedCount > 0) {
      const info = deleteStaleStmt.run(provider, syncToken);
      staleDeletedCount = Number(info?.changes || 0);
    } else {
      const info = deleteAllStmt.run(provider);
      staleDeletedCount = Number(info?.changes || 0);
    }
  });

  tx();

  return {
    provider,
    upsertedCount,
    staleDeletedCount,
    fetchedAt
  };
}

function listCloudMetricsLatest(filters = {}) {
  const where = [];
  const args = [];

  if (filters.provider) {
    where.push('provider = ?');
    args.push(normalizeProvider(filters.provider));
  }
  if (filters.resourceType) {
    where.push('resource_type = ?');
    args.push(normalizeCloudMetricsResourceType(filters.resourceType));
  }

  const limit = Number.isFinite(Number(filters.limit))
    ? Math.min(25_000, Math.max(1, Number(filters.limit)))
    : 5_000;

  const rows = db
    .prepare(
      `
      SELECT
        provider,
        resource_id,
        account_id,
        subscription_id,
        resource_group,
        location,
        resource_type,
        resource_name,
        metrics_json,
        metadata_json,
        fetched_at,
        updated_at
      FROM cloud_metrics_latest
      ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
      ORDER BY provider, resource_type, resource_name COLLATE NOCASE, resource_id
      LIMIT ${limit}
    `
    )
    .all(...args);

  return rows.map((row) => ({
    provider: row.provider,
    resourceId: row.resource_id,
    accountId: row.account_id,
    subscriptionId: row.subscription_id,
    resourceGroup: row.resource_group,
    location: row.location,
    resourceType: row.resource_type,
    resourceName: row.resource_name,
    metrics: safeJsonParse(row.metrics_json, []),
    metadata: safeJsonParse(row.metadata_json, {}),
    fetchedAt: row.fetched_at,
    updatedAt: row.updated_at
  }));
}

function summarizeCloudMetricsProviders() {
  const rows = db
    .prepare(
      `
      SELECT
        provider,
        COUNT(*) AS resource_count,
        MAX(fetched_at) AS last_sync_at
      FROM cloud_metrics_latest
      GROUP BY provider
      ORDER BY provider
    `
    )
    .all();

  return rows.map((row) => ({
    provider: row.provider,
    resourceCount: Number(row.resource_count || 0),
    lastSyncAt: row.last_sync_at || null
  }));
}

function getAppSetting(keyRaw) {
  const key = String(keyRaw || '').trim();
  if (!key) {
    return null;
  }
  const row = db
    .prepare(
      `
      SELECT key, value_json, updated_at
      FROM app_settings
      WHERE key = ?
      LIMIT 1
    `
    )
    .get(key);
  if (!row) {
    return null;
  }
  return {
    key: row.key,
    value: safeJsonParse(row.value_json, null),
    updatedAt: row.updated_at
  };
}

function upsertAppSetting(keyRaw, value) {
  const key = String(keyRaw || '').trim();
  if (!key) {
    return null;
  }
  const updatedAt = nowIso();
  const valueJson = value === undefined ? null : JSON.stringify(value);
  db.prepare(
    `
    INSERT INTO app_settings (key, value_json, updated_at)
    VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET
      value_json = excluded.value_json,
      updated_at = excluded.updated_at
  `
  ).run(key, valueJson, updatedAt);
  return getAppSetting(key);
}

function deleteAppSetting(keyRaw) {
  const key = String(keyRaw || '').trim();
  if (!key) {
    return false;
  }
  const info = db.prepare('DELETE FROM app_settings WHERE key = ?').run(key);
  return info.changes > 0;
}

module.exports = {
  db,
  dbFile,
  listVendors,
  getVendorById,
  upsertVendor,
  deleteVendor,
  insertBillingSnapshot,
  deleteBillingSnapshotsForVendorPeriod,
  listBillingSnapshots,
  summarizeBillingByProvider,
  listBillingBudgetPlans,
  replaceBillingBudgetPlans,
  upsertIpAddressAliases,
  listIpAddressAliases,
  resolveIpAddressAliases,
  insertApiKey,
  listApiKeys,
  setApiKeyActive,
  deleteApiKey,
  findApiKeyByHash,
  touchApiKeyUsage,
  insertSyncRun,
  finishSyncRun,
  getResourceTag,
  upsertResourceTag,
  listResourceTags,
  replaceCloudMetricsLatest,
  listCloudMetricsLatest,
  summarizeCloudMetricsProviders,
  getAppSetting,
  upsertAppSetting,
  deleteAppSetting
};
