const express = require('express');
const { db } = require('../../db');

const app = express();
app.set('etag', false);
app.use(express.json({ limit: '2mb' }));

const IPV4_REGEX = /\b(?:25[0-5]|2[0-4]\d|1?\d{1,2})(?:\.(?:25[0-5]|2[0-4]\d|1?\d{1,2})){3}\b/g;
const DEFAULT_CHECKMK_HOSTS = Object.freeze(['FW10', 'FW11', 'FW3']);
const SERVICE_STATE_LABELS = Object.freeze({
  0: 'healthy',
  1: 'warning',
  2: 'critical',
  3: 'unknown'
});

let syncTimer = null;
let workersStarted = false;

const syncState = {
  running: false,
  enabled: true,
  intervalMs: 5 * 60 * 1000,
  nextRunAt: null,
  lastStartedAt: null,
  lastFinishedAt: null,
  lastStatus: null,
  lastError: null,
  lastSummary: null
};

function ensureSchema() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS firewall_checkmk_services_latest (
      host_id TEXT NOT NULL,
      service_description TEXT NOT NULL,
      service_key TEXT NOT NULL,
      gateway_ip TEXT,
      is_vpn INTEGER NOT NULL DEFAULT 0,
      state_code INTEGER,
      state_label TEXT,
      summary_text TEXT,
      perf_data TEXT,
      transmitting INTEGER,
      raw_json TEXT,
      fetched_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (host_id, service_description)
    );

    CREATE TABLE IF NOT EXISTS firewall_vpn_metadata (
      gateway_ip TEXT PRIMARY KEY,
      vpn_number TEXT,
      account_tags_json TEXT,
      notes TEXT,
      updated_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_fw_services_host ON firewall_checkmk_services_latest(host_id);
    CREATE INDEX IF NOT EXISTS idx_fw_services_vpn ON firewall_checkmk_services_latest(is_vpn, gateway_ip);
    CREATE INDEX IF NOT EXISTS idx_fw_services_state ON firewall_checkmk_services_latest(state_code);
  `);
}

ensureSchema();

const upsertServiceStmt = db.prepare(`
  INSERT INTO firewall_checkmk_services_latest (
    host_id,
    service_description,
    service_key,
    gateway_ip,
    is_vpn,
    state_code,
    state_label,
    summary_text,
    perf_data,
    transmitting,
    raw_json,
    fetched_at,
    updated_at
  ) VALUES (
    @host_id,
    @service_description,
    @service_key,
    @gateway_ip,
    @is_vpn,
    @state_code,
    @state_label,
    @summary_text,
    @perf_data,
    @transmitting,
    @raw_json,
    @fetched_at,
    @updated_at
  )
  ON CONFLICT(host_id, service_description) DO UPDATE SET
    service_key=excluded.service_key,
    gateway_ip=excluded.gateway_ip,
    is_vpn=excluded.is_vpn,
    state_code=excluded.state_code,
    state_label=excluded.state_label,
    summary_text=excluded.summary_text,
    perf_data=excluded.perf_data,
    transmitting=excluded.transmitting,
    raw_json=excluded.raw_json,
    fetched_at=excluded.fetched_at,
    updated_at=excluded.updated_at
`);

const upsertVpnMetadataStmt = db.prepare(`
  INSERT INTO firewall_vpn_metadata (
    gateway_ip,
    vpn_number,
    account_tags_json,
    notes,
    updated_at
  ) VALUES (?, ?, ?, ?, ?)
  ON CONFLICT(gateway_ip) DO UPDATE SET
    vpn_number=excluded.vpn_number,
    account_tags_json=excluded.account_tags_json,
    notes=excluded.notes,
    updated_at=excluded.updated_at
`);

function nowIso() {
  return new Date().toISOString();
}

function parseBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) {
    return false;
  }
  return fallback;
}

function parsePositiveInteger(value, fallback, min = 1, max = 60 * 60 * 1000) {
  const parsed = Number.parseInt(String(value ?? '').trim(), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(min, Math.min(max, parsed));
}

function parseStringList(value, fallback = []) {
  const raw = String(value || '').trim();
  if (!raw) {
    return [...fallback];
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

  const seen = new Set();
  return values
    .map((item) => String(item || '').trim())
    .filter(Boolean)
    .filter((item) => {
      const key = item.toUpperCase();
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });
}

function safeText(value, maxLength = 4096) {
  return String(value === null || value === undefined ? '' : value)
    .trim()
    .slice(0, maxLength);
}

function normalizeGatewayIp(value) {
  const text = safeText(value, 64).toLowerCase();
  const match = text.match(/^(?:25[0-5]|2[0-4]\d|1?\d{1,2})(?:\.(?:25[0-5]|2[0-4]\d|1?\d{1,2})){3}$/);
  return match ? match[0] : '';
}

function extractIpv4(value) {
  const matches = String(value || '').match(IPV4_REGEX) || [];
  return matches.length ? normalizeGatewayIp(matches[0]) : '';
}

function extractAllIpv4(value) {
  const matches = String(value || '').match(IPV4_REGEX) || [];
  const seen = new Set();
  const values = [];
  for (const match of matches) {
    const normalized = normalizeGatewayIp(match);
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    values.push(normalized);
  }
  return values;
}

function normalizeAccountTags(input) {
  let values = [];
  if (Array.isArray(input)) {
    values = input;
  } else if (typeof input === 'string') {
    values = input.split(',');
  } else if (input && typeof input === 'object') {
    values = Object.values(input);
  }

  const seen = new Set();
  return values
    .map((item) => safeText(item, 128))
    .filter(Boolean)
    .filter((item) => {
      const key = item.toLowerCase();
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });
}

function getCheckmkConfig() {
  const baseUrl = safeText(process.env.CHECKMK_BASE_URL || process.env.CHECKMK_URL || '', 512).replace(/\/+$/, '');
  const apiToken = safeText(process.env.CHECKMK_API_KEY || process.env.CHECKMK_AUTOMATION_KEY || '', 2048);
  const authScheme = safeText(process.env.CHECKMK_AUTH_SCHEME || 'automation', 32).toLowerCase();
  const hosts = parseStringList(process.env.CHECKMK_FIREWALL_HOSTS || process.env.CHECKMK_HOSTS || '', DEFAULT_CHECKMK_HOSTS);

  return {
    enabled: parseBoolean(process.env.CHECKMK_ENABLED, true),
    baseUrl,
    apiToken,
    authScheme,
    cloudflareClientId: safeText(process.env.CLOUDFLARE_ACCESS_CLIENT_ID || process.env.CHECKMK_CF_CLIENT_ID || '', 512),
    cloudflareClientSecret: safeText(process.env.CLOUDFLARE_ACCESS_CLIENT_SECRET || process.env.CHECKMK_CF_CLIENT_SECRET || '', 1024),
    hosts,
    intervalMs: parsePositiveInteger(process.env.CHECKMK_SYNC_INTERVAL_MS, 5 * 60 * 1000, 60_000, 12 * 60 * 60 * 1000),
    timeoutMs: parsePositiveInteger(process.env.CHECKMK_TIMEOUT_MS, 45_000, 5000, 120_000),
    runOnStartup: parseBoolean(process.env.CHECKMK_SYNC_RUN_ON_STARTUP, true),
    startupDelayMs: parsePositiveInteger(process.env.CHECKMK_SYNC_STARTUP_DELAY_MS, 1500, 0, 60_000),
    serviceFilterRegex: safeText(process.env.CHECKMK_SERVICE_FILTER_REGEX || '', 512)
  };
}

function getPublicConfigSummary(config = getCheckmkConfig()) {
  return {
    enabled: config.enabled,
    configured: Boolean(config.baseUrl && config.apiToken),
    baseUrl: config.baseUrl,
    hosts: config.hosts,
    intervalMs: config.intervalMs,
    timeoutMs: config.timeoutMs,
    authScheme: config.authScheme,
    hasCloudflareAccess: Boolean(config.cloudflareClientId && config.cloudflareClientSecret),
    serviceFilterRegex: config.serviceFilterRegex || null
  };
}

function buildAuthorizationHeader(config) {
  const rawToken = safeText(config.apiToken, 2048);
  if (!rawToken) {
    return '';
  }

  if (/^bearer\s+/i.test(rawToken)) {
    return rawToken;
  }
  if (/^automation\s+/i.test(rawToken)) {
    return `Bearer ${rawToken}`;
  }

  const scheme = safeText(config.authScheme, 32).toLowerCase();
  if (scheme === 'bearer') {
    return `Bearer ${rawToken}`;
  }
  if (scheme === 'raw') {
    return rawToken;
  }
  return `Bearer automation ${rawToken}`;
}

function buildCheckmkHeaders(config) {
  const headers = {
    accept: 'application/json',
    'Content-Type': 'application/json'
  };
  const auth = buildAuthorizationHeader(config);
  if (auth) {
    headers.Authorization = auth;
  }
  if (config.cloudflareClientId) {
    headers['CF-Access-Client-Id'] = config.cloudflareClientId;
  }
  if (config.cloudflareClientSecret) {
    headers['CF-Access-Client-Secret'] = config.cloudflareClientSecret;
  }
  return headers;
}

function validateConfig(config) {
  const errors = [];
  if (!config.enabled) {
    errors.push('CHECKMK_ENABLED is false.');
  }
  if (!config.baseUrl) {
    errors.push('CHECKMK_BASE_URL is required.');
  }
  if (!config.apiToken) {
    errors.push('CHECKMK_API_KEY is required.');
  }
  if (!Array.isArray(config.hosts) || !config.hosts.length) {
    errors.push('CHECKMK_FIREWALL_HOSTS is empty.');
  }
  return errors;
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 30_000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(1000, timeoutMs));
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: {
        ...(options.headers || {})
      }
    });
    const text = await response.text();
    let payload = null;
    try {
      payload = text ? JSON.parse(text) : null;
    } catch (_error) {
      payload = null;
    }

    if (!response.ok) {
      const detail = payload
        ? JSON.stringify(payload)
        : safeText(text, 400);
      const error = new Error(`CheckMK request failed (${response.status})${detail ? `: ${detail}` : ''}`);
      error.statusCode = response.status;
      throw error;
    }

    return payload || {};
  } finally {
    clearTimeout(timeout);
  }
}

function buildCheckmkUrl(config, endpointPath) {
  const base = String(config.baseUrl || '').replace(/\/+$/, '');
  const pathPart = String(endpointPath || '').replace(/^\/+/, '');
  return `${base}/${pathPart}`;
}

async function fetchCheckmk(config, endpointPath) {
  const url = buildCheckmkUrl(config, endpointPath);
  return fetchJsonWithTimeout(
    url,
    {
      method: 'GET',
      headers: buildCheckmkHeaders(config)
    },
    config.timeoutMs
  );
}

function collectServiceDescriptions(payload) {
  const values = new Set();
  const queue = [payload];
  const visited = new Set();

  const append = (candidate) => {
    const text = safeText(candidate, 300);
    if (!text) {
      return;
    }
    if (/^\/?objects\//i.test(text)) {
      return;
    }
    if (/^https?:\/\//i.test(text)) {
      return;
    }
    values.add(text);
  };

  while (queue.length) {
    const current = queue.shift();
    if (!current || typeof current !== 'object') {
      continue;
    }
    if (visited.has(current)) {
      continue;
    }
    visited.add(current);

    if (Array.isArray(current)) {
      current.forEach((item) => queue.push(item));
      continue;
    }

    for (const [rawKey, value] of Object.entries(current)) {
      const key = String(rawKey || '').trim().toLowerCase();
      if (typeof value === 'string') {
        if (
          key === 'service_description' ||
          key === 'servicedescription' ||
          (key === 'description' && /vpn|tunnel|service/i.test(value)) ||
          (key === 'title' && /vpn|tunnel|service/i.test(value))
        ) {
          append(value);
        }
      }
      if (value && typeof value === 'object') {
        queue.push(value);
      }
    }
  }

  return Array.from(values).sort((left, right) => left.localeCompare(right));
}

function findFirstMatchingValue(payload, keys = [], valueTypes = ['string']) {
  const keySet = new Set(keys.map((item) => String(item || '').trim().toLowerCase()));
  const queue = [payload];
  const visited = new Set();

  while (queue.length) {
    const current = queue.shift();
    if (!current || typeof current !== 'object') {
      continue;
    }
    if (visited.has(current)) {
      continue;
    }
    visited.add(current);

    if (Array.isArray(current)) {
      for (const item of current) {
        queue.push(item);
      }
      continue;
    }

    for (const [rawKey, value] of Object.entries(current)) {
      const key = String(rawKey || '').trim().toLowerCase();
      if (keySet.has(key)) {
        const type = typeof value;
        if (valueTypes.includes(type)) {
          return value;
        }
        if (type === 'string' && valueTypes.includes('number')) {
          const numeric = Number(value);
          if (Number.isFinite(numeric)) {
            return numeric;
          }
        }
      }
      if (value && typeof value === 'object') {
        queue.push(value);
      }
    }
  }

  return null;
}

function detectVpnService(serviceDescription, summaryText = '') {
  const text = `${String(serviceDescription || '')} ${String(summaryText || '')}`.toLowerCase();
  return /vpn|tunnel|ipsec/.test(text);
}

function detectTransmitting(perfData = '', summaryText = '') {
  const text = `${String(perfData || '')} ${String(summaryText || '')}`;
  const numbers = [];
  const regex = /(?:rx|tx|in|out|bytes|bits|traffic|bps|kbps|mbps|gbps)[^\d-]*(-?\d+(?:\.\d+)?)/gi;
  let match = regex.exec(text);
  while (match) {
    const numeric = Number(match[1]);
    if (Number.isFinite(numeric)) {
      numbers.push(numeric);
    }
    match = regex.exec(text);
  }

  if (!numbers.length) {
    const fallback = extractAllIpv4(text);
    if (fallback.length) {
      return null;
    }
    return null;
  }

  return numbers.some((value) => value > 0) ? 1 : 0;
}

function normalizeStateCode(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 3;
  }
  const rounded = Math.floor(numeric);
  if (rounded < 0 || rounded > 3) {
    return 3;
  }
  return rounded;
}

function normalizeServiceRecord(input = {}) {
  const hostId = safeText(input.hostId, 128).toUpperCase();
  const serviceDescription = safeText(input.serviceDescription, 512);
  const stateCode = normalizeStateCode(input.stateCode);
  const summaryText = safeText(input.summaryText, 2048);
  const perfData = safeText(input.perfData, 4096);
  const gatewayIp = normalizeGatewayIp(input.gatewayIp || extractIpv4(serviceDescription) || extractIpv4(summaryText));
  const isVpn = detectVpnService(serviceDescription, summaryText) || Boolean(gatewayIp);
  const transmittingValue = input.transmitting === null || input.transmitting === undefined
    ? detectTransmitting(perfData, summaryText)
    : Number(input.transmitting) === 1
      ? 1
      : Number(input.transmitting) === 0
        ? 0
        : null;
  const fetchedAt = safeText(input.fetchedAt, 64) || nowIso();
  const updatedAt = safeText(input.updatedAt, 64) || fetchedAt;

  return {
    host_id: hostId,
    service_description: serviceDescription,
    service_key: `${hostId}::${serviceDescription}`.toLowerCase(),
    gateway_ip: gatewayIp || null,
    is_vpn: isVpn ? 1 : 0,
    state_code: stateCode,
    state_label: SERVICE_STATE_LABELS[stateCode] || 'unknown',
    summary_text: summaryText || null,
    perf_data: perfData || null,
    transmitting: transmittingValue === null ? null : transmittingValue,
    raw_json: input.raw ? JSON.stringify(input.raw) : null,
    fetched_at: fetchedAt,
    updated_at: updatedAt
  };
}

function saveHostServices(hostId, rows = [], fetchedAt = nowIso()) {
  const normalizedHost = safeText(hostId, 128).toUpperCase();
  const normalizedRows = rows
    .map((row) => normalizeServiceRecord({ ...row, hostId: normalizedHost, fetchedAt, updatedAt: fetchedAt }))
    .filter((row) => row.host_id && row.service_description);

  const tx = db.transaction(() => {
    if (normalizedRows.length) {
      const serviceDescriptions = normalizedRows.map((row) => row.service_description);
      const placeholders = serviceDescriptions.map(() => '?').join(', ');
      db.prepare(
        `
        DELETE FROM firewall_checkmk_services_latest
        WHERE host_id = ?
          AND service_description NOT IN (${placeholders})
      `
      ).run(normalizedHost, ...serviceDescriptions);
    } else {
      db.prepare('DELETE FROM firewall_checkmk_services_latest WHERE host_id = ?').run(normalizedHost);
    }

    for (const row of normalizedRows) {
      upsertServiceStmt.run(row);
    }
  });

  tx();
  return normalizedRows.length;
}

function listFirewallServices(filters = {}) {
  const where = [];
  const args = [];

  if (filters.hostId) {
    where.push('host_id = ?');
    args.push(safeText(filters.hostId, 128).toUpperCase());
  }

  if (filters.onlyVpn) {
    where.push('is_vpn = 1');
  }

  if (filters.searchText) {
    const like = `%${String(filters.searchText || '').trim().toLowerCase()}%`;
    if (like !== '%%') {
      where.push(`(
        LOWER(host_id) LIKE ?
        OR LOWER(service_description) LIKE ?
        OR LOWER(COALESCE(gateway_ip, '')) LIKE ?
        OR LOWER(COALESCE(summary_text, '')) LIKE ?
      )`);
      args.push(like, like, like, like);
    }
  }

  const limit = Math.max(1, Math.min(25_000, Number(filters.limit) || 5000));

  const rows = db
    .prepare(
      `
      SELECT
        host_id,
        service_description,
        gateway_ip,
        is_vpn,
        state_code,
        state_label,
        summary_text,
        perf_data,
        transmitting,
        fetched_at,
        updated_at,
        raw_json
      FROM firewall_checkmk_services_latest
      ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
      ORDER BY host_id COLLATE NOCASE, state_code DESC, service_description COLLATE NOCASE
      LIMIT ${limit}
    `
    )
    .all(...args);

  return rows.map((row) => ({
    hostId: row.host_id,
    serviceDescription: row.service_description,
    gatewayIp: row.gateway_ip || null,
    isVpn: Number(row.is_vpn) === 1,
    stateCode: Number.isFinite(Number(row.state_code)) ? Number(row.state_code) : 3,
    stateLabel: safeText(row.state_label, 32) || 'unknown',
    summaryText: row.summary_text || null,
    perfData: row.perf_data || null,
    transmitting: row.transmitting === null || row.transmitting === undefined ? null : Number(row.transmitting) === 1,
    fetchedAt: row.fetched_at,
    updatedAt: row.updated_at,
    raw: (() => {
      try {
        return row.raw_json ? JSON.parse(row.raw_json) : null;
      } catch (_error) {
        return null;
      }
    })()
  }));
}

function summarizeFirewallHosts() {
  const rows = db
    .prepare(
      `
      SELECT
        host_id,
        COUNT(*) AS service_count,
        SUM(CASE WHEN is_vpn = 1 THEN 1 ELSE 0 END) AS vpn_count,
        SUM(CASE WHEN state_code = 0 THEN 1 ELSE 0 END) AS healthy_count,
        SUM(CASE WHEN state_code > 0 THEN 1 ELSE 0 END) AS issue_count,
        SUM(CASE WHEN is_vpn = 1 AND transmitting = 1 THEN 1 ELSE 0 END) AS vpn_transmitting_count,
        MAX(updated_at) AS updated_at
      FROM firewall_checkmk_services_latest
      GROUP BY host_id
      ORDER BY host_id COLLATE NOCASE
    `
    )
    .all();

  return rows.map((row) => ({
    hostId: row.host_id,
    serviceCount: Number(row.service_count || 0),
    vpnCount: Number(row.vpn_count || 0),
    healthyCount: Number(row.healthy_count || 0),
    issueCount: Number(row.issue_count || 0),
    vpnTransmittingCount: Number(row.vpn_transmitting_count || 0),
    updatedAt: row.updated_at || null
  }));
}

function listVpnMetadataMap() {
  const rows = db
    .prepare(
      `
      SELECT gateway_ip, vpn_number, account_tags_json, notes, updated_at
      FROM firewall_vpn_metadata
    `
    )
    .all();

  const output = new Map();
  for (const row of rows) {
    let accountTags = [];
    try {
      const parsed = row.account_tags_json ? JSON.parse(row.account_tags_json) : [];
      accountTags = normalizeAccountTags(Array.isArray(parsed) ? parsed : []);
    } catch (_error) {
      accountTags = [];
    }
    output.set(String(row.gateway_ip || '').toLowerCase(), {
      gatewayIp: normalizeGatewayIp(row.gateway_ip),
      vpnNumber: safeText(row.vpn_number, 128) || '',
      accountTags,
      notes: safeText(row.notes, 512) || '',
      updatedAt: row.updated_at || null
    });
  }
  return output;
}

function upsertVpnMetadata(input = {}) {
  const gatewayIp = normalizeGatewayIp(input.gatewayIp || input.gateway_ip);
  if (!gatewayIp) {
    const error = new Error('Valid gatewayIp is required.');
    error.statusCode = 400;
    throw error;
  }

  const vpnNumber = safeText(input.vpnNumber || input.vpn_number, 128);
  const accountTags = normalizeAccountTags(input.accountTags || input.account_tags);
  const notes = safeText(input.notes, 512);
  const updatedAt = nowIso();

  upsertVpnMetadataStmt.run(gatewayIp, vpnNumber || null, JSON.stringify(accountTags), notes || null, updatedAt);

  return {
    gatewayIp,
    vpnNumber,
    accountTags,
    notes,
    updatedAt
  };
}

function getVpnRows(filters = {}) {
  const searchText = String(filters.searchText || '').trim().toLowerCase();
  const hostFilter = safeText(filters.hostId, 128).toUpperCase();
  const metadataMap = listVpnMetadataMap();

  let rows = listFirewallServices({
    onlyVpn: true,
    hostId: hostFilter || null,
    limit: 25_000
  }).map((row) => {
    const metadata = metadataMap.get(String(row.gatewayIp || '').toLowerCase()) || {
      gatewayIp: row.gatewayIp,
      vpnNumber: '',
      accountTags: [],
      notes: '',
      updatedAt: null
    };
    const isOnline = Number(row.stateCode) === 0;
    const transmitting = row.transmitting === null ? null : Boolean(row.transmitting);
    return {
      ...row,
      status: isOnline ? 'online' : 'issue',
      isOnline,
      transmitting,
      vpnNumber: metadata.vpnNumber || '',
      accountTags: Array.isArray(metadata.accountTags) ? metadata.accountTags : [],
      notes: metadata.notes || '',
      metadataUpdatedAt: metadata.updatedAt || null
    };
  });

  if (searchText) {
    rows = rows.filter((row) => {
      const haystack = [
        row.hostId,
        row.serviceDescription,
        row.gatewayIp,
        row.vpnNumber,
        (row.accountTags || []).join(','),
        row.notes,
        row.summaryText,
        row.status
      ]
        .map((value) => String(value || '').toLowerCase())
        .join(' ');
      return haystack.includes(searchText);
    });
  }

  const sortBy = String(filters.sortBy || 'status').trim().toLowerCase();
  rows.sort((left, right) => {
    if (sortBy === 'vpn_number') {
      const leftValue = String(left.vpnNumber || '');
      const rightValue = String(right.vpnNumber || '');
      return leftValue.localeCompare(rightValue, undefined, { sensitivity: 'base', numeric: true });
    }
    if (sortBy === 'gateway') {
      return String(left.gatewayIp || '').localeCompare(String(right.gatewayIp || ''), undefined, {
        sensitivity: 'base',
        numeric: true
      });
    }
    if (sortBy === 'host') {
      return String(left.hostId || '').localeCompare(String(right.hostId || ''), undefined, {
        sensitivity: 'base',
        numeric: true
      });
    }

    if (left.isOnline !== right.isOnline) {
      return left.isOnline ? 1 : -1;
    }
    return String(left.serviceDescription || '').localeCompare(String(right.serviceDescription || ''), undefined, {
      sensitivity: 'base',
      numeric: true
    });
  });

  return rows;
}

function matchesServiceFilter(config, serviceDescription) {
  const regexSource = safeText(config.serviceFilterRegex, 512);
  if (!regexSource) {
    return true;
  }
  try {
    const regex = new RegExp(regexSource, 'i');
    return regex.test(String(serviceDescription || ''));
  } catch (_error) {
    return true;
  }
}

async function fetchServiceDiscovery(config, hostId) {
  const payload = await fetchCheckmk(config, `objects/service_discovery/${encodeURIComponent(hostId)}`);
  return collectServiceDescriptions(payload).filter((description) => matchesServiceFilter(config, description));
}

async function fetchServiceStatus(config, hostId, serviceDescription) {
  const query = new URLSearchParams({
    service_description: serviceDescription
  });
  const payload = await fetchCheckmk(
    config,
    `objects/host/${encodeURIComponent(hostId)}/actions/show_service/invoke?${query.toString()}`
  );

  const stateCode = findFirstMatchingValue(payload, ['state', 'service_state', 'status', 'status_code'], ['number']);
  const summaryText = findFirstMatchingValue(
    payload,
    ['plugin_output', 'service_output', 'output', 'summary', 'state_text'],
    ['string']
  );
  const perfData = findFirstMatchingValue(payload, ['perf_data', 'perfdata', 'performance_data'], ['string']);

  return normalizeServiceRecord({
    hostId,
    serviceDescription,
    stateCode,
    summaryText,
    perfData,
    raw: payload
  });
}

async function mapWithConcurrency(items, worker, concurrency = 8) {
  const queue = Array.isArray(items) ? [...items] : [];
  const results = [];

  const workerCount = Math.max(1, Math.min(concurrency, queue.length || 1));

  async function runWorker() {
    while (queue.length) {
      const item = queue.shift();
      // eslint-disable-next-line no-await-in-loop
      const result = await worker(item);
      results.push(result);
    }
  }

  await Promise.all(Array.from({ length: workerCount }, () => runWorker()));
  return results;
}

async function syncHost(config, hostId) {
  const startedAt = nowIso();
  const serviceDescriptions = await fetchServiceDiscovery(config, hostId);
  const serviceRows = [];
  const failures = [];

  await mapWithConcurrency(
    serviceDescriptions,
    async (serviceDescription) => {
      try {
        const row = await fetchServiceStatus(config, hostId, serviceDescription);
        serviceRows.push(row);
      } catch (error) {
        failures.push({
          serviceDescription,
          error: error?.message || 'Service status pull failed.'
        });
        serviceRows.push(
          normalizeServiceRecord({
            hostId,
            serviceDescription,
            stateCode: 3,
            summaryText: `Error: ${error?.message || 'status unavailable'}`,
            perfData: '',
            raw: {
              error: error?.message || 'status unavailable'
            }
          })
        );
      }
    },
    6
  );

  const fetchedAt = nowIso();
  const saved = saveHostServices(hostId, serviceRows, fetchedAt);

  return {
    hostId,
    startedAt,
    finishedAt: fetchedAt,
    discoveredServices: serviceDescriptions.length,
    savedServices: saved,
    failures
  };
}

function getSyncStatusPayload() {
  const summaryRows = summarizeFirewallHosts();
  return {
    sync: {
      ...syncState
    },
    config: getPublicConfigSummary(),
    hosts: summaryRows,
    totalServices: summaryRows.reduce((sum, row) => sum + Number(row.serviceCount || 0), 0),
    totalVpnServices: summaryRows.reduce((sum, row) => sum + Number(row.vpnCount || 0), 0)
  };
}

async function runSync(trigger = 'manual') {
  const config = getCheckmkConfig();
  syncState.enabled = Boolean(config.enabled);
  syncState.intervalMs = config.intervalMs;

  const validationErrors = validateConfig(config);
  if (validationErrors.length) {
    const error = new Error(validationErrors.join(' '));
    error.statusCode = 400;
    syncState.lastStatus = 'error';
    syncState.lastError = error.message;
    throw error;
  }

  if (syncState.running) {
    return {
      started: false,
      running: true,
      trigger,
      message: 'Sync already running.'
    };
  }

  syncState.running = true;
  syncState.lastStartedAt = nowIso();
  syncState.lastStatus = 'running';
  syncState.lastError = null;

  const summary = {
    trigger,
    hosts: [],
    totalDiscoveredServices: 0,
    totalSavedServices: 0,
    totalFailures: 0
  };

  try {
    for (const hostId of config.hosts) {
      // eslint-disable-next-line no-await-in-loop
      const hostSummary = await syncHost(config, hostId);
      summary.hosts.push(hostSummary);
      summary.totalDiscoveredServices += Number(hostSummary.discoveredServices || 0);
      summary.totalSavedServices += Number(hostSummary.savedServices || 0);
      summary.totalFailures += Array.isArray(hostSummary.failures) ? hostSummary.failures.length : 0;
    }

    syncState.lastStatus = summary.totalFailures > 0 ? 'warning' : 'ok';
    syncState.lastSummary = summary;
    return {
      started: true,
      running: false,
      summary
    };
  } catch (error) {
    syncState.lastStatus = 'error';
    syncState.lastError = error?.message || 'Sync failed.';
    syncState.lastSummary = summary;
    throw error;
  } finally {
    syncState.running = false;
    syncState.lastFinishedAt = nowIso();
  }
}

function scheduleNextSync() {
  if (syncTimer) {
    clearTimeout(syncTimer);
    syncTimer = null;
  }

  const config = getCheckmkConfig();
  syncState.enabled = Boolean(config.enabled);
  syncState.intervalMs = config.intervalMs;

  if (!config.enabled) {
    syncState.nextRunAt = null;
    return;
  }

  syncState.nextRunAt = new Date(Date.now() + config.intervalMs).toISOString();
  syncTimer = setTimeout(async () => {
    try {
      await runSync('scheduled');
    } catch (_error) {
      // Keep scheduler running even when provider is unavailable.
    } finally {
      scheduleNextSync();
    }
  }, config.intervalMs);
}

function startBackgroundWorkers() {
  if (workersStarted) {
    return getSyncStatusPayload();
  }
  workersStarted = true;

  const config = getCheckmkConfig();
  syncState.enabled = Boolean(config.enabled);
  syncState.intervalMs = config.intervalMs;

  if (config.enabled && config.runOnStartup) {
    setTimeout(() => {
      void runSync('startup').catch(() => {
        // Startup sync errors are visible via status endpoint.
      });
    }, config.startupDelayMs);
  }

  scheduleNextSync();
  return getSyncStatusPayload();
}

app.get('/api/health', (_req, res) => {
  const statusPayload = getSyncStatusPayload();
  return res.json({
    ok: true,
    generatedAt: nowIso(),
    ...statusPayload
  });
});

app.get('/api/status', (_req, res) => {
  return res.json({
    ok: true,
    generatedAt: nowIso(),
    ...getSyncStatusPayload()
  });
});

app.post('/api/sync', async (req, res, next) => {
  try {
    const force = parseBoolean(req.body?.force, true);
    const result = await runSync(force ? 'manual' : 'manual-nonforce');
    return res.status(result?.running ? 202 : 200).json({
      ok: true,
      ...result,
      generatedAt: nowIso(),
      ...getSyncStatusPayload()
    });
  } catch (error) {
    return next(error);
  }
});

app.get('/api/firewalls', async (req, res, next) => {
  try {
    const refresh = parseBoolean(req.query?.refresh, false);
    if (refresh) {
      await runSync('manual-refresh');
    }

    const hostId = safeText(req.query?.hostId || req.query?.host || '', 128).toUpperCase();
    const searchText = safeText(req.query?.search || req.query?.q || '', 256);

    const services = listFirewallServices({
      hostId: hostId || null,
      searchText,
      onlyVpn: false,
      limit: 20_000
    });

    const hosts = summarizeFirewallHosts();
    const issueRows = services.filter((row) => Number(row.stateCode) !== 0).slice(0, 500);

    return res.json({
      generatedAt: nowIso(),
      hosts,
      services,
      issueRows,
      totalServices: services.length,
      sync: { ...syncState },
      config: getPublicConfigSummary()
    });
  } catch (error) {
    return next(error);
  }
});

app.get('/api/vpn', async (req, res, next) => {
  try {
    const refresh = parseBoolean(req.query?.refresh, false);
    if (refresh) {
      await runSync('manual-refresh');
    }

    const hostId = safeText(req.query?.hostId || req.query?.host || '', 128).toUpperCase();
    const searchText = safeText(req.query?.search || req.query?.q || '', 256);
    const sortBy = safeText(req.query?.sortBy || req.query?.sort || '', 64);
    const rows = getVpnRows({
      hostId: hostId || null,
      searchText,
      sortBy
    });

    const availableHosts = Array.from(
      new Set(rows.map((row) => safeText(row.hostId, 128)).filter(Boolean))
    ).sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base', numeric: true }));

    return res.json({
      generatedAt: nowIso(),
      total: rows.length,
      rows,
      availableHosts,
      sync: { ...syncState },
      config: getPublicConfigSummary()
    });
  } catch (error) {
    return next(error);
  }
});

app.put('/api/vpn/metadata', (req, res, next) => {
  try {
    const metadata = upsertVpnMetadata(req.body || {});
    return res.json({
      ok: true,
      metadata
    });
  } catch (error) {
    return next(error);
  }
});

app.use((error, _req, res, _next) => {
  const statusCode = Number(error?.statusCode) || 500;
  const message = error?.message || 'Firewall module error.';
  return res.status(statusCode).json({
    error: message
  });
});

module.exports = {
  app,
  startBackgroundWorkers
};
