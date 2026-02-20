const express = require('express');
const {
  db,
  listIpAddressAliases,
  upsertIpAddressAliases,
  resolveIpAddressAliases
} = require('../../db');

const app = express();
app.set('etag', false);
app.use(express.json({ limit: '1mb' }));

const IPV4_REGEX = /\b(?:25[0-5]|2[0-4]\d|1?\d{1,2})(?:\.(?:25[0-5]|2[0-4]\d|1?\d{1,2})){3}\b/g;

function normalizeIpAddressValue(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function isValidIpv4Address(value) {
  const normalized = normalizeIpAddressValue(value);
  if (!normalized) {
    return false;
  }
  const octets = normalized.split('.');
  if (octets.length !== 4) {
    return false;
  }
  return octets.every((octet) => {
    if (!/^\d{1,3}$/.test(octet)) {
      return false;
    }
    const numeric = Number(octet);
    return Number.isInteger(numeric) && numeric >= 0 && numeric <= 255;
  });
}

function extractIpv4Addresses(rawValue) {
  const text = String(rawValue || '');
  if (!text) {
    return [];
  }
  const matches = text.match(IPV4_REGEX) || [];
  const seen = new Set();
  const results = [];
  for (const match of matches) {
    const ipAddress = normalizeIpAddressValue(match);
    if (!isValidIpv4Address(ipAddress) || seen.has(ipAddress)) {
      continue;
    }
    seen.add(ipAddress);
    results.push(ipAddress);
  }
  return results;
}

function safeText(value, maxLength = 256) {
  return String(value || '')
    .trim()
    .slice(0, maxLength);
}

function deriveNameFromResourceId(resourceId) {
  const text = String(resourceId || '').trim();
  if (!text) {
    return '';
  }
  const parts = text.split('/').filter(Boolean);
  if (!parts.length) {
    return '';
  }
  return safeText(parts[parts.length - 1], 128);
}

function numericIpSortValue(ipAddress) {
  return String(ipAddress || '')
    .split('.')
    .reduce((acc, segment) => acc * 256 + Number(segment || 0), 0);
}

function queryRows(sql, params = []) {
  try {
    return db.prepare(sql).all(...params);
  } catch (_error) {
    return [];
  }
}

function upsertCandidate(candidateMap, input = {}) {
  const ipAddress = normalizeIpAddressValue(input.ipAddress);
  if (!isValidIpv4Address(ipAddress)) {
    return;
  }

  let candidate = candidateMap.get(ipAddress);
  if (!candidate) {
    candidate = {
      ipAddress,
      suggestedServerName: '',
      providers: new Set(),
      sources: new Set(),
      sampleRefs: new Set()
    };
    candidateMap.set(ipAddress, candidate);
  }

  const serverName = safeText(input.serverName, 128);
  if (!candidate.suggestedServerName && serverName) {
    candidate.suggestedServerName = serverName;
  }

  const provider = safeText(input.provider, 64).toLowerCase();
  if (provider) {
    candidate.providers.add(provider);
  }

  const source = safeText(input.source, 200);
  if (source) {
    candidate.sources.add(source);
  }

  const resourceRef = safeText(input.resourceRef, 240);
  if (resourceRef && candidate.sampleRefs.size < 6) {
    candidate.sampleRefs.add(resourceRef);
  }
}

function collectVsaxCandidates(candidateMap) {
  const rows = queryRows(
    `
      SELECT group_name, device_id, device_name, internal_ip, public_ip
      FROM vsax_devices
      WHERE is_active = 1
    `
  );

  for (const row of rows) {
    const groupName = safeText(row?.group_name, 96);
    const deviceName = safeText(row?.device_name || row?.device_id, 128);
    const sourceLabel = groupName ? `VSAx group ${groupName}` : 'VSAx devices';
    const resourceRef = `vsax://${groupName || 'group'}/${safeText(row?.device_id || row?.device_name, 128)}`;
    const ips = [...extractIpv4Addresses(row?.internal_ip), ...extractIpv4Addresses(row?.public_ip)];

    for (const ipAddress of ips) {
      upsertCandidate(candidateMap, {
        ipAddress,
        serverName: deviceName,
        provider: 'vsax',
        source: sourceLabel,
        resourceRef
      });
    }
  }
}

function collectCloudMetricsCandidates(candidateMap) {
  const rows = queryRows(
    `
      SELECT provider, resource_id, resource_name, resource_type, metadata_json
      FROM cloud_metrics_latest
    `
  );

  for (const row of rows) {
    const provider = safeText(row?.provider, 64).toLowerCase();
    const resourceType = safeText(row?.resource_type, 128);
    const resourceRef = safeText(row?.resource_id, 240);
    const suggestedServerName = safeText(row?.resource_name, 128) || deriveNameFromResourceId(resourceRef);
    const source = `${provider ? provider.toUpperCase() : 'CLOUD'} metrics${resourceType ? ` (${resourceType})` : ''}`;
    const searchText = [row?.resource_id, row?.resource_name, row?.metadata_json].join('\n');
    const ips = extractIpv4Addresses(searchText);

    for (const ipAddress of ips) {
      upsertCandidate(candidateMap, {
        ipAddress,
        serverName: suggestedServerName,
        provider: provider || 'cloud',
        source,
        resourceRef
      });
    }
  }
}

function collectStorageCandidates(candidateMap) {
  const accountRows = queryRows(
    `
      SELECT account_id, name, blob_endpoint
      FROM storage_accounts
      WHERE is_active = 1
    `
  );

  for (const row of accountRows) {
    const resourceRef = safeText(row?.account_id || row?.name, 128);
    const endpoint = safeText(row?.blob_endpoint, 512);
    const suggestedServerName = safeText(row?.name || row?.account_id, 128);
    for (const ipAddress of extractIpv4Addresses(endpoint)) {
      upsertCandidate(candidateMap, {
        ipAddress,
        serverName: suggestedServerName,
        provider: 'azure',
        source: 'Storage account endpoint',
        resourceRef: `storage://${resourceRef}`
      });
    }
  }

  const securityRows = queryRows(
    `
      SELECT account_id, profile_json
      FROM storage_account_security
    `
  );

  for (const row of securityRows) {
    const accountId = safeText(row?.account_id, 240);
    const accountName = safeText(deriveNameFromResourceId(accountId) || accountId, 128);
    const profile = String(row?.profile_json || '');
    for (const ipAddress of extractIpv4Addresses(profile)) {
      upsertCandidate(candidateMap, {
        ipAddress,
        serverName: accountName,
        provider: 'azure',
        source: 'Storage security profile',
        resourceRef: `storage-security://${accountId || 'account'}`
      });
    }
  }
}

function collectAccountEndpointCandidates(candidateMap) {
  const awsRows = queryRows(
    `
      SELECT account_id, display_name, s3_endpoint
      FROM aws_accounts
    `
  );

  for (const row of awsRows) {
    const accountId = safeText(row?.account_id, 128);
    const displayName = safeText(row?.display_name || row?.account_id, 128);
    for (const ipAddress of extractIpv4Addresses(row?.s3_endpoint)) {
      upsertCandidate(candidateMap, {
        ipAddress,
        serverName: displayName,
        provider: 'aws',
        source: 'AWS account endpoint',
        resourceRef: `aws-account://${accountId || displayName || 'account'}`
      });
    }
  }

  const wasabiRows = queryRows(
    `
      SELECT account_id, display_name, s3_endpoint, stats_endpoint
      FROM wasabi_accounts
    `
  );

  for (const row of wasabiRows) {
    const accountId = safeText(row?.account_id, 128);
    const displayName = safeText(row?.display_name || row?.account_id, 128);
    const ips = [...extractIpv4Addresses(row?.s3_endpoint), ...extractIpv4Addresses(row?.stats_endpoint)];

    for (const ipAddress of ips) {
      upsertCandidate(candidateMap, {
        ipAddress,
        serverName: displayName,
        provider: 'wasabi',
        source: 'Wasabi account endpoint',
        resourceRef: `wasabi-account://${accountId || displayName || 'account'}`
      });
    }
  }
}

function listUnmappedIpCandidates(options = {}) {
  const candidateMap = new Map();

  collectVsaxCandidates(candidateMap);
  collectCloudMetricsCandidates(candidateMap);
  collectStorageCandidates(candidateMap);
  collectAccountEndpointCandidates(candidateMap);

  const existing = listIpAddressAliases();
  const mappedSet = new Set(existing.map((row) => normalizeIpAddressValue(row.ip_address || row.ipAddress)));

  const rows = Array.from(candidateMap.values())
    .filter((candidate) => !mappedSet.has(candidate.ipAddress))
    .sort((a, b) => numericIpSortValue(a.ipAddress) - numericIpSortValue(b.ipAddress))
    .map((candidate) => ({
      ipAddress: candidate.ipAddress,
      suggestedServerName: candidate.suggestedServerName || '',
      providers: Array.from(candidate.providers).sort((left, right) => left.localeCompare(right)),
      sources: Array.from(candidate.sources).sort((left, right) => left.localeCompare(right)),
      sampleRefs: Array.from(candidate.sampleRefs)
    }));

  const requestedLimit = Number(options.limit);
  const limit = Number.isFinite(requestedLimit) ? Math.max(1, Math.min(10_000, Math.round(requestedLimit))) : 2_000;
  const limitedRows = rows.slice(0, limit);

  return {
    rows: limitedRows,
    total: limitedRows.length,
    uniqueUnmapped: rows.length,
    uniqueDiscovered: candidateMap.size,
    alreadyMapped: Math.max(0, candidateMap.size - rows.length),
    truncated: rows.length > limit,
    scannedAt: new Date().toISOString()
  };
}

function parseIpRows(raw) {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw
    .map((row) => {
      if (!row || typeof row !== 'object') {
        return null;
      }
      const ipAddress = String(row.ipAddress || row.ip_address || row.ip || '')
        .trim()
        .toLowerCase();
      const serverName = String(row.serverName || row.server_name || '').trim();
      if (!ipAddress || !serverName) {
        return null;
      }
      return { ipAddress, serverName };
    })
    .filter(Boolean);
}

function parseIps(raw) {
  if (Array.isArray(raw)) {
    return raw
      .map((item) => String(item || '').trim().toLowerCase())
      .filter(Boolean);
  }
  return String(raw || '')
    .split(',')
    .map((item) => String(item || '').trim().toLowerCase())
    .filter(Boolean);
}

function escapeCsv(value) {
  const text = value === null || value === undefined ? '' : String(value);
  if (/[",\r\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function toCsv(rows, columns) {
  const header = columns.map((column) => escapeCsv(column)).join(',');
  const body = rows.map((row) => columns.map((column) => escapeCsv(row[column])).join(',')).join('\n');
  return body ? `${header}\n${body}` : header;
}

function csvTimestamp() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    aliases: listIpAddressAliases().length
  });
});

app.get('/api/aliases', (_req, res, next) => {
  try {
    const aliases = listIpAddressAliases();
    res.json({ aliases, total: aliases.length });
  } catch (error) {
    next(error);
  }
});

app.post('/api/aliases/import', (req, res, next) => {
  try {
    const rows = parseIpRows(req.body?.rows);
    if (!rows.length) {
      return res.status(400).json({ error: 'No valid IP address rows provided.' });
    }
    upsertIpAddressAliases(rows, { replace: Boolean(req.body?.replace) });
    const aliases = listIpAddressAliases();
    return res.json({
      imported: rows.length,
      aliases,
      total: aliases.length
    });
  } catch (error) {
    return next(error);
  }
});

app.get('/api/discovery/unmapped', (req, res, next) => {
  try {
    const limitRaw = req.query.limit;
    const payload = listUnmappedIpCandidates({
      limit: limitRaw
    });
    return res.json(payload);
  } catch (error) {
    return next(error);
  }
});

app.post('/api/discovery/inherit', (req, res, next) => {
  try {
    const ipAddress = normalizeIpAddressValue(req.body?.ipAddress || req.body?.ip_address);
    const serverName = safeText(req.body?.serverName || req.body?.server_name, 128);

    if (!isValidIpv4Address(ipAddress)) {
      return res.status(400).json({ error: 'A valid IPv4 address is required.' });
    }
    if (!serverName) {
      return res.status(400).json({ error: 'Server name is required.' });
    }

    upsertIpAddressAliases(
      [
        {
          ipAddress,
          serverName
        }
      ],
      { replace: false }
    );

    const aliases = listIpAddressAliases();
    const alias = aliases.find((row) => normalizeIpAddressValue(row.ip_address || row.ipAddress) === ipAddress) || null;
    return res.json({
      ok: true,
      alias,
      total: aliases.length
    });
  } catch (error) {
    return next(error);
  }
});

app.get('/api/resolve', (req, res, next) => {
  try {
    const aliases = listIpAddressAliases();
    const ips = parseIps(req.query.ips || req.query.ip);
    const resolutions = ips.length
      ? resolveIpAddressAliases(ips)
      : aliases.map((row) => ({
          ipAddress: String(row.ip_address || '').toLowerCase(),
          serverName: row.server_name || null
        }));
    res.json({
      aliases,
      total: aliases.length,
      resolutions
    });
  } catch (error) {
    next(error);
  }
});

app.get('/api/export/csv', (_req, res, next) => {
  try {
    const aliases = listIpAddressAliases();
    const csv = toCsv(aliases, ['ip_address', 'server_name', 'updated_at']);
    const filename = `ip-address-${csvTimestamp()}.csv`;
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(`\uFEFF${csv}`);
  } catch (error) {
    next(error);
  }
});

app.use((error, _req, res, _next) => {
  const message = error?.message || 'IP address module error.';
  const statusCode = Number(error?.statusCode) || 500;
  res.status(statusCode).json({ error: message });
});

module.exports = {
  app
};
