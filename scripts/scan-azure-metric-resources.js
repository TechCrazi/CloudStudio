#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

require('dotenv').config({
  path: path.resolve(__dirname, '..', '.env')
});

const TENANT_ID = String(process.env.AZURE_TENANT_ID || '').trim();
const CLIENT_ID = String(process.env.AZURE_CLIENT_ID || '').trim();
const CLIENT_SECRET = String(process.env.AZURE_CLIENT_SECRET || '').trim();

const MGMT_SCOPE = 'https://management.azure.com/.default';
const MGMT_API_BASE = 'https://management.azure.com';

const REPORT_DIR = path.resolve(__dirname, '..', 'data', 'reports');
const MAX_CONCURRENCY = 6;
const MAX_RETRIES = 4;
const HTTP_TIMEOUT_MS = 30_000;

function nowStamp() {
  return new Date().toISOString().replace(/[:]/g, '-').replace(/\.\d{3}Z$/, 'Z');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toCsvCell(value) {
  const raw = value === null || value === undefined ? '' : String(value);
  if (!/[",\n]/.test(raw)) {
    return raw;
  }
  return `"${raw.replace(/"/g, '""')}"`;
}

function toCsv(rows, columns) {
  const header = columns.map((col) => toCsvCell(col)).join(',');
  const lines = rows.map((row) => columns.map((col) => toCsvCell(row[col])).join(','));
  return [header, ...lines].join('\n');
}

async function fetchWithTimeout(url, options = {}, timeoutMs = HTTP_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal
    });
  } finally {
    clearTimeout(timer);
  }
}

async function fetchJson(url, options = {}, context = 'Azure request') {
  let attempt = 0;
  while (true) {
    const response = await fetchWithTimeout(url, options);
    const text = await response.text();

    if (response.ok) {
      if (!text) {
        return {};
      }
      try {
        return JSON.parse(text);
      } catch (_error) {
        throw new Error(`${context} returned non-JSON response.`);
      }
    }

    const retryable = [429, 500, 502, 503, 504].includes(response.status);
    if (!retryable || attempt >= MAX_RETRIES) {
      throw new Error(`${context} failed (${response.status}): ${text.slice(0, 500)}`);
    }

    const retryAfter = Number.parseInt(String(response.headers.get('retry-after') || '').trim(), 10);
    const backoffMs = Number.isFinite(retryAfter)
      ? Math.max(250, retryAfter * 1000)
      : Math.min(8_000, 500 * 2 ** attempt + Math.floor(Math.random() * 300));
    await sleep(backoffMs);
    attempt += 1;
  }
}

async function getArmToken() {
  if (!TENANT_ID || !CLIENT_ID || !CLIENT_SECRET) {
    throw new Error('Missing Azure credentials. Required: AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET');
  }

  const tokenUrl = `https://login.microsoftonline.com/${encodeURIComponent(TENANT_ID)}/oauth2/v2.0/token`;
  const body = new URLSearchParams({
    client_id: CLIENT_ID,
    client_secret: CLIENT_SECRET,
    grant_type: 'client_credentials',
    scope: MGMT_SCOPE
  });

  const response = await fetchWithTimeout(tokenUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body
  });
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Azure token request failed (${response.status}): ${text.slice(0, 400)}`);
  }
  const payload = JSON.parse(text || '{}');
  const token = String(payload.access_token || '').trim();
  if (!token) {
    throw new Error('Azure token response missing access_token.');
  }
  return token;
}

async function listSubscriptions(token) {
  const result = [];
  let next = `${MGMT_API_BASE}/subscriptions?api-version=2022-12-01`;
  while (next) {
    const payload = await fetchJson(
      next,
      { headers: { Authorization: `Bearer ${token}` } },
      'List subscriptions'
    );
    const rows = Array.isArray(payload.value) ? payload.value : [];
    for (const row of rows) {
      const id = String(row.subscriptionId || '').trim();
      if (!id) {
        continue;
      }
      result.push({
        subscriptionId: id,
        displayName: String(row.displayName || id).trim() || id,
        state: String(row.state || '').trim()
      });
    }
    next = payload.nextLink || null;
  }
  return result;
}

async function listResourcesViaResourceGraph(token, subscriptionIds) {
  const all = [];
  let skipToken = null;
  do {
    const body = {
      subscriptions: subscriptionIds,
      query: 'Resources | project id, name, type, location, resourceGroup, subscriptionId | order by id asc',
      options: {
        $top: 1000,
        resultFormat: 'objectArray',
        ...(skipToken ? { $skipToken: skipToken } : {})
      }
    };
    const payload = await fetchJson(
      `${MGMT_API_BASE}/providers/Microsoft.ResourceGraph/resources?api-version=2022-10-01`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(body)
      },
      'Resource Graph query'
    );

    const rows = Array.isArray(payload.data) ? payload.data : [];
    for (const row of rows) {
      const id = String(row.id || '').trim();
      if (!id) {
        continue;
      }
      all.push({
        id,
        name: String(row.name || '').trim(),
        type: String(row.type || '').trim(),
        location: String(row.location || '').trim(),
        resourceGroup: String(row.resourceGroup || '').trim(),
        subscriptionId: String(row.subscriptionId || '').trim()
      });
    }
    skipToken = payload.$skipToken || payload.skipToken || null;
  } while (skipToken);
  return all;
}

async function mapLimit(items, concurrency, task) {
  const safeConcurrency = Math.max(1, Number(concurrency) || 1);
  const results = new Array(items.length);
  let cursor = 0;

  async function worker() {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= items.length) {
        return;
      }
      results[index] = await task(items[index], index);
    }
  }

  const workers = Array.from({ length: Math.min(safeConcurrency, items.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

async function detectMetricSupport(token, resource) {
  const endpoint = `${MGMT_API_BASE}${resource.id}/providers/microsoft.insights/metricDefinitions?api-version=2018-01-01`;
  try {
    const payload = await fetchJson(
      endpoint,
      {
        headers: {
          Authorization: `Bearer ${token}`
        }
      },
      `Metric definitions: ${resource.id}`
    );
    const defs = Array.isArray(payload.value) ? payload.value : [];
    if (!defs.length) {
      return {
        ...resource,
        hasMetrics: false,
        metricCount: 0,
        sampleMetrics: ''
      };
    }
    const names = defs
      .map((item) => String(item?.name?.value || item?.name?.localizedValue || '').trim())
      .filter(Boolean)
      .slice(0, 5);
    return {
      ...resource,
      hasMetrics: true,
      metricCount: defs.length,
      sampleMetrics: names.join(' | ')
    };
  } catch (error) {
    const message = String(error?.message || '');
    if (/failed \(400\)|failed \(404\)|No registered resource provider found|does not support metrics|not found/i.test(message)) {
      return {
        ...resource,
        hasMetrics: false,
        metricCount: 0,
        sampleMetrics: ''
      };
    }
    return {
      ...resource,
      hasMetrics: false,
      metricCount: 0,
      sampleMetrics: '',
      checkError: message.slice(0, 300)
    };
  }
}

function summarizeByType(rows) {
  const map = new Map();
  for (const row of rows) {
    const key = row.type || 'unknown';
    const next = map.get(key) || { type: key, count: 0 };
    next.count += 1;
    map.set(key, next);
  }
  return [...map.values()].sort((a, b) => b.count - a.count);
}

async function main() {
  fs.mkdirSync(REPORT_DIR, { recursive: true });

  console.log('[azure-metrics-scan] Acquiring ARM token...');
  const token = await getArmToken();

  console.log('[azure-metrics-scan] Listing subscriptions...');
  const subscriptions = await listSubscriptions(token);
  const activeSubs = subscriptions.filter((item) => !item.state || item.state.toLowerCase() === 'enabled');
  const subscriptionIds = activeSubs.map((item) => item.subscriptionId);
  if (!subscriptionIds.length) {
    throw new Error('No enabled Azure subscriptions found for this service principal.');
  }

  console.log(`[azure-metrics-scan] Querying resources across ${subscriptionIds.length} subscription(s)...`);
  const resources = await listResourcesViaResourceGraph(token, subscriptionIds);
  console.log(`[azure-metrics-scan] Resources discovered: ${resources.length}`);

  let processed = 0;
  const scanStartedAt = Date.now();
  const scanResults = await mapLimit(resources, MAX_CONCURRENCY, async (resource) => {
    const result = await detectMetricSupport(token, resource);
    processed += 1;
    if (processed % 200 === 0 || processed === resources.length) {
      const elapsedSec = Math.max(1, Math.round((Date.now() - scanStartedAt) / 1000));
      console.log(`[azure-metrics-scan] Checked ${processed}/${resources.length} resources (${elapsedSec}s)`);
    }
    return result;
  });

  const metricResources = scanResults.filter((item) => item.hasMetrics);
  const typeSummary = summarizeByType(metricResources);
  const errors = scanResults.filter((item) => item.checkError);

  const stamp = nowStamp();
  const jsonPath = path.join(REPORT_DIR, `azure-metric-resources-${stamp}.json`);
  const csvPath = path.join(REPORT_DIR, `azure-metric-resources-${stamp}.csv`);
  const summaryPath = path.join(REPORT_DIR, `azure-metric-resources-summary-${stamp}.json`);

  const csvColumns = [
    'subscriptionId',
    'resourceGroup',
    'type',
    'name',
    'location',
    'id',
    'metricCount',
    'sampleMetrics'
  ];

  fs.writeFileSync(jsonPath, JSON.stringify(metricResources, null, 2));
  fs.writeFileSync(csvPath, toCsv(metricResources, csvColumns));
  fs.writeFileSync(
    summaryPath,
    JSON.stringify(
      {
        scannedAt: new Date().toISOString(),
        subscriptionCount: subscriptionIds.length,
        scannedResourceCount: resources.length,
        metricResourceCount: metricResources.length,
        metricCoveragePct: resources.length ? Number(((metricResources.length / resources.length) * 100).toFixed(2)) : 0,
        checkErrorCount: errors.length,
        topTypes: typeSummary.slice(0, 25)
      },
      null,
      2
    )
  );

  console.log('[azure-metrics-scan] Done.');
  console.log(`  Subscriptions: ${subscriptionIds.length}`);
  console.log(`  Resources scanned: ${resources.length}`);
  console.log(`  Resources with metrics: ${metricResources.length}`);
  console.log(`  Error checks: ${errors.length}`);
  console.log(`  JSON: ${jsonPath}`);
  console.log(`  CSV : ${csvPath}`);
  console.log(`  Summary: ${summaryPath}`);
}

main().catch((error) => {
  console.error('[azure-metrics-scan] FAILED:', error?.message || error);
  process.exitCode = 1;
});

