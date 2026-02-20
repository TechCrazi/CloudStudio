#!/usr/bin/env node

const path = require('path');
const { createHash } = require('crypto');

require('dotenv').config({
  path: path.resolve(__dirname, '..', '.env')
});

const {
  db,
  listVendors,
  getVendorById,
  deleteBillingSnapshotsForVendorPeriod,
  insertBillingSnapshot,
  upsertResourceTag,
  getResourceTag
} = require('../src/db');
const { decryptJson } = require('../src/crypto');
const { pullBillingForVendor } = require('../src/connectors/billing');

let awsTaggingSdk = null;
try {
  awsTaggingSdk = require('@aws-sdk/client-resource-groups-tagging-api');
} catch (_error) {
  awsTaggingSdk = null;
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeProvider(value) {
  const v = String(value || '').trim().toLowerCase();
  if (['azure', 'aws', 'gcp', 'private', 'wasabi', 'vsax', 'other'].includes(v)) {
    return v;
  }
  return 'other';
}

function parseBooleanEnv(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') {
    return defaultValue;
  }
  const normalized = String(value).trim().toLowerCase();
  return ['1', 'true', 'yes', 'y', 'on'].includes(normalized);
}

function parseProviderFilter(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    return null;
  }
  const providers = raw
    .split(',')
    .map((part) => normalizeProvider(part))
    .filter((provider) => provider !== 'other');
  return providers.length ? new Set(providers) : null;
}

function toDateOnly(value) {
  return value.toISOString().slice(0, 10);
}

function buildMonthlyRanges(lookbackMonths = 48, referenceDate = new Date()) {
  const currentMonthStart = new Date(Date.UTC(referenceDate.getUTCFullYear(), referenceDate.getUTCMonth(), 1));
  const firstMonth = new Date(Date.UTC(currentMonthStart.getUTCFullYear(), currentMonthStart.getUTCMonth() - lookbackMonths, 1));
  const ranges = [];
  const cursor = new Date(firstMonth.getTime());
  while (cursor.getTime() < currentMonthStart.getTime()) {
    const monthStart = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth(), 1));
    const monthEnd = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth() + 1, 0));
    ranges.push({
      periodStart: toDateOnly(monthStart),
      periodEnd: toDateOnly(monthEnd)
    });
    cursor.setUTCMonth(cursor.getUTCMonth() + 1, 1);
  }
  return ranges;
}

function getColumnIndex(columns = [], candidateNames = []) {
  const normalizedCandidates = candidateNames.map((name) => String(name || '').trim().toLowerCase());
  return columns.findIndex((column) => {
    const columnName = String(column?.name || '').trim().toLowerCase();
    return normalizedCandidates.includes(columnName);
  });
}

function ensureAzureTaggableResourceRef(resourceRef) {
  const normalizedRef = String(resourceRef || '').trim();
  if (!normalizedRef.startsWith('/subscriptions/')) {
    const error = new Error('Azure resource reference must be a full ARM resource ID.');
    error.code = 'AZURE_INVALID_RESOURCE_REF';
    throw error;
  }

  const namespaceMatch = normalizedRef.match(/\/providers\/([^/]+)/i);
  const namespace = namespaceMatch ? String(namespaceMatch[1] || '').trim() : '';
  if (!namespace || !namespace.includes('.') || namespace.toLowerCase() === 'marketplace') {
    const error = new Error(`Azure resource is not taggable (namespace "${namespace || 'unknown'}").`);
    error.code = 'NON_TAGGABLE_AZURE_RESOURCE';
    throw error;
  }

  return normalizedRef;
}

function parseAzureErrorPayload(details) {
  const text = String(details || '').trim();
  if (!text) {
    return { code: '', message: '' };
  }
  try {
    const payload = JSON.parse(text);
    return {
      code: String(payload?.error?.code || '').trim(),
      message: String(payload?.error?.message || '').trim()
    };
  } catch (_error) {
    return {
      code: '',
      message: text
    };
  }
}

function isRetryableStatus(statusCode) {
  const code = Number(statusCode);
  return code === 429 || code >= 500;
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(url, options = {}, retries = 3) {
  let attempt = 0;
  while (true) {
    attempt += 1;
    const response = await fetch(url, options);
    if (!isRetryableStatus(response.status) || attempt > retries) {
      return response;
    }
    const delayMs = Math.min(5000, 400 * 2 ** (attempt - 1));
    await sleep(delayMs);
  }
}

const azureTokenCache = new Map();

function getAzureTokenCacheKey(credentials) {
  const tenantId = String(credentials?.tenantId || '').trim();
  const clientId = String(credentials?.clientId || '').trim();
  const clientSecret = String(credentials?.clientSecret || '').trim();
  const secretHash = createHash('sha1').update(clientSecret).digest('hex');
  return `${tenantId}::${clientId}::${secretHash}`;
}

async function fetchAzureAccessToken(credentials) {
  const tenantId = String(credentials?.tenantId || '').trim();
  const clientId = String(credentials?.clientId || '').trim();
  const clientSecret = String(credentials?.clientSecret || '').trim();
  if (!tenantId || !clientId || !clientSecret) {
    throw new Error('Azure tag operations require tenantId, clientId, and clientSecret.');
  }

  const cacheKey = getAzureTokenCacheKey(credentials);
  const cached = azureTokenCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now() + 30_000) {
    return cached.accessToken;
  }

  const tokenRes = await fetchWithRetry(
    `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
    {
      method: 'POST',
      headers: {
        'content-type': 'application/x-www-form-urlencoded'
      },
      body: new URLSearchParams({
        client_id: clientId,
        client_secret: clientSecret,
        grant_type: 'client_credentials',
        scope: 'https://management.azure.com/.default'
      })
    },
    4
  );

  if (!tokenRes.ok) {
    const details = await tokenRes.text().catch(() => '');
    throw new Error(`Azure token request failed (${tokenRes.status}): ${details.slice(0, 300)}`);
  }

  const tokenJson = await tokenRes.json();
  const accessToken = String(tokenJson?.access_token || '').trim();
  if (!accessToken) {
    throw new Error('Azure token response missing access_token.');
  }

  const expiresInSec = Number(tokenJson?.expires_in || 3000);
  azureTokenCache.set(cacheKey, {
    accessToken,
    expiresAt: Date.now() + Math.max(60, expiresInSec - 60) * 1000
  });
  return accessToken;
}

async function fetchAzureTags(resourceRef, credentials) {
  const normalizedRef = ensureAzureTaggableResourceRef(resourceRef);
  const accessToken = await fetchAzureAccessToken(credentials);
  const endpoint = `https://management.azure.com${normalizedRef}/providers/Microsoft.Resources/tags/default?api-version=2021-04-01`;

  const response = await fetchWithRetry(
    endpoint,
    {
      method: 'GET',
      headers: {
        authorization: `Bearer ${accessToken}`
      }
    },
    4
  );

  if (!response.ok) {
    const details = await response.text().catch(() => '');
    const parsed = parseAzureErrorPayload(details);

    if (parsed.code === 'InvalidResourceNamespace') {
      const error = new Error('Azure resource namespace is not taggable.');
      error.code = 'NON_TAGGABLE_AZURE_RESOURCE';
      throw error;
    }

    if (response.status === 404) {
      const error = new Error(`Azure resource not found: ${normalizedRef}`);
      error.code = 'AZURE_RESOURCE_NOT_FOUND';
      throw error;
    }

    const error = new Error(
      `Azure tag read failed (${response.status})${parsed.message ? `: ${parsed.message}` : ''}`
    );
    error.code = parsed.code || 'AZURE_TAG_READ_FAILED';
    throw error;
  }

  const payload = await response.json();
  const tags = payload?.properties?.tags;
  if (!tags || typeof tags !== 'object' || Array.isArray(tags)) {
    return {};
  }
  const normalized = {};
  for (const [rawKey, rawValue] of Object.entries(tags)) {
    const key = String(rawKey || '').trim();
    if (!key) {
      continue;
    }
    normalized[key] = String(rawValue === null || rawValue === undefined ? '' : rawValue).trim();
  }
  return normalized;
}

async function createAwsTaggingClient(credentials) {
  if (!awsTaggingSdk) {
    throw new Error('AWS tagging SDK is not installed.');
  }
  const accessKeyId = String(credentials?.accessKeyId || credentials?.accessKey || '').trim();
  const secretAccessKey = String(credentials?.secretAccessKey || credentials?.secretKey || '').trim();
  const sessionToken = String(credentials?.sessionToken || '').trim();
  const profile = String(credentials?.profile || '').trim();
  const region = String(credentials?.region || 'us-east-1').trim() || 'us-east-1';

  const clientConfig = { region };
  if (accessKeyId && secretAccessKey) {
    clientConfig.credentials = {
      accessKeyId,
      secretAccessKey,
      ...(sessionToken ? { sessionToken } : {})
    };
  } else if (profile) {
    process.env.AWS_PROFILE = profile;
  }

  return new awsTaggingSdk.ResourceGroupsTaggingAPIClient(clientConfig);
}

async function fetchAwsTags(resourceRef, credentials) {
  if (!String(resourceRef || '').startsWith('arn:')) {
    const error = new Error('AWS resource reference must be an ARN.');
    error.code = 'AWS_INVALID_RESOURCE_REF';
    throw error;
  }

  const client = await createAwsTaggingClient(credentials);
  const response = await client.send(
    new awsTaggingSdk.GetResourcesCommand({
      ResourceARNList: [resourceRef],
      ResourcesPerPage: 50,
      TagsPerPage: 100
    })
  );

  const mapping = Array.isArray(response?.ResourceTagMappingList) ? response.ResourceTagMappingList[0] : null;
  const tags = {};
  for (const row of Array.isArray(mapping?.Tags) ? mapping.Tags : []) {
    const key = String(row?.Key || '').trim();
    if (!key) {
      continue;
    }
    tags[key] = String(row?.Value || '').trim();
  }
  return tags;
}

function collectAzureTagCandidates(vendor, pulled, candidates) {
  const columns = Array.isArray(pulled?.raw?.costQuery?.properties?.columns)
    ? pulled.raw.costQuery.properties.columns
    : [];
  const rows = Array.isArray(pulled?.raw?.costQuery?.properties?.rows)
    ? pulled.raw.costQuery.properties.rows
    : [];
  if (!rows.length) {
    return;
  }

  const costIndex = getColumnIndex(columns, ['cost']);
  const resourceIdIndex = getColumnIndex(columns, ['resourceid', 'resource_id']);
  if (resourceIdIndex < 0) {
    return;
  }

  const accountId = String(pulled?.raw?.subscriptionId || vendor.subscriptionId || vendor.accountId || '').trim() || null;
  for (const row of rows) {
    if (!Array.isArray(row)) {
      continue;
    }
    const amount = Number(row[costIndex >= 0 ? costIndex : 0]);
    if (!Number.isFinite(amount) || amount <= 0) {
      continue;
    }
    const resourceRef = String(row[resourceIdIndex] || '').trim();
    if (!resourceRef) {
      continue;
    }
    const key = `azure::${vendor.id}::${resourceRef.toLowerCase()}`;
    if (!candidates.has(key)) {
      candidates.set(key, {
        provider: 'azure',
        resourceRef,
        vendorId: vendor.id,
        accountId
      });
    }
  }
}

function collectAwsTagCandidates(vendor, pulled, candidates) {
  const rows = Array.isArray(pulled?.raw?.result?.ResultsByTime) ? pulled.raw.result.ResultsByTime : [];
  if (!rows.length) {
    return;
  }

  const accountId = String(pulled?.raw?.accountId || vendor.accountId || '').trim() || null;
  for (const bucket of rows) {
    for (const group of Array.isArray(bucket?.Groups) ? bucket.Groups : []) {
      const keys = Array.isArray(group?.Keys) ? group.Keys : [];
      for (const keyRaw of keys) {
        const resourceRef = String(keyRaw || '').trim();
        if (!resourceRef.startsWith('arn:')) {
          continue;
        }
        const key = `aws::${vendor.id}::${resourceRef.toLowerCase()}`;
        if (!candidates.has(key)) {
          candidates.set(key, {
            provider: 'aws',
            resourceRef,
            vendorId: vendor.id,
            accountId
          });
        }
      }
    }
  }
}

function collectTagCandidates(vendor, pulled, candidates) {
  const provider = normalizeProvider(vendor.provider);
  if (provider === 'azure') {
    collectAzureTagCandidates(vendor, pulled, candidates);
  } else if (provider === 'aws') {
    collectAwsTagCandidates(vendor, pulled, candidates);
  }
}

async function syncTagCandidate(candidate, vendorCache) {
  const provider = normalizeProvider(candidate.provider);
  const vendor = vendorCache.get(candidate.vendorId) || null;
  if (!vendor) {
    throw new Error('Vendor not found for tag candidate.');
  }

  const credentials = decryptJson(vendor.credentialsEncrypted);
  if (!credentials) {
    throw new Error('Vendor credentials are not configured.');
  }

  let tags = {};
  if (provider === 'azure') {
    tags = await fetchAzureTags(candidate.resourceRef, credentials);
  } else if (provider === 'aws') {
    tags = await fetchAwsTags(candidate.resourceRef, credentials);
  } else {
    throw new Error(`Tag sync unsupported for provider ${provider}.`);
  }

  const saved = upsertResourceTag({
    provider,
    resourceRef: candidate.resourceRef,
    vendorId: candidate.vendorId,
    accountId: candidate.accountId,
    tags,
    source: 'cloud',
    syncedAt: nowIso()
  });
  return saved;
}

function shouldTreatAsExpectedTagFailure(error) {
  const code = String(error?.code || '').trim();
  const message = String(error?.message || '').trim();
  if (['NON_TAGGABLE_AZURE_RESOURCE', 'AZURE_INVALID_RESOURCE_REF', 'AWS_INVALID_RESOURCE_REF', 'AZURE_RESOURCE_NOT_FOUND'].includes(code)) {
    return true;
  }
  if (/InvalidResourceNamespace|not a taggable|must be a full ARM resource ID|must be an ARN|not found/i.test(message)) {
    return true;
  }
  return false;
}

async function main() {
  const lookbackMonths = Number(process.env.BILLING_BACKFILL_MONTHS || 48);
  const onlyMissing = parseBooleanEnv(process.env.BILLING_BACKFILL_ONLY_MISSING, false);
  const providerFilter = parseProviderFilter(process.env.BILLING_BACKFILL_PROVIDERS);
  const perCallDelayMs = Math.max(0, Number(process.env.BILLING_BACKFILL_DELAY_MS || 0));
  const billingRetryCount = Math.max(1, Number(process.env.BILLING_BACKFILL_BILLING_RETRIES || 4));
  const ranges = buildMonthlyRanges(lookbackMonths);
  const vendors = listVendors()
    .filter((vendor) => ['azure', 'aws', 'gcp'].includes(normalizeProvider(vendor.provider)))
    .filter((vendor) => (providerFilter ? providerFilter.has(normalizeProvider(vendor.provider)) : true));

  if (!ranges.length) {
    console.log('No billing ranges to process.');
    return;
  }
  if (!vendors.length) {
    console.log('No eligible vendors configured for billing backfill.');
    return;
  }

  const vendorCache = new Map();
  for (const vendor of vendors) {
    const fullVendor = getVendorById(vendor.id);
    if (fullVendor) {
      vendorCache.set(vendor.id, fullVendor);
    }
  }

  const stats = {
    months: ranges.length,
    vendors: vendors.length,
    billing: {
      attempted: 0,
      ok: 0,
      failed: 0
    },
    tags: {
      candidates: 0,
      cloudSynced: 0,
      localFallback: 0,
      failed: 0
    }
  };

  const startedAt = Date.now();
  const tagCandidates = new Map();
  const failureRows = [];

  console.log(
    `Starting billing backfill for ${ranges.length} month(s) across ${vendors.length} vendor(s): ` +
      `${ranges[0].periodStart} -> ${ranges[ranges.length - 1].periodEnd}` +
      ` | onlyMissing=${onlyMissing}` +
      ` | providers=${providerFilter ? Array.from(providerFilter).join(',') : 'all'}`
  );

  let monthIndex = 0;
  for (const range of ranges) {
    monthIndex += 1;
    console.log(
      `[${monthIndex}/${ranges.length}] Syncing period ${range.periodStart} to ${range.periodEnd}`
    );

    for (const vendorRow of vendors) {
      const vendor = vendorCache.get(vendorRow.id) || getVendorById(vendorRow.id);
      if (!vendor) {
        continue;
      }
      if (onlyMissing) {
        const existing = getBillingSnapshotForVendorPeriod(vendor.id, vendor.provider, range.periodStart, range.periodEnd);
        if (existing) {
          continue;
        }
      }
      const credentials = decryptJson(vendor.credentialsEncrypted);
      stats.billing.attempted += 1;

      if (!credentials) {
        stats.billing.failed += 1;
        failureRows.push({
          type: 'billing',
          vendorId: vendor.id,
          provider: vendor.provider,
          range,
          error: 'Missing credentials.'
        });
        continue;
      }

      try {
        const pulled = await pullBillingForVendorWithRetry(vendor, credentials, range, billingRetryCount);
        deleteBillingSnapshotsForVendorPeriod({
          vendorId: vendor.id,
          provider: vendor.provider,
          periodStart: pulled.periodStart,
          periodEnd: pulled.periodEnd
        });
        insertBillingSnapshot({
          vendorId: vendor.id,
          provider: vendor.provider,
          periodStart: pulled.periodStart,
          periodEnd: pulled.periodEnd,
          currency: pulled.currency,
          amount: pulled.amount,
          source: pulled.source,
          raw: pulled.raw,
          pulledAt: nowIso()
        });
        collectTagCandidates(vendor, pulled, tagCandidates);
        stats.billing.ok += 1;
      } catch (error) {
        stats.billing.failed += 1;
        failureRows.push({
          type: 'billing',
          vendorId: vendor.id,
          provider: vendor.provider,
          range,
          error: error?.message || 'Billing pull failed.'
        });
      }
      if (perCallDelayMs > 0) {
        await sleep(perCallDelayMs);
      }
    }
  }

  stats.tags.candidates = tagCandidates.size;
  console.log(`Billing backfill complete. Tag candidates discovered: ${tagCandidates.size}`);

  let tagIndex = 0;
  for (const candidate of tagCandidates.values()) {
    tagIndex += 1;
    if (tagIndex % 100 === 0 || tagIndex === tagCandidates.size) {
      console.log(`[tags] ${tagIndex}/${tagCandidates.size}`);
    }

    try {
      await syncTagCandidate(candidate, vendorCache);
      stats.tags.cloudSynced += 1;
    } catch (error) {
      if (shouldTreatAsExpectedTagFailure(error)) {
        const existing = getResourceTag(candidate.provider, candidate.resourceRef);
        upsertResourceTag({
          provider: candidate.provider,
          resourceRef: candidate.resourceRef,
          vendorId: candidate.vendorId,
          accountId: candidate.accountId,
          tags: existing?.tags || {},
          source: existing?.source || 'local',
          syncedAt: existing?.syncedAt || null
        });
        stats.tags.localFallback += 1;
      } else {
        stats.tags.failed += 1;
        failureRows.push({
          type: 'tag',
          vendorId: candidate.vendorId,
          provider: candidate.provider,
          resourceRef: candidate.resourceRef,
          error: error?.message || 'Tag sync failed.'
        });
      }
    }
  }

  const elapsedSec = Math.max(1, Math.round((Date.now() - startedAt) / 1000));
  const failurePreview = failureRows.slice(0, 25);
  const summary = {
    startedAt: new Date(startedAt).toISOString(),
    finishedAt: new Date().toISOString(),
    elapsedSec,
    lookbackMonths,
    ranges: {
      periodStart: ranges[0].periodStart,
      periodEnd: ranges[ranges.length - 1].periodEnd
    },
    stats,
    failures: {
      total: failureRows.length,
      preview: failurePreview
    }
  };

  console.log('--- BACKFILL SUMMARY ---');
  console.log(JSON.stringify(summary, null, 2));
}

function getBillingSnapshotForVendorPeriod(vendorId, provider, periodStart, periodEnd) {
  const row = db
    .prepare(
      `
      SELECT id
      FROM billing_snapshots
      WHERE vendor_id = ? AND provider = ? AND period_start = ? AND period_end = ?
      LIMIT 1
    `
    )
    .get(String(vendorId || '').trim(), normalizeProvider(provider), String(periodStart || '').trim(), String(periodEnd || '').trim());
  return row || null;
}

function isRetryableBillingError(error) {
  const message = String(error?.message || '').trim();
  return /Too many requests|429|timeout|temporar|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN/i.test(message);
}

async function pullBillingForVendorWithRetry(vendor, credentials, range, maxAttempts = 4) {
  let attempt = 0;
  let lastError = null;
  while (attempt < maxAttempts) {
    attempt += 1;
    try {
      return await pullBillingForVendor(vendor, credentials, range);
    } catch (error) {
      lastError = error;
      if (!isRetryableBillingError(error) || attempt >= maxAttempts) {
        break;
      }
      const delayMs = Math.min(10_000, 1000 * 2 ** (attempt - 1));
      await sleep(delayMs);
    }
  }
  throw lastError || new Error('Billing pull failed.');
}

main().catch((error) => {
  console.error('Backfill failed:', error?.stack || error?.message || String(error));
  process.exitCode = 1;
});
