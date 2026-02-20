const express = require('express');
const crypto = require('crypto');
const fs = require('fs');
const {
  db,
  listVendors,
  getVendorById,
  upsertVendor,
  insertBillingSnapshot,
  deleteBillingSnapshotsForVendorPeriod,
  listBillingSnapshots,
  summarizeBillingByProvider,
  listBillingBudgetPlans,
  replaceBillingBudgetPlans,
  insertSyncRun,
  finishSyncRun,
  getResourceTag,
  upsertResourceTag
} = require('../../db');
const { decryptJson, encryptJson } = require('../../crypto');
const { pullBillingForVendor, resolveBillingRange } = require('../../connectors/billing');

const app = express();
app.set('etag', false);
app.use(express.json({ limit: '20mb' }));

const BACKFILL_MAX_LOOKBACK_MONTHS = Math.max(1, Number(process.env.BILLING_BACKFILL_MAX_MONTHS || 120));
const BACKFILL_DEFAULT_LOOKBACK_MONTHS = Math.max(
  1,
  Math.min(BACKFILL_MAX_LOOKBACK_MONTHS, Number(process.env.BILLING_BACKFILL_DEFAULT_MONTHS || 48))
);
const BILLING_SUPPORTED_PROVIDERS = new Set(['azure', 'aws', 'gcp', 'rackspace', 'wasabi', 'wasabi-main']);
const BILLING_PROVIDER_LABELS = Object.freeze({
  azure: 'AZURE',
  aws: 'AWS',
  gcp: 'GCP',
  rackspace: 'RACKSPACE',
  wasabi: 'WASABI-WACM',
  'wasabi-wacm': 'WASABI-WACM',
  'wasabi-main': 'WASABI-MAIN',
  private: 'PRIVATE',
  vsax: 'VSAX',
  other: 'OTHER'
});
const WASABI_MAIN_AUTO_TAGS = Object.freeze({
  org: 'lkoasis',
  product: 'lkemrarchive'
});

const billingBackfillState = {
  running: false,
  jobId: null,
  startedAt: null,
  finishedAt: null,
  options: null,
  progress: null,
  summary: null,
  error: null,
  failuresPreview: []
};

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

function getBillingProviderLabel(provider) {
  const normalized = normalizeProvider(provider);
  return BILLING_PROVIDER_LABELS[normalized] || String(normalized || '').toUpperCase() || 'PROVIDER';
}

function isBillingProvider(value) {
  return BILLING_SUPPORTED_PROVIDERS.has(normalizeProvider(value));
}

function resolveRequestedBillingRange(source = {}) {
  const hasStart = Object.prototype.hasOwnProperty.call(source || {}, 'periodStart');
  const hasEnd = Object.prototype.hasOwnProperty.call(source || {}, 'periodEnd');
  if (!hasStart && !hasEnd) {
    return null;
  }
  return resolveBillingRange({
    periodStart: source?.periodStart,
    periodEnd: source?.periodEnd
  });
}

function parseBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  return ['1', 'true', 'yes', 'y', 'on'].includes(normalized);
}

function parseBudgetAmount(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  const numeric = Number(String(value).replace(/[$,\s]/g, ''));
  if (!Number.isFinite(numeric) || numeric < 0) {
    return null;
  }
  return numeric;
}

function normalizeLookbackMonths(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return BACKFILL_DEFAULT_LOOKBACK_MONTHS;
  }
  return Math.min(BACKFILL_MAX_LOOKBACK_MONTHS, Math.max(1, Math.floor(numeric)));
}

function toDateOnly(date) {
  return date.toISOString().slice(0, 10);
}

function buildMonthlyRanges(lookbackMonths = BACKFILL_DEFAULT_LOOKBACK_MONTHS, referenceDate = new Date()) {
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

function selectBillingBackfillVendors(options = {}) {
  const providerFilter = options.provider ? normalizeProvider(options.provider) : null;
  const vendorIds = Array.isArray(options.vendorIds)
    ? options.vendorIds.map((value) => String(value || '').trim()).filter(Boolean)
    : [];
  const vendorIdSet = new Set(vendorIds);

  const vendors = listVendors()
    .filter((vendor) => isBillingProvider(vendor.provider))
    .filter((vendor) => (providerFilter ? normalizeProvider(vendor.provider) === providerFilter : true))
    .filter((vendor) => (vendorIdSet.size ? vendorIdSet.has(vendor.id) : true));

  return {
    providerFilter,
    vendorIds,
    vendors
  };
}

function calculateMissingBackfillCoverage(options = {}) {
  const lookbackMonths = normalizeLookbackMonths(options.lookbackMonths);
  const ranges = buildMonthlyRanges(lookbackMonths);
  const selection = selectBillingBackfillVendors(options);
  const missingByProvider = {};
  const missingPreview = [];
  let missingVendorMonths = 0;

  for (const range of ranges) {
    for (const vendor of selection.vendors) {
      const exists = billingSnapshotExists(vendor.id, vendor.provider, range.periodStart, range.periodEnd);
      if (exists) {
        continue;
      }
      missingVendorMonths += 1;
      const providerKey = normalizeProvider(vendor.provider);
      missingByProvider[providerKey] = Number(missingByProvider[providerKey] || 0) + 1;
      if (missingPreview.length < 100) {
        missingPreview.push({
          vendorId: vendor.id,
          vendorName: vendor.name,
          provider: providerKey,
          periodStart: range.periodStart,
          periodEnd: range.periodEnd
        });
      }
    }
  }

  return {
    lookbackMonths,
    rangesCount: ranges.length,
    vendorCount: selection.vendors.length,
    totalVendorMonths: ranges.length * selection.vendors.length,
    missingVendorMonths,
    missingByProvider,
    missingPreview
  };
}

function billingSnapshotExists(vendorId, provider, periodStart, periodEnd) {
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
  return Boolean(row?.id);
}

function isRetryableBillingError(error) {
  const message = String(error?.message || '').trim();
  return /Too many requests|429|temporar|timeout|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN/i.test(message);
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
      const delayMs = Math.min(12_000, 1000 * 2 ** (attempt - 1));
      await sleep(delayMs);
    }
  }
  throw lastError || new Error('Billing pull failed.');
}

function getBackfillStatusPayload() {
  return {
    running: billingBackfillState.running,
    jobId: billingBackfillState.jobId,
    startedAt: billingBackfillState.startedAt,
    finishedAt: billingBackfillState.finishedAt,
    options: billingBackfillState.options,
    progress: billingBackfillState.progress,
    summary: billingBackfillState.summary,
    error: billingBackfillState.error,
    failuresPreview: billingBackfillState.failuresPreview
  };
}

async function runBillingBackfillJob(options = {}) {
  ensureBillingVendorsFromEnv();

  const lookbackMonths = normalizeLookbackMonths(options.lookbackMonths);
  const ranges = buildMonthlyRanges(lookbackMonths);
  const selection = selectBillingBackfillVendors(options);
  const providerFilter = selection.providerFilter;
  const vendorIds = selection.vendorIds;
  const onlyMissing = Boolean(options.onlyMissing);
  const delayMs = Math.max(0, Math.min(12_000, Number(options.delayMs || 0)));
  const retries = Math.max(1, Math.min(8, Number(options.retries || 4)));
  const vendors = selection.vendors;

  const jobId = crypto.randomUUID();
  const startedAt = toIsoNow();
  const totalVendorMonths = ranges.length * vendors.length;

  billingBackfillState.running = true;
  billingBackfillState.jobId = jobId;
  billingBackfillState.startedAt = startedAt;
  billingBackfillState.finishedAt = null;
  billingBackfillState.options = {
    lookbackMonths,
    provider: providerFilter || 'all',
    onlyMissing,
    delayMs,
    retries,
    vendorIds
  };
  billingBackfillState.progress = {
    totalVendorMonths,
    completedVendorMonths: 0,
    attemptedVendorMonths: 0,
    successVendorMonths: 0,
    failedVendorMonths: 0,
    skippedVendorMonths: 0,
    currentPeriodStart: null,
    currentPeriodEnd: null,
    currentVendorId: null,
    currentVendorName: null
  };
  billingBackfillState.summary = null;
  billingBackfillState.error = null;
  billingBackfillState.failuresPreview = [];

  if (!vendors.length || !ranges.length) {
    billingBackfillState.running = false;
    billingBackfillState.finishedAt = toIsoNow();
    billingBackfillState.summary = {
      vendors: vendors.length,
      months: ranges.length,
      attemptedVendorMonths: 0,
      successVendorMonths: 0,
      failedVendorMonths: 0,
      skippedVendorMonths: 0,
      ok: true
    };
    return getBackfillStatusPayload();
  }

  const failures = [];
  try {
    for (const range of ranges) {
      for (const vendorRow of vendors) {
        const progress = billingBackfillState.progress;
        progress.currentPeriodStart = range.periodStart;
        progress.currentPeriodEnd = range.periodEnd;
        progress.currentVendorId = vendorRow.id;
        progress.currentVendorName = vendorRow.name;

        if (onlyMissing && billingSnapshotExists(vendorRow.id, vendorRow.provider, range.periodStart, range.periodEnd)) {
          progress.skippedVendorMonths += 1;
          progress.completedVendorMonths += 1;
          continue;
        }

        progress.attemptedVendorMonths += 1;
        const vendor = getVendorById(vendorRow.id);
        if (!vendor) {
          progress.failedVendorMonths += 1;
          progress.completedVendorMonths += 1;
          const item = {
            type: 'billing',
            vendorId: vendorRow.id,
            provider: vendorRow.provider,
            periodStart: range.periodStart,
            periodEnd: range.periodEnd,
            error: 'Vendor not found.'
          };
          failures.push(item);
          if (billingBackfillState.failuresPreview.length < 50) {
            billingBackfillState.failuresPreview.push(item);
          }
          continue;
        }

        const credentials = decryptJson(vendor.credentialsEncrypted);
        if (!credentials) {
          progress.failedVendorMonths += 1;
          progress.completedVendorMonths += 1;
          const item = {
            type: 'billing',
            vendorId: vendor.id,
            provider: vendor.provider,
            periodStart: range.periodStart,
            periodEnd: range.periodEnd,
            error: 'Missing credentials.'
          };
          failures.push(item);
          if (billingBackfillState.failuresPreview.length < 50) {
            billingBackfillState.failuresPreview.push(item);
          }
          continue;
        }

        try {
          const pulled = await pullBillingForVendorWithRetry(vendor, credentials, range, retries);
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
            pulledAt: toIsoNow()
          });
          applyBillingAutoTags(vendor, pulled);
          progress.successVendorMonths += 1;
        } catch (error) {
          progress.failedVendorMonths += 1;
          const item = {
            type: 'billing',
            vendorId: vendor.id,
            provider: vendor.provider,
            periodStart: range.periodStart,
            periodEnd: range.periodEnd,
            error: error?.message || 'Billing pull failed.'
          };
          failures.push(item);
          if (billingBackfillState.failuresPreview.length < 50) {
            billingBackfillState.failuresPreview.push(item);
          }
        } finally {
          progress.completedVendorMonths += 1;
        }

        if (delayMs > 0) {
          await sleep(delayMs);
        }
      }
    }

    billingBackfillState.running = false;
    billingBackfillState.finishedAt = toIsoNow();
    billingBackfillState.progress.currentPeriodStart = null;
    billingBackfillState.progress.currentPeriodEnd = null;
    billingBackfillState.progress.currentVendorId = null;
    billingBackfillState.progress.currentVendorName = null;
    billingBackfillState.summary = {
      vendors: vendors.length,
      months: ranges.length,
      attemptedVendorMonths: billingBackfillState.progress.attemptedVendorMonths,
      successVendorMonths: billingBackfillState.progress.successVendorMonths,
      failedVendorMonths: billingBackfillState.progress.failedVendorMonths,
      skippedVendorMonths: billingBackfillState.progress.skippedVendorMonths,
      ok: billingBackfillState.progress.failedVendorMonths === 0,
      failureCount: failures.length
    };
    billingBackfillState.error = null;
    return getBackfillStatusPayload();
  } catch (error) {
    billingBackfillState.running = false;
    billingBackfillState.finishedAt = toIsoNow();
    billingBackfillState.error = error?.message || 'Billing backfill failed.';
    billingBackfillState.summary = {
      vendors: vendors.length,
      months: ranges.length,
      attemptedVendorMonths: billingBackfillState.progress.attemptedVendorMonths,
      successVendorMonths: billingBackfillState.progress.successVendorMonths,
      failedVendorMonths: billingBackfillState.progress.failedVendorMonths,
      skippedVendorMonths: billingBackfillState.progress.skippedVendorMonths,
      ok: false,
      failureCount: failures.length
    };
    throw error;
  }
}

function normalizeBillingResourceType(value) {
  const normalized = String(value || '')
    .replace(/\s+/g, ' ')
    .trim();
  return normalized || 'Uncategorized';
}

function normalizeCurrencyCode(value) {
  return String(value || 'USD').trim().toUpperCase() || 'USD';
}

function normalizeBudgetTargetType(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized === 'org' || normalized === 'product' || normalized === 'account') {
    return normalized;
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

function parseDateOnlyUtc(dateText) {
  const raw = String(dateText || '').trim();
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(raw);
  if (!match) {
    return null;
  }
  const year = Number(match[1]);
  const monthIndex = Number(match[2]) - 1;
  const day = Number(match[3]);
  if (
    !Number.isInteger(year) ||
    !Number.isInteger(monthIndex) ||
    !Number.isInteger(day) ||
    monthIndex < 0 ||
    monthIndex > 11 ||
    day < 1 ||
    day > 31
  ) {
    return null;
  }
  const date = new Date(Date.UTC(year, monthIndex, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== monthIndex ||
    date.getUTCDate() !== day
  ) {
    return null;
  }
  return date;
}

function getUtcMonthRange(year, month) {
  const safeYear = normalizeBudgetYear(year);
  const safeMonth = normalizeBudgetMonth(month);
  if (!safeMonth) {
    return null;
  }
  const periodStart = new Date(Date.UTC(safeYear, safeMonth - 1, 1));
  const periodEnd = new Date(Date.UTC(safeYear, safeMonth, 0));
  return {
    periodStart,
    periodEnd
  };
}

function getUtcYearRange(year) {
  const safeYear = normalizeBudgetYear(year);
  return {
    periodStart: new Date(Date.UTC(safeYear, 0, 1)),
    periodEnd: new Date(Date.UTC(safeYear, 11, 31))
  };
}

function getOverlapDaysInclusive(leftStart, leftEnd, rightStart, rightEnd) {
  const startMs = Math.max(leftStart.getTime(), rightStart.getTime());
  const endMs = Math.min(leftEnd.getTime(), rightEnd.getTime());
  if (endMs < startMs) {
    return 0;
  }
  return Math.floor((endMs - startMs) / 86_400_000) + 1;
}

function resolveBudgetPlanRange(plan = {}) {
  const budgetYear = normalizeBudgetYear(plan?.budgetYear);
  const budgetMonth = normalizeBudgetMonth(plan?.budgetMonth);
  return budgetMonth ? getUtcMonthRange(budgetYear, budgetMonth) : getUtcYearRange(budgetYear);
}

function doesBudgetPlanOverlapRange(plan, rangeStart, rangeEnd) {
  const planRange = resolveBudgetPlanRange(plan);
  if (!planRange) {
    return false;
  }
  const overlapDays = getOverlapDaysInclusive(planRange.periodStart, planRange.periodEnd, rangeStart, rangeEnd);
  return overlapDays > 0;
}

function prorateBudgetPlanAmountForRange(plan, rangeStart, rangeEnd) {
  const amount = Number(plan?.amount || 0);
  if (!Number.isFinite(amount)) {
    return 0;
  }
  const planRange = resolveBudgetPlanRange(plan);
  if (!planRange) {
    return 0;
  }
  const overlapDays = getOverlapDaysInclusive(planRange.periodStart, planRange.periodEnd, rangeStart, rangeEnd);
  if (!overlapDays) {
    return 0;
  }
  const planWindowDays = getOverlapDaysInclusive(planRange.periodStart, planRange.periodEnd, planRange.periodStart, planRange.periodEnd);
  if (!planWindowDays) {
    return 0;
  }
  return amount * (overlapDays / planWindowDays);
}

function buildAccountBudgetLookupForRange(plans = [], periodStart, periodEnd) {
  const startDate = parseDateOnlyUtc(periodStart);
  const endDate = parseDateOnlyUtc(periodEnd);
  if (!startDate || !endDate || endDate.getTime() < startDate.getTime()) {
    return new Map();
  }

  const lookup = new Map();
  for (const plan of Array.isArray(plans) ? plans : []) {
    if (!plan) {
      continue;
    }
    const targetType = normalizeBudgetTargetType(plan.targetType);
    if (targetType !== 'account') {
      continue;
    }
    const targetId = String(plan.targetId || '').trim();
    if (!targetId) {
      continue;
    }
    const currency = normalizeCurrencyCode(plan.currency);
    if (!doesBudgetPlanOverlapRange(plan, startDate, endDate)) {
      continue;
    }
    const contribution = prorateBudgetPlanAmountForRange(plan, startDate, endDate);
    if (!Number.isFinite(contribution)) {
      continue;
    }

    const current = lookup.get(targetId) || new Map();
    current.set(currency, (current.get(currency) || 0) + contribution);
    lookup.set(targetId, current);
  }
  return lookup;
}

function normalizeRackspaceRefSegment(value) {
  const text = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return text || 'unknown';
}

function parseJsonSafe(raw, fallback = null) {
  if (!raw) {
    return fallback;
  }
  try {
    return JSON.parse(raw);
  } catch (_error) {
    return fallback;
  }
}

function csvEscape(value) {
  const text = String(value === undefined || value === null ? '' : value);
  if (!/[",\r\n]/.test(text)) {
    return text;
  }
  return `"${text.replace(/"/g, '""')}"`;
}

function rowsToCsv(header = [], rows = []) {
  const safeHeader = Array.isArray(header) ? header : [];
  const safeRows = Array.isArray(rows) ? rows : [];
  const lines = [safeHeader.map((value) => csvEscape(value)).join(',')];
  for (const row of safeRows) {
    const line = safeHeader.map((key) => csvEscape(row?.[key])).join(',');
    lines.push(line);
  }
  return `${lines.join('\n')}\n`;
}

function resourceTagLookupKey(provider, resourceRef) {
  const normalizedProvider = normalizeProvider(provider);
  const normalizedRef = String(resourceRef || '').trim();
  if (!normalizedRef) {
    return '';
  }
  return `${normalizedProvider}::${normalizedRef}`;
}

function collectWasabiMainLineItemRefs(vendor, pulled = {}) {
  if (normalizeProvider(vendor?.provider) !== 'wasabi-main') {
    return [];
  }
  const lineItems = Array.isArray(pulled?.raw?.lineItems) ? pulled.raw.lineItems : [];
  if (!lineItems.length) {
    return [];
  }
  const fallbackAccountId = String(pulled?.raw?.accountId || vendor?.accountId || '').trim() || null;
  const refs = new Map();
  for (const line of lineItems) {
    const resourceRef = String(line?.resourceRef || line?.resource_ref || '').trim();
    if (!resourceRef) {
      continue;
    }
    if (!refs.has(resourceRef)) {
      const accountId = String(line?.accountId || line?.account_id || fallbackAccountId || '').trim() || null;
      refs.set(resourceRef, {
        resourceRef,
        accountId
      });
    }
  }
  return Array.from(refs.values());
}

function applyWasabiMainAutoTags(vendor, pulled = {}) {
  const refs = collectWasabiMainLineItemRefs(vendor, pulled);
  if (!refs.length) {
    return { provider: normalizeProvider(vendor?.provider), scanned: 0, updated: 0 };
  }
  let updated = 0;
  const vendorId = String(vendor?.id || '').trim() || null;
  for (const row of refs) {
    const existing = getResourceTag('wasabi-main', row.resourceRef);
    const existingTags =
      existing?.tags && typeof existing.tags === 'object' && !Array.isArray(existing.tags) ? existing.tags : {};
    const mergedTags = {
      ...existingTags,
      ...WASABI_MAIN_AUTO_TAGS
    };
    const changed = Object.keys(WASABI_MAIN_AUTO_TAGS).some(
      (key) => String(existingTags?.[key] ?? '') !== WASABI_MAIN_AUTO_TAGS[key]
    );
    if (!changed && existing) {
      continue;
    }
    upsertResourceTag({
      provider: 'wasabi-main',
      resourceRef: row.resourceRef,
      vendorId: vendorId || existing?.vendorId || null,
      accountId: row.accountId || existing?.accountId || null,
      tags: mergedTags,
      source: existing?.source || 'local',
      syncedAt: existing?.syncedAt || null
    });
    updated += 1;
  }
  return {
    provider: 'wasabi-main',
    scanned: refs.length,
    updated
  };
}

function extractWasabiAccountIdFromResourceRef(resourceRef) {
  const match = String(resourceRef || '')
    .trim()
    .match(/^(?:wasabi|wasabi-main):\/\/([^/]+)/i);
  return match ? String(match[1] || '').trim() : '';
}

function collectWasabiWacmLineItems(vendor, pulled = {}) {
  if (normalizeProvider(vendor?.provider) !== 'wasabi') {
    return [];
  }
  const lineItems = Array.isArray(pulled?.raw?.lineItems) ? pulled.raw.lineItems : [];
  if (!lineItems.length) {
    return [];
  }
  const fallbackAccountId = String(pulled?.raw?.accountId || vendor?.accountId || '').trim() || null;
  const refs = new Map();
  for (const line of lineItems) {
    const resourceRef = String(line?.resourceRef || line?.resource_ref || '').trim();
    if (!resourceRef) {
      continue;
    }
    const accountId =
      String(line?.accountId || line?.account_id || fallbackAccountId || '').trim() ||
      extractWasabiAccountIdFromResourceRef(resourceRef) ||
      null;
    if (!refs.has(resourceRef)) {
      refs.set(resourceRef, {
        resourceRef,
        accountId
      });
    }
  }
  return Array.from(refs.values());
}

function loadProviderAccountTagTemplate(provider, accountId) {
  const normalizedProvider = normalizeProvider(provider);
  const normalizedAccountId = String(accountId || '').trim();
  if (!normalizedAccountId) {
    return {};
  }
  const rows = db
    .prepare(
      `
      SELECT tags_json
      FROM resource_tags
      WHERE provider = ?
        AND account_id = ?
      ORDER BY updated_at ASC
    `
    )
    .all(normalizedProvider, normalizedAccountId);
  const merged = {};
  for (const row of rows) {
    const tags = parseJsonSafe(row?.tags_json, {});
    if (!tags || typeof tags !== 'object' || Array.isArray(tags)) {
      continue;
    }
    for (const [keyRaw, valueRaw] of Object.entries(tags)) {
      const key = String(keyRaw || '').trim();
      if (!key) {
        continue;
      }
      merged[key] = String(valueRaw === null || valueRaw === undefined ? '' : valueRaw).trim();
    }
  }
  return merged;
}

function applyWasabiWacmAccountTagInheritance(vendor, pulled = {}) {
  const lines = collectWasabiWacmLineItems(vendor, pulled);
  if (!lines.length) {
    return { provider: normalizeProvider(vendor?.provider), scanned: 0, updated: 0 };
  }
  const vendorId = String(vendor?.id || '').trim() || null;
  const byAccount = new Map();
  for (const line of lines) {
    const accountId = String(line?.accountId || '').trim();
    if (!accountId) {
      continue;
    }
    const accountRows = byAccount.get(accountId) || [];
    accountRows.push(line);
    byAccount.set(accountId, accountRows);
  }

  let updated = 0;
  let scanned = 0;
  for (const [accountId, accountRows] of byAccount.entries()) {
    const tagTemplate = loadProviderAccountTagTemplate('wasabi', accountId);
    const tagKeys = Object.keys(tagTemplate);
    if (!tagKeys.length) {
      continue;
    }
    for (const row of accountRows) {
      scanned += 1;
      const existing = getResourceTag('wasabi', row.resourceRef);
      const existingTags =
        existing?.tags && typeof existing.tags === 'object' && !Array.isArray(existing.tags) ? existing.tags : {};
      const changed = !existing || tagKeys.some((key) => String(existingTags?.[key] ?? '') !== String(tagTemplate[key]));
      if (!changed) {
        continue;
      }
      upsertResourceTag({
        provider: 'wasabi',
        resourceRef: row.resourceRef,
        vendorId: vendorId || existing?.vendorId || null,
        accountId: accountId || existing?.accountId || null,
        tags: {
          ...existingTags,
          ...tagTemplate
        },
        source: existing?.source || 'local',
        syncedAt: existing?.syncedAt || null
      });
      updated += 1;
    }
  }

  return {
    provider: 'wasabi',
    scanned,
    updated
  };
}

function applyBillingAutoTags(vendor, pulled = {}) {
  applyWasabiMainAutoTags(vendor, pulled);
  applyWasabiWacmAccountTagInheritance(vendor, pulled);
}

function formatTagsForCsv(tags) {
  if (!tags || typeof tags !== 'object' || Array.isArray(tags)) {
    return '';
  }
  const entries = Object.entries(tags)
    .map(([key, value]) => [String(key || '').trim(), String(value === null || value === undefined ? '' : value).trim()])
    .filter(([key]) => key.length > 0)
    .sort((left, right) => left[0].localeCompare(right[0]));
  if (!entries.length) {
    return '';
  }
  return entries.map(([key, value]) => `${key}=${value}`).join('; ');
}

function buildResourceTagLookupForCsv(detailRows = []) {
  const refsByProvider = new Map();

  for (const detail of Array.isArray(detailRows) ? detailRows : []) {
    const provider = normalizeProvider(detail?.provider);
    const resourceRef = String(detail?.resource_ref || detail?.resourceRef || '').trim();
    if (!resourceRef) {
      continue;
    }
    const providerRefs = refsByProvider.get(provider) || new Set();
    providerRefs.add(resourceRef);
    refsByProvider.set(provider, providerRefs);
  }

  const lookup = new Map();
  const maxRefsPerQuery = 400;

  for (const [provider, refs] of refsByProvider.entries()) {
    const refsList = Array.from(refs);
    for (let index = 0; index < refsList.length; index += maxRefsPerQuery) {
      const batch = refsList.slice(index, index + maxRefsPerQuery);
      if (!batch.length) {
        continue;
      }
      const placeholders = batch.map(() => '?').join(', ');
      const rows = db
        .prepare(
          `
          SELECT provider, resource_ref, tags_json, source, synced_at
          FROM resource_tags
          WHERE provider = ?
            AND resource_ref IN (${placeholders})
        `
        )
        .all(provider, ...batch);

      for (const row of rows) {
        const key = resourceTagLookupKey(row.provider, row.resource_ref);
        if (!key) {
          continue;
        }
        lookup.set(key, {
          tags: parseJsonSafe(row.tags_json, {}) || {},
          source: String(row.source || '').trim() || null,
          syncedAt: String(row.synced_at || '').trim() || null
        });
      }
    }
  }

  return lookup;
}

function appendTagColumnsToDetailRows(detailRows = []) {
  const lookup = buildResourceTagLookupForCsv(detailRows);
  return (Array.isArray(detailRows) ? detailRows : []).map((row) => {
    const key = resourceTagLookupKey(row.provider, row.resource_ref || row.resourceRef || '');
    const tagRow = key ? lookup.get(key) : null;
    const tags = tagRow?.tags && typeof tagRow.tags === 'object' ? tagRow.tags : {};
    return {
      ...row,
      tags: formatTagsForCsv(tags),
      tags_json: JSON.stringify(tags || {}),
      tag_source: tagRow?.source || '',
      tag_synced_at: tagRow?.syncedAt || ''
    };
  });
}

function toFileSlug(value, fallback = 'all') {
  const slug = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return slug || fallback;
}

const RACKSPACE_PERIOD_SUFFIX_REGEX =
  /\s*\((?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{4}\)\s*$/i;

function stripRackspacePeriodSuffix(value) {
  const normalized = String(value || '')
    .replace(/\s+/g, ' ')
    .trim();
  if (!normalized) {
    return '';
  }
  const cleaned = normalized.replace(RACKSPACE_PERIOD_SUFFIX_REGEX, '').trim();
  return cleaned || normalized;
}

function normalizeRackspaceResourceLabel(value) {
  const base = stripRackspacePeriodSuffix(value);
  if (!base) {
    return '';
  }
  return base
    .replace(/\s+Hosting Service$/i, '')
    .replace(/\s+Hypervisor$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseCsvRows(text) {
  let safeText = typeof text === 'string' ? text : '';
  if (safeText.charCodeAt(0) === 0xfeff) {
    safeText = safeText.slice(1);
  }
  const rows = [];
  let row = [];
  let value = '';
  let inQuotes = false;
  const pushValue = () => {
    row.push(value);
    value = '';
  };
  const pushRow = () => {
    if (row.length || value) {
      pushValue();
      rows.push(row);
      row = [];
    }
  };
  for (let i = 0; i < safeText.length; i += 1) {
    const char = safeText[i];
    if (inQuotes) {
      if (char === '"') {
        if (safeText[i + 1] === '"') {
          value += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        value += char;
      }
      continue;
    }
    if (char === '"') {
      inQuotes = true;
      continue;
    }
    if (char === ',') {
      pushValue();
      continue;
    }
    if (char === '\n') {
      pushRow();
      continue;
    }
    if (char === '\r') {
      continue;
    }
    value += char;
  }
  pushRow();
  return rows;
}

function parseRackspaceCsvDateTime(value) {
  const text = String(value || '').trim();
  if (!text) {
    return null;
  }
  const match = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (match) {
    const month = Number(match[1]);
    const day = Number(match[2]);
    const year = Number(match[3]);
    const hour = Number(match[4] || 0);
    const minute = Number(match[5] || 0);
    const second = Number(match[6] || 0);
    const parsed = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
    if (!Number.isNaN(parsed.getTime())) {
      return parsed;
    }
  }
  const fallback = new Date(text);
  if (Number.isNaN(fallback.getTime())) {
    return null;
  }
  return fallback;
}

function toDateOnlyUtc(value) {
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
    return null;
  }
  return value.toISOString().slice(0, 10);
}

function parseNumberOrZero(value) {
  const parsed = Number(String(value === undefined || value === null ? '' : value).replace(/[^0-9.+-]/g, ''));
  return Number.isFinite(parsed) ? parsed : 0;
}

function getRackspaceCsvPeriodEndDate(billEndDate) {
  if (!(billEndDate instanceof Date) || Number.isNaN(billEndDate.getTime())) {
    return null;
  }
  const inclusiveEnd = new Date(billEndDate.getTime() - 24 * 60 * 60 * 1000);
  return toDateOnlyUtc(inclusiveEnd);
}

function summarizeRackspaceUsageRowsByResourceType(rows = []) {
  const map = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const resourceType = normalizeBillingResourceType(row?.resourceType || row?.serviceType || row?.eventType || 'Uncategorized');
    const currency = normalizeCurrencyCode(row?.currency || 'USD');
    const amount = Number(row?.amount || 0);
    if (!Number.isFinite(amount) || amount === 0) {
      continue;
    }
    const key = `${resourceType}::${currency}`;
    const current = map.get(key) || {
      resourceType,
      currency,
      amount: 0
    };
    current.amount += amount;
    map.set(key, current);
  }
  return Array.from(map.values())
    .map((row) => ({
      resourceType: row.resourceType,
      currency: row.currency,
      amount: Math.round(row.amount * 100) / 100
    }))
    .sort((left, right) => right.amount - left.amount);
}

function parseRackspaceUsageCsv(csvText, options = {}) {
  const rows = parseCsvRows(csvText);
  if (!rows.length) {
    return {
      rows: [],
      groupedByPeriod: new Map(),
      parentAccountNumbers: [],
      accountNumbers: [],
      billNumbers: []
    };
  }
  const header = Array.isArray(rows[0]) ? rows[0] : [];
  const body = rows.slice(1);
  const indexByHeader = new Map(
    header.map((value, index) => [String(value || '').trim().toUpperCase(), index])
  );
  const col = (name, row) => {
    const idx = indexByHeader.get(String(name || '').trim().toUpperCase());
    if (idx === undefined || idx < 0) {
      return '';
    }
    return String(row[idx] || '').trim();
  };

  const normalizedRows = [];
  const groupedByPeriod = new Map();
  const parentAccountNumbers = new Set();
  const accountNumbers = new Set();
  const billNumbers = new Set();
  const sourceFileName = String(options.fileName || '').trim() || null;
  const importedAt = toIsoNow();

  for (const row of body) {
    if (!Array.isArray(row) || !row.length) {
      continue;
    }
    const amount = parseNumberOrZero(col('AMOUNT', row));
    if (!Number.isFinite(amount) || amount === 0) {
      continue;
    }
    const serviceTypeRaw = col('SERVICE_TYPE', row);
    const eventTypeRaw = col('EVENT_TYPE', row);
    const impactTypeRaw = col('IMPACT_TYPE', row);
    const serviceType = normalizeRackspaceResourceLabel(serviceTypeRaw) || stripRackspacePeriodSuffix(serviceTypeRaw) || 'Uncategorized';
    const eventType = stripRackspacePeriodSuffix(eventTypeRaw) || null;
    const impactType = String(impactTypeRaw || '').replace(/\s+/g, ' ').trim() || null;

    const billStartDate = parseRackspaceCsvDateTime(col('BILL_START_DATE', row));
    const billEndDate = parseRackspaceCsvDateTime(col('BILL_END_DATE', row));
    const periodStart = toDateOnlyUtc(billStartDate);
    const periodEnd = getRackspaceCsvPeriodEndDate(billEndDate);
    if (!periodStart || !periodEnd) {
      continue;
    }

    const eventStartDate = toDateOnlyUtc(parseRackspaceCsvDateTime(col('EVENT_START_DATE', row))) || periodStart;
    const eventEndDate = toDateOnlyUtc(parseRackspaceCsvDateTime(col('EVENT_END_DATE', row))) || eventStartDate;

    const accountNo = col('ACCOUNT_NO', row) || null;
    const parentAccountNo = col('PARENT_ACCOUNT_NO', row) || null;
    const billNo = col('BILL_NO', row) || null;
    const usageRecordId = col('USAGE_RECORD_ID', row) || null;
    const resId = col('RES_ID', row) || null;
    const resName = col('RES_NAME', row) || null;
    const resourceType = normalizeBillingResourceType(
      normalizeRackspaceResourceLabel(serviceType || eventType || 'Uncategorized') || serviceType || eventType || 'Uncategorized'
    );
    const fallbackDeviceName = String(
      `${serviceType}${eventType ? ` ${eventType}` : ''}${impactType ? ` (${impactType})` : ''}`
    )
      .replace(/\s+/g, ' ')
      .trim();
    const deviceName =
      resName ||
      resId ||
      fallbackDeviceName ||
      usageRecordId ||
      resourceType;
    const deviceKey = resId || resName || `${serviceType}:${eventType || impactType || 'shared'}`;
    const currency = normalizeCurrencyCode(col('CURRENCY', row) || 'USD');
    const resourceRef = `rackspace://${normalizeRackspaceRefSegment(
      accountNo || parentAccountNo || 'account'
    )}/${normalizeRackspaceRefSegment(resourceType)}/${normalizeRackspaceRefSegment(deviceKey)}`;

    if (parentAccountNo) {
      parentAccountNumbers.add(parentAccountNo);
    }
    if (accountNo) {
      accountNumbers.add(accountNo);
    }
    if (billNo) {
      billNumbers.add(billNo);
    }

    const normalized = {
      accountNo,
      parentAccountNo,
      billNo,
      periodStart,
      periodEnd,
      eventStartDate,
      eventEndDate,
      serviceType,
      eventType,
      impactType,
      amount,
      currency,
      usageRecordId,
      resId,
      resName,
      deviceName,
      resourceType,
      resourceRef,
      quantity: parseNumberOrZero(col('QUANTITY', row)),
      uom: col('UOM', row) || null,
      rate: parseNumberOrZero(col('RATE', row)),
      dcId: col('DC_ID', row) || null,
      regionId: col('REGION_ID', row) || null,
      attributes: {
        attribute1: col('ATTRIBUTE_1', row) || null,
        attribute2: col('ATTRIBUTE_2', row) || null,
        attribute3: col('ATTRIBUTE_3', row) || null,
        attribute4: col('ATTRIBUTE_4', row) || null,
        attribute5: col('ATTRIBUTE_5', row) || null,
        attribute6: col('ATTRIBUTE_6', row) || null,
        attribute7: col('ATTRIBUTE_7', row) || null,
        attribute8: col('ATTRIBUTE_8', row) || null
      },
      sourceFileName,
      importedAt
    };
    normalizedRows.push(normalized);

    const periodKey = `${periodStart}::${periodEnd}`;
    const periodRows = groupedByPeriod.get(periodKey) || [];
    periodRows.push(normalized);
    groupedByPeriod.set(periodKey, periodRows);
  }

  return {
    rows: normalizedRows,
    groupedByPeriod,
    parentAccountNumbers: Array.from(parentAccountNumbers),
    accountNumbers: Array.from(accountNumbers),
    billNumbers: Array.from(billNumbers)
  };
}

function resolveRackspaceVendorForCsvImport(vendorId, parsedCsv) {
  if (vendorId) {
    const vendor = getVendorById(vendorId);
    if (!vendor) {
      throw new Error('Rackspace vendor not found for CSV import.');
    }
    if (normalizeProvider(vendor.provider) !== 'rackspace') {
      throw new Error('Selected vendor is not a Rackspace provider.');
    }
    return vendor;
  }

  const rackspaceVendors = listVendors().filter((vendor) => normalizeProvider(vendor.provider) === 'rackspace');
  if (!rackspaceVendors.length) {
    throw new Error('No Rackspace vendor configured.');
  }
  if (rackspaceVendors.length === 1) {
    return rackspaceVendors[0];
  }

  const accountCandidates = new Set([
    ...(Array.isArray(parsedCsv?.parentAccountNumbers) ? parsedCsv.parentAccountNumbers : []),
    ...(Array.isArray(parsedCsv?.accountNumbers) ? parsedCsv.accountNumbers : [])
  ]);
  for (const candidate of accountCandidates) {
    const match = rackspaceVendors.find((vendor) => String(vendor?.accountId || '').trim() === String(candidate).trim());
    if (match) {
      return match;
    }
  }

  throw new Error('Multiple Rackspace vendors configured. Provide vendorId for CSV import.');
}

function findBillingSnapshotRow(vendorId, provider, periodStart, periodEnd) {
  return db
    .prepare(
      `
      SELECT id, raw_json, currency, amount, source
      FROM billing_snapshots
      WHERE vendor_id = ? AND provider = ? AND period_start = ? AND period_end = ?
      ORDER BY pulled_at DESC
      LIMIT 1
    `
    )
    .get(String(vendorId || '').trim(), normalizeProvider(provider), String(periodStart || '').trim(), String(periodEnd || '').trim());
}

function mergeRackspaceUsageCsvIntoSnapshots(vendor, parsedCsv, options = {}) {
  const vendorId = String(vendor?.id || '').trim();
  if (!vendorId) {
    throw new Error('Vendor ID is required for Rackspace CSV import.');
  }
  const importedAt = toIsoNow();
  const sourceFileName = String(options.fileName || '').trim() || null;
  const groups = parsedCsv?.groupedByPeriod instanceof Map ? parsedCsv.groupedByPeriod : new Map();
  const summary = {
    vendorId,
    periodsFound: groups.size,
    snapshotsUpdated: 0,
    snapshotsInserted: 0,
    rowsImported: 0,
    periods: [],
    sourceFileName
  };

  for (const [periodKey, periodRowsRaw] of groups.entries()) {
    const periodRows = Array.isArray(periodRowsRaw) ? periodRowsRaw : [];
    const [periodStart, periodEnd] = String(periodKey || '').split('::');
    if (!periodStart || !periodEnd || !periodRows.length) {
      continue;
    }
    const periodRowsSanitized = periodRows.map((row) => ({
      accountNo: row.accountNo || null,
      parentAccountNo: row.parentAccountNo || null,
      billNo: row.billNo || null,
      periodStart: row.periodStart || periodStart,
      periodEnd: row.periodEnd || periodEnd,
      eventStartDate: row.eventStartDate || null,
      eventEndDate: row.eventEndDate || null,
      serviceType: row.serviceType || null,
      eventType: row.eventType || null,
      impactType: row.impactType || null,
      amount: Number(row.amount || 0),
      currency: normalizeCurrencyCode(row.currency || 'USD'),
      usageRecordId: row.usageRecordId || null,
      resId: row.resId || null,
      resName: row.resName || null,
      deviceName: row.deviceName || null,
      resourceType: row.resourceType || null,
      resourceRef: row.resourceRef || null,
      quantity: Number(row.quantity || 0),
      uom: row.uom || null,
      rate: Number(row.rate || 0),
      dcId: row.dcId || null,
      regionId: row.regionId || null,
      attributes: row.attributes && typeof row.attributes === 'object' ? row.attributes : {},
      sourceFileName: row.sourceFileName || sourceFileName || null
    }));
    const breakdown = summarizeRackspaceUsageRowsByResourceType(periodRowsSanitized);
    const periodAmount = periodRowsSanitized.reduce((sum, row) => sum + Number(row.amount || 0), 0);
    const periodCurrency = normalizeCurrencyCode(periodRowsSanitized[0]?.currency || 'USD');
    const billNos = Array.from(
      new Set(periodRowsSanitized.map((row) => String(row.billNo || '').trim()).filter(Boolean))
    );
    const accountNos = Array.from(
      new Set(periodRowsSanitized.map((row) => String(row.accountNo || '').trim()).filter(Boolean))
    );

    const existing = findBillingSnapshotRow(vendorId, 'rackspace', periodStart, periodEnd);
    if (existing?.id) {
      const raw = parseJsonSafe(existing.raw_json, {}) || {};
      raw.rackspaceUsageRows = periodRowsSanitized;
      raw.rackspaceUsageCsvMeta = {
        importedAt,
        sourceFileName,
        rowCount: periodRowsSanitized.length,
        billNos,
        accountNos
      };
      raw.resourceBreakdown = breakdown;
      db.prepare(
        `
        UPDATE billing_snapshots
        SET raw_json = ?, pulled_at = ?
        WHERE id = ?
      `
      ).run(JSON.stringify(raw), importedAt, existing.id);
      summary.snapshotsUpdated += 1;
    } else {
      insertBillingSnapshot({
        vendorId,
        provider: 'rackspace',
        periodStart,
        periodEnd,
        currency: periodCurrency,
        amount: periodAmount,
        source: 'rackspace-csv-import',
        raw: {
          accountId: vendor.accountId || null,
          accountName: vendor.name || null,
          rackspaceUsageRows: periodRowsSanitized,
          rackspaceUsageCsvMeta: {
            importedAt,
            sourceFileName,
            rowCount: periodRowsSanitized.length,
            billNos,
            accountNos
          },
          resourceBreakdown: breakdown
        },
        pulledAt: importedAt
      });
      summary.snapshotsInserted += 1;
    }
    summary.rowsImported += periodRowsSanitized.length;
    summary.periods.push({
      periodStart,
      periodEnd,
      rowCount: periodRowsSanitized.length,
      amount: Math.round(periodAmount * 100) / 100,
      currency: periodCurrency,
      billNos
    });
  }

  summary.periods.sort((left, right) => String(left.periodStart || '').localeCompare(String(right.periodStart || '')));
  return summary;
}

function listBillingSnapshotsForExport(filters = {}) {
  const where = [];
  const args = [];

  if (filters.provider) {
    where.push('bs.provider = ?');
    args.push(normalizeProvider(filters.provider));
  }
  if (filters.vendorId) {
    where.push('bs.vendor_id = ?');
    args.push(String(filters.vendorId).trim());
  }
  if (filters.periodStart) {
    where.push('bs.period_start >= ?');
    args.push(String(filters.periodStart).trim());
  }
  if (filters.periodEnd) {
    where.push('bs.period_end <= ?');
    args.push(String(filters.periodEnd).trim());
  }

  const rows = db
    .prepare(
      `
      SELECT
        bs.id,
        bs.vendor_id,
        bs.provider,
        bs.period_start,
        bs.period_end,
        bs.currency,
        bs.amount,
        bs.source,
        bs.raw_json,
        bs.pulled_at,
        v.name AS vendor_name,
        v.account_id AS vendor_account_id,
        v.subscription_id AS vendor_subscription_id
      FROM billing_snapshots bs
      LEFT JOIN vendors v ON v.id = bs.vendor_id
      ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
      ORDER BY bs.period_start ASC, bs.provider ASC, bs.vendor_id ASC, bs.pulled_at ASC
    `
    )
    .all(...args);

  return rows.map((row) => ({
    id: row.id,
    vendorId: row.vendor_id,
    provider: normalizeProvider(row.provider),
    periodStart: row.period_start,
    periodEnd: row.period_end,
    currency: normalizeCurrencyCode(row.currency),
    amount: Number(row.amount || 0),
    source: String(row.source || '').trim(),
    raw: parseJsonSafe(row.raw_json, null),
    pulledAt: row.pulled_at,
    vendorName: String(row.vendor_name || '').trim() || null,
    accountId:
      String(row.vendor_account_id || '').trim() ||
      String(row.vendor_subscription_id || '').trim() ||
      null
  }));
}

function normalizeLookupKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

function normalizeAwsAccessKey(value) {
  return normalizeLookupKey(value);
}

function getColumnIndex(columns = [], candidateNames = []) {
  const normalizedCandidates = candidateNames.map((name) => String(name || '').trim().toLowerCase());
  return columns.findIndex((column) => {
    const columnName = String(column?.name || '').trim().toLowerCase();
    return normalizedCandidates.includes(columnName);
  });
}

function readAzureSubscriptionNameMap() {
  try {
    const rows = db
      .prepare('SELECT subscription_id, display_name FROM subscriptions')
      .all();
    const map = new Map();
    for (const row of rows) {
      const id = String(row?.subscription_id || '').trim();
      const name = String(row?.display_name || '').trim();
      if (!id || !name) {
        continue;
      }
      map.set(id, name);
    }
    return map;
  } catch (_error) {
    return new Map();
  }
}

function readAwsAccountLabelMap() {
  const accountIdMap = new Map();
  const accessKeyMap = new Map();
  const labels = [];
  const rawJson = String(process.env.AWS_ACCOUNTS_JSON || '').trim();
  if (rawJson) {
    try {
      const parsed = JSON.parse(rawJson);
      if (Array.isArray(parsed)) {
        for (const row of parsed) {
          if (!row || typeof row !== 'object') {
            continue;
          }
          const accountId = String(row.accountId || row.id || row.account || '').trim();
          const label = String(row.displayName || row.label || row.name || row.accountName || accountId).trim();
          if (label) {
            labels.push(label);
          }
          if (accountId && label) {
            accountIdMap.set(normalizeLookupKey(accountId), label);
          }
          const accessKeys = [
            row.accessKeyId,
            row.accessKey,
            row.awsAccessKeyId,
            row.keyId
          ];
          for (const accessKeyRaw of accessKeys) {
            const accessKey = normalizeAwsAccessKey(accessKeyRaw);
            if (!accessKey || !label) {
              continue;
            }
            accessKeyMap.set(accessKey, label);
          }
        }
      }
    } catch (_error) {
      // Ignore invalid AWS_ACCOUNTS_JSON and continue with defaults.
    }
  }

  const defaultAccountId = String(process.env.AWS_DEFAULT_ACCOUNT_ID || '').trim();
  const defaultLabel = String(process.env.AWS_DEFAULT_ACCOUNT_LABEL || '').trim();
  if (defaultAccountId && defaultLabel) {
    accountIdMap.set(normalizeLookupKey(defaultAccountId), defaultLabel);
    labels.push(defaultLabel);
  }
  return {
    accountIdMap,
    accessKeyMap,
    labels: Array.from(new Set(labels))
  };
}

function getAwsCredentialAccessKey(credentials) {
  if (!credentials || typeof credentials !== 'object') {
    return '';
  }
  return String(
    credentials.accessKeyId ||
      credentials.accessKey ||
      credentials.awsAccessKeyId ||
      credentials.keyId ||
      ''
  ).trim();
}

function parseAwsAccountsFromEnv() {
  const rawJson = String(process.env.AWS_ACCOUNTS_JSON || '').trim();
  if (!rawJson) {
    return [];
  }

  let parsed = null;
  try {
    parsed = JSON.parse(rawJson);
  } catch (_error) {
    return [];
  }
  if (!Array.isArray(parsed)) {
    return [];
  }

  const seenKeys = new Set();
  const accounts = [];
  for (const row of parsed) {
    if (!row || typeof row !== 'object') {
      continue;
    }

    const accountId = String(row.accountId || row.id || row.account || '').trim();
    const displayName = String(
      row.displayName || row.label || row.name || row.accountName || accountId || 'AWS account'
    ).trim();
    const accessKeyId = String(row.accessKeyId || row.accessKey || row.awsAccessKeyId || row.keyId || '').trim();
    const secretAccessKey = String(row.secretAccessKey || row.secretKey || row.awsSecretAccessKey || '').trim();
    const sessionToken = String(row.sessionToken || row.token || '').trim();
    const profile = String(row.profile || '').trim();
    const region = String(row.region || '').trim();

    if (!(accessKeyId && secretAccessKey) && !profile) {
      continue;
    }

    const dedupeKey = normalizeLookupKey(`${accountId}|${displayName}|${accessKeyId}|${profile}`);
    if (!dedupeKey || seenKeys.has(dedupeKey)) {
      continue;
    }
    seenKeys.add(dedupeKey);

    accounts.push({
      accountId,
      displayName,
      credentials: {
        ...(accessKeyId ? { accessKeyId } : {}),
        ...(secretAccessKey ? { secretAccessKey } : {}),
        ...(sessionToken ? { sessionToken } : {}),
        ...(profile ? { profile } : {}),
        ...(region ? { region } : {})
      }
    });
  }

  return accounts;
}

function readAwsVendorAccessKeyById(vendors = []) {
  const map = new Map();
  for (const vendor of Array.isArray(vendors) ? vendors : []) {
    if (normalizeProvider(vendor?.provider) !== 'aws') {
      continue;
    }
    const fullVendor = getVendorById(vendor.id);
    if (!fullVendor?.credentialsEncrypted) {
      continue;
    }
    const credentials = decryptJson(fullVendor.credentialsEncrypted);
    const accessKey = getAwsCredentialAccessKey(credentials);
    if (!accessKey) {
      continue;
    }
    map.set(vendor.id, accessKey);
  }
  return map;
}

function ensureAwsVendorsFromEnv() {
  const envAccounts = parseAwsAccountsFromEnv();
  if (!envAccounts.length) {
    return { created: 0 };
  }

  const awsVendors = listVendors().filter((vendor) => normalizeProvider(vendor?.provider) === 'aws');
  const vendorAccessKeys = readAwsVendorAccessKeyById(awsVendors);
  const vendorByAccessKey = new Map();
  const vendorByAccountId = new Map();

  for (const vendor of awsVendors) {
    const accessKey = normalizeAwsAccessKey(vendorAccessKeys.get(vendor.id));
    if (accessKey) {
      vendorByAccessKey.set(accessKey, vendor);
    }
    const accountKey = normalizeLookupKey(vendor.accountId);
    if (accountKey) {
      vendorByAccountId.set(accountKey, vendor);
    }
  }

  let created = 0;
  for (const envAccount of envAccounts) {
    const envAccessKey = normalizeAwsAccessKey(envAccount?.credentials?.accessKeyId);
    const envAccountKey = normalizeLookupKey(envAccount.accountId);

    if (envAccessKey && vendorByAccessKey.has(envAccessKey)) {
      continue;
    }
    if (envAccountKey && vendorByAccountId.has(envAccountKey)) {
      continue;
    }

    const credentials = envAccount.credentials || {};
    const hasCreds = Boolean(
      (credentials.accessKeyId && credentials.secretAccessKey) || credentials.profile
    );
    if (!hasCreds) {
      continue;
    }

    const createdVendor = upsertVendor({
      name: `AWS ${envAccount.displayName}`.trim(),
      provider: 'aws',
      cloudType: 'public',
      authMethod: 'api_key',
      accountId: envAccount.accountId || null,
      metadata: {
        source: 'aws_accounts_json',
        managedBy: 'cloudstudio-billing',
        displayName: envAccount.displayName
      },
      credentialsEncrypted: encryptJson(credentials)
    });
    created += 1;

    const createdAccessKey = normalizeAwsAccessKey(credentials.accessKeyId);
    if (createdAccessKey) {
      vendorByAccessKey.set(createdAccessKey, createdVendor);
    }
    const createdAccountKey = normalizeLookupKey(createdVendor.accountId || envAccount.accountId);
    if (createdAccountKey) {
      vendorByAccountId.set(createdAccountKey, createdVendor);
    }
  }

  return { created };
}

function parseRackspaceAccountsFromEnv() {
  const accounts = [];
  const seen = new Set();

  function pushAccount(input = {}) {
    if (!input || typeof input !== 'object') {
      return;
    }
    const accountId = String(
      input.accountId || input.account_id || input.accountNumber || input.ran || input.RAN || ''
    ).trim();
    const displayName = String(
      input.displayName ||
        input.accountName ||
        input.label ||
        input.name ||
        input.companyName ||
        accountId ||
        'Rackspace account'
    ).trim();
    const username = String(input.username || input.userName || input.user || '').trim();
    const apiKey = String(input.apiKey || input.api_key || input.apikey || '').trim();
    const billingBaseUrl = String(input.billingBaseUrl || input.baseUrl || input.baseURL || '').trim();
    const identityUrl = String(input.identityUrl || '').trim();
    const region = String(input.region || '').trim();

    if (!username || !apiKey) {
      return;
    }

    const dedupeKey = normalizeLookupKey(`${accountId}|${username}`);
    if (!dedupeKey || seen.has(dedupeKey)) {
      return;
    }
    seen.add(dedupeKey);

    accounts.push({
      accountId,
      displayName,
      credentials: {
        username,
        apiKey,
        ...(accountId ? { accountId } : {}),
        ...(billingBaseUrl ? { billingBaseUrl } : {}),
        ...(identityUrl ? { identityUrl } : {}),
        ...(region ? { region } : {})
      }
    });
  }

  const rawJson = String(process.env.RACKSPACE_ACCOUNTS_JSON || '').trim();
  if (rawJson) {
    try {
      const parsed = JSON.parse(rawJson);
      if (Array.isArray(parsed)) {
        for (const row of parsed) {
          pushAccount(row);
        }
      }
    } catch (_error) {
      // Ignore invalid RACKSPACE_ACCOUNTS_JSON and continue with defaults.
    }
  }

  pushAccount({
    accountId: process.env.RACKSPACE_ACCOUNT_ID,
    displayName: process.env.RACKSPACE_ACCOUNT_LABEL || process.env.RACKSPACE_ACCOUNT_NAME,
    username: process.env.RACKSPACE_USERNAME,
    apiKey: process.env.RACKSPACE_API_KEY,
    billingBaseUrl: process.env.RACKSPACE_BILLING_BASE_URL || process.env.RACKSPACE_BASE_URL,
    identityUrl: process.env.RACKSPACE_IDENTITY_URL,
    region: process.env.RACKSPACE_REGION
  });

  return accounts;
}

function readRackspaceAccountLabelMap() {
  const accountIdMap = new Map();
  const labels = [];
  const accounts = parseRackspaceAccountsFromEnv();
  for (const account of accounts) {
    const accountId = String(account?.accountId || '').trim();
    const label = String(account?.displayName || '').trim();
    if (label) {
      labels.push(label);
    }
    if (accountId && label) {
      accountIdMap.set(normalizeLookupKey(accountId), label);
    }
  }
  return {
    accountIdMap,
    labels: Array.from(new Set(labels))
  };
}

function ensureRackspaceVendorsFromEnv() {
  const envAccounts = parseRackspaceAccountsFromEnv();
  if (!envAccounts.length) {
    return { created: 0, updated: 0 };
  }

  const rackspaceVendors = listVendors().filter((vendor) => normalizeProvider(vendor?.provider) === 'rackspace');
  const vendorByAccountId = new Map();
  const vendorByUsername = new Map();

  for (const vendor of rackspaceVendors) {
    const accountKey = normalizeLookupKey(vendor.accountId);
    if (accountKey) {
      vendorByAccountId.set(accountKey, vendor);
    }
    const fullVendor = getVendorById(vendor.id);
    const credentials = decryptJson(fullVendor?.credentialsEncrypted);
    const username = String(credentials?.username || credentials?.userName || credentials?.user || '').trim();
    const usernameKey = normalizeLookupKey(username);
    if (usernameKey) {
      vendorByUsername.set(usernameKey, vendor);
    }
  }

  let created = 0;
  let updated = 0;
  for (const envAccount of envAccounts) {
    const accountKey = normalizeLookupKey(envAccount.accountId);
    const usernameKey = normalizeLookupKey(envAccount?.credentials?.username);
    if (accountKey && vendorByAccountId.has(accountKey)) {
      continue;
    }
    if (
      accountKey &&
      usernameKey &&
      envAccounts.length === 1 &&
      vendorByUsername.has(usernameKey)
    ) {
      const existingVendor = vendorByUsername.get(usernameKey);
      const existingFullVendor = existingVendor ? getVendorById(existingVendor.id) : null;
      const existingSource = String(existingFullVendor?.metadata?.source || '').trim().toLowerCase();
      if (existingVendor && (!existingSource || existingSource === 'rackspace_env')) {
        const display = String(
          envAccount.displayName || envAccount.accountId || envAccount?.credentials?.username || 'Account'
        ).trim();
        const normalizedName = /^rackspace\s+/i.test(display) ? display : `Rackspace ${display}`;
        const updatedVendor = upsertVendor({
          id: existingVendor.id,
          name: normalizedName,
          provider: 'rackspace',
          cloudType: 'public',
          authMethod: 'api_key',
          accountId: envAccount.accountId || null,
          metadata: {
            source: 'rackspace_env',
            managedBy: 'cloudstudio-billing',
            displayName: envAccount.displayName || null
          },
          credentialsEncrypted: encryptJson(envAccount.credentials || {})
        });
        updated += 1;
        if (accountKey) {
          vendorByAccountId.set(accountKey, updatedVendor);
        }
        if (usernameKey) {
          vendorByUsername.set(usernameKey, updatedVendor);
        }
        continue;
      }
    }
    if (!accountKey && usernameKey && vendorByUsername.has(usernameKey)) {
      continue;
    }

    const display = String(envAccount.displayName || envAccount.accountId || envAccount?.credentials?.username || 'Account').trim();
    const normalizedName = /^rackspace\s+/i.test(display) ? display : `Rackspace ${display}`;
    const createdVendor = upsertVendor({
      name: normalizedName,
      provider: 'rackspace',
      cloudType: 'public',
      authMethod: 'api_key',
      accountId: envAccount.accountId || null,
      metadata: {
        source: 'rackspace_env',
        managedBy: 'cloudstudio-billing',
        displayName: envAccount.displayName || null
      },
      credentialsEncrypted: encryptJson(envAccount.credentials || {})
    });
    created += 1;

    if (accountKey) {
      vendorByAccountId.set(accountKey, createdVendor);
    }
    if (usernameKey) {
      vendorByUsername.set(usernameKey, createdVendor);
    }
  }

  return { created, updated };
}

function parseWasabiWacmAccountsFromEnv() {
  const accounts = [];
  const seen = new Set();

  function pushAccount(input = {}) {
    if (!input || typeof input !== 'object') {
      return;
    }

    const rawAccountId = String(
      input.accountId ||
        input.subAccountId ||
        input.wasabiAccountNumber ||
        input.accountNumber ||
        ''
    ).trim();
    const accountId = /^\d+$/.test(rawAccountId) ? rawAccountId : '';
    const displayName = String(
      input.displayName ||
        input.accountName ||
        input.label ||
        input.name ||
        input.subAccountName ||
        input.subAccountEmail ||
        rawAccountId ||
        'Wasabi account'
    ).trim();
    const username = String(input.wacmUsername || input.username || input.userName || input.user || '').trim();
    const apiKey = String(input.wacmApiKey || input.apiKey || input.api_key || input.apikey || '').trim();
    const endpoint = String(input.wacmEndpoint || input.endpoint || input.baseUrl || '').trim();
    const dedupeKey = normalizeLookupKey(`${accountId}|${displayName}|${username}`);

    if (!username || !apiKey || !dedupeKey || seen.has(dedupeKey)) {
      return;
    }
    seen.add(dedupeKey);

    const credentials = {
      wacmUsername: username,
      wacmApiKey: apiKey
    };
    if (endpoint) {
      credentials.wacmEndpoint = endpoint;
    }
    if (accountId) {
      credentials.accountId = accountId;
    }

    accounts.push({
      accountId: accountId || null,
      displayName: displayName || 'Wasabi account',
      credentials
    });
  }

  const rawJson = String(process.env.WASABI_WACM_ACCOUNTS_JSON || '').trim();
  if (rawJson) {
    try {
      const parsed = JSON.parse(rawJson);
      if (Array.isArray(parsed)) {
        for (const row of parsed) {
          pushAccount(row);
        }
      }
    } catch (_error) {
      // Ignore invalid WASABI_WACM_ACCOUNTS_JSON and continue with defaults.
    }
  }

  pushAccount({
    accountId: process.env.WASABI_WACM_ACCOUNT_ID,
    displayName: process.env.WASABI_WACM_ACCOUNT_LABEL || process.env.WASABI_ACCOUNT_LABEL || 'Control account',
    wacmUsername: process.env.WASABI_WACM_USERNAME,
    wacmApiKey: process.env.WASABI_WACM_API_KEY,
    wacmEndpoint: process.env.WASABI_WACM_ENDPOINT
  });

  return accounts;
}

function ensureWasabiVendorsFromEnv() {
  const envAccounts = parseWasabiWacmAccountsFromEnv();
  if (!envAccounts.length) {
    return { created: 0 };
  }

  const wasabiVendors = listVendors().filter((vendor) => normalizeProvider(vendor?.provider) === 'wasabi');
  const vendorByAccountId = new Map();
  const vendorByUsername = new Map();

  for (const vendor of wasabiVendors) {
    const accountKey = normalizeLookupKey(vendor.accountId);
    if (accountKey) {
      vendorByAccountId.set(accountKey, vendor);
    }
    const fullVendor = getVendorById(vendor.id);
    const credentials = decryptJson(fullVendor?.credentialsEncrypted);
    const username = String(
      credentials?.wacmUsername ||
        credentials?.username ||
        credentials?.userName ||
        credentials?.user ||
        ''
    ).trim();
    const usernameKey = normalizeLookupKey(username);
    if (usernameKey) {
      vendorByUsername.set(usernameKey, vendor);
    }
  }

  let created = 0;
  for (const envAccount of envAccounts) {
    const accountKey = normalizeLookupKey(envAccount.accountId);
    const usernameKey = normalizeLookupKey(envAccount?.credentials?.wacmUsername);
    if (accountKey && vendorByAccountId.has(accountKey)) {
      continue;
    }
    if (!accountKey && usernameKey && vendorByUsername.has(usernameKey)) {
      continue;
    }

    const display = String(envAccount.displayName || envAccount.accountId || 'Account').trim();
    const normalizedName = /^wasabi\s+/i.test(display) ? display : `Wasabi ${display}`;
    const createdVendor = upsertVendor({
      name: normalizedName,
      provider: 'wasabi',
      cloudType: 'public',
      authMethod: 'api_key',
      accountId: envAccount.accountId || null,
      metadata: {
        source: 'wasabi_wacm_env',
        managedBy: 'cloudstudio-billing',
        displayName: envAccount.displayName || null
      },
      credentialsEncrypted: encryptJson(envAccount.credentials || {})
    });
    created += 1;

    if (accountKey) {
      vendorByAccountId.set(accountKey, createdVendor);
    }
    if (usernameKey) {
      vendorByUsername.set(usernameKey, createdVendor);
    }
  }

  return { created };
}

function parseWasabiMainAccountsFromEnv() {
  const accounts = [];
  const seen = new Set();

  function pushAccount(input = {}) {
    if (!input || typeof input !== 'object') {
      return;
    }

    const accountId = String(input.accountId || input.wasabiAccountNumber || input.accountNumber || '').trim();
    const displayName = String(
      input.displayName ||
        input.accountName ||
        input.label ||
        input.name ||
        process.env.WASABI_MAIN_ACCOUNT_LABEL ||
        process.env.WASABI_ACCOUNT_LABEL ||
        'Wasabi main'
    ).trim();
    const username = String(input.wacmUsername || input.username || input.userName || input.user || '').trim();
    const apiKey = String(input.wacmApiKey || input.apiKey || input.api_key || input.apikey || '').trim();
    const usageEndpoint = String(
      input.wacmUsageEndpoint || input.usageEndpoint || input.endpoint || input.baseUrl || ''
    ).trim();
    const dedupeKey = normalizeLookupKey(`${accountId}|${displayName}|${username}`);
    if (!username || !apiKey || !dedupeKey || seen.has(dedupeKey)) {
      return;
    }
    seen.add(dedupeKey);

    const credentials = {
      wacmUsername: username,
      wacmApiKey: apiKey
    };
    if (usageEndpoint) {
      credentials.wacmUsageEndpoint = usageEndpoint;
    }
    if (accountId) {
      credentials.accountId = accountId;
    }

    const optionalCredentialKeys = [
      'currency',
      'daysInMonth',
      'activeStorageCostPerTbMonth',
      'deletedStorageCostPerTbMonth',
      'minimumBillableTb',
      'apiCallsCostPerMillion',
      'ingressCostPerTb',
      'egressCostPerTb',
      'includeAllAccounts',
      'includeSubAccounts',
      'wacmControlUsageEndpoint',
      'controlUsageEndpoint'
    ];
    for (const key of optionalCredentialKeys) {
      if (Object.prototype.hasOwnProperty.call(input, key)) {
        credentials[key] = input[key];
      }
    }

    accounts.push({
      accountId: accountId || null,
      displayName: displayName || 'Wasabi main',
      credentials
    });
  }

  const rawJson = String(process.env.WASABI_MAIN_ACCOUNTS_JSON || '').trim();
  if (rawJson) {
    try {
      const parsed = JSON.parse(rawJson);
      if (Array.isArray(parsed)) {
        for (const row of parsed) {
          pushAccount(row);
        }
      }
    } catch (_error) {
      // Ignore invalid WASABI_MAIN_ACCOUNTS_JSON and continue with defaults.
    }
  }

  pushAccount({
    accountId: process.env.WASABI_MAIN_ACCOUNT_ID || process.env.WASABI_ACCOUNT_ID,
    displayName: process.env.WASABI_MAIN_ACCOUNT_LABEL || process.env.WASABI_ACCOUNT_LABEL || 'Main account',
    wacmUsername: process.env.WASABI_MAIN_WACM_USERNAME || process.env.WASABI_WACM_USERNAME,
    wacmApiKey: process.env.WASABI_MAIN_WACM_API_KEY || process.env.WASABI_WACM_API_KEY,
    wacmUsageEndpoint:
      process.env.WASABI_MAIN_USAGE_ENDPOINT ||
      process.env.WASABI_WACM_USAGE_ENDPOINT ||
      process.env.WASABI_WACM_ENDPOINT,
    wacmControlUsageEndpoint: process.env.WASABI_MAIN_CONTROL_USAGE_ENDPOINT,
    currency: process.env.WASABI_MAIN_CURRENCY,
    daysInMonth: process.env.WASABI_MAIN_DAYS_IN_MONTH,
    activeStorageCostPerTbMonth: process.env.WASABI_MAIN_ACTIVE_STORAGE_TB_MONTH,
    deletedStorageCostPerTbMonth: process.env.WASABI_MAIN_DELETED_STORAGE_TB_MONTH,
    minimumBillableTb: process.env.WASABI_MAIN_MIN_BILLABLE_TB,
    apiCallsCostPerMillion: process.env.WASABI_MAIN_API_CALLS_PER_MILLION,
    ingressCostPerTb: process.env.WASABI_MAIN_INGRESS_TB_COST,
    egressCostPerTb: process.env.WASABI_MAIN_EGRESS_TB_COST,
    includeAllAccounts: process.env.WASABI_MAIN_INCLUDE_ALL_ACCOUNTS,
    includeSubAccounts: process.env.WASABI_MAIN_INCLUDE_SUB_ACCOUNTS
  });

  return accounts;
}

function ensureWasabiMainVendorsFromEnv() {
  const envAccounts = parseWasabiMainAccountsFromEnv();
  if (!envAccounts.length) {
    return { created: 0 };
  }

  const vendors = listVendors().filter((vendor) => normalizeProvider(vendor?.provider) === 'wasabi-main');
  const vendorByAccountId = new Map();
  const vendorByUsername = new Map();

  for (const vendor of vendors) {
    const accountKey = normalizeLookupKey(vendor.accountId);
    if (accountKey) {
      vendorByAccountId.set(accountKey, vendor);
    }
    const fullVendor = getVendorById(vendor.id);
    const credentials = decryptJson(fullVendor?.credentialsEncrypted);
    const username = String(
      credentials?.wacmUsername ||
        credentials?.username ||
        credentials?.userName ||
        credentials?.user ||
        ''
    ).trim();
    const usernameKey = normalizeLookupKey(username);
    if (usernameKey) {
      vendorByUsername.set(usernameKey, vendor);
    }
  }

  let created = 0;
  for (const envAccount of envAccounts) {
    const accountKey = normalizeLookupKey(envAccount.accountId);
    const usernameKey = normalizeLookupKey(envAccount?.credentials?.wacmUsername);
    if (accountKey && vendorByAccountId.has(accountKey)) {
      continue;
    }
    if (!accountKey && usernameKey && vendorByUsername.has(usernameKey)) {
      continue;
    }

    const display = String(envAccount.displayName || envAccount.accountId || 'Main account').trim();
    const normalizedName = /^wasabi\s+/i.test(display) ? display : `Wasabi ${display}`;
    const createdVendor = upsertVendor({
      name: normalizedName,
      provider: 'wasabi-main',
      cloudType: 'public',
      authMethod: 'api_key',
      accountId: envAccount.accountId || null,
      metadata: {
        source: 'wasabi_main_env',
        managedBy: 'cloudstudio-billing',
        displayName: envAccount.displayName || null
      },
      credentialsEncrypted: encryptJson(envAccount.credentials || {})
    });
    created += 1;

    if (accountKey) {
      vendorByAccountId.set(accountKey, createdVendor);
    }
    if (usernameKey) {
      vendorByUsername.set(usernameKey, createdVendor);
    }
  }

  return { created };
}

function ensureBillingVendorsFromEnv() {
  const aws = ensureAwsVendorsFromEnv();
  const rackspace = ensureRackspaceVendorsFromEnv();
  const wasabi = ensureWasabiVendorsFromEnv();
  const wasabiMain = ensureWasabiMainVendorsFromEnv();
  return {
    created:
      Number(aws?.created || 0) +
      Number(rackspace?.created || 0) +
      Number(wasabi?.created || 0) +
      Number(wasabiMain?.created || 0),
    updated: Number(rackspace?.updated || 0),
    awsCreated: Number(aws?.created || 0),
    rackspaceCreated: Number(rackspace?.created || 0),
    rackspaceUpdated: Number(rackspace?.updated || 0),
    wasabiCreated: Number(wasabi?.created || 0),
    wasabiMainCreated: Number(wasabiMain?.created || 0)
  };
}

function resolveVendorAccountName(vendor, context = {}) {
  const provider = normalizeProvider(vendor?.provider);
  const name = String(vendor?.name || '').trim();
  if (provider === 'azure') {
    const subscriptionId = String(vendor?.subscriptionId || vendor?.accountId || '').trim();
    const mapped = subscriptionId ? context.azureSubscriptionNameMap?.get(subscriptionId) : null;
    if (mapped) {
      return mapped;
    }
    return name.replace(/^azure\s+/i, '').trim() || subscriptionId || 'Azure subscription';
  }

  if (provider === 'aws') {
    const accountId = String(vendor?.accountId || '').trim();
    const mappedById = context.awsAccountLookupById?.get(normalizeLookupKey(accountId));
    if (mappedById) {
      return mappedById;
    }
    const accessKey = String(context.awsVendorAccessKeyById?.get(vendor?.id) || '').trim();
    const mappedByAccessKey = context.awsAccountLookupByAccessKey?.get(normalizeAwsAccessKey(accessKey));
    if (mappedByAccessKey) {
      return mappedByAccessKey;
    }
    const labels = Array.isArray(context.awsAccountLabels) ? context.awsAccountLabels : [];
    if (labels.length === 1) {
      return labels[0];
    }
    const compactName = name.replace(/^aws\s+/i, '').replace(/^payer\s+/i, '').trim();
    return compactName || accountId || 'AWS account';
  }

  if (provider === 'rackspace') {
    const accountId = String(vendor?.accountId || '').trim();
    const mappedById = context.rackspaceAccountLookupById?.get(normalizeLookupKey(accountId));
    if (mappedById) {
      return mappedById;
    }
    const labels = Array.isArray(context.rackspaceAccountLabels) ? context.rackspaceAccountLabels : [];
    if (labels.length === 1) {
      return labels[0];
    }
    const compactName = name.replace(/^rackspace\s+/i, '').trim();
    return compactName || accountId || String(context.rackspaceDefaultUsername || '').trim() || 'Rackspace account';
  }

  if (provider === 'wasabi') {
    const accountId = String(vendor?.accountId || '').trim();
    const compactName = name.replace(/^wasabi\s+/i, '').trim();
    return compactName || accountId || 'Wasabi-WACM account';
  }

  if (provider === 'wasabi-main') {
    const accountId = String(vendor?.accountId || '').trim();
    const compactName = name.replace(/^wasabi\s+/i, '').trim();
    return compactName || accountId || 'Wasabi-MAIN account';
  }

  return name || String(vendor?.accountId || vendor?.subscriptionId || '').trim() || 'Account';
}

function summarizeBillingByResourceType(snapshots = []) {
  const resourceMap = new Map();
  const providerTotals = new Map();

  for (const snapshot of Array.isArray(snapshots) ? snapshots : []) {
    if (!snapshot) {
      continue;
    }

    const provider = normalizeProvider(snapshot.provider);
    const snapshotCurrency = normalizeCurrencyCode(snapshot.currency);
    const detailRows = extractSnapshotResourceDetails(snapshot);
    const rowsToSummarize = detailRows.length
      ? detailRows
      : (() => {
          const snapshotCurrency = normalizeCurrencyCode(snapshot.currency);
          const rawBreakdown = Array.isArray(snapshot?.raw?.resourceBreakdown) ? snapshot.raw.resourceBreakdown : [];
          const fallbackAmount = Number(snapshot.amount || 0);
          return rawBreakdown.length
            ? rawBreakdown
            : Number.isFinite(fallbackAmount) && fallbackAmount !== 0
            ? [
                {
                  resourceType: 'Uncategorized',
                  currency: snapshotCurrency,
                  amount: fallbackAmount
                }
              ]
            : [];
        })();

    for (const row of rowsToSummarize) {
      const amount = Number(row?.amount || 0);
      if (!Number.isFinite(amount) || amount === 0) {
        continue;
      }

      const resourceTypeRaw = normalizeBillingResourceType(row?.resourceType);
      const resourceType =
        provider === 'rackspace'
          ? normalizeBillingResourceType(normalizeRackspaceResourceLabel(resourceTypeRaw) || stripRackspacePeriodSuffix(resourceTypeRaw))
          : resourceTypeRaw;
      const currency = normalizeCurrencyCode(row?.currency || snapshotCurrency);
      const key = `${provider}::${resourceType}::${currency}`;
      const current = resourceMap.get(key) || {
        provider,
        resourceType,
        currency,
        totalAmount: 0,
        snapshotCount: 0
      };
      current.totalAmount += amount;
      current.snapshotCount += 1;
      resourceMap.set(key, current);

      const providerKey = `${provider}::${currency}`;
      providerTotals.set(providerKey, (providerTotals.get(providerKey) || 0) + amount);
    }
  }

  return Array.from(resourceMap.values())
    .map((row) => {
      const providerTotal = providerTotals.get(`${row.provider}::${row.currency}`) || 0;
      const sharePercent = providerTotal > 0 ? (row.totalAmount / providerTotal) * 100 : 0;
      return {
        ...row,
        providerLabel: getBillingProviderLabel(row.provider),
        sharePercent
      };
    })
    .sort((left, right) => {
      if (left.provider !== right.provider) {
        return left.provider.localeCompare(right.provider);
      }
      return right.totalAmount - left.totalAmount;
    });
}

function extractAzureResourceDetailRows(snapshot) {
  const query = snapshot?.raw?.costQuery;
  const columns = Array.isArray(query?.properties?.columns) ? query.properties.columns : [];
  const rows = Array.isArray(query?.properties?.rows) ? query.properties.rows : [];
  if (!rows.length) {
    return [];
  }

  const costIndex = getColumnIndex(columns, ['cost']);
  const currencyIndex = columns.findIndex((column) => String(column?.name || '').toLowerCase().includes('currency'));
  const serviceIndex = getColumnIndex(columns, [
    'servicename',
    'metercategory',
    'servicefamily',
    'resourcetype'
  ]);
  const resourceIdIndex = getColumnIndex(columns, ['resourceid', 'resource id']);
  const resourceGroupIndex = getColumnIndex(columns, ['resourcegroupname', 'resource group name']);
  const meterIndex = getColumnIndex(columns, ['metername', 'meter']);

  const details = [];
  for (const row of rows) {
    if (!Array.isArray(row)) {
      continue;
    }

    const amount = Number(row[costIndex >= 0 ? costIndex : 0]);
    if (!Number.isFinite(amount) || amount <= 0) {
      continue;
    }

    const resourceType = normalizeBillingResourceType(row[serviceIndex >= 0 ? serviceIndex : 1]);
    const currency = normalizeCurrencyCode(row[currencyIndex >= 0 ? currencyIndex : snapshot?.currency || 'USD']);
    const resourceRef = String(row[resourceIdIndex >= 0 ? resourceIdIndex : -1] || '').trim();
    const resourceGroup = String(row[resourceGroupIndex >= 0 ? resourceGroupIndex : -1] || '').trim();
    const meterName = String(row[meterIndex >= 0 ? meterIndex : -1] || '').trim();
    const resourceNameFromId = resourceRef ? resourceRef.split('/').filter(Boolean).pop() : '';
    const detailName = resourceNameFromId || meterName || resourceGroup || resourceType;

    details.push({
      provider: 'azure',
      resourceType,
      detailName,
      resourceRef,
      currency,
      amount,
      vendorId: snapshot.vendorId || null,
      accountId: String(snapshot?.raw?.subscriptionId || '').trim() || null
    });
  }
  return details;
}

function extractAwsResourceDetailRows(snapshot) {
  const first = Array.isArray(snapshot?.raw?.result?.ResultsByTime) ? snapshot.raw.result.ResultsByTime[0] : null;
  const groups = Array.isArray(first?.Groups) ? first.Groups : [];
  if (!groups.length) {
    return [];
  }

  const details = [];
  for (const group of groups) {
    const metric = group?.Metrics?.UnblendedCost || {};
    const amount = Number(metric?.Amount || 0);
    if (!Number.isFinite(amount) || amount <= 0) {
      continue;
    }
    const serviceName = normalizeBillingResourceType(group?.Keys?.[0], 'Other');
    const usageType = normalizeBillingResourceType(group?.Keys?.[1], serviceName);
    const accountId = String(snapshot?.raw?.accountId || '').trim() || null;
    const accountRef = accountId || String(snapshot?.vendorId || 'account').trim() || 'account';
    const resourceRef = `aws-billing://${normalizeRackspaceRefSegment(accountRef)}/${normalizeRackspaceRefSegment(
      serviceName
    )}/${normalizeRackspaceRefSegment(usageType)}`;
    const currency = normalizeCurrencyCode(metric?.Unit || snapshot?.currency || 'USD');
    details.push({
      provider: 'aws',
      resourceType: serviceName,
      detailName: usageType,
      resourceRef,
      currency,
      amount,
      vendorId: snapshot.vendorId || null,
      accountId,
      sourceType: 'aws_cost_usage_type'
    });
  }
  return details;
}

function extractRackspaceResourceDetailRows(snapshot) {
  const usageRows = Array.isArray(snapshot?.raw?.rackspaceUsageRows) ? snapshot.raw.rackspaceUsageRows : [];
  if (usageRows.length) {
    const details = [];
    for (const row of usageRows) {
      const amount = Number(row?.amount || 0);
      if (!Number.isFinite(amount) || amount === 0) {
        continue;
      }
      const resourceType = normalizeBillingResourceType(
        normalizeRackspaceResourceLabel(row?.resourceType || row?.serviceType || row?.eventType || 'Uncategorized') ||
          stripRackspacePeriodSuffix(row?.resourceType || row?.serviceType || row?.eventType || 'Uncategorized')
      );
      const csvDeviceName = String(
        row?.resName ||
          row?.resId ||
          `${String(row?.serviceType || resourceType)}${row?.eventType ? ` ${String(row.eventType)}` : ''}` ||
          ''
      )
        .replace(/\s+/g, ' ')
        .trim();
      const deviceName = String(csvDeviceName || row?.deviceName || row?.usageRecordId || row?.eventType || resourceType)
        .replace(/\s+/g, ' ')
        .trim();
      const accountId = String(row?.accountNo || row?.parentAccountNo || snapshot?.raw?.accountId || '').trim() || null;
      const resourceRef = String(row?.resourceRef || '').trim();
      details.push({
        provider: 'rackspace',
        resourceType,
        detailName: deviceName || resourceType,
        resourceRef,
        currency: normalizeCurrencyCode(row?.currency || snapshot?.currency || 'USD'),
        amount,
        vendorId: snapshot.vendorId || null,
        accountId,
        itemType: String(row?.impactType || '').trim() || null,
        lineItemId: String(row?.usageRecordId || '').trim() || null,
        sectionType: String(row?.eventType || '').trim() || null,
        invoiceId: String(row?.billNo || '').trim() || null,
        invoiceDate: null,
        coverageStartDate: String(row?.eventStartDate || row?.periodStart || '').trim() || null,
        coverageEndDate: String(row?.eventEndDate || row?.periodEnd || '').trim() || null,
        sourceType: 'rackspace_usage_csv'
      });
    }
    if (details.length) {
      return details;
    }
  }

  const invoiceDetails = Array.isArray(snapshot?.raw?.invoiceDetails) ? snapshot.raw.invoiceDetails : [];
  if (invoiceDetails.length) {
    const details = [];
    for (const invoicePayload of invoiceDetails) {
      const invoice = invoicePayload?.invoice || {};
      const invoiceId = String(invoice?.id || '').trim();
      const invoiceDate = String(invoice?.date || '').trim();
      const coverageStartDate = String(invoice?.coverageStartDate || '').trim();
      const coverageEndDate = String(invoice?.coverageEndDate || '').trim();
      const invoiceCurrency = normalizeCurrencyCode(invoice?.currency || snapshot?.currency || 'USD');
      const sections = Array.isArray(invoice?.invoiceSection) ? invoice.invoiceSection : [];
      for (const section of sections) {
        const sectionId = String(section?.sectionId || '').trim();
        const sectionType = String(section?.sectionType || '').trim();
        const invoiceItems = Array.isArray(section?.invoiceItem) ? section.invoiceItem : [];
        for (const item of invoiceItems) {
          const rawAmount =
            item?.itemAmount ??
            item?.amount ??
            item?.total ??
            item?.cost ??
            item?.charge ??
            item?.value;
          const amount = Number(rawAmount);
          const safeAmount = Number.isFinite(amount)
            ? amount
            : Number(String(rawAmount || '').replace(/[^0-9.+-]/g, ''));
          if (!Number.isFinite(safeAmount) || safeAmount <= 0) {
            continue;
          }

          const itemType = String(item?.itemType || '').trim();
          const itemName = stripRackspacePeriodSuffix(
            String(item?.name || item?.description || '').replace(/\s+/g, ' ').trim()
          );
          const itemId = String(item?.id || '').trim();
          const resourceType = normalizeBillingResourceType(
            normalizeRackspaceResourceLabel(itemName || itemType || 'Uncategorized') ||
              stripRackspacePeriodSuffix(itemName || itemType || 'Uncategorized')
          );
          const detailName = itemName || itemType || resourceType;
          const refAccount = sectionId || String(snapshot?.raw?.accountId || '').trim() || 'account';
          const refSegments = [
            normalizeRackspaceRefSegment(refAccount),
            normalizeRackspaceRefSegment(itemType || 'invoice-item'),
            normalizeRackspaceRefSegment(detailName)
          ];
          if (itemId) {
            refSegments.push(normalizeRackspaceRefSegment(itemId));
          } else if (invoiceId) {
            refSegments.push(normalizeRackspaceRefSegment(invoiceId));
          }
          const resourceRef = `rackspace://${refSegments.join('/')}`;

          details.push({
            provider: 'rackspace',
            resourceType,
            detailName,
            resourceRef,
            currency: invoiceCurrency,
            amount: safeAmount,
            vendorId: snapshot.vendorId || null,
            accountId: refAccount || null,
            itemType: itemType || null,
            lineItemId: itemId || null,
            sectionType: sectionType || null,
            invoiceId: invoiceId || null,
            invoiceDate: invoiceDate || null,
            coverageStartDate: coverageStartDate || null,
            coverageEndDate: coverageEndDate || null,
            sourceType: 'rackspace_invoice'
          });
        }
      }
    }
    if (details.length) {
      return details;
    }
  }

  const summary = snapshot?.raw?.billingSummary;
  const sourceRows = Array.isArray(summary?.billingSummary?.item) ? summary.billingSummary.item : [];
  const details = [];
  for (const row of sourceRows) {
    const rawAmount =
      row?.amount ??
        row?.total ??
        row?.cost ??
        row?.charge ??
        row?.chargeAmount ??
        row?.extendedAmount ??
        row?.value;
    const amount = Number(rawAmount);
    const safeAmount = Number.isFinite(amount)
      ? amount
      : Number(String(rawAmount || '').replace(/[^0-9.+-]/g, ''));
    if (!Number.isFinite(safeAmount) || safeAmount <= 0) {
      continue;
    }
    const resourceType = normalizeBillingResourceType(
      normalizeRackspaceResourceLabel(
        row?.resourceType ??
          row?.type ??
          row?.serviceName ??
          row?.service ??
          row?.category ??
          row?.description ??
          row?.name
      ) ||
        stripRackspacePeriodSuffix(
          row?.resourceType ??
            row?.type ??
            row?.serviceName ??
            row?.service ??
            row?.category ??
            row?.description ??
            row?.name
        )
    );
    const detailName = String(
      stripRackspacePeriodSuffix(
        row?.detailName ||
          row?.usageType ||
          row?.description ||
          row?.name ||
          row?.resourceName ||
          row?.serviceName ||
          row?.type ||
          resourceType
      )
    ).trim();
    const rawResourceRef = String(row?.resourceRef || row?.resourceId || row?.id || row?.itemId || '').trim();
    const refAccount = String(snapshot?.raw?.accountId || '').trim() || 'account';
    const rowId = String(row?.id || '').trim();
    const invoiceId = String(row?.invoiceId || row?.id || '').trim();
    const fallbackRefSegments = [
      normalizeRackspaceRefSegment(refAccount),
      normalizeRackspaceRefSegment(String(row?.type || 'summary')),
      normalizeRackspaceRefSegment(detailName || resourceType)
    ];
    if (rowId) {
      fallbackRefSegments.push(normalizeRackspaceRefSegment(rowId));
    } else if (invoiceId) {
      fallbackRefSegments.push(normalizeRackspaceRefSegment(invoiceId));
    }
    const resourceRef = rawResourceRef || `rackspace://${fallbackRefSegments.join('/')}`;
    const currency = normalizeCurrencyCode(
      row?.currency || row?.currencyCode || snapshot?.currency || summary?.currency || 'USD'
    );
    details.push({
      provider: 'rackspace',
      resourceType,
      detailName: detailName || resourceType,
      resourceRef,
      currency,
      amount: safeAmount,
      vendorId: snapshot.vendorId || null,
      accountId: String(snapshot?.raw?.accountId || '').trim() || null,
      itemType: String(row?.type || '').trim() || null,
      lineItemId: String(row?.id || '').trim() || null,
      sectionType: null,
      invoiceId: invoiceId || null,
      invoiceDate: String(row?.date || '').trim() || null,
      coverageStartDate: String(row?.coverageStartDate || '').trim() || null,
      coverageEndDate: String(row?.coverageEndDate || '').trim() || null,
      sourceType: 'rackspace_summary'
    });
  }

  return details;
}

function extractWasabiResourceDetailRows(snapshot) {
  const lineItems = Array.isArray(snapshot?.raw?.lineItems) ? snapshot.raw.lineItems : [];
  if (!lineItems.length) {
    return [];
  }

  const provider = normalizeProvider(snapshot?.provider);
  const sourceType = provider === 'wasabi-main' ? 'wasabi_main_usage' : 'wasabi_wacm';
  const details = [];
  for (const row of lineItems) {
    const amount = Number(row?.amount || 0);
    if (!Number.isFinite(amount) || amount <= 0) {
      continue;
    }
    const resourceType = normalizeBillingResourceType(row?.resourceType || 'Storage total');
    const accountId = String(
      row?.accountId ||
        row?.subAccountId ||
        row?.wasabiAccountNumber ||
        snapshot?.raw?.accountId ||
        ''
    ).trim() || null;
    const detailName = String(
      row?.detailName ||
        row?.accountName ||
        row?.subAccountName ||
        accountId ||
        resourceType
    )
      .replace(/\s+/g, ' ')
      .trim();
    const resourceRef = String(row?.resourceRef || '').trim();
    details.push({
      provider,
      resourceType,
      detailName: detailName || resourceType,
      resourceRef,
      currency: normalizeCurrencyCode(row?.currency || snapshot?.currency || 'USD'),
      amount,
      vendorId: snapshot.vendorId || null,
      accountId,
      itemType: provider === 'wasabi-main' ? 'USAGE' : 'WACM',
      lineItemId: String(row?.invoiceId || row?.id || '').trim() || null,
      sectionType: String(row?.resourceType || '').trim() || null,
      invoiceId: String(row?.controlInvoiceId || row?.invoiceId || '').trim() || null,
      invoiceDate: null,
      coverageStartDate: String(row?.periodStart || '').trim() || null,
      coverageEndDate: String(row?.periodEnd || '').trim() || null,
      sourceType: String(row?.sourceType || '').trim() || sourceType
    });
  }

  return details;
}

function extractGenericResourceDetailRows(snapshot) {
  const rows = Array.isArray(snapshot?.raw?.resourceBreakdown) ? snapshot.raw.resourceBreakdown : [];
  return rows
    .map((row) => {
      const amount = Number(row?.amount || 0);
      if (!Number.isFinite(amount) || amount <= 0) {
        return null;
      }
      const resourceType = normalizeBillingResourceType(row?.resourceType);
      return {
        provider: normalizeProvider(snapshot?.provider),
        resourceType,
        detailName: resourceType,
        resourceRef: '',
        currency: normalizeCurrencyCode(row?.currency || snapshot?.currency || 'USD'),
        amount,
        vendorId: snapshot?.vendorId || null,
        accountId: String(snapshot?.raw?.accountId || snapshot?.raw?.subscriptionId || '').trim() || null
      };
    })
    .filter(Boolean);
}

function extractSnapshotResourceDetails(snapshot) {
  const provider = normalizeProvider(snapshot?.provider);
  if (provider === 'azure') {
    const rows = extractAzureResourceDetailRows(snapshot);
    if (rows.length) {
      return rows;
    }
  }
  if (provider === 'aws') {
    const rows = extractAwsResourceDetailRows(snapshot);
    if (rows.length) {
      return rows;
    }
  }
  if (provider === 'wasabi' || provider === 'wasabi-main') {
    const rows = extractWasabiResourceDetailRows(snapshot);
    if (rows.length) {
      return rows;
    }
  }
  if (provider === 'rackspace') {
    const rows = extractRackspaceResourceDetailRows(snapshot);
    if (rows.length) {
      return rows;
    }
  }
  return extractGenericResourceDetailRows(snapshot);
}

function summarizeBillingResourceDetails(snapshots = []) {
  const groupMap = new Map();

  for (const snapshot of Array.isArray(snapshots) ? snapshots : []) {
    if (!snapshot) {
      continue;
    }
    const detailRows = extractSnapshotResourceDetails(snapshot);
    for (const detail of detailRows) {
      const provider = normalizeProvider(detail.provider || snapshot.provider);
      const resourceType = normalizeBillingResourceType(detail.resourceType);
      const currency = normalizeCurrencyCode(detail.currency || snapshot.currency || 'USD');
      const amount = Number(detail.amount || 0);
      if (!Number.isFinite(amount) || amount === 0) {
        continue;
      }

      const groupKey = `${provider}::${resourceType}::${currency}`;
      const group = groupMap.get(groupKey) || {
        provider,
        resourceType,
        currency,
        totalAmount: 0,
        detailsMap: new Map()
      };
      group.totalAmount += amount;

      const detailNameRaw = String(detail.detailName || resourceType).trim() || resourceType;
      const detailName =
        provider === 'rackspace'
          ? stripRackspacePeriodSuffix(detailNameRaw) || detailNameRaw
          : detailNameRaw;
      const resourceRef = String(detail.resourceRef || '').trim();
      const sourceType = String(detail.sourceType || '').trim();
      let detailKey = `${detailName}::${resourceRef}::${String(detail.vendorId || '')}::${String(
        detail.accountId || ''
      )}::${String(detail.itemType || '')}::${String(detail.lineItemId || '')}::${String(detail.invoiceId || '')}`;
      if (provider === 'rackspace') {
        if (sourceType === 'rackspace_usage_csv') {
          detailKey = `device::${resourceRef || detailName}::${String(detail.vendorId || '')}::${String(detail.accountId || '')}`;
        } else {
          detailKey = `line::${normalizeBillingResourceType(
            normalizeRackspaceResourceLabel(resourceType) || stripRackspacePeriodSuffix(resourceType)
          )}::${detailName}::${String(detail.vendorId || '')}::${String(detail.accountId || '')}::${String(
            detail.invoiceId || ''
          )}::${String(detail.coverageStartDate || '')}::${String(detail.coverageEndDate || '')}`;
        }
      }
      const current = group.detailsMap.get(detailKey) || {
        provider,
        resourceType,
        detailName,
        resourceRef,
        currency,
        amount: 0,
        snapshotCount: 0,
        vendorId: detail.vendorId || null,
        accountId: detail.accountId || null,
        itemType: detail.itemType || null,
        lineItemId: detail.lineItemId || null,
        sectionType: detail.sectionType || null,
        invoiceId: detail.invoiceId || null,
        invoiceDate: detail.invoiceDate || null,
        coverageStartDate: detail.coverageStartDate || null,
        coverageEndDate: detail.coverageEndDate || null,
        sourceType: sourceType || null
      };
      current.amount += amount;
      current.snapshotCount += 1;
      current.itemType = current.itemType || detail.itemType || null;
      current.lineItemId = current.lineItemId || detail.lineItemId || null;
      current.sectionType = current.sectionType || detail.sectionType || null;
      current.invoiceId = current.invoiceId || detail.invoiceId || null;
      current.invoiceDate = current.invoiceDate || detail.invoiceDate || null;
      current.coverageStartDate = current.coverageStartDate || detail.coverageStartDate || null;
      current.coverageEndDate = current.coverageEndDate || detail.coverageEndDate || null;
      group.detailsMap.set(detailKey, current);

      groupMap.set(groupKey, group);
    }
  }

  return Array.from(groupMap.values())
    .map((group) => ({
      provider: group.provider,
      resourceType: group.resourceType,
      currency: group.currency,
      totalAmount: group.totalAmount,
      details: Array.from(group.detailsMap.values()).sort((left, right) => {
        if (right.amount !== left.amount) {
          return right.amount - left.amount;
        }
        return String(left.detailName || '').localeCompare(String(right.detailName || ''));
      }),
      detailCount: group.detailsMap.size
    }))
    .sort((left, right) => {
      if (left.provider !== right.provider) {
        return left.provider.localeCompare(right.provider);
      }
      if (left.resourceType !== right.resourceType) {
        return left.resourceType.localeCompare(right.resourceType);
      }
      return left.currency.localeCompare(right.currency);
    });
}

function listBillingAccountTotals(filters = {}, context = {}) {
  const periodStart = String(filters.periodStart || '').trim();
  const periodEnd = String(filters.periodEnd || '').trim();
  const providerFilter = filters.provider ? normalizeProvider(filters.provider) : null;

  const where = ['period_start >= ?', 'period_end <= ?'];
  const args = [periodStart, periodEnd];
  if (providerFilter) {
    where.push('provider = ?');
    args.push(providerFilter);
  }

  const rows = db
    .prepare(
      `
      SELECT vendor_id, provider, currency, SUM(amount) AS total_amount, COUNT(*) AS snapshot_count, MAX(pulled_at) AS last_pulled_at
      FROM billing_snapshots
      WHERE ${where.join(' AND ')}
      GROUP BY vendor_id, provider, currency
      ORDER BY provider, vendor_id
    `
    )
    .all(...args);

  const vendors = listVendors()
    .filter((vendor) => isBillingProvider(vendor.provider))
    .filter((vendor) => (providerFilter ? vendor.provider === providerFilter : true));
  const vendorsById = new Map(vendors.map((vendor) => [vendor.id, vendor]));

  const totalsByVendor = new Map();
  for (const row of rows) {
    const vendorId = String(row.vendor_id || '').trim();
    const key = vendorId || `provider:${normalizeProvider(row.provider)}`;
    const current = totalsByVendor.get(key) || {
      vendorId: vendorId || null,
      provider: normalizeProvider(row.provider),
      currency: normalizeCurrencyCode(row.currency),
      totalAmount: 0,
      snapshotCount: 0,
      lastPulledAt: null
    };
    current.totalAmount += Number(row.total_amount || 0);
    current.snapshotCount += Number(row.snapshot_count || 0);
    const lastPulledAt = String(row.last_pulled_at || '').trim();
    if (!current.lastPulledAt || (lastPulledAt && lastPulledAt > current.lastPulledAt)) {
      current.lastPulledAt = lastPulledAt || current.lastPulledAt;
    }
    totalsByVendor.set(key, current);
  }

  const accounts = [];
  const seenVendorIds = new Set();
  for (const vendor of vendors) {
    seenVendorIds.add(vendor.id);
    const key = vendor.id;
    const total = totalsByVendor.get(key) || null;
    const accountId = String(vendor.accountId || vendor.subscriptionId || '').trim() || null;
    accounts.push({
      scopeId: `vendor:${vendor.id}`,
      vendorId: vendor.id,
      provider: vendor.provider,
      providerLabel: getBillingProviderLabel(vendor.provider),
      accountId,
      accountName: resolveVendorAccountName(vendor, context),
      totalAmount: Number(total?.totalAmount || 0),
      currency: total?.currency || 'USD',
      snapshotCount: Number(total?.snapshotCount || 0),
      lastPulledAt: total?.lastPulledAt || null
    });
  }

  for (const [key, total] of totalsByVendor.entries()) {
    if (!key.startsWith('provider:')) {
      if (total.vendorId && seenVendorIds.has(total.vendorId)) {
        continue;
      }
      const vendor = total.vendorId ? vendorsById.get(total.vendorId) : null;
      if (vendor) {
        continue;
      }
    }
    if (!key.startsWith('provider:')) {
      continue;
    }
    const provider = String(key.slice('provider:'.length) || total.provider).trim() || total.provider;
    accounts.push({
      scopeId: `provider:${provider}`,
      vendorId: null,
      provider,
      providerLabel: getBillingProviderLabel(provider),
      accountId: null,
      accountName: 'Unlinked account',
      totalAmount: Number(total.totalAmount || 0),
      currency: total.currency || 'USD',
      snapshotCount: Number(total.snapshotCount || 0),
      lastPulledAt: total.lastPulledAt || null
    });
  }

  accounts.sort((left, right) => {
    if (left.provider !== right.provider) {
      return left.provider.localeCompare(right.provider);
    }
    if (right.totalAmount !== left.totalAmount) {
      return right.totalAmount - left.totalAmount;
    }
    return String(left.accountName || '').localeCompare(String(right.accountName || ''));
  });

  const providerMap = new Map();
  const currencyMap = new Map();
  for (const row of accounts) {
    const providerKey = `${row.provider}::${row.currency}`;
    const providerCurrent = providerMap.get(providerKey) || {
      provider: row.provider,
      providerLabel: row.providerLabel,
      currency: row.currency,
      totalAmount: 0,
      accountCount: 0
    };
    providerCurrent.totalAmount += Number(row.totalAmount || 0);
    providerCurrent.accountCount += 1;
    providerMap.set(providerKey, providerCurrent);

    currencyMap.set(row.currency, (currencyMap.get(row.currency) || 0) + Number(row.totalAmount || 0));
  }

  const providerTotals = Array.from(providerMap.values()).sort((left, right) => {
    if (left.provider !== right.provider) {
      return left.provider.localeCompare(right.provider);
    }
    return right.totalAmount - left.totalAmount;
  });
  const grandTotals = Array.from(currencyMap.entries()).map(([currency, totalAmount]) => ({
    currency,
    totalAmount
  }));

  return {
    accounts,
    providerTotals,
    grandTotals
  };
}

function roundCurrencyAmount(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return Math.round(numeric * 100) / 100;
}

function buildBillingAccountContext(provider = null) {
  const normalizedProvider = provider ? normalizeProvider(provider) : null;
  const awsAccountLabels = readAwsAccountLabelMap();
  const rackspaceAccountLabels = readRackspaceAccountLabelMap();
  const contextVendors = listVendors()
    .filter((vendor) => isBillingProvider(vendor.provider))
    .filter((vendor) => (normalizedProvider ? vendor.provider === normalizedProvider : true));
  return {
    contextVendors,
    context: {
      azureSubscriptionNameMap: readAzureSubscriptionNameMap(),
      awsAccountLookupById: awsAccountLabels.accountIdMap,
      awsAccountLookupByAccessKey: awsAccountLabels.accessKeyMap,
      awsVendorAccessKeyById: readAwsVendorAccessKeyById(contextVendors),
      awsAccountLabels: awsAccountLabels.labels,
      rackspaceAccountLookupById: rackspaceAccountLabels.accountIdMap,
      rackspaceAccountLabels: rackspaceAccountLabels.labels,
      rackspaceDefaultUsername: String(process.env.RACKSPACE_USERNAME || '').trim()
    }
  };
}

function annotateAccountTotalsWithBudget(accounts = [], period = {}) {
  const safeAccounts = Array.isArray(accounts) ? accounts : [];
  const periodStart = String(period.periodStart || '').trim();
  const periodEnd = String(period.periodEnd || '').trim();
  const startDate = parseDateOnlyUtc(periodStart);
  const endDate = parseDateOnlyUtc(periodEnd);
  if (!startDate || !endDate || endDate.getTime() < startDate.getTime()) {
    return safeAccounts.map((row) => ({
      ...row,
      budgetAmount: 0,
      budgetCurrency: normalizeCurrencyCode(row?.currency || 'USD'),
      budgetConfigured: false,
      budgetDelta: roundCurrencyAmount(Number(row?.totalAmount || 0)),
      budgetStatus: 'unset'
    }));
  }

  const yearStart = startDate.getUTCFullYear();
  const yearEnd = endDate.getUTCFullYear();
  const plans = listBillingBudgetPlans({
    targetType: 'account',
    budgetYearStart: yearStart,
    budgetYearEnd: yearEnd,
    limit: 50_000
  });
  const lookup = buildAccountBudgetLookupForRange(plans, periodStart, periodEnd);

  return safeAccounts.map((row) => {
    const scopeId = String(row?.scopeId || '').trim();
    const accountBudgetByCurrency = scopeId ? lookup.get(scopeId) : null;
    const rowCurrency = normalizeCurrencyCode(row?.currency || 'USD');
    let budgetCurrency = rowCurrency;
    let budgetAmount = 0;
    let budgetConfigured = false;

    if (accountBudgetByCurrency instanceof Map && accountBudgetByCurrency.size > 0) {
      budgetConfigured = true;
      if (accountBudgetByCurrency.has(rowCurrency)) {
        budgetAmount = Number(accountBudgetByCurrency.get(rowCurrency) || 0);
        budgetCurrency = rowCurrency;
      } else {
        const firstBudget = accountBudgetByCurrency.entries().next().value || null;
        if (firstBudget) {
          budgetCurrency = normalizeCurrencyCode(firstBudget[0] || rowCurrency);
          budgetAmount = Number(firstBudget[1] || 0);
        }
      }
    }

    const totalAmount = Number(row?.totalAmount || 0);
    const delta = totalAmount - budgetAmount;
    let budgetStatus = 'unset';
    if (budgetConfigured) {
      if (Math.abs(delta) < 0.005) {
        budgetStatus = 'on-target';
      } else if (delta > 0) {
        budgetStatus = 'over';
      } else {
        budgetStatus = 'under';
      }
    }

    return {
      ...row,
      budgetAmount: roundCurrencyAmount(budgetAmount),
      budgetCurrency,
      budgetConfigured,
      budgetDelta: roundCurrencyAmount(delta),
      budgetStatus
    };
  });
}

function getBudgetYearRange(yearValue) {
  const year = normalizeBudgetYear(yearValue);
  return {
    year,
    periodStart: `${String(year).padStart(4, '0')}-01-01`,
    periodEnd: `${String(year).padStart(4, '0')}-12-31`
  };
}

function buildBillingBudgetAccountRowsForYear(yearValue, provider = null) {
  const yearRange = getBudgetYearRange(yearValue);
  const normalizedProvider = provider ? normalizeProvider(provider) : null;
  const { context } = buildBillingAccountContext(normalizedProvider);
  const totals = listBillingAccountTotals(
    {
      periodStart: yearRange.periodStart,
      periodEnd: yearRange.periodEnd,
      provider: normalizedProvider || null
    },
    context
  );

  const accountRows = (Array.isArray(totals.accounts) ? totals.accounts : []).filter((row) =>
    String(row?.scopeId || '').startsWith('vendor:')
  );
  const accountByScope = new Map(accountRows.map((row) => [String(row.scopeId || '').trim(), row]));
  const plans = listBillingBudgetPlans({
    targetType: 'account',
    budgetYear: yearRange.year,
    targetProvider: normalizedProvider || undefined,
    limit: 50_000
  });
  const monthTemplate = () => ({
    1: null,
    2: null,
    3: null,
    4: null,
    5: null,
    6: null,
    7: null,
    8: null,
    9: null,
    10: null,
    11: null,
    12: null
  });

  const rowsByScope = new Map();
  for (const account of accountRows) {
    const scopeId = String(account.scopeId || '').trim();
    if (!scopeId) {
      continue;
    }
    rowsByScope.set(scopeId, {
      scopeId,
      vendorId: account.vendorId || null,
      provider: normalizeProvider(account.provider),
      providerLabel: getBillingProviderLabel(account.provider),
      accountId: account.accountId || null,
      accountName: account.accountName || account.accountId || 'Account',
      currency: normalizeCurrencyCode(account.currency || 'USD'),
      months: monthTemplate()
    });
  }

  for (const plan of plans) {
    const scopeId = String(plan?.targetId || '').trim();
    if (!scopeId) {
      continue;
    }
    const providerCode = normalizeProvider(plan?.targetProvider || accountByScope.get(scopeId)?.provider || 'other');
    const existing = rowsByScope.get(scopeId) || {
      scopeId,
      vendorId: String(plan?.vendorId || '').trim() || null,
      provider: providerCode,
      providerLabel: getBillingProviderLabel(providerCode),
      accountId: String(plan?.accountId || '').trim() || null,
      accountName: String(plan?.targetLabel || '').trim() || String(plan?.accountId || '').trim() || scopeId,
      currency: normalizeCurrencyCode(plan?.currency || 'USD'),
      months: monthTemplate()
    };
    const amount = Number(plan.amount || 0);
    if (!Number.isFinite(amount) || amount < 0) {
      continue;
    }
    if (plan.budgetMonth) {
      const month = normalizeBudgetMonth(plan.budgetMonth);
      if (month) {
        const currentValue = Number(existing.months[month] || 0);
        existing.months[month] = roundCurrencyAmount(currentValue + amount);
      }
    } else {
      const distributed = amount / 12;
      for (let month = 1; month <= 12; month += 1) {
        const currentValue = Number(existing.months[month] || 0);
        existing.months[month] = roundCurrencyAmount(currentValue + distributed);
      }
    }
    rowsByScope.set(scopeId, existing);
  }

  const rows = Array.from(rowsByScope.values()).sort((left, right) => {
    if (left.provider !== right.provider) {
      return left.provider.localeCompare(right.provider);
    }
    const byName = String(left.accountName || '').localeCompare(String(right.accountName || ''));
    if (byName !== 0) {
      return byName;
    }
    return String(left.accountId || '').localeCompare(String(right.accountId || ''));
  });

  return {
    year: yearRange.year,
    rows
  };
}

app.get('/api/health', (_req, res) => {
  const vendorCount = listVendors().filter((vendor) => isBillingProvider(vendor.provider)).length;
  res.json({
    ok: true,
    vendors: vendorCount
  });
});

app.post('/api/vendors/:vendorId/test', async (req, res) => {
  const vendorId = String(req.params.vendorId || '').trim();
  const vendor = getVendorById(vendorId);
  if (!vendor) {
    return res.status(404).json({ error: 'Vendor not found.' });
  }

  const credentials = decryptJson(vendor.credentialsEncrypted);
  if (!credentials) {
    return res.status(400).json({ error: 'Vendor has no credentials configured.' });
  }

  let billingRange = null;
  try {
    billingRange = resolveRequestedBillingRange(req.body || {});
  } catch (error) {
    return res.status(400).json({
      ok: false,
      error: error?.message || 'Invalid billing range.'
    });
  }

  try {
    const preview = await pullBillingForVendor(vendor, credentials, billingRange || undefined);
    return res.json({
      ok: true,
      provider: vendor.provider,
      preview: {
        periodStart: preview.periodStart,
        periodEnd: preview.periodEnd,
        amount: preview.amount,
        currency: preview.currency,
        source: preview.source
      }
    });
  } catch (error) {
    return res.status(400).json({
      ok: false,
      error: error?.message || 'Vendor test failed.'
    });
  }
});

app.get('/api/billing', (req, res) => {
  const provider = req.query.provider ? normalizeProvider(req.query.provider) : null;
  const vendorId = req.query.vendorId ? String(req.query.vendorId).trim() : null;
  const limit = req.query.limit ? Number(req.query.limit) : 250;
  const periodStart = req.query.periodStart ? String(req.query.periodStart).trim() : null;
  const periodEnd = req.query.periodEnd ? String(req.query.periodEnd).trim() : null;

  const snapshots = listBillingSnapshots({ provider, vendorId, limit, periodStart, periodEnd });
  const breakdownRows = listBillingSnapshots({
    provider,
    vendorId,
    limit: 5000,
    periodStart,
    periodEnd
  });
  const resourceDetails = summarizeBillingResourceDetails(breakdownRows);

  const summaryRows = summarizeBillingByProvider({ periodStart, periodEnd }).map((row) => ({
    ...row,
    providerLabel: getBillingProviderLabel(row?.provider)
  }));

  res.json({
    snapshots,
    resourceBreakdown: summarizeBillingByResourceType(breakdownRows),
    resourceDetails,
    summary: summaryRows,
    total: snapshots.length
  });
});

app.get('/api/billing/accounts', (req, res) => {
  ensureBillingVendorsFromEnv();

  let range = null;
  try {
    range = resolveRequestedBillingRange(req.query || {});
  } catch (error) {
    return res.status(400).json({ error: error?.message || 'Invalid billing range.' });
  }

  const effectiveRange = range || getLastMonthRange();
  const providerRaw = String(req.query.provider || '').trim().toLowerCase();
  const providerFilter = providerRaw && providerRaw !== 'all' ? normalizeProvider(providerRaw) : null;
  const { context } = buildBillingAccountContext(providerFilter);
  const payload = listBillingAccountTotals(
    {
      periodStart: effectiveRange.periodStart,
      periodEnd: effectiveRange.periodEnd,
      provider: providerFilter || null
    },
    context
  );
  const accountsWithBudget = annotateAccountTotalsWithBudget(payload.accounts, effectiveRange);

  return res.json({
    period: {
      periodStart: effectiveRange.periodStart,
      periodEnd: effectiveRange.periodEnd
    },
    accounts: accountsWithBudget,
    providerTotals: payload.providerTotals,
    grandTotals: payload.grandTotals
  });
});

app.get('/api/billing/budgets/accounts', (req, res) => {
  ensureBillingVendorsFromEnv();
  const year = normalizeBudgetYear(req.query?.year);
  const providerRaw = String(req.query?.provider || '').trim().toLowerCase();
  const provider = providerRaw && providerRaw !== 'all' ? normalizeProvider(providerRaw) : null;
  const payload = buildBillingBudgetAccountRowsForYear(year, provider);
  return res.json({
    year: payload.year,
    rows: payload.rows,
    supportedTargetTypes: ['account', 'org', 'product']
  });
});

app.post('/api/billing/budgets/accounts/save', (req, res) => {
  ensureBillingVendorsFromEnv();
  const body = req.body || {};
  const year = normalizeBudgetYear(body.year);
  const rows = Array.isArray(body.budgets) ? body.budgets : [];
  const entries = [];
  const errors = [];

  for (const row of rows) {
    if (!row || typeof row !== 'object') {
      continue;
    }
    const scopeId = String(row.scopeId || '').trim();
    if (!scopeId) {
      continue;
    }
    const provider = normalizeProvider(row.provider);
    const currency = normalizeCurrencyCode(row.currency || 'USD');
    const months = row.months && typeof row.months === 'object' && !Array.isArray(row.months) ? row.months : {};

    for (let month = 1; month <= 12; month += 1) {
      const rawValue = Object.prototype.hasOwnProperty.call(months, String(month))
        ? months[String(month)]
        : months[month];
      if (rawValue === '' || rawValue === null || rawValue === undefined) {
        continue;
      }
      const amount = parseBudgetAmount(rawValue);
      if (amount === null) {
        errors.push(`Invalid budget amount for ${scopeId} month ${month}.`);
        continue;
      }
      entries.push({
        targetType: 'account',
        targetProvider: provider,
        targetId: scopeId,
        targetKey: null,
        targetLabel: String(row.accountName || '').trim() || null,
        vendorId: String(row.vendorId || '').trim() || null,
        accountId: String(row.accountId || '').trim() || null,
        budgetYear: year,
        budgetMonth: month,
        currency,
        amount,
        metadata: {
          source: 'billing-budget-ui',
          updatedAt: toIsoNow()
        }
      });
    }
  }

  if (errors.length) {
    return res.status(400).json({
      error: errors.slice(0, 10).join(' '),
      errorCount: errors.length
    });
  }

  const result = replaceBillingBudgetPlans({
    targetType: 'account',
    budgetYear: year,
    entries
  });
  const refreshed = buildBillingBudgetAccountRowsForYear(year);
  return res.json({
    ok: true,
    year,
    savedEntries: result.inserted,
    rows: refreshed.rows,
    supportedTargetTypes: ['account', 'org', 'product']
  });
});

app.post('/api/billing/sync', async (req, res) => {
  if (billingBackfillState.running) {
    return res.status(409).json({ error: 'Billing backfill is running. Try again after it completes.' });
  }

  ensureBillingVendorsFromEnv();

  const body = req.body || {};
  const vendorIds = Array.isArray(body.vendorIds)
    ? body.vendorIds.map((value) => String(value || '').trim()).filter(Boolean)
    : [];
  let billingRange = null;
  try {
    billingRange = resolveRequestedBillingRange(body);
  } catch (error) {
    return res.status(400).json({ error: error?.message || 'Invalid billing range.' });
  }

  const selectedVendors = listVendors()
    .filter((vendor) => isBillingProvider(vendor.provider))
    .filter((vendor) => (vendorIds.length ? vendorIds.includes(vendor.id) : true));

  if (!selectedVendors.length) {
    return res.status(400).json({ error: 'No eligible vendors configured for billing sync.' });
  }

  const runId = insertSyncRun({
    kind: 'billing',
    status: 'started',
    startedAt: toIsoNow(),
    details: {
      vendorCount: selectedVendors.length,
      periodStart: billingRange?.periodStart || null,
      periodEnd: billingRange?.periodEnd || null
    }
  });

  const results = [];
  for (const vendorRow of selectedVendors) {
    const vendor = getVendorById(vendorRow.id);
    if (!vendor) {
      continue;
    }

    const credentials = decryptJson(vendor.credentialsEncrypted);
    if (!credentials) {
      results.push({ vendorId: vendor.id, provider: vendor.provider, ok: false, error: 'Missing credentials.' });
      continue;
    }

    try {
      const pulled = await pullBillingForVendor(vendor, credentials, billingRange || undefined);
      deleteBillingSnapshotsForVendorPeriod({
        vendorId: vendor.id,
        provider: vendor.provider,
        periodStart: pulled.periodStart,
        periodEnd: pulled.periodEnd
      });
      const snapshot = insertBillingSnapshot({
        vendorId: vendor.id,
        provider: vendor.provider,
        periodStart: pulled.periodStart,
        periodEnd: pulled.periodEnd,
        currency: pulled.currency,
        amount: pulled.amount,
        source: pulled.source,
        raw: pulled.raw,
        pulledAt: toIsoNow()
      });
      applyBillingAutoTags(vendor, pulled);

      results.push({
        vendorId: vendor.id,
        provider: vendor.provider,
        ok: true,
        snapshot
      });
    } catch (error) {
      results.push({
        vendorId: vendor.id,
        provider: vendor.provider,
        ok: false,
        error: error?.message || 'Billing pull failed.'
      });
    }
  }

  const okCount = results.filter((item) => item.ok).length;
  finishSyncRun(runId, okCount === results.length ? 'ok' : 'partial', {
    results,
    okCount,
    failedCount: results.length - okCount
  });

  return res.json({
    runId,
    period: billingRange
      ? {
          periodStart: billingRange.periodStart,
          periodEnd: billingRange.periodEnd
        }
      : null,
    summary: {
      vendors: results.length,
      ok: okCount,
      failed: results.length - okCount
    },
    results,
    billingSummary: summarizeBillingByProvider({
      periodStart: billingRange?.periodStart || null,
      periodEnd: billingRange?.periodEnd || null
    }).map((row) => ({
      ...row,
      providerLabel: getBillingProviderLabel(row?.provider)
    }))
  });
});

app.get('/api/billing/backfill/status', (_req, res) => {
  return res.json(getBackfillStatusPayload());
});

app.post('/api/billing/rackspace/import-csv', (req, res) => {
  ensureBillingVendorsFromEnv();

  const vendorId = req.body?.vendorId ? String(req.body.vendorId).trim() : null;
  const fileName = req.body?.fileName ? String(req.body.fileName).trim() : null;
  const filePath = req.body?.filePath ? String(req.body.filePath).trim() : null;
  let csvText = req.body?.csvText ? String(req.body.csvText) : '';

  if (!csvText && filePath) {
    try {
      csvText = fs.readFileSync(filePath, 'utf-8');
    } catch (error) {
      return res.status(400).json({
        error: `Could not read Rackspace CSV file: ${error?.message || String(error)}`
      });
    }
  }
  if (!csvText.trim()) {
    return res.status(400).json({ error: 'csvText or filePath is required.' });
  }

  try {
    const parsed = parseRackspaceUsageCsv(csvText, { fileName: fileName || filePath || null });
    if (!parsed.rows.length) {
      return res.status(400).json({
        error: 'No non-zero usage rows were parsed from the Rackspace CSV.'
      });
    }
    const vendor = resolveRackspaceVendorForCsvImport(vendorId, parsed);
    const merged = mergeRackspaceUsageCsvIntoSnapshots(vendor, parsed, {
      fileName: fileName || filePath || null
    });
    return res.json({
      ok: true,
      vendor: {
        id: vendor.id,
        name: vendor.name,
        accountId: vendor.accountId || null
      },
      import: merged
    });
  } catch (error) {
    return res.status(400).json({
      error: error?.message || 'Rackspace CSV import failed.'
    });
  }
});

app.get('/api/billing/export/csv', (req, res) => {
  let range = null;
  try {
    range = resolveRequestedBillingRange(req.query || {});
  } catch (error) {
    return res.status(400).json({ error: error?.message || 'Invalid billing range.' });
  }

  const effectiveRange = range || getLastMonthRange();
  const provider = req.query.provider ? normalizeProvider(req.query.provider) : null;
  const vendorId = req.query.vendorId ? String(req.query.vendorId).trim() : null;
  const selectedResourceType = normalizeBillingResourceType(req.query.resourceType || '');
  const exportResourceDetails = String(req.query.resourceType || '').trim().length > 0;

  const snapshots = listBillingSnapshotsForExport({
    provider,
    vendorId,
    periodStart: effectiveRange.periodStart,
    periodEnd: effectiveRange.periodEnd
  });

  const baseScope = vendorId ? `vendor-${toFileSlug(vendorId, 'vendor')}` : provider ? provider : 'all-providers';
  const periodSlug = `${effectiveRange.periodStart}-to-${effectiveRange.periodEnd}`;
  const contentType = 'text/csv; charset=utf-8';

  if (!exportResourceDetails) {
    const vendorNameById = new Map(
      listVendors()
        .map((vendor) => [String(vendor?.id || '').trim(), String(vendor?.name || '').trim()])
        .filter((row) => row[0])
    );
    const detailGroups = summarizeBillingResourceDetails(snapshots);
    const detailRowsRaw = detailGroups.flatMap((group) => {
      const details = Array.isArray(group?.details) ? group.details : [];
      return details.map((detail) => ({
        row_kind: 'resource_detail',
        pulled_at: '',
        provider: group.provider || '',
        vendor_id: detail.vendorId || '',
        vendor_name: detail.vendorId ? vendorNameById.get(String(detail.vendorId).trim()) || '' : '',
        account_id: detail.accountId || '',
        period_start: effectiveRange.periodStart,
        period_end: effectiveRange.periodEnd,
        resource_type: group.resourceType || '',
        detail_name: detail.detailName || '',
        resource_ref: detail.resourceRef || '',
        amount: Number.isFinite(Number(detail.amount)) ? Number(detail.amount).toFixed(2) : '',
        currency: detail.currency || group.currency || 'USD',
        source: 'resource-breakdown',
        snapshot_rows: Number(detail.snapshotCount || 0)
      }));
    });
    const detailRows = appendTagColumnsToDetailRows(detailRowsRaw);
    const header = [
      'row_kind',
      'pulled_at',
      'provider',
      'vendor_id',
      'vendor_name',
      'account_id',
      'period_start',
      'period_end',
      'resource_type',
      'detail_name',
      'resource_ref',
      'tags',
      'tags_json',
      'tag_source',
      'tag_synced_at',
      'amount',
      'currency',
      'source',
      'snapshot_rows'
    ];
    const snapshotRows = snapshots.map((row) => ({
      row_kind: 'snapshot',
      pulled_at: row.pulledAt || '',
      provider: row.provider || '',
      vendor_id: row.vendorId || '',
      vendor_name: row.vendorName || '',
      account_id: row.accountId || '',
      period_start: row.periodStart || '',
      period_end: row.periodEnd || '',
      resource_type: '',
      detail_name: '',
      resource_ref: '',
      tags: '',
      tags_json: '{}',
      tag_source: '',
      tag_synced_at: '',
      amount: Number.isFinite(Number(row.amount)) ? Number(row.amount).toFixed(2) : '',
      currency: row.currency || 'USD',
      source: row.source || '',
      snapshot_rows: 1
    }));
    const csv = rowsToCsv(header, [...snapshotRows, ...detailRows]);
    const filename = `billing-${toFileSlug(baseScope, 'scope')}-${periodSlug}.csv`;
    res.setHeader('content-type', contentType);
    res.setHeader('content-disposition', `attachment; filename="${filename}"`);
    return res.send(csv);
  }

  const detailGroups = summarizeBillingResourceDetails(snapshots);
  const filteredGroups = detailGroups.filter(
    (group) => normalizeBillingResourceType(group?.resourceType) === selectedResourceType
  );
  const detailRowsRaw = filteredGroups.flatMap((group) => {
    const details = Array.isArray(group?.details) ? group.details : [];
    return details.map((detail) => ({
      provider: group.provider,
      resource_type: group.resourceType,
      detail_name: detail.detailName || '',
      resource_ref: detail.resourceRef || '',
      amount: Number.isFinite(Number(detail.amount)) ? Number(detail.amount).toFixed(2) : '',
      currency: detail.currency || group.currency || 'USD',
      vendor_id: detail.vendorId || '',
      account_id: detail.accountId || '',
      snapshot_rows: Number(detail.snapshotCount || 0),
      period_start: effectiveRange.periodStart,
      period_end: effectiveRange.periodEnd
    }));
  });
  const detailRows = appendTagColumnsToDetailRows(detailRowsRaw);

  const detailsHeader = [
    'provider',
    'resource_type',
    'detail_name',
    'resource_ref',
    'tags',
    'tags_json',
    'tag_source',
    'tag_synced_at',
    'amount',
    'currency',
    'vendor_id',
    'account_id',
    'snapshot_rows',
    'period_start',
    'period_end'
  ];
  const detailsCsv = rowsToCsv(detailsHeader, detailRows);
  const filename = `billing-${toFileSlug(baseScope, 'scope')}-${toFileSlug(selectedResourceType, 'resource')}-${periodSlug}.csv`;
  res.setHeader('content-type', contentType);
  res.setHeader('content-disposition', `attachment; filename="${filename}"`);
  return res.send(detailsCsv);
});

app.post('/api/billing/backfill/ensure', async (req, res) => {
  ensureBillingVendorsFromEnv();

  if (billingBackfillState.running) {
    return res.status(202).json({
      ok: true,
      started: false,
      reason: 'already-running',
      status: getBackfillStatusPayload()
    });
  }

  const body = req.body || {};
  const provider = body.provider ? normalizeProvider(body.provider) : null;
  if (body.provider && provider === 'other') {
    return res.status(400).json({ error: 'Invalid provider for backfill.' });
  }

  const vendorIds = Array.isArray(body.vendorIds)
    ? body.vendorIds.map((value) => String(value || '').trim()).filter(Boolean)
    : [];
  const options = {
    lookbackMonths: normalizeLookbackMonths(body.lookbackMonths || 12),
    provider,
    vendorIds,
    onlyMissing: true,
    delayMs: Math.max(0, Math.min(12_000, Number(body.delayMs || 1200))),
    retries: Math.max(1, Math.min(8, Number(body.retries || 4)))
  };
  const coverage = calculateMissingBackfillCoverage(options);
  if (!coverage.vendorCount || !coverage.rangesCount || coverage.missingVendorMonths === 0) {
    return res.json({
      ok: true,
      started: false,
      reason: 'no-missing-periods',
      coverage,
      status: getBackfillStatusPayload()
    });
  }

  void runBillingBackfillJob(options).catch((error) => {
    console.error('[billing] Ensure backfill job failed', {
      message: error?.message || String(error),
      stack: error?.stack || null
    });
  });

  return res.status(202).json({
    ok: true,
    started: true,
    reason: 'missing-periods-detected',
    coverage,
    status: getBackfillStatusPayload()
  });
});

app.post('/api/billing/backfill', async (req, res) => {
  if (billingBackfillState.running) {
    return res.status(409).json({
      error: 'Billing backfill is already running.',
      status: getBackfillStatusPayload()
    });
  }

  const body = req.body || {};
  const provider = body.provider ? normalizeProvider(body.provider) : null;
  if (body.provider && provider === 'other') {
    return res.status(400).json({ error: 'Invalid provider for backfill.' });
  }

  const vendorIds = Array.isArray(body.vendorIds)
    ? body.vendorIds.map((value) => String(value || '').trim()).filter(Boolean)
    : [];

  const options = {
    lookbackMonths: normalizeLookbackMonths(body.lookbackMonths),
    provider,
    vendorIds,
    onlyMissing: parseBoolean(body.onlyMissing, true),
    delayMs: Math.max(0, Math.min(12_000, Number(body.delayMs || 0))),
    retries: Math.max(1, Math.min(8, Number(body.retries || 4)))
  };

  void runBillingBackfillJob(options).catch((error) => {
    console.error('[billing] Backfill job failed', {
      message: error?.message || String(error),
      stack: error?.stack || null
    });
  });

  return res.status(202).json({
    ok: true,
    status: getBackfillStatusPayload()
  });
});

app.use((error, _req, res, _next) => {
  const statusCode = Number(error?.statusCode) || 500;
  const message = error?.message || 'Billing module request failed.';
  res.status(statusCode).json({ error: message });
});

module.exports = {
  app
};
