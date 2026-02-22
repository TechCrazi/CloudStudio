const express = require('express');
const {
  listVendors,
  getVendorById,
  replaceGrafanaCloudDailyIngest,
  listGrafanaCloudDailyIngest
} = require('../../db');
const { decryptJson } = require('../../crypto');
const { pullGrafanaCloudBilling } = require('../../connectors/billing');

const app = express();
app.set('etag', false);
app.use(express.json({ limit: '5mb' }));

let syncTimer = null;

const syncState = {
  running: false,
  enabled: true,
  intervalMs: 24 * 60 * 60 * 1000,
  lookbackMonths: 2,
  nextRunAt: null,
  lastStartedAt: null,
  lastFinishedAt: null,
  lastStatus: null,
  lastError: null,
  lastSummary: null
};

const GRAFANA_PRODUCT_ORDER = Object.freeze([
  'traces',
  'logs',
  'logs_retention',
  'metrics',
  'app_observability',
  'billable_users',
  'k8s_containers',
  'k8s_hosts',
  'synthetics'
]);

const GRAFANA_PRODUCT_LABELS = Object.freeze({
  traces: 'Traces',
  logs: 'Logs',
  logs_retention: 'Logs Retention',
  metrics: 'Metrics',
  app_observability: 'Application Observability',
  billable_users: 'Grafana Users',
  k8s_containers: 'Kubernetes Containers',
  k8s_hosts: 'Kubernetes Hosts',
  synthetics: 'Synthetics'
});

function normalizeProvider(value) {
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  if (normalized === 'grafana' || normalized === 'grafanacloud') {
    return 'grafana-cloud';
  }
  return normalized === 'grafana-cloud' ? normalized : 'other';
}

function normalizeDateOnly(value) {
  const text = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return '';
  }
  const date = new Date(`${text}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  if (date.toISOString().slice(0, 10) !== text) {
    return '';
  }
  return text;
}

function parseBoolean(value, fallback = true) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on', 'y'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'no', 'off', 'n'].includes(normalized)) {
    return false;
  }
  return fallback;
}

function parsePositiveInteger(value, fallback, min = 1, max = 120) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.max(min, Math.min(max, Math.floor(numeric)));
}

function getMonthStartDateOnly(date) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1));
  return d.toISOString().slice(0, 10);
}

function addMonthsDateOnly(dateOnly, offset) {
  const parsed = normalizeDateOnly(dateOnly);
  if (!parsed) {
    return dateOnly;
  }
  const date = new Date(`${parsed}T00:00:00.000Z`);
  date.setUTCMonth(date.getUTCMonth() + Number(offset || 0), 1);
  return date.toISOString().slice(0, 10);
}

function resolveSyncRange(options = {}) {
  const requestedStart = normalizeDateOnly(options.periodStart);
  const requestedEnd = normalizeDateOnly(options.periodEnd);
  if (requestedStart && requestedEnd) {
    return {
      periodStart: requestedStart,
      periodEnd: requestedEnd
    };
  }

  const now = new Date();
  const today = now.toISOString().slice(0, 10);
  const lookbackMonths = parsePositiveInteger(
    options.lookbackMonths,
    parsePositiveInteger(process.env.GRAFANA_CLOUD_INGEST_LOOKBACK_MONTHS, 2, 1, 36),
    1,
    36
  );
  const currentMonthStart = getMonthStartDateOnly(now);
  const periodStart = addMonthsDateOnly(currentMonthStart, -(lookbackMonths - 1));
  return {
    periodStart,
    periodEnd: today
  };
}

function getGrafanaCloudVendors() {
  return listVendors().filter((vendor) => normalizeProvider(vendor?.provider) === 'grafana-cloud');
}

function toPublicVendor(vendor = {}) {
  return {
    id: vendor.id,
    name: vendor.name,
    provider: normalizeProvider(vendor.provider),
    accountId: String(vendor.accountId || '').trim() || null,
    metadata: vendor.metadata && typeof vendor.metadata === 'object' ? vendor.metadata : {}
  };
}

function isDetailMetricRow(row = {}) {
  const raw = row?.raw;
  return Boolean(raw && typeof raw === 'object' && raw.isDetailMetric === true);
}

function parseDateOnly(value) {
  const normalized = normalizeDateOnly(value);
  if (!normalized) {
    return null;
  }
  const date = new Date(`${normalized}T00:00:00.000Z`);
  return Number.isNaN(date.getTime()) ? null : date;
}

function formatDateOnly(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return '';
  }
  return date.toISOString().slice(0, 10);
}

function buildPreviousRange(periodStart, periodEnd) {
  const startDate = parseDateOnly(periodStart);
  const endDate = parseDateOnly(periodEnd);
  if (!startDate || !endDate || endDate.getTime() < startDate.getTime()) {
    return null;
  }
  const dayMs = 24 * 60 * 60 * 1000;
  const dayCount = Math.floor((endDate.getTime() - startDate.getTime()) / dayMs) + 1;
  const previousEnd = new Date(startDate.getTime() - dayMs);
  const previousStart = new Date(previousEnd.getTime() - (dayCount - 1) * dayMs);
  return {
    periodStart: formatDateOnly(previousStart),
    periodEnd: formatDateOnly(previousEnd)
  };
}

function summarizeGrafanaDetailRows(rows = [], compareRows = [], compareRange = null) {
  const currentMap = new Map();
  const previousMap = new Map();
  let detailRowCount = 0;

  const collectRows = (sourceRows, targetMap, countDetails = false) => {
    for (const row of Array.isArray(sourceRows) ? sourceRows : []) {
      if (!isDetailMetricRow(row)) {
        continue;
      }
      if (countDetails) {
        detailRowCount += 1;
      }
      const raw = row.raw && typeof row.raw === 'object' ? row.raw : {};
      const productKey = String(raw.productKey || '').trim().toLowerCase() || 'other';
      const productLabel = String(raw.productLabel || GRAFANA_PRODUCT_LABELS[productKey] || productKey)
        .replace(/\s+/g, ' ')
        .trim();
      const metricKey = String(raw.metricKey || row.dimensionKey || 'metric').trim().toLowerCase() || 'metric';
      const metricLabel = String(raw.metricLabel || row.dimensionName || metricKey)
        .replace(/\s+/g, ' ')
        .trim();
      const metricKind = String(raw.metricKind || '').trim().toLowerCase() === 'cost' ? 'cost' : 'usage';
      const metricUnit = String(raw.metricUnit || row.unit || '').trim() || null;
      const explicitValue = Number(raw.metricValue);
      const fallbackValue = Number(row.totalUsage);
      const lastFallbackValue = Number(row.amountDue);
      const metricValue = Number.isFinite(explicitValue)
        ? explicitValue
        : Number.isFinite(fallbackValue)
        ? fallbackValue
        : Number.isFinite(lastFallbackValue)
        ? lastFallbackValue
        : null;
      if (!Number.isFinite(metricValue)) {
        continue;
      }
      const usageDate = String(row?.usageDate || '').trim();
      const mapKey = `${productKey}::${metricKey}`;
      const current = targetMap.get(mapKey) || {
        productKey,
        productLabel,
        metricKey,
        metricLabel,
        metricKind,
        metricUnit,
        isPrimaryCost: Boolean(raw.isPrimaryCost),
        value: 0,
        rowCount: 0,
        lastUsageDate: ''
      };
      current.value += metricValue;
      current.rowCount += 1;
      current.isPrimaryCost = current.isPrimaryCost || Boolean(raw.isPrimaryCost);
      if (usageDate && (!current.lastUsageDate || usageDate > current.lastUsageDate)) {
        current.lastUsageDate = usageDate;
      }
      targetMap.set(mapKey, current);
    }
  };

  collectRows(rows, currentMap, true);
  collectRows(compareRows, previousMap, false);

  const metrics = [];
  const productsByKey = new Map();

  for (const [mapKey, metric] of currentMap.entries()) {
    const previous = previousMap.get(mapKey);
    const previousValue = previous ? previous.value : null;
    const deltaValue = Number.isFinite(previousValue) ? metric.value - previousValue : null;
    const deltaPercent =
      Number.isFinite(previousValue) && Math.abs(previousValue) > 0
        ? (deltaValue / Math.abs(previousValue)) * 100
        : null;

    const metricRow = {
      productKey: metric.productKey,
      productLabel: metric.productLabel,
      metricKey: metric.metricKey,
      metricLabel: metric.metricLabel,
      metricKind: metric.metricKind,
      metricUnit: metric.metricUnit,
      isPrimaryCost: Boolean(metric.isPrimaryCost),
      value: Math.round(metric.value * 1_000_000) / 1_000_000,
      previousValue: Number.isFinite(previousValue) ? Math.round(previousValue * 1_000_000) / 1_000_000 : null,
      deltaValue: Number.isFinite(deltaValue) ? Math.round(deltaValue * 1_000_000) / 1_000_000 : null,
      deltaPercent: Number.isFinite(deltaPercent) ? Math.round(deltaPercent * 100) / 100 : null,
      rowCount: metric.rowCount,
      lastUsageDate: metric.lastUsageDate || null
    };
    metrics.push(metricRow);

    const productCurrent = productsByKey.get(metric.productKey) || {
      productKey: metric.productKey,
      productLabel: metric.productLabel || GRAFANA_PRODUCT_LABELS[metric.productKey] || metric.productKey,
      totalCost: 0,
      totalUsageMetrics: 0,
      metricCount: 0
    };
    if (metric.metricKind === 'cost') {
      productCurrent.totalCost += metricRow.value;
    } else {
      productCurrent.totalUsageMetrics += 1;
    }
    productCurrent.metricCount += 1;
    productsByKey.set(metric.productKey, productCurrent);
  }

  const productOrderIndex = new Map(GRAFANA_PRODUCT_ORDER.map((key, index) => [key, index]));
  metrics.sort((left, right) => {
    const leftOrder = productOrderIndex.has(left.productKey) ? productOrderIndex.get(left.productKey) : 999;
    const rightOrder = productOrderIndex.has(right.productKey) ? productOrderIndex.get(right.productKey) : 999;
    if (leftOrder !== rightOrder) {
      return leftOrder - rightOrder;
    }
    if (left.metricKind !== right.metricKind) {
      return left.metricKind === 'cost' ? -1 : 1;
    }
    if (left.isPrimaryCost !== right.isPrimaryCost) {
      return left.isPrimaryCost ? -1 : 1;
    }
    return String(left.metricLabel).localeCompare(String(right.metricLabel));
  });

  const products = Array.from(productsByKey.values())
    .map((product) => ({
      ...product,
      totalCost: Math.round(product.totalCost * 100) / 100
    }))
    .sort((left, right) => {
      const leftOrder = productOrderIndex.has(left.productKey) ? productOrderIndex.get(left.productKey) : 999;
      const rightOrder = productOrderIndex.has(right.productKey) ? productOrderIndex.get(right.productKey) : 999;
      if (leftOrder !== rightOrder) {
        return leftOrder - rightOrder;
      }
      return String(left.productLabel).localeCompare(String(right.productLabel));
    });

  return {
    rowCount: detailRowCount,
    products,
    metrics,
    compareRange
  };
}

function summarizeDailyRows(rows = []) {
  const byDateMap = new Map();
  const byDimensionMap = new Map();
  const byStackMap = new Map();
  let baseRowCount = 0;
  let detailRowCount = 0;

  for (const row of Array.isArray(rows) ? rows : []) {
    if (isDetailMetricRow(row)) {
      detailRowCount += 1;
      continue;
    }
    const usageDate = String(row?.usageDate || '').trim();
    if (!usageDate) {
      continue;
    }
    baseRowCount += 1;
    const dimensionKey = String(row?.dimensionKey || '').trim().toLowerCase() || 'usage';
    const dimensionName = String(row?.dimensionName || dimensionKey).trim() || dimensionKey;
    const stackSlug = String(row?.stackSlug || '').trim().toLowerCase() || 'org';
    const stackName = String(row?.stackName || stackSlug).trim() || stackSlug;

    const amountDue = Number(row?.amountDue || 0);
    const ingestUsage = Number(row?.ingestUsage || 0);
    const queryUsage = Number(row?.queryUsage || 0);
    const totalUsage = Number(row?.totalUsage || 0);

    const dateCurrent = byDateMap.get(usageDate) || {
      usageDate,
      amountDue: 0,
      ingestUsage: 0,
      queryUsage: 0,
      totalUsage: 0,
      rowCount: 0,
      stackCount: 0,
      dimensionCount: 0
    };
    dateCurrent.amountDue += Number.isFinite(amountDue) ? amountDue : 0;
    dateCurrent.ingestUsage += Number.isFinite(ingestUsage) ? ingestUsage : 0;
    dateCurrent.queryUsage += Number.isFinite(queryUsage) ? queryUsage : 0;
    dateCurrent.totalUsage += Number.isFinite(totalUsage) ? totalUsage : 0;
    dateCurrent.rowCount += 1;
    byDateMap.set(usageDate, dateCurrent);

    const dimensionMapKey = `${usageDate}::${dimensionKey}`;
    const dimensionCurrent = byDimensionMap.get(dimensionMapKey) || {
      usageDate,
      dimensionKey,
      dimensionName,
      amountDue: 0,
      ingestUsage: 0,
      queryUsage: 0,
      totalUsage: 0,
      rowCount: 0
    };
    dimensionCurrent.amountDue += Number.isFinite(amountDue) ? amountDue : 0;
    dimensionCurrent.ingestUsage += Number.isFinite(ingestUsage) ? ingestUsage : 0;
    dimensionCurrent.queryUsage += Number.isFinite(queryUsage) ? queryUsage : 0;
    dimensionCurrent.totalUsage += Number.isFinite(totalUsage) ? totalUsage : 0;
    dimensionCurrent.rowCount += 1;
    byDimensionMap.set(dimensionMapKey, dimensionCurrent);

    const stackMapKey = `${usageDate}::${stackSlug}`;
    const stackCurrent = byStackMap.get(stackMapKey) || {
      usageDate,
      stackSlug,
      stackName,
      amountDue: 0,
      ingestUsage: 0,
      queryUsage: 0,
      totalUsage: 0,
      rowCount: 0
    };
    stackCurrent.amountDue += Number.isFinite(amountDue) ? amountDue : 0;
    stackCurrent.ingestUsage += Number.isFinite(ingestUsage) ? ingestUsage : 0;
    stackCurrent.queryUsage += Number.isFinite(queryUsage) ? queryUsage : 0;
    stackCurrent.totalUsage += Number.isFinite(totalUsage) ? totalUsage : 0;
    stackCurrent.rowCount += 1;
    byStackMap.set(stackMapKey, stackCurrent);
  }

  const dateStats = Array.from(byDateMap.values())
    .map((row) => ({
      ...row,
      amountDue: Math.round(row.amountDue * 100) / 100,
      ingestUsage: Math.round(row.ingestUsage * 1000) / 1000,
      queryUsage: Math.round(row.queryUsage * 1000) / 1000,
      totalUsage: Math.round(row.totalUsage * 1000) / 1000,
      stackCount: Array.from(byStackMap.values()).filter((item) => item.usageDate === row.usageDate).length,
      dimensionCount: Array.from(byDimensionMap.values()).filter((item) => item.usageDate === row.usageDate).length
    }))
    .sort((left, right) => String(left.usageDate).localeCompare(String(right.usageDate)));

  const dimensionStats = Array.from(byDimensionMap.values())
    .map((row) => ({
      ...row,
      amountDue: Math.round(row.amountDue * 100) / 100,
      ingestUsage: Math.round(row.ingestUsage * 1000) / 1000,
      queryUsage: Math.round(row.queryUsage * 1000) / 1000,
      totalUsage: Math.round(row.totalUsage * 1000) / 1000
    }))
    .sort((left, right) => {
      if (left.usageDate !== right.usageDate) {
        return String(left.usageDate).localeCompare(String(right.usageDate));
      }
      return String(left.dimensionName).localeCompare(String(right.dimensionName));
    });

  const stackStats = Array.from(byStackMap.values())
    .map((row) => ({
      ...row,
      amountDue: Math.round(row.amountDue * 100) / 100,
      ingestUsage: Math.round(row.ingestUsage * 1000) / 1000,
      queryUsage: Math.round(row.queryUsage * 1000) / 1000,
      totalUsage: Math.round(row.totalUsage * 1000) / 1000
    }))
    .sort((left, right) => {
      if (left.usageDate !== right.usageDate) {
        return String(left.usageDate).localeCompare(String(right.usageDate));
      }
      return String(left.stackName).localeCompare(String(right.stackName));
    });

  return {
    byDate: dateStats,
    byDimension: dimensionStats,
    byStack: stackStats,
    baseRowCount,
    detailRowCount
  };
}

async function runGrafanaCloudSync(trigger = 'manual', options = {}) {
  if (syncState.running) {
    return {
      started: false,
      reason: 'Sync already running.'
    };
  }

  const range = resolveSyncRange(options);
  const vendorIdFilter = String(options.vendorId || '').trim();
  const vendors = getGrafanaCloudVendors().filter((vendor) => (vendorIdFilter ? vendor.id === vendorIdFilter : true));

  if (!vendors.length) {
    syncState.lastStatus = 'skipped';
    syncState.lastError = 'No Grafana Cloud vendors configured.';
    return {
      started: false,
      reason: 'No Grafana Cloud vendors configured.',
      range
    };
  }

  syncState.running = true;
  syncState.lastStartedAt = new Date().toISOString();
  syncState.lastStatus = 'running';
  syncState.lastError = null;

  const summary = {
    trigger,
    range,
    vendors: vendors.length,
    processed: 0,
    succeeded: 0,
    failed: 0,
    upsertedRows: 0,
    errors: []
  };

  try {
    for (const vendor of vendors) {
      summary.processed += 1;
      const fullVendor = getVendorById(vendor.id);
      const credentials = decryptJson(fullVendor?.credentialsEncrypted);
      if (!credentials) {
        summary.failed += 1;
        summary.errors.push({
          vendorId: vendor.id,
          vendorName: vendor.name,
          error: 'Missing credentials.'
        });
        continue;
      }

      try {
        const pulled = await pullGrafanaCloudBilling(vendor, credentials, range);
        const dailyRows = Array.isArray(pulled?.raw?.dailyIngestRows) ? pulled.raw.dailyIngestRows : [];
        const persisted = replaceGrafanaCloudDailyIngest(vendor.id, dailyRows, {
          provider: 'grafana-cloud',
          periodStart: range.periodStart,
          periodEnd: range.periodEnd,
          fetchedAt: new Date().toISOString()
        });
        summary.succeeded += 1;
        summary.upsertedRows += Number(persisted?.upserted || 0);
      } catch (error) {
        summary.failed += 1;
        summary.errors.push({
          vendorId: vendor.id,
          vendorName: vendor.name,
          error: error?.message || 'Grafana Cloud sync failed.'
        });
      }
    }

    syncState.lastFinishedAt = new Date().toISOString();
    syncState.lastStatus = summary.failed > 0 ? 'partial' : 'ok';
    syncState.lastSummary = summary;
    syncState.lastError = summary.failed > 0 ? `${summary.failed} vendor sync(s) failed.` : null;

    return {
      started: true,
      ok: summary.failed === 0,
      summary
    };
  } finally {
    syncState.running = false;
  }
}

function getSyncStatus() {
  return {
    ...syncState,
    nextRunAt: syncState.nextRunAt
  };
}

function scheduleNextRun(delayMs = null) {
  const effectiveDelay = Number.isFinite(Number(delayMs)) ? Number(delayMs) : syncState.intervalMs;
  syncState.nextRunAt = new Date(Date.now() + Math.max(10_000, effectiveDelay)).toISOString();
}

function startBackgroundWorkers() {
  syncState.enabled = parseBoolean(process.env.GRAFANA_CLOUD_INGEST_SYNC_ENABLED, true);
  syncState.intervalMs = Math.max(
    15 * 60 * 1000,
    Number(process.env.GRAFANA_CLOUD_INGEST_SYNC_INTERVAL_MS || 24 * 60 * 60 * 1000)
  );
  syncState.lookbackMonths = parsePositiveInteger(process.env.GRAFANA_CLOUD_INGEST_LOOKBACK_MONTHS, 2, 1, 36);

  if (!syncState.enabled) {
    syncState.nextRunAt = null;
    if (syncTimer) {
      clearInterval(syncTimer);
      syncTimer = null;
    }
    return;
  }

  if (syncTimer) {
    clearInterval(syncTimer);
  }

  syncTimer = setInterval(() => {
    void runGrafanaCloudSync('scheduled', {
      lookbackMonths: syncState.lookbackMonths
    }).catch((error) => {
      syncState.lastStatus = 'error';
      syncState.lastError = error?.message || 'Grafana Cloud scheduled sync failed.';
    });
    scheduleNextRun(syncState.intervalMs);
  }, syncState.intervalMs);

  const runOnStartup = parseBoolean(process.env.GRAFANA_CLOUD_INGEST_SYNC_RUN_ON_STARTUP, true);
  if (runOnStartup) {
    const startupDelayMs = Math.max(0, Number(process.env.GRAFANA_CLOUD_INGEST_STARTUP_DELAY_MS || 15_000));
    setTimeout(() => {
      void runGrafanaCloudSync('startup', {
        lookbackMonths: syncState.lookbackMonths
      }).catch((error) => {
        syncState.lastStatus = 'error';
        syncState.lastError = error?.message || 'Grafana Cloud startup sync failed.';
      });
    }, startupDelayMs);
  }

  scheduleNextRun(syncState.intervalMs);
}

app.get('/api/health', (_req, res) => {
  const vendors = getGrafanaCloudVendors();
  res.json({
    ok: true,
    vendors: vendors.length,
    sync: getSyncStatus()
  });
});

app.get('/api/vendors', (_req, res) => {
  const vendors = getGrafanaCloudVendors().map(toPublicVendor);
  res.json({
    vendors,
    total: vendors.length
  });
});

app.get('/api/status', (_req, res) => {
  res.json({
    sync: getSyncStatus()
  });
});

app.post('/api/sync', async (req, res) => {
  try {
    const body = req.body && typeof req.body === 'object' ? req.body : {};
    const result = await runGrafanaCloudSync('manual', {
      vendorId: body.vendorId,
      periodStart: body.periodStart,
      periodEnd: body.periodEnd,
      lookbackMonths: body.lookbackMonths
    });
    return res.status(result?.started ? 200 : 202).json({
      ...result,
      sync: getSyncStatus()
    });
  } catch (error) {
    return res.status(500).json({
      error: error?.message || 'Grafana Cloud sync failed.',
      sync: getSyncStatus()
    });
  }
});

app.get('/api/daily-ingest', (req, res) => {
  const periodStart = normalizeDateOnly(req.query.periodStart) || '';
  const periodEnd = normalizeDateOnly(req.query.periodEnd) || '';
  const vendorId = String(req.query.vendorId || '').trim() || null;
  const stackSlug = String(req.query.stackSlug || '').trim() || null;
  const dimensionKey = String(req.query.dimensionKey || '').trim() || null;
  const limit = Number.isFinite(Number(req.query.limit))
    ? Math.min(200_000, Math.max(1, Number(req.query.limit)))
    : 50_000;

  const rows = listGrafanaCloudDailyIngest({
    provider: 'grafana-cloud',
    vendorId,
    periodStart: periodStart || undefined,
    periodEnd: periodEnd || undefined,
    stackSlug: stackSlug || undefined,
    dimensionKey: dimensionKey || undefined,
    limit
  });

  const summary = summarizeDailyRows(rows);
  const compareRange = periodStart && periodEnd ? buildPreviousRange(periodStart, periodEnd) : null;
  const compareRows = compareRange
    ? listGrafanaCloudDailyIngest({
        provider: 'grafana-cloud',
        vendorId,
        periodStart: compareRange.periodStart,
        periodEnd: compareRange.periodEnd,
        stackSlug: stackSlug || undefined,
        dimensionKey: dimensionKey || undefined,
        limit
      })
    : [];
  const details = summarizeGrafanaDetailRows(rows, compareRows, compareRange);
  const vendors = getGrafanaCloudVendors().map(toPublicVendor);
  return res.json({
    vendors,
    periodStart: periodStart || null,
    periodEnd: periodEnd || null,
    rows,
    summary,
    details,
    total: rows.length,
    sync: getSyncStatus()
  });
});

module.exports = {
  app,
  startBackgroundWorkers
};
