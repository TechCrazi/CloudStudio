const { CostExplorerClient, GetCostAndUsageCommand } = require('@aws-sdk/client-cost-explorer');

function getCurrentMonthRange(referenceDate = new Date()) {
  const start = new Date(Date.UTC(referenceDate.getUTCFullYear(), referenceDate.getUTCMonth(), 1));
  const next = new Date(Date.UTC(referenceDate.getUTCFullYear(), referenceDate.getUTCMonth() + 1, 1));
  return {
    periodStart: start.toISOString().slice(0, 10),
    periodEndExclusive: next.toISOString().slice(0, 10),
    periodEnd: new Date(Date.UTC(referenceDate.getUTCFullYear(), referenceDate.getUTCMonth(), referenceDate.getUTCDate())).toISOString().slice(0, 10)
  };
}

function parseIsoDateOnly(value) {
  const text = String(value || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return null;
  }
  const date = new Date(`${text}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  if (date.toISOString().slice(0, 10) !== text) {
    return null;
  }
  return {
    text,
    date
  };
}

function parseIsoDateTimeToDateOnly(value) {
  const text = String(value || '').trim();
  if (!text) {
    return null;
  }
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString().slice(0, 10);
}

function addDaysToDateOnly(value, days) {
  const parsed = parseIsoDateOnly(value);
  if (!parsed) {
    return value;
  }
  const date = new Date(parsed.date.getTime() + Number(days || 0) * 24 * 60 * 60 * 1000);
  return date.toISOString().slice(0, 10);
}

function maxDateOnly(valueA, valueB) {
  const parsedA = parseIsoDateOnly(valueA);
  const parsedB = parseIsoDateOnly(valueB);
  if (!parsedA && !parsedB) {
    return String(valueA || valueB || '').trim();
  }
  if (!parsedA) {
    return parsedB.text;
  }
  if (!parsedB) {
    return parsedA.text;
  }
  return parsedA.date.getTime() >= parsedB.date.getTime() ? parsedA.text : parsedB.text;
}

function getDateOnlyYearsAgo(years = 2, referenceDate = new Date()) {
  const offsetYears = Number.isFinite(Number(years)) ? Number(years) : 2;
  const utcDate = new Date(
    Date.UTC(
      referenceDate.getUTCFullYear() - Math.floor(offsetYears),
      referenceDate.getUTCMonth(),
      referenceDate.getUTCDate()
    )
  );
  return utcDate.toISOString().slice(0, 10);
}

function intervalsOverlap(startA, endAExclusive, startB, endBExclusive) {
  const aStart = parseIsoDateOnly(startA);
  const aEnd = parseIsoDateOnly(endAExclusive);
  const bStart = parseIsoDateOnly(startB);
  const bEnd = parseIsoDateOnly(endBExclusive);
  if (!aStart || !aEnd || !bStart || !bEnd) {
    return false;
  }
  return aStart.date.getTime() < bEnd.date.getTime() && bStart.date.getTime() < aEnd.date.getTime();
}

function resolveBillingRange(options = {}) {
  const hasStart = Object.prototype.hasOwnProperty.call(options || {}, 'periodStart');
  const hasEnd = Object.prototype.hasOwnProperty.call(options || {}, 'periodEnd');

  if (!hasStart && !hasEnd) {
    return getCurrentMonthRange(options?.referenceDate);
  }

  if (!hasStart || !hasEnd) {
    throw new Error('Both periodStart and periodEnd are required when syncing a custom billing range.');
  }

  const start = parseIsoDateOnly(options.periodStart);
  const end = parseIsoDateOnly(options.periodEnd);
  if (!start || !end) {
    throw new Error('Billing period dates must use YYYY-MM-DD format.');
  }
  if (end.date.getTime() < start.date.getTime()) {
    throw new Error('Billing periodEnd must be on or after periodStart.');
  }

  const endExclusiveDate = new Date(end.date.getTime() + 24 * 60 * 60 * 1000);
  return {
    periodStart: start.text,
    periodEnd: end.text,
    periodEndExclusive: endExclusiveDate.toISOString().slice(0, 10)
  };
}

function normalizeNumber(value) {
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    return numeric;
  }
  if (typeof value === 'string') {
    const cleaned = value.replace(/[^0-9.+-]/g, '');
    const parsed = Number(cleaned);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  if (value && typeof value === 'object') {
    const nested = Number(value.amount ?? value.total ?? value.cost ?? value.value ?? value.charge);
    if (Number.isFinite(nested)) {
      return nested;
    }
  }
  return Number.isFinite(numeric) ? numeric : 0;
}

function parseBooleanLike(value, fallback = false) {
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

function normalizeResourceType(value, fallback = 'Uncategorized') {
  const normalized = String(value || '')
    .replace(/\s+/g, ' ')
    .trim();
  return normalized || fallback;
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

function normalizeRackspaceRefSegment(value) {
  const text = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return text || 'unknown';
}

function summarizeBreakdownRows(rows = []) {
  const map = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const resourceType = normalizeResourceType(row?.resourceType);
    const currency = String(row?.currency || 'USD').trim().toUpperCase() || 'USD';
    const amount = normalizeNumber(row?.amount);
    if (amount === 0) {
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

  return Array.from(map.values()).sort((left, right) => right.amount - left.amount);
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

function buildRackspaceUsageRowDedupeKey(row = {}) {
  return [
    String(row.billNo || '').trim(),
    String(row.accountNo || '').trim(),
    String(row.parentAccountNo || '').trim(),
    String(row.usageRecordId || '').trim(),
    String(row.resId || '').trim(),
    String(row.resName || '').trim(),
    String(row.serviceType || '').trim(),
    String(row.eventType || '').trim(),
    String(row.eventStartDate || '').trim(),
    String(row.eventEndDate || '').trim(),
    String(row.amount || '').trim(),
    String(row.currency || '').trim()
  ].join('::');
}

function summarizeRackspaceUsageRowsByResourceType(rows = []) {
  const map = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const resourceType = normalizeResourceType(row?.resourceType || row?.serviceType || row?.eventType || 'Uncategorized');
    const currency = String(row?.currency || 'USD').trim().toUpperCase() || 'USD';
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

function rangesOverlapInclusive(startA, endA, startB, endB) {
  const aStart = parseIsoDateOnly(startA);
  const aEnd = parseIsoDateOnly(endA);
  const bStart = parseIsoDateOnly(startB);
  const bEnd = parseIsoDateOnly(endB);
  if (!aStart || !aEnd || !bStart || !bEnd) {
    return false;
  }
  return aStart.date.getTime() <= bEnd.date.getTime() && bStart.date.getTime() <= aEnd.date.getTime();
}

function parseRackspaceUsageCsv(csvText, options = {}) {
  const rows = parseCsvRows(csvText);
  if (!rows.length) {
    return [];
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

  const queryPeriodStart = String(options.periodStart || '').trim();
  const queryPeriodEnd = String(options.periodEnd || '').trim();
  const sourceFileName = String(options.fileName || '').trim() || null;
  const normalizedRows = [];
  const seen = new Set();

  for (const row of body) {
    if (!Array.isArray(row) || !row.length) {
      continue;
    }
    const amount = parseNumberOrZero(col('AMOUNT', row));
    if (!Number.isFinite(amount) || amount === 0) {
      continue;
    }
    const billStartDate = parseRackspaceCsvDateTime(col('BILL_START_DATE', row));
    const billEndDate = parseRackspaceCsvDateTime(col('BILL_END_DATE', row));
    const periodStart = toDateOnlyUtc(billStartDate);
    const periodEnd = getRackspaceCsvPeriodEndDate(billEndDate);
    if (!periodStart || !periodEnd) {
      continue;
    }
    if (queryPeriodStart && queryPeriodEnd && !rangesOverlapInclusive(periodStart, periodEnd, queryPeriodStart, queryPeriodEnd)) {
      continue;
    }

    const serviceTypeRaw = col('SERVICE_TYPE', row);
    const eventTypeRaw = col('EVENT_TYPE', row);
    const impactTypeRaw = col('IMPACT_TYPE', row);
    const serviceType = normalizeRackspaceResourceLabel(serviceTypeRaw) || stripRackspacePeriodSuffix(serviceTypeRaw) || 'Uncategorized';
    const eventType = stripRackspacePeriodSuffix(eventTypeRaw) || null;
    const impactType = String(impactTypeRaw || '').replace(/\s+/g, ' ').trim() || null;

    const eventStartDate = toDateOnlyUtc(parseRackspaceCsvDateTime(col('EVENT_START_DATE', row))) || periodStart;
    const eventEndDate = toDateOnlyUtc(parseRackspaceCsvDateTime(col('EVENT_END_DATE', row))) || eventStartDate;

    const accountNo = col('ACCOUNT_NO', row) || null;
    const parentAccountNo = col('PARENT_ACCOUNT_NO', row) || null;
    const billNo = col('BILL_NO', row) || null;
    const usageRecordId = col('USAGE_RECORD_ID', row) || null;
    const resId = col('RES_ID', row) || null;
    const resName = col('RES_NAME', row) || null;
    const resourceType = normalizeResourceType(
      normalizeRackspaceResourceLabel(serviceType || eventType || 'Uncategorized') || serviceType || eventType || 'Uncategorized'
    );
    const fallbackDeviceName = String(
      `${serviceType}${eventType ? ` ${eventType}` : ''}${impactType ? ` (${impactType})` : ''}`
    )
      .replace(/\s+/g, ' ')
      .trim();
    const deviceName = resName || resId || fallbackDeviceName || usageRecordId || resourceType;
    const deviceKey = resId || resName || `${serviceType}:${eventType || impactType || 'shared'}`;
    const currency = String(col('CURRENCY', row) || 'USD').trim().toUpperCase() || 'USD';
    const resourceRef = `rackspace://${normalizeRackspaceRefSegment(
      accountNo || parentAccountNo || 'account'
    )}/${normalizeRackspaceRefSegment(resourceType)}/${normalizeRackspaceRefSegment(deviceKey)}`;

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
      sourceFileName
    };
    const key = buildRackspaceUsageRowDedupeKey(normalized);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    normalizedRows.push(normalized);
  }

  return normalizedRows;
}

function getColumnIndex(columns, candidateNames = []) {
  const normalizedCandidates = candidateNames.map((name) => String(name).trim().toLowerCase());
  return columns.findIndex((column) => {
    const name = String(column?.name || '').trim().toLowerCase();
    return normalizedCandidates.includes(name);
  });
}

function extractAzureResourceBreakdown(queryJson, fallbackCurrency = 'USD') {
  const columns = Array.isArray(queryJson?.properties?.columns) ? queryJson.properties.columns : [];
  const rows = Array.isArray(queryJson?.properties?.rows) ? queryJson.properties.rows : [];
  if (!rows.length) {
    return [];
  }

  const costIndex = getColumnIndex(columns, ['cost']);
  const currencyIndex = columns.findIndex((column) => String(column?.name || '').toLowerCase().includes('currency'));
  const resourceTypeIndex = getColumnIndex(columns, [
    'servicename',
    'metercategory',
    'resourcegroupname',
    'resourcetype',
    'servicefamily'
  ]);

  const breakdownRows = [];
  for (const row of rows) {
    if (!Array.isArray(row)) {
      continue;
    }

    const amount = normalizeNumber(row[costIndex >= 0 ? costIndex : 0]);
    if (amount <= 0) {
      continue;
    }

    const resourceType = normalizeResourceType(row[resourceTypeIndex >= 0 ? resourceTypeIndex : 1]);
    const currency = String(row[currencyIndex >= 0 ? currencyIndex : ''] || fallbackCurrency)
      .trim()
      .toUpperCase();

    breakdownRows.push({
      resourceType,
      currency: currency || fallbackCurrency,
      amount
    });
  }

  return summarizeBreakdownRows(breakdownRows);
}

function extractAwsResourceBreakdown(response, fallbackCurrency = 'USD') {
  const first = Array.isArray(response?.ResultsByTime) ? response.ResultsByTime[0] : null;
  const groups = Array.isArray(first?.Groups) ? first.Groups : [];
  if (!groups.length) {
    return [];
  }

  const rows = groups.map((group) => {
    const metric = group?.Metrics?.UnblendedCost || {};
    return {
      resourceType: normalizeResourceType(group?.Keys?.[0], 'Other'),
      currency: String(metric?.Unit || fallbackCurrency).trim().toUpperCase() || fallbackCurrency,
      amount: normalizeNumber(metric?.Amount)
    };
  });

  return summarizeBreakdownRows(rows);
}

function normalizeBaseUrl(value, fallbackValue) {
  const raw = String(value || '').trim();
  const base = raw || String(fallbackValue || '').trim();
  if (!base) {
    return '';
  }
  const withProtocol = /^https?:\/\//i.test(base) ? base : `https://${base}`;
  return withProtocol.replace(/\/+$/g, '');
}

function pickFirstString(values = [], fallback = '') {
  for (const value of Array.isArray(values) ? values : []) {
    const text = String(value || '').trim();
    if (text) {
      return text;
    }
  }
  return fallback;
}

function getNestedNumber(input) {
  if (input === null || input === undefined) {
    return 0;
  }
  if (Array.isArray(input)) {
    for (const item of input) {
      const amount = getNestedNumber(item);
      if (amount !== 0) {
        return amount;
      }
    }
    return 0;
  }
  if (typeof input === 'object') {
    return normalizeNumber(input.amount ?? input.total ?? input.cost ?? input.value ?? input.charge);
  }
  return normalizeNumber(input);
}

function findRackspaceBillingBaseUrlFromCatalog(tokenJson, preferredRegion = '') {
  const region = String(preferredRegion || '').trim().toUpperCase();
  const catalog = Array.isArray(tokenJson?.access?.serviceCatalog) ? tokenJson.access.serviceCatalog : [];
  const billingServices = catalog.filter((entry) => {
    const type = String(entry?.type || '').trim().toLowerCase();
    const name = String(entry?.name || '').trim().toLowerCase();
    return type.includes('billing') || name.includes('billing');
  });

  for (const service of billingServices) {
    const endpoints = Array.isArray(service?.endpoints) ? service.endpoints : [];
    if (!endpoints.length) {
      continue;
    }
    const preferredEndpoint = region
      ? endpoints.find((endpoint) => String(endpoint?.region || '').trim().toUpperCase() === region) || endpoints[0]
      : endpoints[0];
    const endpointUrl = pickFirstString([
      preferredEndpoint?.publicURL,
      preferredEndpoint?.publicUrl,
      preferredEndpoint?.url,
      preferredEndpoint?.internalURL
    ]);
    if (endpointUrl) {
      return normalizeBaseUrl(endpointUrl, '');
    }
  }
  return '';
}

function extractRackspaceAccountId(row) {
  return pickFirstString([
    row?.accountNumber,
    row?.account_id,
    row?.accountId,
    row?.id,
    row?.number,
    row?.ran,
    row?.RAN
  ]);
}

function extractRackspaceAccountName(row) {
  return pickFirstString([
    row?.accountName,
    row?.displayName,
    row?.companyName,
    row?.name,
    row?.label
  ]);
}

function resolveRackspaceAccountIdFromToken(tokenJson) {
  const catalog = Array.isArray(tokenJson?.access?.serviceCatalog) ? tokenJson.access.serviceCatalog : [];
  for (const service of catalog) {
    const endpoints = Array.isArray(service?.endpoints) ? service.endpoints : [];
    for (const endpoint of endpoints) {
      const url = String(endpoint?.publicURL || endpoint?.publicUrl || endpoint?.url || endpoint?.internalURL || '').trim();
      if (!url) {
        continue;
      }
      const accountFromHybrid = url.match(/hybrid:(\d{3}-[a-z0-9-]+)/i);
      if (accountFromHybrid?.[1]) {
        return accountFromHybrid[1];
      }
      const accountFromPath = url.match(/\/accounts\/(\d{3}-[a-z0-9-]+)/i);
      if (accountFromPath?.[1]) {
        return accountFromPath[1];
      }
      const genericRan = url.match(/\b(\d{3}-[a-z0-9-]{3,})\b/i);
      if (genericRan?.[1]) {
        return genericRan[1];
      }
    }
  }

  const tokenTenant = tokenJson?.access?.token?.tenant || tokenJson?.token?.tenant || {};
  const tenantCandidates = [tokenTenant?.id, tokenTenant?.name];
  for (const candidate of tenantCandidates) {
    const text = String(candidate || '').trim();
    if (!text) {
      continue;
    }
    const directRan = text.match(/^(\d{3}-[a-z0-9-]{3,})$/i);
    if (directRan?.[1]) {
      return directRan[1];
    }
    const hybridRan = text.match(/hybrid:(\d{3}-[a-z0-9-]{3,})/i);
    if (hybridRan?.[1]) {
      return hybridRan[1];
    }
  }

  return '';
}

function extractRackspaceBillingRows(summaryJson) {
  const rows = [];
  const containers = [
    summaryJson,
    summaryJson?.summary,
    summaryJson?.billingSummary,
    summaryJson?.data,
    summaryJson?.result
  ].filter(Boolean);
  const arrayKeys = [
    'resourceBreakdown',
    'serviceBreakdown',
    'services',
    'charges',
    'lineItems',
    'item',
    'items',
    'categories',
    'chargeDetails',
    'details'
  ];

  for (const container of containers) {
    for (const key of arrayKeys) {
      const candidate = container?.[key];
      if (!Array.isArray(candidate)) {
        continue;
      }
      for (const row of candidate) {
        rows.push(row);
      }
    }
  }

  return rows;
}

function normalizeRackspaceSummaryRows(summaryJson = {}) {
  const rows = extractRackspaceBillingRows(summaryJson);
  return rows.map((row) => {
    const postedDate = parseIsoDateTimeToDateOnly(row?.date);
    const coverageStartDate = parseIsoDateTimeToDateOnly(row?.coverageStartDate);
    const coverageEndDateRaw = parseIsoDateTimeToDateOnly(row?.coverageEndDate);
    const coverageEndExclusive = coverageEndDateRaw || (postedDate ? addDaysToDateOnly(postedDate, 1) : null);
    const coverageStart = coverageStartDate || postedDate || null;
    const amount = normalizeNumber(
      row?.amount ??
        row?.total ??
        row?.cost ??
        row?.charge ??
        row?.chargeAmount ??
        row?.extendedAmount ??
        row?.value
    );
    const type = String(row?.type || '').trim().toUpperCase();
    const links = Array.isArray(row?.link) ? row.link : [];
    const invoiceLink = links.find((link) => String(link?.rel || '').trim().toLowerCase() === 'self');
    return {
      sourceRow: row,
      amount,
      type,
      postedDate,
      coverageStart,
      coverageEndExclusive,
      coverageStartRaw: String(row?.coverageStartDate || '').trim() || null,
      coverageEndRaw: String(row?.coverageEndDate || '').trim() || null,
      invoiceId: String(row?.id || '').trim() || null,
      invoiceHref: String(invoiceLink?.href || '').trim() || null
    };
  });
}

function extractRackspaceResourceBreakdown(summaryJson, fallbackCurrency = 'USD') {
  const currency = normalizeResourceType(
    pickFirstString([
      summaryJson?.currency,
      summaryJson?.currencyCode,
      summaryJson?.summary?.currency,
      summaryJson?.summary?.currencyCode,
      summaryJson?.billingSummary?.currency,
      summaryJson?.billingSummary?.currencyCode
    ], fallbackCurrency),
    fallbackCurrency
  ).toUpperCase();
  const rows = extractRackspaceBillingRows(summaryJson);
  if (!rows.length) {
    return [];
  }

  const normalizedRows = rows
    .map((row) => {
      const amount = normalizeNumber(
        row?.amount ??
          row?.total ??
          row?.cost ??
          row?.charge ??
          row?.chargeAmount ??
          row?.extendedAmount ??
          row?.value
      );
      if (!Number.isFinite(amount) || amount <= 0) {
        return null;
      }
      const resourceType = normalizeResourceType(
        row?.resourceType ??
          row?.type ??
          row?.serviceName ??
          row?.service ??
          row?.category ??
          row?.description ??
          row?.name,
        'Uncategorized'
      );
      const rowCurrency = String(row?.currency || row?.currencyCode || currency || fallbackCurrency).trim().toUpperCase()
        || fallbackCurrency;
      return {
        resourceType,
        currency: rowCurrency,
        amount
      };
    })
    .filter(Boolean);

  return summarizeBreakdownRows(normalizedRows);
}

function extractRackspaceTotalAmount(summaryJson, fallbackAmount = 0) {
  const billingSummary = summaryJson?.billingSummary || {};
  const items = Array.isArray(billingSummary?.item) ? billingSummary.item : [];
  let hasItemAmounts = false;
  let itemAmountTotal = 0;
  for (const item of items) {
    const amount = normalizeNumber(
      item?.amount ??
        item?.total ??
        item?.cost ??
        item?.charge ??
        item?.chargeAmount ??
        item?.extendedAmount ??
        item?.value
    );
    if (!Number.isFinite(amount)) {
      continue;
    }
    hasItemAmounts = true;
    itemAmountTotal += amount;
  }
  if (hasItemAmounts) {
    return itemAmountTotal;
  }

  const directCandidates = [
    summaryJson?.amount,
    summaryJson?.totalAmount,
    summaryJson?.total,
    summaryJson?.grandTotal,
    summaryJson?.balanceDue,
    summaryJson?.amountDue,
    summaryJson?.summary?.amount,
    summaryJson?.summary?.totalAmount,
    summaryJson?.summary?.total,
    summaryJson?.billingSummary?.amount
  ];
  for (const candidate of directCandidates) {
    const amount = getNestedNumber(candidate);
    if (items.length && Number.isInteger(amount) && amount === items.length) {
      continue;
    }
    if (Number.isFinite(amount) && amount > 0) {
      return amount;
    }
  }
  return normalizeNumber(fallbackAmount);
}

function summarizeRackspaceInvoiceSections(invoiceDetails = [], fallbackCurrency = 'USD') {
  const rows = [];
  for (const invoiceDetail of Array.isArray(invoiceDetails) ? invoiceDetails : []) {
    const invoice = invoiceDetail?.invoice || {};
    const invoiceCurrency = String(invoice?.currency || fallbackCurrency || 'USD').trim().toUpperCase() || fallbackCurrency;
    const sections = Array.isArray(invoice?.invoiceSection) ? invoice.invoiceSection : [];
    for (const section of sections) {
      const sectionId = String(section?.sectionId || '').trim();
      const items = Array.isArray(section?.invoiceItem) ? section.invoiceItem : [];
      for (const item of items) {
        const amount = normalizeNumber(item?.itemAmount ?? item?.amount ?? item?.total ?? item?.cost ?? item?.value);
        if (amount === 0) {
          continue;
        }
        const itemType = String(item?.itemType || '').trim();
        const name = String(item?.name || item?.description || '').replace(/\s+/g, ' ').trim();
        const resourceType = normalizeResourceType(name || itemType || 'Uncategorized');
        rows.push({
          resourceType,
          currency: invoiceCurrency,
          amount,
          accountId: sectionId || null,
          itemType: itemType || null
        });
      }
    }
  }
  return summarizeBreakdownRows(rows);
}

function buildRackspaceApiUrl(baseUrl, pathSuffix) {
  const normalizedBase = normalizeBaseUrl(baseUrl, 'https://billing.api.rackspacecloud.com');
  const url = new URL(normalizedBase);
  const currentPath = url.pathname.replace(/\/+$/g, '');
  const hasVersionInPath = /\/v\d+($|\/)/i.test(currentPath);
  const basePath = hasVersionInPath ? currentPath : `${currentPath || ''}/v2`;
  const suffix = String(pathSuffix || '').trim().replace(/^\/+/g, '');
  return `${url.origin}${basePath}/${suffix}`;
}

async function fetchRackspaceJson(url, options = {}, label = 'Rackspace request') {
  const response = await fetch(url, options);
  const text = await response.text();
  let payload = null;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch (_error) {
      payload = null;
    }
  }

  if (!response.ok) {
    const detail = text || JSON.stringify(payload || {});
    throw new Error(`${label} failed (${response.status}): ${detail.slice(0, 300)}`);
  }

  return payload;
}

async function fetchRackspaceText(url, options = {}, label = 'Rackspace request') {
  const response = await fetch(url, options);
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`${label} failed (${response.status}): ${text.slice(0, 300)}`);
  }
  return text;
}

function normalizeWasabiWacmEndpoint(value) {
  const fallback = 'https://api.wacm.wasabisys.com/api/v1/invoices';
  const normalized = normalizeBaseUrl(value, fallback);
  try {
    const url = new URL(normalized);
    let pathname = String(url.pathname || '').trim().replace(/\/+$/g, '');
    if (!pathname || pathname === '/') {
      pathname = '/api/v1/invoices';
    } else if (/\/api\/v1$/i.test(pathname)) {
      pathname = `${pathname}/invoices`;
    } else if (!/\/invoices$/i.test(pathname)) {
      pathname = '/api/v1/invoices';
    }
    url.pathname = pathname;
    return url.toString();
  } catch (_error) {
    return fallback;
  }
}

function normalizeWasabiWacmUsageEndpoint(value) {
  const fallback = 'https://api.wacm.wasabisys.com/api/v1/usages';
  const normalized = normalizeBaseUrl(value, fallback);
  try {
    const url = new URL(normalized);
    let pathname = String(url.pathname || '').trim().replace(/\/+$/g, '');
    if (!pathname || pathname === '/') {
      pathname = '/api/v1/usages';
    } else if (/\/api\/v1$/i.test(pathname)) {
      pathname = `${pathname}/usages`;
    } else if (!/\/usages$/i.test(pathname)) {
      pathname = '/api/v1/usages';
    }
    url.pathname = pathname;
    return url.toString();
  } catch (_error) {
    return fallback;
  }
}

function normalizeWasabiWacmControlUsageEndpoint(value) {
  const fallback = 'https://api.wacm.wasabisys.com/api/v1/control-accounts/usages';
  const normalized = normalizeBaseUrl(value, fallback);
  try {
    const url = new URL(normalized);
    let pathname = String(url.pathname || '').trim().replace(/\/+$/g, '');
    if (!pathname || pathname === '/') {
      pathname = '/api/v1/control-accounts/usages';
    } else if (/\/api\/v1$/i.test(pathname)) {
      pathname = `${pathname}/control-accounts/usages`;
    } else if (!/\/control-accounts\/usages$/i.test(pathname)) {
      pathname = '/api/v1/control-accounts/usages';
    }
    url.pathname = pathname;
    return url.toString();
  } catch (_error) {
    return fallback;
  }
}

function parseWasabiWacmItems(payload) {
  if (Array.isArray(payload?.data)) {
    return payload.data;
  }
  if (Array.isArray(payload?.data?.items)) {
    return payload.data.items;
  }
  if (Array.isArray(payload?.items)) {
    return payload.items;
  }
  return [];
}

function parseWasabiWacmPagination(payload) {
  const pageRaw = payload?.data?.page ?? payload?.page;
  const sizeRaw = payload?.data?.size ?? payload?.size;
  const totalRaw = payload?.data?.total ?? payload?.total ?? payload?.data?.totalCount ?? payload?.totalCount;
  const page = Number(pageRaw);
  const size = Number(sizeRaw);
  const total = Number(totalRaw);
  return {
    page: Number.isFinite(page) ? page : null,
    size: Number.isFinite(size) ? size : null,
    total: Number.isFinite(total) ? total : null
  };
}

function normalizeWasabiLookupKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

function shouldUseWasabiVendorAccountIdFilter(value) {
  const text = String(value || '').trim();
  return /^\d+$/.test(text);
}

function computeWasabiInvoiceTotal(row = {}) {
  const directCandidates = [
    row?.totalStorage,
    row?.totalAmount,
    row?.amount,
    row?.invoiceTotal,
    row?.total
  ];
  for (const candidate of directCandidates) {
    const amount = normalizeNumber(candidate);
    if (Number.isFinite(amount) && amount > 0) {
      return amount;
    }
  }
  const componentTotal =
    normalizeNumber(row?.activeStorageTotalCost) +
    normalizeNumber(row?.deletedStorageTotalCost) +
    normalizeNumber(row?.minimumActiveStorageTotalCost) +
    normalizeNumber(row?.apiCallsTotalCost) +
    normalizeNumber(row?.ingressTotalCost) +
    normalizeNumber(row?.egressTotalCost);
  return Number.isFinite(componentTotal) && componentTotal > 0 ? componentTotal : 0;
}

function buildWasabiInvoicePeriod(invoice = {}, fallbackRange = null) {
  const periodStartRaw = String(invoice?.periodStart || '').trim();
  const periodEndRaw = String(invoice?.periodEnd || '').trim();
  const parsedStart = parseIsoDateOnly(periodStartRaw);
  const parsedEnd = parseIsoDateOnly(periodEndRaw);
  const periodStart = parsedStart?.text || fallbackRange?.periodStart || null;
  const periodEnd = parsedEnd?.text || fallbackRange?.periodEnd || periodStart || null;
  const periodEndExclusive = parsedEnd?.text
    ? addDaysToDateOnly(parsedEnd.text, 1)
    : periodEnd
    ? addDaysToDateOnly(periodEnd, 1)
    : null;
  return {
    periodStart,
    periodEnd,
    periodEndExclusive
  };
}

function invoiceOverlapsBillingRange(invoice = {}, range = {}) {
  const period = buildWasabiInvoicePeriod(invoice, range);
  if (!period.periodStart || !period.periodEndExclusive || !range?.periodStart || !range?.periodEndExclusive) {
    return false;
  }
  return intervalsOverlap(period.periodStart, period.periodEndExclusive, range.periodStart, range.periodEndExclusive);
}

function invoiceMatchesWasabiAccountFilter(invoice = {}, accountFilters = []) {
  if (!Array.isArray(accountFilters) || !accountFilters.length) {
    return true;
  }
  const lookupValues = [
    invoice?.subAccountId,
    invoice?.wasabiAccountNumber,
    invoice?.subInvoiceId,
    invoice?.controlInvoiceId,
    invoice?.subAccountName,
    invoice?.subAccountEmail,
    invoice?.channelAccountId,
    invoice?.channelAccountName,
    invoice?.channelAccountEmail
  ]
    .map((value) => String(value || '').trim())
    .filter(Boolean);
  if (!lookupValues.length) {
    return false;
  }
  const normalizedLookupValues = new Set(lookupValues.map((value) => normalizeWasabiLookupKey(value)).filter(Boolean));
  return accountFilters.some((filterKey) => normalizedLookupValues.has(filterKey));
}

function buildWasabiUsagePeriod(usage = {}, fallbackRange = null) {
  const startRaw = String(usage?.startTime || usage?.periodStart || '').trim();
  const endRaw = String(usage?.endTime || usage?.periodEnd || '').trim();
  const parsedStart = parseIsoDateOnly(startRaw);
  const parsedEnd = parseIsoDateOnly(endRaw);
  const periodStart = parsedStart?.text || fallbackRange?.periodStart || null;
  let periodEnd = null;
  if (parsedEnd?.text) {
    if (parsedStart && parsedEnd.date.getTime() > parsedStart.date.getTime()) {
      periodEnd = addDaysToDateOnly(parsedEnd.text, -1);
    } else {
      periodEnd = parsedEnd.text;
    }
  } else {
    periodEnd = fallbackRange?.periodEnd || periodStart || null;
  }
  const periodEndExclusive = parsedEnd?.text || (periodEnd ? addDaysToDateOnly(periodEnd, 1) : null);
  return {
    periodStart,
    periodEnd,
    periodEndExclusive
  };
}

function usageOverlapsBillingRange(usage = {}, range = {}) {
  const period = buildWasabiUsagePeriod(usage, range);
  if (!period.periodStart || !period.periodEndExclusive || !range?.periodStart || !range?.periodEndExclusive) {
    return false;
  }
  return intervalsOverlap(period.periodStart, period.periodEndExclusive, range.periodStart, range.periodEndExclusive);
}

function usageMatchesWasabiAccountFilter(usage = {}, accountFilters = []) {
  if (!Array.isArray(accountFilters) || !accountFilters.length) {
    return true;
  }
  const lookupValues = [
    usage?.subAccountId,
    usage?.wasabiAccountNumber,
    usage?.subAccountName,
    usage?.subAccountEmail,
    usage?.channelAccountId,
    usage?.channelAccountName,
    usage?.channelAccountEmail
  ]
    .map((value) => String(value || '').trim())
    .filter(Boolean);
  if (!lookupValues.length) {
    return false;
  }
  const normalizedLookupValues = new Set(lookupValues.map((value) => normalizeWasabiLookupKey(value)).filter(Boolean));
  return accountFilters.some((filterKey) => normalizedLookupValues.has(filterKey));
}

function sanitizeWasabiUsageRow(row = {}, range = {}) {
  const period = buildWasabiUsagePeriod(row, range);
  return {
    id: row?.id ?? null,
    periodStart: period.periodStart,
    periodEnd: period.periodEnd,
    subAccountId: row?.subAccountId ?? null,
    subAccountName: String(row?.subAccountName || '').trim() || null,
    subAccountEmail: String(row?.subAccountEmail || '').trim() || null,
    wasabiAccountNumber: row?.wasabiAccountNumber ?? null,
    channelAccountId: row?.channelAccountId ?? null,
    channelAccountName: String(row?.channelAccountName || '').trim() || null,
    channelAccountEmail: String(row?.channelAccountEmail || '').trim() || null,
    activeStorage: normalizeNumber(row?.activeStorage),
    deletedStorage: normalizeNumber(row?.deletedStorage),
    storageWrote: normalizeNumber(row?.storageWrote),
    storageRead: normalizeNumber(row?.storageRead),
    activeObjects: normalizeNumber(row?.activeObjects),
    deletedObjects: normalizeNumber(row?.deletedObjects),
    ingress: normalizeNumber(row?.ingress),
    egress: normalizeNumber(row?.egress),
    apiCalls: normalizeNumber(row?.apiCalls)
  };
}

function sanitizeWasabiControlUsageRow(row = {}, range = {}) {
  const normalized = sanitizeWasabiUsageRow(
    {
      ...row,
      subAccountId: row?.controlAccountId ?? row?.subAccountId,
      subAccountName: row?.controlAccountName ?? row?.subAccountName,
      subAccountEmail: row?.controlAccountEmail ?? row?.subAccountEmail,
      wasabiAccountNumber: row?.controlAccountId ?? row?.wasabiAccountNumber
    },
    range
  );
  return {
    ...normalized,
    controlAccountId: row?.controlAccountId ?? null,
    controlAccountName: String(row?.controlAccountName || '').trim() || null,
    controlAccountEmail: String(row?.controlAccountEmail || '').trim() || null,
    sourceType: 'wasabi_main_control_usage'
  };
}

function resolveWasabiMainPricingConfig(credentials = {}) {
  const daysInMonth = Math.max(
    1,
    Number(
      credentials?.daysInMonth ||
        process.env.WASABI_MAIN_DAYS_IN_MONTH ||
        process.env.WASABI_PRICING_DAYS_IN_MONTH ||
        30
    )
  );
  const activeStorageCostPerTbMonth = Math.max(
    0,
    Number(
      credentials?.activeStorageCostPerTbMonth ||
        process.env.WASABI_MAIN_ACTIVE_STORAGE_TB_MONTH ||
        process.env.WASABI_PRICING_STORAGE_TB_MONTH ||
        9
    )
  );
  const deletedStorageCostPerTbMonth = Math.max(
    0,
    Number(
      credentials?.deletedStorageCostPerTbMonth ||
        process.env.WASABI_MAIN_DELETED_STORAGE_TB_MONTH ||
        activeStorageCostPerTbMonth
    )
  );
  const minimumBillableTb = Math.max(
    0,
    Number(
      credentials?.minimumBillableTb ||
        process.env.WASABI_MAIN_MIN_BILLABLE_TB ||
        process.env.WASABI_PRICING_MIN_BILLABLE_TB ||
        0
    )
  );
  const apiCallsCostPerMillion = Math.max(
    0,
    Number(credentials?.apiCallsCostPerMillion || process.env.WASABI_MAIN_API_CALLS_PER_MILLION || 0)
  );
  const ingressCostPerTb = Math.max(
    0,
    Number(credentials?.ingressCostPerTb || process.env.WASABI_MAIN_INGRESS_TB_COST || 0)
  );
  const egressCostPerTb = Math.max(
    0,
    Number(credentials?.egressCostPerTb || process.env.WASABI_MAIN_EGRESS_TB_COST || 0)
  );
  const currency = String(credentials?.currency || process.env.WASABI_MAIN_CURRENCY || 'USD')
    .trim()
    .toUpperCase() || 'USD';
  return {
    currency,
    daysInMonth,
    activeStorageCostPerTbMonth,
    deletedStorageCostPerTbMonth,
    minimumBillableTb,
    apiCallsCostPerMillion,
    ingressCostPerTb,
    egressCostPerTb
  };
}

function buildWasabiMainUsageResourceRef(usage = {}, componentKey = 'total') {
  const accountSegment =
    String(usage?.subAccountId || usage?.wasabiAccountNumber || usage?.subAccountName || '').trim() || 'account';
  const dateSegment = String(usage?.periodStart || '').trim() || 'date';
  const rowSegment = String(usage?.id || '').trim() || 'usage';
  return `wasabi-main://${normalizeRackspaceRefSegment(accountSegment)}/${normalizeRackspaceRefSegment(
    dateSegment
  )}/${normalizeRackspaceRefSegment(rowSegment)}/${normalizeRackspaceRefSegment(componentKey)}`;
}

function buildWasabiMainUsageLineItems(usage = {}, pricing = {}) {
  const currency = String(pricing?.currency || 'USD').trim().toUpperCase() || 'USD';
  const daysInMonth = Math.max(1, Number(pricing?.daysInMonth || 30));
  const accountId = String(usage?.subAccountId || usage?.wasabiAccountNumber || '').trim() || null;
  const accountLabel = String(usage?.subAccountName || usage?.subAccountEmail || accountId || 'Wasabi account')
    .replace(/\s+/g, ' ')
    .trim();

  const activeStorage = Math.max(0, normalizeNumber(usage?.activeStorage));
  const deletedStorage = Math.max(0, normalizeNumber(usage?.deletedStorage));
  const minimumStorageGap = Math.max(0, Number(pricing?.minimumBillableTb || 0) - activeStorage);
  const ingress = Math.max(0, normalizeNumber(usage?.ingress));
  const egress = Math.max(0, normalizeNumber(usage?.egress));
  const apiCalls = Math.max(0, normalizeNumber(usage?.apiCalls));

  const componentRows = [
    {
      key: 'active-storage',
      resourceType: 'Active storage',
      quantity: activeStorage,
      unit: 'TB-day',
      amount: (activeStorage * Number(pricing?.activeStorageCostPerTbMonth || 0)) / daysInMonth
    },
    {
      key: 'deleted-storage',
      resourceType: 'Deleted storage',
      quantity: deletedStorage,
      unit: 'TB-day',
      amount: (deletedStorage * Number(pricing?.deletedStorageCostPerTbMonth || 0)) / daysInMonth
    },
    {
      key: 'minimum-storage',
      resourceType: 'Minimum storage charge',
      quantity: minimumStorageGap,
      unit: 'TB-day',
      amount: (minimumStorageGap * Number(pricing?.activeStorageCostPerTbMonth || 0)) / daysInMonth
    },
    {
      key: 'ingress',
      resourceType: 'Ingress',
      quantity: ingress,
      unit: 'TB',
      amount: ingress * Number(pricing?.ingressCostPerTb || 0)
    },
    {
      key: 'egress',
      resourceType: 'Egress',
      quantity: egress,
      unit: 'TB',
      amount: egress * Number(pricing?.egressCostPerTb || 0)
    },
    {
      key: 'api-calls',
      resourceType: 'API calls',
      quantity: apiCalls,
      unit: 'requests',
      amount: (apiCalls / 1_000_000) * Number(pricing?.apiCallsCostPerMillion || 0)
    }
  ];

  return componentRows
    .filter((row) => Number.isFinite(row.amount) && row.amount > 0)
    .map((row) => ({
      accountId,
      accountName: accountLabel || null,
      periodStart: usage?.periodStart || null,
      periodEnd: usage?.periodEnd || usage?.periodStart || null,
      id: String(usage?.id || '').trim() || null,
      invoiceId: null,
      controlInvoiceId: null,
      resourceType: row.resourceType,
      detailName: accountLabel || row.resourceType,
      amount: row.amount,
      currency,
      quantity: row.quantity,
      quantityUnit: row.unit,
      usageRecordId: String(usage?.id || '').trim() || null,
      resourceRef: buildWasabiMainUsageResourceRef(usage, row.key),
      sourceType: 'wasabi_main_usage'
    }));
}

function buildWasabiLineItemResourceRef(invoice = {}, costType = 'total') {
  const accountSegment =
    String(invoice?.subAccountId || '').trim() ||
    String(invoice?.wasabiAccountNumber || '').trim() ||
    String(invoice?.subAccountName || '').trim() ||
    'account';
  const invoiceSegment =
    String(invoice?.id || '').trim() ||
    String(invoice?.subInvoiceId || '').trim() ||
    String(invoice?.controlInvoiceId || '').trim() ||
    'invoice';
  return `wasabi://${normalizeRackspaceRefSegment(accountSegment)}/${normalizeRackspaceRefSegment(
    invoiceSegment
  )}/${normalizeRackspaceRefSegment(costType)}`;
}

function sanitizeWasabiInvoiceRow(row = {}, range = {}) {
  const period = buildWasabiInvoicePeriod(row, range);
  const totalAmount = computeWasabiInvoiceTotal(row);
  return {
    id: row?.id ?? null,
    subInvoiceId: row?.subInvoiceId ?? null,
    controlInvoiceId: row?.controlInvoiceId ?? null,
    subAccountId: row?.subAccountId ?? null,
    subAccountName: String(row?.subAccountName || '').trim() || null,
    subAccountEmail: String(row?.subAccountEmail || '').trim() || null,
    wasabiAccountNumber: row?.wasabiAccountNumber ?? null,
    channelAccountId: row?.channelAccountId ?? null,
    channelAccountName: String(row?.channelAccountName || '').trim() || null,
    periodStart: period.periodStart,
    periodEnd: period.periodEnd,
    totalAmount,
    currency: null,
    activeStorageTotalCost: normalizeNumber(row?.activeStorageTotalCost),
    deletedStorageTotalCost: normalizeNumber(row?.deletedStorageTotalCost),
    minimumActiveStorageTotalCost: normalizeNumber(row?.minimumActiveStorageTotalCost),
    apiCallsTotalCost: normalizeNumber(row?.apiCallsTotalCost),
    ingressTotalCost: normalizeNumber(row?.ingressTotalCost),
    egressTotalCost: normalizeNumber(row?.egressTotalCost)
  };
}

function buildWasabiInvoiceLineItems(invoice = {}, currency = 'USD', fallbackRange = null) {
  const period = buildWasabiInvoicePeriod(invoice, fallbackRange || {});
  const accountId = String(invoice?.subAccountId || invoice?.wasabiAccountNumber || '').trim() || null;
  const accountLabel = String(invoice?.subAccountName || invoice?.subAccountEmail || accountId || 'Wasabi account')
    .replace(/\s+/g, ' ')
    .trim();
  const baseItem = {
    invoiceId: String(invoice?.id || invoice?.subInvoiceId || '').trim() || null,
    controlInvoiceId: String(invoice?.controlInvoiceId || '').trim() || null,
    accountId,
    accountName: accountLabel || null,
    periodStart: period.periodStart,
    periodEnd: period.periodEnd
  };
  const rows = [];
  const components = [
    { key: 'active-storage', resourceType: 'Active storage', amount: normalizeNumber(invoice?.activeStorageTotalCost) },
    { key: 'deleted-storage', resourceType: 'Deleted storage', amount: normalizeNumber(invoice?.deletedStorageTotalCost) },
    {
      key: 'minimum-storage',
      resourceType: 'Minimum active storage',
      amount: normalizeNumber(invoice?.minimumActiveStorageTotalCost)
    },
    { key: 'api-calls', resourceType: 'API calls', amount: normalizeNumber(invoice?.apiCallsTotalCost) },
    { key: 'ingress', resourceType: 'Ingress', amount: normalizeNumber(invoice?.ingressTotalCost) },
    { key: 'egress', resourceType: 'Egress', amount: normalizeNumber(invoice?.egressTotalCost) }
  ];
  for (const component of components) {
    if (!Number.isFinite(component.amount) || component.amount <= 0) {
      continue;
    }
    rows.push({
      ...baseItem,
      resourceType: component.resourceType,
      detailName: accountLabel || component.resourceType,
      amount: component.amount,
      currency,
      resourceRef: buildWasabiLineItemResourceRef(invoice, component.key)
    });
  }

  if (!rows.length) {
    const total = computeWasabiInvoiceTotal(invoice);
    if (total > 0) {
      rows.push({
        ...baseItem,
        resourceType: 'Storage total',
        detailName: accountLabel || 'Storage total',
        amount: total,
        currency,
        resourceRef: buildWasabiLineItemResourceRef(invoice, 'storage-total')
      });
    }
  }
  return rows;
}

async function fetchWasabiWacmJson(url, options = {}, label = 'Wasabi WACM request') {
  const response = await fetch(url, options);
  const text = await response.text();
  let payload = null;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch (_error) {
      payload = null;
    }
  }
  if (!response.ok) {
    const details = text || JSON.stringify(payload || {});
    throw new Error(`${label} failed (${response.status}): ${details.slice(0, 300)}`);
  }
  if (payload && payload.success === false) {
    const message = String(payload.message || `${label} failed`).trim();
    throw new Error(`${label} failed: ${message}`);
  }
  return payload || {};
}

async function pullRackspaceBilling(vendor, credentials, options = {}) {
  const username = pickFirstString([
    credentials?.username,
    credentials?.userName,
    credentials?.user,
    process.env.RACKSPACE_USERNAME
  ]);
  const apiKey = pickFirstString([
    credentials?.apiKey,
    credentials?.api_key,
    credentials?.apikey,
    process.env.RACKSPACE_API_KEY
  ]);
  const preferredRegion = pickFirstString([
    credentials?.region,
    process.env.RACKSPACE_REGION
  ]);
  const identityUrl = normalizeBaseUrl(
    credentials?.identityUrl || process.env.RACKSPACE_IDENTITY_URL,
    'https://identity.api.rackspacecloud.com/v2.0/tokens'
  );

  if (!username || !apiKey) {
    throw new Error('Rackspace billing pull requires username and apiKey credentials.');
  }

  const tokenPayload = {
    auth: {
      'RAX-KSKEY:apiKeyCredentials': {
        username,
        apiKey
      }
    }
  };

  const tokenJson = await fetchRackspaceJson(
    identityUrl,
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json'
      },
      body: JSON.stringify(tokenPayload)
    },
    'Rackspace token request'
  );

  const accessToken = pickFirstString([
    tokenJson?.access?.token?.id,
    tokenJson?.token?.id,
    tokenJson?.accessToken
  ]);
  if (!accessToken) {
    throw new Error('Rackspace token response missing access token.');
  }

  const configuredBillingBaseUrl = pickFirstString([
    credentials?.billingBaseUrl,
    process.env.RACKSPACE_BILLING_BASE_URL
  ]);
  const catalogBillingBaseUrl = findRackspaceBillingBaseUrlFromCatalog(tokenJson, preferredRegion);
  const billingBaseUrl = normalizeBaseUrl(
    configuredBillingBaseUrl || catalogBillingBaseUrl || process.env.RACKSPACE_BASE_URL,
    'https://billing.api.rackspacecloud.com'
  );
  const requireDetailCsv = !['0', 'false', 'no', 'off'].includes(
    String(
      credentials?.requireDetailCsv !== undefined ? credentials.requireDetailCsv : process.env.RACKSPACE_REQUIRE_DETAIL_CSV || 'true'
    )
      .trim()
      .toLowerCase()
  );

  const commonHeaders = {
    'content-type': 'application/json',
    'x-auth-token': accessToken
  };
  const explicitAccountId = pickFirstString([
    credentials?.accountId,
    credentials?.account_id,
    credentials?.accountNumber,
    credentials?.ran,
    vendor?.accountId,
    process.env.RACKSPACE_ACCOUNT_ID,
    resolveRackspaceAccountIdFromToken(tokenJson)
  ]);
  let selectedAccountId = explicitAccountId;
  let selectedAccountName = '';

  if (!selectedAccountId) {
    throw new Error(
      'Rackspace billing pull requires a billing account number (RAN). Set accountId on the vendor or RACKSPACE_ACCOUNT_ID (example: 020-XXXX).'
    );
  }

  const accountUrl = buildRackspaceApiUrl(billingBaseUrl, `accounts/${encodeURIComponent(selectedAccountId)}`);
  let accountJson = null;
  try {
    accountJson = await fetchRackspaceJson(
      accountUrl,
      {
        method: 'GET',
        headers: commonHeaders
      },
      'Rackspace billing account request'
    );
  } catch (error) {
    const message = String(error?.message || '').trim();
    if (/RAN format error/i.test(message)) {
      throw new Error(
        `Rackspace billing account number is invalid (${selectedAccountId}). Set RACKSPACE_ACCOUNT_ID to a valid RAN (example: 020-XXXX).`
      );
    }
    throw error;
  }
  const accountPayload = accountJson?.billingAccount || accountJson?.account || accountJson || {};
  const resolvedAccountId = extractRackspaceAccountId(accountPayload);
  if (resolvedAccountId) {
    selectedAccountId = resolvedAccountId;
  }
  selectedAccountName = extractRackspaceAccountName(accountPayload);

  const range = resolveBillingRange(options);
  const rackspaceHistoryWindowStart = getDateOnlyYearsAgo(2);
  const summaryQueryStart = maxDateOnly(range.periodStart, rackspaceHistoryWindowStart);
  const parsedSummaryStart = parseIsoDateOnly(summaryQueryStart);
  const parsedWindowEnd = parseIsoDateOnly(range.periodEndExclusive);
  if (!parsedSummaryStart || !parsedWindowEnd || parsedSummaryStart.date.getTime() >= parsedWindowEnd.date.getTime()) {
    throw new Error(
      `Rackspace billing range ${range.periodStart} to ${range.periodEnd} is outside the API history window (last 2 years).`
    );
  }
  const summaryQueryEnd = addDaysToDateOnly(range.periodEndExclusive, 45);
  const summaryUrl = new URL(buildRackspaceApiUrl(billingBaseUrl, `accounts/${encodeURIComponent(selectedAccountId)}/billing-summary`));
  summaryUrl.searchParams.set('startDate', summaryQueryStart);
  summaryUrl.searchParams.set('endDate', summaryQueryEnd);
  const summaryJson = await fetchRackspaceJson(
    summaryUrl.toString(),
    {
      method: 'GET',
      headers: commonHeaders
    },
    'Rackspace billing summary request'
  );

  const normalizedRows = normalizeRackspaceSummaryRows(summaryJson);
  const windowStart = range.periodStart;
  const windowEndExclusive = range.periodEndExclusive;
  const coverageRows = normalizedRows.filter((row) => {
    if (!row.coverageStart || !row.coverageEndExclusive) {
      return false;
    }
    return intervalsOverlap(row.coverageStart, row.coverageEndExclusive, windowStart, windowEndExclusive);
  });
  const chargeRows = coverageRows.filter((row) => !['PAYMENT', 'REFUND'].includes(row.type));
  const selectedRows = chargeRows.length
    ? chargeRows
    : normalizedRows.filter((row) => {
        if (['PAYMENT', 'REFUND'].includes(row.type)) {
          return false;
        }
        if (!row.postedDate) {
          return false;
        }
        return row.postedDate >= windowStart && row.postedDate <= range.periodEnd;
      });

  const invoiceDetails = [];
  const rackspaceUsageRows = [];
  const invoiceDetailCsv = [];
  const usageDedupe = new Set();
  const invoiceRows = selectedRows.filter((row) => {
    const href = String(row?.invoiceHref || '').trim().toLowerCase();
    return href.includes('/invoices/');
  });
  for (const row of selectedRows) {
    const href = String(row.invoiceHref || '').trim();
    if (!href || !href.toLowerCase().includes('/invoices/')) {
      continue;
    }
    try {
      const detail = await fetchRackspaceJson(
        href,
        {
          method: 'GET',
          headers: commonHeaders
        },
        'Rackspace invoice detail request'
      );
      invoiceDetails.push(detail);
    } catch (_error) {
      // Keep billing pull resilient if one invoice detail request fails.
    }

    const detailCsvUrl = `${href.replace(/\/+$/g, '')}/detail`;
    try {
      const csvText = await fetchRackspaceText(
        detailCsvUrl,
        {
          method: 'GET',
          headers: {
            'x-auth-token': accessToken,
            accept: 'text/csv'
          }
        },
        'Rackspace invoice detail CSV request'
      );
      const parsedUsageRows = parseRackspaceUsageCsv(csvText, {
        periodStart: range.periodStart,
        periodEnd: range.periodEnd,
        fileName: `${String(row.invoiceId || 'invoice').trim() || 'invoice'}.csv`
      });
      let addedRows = 0;
      for (const usageRow of parsedUsageRows) {
        const key = buildRackspaceUsageRowDedupeKey(usageRow);
        if (usageDedupe.has(key)) {
          continue;
        }
        usageDedupe.add(key);
        rackspaceUsageRows.push(usageRow);
        addedRows += 1;
      }
      invoiceDetailCsv.push({
        invoiceId: row.invoiceId || null,
        invoiceHref: href,
        detailUrl: detailCsvUrl,
        rowCount: addedRows,
        ok: true
      });
    } catch (error) {
      invoiceDetailCsv.push({
        invoiceId: row.invoiceId || null,
        invoiceHref: href,
        detailUrl: detailCsvUrl,
        rowCount: 0,
        ok: false,
        error: error?.message || 'Rackspace invoice detail CSV request failed.'
      });
    }
  }

  const csvSuccessCount = invoiceDetailCsv.filter((item) => item && item.ok).length;
  if (requireDetailCsv && invoiceRows.length && csvSuccessCount === 0) {
    throw new Error(
      `Rackspace invoice detail CSV import failed for ${range.periodStart} to ${range.periodEnd}: 0/${invoiceRows.length} invoice CSV files succeeded.`
    );
  }
  if (requireDetailCsv && invoiceRows.length && rackspaceUsageRows.length === 0) {
    throw new Error(
      `Rackspace invoice detail CSV import returned no usage rows for ${range.periodStart} to ${range.periodEnd}.`
    );
  }

  const csvBreakdown = summarizeRackspaceUsageRowsByResourceType(rackspaceUsageRows);
  let resourceBreakdown = csvBreakdown.length
    ? csvBreakdown
    : summarizeRackspaceInvoiceSections(invoiceDetails, 'USD');
  if (!resourceBreakdown.length) {
    const fallbackRows = selectedRows.map((row) => ({
      resourceType: normalizeResourceType(
        row?.sourceRow?.name ||
          row?.sourceRow?.description ||
          row?.sourceRow?.serviceName ||
          row?.sourceRow?.type ||
          'Uncategorized'
      ),
      currency: String(
        row?.sourceRow?.currency ||
          row?.sourceRow?.currencyCode ||
          summaryJson?.billingSummary?.currency ||
          summaryJson?.currency ||
          'USD'
      ).trim().toUpperCase() || 'USD',
      amount: row.amount
    }));
    resourceBreakdown = summarizeBreakdownRows(fallbackRows);
  }

  const amountFromUsageRows = rackspaceUsageRows.reduce((sum, row) => sum + normalizeNumber(row.amount), 0);
  const amountFromSelectedRows = selectedRows.reduce((sum, row) => sum + normalizeNumber(row.amount), 0);
  const amount = rackspaceUsageRows.length ? amountFromUsageRows : amountFromSelectedRows;
  const currency = String(
    pickFirstString([
      resourceBreakdown[0]?.currency,
      summaryJson?.summary?.currency,
      summaryJson?.summary?.currencyCode,
      summaryJson?.billingSummary?.currency,
      summaryJson?.billingSummary?.currencyCode,
      'USD'
    ], 'USD')
  ).trim().toUpperCase() || 'USD';

  return {
    periodStart: range.periodStart,
    periodEnd: range.periodEnd,
    amount,
    currency,
    source: 'rackspace-billing-api',
    raw: {
      accountId: selectedAccountId,
      accountName: selectedAccountName || null,
      accountDetails: accountJson,
      billingSummary: summaryJson,
      summaryQuery: {
        startDate: summaryQueryStart,
        requestedStartDate: range.periodStart,
        endDate: summaryQueryEnd
      },
      selectedRows: selectedRows.map((row) => ({
        invoiceId: row.invoiceId,
        invoiceHref: row.invoiceHref,
        amount: row.amount,
        type: row.type,
        postedDate: row.postedDate,
        coverageStart: row.coverageStart,
        coverageEndExclusive: row.coverageEndExclusive
      })),
      invoiceDetails,
      rackspaceUsageRows,
      rackspaceUsageCsvMeta: {
        importedAt: new Date().toISOString(),
        source: 'rackspace-invoice-detail-csv',
        rowCount: rackspaceUsageRows.length,
        invoices: invoiceDetailCsv
      },
      resourceBreakdown
    }
  };
}

async function pullAzureBilling(vendor, credentials, options = {}) {
  const tenantId = String(credentials?.tenantId || '').trim();
  const clientId = String(credentials?.clientId || '').trim();
  const clientSecret = String(credentials?.clientSecret || '').trim();
  const subscriptionId = String(credentials?.subscriptionId || vendor.subscriptionId || '').trim();

  if (!tenantId || !clientId || !clientSecret || !subscriptionId) {
    throw new Error('Azure billing pull requires tenantId, clientId, clientSecret, and subscriptionId.');
  }

  const tokenUrl = `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`;
  const tokenBody = new URLSearchParams({
    client_id: clientId,
    client_secret: clientSecret,
    grant_type: 'client_credentials',
    scope: 'https://management.azure.com/.default'
  });

  const tokenRes = await fetch(tokenUrl, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded'
    },
    body: tokenBody
  });

  if (!tokenRes.ok) {
    const details = await tokenRes.text();
    throw new Error(`Azure token request failed (${tokenRes.status}): ${details.slice(0, 300)}`);
  }

  const tokenJson = await tokenRes.json();
  const accessToken = tokenJson.access_token;
  if (!accessToken) {
    throw new Error('Azure token response missing access_token.');
  }

  const range = resolveBillingRange(options);
  const isCustomRange = Boolean(options?.periodStart || options?.periodEnd);
  const queryUrl = `https://management.azure.com/subscriptions/${encodeURIComponent(
    subscriptionId
  )}/providers/Microsoft.CostManagement/query?api-version=2023-03-01`;

  const buildAzureQueryPayload = (includeResourceId) => {
    const payload = {
      type: 'ActualCost',
      timeframe: isCustomRange ? 'Custom' : 'MonthToDate',
      dataset: {
        granularity: 'None',
        aggregation: {
          totalCost: {
            name: 'Cost',
            function: 'Sum'
          }
        },
        grouping: [
          {
            type: 'Dimension',
            name: 'ServiceName'
          },
          ...(includeResourceId
            ? [
                {
                  type: 'Dimension',
                  name: 'ResourceId'
                }
              ]
            : [])
        ]
      }
    };
    if (isCustomRange) {
      payload.timePeriod = {
        from: `${range.periodStart}T00:00:00.000Z`,
        to: `${range.periodEndExclusive}T00:00:00.000Z`
      };
    }
    return payload;
  };

  async function runAzureCostQuery(payload) {
    const queryRes = await fetch(queryUrl, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${accessToken}`,
        'content-type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    if (!queryRes.ok) {
      const details = await queryRes.text().catch(() => '');
      const error = new Error(`Azure cost query failed (${queryRes.status}): ${details.slice(0, 300)}`);
      error.statusCode = queryRes.status;
      throw error;
    }

    return queryRes.json();
  }

  let queryJson = null;
  let queryMode = 'service';
  try {
    queryJson = await runAzureCostQuery(buildAzureQueryPayload(true));
    queryMode = 'service+resource';
  } catch (error) {
    if (Number(error?.statusCode) < 400 || Number(error?.statusCode) >= 500) {
      throw error;
    }
    queryJson = await runAzureCostQuery(buildAzureQueryPayload(false));
    queryMode = 'service';
  }

  const resourceBreakdown = extractAzureResourceBreakdown(queryJson, 'USD');
  const amount = resourceBreakdown.reduce((sum, row) => sum + normalizeNumber(row.amount), 0);
  const currency = resourceBreakdown[0]?.currency || 'USD';

  return {
    periodStart: range.periodStart,
    periodEnd: range.periodEnd,
    amount,
    currency,
    source: 'azure-cost-management-api',
    raw: {
      subscriptionId,
      queryMode,
      costQuery: queryJson,
      resourceBreakdown
    }
  };
}

async function pullAwsBilling(vendor, credentials, options = {}) {
  const accessKeyId = String(credentials?.accessKeyId || '').trim();
  const secretAccessKey = String(credentials?.secretAccessKey || '').trim();
  const sessionToken = String(credentials?.sessionToken || '').trim();
  const profile = String(credentials?.profile || '').trim();

  const range = resolveBillingRange(options);
  const clientConfig = {
    region: 'us-east-1'
  };
  if (accessKeyId && secretAccessKey) {
    clientConfig.credentials = {
      accessKeyId,
      secretAccessKey,
      ...(sessionToken ? { sessionToken } : {})
    };
  }

  const prevProfile = process.env.AWS_PROFILE;
  const shouldSetProfile = !clientConfig.credentials && Boolean(profile);
  if (shouldSetProfile) {
    process.env.AWS_PROFILE = profile;
  }
  const client = new CostExplorerClient(clientConfig);

  try {
    const command = new GetCostAndUsageCommand({
      TimePeriod: {
        Start: range.periodStart,
        End: range.periodEndExclusive
      },
      Granularity: 'MONTHLY',
      Metrics: ['UnblendedCost'],
      GroupBy: [
        {
          Type: 'DIMENSION',
          Key: 'SERVICE'
        },
        {
          Type: 'DIMENSION',
          Key: 'USAGE_TYPE'
        }
      ]
    });

    const response = await client.send(command);
    const first = Array.isArray(response?.ResultsByTime) ? response.ResultsByTime[0] : null;
    const costNode = first?.Total?.UnblendedCost || null;
    const resourceBreakdown = extractAwsResourceBreakdown(response, String(costNode?.Unit || 'USD'));
    const breakdownTotal = resourceBreakdown.reduce((sum, row) => sum + normalizeNumber(row.amount), 0);
    const amount = normalizeNumber(costNode?.Amount) || breakdownTotal;
    const currency = String(costNode?.Unit || resourceBreakdown[0]?.currency || 'USD').trim().toUpperCase();

    return {
      periodStart: range.periodStart,
      periodEnd: range.periodEnd,
      amount,
      currency,
      source: 'aws-cost-explorer-api',
      raw: {
        accountId: vendor.accountId || null,
        result: response,
        resourceBreakdown
      }
    };
  } catch (error) {
    if (!clientConfig.credentials) {
      throw new Error(
        `AWS billing pull failed using default credential chain${profile ? ` (profile ${profile})` : ''}: ${
          error?.message || String(error)
        }`
      );
    }
    throw error;
  } finally {
    if (shouldSetProfile) {
      if (prevProfile) {
        process.env.AWS_PROFILE = prevProfile;
      } else {
        delete process.env.AWS_PROFILE;
      }
    }
  }
}

async function pullWasabiBilling(vendor, credentials, options = {}) {
  const username = pickFirstString([
    credentials?.wacmUsername,
    credentials?.username,
    credentials?.userName,
    credentials?.user,
    process.env.WASABI_WACM_USERNAME
  ]);
  const apiKey = pickFirstString([
    credentials?.wacmApiKey,
    credentials?.apiKey,
    credentials?.api_key,
    credentials?.apikey,
    process.env.WASABI_WACM_API_KEY
  ]);
  if (!username || !apiKey) {
    throw new Error('Wasabi billing pull requires WASABI_WACM_USERNAME and WASABI_WACM_API_KEY credentials.');
  }

  const endpoint = normalizeWasabiWacmEndpoint(
    pickFirstString([
      credentials?.wacmEndpoint,
      credentials?.endpoint,
      credentials?.baseUrl,
      process.env.WASABI_WACM_ENDPOINT
    ])
  );
  const range = resolveBillingRange(options);
  const pageSize = Math.max(
    10,
    Math.min(500, Math.floor(Number(credentials?.pageSize || process.env.WASABI_WACM_PAGE_SIZE || 100)))
  );
  const maxPages = Math.max(
    1,
    Math.min(500, Math.floor(Number(credentials?.maxPages || process.env.WASABI_WACM_MAX_PAGES || 200)))
  );
  const defaultCurrency = String(credentials?.currency || process.env.WASABI_WACM_CURRENCY || 'USD')
    .trim()
    .toUpperCase() || 'USD';
  const authHeader = `Basic ${Buffer.from(`${username}:${apiKey}`).toString('base64')}`;

  const configuredAccountFilters = [
    credentials?.subAccountId,
    credentials?.wasabiAccountNumber,
    credentials?.accountId
  ];
  if (shouldUseWasabiVendorAccountIdFilter(vendor?.accountId)) {
    configuredAccountFilters.push(vendor.accountId);
  }
  const normalizedAccountFilters = Array.from(
    new Set(
      configuredAccountFilters
        .map((value) => normalizeWasabiLookupKey(value))
        .filter(Boolean)
    )
  );

  let page = 1;
  let pagesFetched = 0;
  let totalReported = null;
  const allInvoices = [];

  while (page <= maxPages) {
    const url = new URL(endpoint);
    url.searchParams.set('page', String(page));
    url.searchParams.set('size', String(pageSize));
    const payload = await fetchWasabiWacmJson(
      url.toString(),
      {
        method: 'GET',
        headers: {
          authorization: authHeader,
          accept: 'application/json'
        }
      },
      'Wasabi invoice list request'
    );

    const items = parseWasabiWacmItems(payload);
    const pagination = parseWasabiWacmPagination(payload);
    pagesFetched += 1;
    if (Number.isFinite(pagination.total) && pagination.total >= 0) {
      totalReported = pagination.total;
    }
    if (!items.length) {
      break;
    }
    allInvoices.push(...items);

    if (items.length < pageSize) {
      break;
    }
    if (Number.isFinite(totalReported) && allInvoices.length >= totalReported) {
      break;
    }
    page += 1;
  }

  const filteredInvoices = allInvoices
    .filter((invoice) => invoiceOverlapsBillingRange(invoice, range))
    .filter((invoice) => invoiceMatchesWasabiAccountFilter(invoice, normalizedAccountFilters));
  const invoices = filteredInvoices.map((invoice) => sanitizeWasabiInvoiceRow(invoice, range));
  const lineItems = filteredInvoices.flatMap((invoice) =>
    buildWasabiInvoiceLineItems(invoice, defaultCurrency, range)
  );
  const resourceBreakdown = summarizeBreakdownRows(
    lineItems.map((line) => ({
      resourceType: line.resourceType,
      currency: line.currency || defaultCurrency,
      amount: line.amount
    }))
  );
  const amount = invoices.reduce((sum, invoice) => sum + normalizeNumber(invoice.totalAmount), 0);
  const currency = resourceBreakdown[0]?.currency || defaultCurrency;

  return {
    periodStart: range.periodStart,
    periodEnd: range.periodEnd,
    amount,
    currency,
    source: 'wasabi-wacm-api',
    raw: {
      endpoint,
      accountFilter: normalizedAccountFilters,
      pagination: {
        pagesFetched,
        pageSize,
        maxPages,
        totalFetched: allInvoices.length,
        totalReported
      },
      invoices,
      lineItems,
      resourceBreakdown
    }
  };
}

async function pullWasabiMainBilling(vendor, credentials, options = {}) {
  const username = pickFirstString([
    credentials?.wacmUsername,
    credentials?.username,
    credentials?.userName,
    credentials?.user,
    process.env.WASABI_MAIN_WACM_USERNAME,
    process.env.WASABI_WACM_USERNAME
  ]);
  const apiKey = pickFirstString([
    credentials?.wacmApiKey,
    credentials?.apiKey,
    credentials?.api_key,
    credentials?.apikey,
    process.env.WASABI_MAIN_WACM_API_KEY,
    process.env.WASABI_WACM_API_KEY
  ]);
  if (!username || !apiKey) {
    throw new Error(
      'Wasabi-MAIN billing pull requires WASABI_MAIN_WACM_USERNAME/WASABI_MAIN_WACM_API_KEY (or WASABI_WACM_* fallback).'
    );
  }

  const endpoint = normalizeWasabiWacmUsageEndpoint(
    pickFirstString([
      credentials?.wacmUsageEndpoint,
      credentials?.usageEndpoint,
      credentials?.endpoint,
      credentials?.baseUrl,
      process.env.WASABI_MAIN_USAGE_ENDPOINT,
      process.env.WASABI_WACM_USAGE_ENDPOINT,
      process.env.WASABI_WACM_ENDPOINT
    ])
  );
  const controlUsageEndpoint = normalizeWasabiWacmControlUsageEndpoint(
    pickFirstString([
      credentials?.wacmControlUsageEndpoint,
      credentials?.controlUsageEndpoint,
      credentials?.controlEndpoint,
      process.env.WASABI_MAIN_CONTROL_USAGE_ENDPOINT
    ])
  );
  const range = resolveBillingRange(options);
  const pageSize = Math.max(
    10,
    Math.min(200, Math.floor(Number(credentials?.pageSize || process.env.WASABI_MAIN_PAGE_SIZE || 100)))
  );
  const maxPages = Math.max(
    1,
    Math.min(500, Math.floor(Number(credentials?.maxPages || process.env.WASABI_MAIN_MAX_PAGES || 200)))
  );
  const includeAllAccounts = parseBooleanLike(
    credentials?.includeAllAccounts ?? process.env.WASABI_MAIN_INCLUDE_ALL_ACCOUNTS,
    true
  );
  const includeSubAccounts = parseBooleanLike(
    credentials?.includeSubAccounts ?? process.env.WASABI_MAIN_INCLUDE_SUB_ACCOUNTS,
    false
  );
  const authHeader = `Basic ${Buffer.from(`${username}:${apiKey}`).toString('base64')}`;
  const pricingConfig = resolveWasabiMainPricingConfig(credentials);

  const configuredAccountFilters = [
    credentials?.subAccountId,
    credentials?.wasabiAccountNumber,
    credentials?.accountId
  ];
  if (!includeAllAccounts && String(vendor?.accountId || '').trim()) {
    configuredAccountFilters.push(vendor.accountId);
  }
  const normalizedAccountFilters = Array.from(
    new Set(
      configuredAccountFilters
        .map((value) => normalizeWasabiLookupKey(value))
        .filter(Boolean)
    )
  );

  async function fetchWasabiUsageRecords(listEndpoint, label, startPage = 1) {
    let page = startPage;
    let pagesFetched = 0;
    let totalReported = null;
    const rows = [];
    let reachedEmptyPage = false;
    while (pagesFetched < maxPages) {
      const url = new URL(listEndpoint);
      url.searchParams.set('from', range.periodStart);
      url.searchParams.set('to', range.periodEnd);
      url.searchParams.set('page', String(page));
      url.searchParams.set('size', String(pageSize));
      const payload = await fetchWasabiWacmJson(
        url.toString(),
        {
          method: 'GET',
          headers: {
            authorization: authHeader,
            accept: 'application/json'
          }
        },
        label
      );

      const items = parseWasabiWacmItems(payload);
      const pagination = parseWasabiWacmPagination(payload);
      pagesFetched += 1;
      if (Number.isFinite(pagination.total) && pagination.total >= 0) {
        totalReported = pagination.total;
      }
      if (!items.length) {
        reachedEmptyPage = true;
        break;
      }
      rows.push(...items);
      if (items.length < pageSize) {
        break;
      }
      if (Number.isFinite(totalReported) && rows.length >= totalReported) {
        break;
      }
      page += 1;
    }
    return {
      rows,
      pagination: {
        startPage,
        pagesFetched,
        pageSize,
        maxPages,
        totalFetched: rows.length,
        totalReported,
        reachedEmptyPage
      }
    };
  }

  const subUsageFetch = includeSubAccounts
    ? await fetchWasabiUsageRecords(endpoint, 'Wasabi usage list request', 1)
    : {
        rows: [],
        pagination: {
          startPage: 1,
          pagesFetched: 0,
          pageSize,
          maxPages,
          totalFetched: 0,
          totalReported: 0,
          reachedEmptyPage: false,
          skipped: true
        }
      };

  let controlUsageFetch = {
    rows: [],
    pagination: {
      startPage: 0,
      pagesFetched: 0,
      pageSize,
      maxPages,
      totalFetched: 0,
      totalReported: null,
      reachedEmptyPage: false
    }
  };
  let controlUsageWarning = null;
  try {
    controlUsageFetch = await fetchWasabiUsageRecords(
      controlUsageEndpoint,
      'Wasabi control account usage list request',
      0
    );
  } catch (error) {
    controlUsageWarning = error?.message || 'Control account usage endpoint request failed.';
  }

  const includeRowByAccountFilter = (row) =>
    includeAllAccounts ? true : usageMatchesWasabiAccountFilter(row, normalizedAccountFilters);

  const subUsages = includeSubAccounts
    ? subUsageFetch.rows
        .filter((row) => usageOverlapsBillingRange(row, range))
        .filter(includeRowByAccountFilter)
        .map((row) => sanitizeWasabiUsageRow(row, range))
    : [];

  const controlUsages = controlUsageFetch.rows
    .filter((row) => usageOverlapsBillingRange(row, range))
    .map((row) => sanitizeWasabiControlUsageRow(row, range));

  const usageByKey = new Map();
  for (const usage of [...subUsages, ...controlUsages]) {
    const key = [
      String(usage?.sourceType || 'wasabi_main_usage'),
      String(usage?.id || ''),
      String(usage?.subAccountId || usage?.wasabiAccountNumber || ''),
      String(usage?.periodStart || ''),
      String(usage?.periodEnd || '')
    ].join('::');
    if (!usageByKey.has(key)) {
      usageByKey.set(key, usage);
    }
  }
  const usages = Array.from(usageByKey.values());
  const lineItems = usages.flatMap((row) => buildWasabiMainUsageLineItems(row, pricingConfig));
  const resourceBreakdown = summarizeBreakdownRows(
    lineItems.map((line) => ({
      resourceType: line.resourceType,
      currency: line.currency || pricingConfig.currency,
      amount: line.amount
    }))
  );
  const amount = lineItems.reduce((sum, row) => sum + normalizeNumber(row.amount), 0);
  const currency = resourceBreakdown[0]?.currency || pricingConfig.currency || 'USD';

  return {
    periodStart: range.periodStart,
    periodEnd: range.periodEnd,
    amount,
    currency,
    source: 'wasabi-main-usage-api',
    raw: {
      endpoint,
      controlUsageEndpoint,
      accountFilter: includeAllAccounts ? [] : normalizedAccountFilters,
      includeAllAccounts,
      includeSubAccounts,
      pricing: pricingConfig,
      pagination: {
        subUsage: subUsageFetch.pagination,
        controlUsage: controlUsageFetch.pagination
      },
      controlUsageWarning,
      controlUsageIncluded: controlUsages.length > 0,
      subUsages,
      controlUsages,
      usages,
      lineItems,
      resourceBreakdown
    }
  };
}

async function pullGcpBilling(vendor, credentials, options = {}) {
  const billingAccountId = String(credentials?.billingAccountId || vendor.accountId || '').trim();
  const manualAmount = credentials && Number.isFinite(Number(credentials.manualMonthlyCost))
    ? Number(credentials.manualMonthlyCost)
    : null;

  if (manualAmount !== null) {
    const range = resolveBillingRange(options);
    return {
      periodStart: range.periodStart,
      periodEnd: range.periodEnd,
      amount: manualAmount,
      currency: String(credentials.manualCurrency || 'USD').trim().toUpperCase(),
      source: 'gcp-manual-fallback',
      raw: {
        billingAccountId,
        note: 'Manual value used because direct billing export integration is not configured.',
        resourceBreakdown: [
          {
            resourceType: 'Manual',
            currency: String(credentials.manualCurrency || 'USD').trim().toUpperCase(),
            amount: manualAmount
          }
        ]
      }
    };
  }

  throw new Error(
    'GCP automated billing pull requires billing export integration. Add manualMonthlyCost in credentials for now.'
  );
}

async function pullBillingForVendor(vendor, credentials, options = {}) {
  switch (vendor.provider) {
    case 'azure':
      return pullAzureBilling(vendor, credentials, options);
    case 'aws':
      return pullAwsBilling(vendor, credentials, options);
    case 'wasabi':
    case 'wasabi-wacm':
      return pullWasabiBilling(vendor, credentials, options);
    case 'wasabi-main':
      return pullWasabiMainBilling(vendor, credentials, options);
    case 'gcp':
      return pullGcpBilling(vendor, credentials, options);
    case 'rackspace':
      return pullRackspaceBilling(vendor, credentials, options);
    default:
      throw new Error(`Provider ${vendor.provider} billing pull is not supported yet.`);
  }
}

module.exports = {
  pullBillingForVendor,
  getCurrentMonthRange,
  resolveBillingRange
};
