const { listVendors, getVendorById } = require('./db');
const { decryptJson } = require('./crypto');

const DEFAULT_REGION = 'us-east-1';
const DEFAULT_CLOUDWATCH_REGION = 'us-east-1';
const TRUE_VALUES = new Set(['1', 'true', 'yes', 'y', 'on']);
const FALSE_VALUES = new Set(['0', 'false', 'no', 'n', 'off']);

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

function normalizeLookupKey(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeAwsAccessKey(value) {
  return String(value || '').trim().toUpperCase();
}

function normalizeAwsAccountId(value, fallback = 'aws-default') {
  return String(value || fallback)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-');
}

function parseBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'number') {
    return value !== 0;
  }
  const text = String(value).trim().toLowerCase();
  if (TRUE_VALUES.has(text)) {
    return true;
  }
  if (FALSE_VALUES.has(text)) {
    return false;
  }
  return fallback;
}

function normalizeRegion(value, fallback = DEFAULT_REGION) {
  const text = String(value || '').trim();
  if (!text) {
    return String(fallback || DEFAULT_REGION).trim() || DEFAULT_REGION;
  }
  return text;
}

function normalizeRegionList(value, fallbackRegion = DEFAULT_REGION) {
  const fallback = normalizeRegion(fallbackRegion, DEFAULT_REGION);

  if (Array.isArray(value)) {
    const normalized = value
      .map((entry) => normalizeRegion(entry, ''))
      .filter(Boolean);
    return normalized.length ? Array.from(new Set(normalized)) : [fallback];
  }

  if (typeof value === 'string') {
    const normalized = value
      .split(',')
      .map((entry) => normalizeRegion(entry, ''))
      .filter(Boolean);
    return normalized.length ? Array.from(new Set(normalized)) : [fallback];
  }

  return [fallback];
}

function normalizeUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    return null;
  }
  try {
    const parsed = new URL(raw);
    return parsed.toString().replace(/\/$/, '');
  } catch (_error) {
    return null;
  }
}

function firstNonEmpty(...values) {
  for (const value of values) {
    if (value === undefined || value === null) {
      continue;
    }
    const text = String(value).trim();
    if (text) {
      return text;
    }
  }
  return '';
}

function normalizeAwsAccountCandidate(rawAccount, index = 0, defaults = {}) {
  if (!rawAccount || typeof rawAccount !== 'object' || Array.isArray(rawAccount)) {
    return null;
  }

  const accessKeyId = firstNonEmpty(
    rawAccount.accessKeyId,
    rawAccount.accessKey,
    rawAccount.access_key,
    rawAccount.awsAccessKeyId,
    rawAccount.keyId,
    rawAccount.access_key_id,
    rawAccount.aws_access_key_id
  );
  const secretAccessKey = firstNonEmpty(
    rawAccount.secretAccessKey,
    rawAccount.secretKey,
    rawAccount.secret_key,
    rawAccount.awsSecretAccessKey,
    rawAccount.secret_access_key,
    rawAccount.aws_secret_access_key
  );
  const profile = firstNonEmpty(
    rawAccount.profile,
    rawAccount.awsProfile,
    rawAccount.aws_profile
  );
  if (!(accessKeyId && secretAccessKey) && !profile) {
    return null;
  }

  const accountId = normalizeAwsAccountId(
    firstNonEmpty(
      rawAccount.accountId,
      rawAccount.account_id,
      rawAccount.account,
      rawAccount.id,
      rawAccount.accountNumber,
      rawAccount.account_number,
      defaults.accountId,
      `aws-${index + 1}`
    ),
    `aws-${index + 1}`
  );
  const displayName =
    firstNonEmpty(
      rawAccount.displayName,
      rawAccount.label,
      rawAccount.name,
      rawAccount.accountName,
      defaults.displayName,
      accountId,
      'AWS'
    ) || 'AWS';
  const region = normalizeRegion(
    firstNonEmpty(
      rawAccount.region,
      rawAccount.defaultRegion,
      rawAccount.awsRegion,
      defaults.region,
      process.env.AWS_DEFAULT_REGION,
      DEFAULT_REGION
    ),
    DEFAULT_REGION
  );
  const cloudWatchRegion = normalizeRegion(
    firstNonEmpty(
      rawAccount.cloudWatchRegion,
      rawAccount.cloudwatchRegion,
      rawAccount.cloud_watch_region,
      defaults.cloudWatchRegion,
      process.env.AWS_DEFAULT_CLOUDWATCH_REGION,
      region,
      DEFAULT_CLOUDWATCH_REGION
    ),
    region
  );
  const efsRegions = normalizeRegionList(
    rawAccount.efsRegions || rawAccount.efs_regions || defaults.efsRegions || process.env.AWS_DEFAULT_EFS_REGIONS || '',
    region
  );
  const metricRegions = normalizeRegionList(
    rawAccount.metricRegions ||
      rawAccount.cloudWatchRegions ||
      rawAccount.cloudwatchRegions ||
      rawAccount.monitoringRegions ||
      rawAccount.regions ||
      defaults.metricRegions ||
      process.env.CLOUD_METRICS_AWS_REGIONS ||
      cloudWatchRegion,
    cloudWatchRegion
  );
  const sessionToken = firstNonEmpty(
    rawAccount.sessionToken,
    rawAccount.session_token,
    rawAccount.token,
    rawAccount.awsSessionToken
  );

  return {
    accountId,
    displayName,
    accessKeyId,
    secretAccessKey,
    sessionToken: sessionToken || null,
    profile: profile || null,
    region,
    cloudWatchRegion,
    efsRegions,
    metricRegions,
    s3Endpoint: normalizeUrl(rawAccount.s3Endpoint || rawAccount.s3_endpoint || rawAccount.endpoint || defaults.s3Endpoint),
    forcePathStyle: parseBoolean(
      rawAccount.forcePathStyle ?? rawAccount.force_path_style ?? rawAccount.s3ForcePathStyle ?? defaults.forcePathStyle,
      false
    ),
    requestMetricsEnabledByDefault: parseBoolean(
      rawAccount.requestMetricsEnabledByDefault ??
        rawAccount.request_metrics_enabled_default ??
        rawAccount.enableRequestMetrics ??
        defaults.requestMetricsEnabledByDefault,
      false
    )
  };
}

function parseAwsAccountsJsonPayload(rawJson) {
  const normalized = normalizeJsonEnvValue(rawJson);
  if (!normalized) {
    return [];
  }
  try {
    const parsed = JSON.parse(normalized);
    if (Array.isArray(parsed)) {
      return parsed;
    }
    if (parsed && typeof parsed === 'object') {
      return Object.values(parsed);
    }
    return [];
  } catch (_error) {
    return [];
  }
}

function collectAwsCandidatesFromCredentials(credentials = {}) {
  const candidates = [];
  const pushCandidate = (value) => {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      candidates.push(value);
    }
  };

  pushCandidate(credentials);
  pushCandidate(credentials.aws);
  pushCandidate(credentials.AWS);

  const arrayKeys = ['accounts', 'awsAccounts', 'aws_accounts', 'AWS_ACCOUNTS'];
  for (const key of arrayKeys) {
    const value = credentials[key];
    if (!Array.isArray(value)) {
      continue;
    }
    for (const row of value) {
      pushCandidate(row);
    }
  }

  const jsonKeys = ['AWS_ACCOUNTS_JSON', 'awsAccountsJson', 'aws_accounts_json'];
  for (const key of jsonKeys) {
    const payload = parseAwsAccountsJsonPayload(credentials[key]);
    for (const row of payload) {
      pushCandidate(row);
    }
  }

  return candidates;
}

function readAwsVendors(options = {}) {
  const includeHidden = Boolean(options.includeHidden);
  const vendors = listVendors().filter((vendor) => normalizeLookupKey(vendor.provider) === 'aws');
  return vendors.filter((vendor) => {
    if (includeHidden) {
      return true;
    }
    const hiddenRaw = vendor?.metadata?.hidden;
    if (hiddenRaw === true || hiddenRaw === 1) {
      return false;
    }
    if (typeof hiddenRaw === 'string') {
      return !TRUE_VALUES.has(hiddenRaw.trim().toLowerCase());
    }
    return true;
  });
}

function listAwsVendorAccounts(options = {}) {
  const includeHidden = Boolean(options.includeHidden);
  const vendors = readAwsVendors({ includeHidden });
  const accounts = [];

  for (const vendor of vendors) {
    const fullVendor = getVendorById(vendor.id);
    if (!fullVendor || !fullVendor.credentialsEncrypted) {
      continue;
    }

    let credentials = null;
    try {
      credentials = decryptJson(fullVendor.credentialsEncrypted);
    } catch (_error) {
      continue;
    }
    if (!credentials || typeof credentials !== 'object') {
      continue;
    }

    const metadata =
      fullVendor.metadata && typeof fullVendor.metadata === 'object' && !Array.isArray(fullVendor.metadata)
        ? fullVendor.metadata
        : {};
    const defaults = {
      accountId: firstNonEmpty(fullVendor.accountId, metadata.accountId, metadata.subscriptionId),
      displayName:
        firstNonEmpty(
          metadata.displayName,
          metadata.accountName,
          String(fullVendor.name || '').replace(/^aws\s+/i, '').trim(),
          fullVendor.accountId
        ) || 'AWS',
      region: firstNonEmpty(metadata.region, process.env.AWS_DEFAULT_REGION, DEFAULT_REGION),
      cloudWatchRegion: firstNonEmpty(
        metadata.cloudWatchRegion,
        metadata.cloudwatchRegion,
        process.env.AWS_DEFAULT_CLOUDWATCH_REGION
      ),
      efsRegions: metadata.efsRegions || metadata.efs_regions || process.env.AWS_DEFAULT_EFS_REGIONS || '',
      metricRegions:
        metadata.metricRegions ||
        metadata.cloudWatchRegions ||
        metadata.cloudwatchRegions ||
        process.env.CLOUD_METRICS_AWS_REGIONS ||
        '',
      s3Endpoint: metadata.s3Endpoint || metadata.s3_endpoint || '',
      forcePathStyle: metadata.forcePathStyle ?? metadata.force_path_style ?? false,
      requestMetricsEnabledByDefault:
        metadata.requestMetricsEnabledByDefault ??
        metadata.request_metrics_enabled_default ??
        false
    };

    const candidates = collectAwsCandidatesFromCredentials(credentials);
    if (!candidates.length) {
      continue;
    }

    let candidateIndex = 0;
    for (const candidate of candidates) {
      const parsed = normalizeAwsAccountCandidate(candidate, candidateIndex, defaults);
      candidateIndex += 1;
      if (!parsed) {
        continue;
      }
      accounts.push({
        ...parsed,
        vendorId: fullVendor.id,
        vendorName: fullVendor.name,
        source: 'vendor'
      });
    }
  }

  return dedupeAwsAccounts(accounts);
}

function parseAwsAccountsFromEnv(options = {}) {
  const includeProfileOnly = options.includeProfileOnly !== false;
  const rawJson = normalizeJsonEnvValue(process.env.AWS_ACCOUNTS_JSON || '');
  let accounts = [];

  if (rawJson) {
    try {
      const parsed = JSON.parse(rawJson);
      const rows = Array.isArray(parsed) ? parsed : parsed && typeof parsed === 'object' ? Object.values(parsed) : [];
      accounts = rows
        .map((row, index) => normalizeAwsAccountCandidate(row, index))
        .filter((row) => (includeProfileOnly ? true : Boolean(row.accessKeyId && row.secretAccessKey)));
    } catch (_error) {
      accounts = [];
    }
  }

  if (!accounts.length) {
    const fallback = normalizeAwsAccountCandidate(
      {
        accountId: process.env.AWS_DEFAULT_ACCOUNT_ID || 'aws-default',
        displayName: process.env.AWS_DEFAULT_ACCOUNT_LABEL || 'AWS',
        accessKeyId: process.env.AWS_DEFAULT_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.AWS_DEFAULT_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY || '',
        sessionToken: process.env.AWS_DEFAULT_SESSION_TOKEN || process.env.AWS_SESSION_TOKEN || '',
        profile: process.env.AWS_PROFILE || '',
        region: process.env.AWS_DEFAULT_REGION || DEFAULT_REGION,
        cloudWatchRegion: process.env.AWS_DEFAULT_CLOUDWATCH_REGION || process.env.AWS_DEFAULT_REGION || DEFAULT_CLOUDWATCH_REGION,
        metricRegions:
          process.env.CLOUD_METRICS_AWS_REGIONS ||
          process.env.AWS_DEFAULT_CLOUDWATCH_REGION ||
          process.env.AWS_DEFAULT_REGION ||
          DEFAULT_CLOUDWATCH_REGION,
        efsRegions: process.env.AWS_DEFAULT_EFS_REGIONS || process.env.AWS_DEFAULT_REGION || DEFAULT_REGION,
        s3Endpoint: process.env.AWS_DEFAULT_S3_ENDPOINT || '',
        forcePathStyle: process.env.AWS_DEFAULT_FORCE_PATH_STYLE || false,
        requestMetricsEnabledByDefault: process.env.AWS_DEFAULT_REQUEST_METRICS_ENABLED || false
      },
      0
    );
    if (fallback && (includeProfileOnly || (fallback.accessKeyId && fallback.secretAccessKey))) {
      accounts = [fallback];
    }
  }

  return dedupeAwsAccounts(
    accounts.map((account) => ({
      ...account,
      source: 'env'
    }))
  );
}

function dedupeAwsAccounts(accounts = []) {
  const deduped = new Map();
  for (const account of Array.isArray(accounts) ? accounts : []) {
    if (!account || typeof account !== 'object') {
      continue;
    }
    const key = [
      normalizeLookupKey(account.vendorId || ''),
      normalizeAwsAccountId(account.accountId || ''),
      normalizeAwsAccessKey(account.accessKeyId || ''),
      normalizeLookupKey(account.profile || '')
    ].join('|');
    if (!key || deduped.has(key)) {
      continue;
    }
    deduped.set(key, account);
  }
  return Array.from(deduped.values());
}

function getAwsAccountConfigs(options = {}) {
  const includeEnvFallback = options.includeEnvFallback !== false;
  const includeProfileOnly = options.includeProfileOnly !== false;
  const vendorAccounts = listAwsVendorAccounts({
    includeHidden: Boolean(options.includeHidden)
  }).filter((row) => (includeProfileOnly ? true : Boolean(row.accessKeyId && row.secretAccessKey)));

  if (vendorAccounts.length) {
    return vendorAccounts;
  }
  if (!includeEnvFallback) {
    return [];
  }
  return parseAwsAccountsFromEnv({
    includeProfileOnly
  });
}

function buildAwsAccountLabelLookup(accounts = []) {
  const accountIdMap = new Map();
  const accessKeyMap = new Map();
  const labels = [];

  for (const account of Array.isArray(accounts) ? accounts : []) {
    const accountId = String(account?.accountId || '').trim();
    const label = String(account?.displayName || account?.vendorName || accountId || '').trim();
    if (!label) {
      continue;
    }
    labels.push(label);
    if (accountId) {
      accountIdMap.set(normalizeLookupKey(accountId), label);
    }
    const accessKey = normalizeAwsAccessKey(account?.accessKeyId);
    if (accessKey) {
      accessKeyMap.set(accessKey, label);
    }
  }

  return {
    accountIdMap,
    accessKeyMap,
    labels: Array.from(new Set(labels))
  };
}

module.exports = {
  normalizeAwsAccessKey,
  normalizeAwsAccountId,
  normalizeJsonEnvValue,
  parseAwsAccountsFromEnv,
  listAwsVendorAccounts,
  getAwsAccountConfigs,
  buildAwsAccountLabelLookup
};
