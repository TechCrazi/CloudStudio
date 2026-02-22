function normalizeCloudDatabaseProvider(value, fallback = 'all') {
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  if (!normalized) {
    return String(fallback || 'all')
      .trim()
      .toLowerCase();
  }
  return normalized;
}

const CLOUD_DATABASE_PROVIDER_ORDER = Object.freeze(['azure', 'aws', 'gcp', 'rackspace', 'wasabi', 'other']);
const CLOUD_DATABASE_PROVIDER_LABELS = Object.freeze({
  azure: 'Azure',
  aws: 'AWS',
  gcp: 'GCP',
  rackspace: 'Rackspace',
  wasabi: 'Wasabi',
  other: 'Other'
});

const CLOUD_DATABASE_RESOURCE_TYPE_LABELS = Object.freeze({
  'microsoft.sql/servers': 'SQL Servers',
  'microsoft.sql/servers/databases': 'SQL Databases',
  'microsoft.sql/managedinstances': 'SQL Managed Instances',
  'microsoft.sql/managedinstances/databases': 'Managed Instance Databases',
  'microsoft.dbformysql/flexibleservers': 'MySQL Flexible Servers',
  'microsoft.dbforpostgresql/flexibleservers': 'PostgreSQL Flexible Servers',
  'aws/rds/dbinstanceidentifier': 'RDS Instances',
  'aws/rds/dbclusteridentifier': 'RDS Clusters'
});

const CLOUD_DATABASE_TYPE_PREFIXES = Object.freeze([
  'microsoft.sql/',
  'microsoft.dbformysql/',
  'microsoft.dbforpostgresql/',
  'microsoft.documentdb/databaseaccounts',
  'aws/rds/',
  'aws/docdb/',
  'aws/redshift/'
]);

function normalizeResourceType(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function normalizeMetricName(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

function toFiniteNumberOrNull(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function isCloudDatabaseResourceType(resourceType) {
  const type = normalizeResourceType(resourceType);
  if (!type) {
    return false;
  }
  return CLOUD_DATABASE_TYPE_PREFIXES.some((prefix) => type.startsWith(prefix));
}

function metricToNumericValue(metric, options = {}) {
  const preferTotal = Boolean(options.preferTotal);
  if (!metric || typeof metric !== 'object') {
    return null;
  }
  if (preferTotal) {
    const total = toFiniteNumberOrNull(metric.total);
    if (total !== null) {
      return total;
    }
  }
  const value = toFiniteNumberOrNull(metric.value);
  if (value !== null) {
    return value;
  }
  const average = toFiniteNumberOrNull(metric.average);
  if (average !== null) {
    return average;
  }
  const maximum = toFiniteNumberOrNull(metric.maximum);
  if (maximum !== null) {
    return maximum;
  }
  const minimum = toFiniteNumberOrNull(metric.minimum);
  if (minimum !== null) {
    return minimum;
  }
  const count = toFiniteNumberOrNull(metric.count);
  if (count !== null) {
    return count;
  }
  return toFiniteNumberOrNull(metric.total);
}

function findMetricByName(metrics = [], options = {}) {
  const entries = Array.isArray(metrics) ? metrics : [];
  const exact = (Array.isArray(options.exact) ? options.exact : []).map((item) => normalizeMetricName(item)).filter(Boolean);
  const contains = (Array.isArray(options.contains) ? options.contains : [])
    .map((item) => normalizeMetricName(item))
    .filter(Boolean);

  if (exact.length) {
    for (const metric of entries) {
      const key = normalizeMetricName(metric?.name || metric?.metric || '');
      if (key && exact.includes(key)) {
        return metric;
      }
    }
  }

  if (contains.length) {
    for (const metric of entries) {
      const key = normalizeMetricName(metric?.name || metric?.metric || '');
      if (!key) {
        continue;
      }
      if (contains.some((fragment) => key.includes(fragment))) {
        return metric;
      }
    }
  }

  return null;
}

function detectCloudDatabaseEngine(resourceType, resourceName) {
  const type = normalizeResourceType(resourceType);
  const name = String(resourceName || '').trim();
  if (type.startsWith('microsoft.sql/')) {
    if (type.includes('/managedinstances/')) {
      return 'Azure SQL Managed Instance';
    }
    if (type.endsWith('/managedinstances')) {
      return 'Azure SQL Managed Instance';
    }
    if (type.endsWith('/databases')) {
      return 'Azure SQL Database';
    }
    return 'Azure SQL';
  }
  if (type.startsWith('microsoft.dbformysql/')) {
    return 'Azure Database for MySQL';
  }
  if (type.startsWith('microsoft.dbforpostgresql/')) {
    return 'Azure Database for PostgreSQL';
  }
  if (type.startsWith('aws/rds/')) {
    return 'AWS RDS';
  }
  if (type.startsWith('aws/docdb/')) {
    return 'AWS DocumentDB';
  }
  if (type.startsWith('aws/redshift/')) {
    return 'AWS Redshift';
  }
  return name ? `Database (${name})` : 'Database';
}

function buildCloudDatabaseStatus(row = {}, metrics = []) {
  const metadata = row?.metadata && typeof row.metadata === 'object' ? row.metadata : {};
  const metricError = String(metadata?.error || '').trim();
  if (metricError) {
    return {
      status: 'issue',
      statusLabel: 'Issue',
      note: metricError
    };
  }

  const availabilityMetric = findMetricByName(metrics, {
    exact: ['Availability'],
    contains: ['availability']
  });
  const availabilityValue = metricToNumericValue(availabilityMetric, {});
  if (availabilityValue !== null) {
    if (availabilityValue <= 0) {
      return {
        status: 'issue',
        statusLabel: 'Issue',
        note: 'Availability metric indicates outage.'
      };
    }
    return {
      status: 'online',
      statusLabel: 'Online',
      note: 'Availability metric indicates service is online.'
    };
  }

  const fetchedAt = String(row?.fetchedAt || '').trim();
  const fetchedMs = fetchedAt ? Date.parse(fetchedAt) : NaN;
  if (Number.isFinite(fetchedMs)) {
    const ageMs = Date.now() - fetchedMs;
    if (ageMs <= 15 * 60 * 1000) {
      return {
        status: 'online',
        statusLabel: 'Online',
        note: 'Recent metrics were collected.'
      };
    }
    if (ageMs > 2 * 60 * 60 * 1000) {
      return {
        status: 'unknown',
        statusLabel: 'Unknown',
        note: 'Metrics are stale.'
      };
    }
  }

  if (Array.isArray(metrics) && metrics.length > 0) {
    return {
      status: 'online',
      statusLabel: 'Online',
      note: 'Metrics data is available.'
    };
  }

  return {
    status: 'unknown',
    statusLabel: 'Unknown',
    note: 'No database metrics available yet.'
  };
}

function buildCloudDatabaseSecurity(row = {}, metrics = []) {
  const metadata = row?.metadata && typeof row.metadata === 'object' ? row.metadata : {};
  const signals = [];

  const blockedMetric = findMetricByName(metrics, {
    contains: ['blockedbyfirewall']
  });
  const blockedValue = metricToNumericValue(blockedMetric);
  if (blockedValue !== null && blockedValue > 0) {
    signals.push({
      key: 'firewall_blocks',
      label: 'Firewall blocks',
      value: blockedValue,
      severity: 'warning'
    });
  }

  const failedConnectionsMetric = findMetricByName(metrics, {
    contains: ['failedconnections', 'connectionfailed']
  });
  const failedConnectionsValue = metricToNumericValue(failedConnectionsMetric, { preferTotal: true });
  if (failedConnectionsValue !== null && failedConnectionsValue > 0) {
    signals.push({
      key: 'failed_connections',
      label: 'Failed connections',
      value: failedConnectionsValue,
      severity: 'warning'
    });
  }

  if (typeof metadata.publiclyAccessible === 'boolean') {
    signals.push({
      key: 'publicly_accessible',
      label: `Public access ${metadata.publiclyAccessible ? 'enabled' : 'disabled'}`,
      value: metadata.publiclyAccessible,
      severity: metadata.publiclyAccessible ? 'warning' : 'healthy'
    });
  }

  if (typeof metadata.storageEncrypted === 'boolean') {
    signals.push({
      key: 'storage_encrypted',
      label: `Storage encryption ${metadata.storageEncrypted ? 'enabled' : 'disabled'}`,
      value: metadata.storageEncrypted,
      severity: metadata.storageEncrypted ? 'healthy' : 'warning'
    });
  }

  if (typeof metadata.iamDatabaseAuthenticationEnabled === 'boolean') {
    signals.push({
      key: 'iam_db_auth',
      label: `IAM DB auth ${metadata.iamDatabaseAuthenticationEnabled ? 'enabled' : 'disabled'}`,
      value: metadata.iamDatabaseAuthenticationEnabled,
      severity: metadata.iamDatabaseAuthenticationEnabled ? 'healthy' : 'warning'
    });
  }

  if (metadata.publicNetworkAccess) {
    const raw = String(metadata.publicNetworkAccess).trim();
    const enabled = raw.toLowerCase() === 'enabled';
    signals.push({
      key: 'public_network_access',
      label: `Public network ${raw}`,
      value: raw,
      severity: enabled ? 'warning' : 'healthy'
    });
  }

  if (metadata.minimalTlsVersion) {
    const value = String(metadata.minimalTlsVersion).trim();
    const strong = /^1\.2|1\.3/i.test(value);
    signals.push({
      key: 'tls_version',
      label: `TLS ${value}`,
      value,
      severity: strong ? 'healthy' : 'warning'
    });
  }

  const topSignals = signals.slice(0, 3);
  const summary = topSignals.length ? topSignals.map((signal) => signal.label).join(' | ') : 'No security signals collected.';

  return {
    signals,
    summary
  };
}

function deriveCloudDatabaseRow(rawRow = {}) {
  const provider = normalizeCloudDatabaseProvider(rawRow?.provider, 'other');
  const resourceType = normalizeResourceType(rawRow?.resourceType || rawRow?.resource_type);
  const resourceName =
    String(rawRow?.resourceName || rawRow?.resource_name || rawRow?.resourceId || rawRow?.resource_id || '')
      .trim()
      .slice(0, 512) || 'database-resource';
  const metrics = Array.isArray(rawRow?.metrics) ? rawRow.metrics : [];

  const cpuMetric = findMetricByName(metrics, {
    exact: ['CPU percentage', 'Average CPU percentage', 'CPUUtilization', 'Percentage CPU', 'App CPU percentage'],
    contains: ['cpupercentage', 'cpuutilization', 'percentagecpu', 'appcpubilled', 'appcpupercentage']
  });
  const memoryMetric = findMetricByName(metrics, {
    exact: ['App memory percentage', 'Memory percentage'],
    contains: ['memorypercentage', 'memoryutilization', 'memoryusagepercent', 'appmemorypercentage']
  });
  const storageUsedMetric = findMetricByName(metrics, {
    exact: ['Storage space used', 'Data space used', 'Storage used'],
    contains: ['storagespaceused', 'dataspaceused', 'storageused', 'dbstorageused']
  });
  const storageAllocatedMetric = findMetricByName(metrics, {
    exact: ['Storage space reserved', 'Data space allocated', 'Allocated storage'],
    contains: ['storagespacereserved', 'dataspaceallocated', 'allocatedstorage', 'storageallocated', 'maxstoragesize']
  });
  const storageUsedPercentMetric = findMetricByName(metrics, {
    contains: ['storagespaceusedpercent', 'dataspaceusedpercent', 'storageusedpercent', 'storagepercent']
  });
  const transactionsMetric = findMetricByName(metrics, {
    exact: ['Transactions', 'IO requests count', 'Batch Requests', 'XTP request count'],
    contains: ['transaction', 'transactions', 'iorequestscount', 'batchrequests', 'xact', 'requestcount']
  });

  const status = buildCloudDatabaseStatus(rawRow, metrics);
  const security = buildCloudDatabaseSecurity(rawRow, metrics);

  return {
    provider,
    providerLabel: CLOUD_DATABASE_PROVIDER_LABELS[provider] || provider.toUpperCase(),
    resourceId: rawRow?.resourceId || rawRow?.resource_id || null,
    resourceName,
    resourceType,
    resourceTypeLabel: CLOUD_DATABASE_RESOURCE_TYPE_LABELS[resourceType] || rawRow?.resourceTypeLabel || resourceType || 'Database',
    engine: detectCloudDatabaseEngine(resourceType, resourceName),
    accountId: rawRow?.accountId || rawRow?.account_id || rawRow?.subscriptionId || rawRow?.subscription_id || '-',
    location: rawRow?.location || rawRow?.resourceGroup || rawRow?.resource_group || '-',
    fetchedAt: rawRow?.fetchedAt || rawRow?.fetched_at || null,
    updatedAt: rawRow?.updatedAt || rawRow?.updated_at || rawRow?.fetchedAt || rawRow?.fetched_at || null,
    status: status.status,
    statusLabel: status.statusLabel,
    statusNote: status.note,
    cpuPercent: metricToNumericValue(cpuMetric),
    memoryPercent: metricToNumericValue(memoryMetric),
    transactions24h: metricToNumericValue(transactionsMetric, { preferTotal: true }),
    storageUsed: {
      value: metricToNumericValue(storageUsedMetric),
      unit: storageUsedMetric?.unit || null,
      metric: storageUsedMetric?.name || null
    },
    storageAllocated: {
      value: metricToNumericValue(storageAllocatedMetric),
      unit: storageAllocatedMetric?.unit || null,
      metric: storageAllocatedMetric?.name || null
    },
    storageUsedPercent: metricToNumericValue(storageUsedPercentMetric),
    securitySummary: security.summary,
    securitySignals: security.signals,
    metadata: rawRow?.metadata && typeof rawRow.metadata === 'object' ? rawRow.metadata : {},
    metrics
  };
}

function sortProviders(providers = []) {
  const orderMap = new Map(CLOUD_DATABASE_PROVIDER_ORDER.map((provider, index) => [provider, index]));
  return [...providers].sort((left, right) => {
    const leftId = normalizeCloudDatabaseProvider(left?.id, 'other');
    const rightId = normalizeCloudDatabaseProvider(right?.id, 'other');
    const leftOrder = orderMap.has(leftId) ? orderMap.get(leftId) : 999;
    const rightOrder = orderMap.has(rightId) ? orderMap.get(rightId) : 999;
    if (leftOrder !== rightOrder) {
      return leftOrder - rightOrder;
    }
    return leftId.localeCompare(rightId, undefined, { sensitivity: 'base' });
  });
}

function buildCloudDatabaseViewFromCloudMetrics(metricRows = [], options = {}) {
  const rawRows = Array.isArray(metricRows) ? metricRows : [];
  const databaseRows = rawRows
    .filter((row) => isCloudDatabaseResourceType(row?.resourceType || row?.resource_type))
    .map((row) => deriveCloudDatabaseRow(row));

  const providerMap = new Map();
  for (const row of databaseRows) {
    const provider = normalizeCloudDatabaseProvider(row.provider, 'other');
    const existing =
      providerMap.get(provider) || {
        id: provider,
        label: CLOUD_DATABASE_PROVIDER_LABELS[provider] || provider.toUpperCase(),
        resourceCount: 0,
        onlineCount: 0,
        issueCount: 0,
        unknownCount: 0,
        lastSyncAt: null
      };
    existing.resourceCount += 1;
    if (row.status === 'online') {
      existing.onlineCount += 1;
    } else if (row.status === 'issue') {
      existing.issueCount += 1;
    } else {
      existing.unknownCount += 1;
    }
    const fetchedAt = String(row.fetchedAt || '').trim();
    if (fetchedAt && (!existing.lastSyncAt || fetchedAt > existing.lastSyncAt)) {
      existing.lastSyncAt = fetchedAt;
    }
    providerMap.set(provider, existing);
  }

  const providers = sortProviders(Array.from(providerMap.values()));
  const availableProviderIds = new Set(providers.map((provider) => normalizeCloudDatabaseProvider(provider.id, 'other')));
  const requestedProvider = normalizeCloudDatabaseProvider(options.provider, providers[0]?.id || 'all');
  const provider = requestedProvider === 'all' || !requestedProvider ? 'all' : availableProviderIds.has(requestedProvider) ? requestedProvider : 'all';

  const filteredRows = provider === 'all' ? databaseRows : databaseRows.filter((row) => normalizeCloudDatabaseProvider(row.provider, 'other') === provider);
  filteredRows.sort((left, right) => {
    const statusWeight = (statusValue) => {
      if (statusValue === 'issue') return 0;
      if (statusValue === 'unknown') return 1;
      return 2;
    };
    const leftWeight = statusWeight(String(left?.status || '').toLowerCase());
    const rightWeight = statusWeight(String(right?.status || '').toLowerCase());
    if (leftWeight !== rightWeight) {
      return leftWeight - rightWeight;
    }
    return String(left?.resourceName || '').localeCompare(String(right?.resourceName || ''), undefined, {
      sensitivity: 'base',
      numeric: true
    });
  });

  const summary = {
    totalResources: filteredRows.length,
    onlineCount: filteredRows.filter((row) => row.status === 'online').length,
    issueCount: filteredRows.filter((row) => row.status === 'issue').length,
    unknownCount: filteredRows.filter((row) => row.status === 'unknown').length,
    totalTransactions24h: filteredRows.reduce((sum, row) => sum + Number(row?.transactions24h || 0), 0)
  };

  const lastSyncAt = filteredRows.reduce((latest, row) => {
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
    generatedAt: String(options.generatedAt || new Date().toISOString()),
    provider,
    providerLabel: provider === 'all' ? 'All providers' : CLOUD_DATABASE_PROVIDER_LABELS[provider] || provider.toUpperCase(),
    providers,
    totalResources: databaseRows.length,
    filteredResources: filteredRows.length,
    lastSyncAt,
    summary,
    rows: filteredRows
  };
}

function registerCloudDatabaseRoutes(app, handlers = {}) {
  if (!(app && typeof app.get === 'function' && typeof app.post === 'function')) {
    throw new Error('registerCloudDatabaseRoutes requires an express app instance.');
  }

  const buildView =
    typeof handlers.buildView === 'function'
      ? handlers.buildView
      : () => {
          throw new Error('buildView handler is required.');
        };
  const runSync =
    typeof handlers.runSync === 'function'
      ? handlers.runSync
      : async () => {
          throw new Error('runSync handler is required.');
        };
  const getDefaultProvider =
    typeof handlers.getDefaultProvider === 'function' ? handlers.getDefaultProvider : () => 'all';
  const includePlatformRoutes = handlers.includePlatformRoutes !== false;
  const includePublicRoutes = handlers.includePublicRoutes !== false;

  if (includePlatformRoutes) {
    app.get('/api/platform/cloud-database/providers', async (_req, res, next) => {
      try {
        const provider = normalizeCloudDatabaseProvider(getDefaultProvider(), 'all');
        const payload = await Promise.resolve(buildView(provider));
        res.json({
          generatedAt: payload?.generatedAt || new Date().toISOString(),
          provider: payload?.provider || provider,
          providers: Array.isArray(payload?.providers) ? payload.providers : [],
          syncStatus: payload?.syncStatus || null
        });
      } catch (error) {
        next(error);
      }
    });

    app.get('/api/platform/cloud-database', async (req, res, next) => {
      try {
        const provider = normalizeCloudDatabaseProvider(req.query?.provider || getDefaultProvider(), 'all');
        const payload = await Promise.resolve(buildView(provider));
        res.json(payload);
      } catch (error) {
        next(error);
      }
    });

    app.post('/api/platform/cloud-database/sync', async (req, res, next) => {
      try {
        const provider = normalizeCloudDatabaseProvider(req.body?.provider || req.query?.provider || getDefaultProvider(), 'all');
        const syncResult = await runSync(provider);
        const payload = await Promise.resolve(buildView(provider));
        res.json({
          ...payload,
          syncResult
        });
      } catch (error) {
        next(error);
      }
    });
  }

  if (includePublicRoutes) {
    app.get('/api/public/cloud-database', async (req, res, next) => {
      try {
        const provider = normalizeCloudDatabaseProvider(req.query?.provider || getDefaultProvider(), 'all');
        const payload = await Promise.resolve(buildView(provider));
        res.json(payload);
      } catch (error) {
        next(error);
      }
    });
  }
}

module.exports = {
  normalizeCloudDatabaseProvider,
  isCloudDatabaseResourceType,
  buildCloudDatabaseViewFromCloudMetrics,
  registerCloudDatabaseRoutes
};
