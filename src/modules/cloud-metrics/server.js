const express = require('express');

function normalizeProviderFallback(value, fallback = 'azure') {
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  if (!normalized) {
    return String(fallback || 'azure').trim().toLowerCase() || 'azure';
  }
  return normalized;
}

function normalizeResourceName(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function normalizeResourceType(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function normalizeAccountId(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function parseInteger(value, fallback, min, max) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, Math.floor(numeric)));
}

function flattenCloudMetricsTypeGroups(typeGroups = []) {
  const rows = [];
  const safeGroups = Array.isArray(typeGroups) ? typeGroups : [];
  for (const group of safeGroups) {
    const resources = Array.isArray(group?.resources) ? group.resources : [];
    const resourceType = String(group?.resourceType || '').trim() || null;
    const resourceTypeLabel = String(group?.resourceTypeLabel || resourceType || '').trim() || null;
    for (const resource of resources) {
      rows.push({
        provider: String(resource?.provider || '').trim().toLowerCase() || null,
        resourceType,
        resourceTypeLabel,
        resourceId: resource?.resourceId || null,
        resourceName: resource?.resourceName || resource?.resourceId || null,
        accountId: resource?.accountId || resource?.subscriptionId || null,
        subscriptionId: resource?.subscriptionId || null,
        resourceGroup: resource?.resourceGroup || null,
        location: resource?.location || null,
        metricCount: Number(resource?.metricCount || 0),
        fetchedAt: resource?.fetchedAt || null,
        status: resource?.metadata?.error ? 'warning' : 'healthy',
        metadata: resource?.metadata && typeof resource.metadata === 'object' ? resource.metadata : {},
        metrics: Array.isArray(resource?.metrics) ? resource.metrics : []
      });
    }
  }
  return rows;
}

function applyFlatFilters(rows = [], query = {}) {
  const providerFilter = String(query?.provider || '')
    .trim()
    .toLowerCase();
  const typeFilter = normalizeResourceType(query?.resourceType);
  const accountFilter = normalizeAccountId(query?.accountId);
  const searchFilter = normalizeResourceName(query?.search);

  return rows.filter((row) => {
    if (providerFilter && String(row?.provider || '').trim().toLowerCase() !== providerFilter) {
      return false;
    }
    if (typeFilter && normalizeResourceType(row?.resourceType) !== typeFilter) {
      return false;
    }
    if (accountFilter && normalizeAccountId(row?.accountId) !== accountFilter) {
      return false;
    }
    if (searchFilter) {
      const name = normalizeResourceName(row?.resourceName);
      const id = normalizeResourceName(row?.resourceId);
      const group = normalizeResourceName(row?.resourceGroup);
      if (!name.includes(searchFilter) && !id.includes(searchFilter) && !group.includes(searchFilter)) {
        return false;
      }
    }
    return true;
  });
}

function selectFlatFields(row = {}) {
  return {
    provider: row.provider,
    resourceType: row.resourceType,
    resourceTypeLabel: row.resourceTypeLabel,
    resourceId: row.resourceId,
    resourceName: row.resourceName,
    accountId: row.accountId,
    subscriptionId: row.subscriptionId,
    resourceGroup: row.resourceGroup,
    location: row.location,
    metricCount: row.metricCount,
    fetchedAt: row.fetchedAt,
    status: row.status,
    metrics: Array.isArray(row.metrics) ? row.metrics : []
  };
}

function registerCloudMetricsRoutes(app, handlers = {}) {
  if (!(app && typeof app.get === 'function' && typeof app.post === 'function')) {
    throw new Error('registerCloudMetricsRoutes requires an express app instance.');
  }

  const normalizeProvider =
    typeof handlers.normalizeProvider === 'function' ? handlers.normalizeProvider : normalizeProviderFallback;
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
    typeof handlers.getDefaultProvider === 'function'
      ? handlers.getDefaultProvider
      : () => normalizeProvider('azure', 'azure');
  const includePlatformRoutes = handlers.includePlatformRoutes !== false;
  const includePublicRoutes = handlers.includePublicRoutes !== false;

  if (includePlatformRoutes) {
    app.get('/api/platform/cloud-metrics/providers', async (_req, res, next) => {
      try {
        const selectedProvider = normalizeProvider(getDefaultProvider(), 'azure');
        const data = await Promise.resolve(buildView(selectedProvider));
        res.json({
          generatedAt: data.generatedAt,
          providers: Array.isArray(data.providers) ? data.providers : [],
          syncStatus: data.syncStatus || null
        });
      } catch (error) {
        next(error);
      }
    });

    app.get('/api/platform/cloud-metrics', async (req, res, next) => {
      try {
        const requestedProvider = String(req.query.provider || '').trim().toLowerCase();
        const provider = normalizeProvider(requestedProvider || getDefaultProvider(), 'azure');
        const data = await Promise.resolve(buildView(provider));
        res.json(data);
      } catch (error) {
        next(error);
      }
    });

    app.post('/api/platform/cloud-metrics/sync', async (req, res, next) => {
      try {
        const requestedProvider = String(req.body?.provider || req.query?.provider || getDefaultProvider())
          .trim()
          .toLowerCase();
        const provider = normalizeProvider(requestedProvider, 'azure');
        const result = await runSync(provider);
        const data = await Promise.resolve(buildView(provider));
        res.json({
          ...data,
          syncResult: result
        });
      } catch (error) {
        next(error);
      }
    });

    app.get('/api/platform/utilization', async (req, res, next) => {
      try {
        const requestedProvider = String(req.query.provider || '').trim().toLowerCase();
        const provider = normalizeProvider(requestedProvider || getDefaultProvider(), 'azure');
        const data = await Promise.resolve(buildView(provider));
        res.json(data);
      } catch (error) {
        next(error);
      }
    });
  }

  if (includePublicRoutes) {
    app.get('/api/public/cloud-metrics', async (req, res, next) => {
      try {
        const requestedProvider = String(req.query.provider || '').trim().toLowerCase();
        const provider = normalizeProvider(requestedProvider || getDefaultProvider(), 'azure');
        res.json(await Promise.resolve(buildView(provider)));
      } catch (error) {
        next(error);
      }
    });

    app.get('/api/public/cloud-metrics/resources', async (req, res, next) => {
      try {
        const requestedProvider = String(req.query.provider || '').trim().toLowerCase();
        const provider = normalizeProvider(requestedProvider || getDefaultProvider(), 'azure');
        const data = await Promise.resolve(buildView(provider));
        const flattened = flattenCloudMetricsTypeGroups(data?.typeGroups || []);
        const filtered = applyFlatFilters(flattened, req.query || {});
        const offset = parseInteger(req.query?.offset, 0, 0, 1_000_000);
        const limit = parseInteger(req.query?.limit, 500, 1, 10_000);
        const sliced = filtered.slice(offset, offset + limit).map(selectFlatFields);
        res.json({
          generatedAt: data?.generatedAt || new Date().toISOString(),
          provider,
          total: filtered.length,
          offset,
          limit,
          rows: sliced,
          syncStatus: data?.syncStatus || null
        });
      } catch (error) {
        next(error);
      }
    });

    app.get('/api/public/utilization', async (req, res, next) => {
      try {
        const requestedProvider = String(req.query.provider || '').trim().toLowerCase();
        const provider = normalizeProvider(requestedProvider || getDefaultProvider(), 'azure');
        res.json(await Promise.resolve(buildView(provider)));
      } catch (error) {
        next(error);
      }
    });
  }
}

module.exports = {
  registerCloudMetricsRoutes
};
