const express = require('express');
const {
  db,
  listVendors,
  getVendorById,
  getResourceTag,
  upsertResourceTag,
  listResourceTags
} = require('../../db');
const { decryptJson } = require('../../crypto');

const app = express();
app.set('etag', false);
app.use(express.json({ limit: '2mb' }));

let awsTaggingSdk = undefined;

function normalizeProvider(value) {
  const v = String(value || '').trim().toLowerCase();
  if (['azure', 'aws', 'gcp', 'rackspace', 'private', 'wasabi', 'vsax', 'other'].includes(v)) {
    return v;
  }
  return 'other';
}

function normalizeTagKey(value) {
  return String(value || '')
    .trim()
    .slice(0, 256);
}

function normalizeTagValue(value) {
  return String(value === null || value === undefined ? '' : value)
    .trim()
    .slice(0, 2048);
}

function normalizeTagsObject(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    return {};
  }
  const tags = {};
  for (const [rawKey, rawValue] of Object.entries(input)) {
    const key = normalizeTagKey(rawKey);
    if (!key) {
      continue;
    }
    tags[key] = normalizeTagValue(rawValue);
  }
  return tags;
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

function extractWasabiAccountIdFromResourceRef(resourceRef) {
  const match = String(resourceRef || '')
    .trim()
    .match(/^wasabi:\/\/([^/]+)/i);
  return match ? String(match[1] || '').trim() : '';
}

function listWasabiBillingResourceRefsByAccount(accountId) {
  const normalizedAccountId = String(accountId || '').trim();
  if (!normalizedAccountId) {
    return [];
  }
  const rows = db
    .prepare(
      `
      SELECT raw_json
      FROM billing_snapshots
      WHERE provider = 'wasabi'
    `
    )
    .all();

  const refs = new Set();
  for (const row of rows) {
    const raw = parseJsonSafe(row?.raw_json, {}) || {};
    const lineItems = Array.isArray(raw?.lineItems) ? raw.lineItems : [];
    for (const lineItem of lineItems) {
      const lineAccountId =
        String(lineItem?.accountId || lineItem?.account_id || '').trim() ||
        extractWasabiAccountIdFromResourceRef(lineItem?.resourceRef || lineItem?.resource_ref || '');
      if (!lineAccountId || lineAccountId !== normalizedAccountId) {
        continue;
      }
      const resourceRef = String(lineItem?.resourceRef || lineItem?.resource_ref || '').trim();
      if (!resourceRef) {
        continue;
      }
      refs.add(resourceRef);
    }
  }

  const taggedRows = db
    .prepare(
      `
      SELECT resource_ref
      FROM resource_tags
      WHERE provider = 'wasabi'
        AND account_id = ?
    `
    )
    .all(normalizedAccountId);
  for (const row of taggedRows) {
    const resourceRef = String(row?.resource_ref || '').trim();
    if (resourceRef) {
      refs.add(resourceRef);
    }
  }

  return Array.from(refs);
}

function propagateWasabiWacmTagsByAccount(input = {}) {
  const normalizedAccountId = String(input?.accountId || '').trim();
  const normalizedTags = normalizeTagsObject(input?.tags || {});
  if (!normalizedAccountId || !Object.keys(normalizedTags).length) {
    return {
      accountId: normalizedAccountId || null,
      scanned: 0,
      updated: 0
    };
  }

  const resourceRefs = listWasabiBillingResourceRefsByAccount(normalizedAccountId);
  let updated = 0;
  for (const resourceRef of resourceRefs) {
    const existing = getResourceTag('wasabi', resourceRef);
    const existingTags =
      existing?.tags && typeof existing.tags === 'object' && !Array.isArray(existing.tags) ? existing.tags : {};
    const mergedTags = {
      ...existingTags,
      ...normalizedTags
    };
    const changed = !existing || Object.keys(normalizedTags).some((key) => String(existingTags?.[key] ?? '') !== normalizedTags[key]);
    if (!changed) {
      continue;
    }
    upsertResourceTag({
      provider: 'wasabi',
      resourceRef,
      vendorId: input?.vendorId || existing?.vendorId || null,
      accountId: normalizedAccountId,
      tags: mergedTags,
      source: existing?.source || input?.source || 'local',
      syncedAt: existing?.syncedAt || input?.syncedAt || null
    });
    updated += 1;
  }

  return {
    accountId: normalizedAccountId,
    scanned: resourceRefs.length,
    updated
  };
}

function getAzureResourceNamespace(resourceRef) {
  const match = String(resourceRef || '')
    .trim()
    .match(/\/providers\/([^/]+)/i);
  return match ? String(match[1] || '').trim() : '';
}

function extractNamespaceFromAzureErrorMessage(message) {
  const match = String(message || '')
    .trim()
    .match(/resource namespace '([^']+)'/i);
  return match ? String(match[1] || '').trim() : '';
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

function ensureAzureTaggableResourceRef(resourceRef) {
  const normalizedRef = String(resourceRef || '').trim();
  if (!normalizedRef.startsWith('/subscriptions/')) {
    const error = new Error('Azure resource reference must be a full ARM resource ID.');
    error.code = 'AZURE_INVALID_RESOURCE_REF';
    throw error;
  }
  const namespace = getAzureResourceNamespace(normalizedRef);
  if (!namespace || !namespace.includes('.') || namespace.toLowerCase() === 'marketplace') {
    const printable = namespace || 'unknown';
    const error = new Error(`This Azure billing line item is not a taggable ARM resource (namespace "${printable}").`);
    error.code = 'NON_TAGGABLE_AZURE_RESOURCE';
    throw error;
  }
  return normalizedRef;
}

function formatCloudTagWarning(provider, error, mode = 'update') {
  const normalizedProvider = normalizeProvider(provider);
  const rawMessage = String(error?.message || '').trim();
  const code = String(error?.code || '').trim();
  const localFallback =
    mode === 'read' ? 'Using locally saved tags only.' : 'Tags were saved locally only.';

  if (normalizedProvider === 'azure') {
    if (code === 'NON_TAGGABLE_AZURE_RESOURCE' || /InvalidResourceNamespace/i.test(rawMessage)) {
      return `This Azure billing line item is not a taggable ARM resource. ${localFallback}`;
    }
    if (code === 'AZURE_INVALID_RESOURCE_REF' || /full ARM resource ID/i.test(rawMessage)) {
      return `This Azure billing line item is missing a taggable ARM resource ID. ${localFallback}`;
    }
    if (/AuthorizationFailed|Forbidden|Unauthorized|401|403/i.test(rawMessage)) {
      return mode === 'read'
        ? 'Azure tag read is not authorized for this resource. Using locally saved tags only.'
        : 'Azure tag update is not authorized for this resource. Tags were saved locally only.';
    }
  }

  if (normalizedProvider === 'aws') {
    if (/must be an arn/i.test(rawMessage)) {
      return mode === 'read'
        ? 'This AWS billing line item does not include a resource ARN. Using locally saved tags only.'
        : 'This AWS billing line item does not include a resource ARN. Tags were saved locally only.';
    }
    if (/AccessDenied|Unauthorized|403/i.test(rawMessage)) {
      return mode === 'read'
        ? 'AWS tag read is not authorized for this resource. Using locally saved tags only.'
        : 'AWS tag update is not authorized for this resource. Tags were saved locally only.';
    }
  }

  if (rawMessage) {
    return rawMessage;
  }

  return mode === 'read'
    ? 'Cloud tag sync failed. Using locally saved tags only.'
    : 'Cloud tag update failed. Tags were saved locally only.';
}

function getAwsTaggingSdk() {
  if (awsTaggingSdk !== undefined) {
    return awsTaggingSdk;
  }
  try {
    awsTaggingSdk = require('@aws-sdk/client-resource-groups-tagging-api');
  } catch (_error) {
    awsTaggingSdk = null;
  }
  return awsTaggingSdk;
}

function normalizeLookupKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function extractAwsAccountIdFromArn(arn) {
  const match = String(arn || '')
    .trim()
    .match(/^arn:aws:[^:]*:[^:]*:(\d{12}):/i);
  return match ? String(match[1] || '').trim() : '';
}

function parseBoolean(value, fallback = true) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  return ['1', 'true', 'yes', 'y', 'on'].includes(normalized);
}

function listAwsKnownResourceRefs(accountIdCandidates = []) {
  const candidates = Array.isArray(accountIdCandidates)
    ? accountIdCandidates.map((item) => normalizeLookupKey(item)).filter(Boolean)
    : [];
  const candidateSet = new Set(candidates);
  const refs = [];
  const seen = new Set();

  try {
    const bucketRows = db
      .prepare(
        `
        SELECT account_id, bucket_name
        FROM aws_buckets
        WHERE is_active = 1 OR is_active IS NULL
      `
      )
      .all();

    for (const row of bucketRows) {
      const accountId = String(row?.account_id || '').trim();
      const accountKey = normalizeLookupKey(accountId);
      if (candidateSet.size && accountKey && !candidateSet.has(accountKey)) {
        continue;
      }
      const bucketName = String(row?.bucket_name || '').trim();
      if (!bucketName) {
        continue;
      }
      const resourceRef = `arn:aws:s3:::${bucketName}`;
      if (seen.has(resourceRef)) {
        continue;
      }
      seen.add(resourceRef);
      refs.push({
        resourceRef,
        accountId: accountId || null
      });
    }
  } catch (_error) {
    // aws_buckets may not exist in minimal deployments.
  }

  return refs;
}

function resolveVendorForResource({ provider, vendorId, accountId }) {
  const normalizedProvider = normalizeProvider(provider);
  if (vendorId) {
    const vendor = getVendorById(String(vendorId).trim());
    if (vendor && normalizeProvider(vendor.provider) === normalizedProvider) {
      return vendor;
    }
  }

  const normalizedAccountId = String(accountId || '').trim();
  const vendors = listVendors().filter((vendor) => normalizeProvider(vendor.provider) === normalizedProvider);
  if (!normalizedAccountId) {
    return vendors[0] || null;
  }

  const byAccount = vendors.find((vendor) => String(vendor.accountId || '').trim() === normalizedAccountId);
  if (byAccount) {
    return byAccount;
  }
  const bySubscription = vendors.find((vendor) => String(vendor.subscriptionId || '').trim() === normalizedAccountId);
  return bySubscription || vendors[0] || null;
}

async function fetchAzureAccessToken(credentials) {
  const tenantId = String(credentials?.tenantId || '').trim();
  const clientId = String(credentials?.clientId || '').trim();
  const clientSecret = String(credentials?.clientSecret || '').trim();
  if (!tenantId || !clientId || !clientSecret) {
    throw new Error('Azure tag operations require tenantId, clientId, and clientSecret.');
  }

  const tokenRes = await fetch(`https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`, {
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
  });

  if (!tokenRes.ok) {
    const details = await tokenRes.text().catch(() => '');
    throw new Error(`Azure token request failed (${tokenRes.status}): ${details.slice(0, 300)}`);
  }

  const tokenJson = await tokenRes.json();
  const accessToken = String(tokenJson?.access_token || '').trim();
  if (!accessToken) {
    throw new Error('Azure token response missing access_token.');
  }
  return accessToken;
}

async function fetchAzureTags(resourceRef, credentials) {
  const normalizedRef = ensureAzureTaggableResourceRef(resourceRef);
  const accessToken = await fetchAzureAccessToken(credentials);
  const endpoint = `https://management.azure.com${normalizedRef}/providers/Microsoft.Resources/tags/default?api-version=2021-04-01`;
  const response = await fetch(endpoint, {
    method: 'GET',
    headers: {
      authorization: `Bearer ${accessToken}`
    }
  });
  if (!response.ok) {
    const details = await response.text().catch(() => '');
    const parsed = parseAzureErrorPayload(details);
    if (parsed.code === 'InvalidResourceNamespace') {
      const namespace =
        extractNamespaceFromAzureErrorMessage(parsed.message) ||
        getAzureResourceNamespace(normalizedRef) ||
        'unknown';
      const error = new Error(
        `This Azure billing line item is not a taggable ARM resource (namespace "${namespace}").`
      );
      error.code = 'NON_TAGGABLE_AZURE_RESOURCE';
      throw error;
    }
    const error = new Error(
      `Azure tag read failed (${response.status})${parsed.message ? `: ${parsed.message}` : ''}`
    );
    error.code = parsed.code || 'AZURE_TAG_READ_FAILED';
    throw error;
  }
  const payload = await response.json();
  return normalizeTagsObject(payload?.properties?.tags || {});
}

async function updateAzureTags(resourceRef, tags, credentials) {
  const normalizedRef = ensureAzureTaggableResourceRef(resourceRef);
  const accessToken = await fetchAzureAccessToken(credentials);
  const normalizedTags = normalizeTagsObject(tags);
  const endpoint = `https://management.azure.com${normalizedRef}/providers/Microsoft.Resources/tags/default?api-version=2021-04-01`;
  let response = await fetch(endpoint, {
    method: 'PATCH',
    headers: {
      authorization: `Bearer ${accessToken}`,
      'content-type': 'application/json'
    },
    body: JSON.stringify({
      operation: 'Replace',
      properties: {
        tags: normalizedTags
      }
    })
  });

  // Compatibility fallback for environments that only accept PUT payloads.
  if (!response.ok) {
    const patchDetails = await response.text().catch(() => '');
    const patchParsed = parseAzureErrorPayload(patchDetails);
    const patchMessage = String(patchParsed?.message || '').trim();
    const shouldFallbackToPut =
      Number(response.status) === 400 &&
      /request content was invalid|could not find member 'operation'|invalidrequestcontent/i.test(patchMessage);

    if (shouldFallbackToPut) {
      response = await fetch(endpoint, {
        method: 'PUT',
        headers: {
          authorization: `Bearer ${accessToken}`,
          'content-type': 'application/json'
        },
        body: JSON.stringify({
          properties: {
            tags: normalizedTags
          }
        })
      });
    } else {
      const parsed = patchParsed;
      if (parsed.code === 'InvalidResourceNamespace') {
        const namespace =
          extractNamespaceFromAzureErrorMessage(parsed.message) ||
          getAzureResourceNamespace(normalizedRef) ||
          'unknown';
        const error = new Error(
          `This Azure billing line item is not a taggable ARM resource (namespace "${namespace}").`
        );
        error.code = 'NON_TAGGABLE_AZURE_RESOURCE';
        throw error;
      }
      const error = new Error(
        `Azure tag update failed (${response.status})${parsed.message ? `: ${parsed.message}` : ''}`
      );
      error.code = parsed.code || 'AZURE_TAG_UPDATE_FAILED';
      throw error;
    }
  }

  if (!response.ok) {
    const details = await response.text().catch(() => '');
    const parsed = parseAzureErrorPayload(details);
    if (parsed.code === 'InvalidResourceNamespace') {
      const namespace =
        extractNamespaceFromAzureErrorMessage(parsed.message) ||
        getAzureResourceNamespace(normalizedRef) ||
        'unknown';
      const error = new Error(
        `This Azure billing line item is not a taggable ARM resource (namespace "${namespace}").`
      );
      error.code = 'NON_TAGGABLE_AZURE_RESOURCE';
      throw error;
    }
    const error = new Error(
      `Azure tag update failed (${response.status})${parsed.message ? `: ${parsed.message}` : ''}`
    );
    error.code = parsed.code || 'AZURE_TAG_UPDATE_FAILED';
    throw error;
  }
  const payload = await response.json().catch(() => ({}));
  return normalizeTagsObject(payload?.properties?.tags || normalizedTags);
}

async function createAwsTaggingClient(credentials) {
  const sdk = getAwsTaggingSdk();
  if (!sdk) {
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

  return new sdk.ResourceGroupsTaggingAPIClient(clientConfig);
}

async function fetchAwsTags(resourceRef, credentials) {
  const sdk = getAwsTaggingSdk();
  if (!sdk) {
    throw new Error('AWS tagging SDK is not installed.');
  }
  const arn = String(resourceRef || '').trim();
  if (!arn.startsWith('arn:')) {
    throw new Error('AWS resource reference must be an ARN.');
  }

  const client = await createAwsTaggingClient(credentials);
  const response = await client.send(
    new sdk.GetResourcesCommand({
      ResourceARNList: [arn]
    })
  );
  const mapping = Array.isArray(response?.ResourceTagMappingList) ? response.ResourceTagMappingList[0] : null;
  const tags = {};
  for (const row of Array.isArray(mapping?.Tags) ? mapping.Tags : []) {
    const key = normalizeTagKey(row?.Key);
    if (!key) {
      continue;
    }
    tags[key] = normalizeTagValue(row?.Value || '');
  }
  return tags;
}

async function updateAwsTags(resourceRef, tags, credentials) {
  const sdk = getAwsTaggingSdk();
  if (!sdk) {
    throw new Error('AWS tagging SDK is not installed.');
  }
  const arn = String(resourceRef || '').trim();
  if (!arn.startsWith('arn:')) {
    throw new Error('AWS resource reference must be an ARN.');
  }

  const normalizedTags = normalizeTagsObject(tags);
  const client = await createAwsTaggingClient(credentials);
  const current = await fetchAwsTags(arn, credentials);
  const currentKeys = Object.keys(current);
  const nextKeys = Object.keys(normalizedTags);
  const keysToRemove = currentKeys.filter((key) => !nextKeys.includes(key));

  if (nextKeys.length) {
    const tagResult = await client.send(
      new sdk.TagResourcesCommand({
        ResourceARNList: [arn],
        Tags: normalizedTags
      })
    );
    const failed = tagResult?.FailedResourcesMap || {};
    if (Object.keys(failed).length) {
      const firstError = Object.values(failed)[0];
      const message = firstError?.ErrorMessage || firstError?.StatusCode || 'AWS tag update failed.';
      throw new Error(String(message));
    }
  }

  if (keysToRemove.length) {
    const untagResult = await client.send(
      new sdk.UntagResourcesCommand({
        ResourceARNList: [arn],
        TagKeys: keysToRemove
      })
    );
    const failed = untagResult?.FailedResourcesMap || {};
    if (Object.keys(failed).length) {
      const firstError = Object.values(failed)[0];
      const message = firstError?.ErrorMessage || firstError?.StatusCode || 'AWS untag failed.';
      throw new Error(String(message));
    }
  }

  return normalizedTags;
}

async function syncAwsTagsForVendor(options = {}) {
  const sdk = getAwsTaggingSdk();
  if (!sdk) {
    throw new Error('AWS tagging SDK is not installed.');
  }

  const vendor = options?.vendor || null;
  if (!vendor?.id) {
    throw new Error('AWS vendor is required for tag sync.');
  }

  const fullVendor = getVendorById(vendor.id);
  const credentials = decryptJson(fullVendor?.credentialsEncrypted);
  if (!credentials) {
    throw new Error('Vendor credentials are not configured.');
  }

  const client = await createAwsTaggingClient(credentials);
  const syncedAt = new Date().toISOString();
  const accountFilter = normalizeLookupKey(options?.accountId || vendor?.accountId || '');
  const maxPages = Math.max(1, Math.min(1000, Number(options?.maxPages || 250)));
  const includeKnownResources = parseBoolean(options?.includeKnownResources, true);
  const knownAccountCandidates = [options?.accountId, vendor?.accountId];

  const summary = {
    provider: 'aws',
    vendorId: vendor.id,
    accountId: String(options?.accountId || vendor?.accountId || '').trim() || null,
    pagesFetched: 0,
    syncedResources: 0,
    taggedResources: 0,
    untaggedResources: 0,
    knownResourcesChecked: 0,
    knownResourceErrors: 0,
    truncated: false,
    errors: []
  };

  const seenRefs = new Set();
  let paginationToken = '';

  while (summary.pagesFetched < maxPages) {
    const commandInput = {
      ResourcesPerPage: 100
    };
    if (paginationToken) {
      commandInput.PaginationToken = paginationToken;
    }

    const page = await client.send(new sdk.GetResourcesCommand(commandInput));
    summary.pagesFetched += 1;
    const mappings = Array.isArray(page?.ResourceTagMappingList) ? page.ResourceTagMappingList : [];

    for (const mapping of mappings) {
      const resourceRef = String(mapping?.ResourceARN || '').trim();
      if (!resourceRef || !resourceRef.startsWith('arn:')) {
        continue;
      }

      const arnAccountId = extractAwsAccountIdFromArn(resourceRef);
      const resolvedAccountId =
        String(arnAccountId || options?.accountId || vendor?.accountId || '').trim() || null;
      const resolvedAccountKey = normalizeLookupKey(resolvedAccountId);

      if (accountFilter && resolvedAccountKey && resolvedAccountKey !== accountFilter) {
        continue;
      }
      if (accountFilter && !resolvedAccountKey && arnAccountId && normalizeLookupKey(arnAccountId) !== accountFilter) {
        continue;
      }

      const tags = {};
      for (const tagRow of Array.isArray(mapping?.Tags) ? mapping.Tags : []) {
        const key = normalizeTagKey(tagRow?.Key);
        if (!key) {
          continue;
        }
        tags[key] = normalizeTagValue(tagRow?.Value || '');
      }

      upsertResourceTag({
        provider: 'aws',
        resourceRef,
        vendorId: vendor.id,
        accountId: resolvedAccountId,
        tags,
        source: 'cloud',
        syncedAt
      });

      seenRefs.add(resourceRef);
      summary.syncedResources += 1;
      if (Object.keys(tags).length) {
        summary.taggedResources += 1;
      } else {
        summary.untaggedResources += 1;
      }
    }

    const nextToken = String(page?.PaginationToken || '').trim();
    if (!nextToken) {
      break;
    }
    paginationToken = nextToken;
  }

  if (paginationToken && summary.pagesFetched >= maxPages) {
    summary.truncated = true;
  }

  if (includeKnownResources) {
    const knownRefs = listAwsKnownResourceRefs(knownAccountCandidates);
    for (const known of knownRefs) {
      const resourceRef = String(known?.resourceRef || '').trim();
      if (!resourceRef || seenRefs.has(resourceRef)) {
        continue;
      }

      summary.knownResourcesChecked += 1;
      try {
        const tags = await fetchAwsTags(resourceRef, credentials);
        upsertResourceTag({
          provider: 'aws',
          resourceRef,
          vendorId: vendor.id,
          accountId: String(known?.accountId || options?.accountId || vendor?.accountId || '').trim() || null,
          tags,
          source: 'cloud',
          syncedAt
        });
        summary.syncedResources += 1;
        if (Object.keys(tags).length) {
          summary.taggedResources += 1;
        } else {
          summary.untaggedResources += 1;
        }
        seenRefs.add(resourceRef);
      } catch (error) {
        summary.knownResourceErrors += 1;
        if (summary.errors.length < 25) {
          summary.errors.push({
            resourceRef,
            message: String(error?.message || 'AWS known-resource tag sync failed.')
          });
        }
      }
    }
  }

  return summary;
}

async function fetchCloudTagsForResource(resource, vendor) {
  const provider = normalizeProvider(resource?.provider);
  const resourceRef = String(resource?.resourceRef || '').trim();
  if (!resourceRef) {
    return { tags: {}, cloudSynced: false, source: 'local' };
  }
  if (!vendor) {
    throw new Error('Vendor credentials not found for this resource.');
  }

  const fullVendor = getVendorById(vendor.id);
  const credentials = decryptJson(fullVendor?.credentialsEncrypted);
  if (!credentials) {
    throw new Error('Vendor credentials are not configured.');
  }

  if (provider === 'azure') {
    const tags = await fetchAzureTags(resourceRef, credentials);
    return { tags, cloudSynced: true, source: 'cloud' };
  }
  if (provider === 'aws') {
    const tags = await fetchAwsTags(resourceRef, credentials);
    return { tags, cloudSynced: true, source: 'cloud' };
  }

  throw new Error(`Tag sync is not supported for provider ${provider}.`);
}

async function pushCloudTagsForResource(resource, vendor, tags) {
  const provider = normalizeProvider(resource?.provider);
  const resourceRef = String(resource?.resourceRef || '').trim();
  if (!resourceRef) {
    throw new Error('resourceRef is required for cloud tag updates.');
  }
  if (!vendor) {
    throw new Error('Vendor credentials not found for this resource.');
  }

  const fullVendor = getVendorById(vendor.id);
  const credentials = decryptJson(fullVendor?.credentialsEncrypted);
  if (!credentials) {
    throw new Error('Vendor credentials are not configured.');
  }

  if (provider === 'azure') {
    const updated = await updateAzureTags(resourceRef, tags, credentials);
    return { tags: updated, cloudSynced: true, source: 'cloud' };
  }
  if (provider === 'aws') {
    const updated = await updateAwsTags(resourceRef, tags, credentials);
    return { tags: updated, cloudSynced: true, source: 'cloud' };
  }

  throw new Error(`Cloud tag update is not supported for provider ${provider}.`);
}

app.get('/api/health', (_req, res) => {
  const total = listResourceTags({ limit: 1_000_000 }).length;
  res.json({
    ok: true,
    total
  });
});

app.get('/api/tags', (req, res) => {
  const provider = req.query.provider ? normalizeProvider(req.query.provider) : null;
  const resourceRef = req.query.resourceRef ? String(req.query.resourceRef).trim() : null;
  const vendorId = req.query.vendorId ? String(req.query.vendorId).trim() : null;
  const accountId = req.query.accountId ? String(req.query.accountId).trim() : null;
  const limit = req.query.limit ? Number(req.query.limit) : 1000;
  const rows = listResourceTags({
    provider,
    resourceRef,
    vendorId,
    accountId,
    limit
  });
  res.json({ rows, total: rows.length });
});

app.post('/api/tags/sync/aws', async (req, res, next) => {
  try {
    const vendorId = req.body?.vendorId ? String(req.body.vendorId).trim() : null;
    const accountId = req.body?.accountId ? String(req.body.accountId).trim() : null;
    const maxPages = req.body?.maxPages ? Number(req.body.maxPages) : null;
    const includeKnownResources =
      req.body?.includeKnownResources === undefined
        ? true
        : parseBoolean(req.body.includeKnownResources, true);

    const vendor = resolveVendorForResource({
      provider: 'aws',
      vendorId,
      accountId
    });
    if (!vendor) {
      return res.status(400).json({
        error: 'No AWS vendor found for tag sync. Configure AWS credentials first.'
      });
    }

    const summary = await syncAwsTagsForVendor({
      vendor,
      accountId,
      maxPages,
      includeKnownResources
    });

    return res.json({
      ok: true,
      vendor: {
        id: vendor.id,
        name: vendor.name,
        accountId: vendor.accountId || null
      },
      summary
    });
  } catch (error) {
    return next(error);
  }
});

app.post('/api/tags/resolve', async (req, res, next) => {
  const rows = Array.isArray(req.body?.resources) ? req.body.resources : [];
  const forceRefresh = Boolean(req.body?.forceRefresh);
  if (!rows.length) {
    return res.status(400).json({ error: 'resources[] is required.' });
  }

  try {
    const results = [];
    for (const row of rows) {
      const provider = normalizeProvider(row?.provider);
      const resourceRef = String(row?.resourceRef || '').trim();
      const vendorId = row?.vendorId ? String(row.vendorId).trim() : null;
      const accountId = row?.accountId ? String(row.accountId).trim() : null;
      if (!resourceRef || provider === 'other') {
        continue;
      }

      const local = getResourceTag(provider, resourceRef);
      if (local && !forceRefresh) {
        results.push({
          provider,
          resourceRef,
          vendorId: local.vendorId || vendorId || null,
          accountId: local.accountId || accountId || null,
          tags: local.tags || {},
          source: local.source || 'local',
          syncedAt: local.syncedAt || null,
          cloudSynced: local.source === 'cloud',
          warning: null
        });
        continue;
      }

      const vendor = resolveVendorForResource({ provider, vendorId, accountId });
      try {
        const cloud = await fetchCloudTagsForResource({ provider, resourceRef }, vendor);
        const saved = upsertResourceTag({
          provider,
          resourceRef,
          vendorId: vendor?.id || vendorId || null,
          accountId: accountId || vendor?.accountId || vendor?.subscriptionId || null,
          tags: cloud.tags,
          source: cloud.source || 'cloud',
          syncedAt: new Date().toISOString()
        });
        results.push({
          provider,
          resourceRef,
          vendorId: saved?.vendorId || vendor?.id || vendorId || null,
          accountId: saved?.accountId || accountId || vendor?.accountId || null,
          tags: saved?.tags || {},
          source: saved?.source || 'cloud',
          syncedAt: saved?.syncedAt || null,
          cloudSynced: true,
          warning: null
        });
      } catch (error) {
        const fallback = upsertResourceTag({
          provider,
          resourceRef,
          vendorId: vendor?.id || vendorId || null,
          accountId: accountId || vendor?.accountId || vendor?.subscriptionId || null,
          tags: local?.tags || {},
          source: local?.source || 'local'
        });
        results.push({
          provider,
          resourceRef,
          vendorId: fallback?.vendorId || vendor?.id || vendorId || null,
          accountId: fallback?.accountId || accountId || null,
          tags: fallback?.tags || {},
          source: fallback?.source || 'local',
          syncedAt: fallback?.syncedAt || null,
          cloudSynced: false,
          warning: formatCloudTagWarning(provider, error, 'read')
        });
      }
    }

    res.json({
      rows: results,
      total: results.length
    });
  } catch (error) {
    next(error);
  }
});

app.put('/api/tags', async (req, res, next) => {
  const provider = normalizeProvider(req.body?.provider);
  const resourceRef = String(req.body?.resourceRef || '').trim();
  const vendorId = req.body?.vendorId ? String(req.body.vendorId).trim() : null;
  const accountId = req.body?.accountId ? String(req.body.accountId).trim() : null;
  const tags = normalizeTagsObject(req.body?.tags || {});

  if (!resourceRef) {
    return res.status(400).json({ error: 'resourceRef is required.' });
  }

  try {
    const vendor = resolveVendorForResource({ provider, vendorId, accountId });
    let cloudSynced = false;
    let warning = null;
    let finalTags = tags;

    try {
      const cloud = await pushCloudTagsForResource({ provider, resourceRef }, vendor, tags);
      finalTags = normalizeTagsObject(cloud.tags || tags);
      cloudSynced = true;
    } catch (error) {
      warning = formatCloudTagWarning(provider, error, 'update');
    }

    const saved = upsertResourceTag({
      provider,
      resourceRef,
      vendorId: vendor?.id || vendorId || null,
      accountId: accountId || vendor?.accountId || vendor?.subscriptionId || null,
      tags: finalTags,
      source: cloudSynced ? 'cloud' : 'local',
      syncedAt: cloudSynced ? new Date().toISOString() : null
    });

    let propagated = null;
    if (provider === 'wasabi') {
      const cascadeAccountId =
        String(saved?.accountId || accountId || extractWasabiAccountIdFromResourceRef(resourceRef) || '').trim() || null;
      propagated = propagateWasabiWacmTagsByAccount({
        accountId: cascadeAccountId,
        vendorId: vendor?.id || vendorId || null,
        tags: finalTags,
        source: cloudSynced ? 'cloud' : 'local',
        syncedAt: cloudSynced ? new Date().toISOString() : null
      });
    }

    res.json({
      row: saved,
      cloudSynced,
      warning,
      propagated
    });
  } catch (error) {
    next(error);
  }
});

app.use((error, _req, res, _next) => {
  const statusCode = Number(error?.statusCode) || 500;
  res.status(statusCode).json({
    error: error?.message || 'Tag module request failed.'
  });
});

module.exports = {
  app
};
