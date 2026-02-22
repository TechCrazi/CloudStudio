const state = {
  services: null,
  authUser: null,
  vendorProviders: [],
  vendors: {
    importTargetId: null
  },
  overviewBilling: {
    rangePreset: 'last_month'
  },
  billing: {
    accounts: [],
    providerTotals: [],
    grandTotals: [],
    period: null,
    rangePreset: 'last_month',
    selectedScopeId: 'all',
    resourceBreakdownRows: [],
    resourceDetailGroups: [],
    latestSnapshots: [],
    expandedResourceKeys: [],
    tagsByResourceRef: {},
    typeProviderFilter: 'all',
    typeResourceFilter: 'all',
    typeTagFilter: 'all',
    typeSort: 'amount_desc',
    accountSort: 'provider_asc',
    searchText: '',
    navExpanded: false,
    snapshotsCollapsed: true,
    trendCollapsed: true,
    backfillStatus: null,
    budgetYear: new Date().getUTCFullYear(),
    budgetRows: [],
    budgetSupportedTargets: ['account', 'org', 'product']
  },
  tags: {
    rows: [],
    searchText: ''
  },
  cloudMetrics: {
    providers: [],
    selectedProvider: 'azure',
    typeGroups: [],
    expandedTypeKeys: [],
    expandedResourceKeys: [],
    syncStatus: null
  },
  cloudDatabase: {
    providers: [],
    selectedProvider: 'all',
    rows: [],
    filteredRows: [],
    expandedRowKeys: [],
    searchText: '',
    summary: null,
    syncStatus: null,
    lastSyncAt: null
  },
  grafanaCloud: {
    vendors: [],
    selectedVendorId: 'all',
    rangePreset: 'month_to_date',
    periodStart: '',
    periodEnd: '',
    rows: [],
    summary: null,
    details: null,
    sync: null
  },
  live: {
    rows: [],
    filteredRows: [],
    availableGroups: [],
    selectedGroupName: 'all',
    pinnedGroupName: null,
    groupSearchText: '',
    searchText: '',
    navExpanded: false
  },
  storage: {
    navExpanded: false,
    selectedProvider: 'unified'
  },
  firewall: {
    hosts: [],
    services: [],
    issueRows: [],
    hostFilter: 'all',
    searchText: '',
    sync: null,
    config: null
  },
  vpn: {
    rows: [],
    hostFilter: 'all',
    searchText: '',
    sortBy: 'status'
  },
  storageEmbed: {
    initialized: false,
    stylesInjected: false,
    moduleLoaded: false,
    initPromise: null
  },
  pricingEmbed: {
    initialized: false,
    stylesInjected: false,
    moduleLoaded: false,
    initPromise: null
  },
  ipDiscovery: {
    rows: [],
    searchText: '',
    loading: false
  },
  admin: {
    navExpanded: false,
    users: [],
    viewCatalog: [],
    editingUsername: null,
    runtimeConfig: null
  }
};

const ACTIVE_VIEW_STORAGE_KEY = 'cloudstudio_active_view';
const BILLING_SCOPE_STORAGE_KEY = 'cloudstudio_billing_scope';
const BILLING_PRESET_STORAGE_KEY = 'cloudstudio_billing_preset';
const OVERVIEW_BILLING_PRESET_STORAGE_KEY = 'cloudstudio_overview_billing_preset';
const BILLING_NAV_EXPANDED_STORAGE_KEY = 'cloudstudio_billing_nav_expanded';
const STORAGE_NAV_EXPANDED_STORAGE_KEY = 'cloudstudio_storage_nav_expanded';
const STORAGE_NAV_PROVIDER_STORAGE_KEY = 'cloudstudio_storage_provider';
const BILLING_SEARCH_STORAGE_KEY = 'cloudstudio_billing_search';
const BILLING_ACCOUNT_SORT_STORAGE_KEY = 'cloudstudio_billing_account_sort';
const ADMIN_NAV_EXPANDED_STORAGE_KEY = 'cloudstudio_admin_nav_expanded';
const BILLING_SNAPSHOTS_COLLAPSED_STORAGE_KEY = 'cloudstudio_billing_snapshots_collapsed';
const BILLING_TREND_COLLAPSED_STORAGE_KEY = 'cloudstudio_billing_trend_collapsed';
const CLOUD_METRICS_PROVIDER_STORAGE_KEY = 'cloudstudio_cloud_metrics_provider';
const CLOUD_DATABASE_PROVIDER_STORAGE_KEY = 'cloudstudio_cloud_database_provider';
const CLOUD_DATABASE_SEARCH_STORAGE_KEY = 'cloudstudio_cloud_database_search';
const GRAFANA_CLOUD_PRESET_STORAGE_KEY = 'cloudstudio_grafana_cloud_preset';
const LIVE_GROUP_STORAGE_KEY = 'cloudstudio_live_group';
const LIVE_GROUP_SEARCH_STORAGE_KEY = 'cloudstudio_live_group_search';
const LIVE_SEARCH_STORAGE_KEY = 'cloudstudio_live_search';
const LIVE_NAV_EXPANDED_STORAGE_KEY = 'cloudstudio_live_nav_expanded';
const LIVE_PINNED_GROUP_STORAGE_KEY = 'cloudstudio_live_pinned_group';
const FIREWALL_SEARCH_STORAGE_KEY = 'cloudstudio_firewall_search';
const FIREWALL_HOST_STORAGE_KEY = 'cloudstudio_firewall_host';
const VPN_SEARCH_STORAGE_KEY = 'cloudstudio_vpn_search';
const VPN_HOST_STORAGE_KEY = 'cloudstudio_vpn_host';
const VPN_SORT_STORAGE_KEY = 'cloudstudio_vpn_sort';

function normalizeStorageNavUserKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function getUserScopedLocalStorageKey(baseKey, userKey = state.authUser?.username) {
  const key = String(baseKey || '').trim();
  if (!key) {
    return '';
  }
  const normalizedUserKey = normalizeStorageNavUserKey(userKey);
  if (!normalizedUserKey) {
    return key;
  }
  return `${key}.${normalizedUserKey}`;
}

function readUserScopedLocalStorage(baseKey, options = {}) {
  const fallbackToLegacy = options.fallbackToLegacy !== false;
  const scopedKey = getUserScopedLocalStorageKey(baseKey);
  try {
    const scopedValue = localStorage.getItem(scopedKey);
    if (scopedValue !== null && scopedValue !== undefined) {
      return scopedValue;
    }
    if (fallbackToLegacy && scopedKey !== baseKey) {
      return localStorage.getItem(baseKey);
    }
  } catch (_error) {
    // Ignore storage read errors.
  }
  return null;
}

function writeUserScopedLocalStorage(baseKey, value) {
  const scopedKey = getUserScopedLocalStorageKey(baseKey);
  try {
    localStorage.setItem(scopedKey, value);
  } catch (_error) {
    // Ignore storage write errors.
  }
}

const dom = {
  menuBtn: document.getElementById('menuBtn'),
  drawer: document.getElementById('drawer'),
  workspace: document.getElementById('workspace'),
  navItems: Array.from(document.querySelectorAll('.nav-item[data-view]')),
  views: Array.from(document.querySelectorAll('.view')),
  adminNavBtn: document.getElementById('adminNavBtn'),
  adminNavChevron: document.getElementById('adminNavChevron'),
  adminNavGroup: document.getElementById('adminNavGroup'),
  storageNavBtn: document.getElementById('storageNavBtn'),
  storageNavChevron: document.getElementById('storageNavChevron'),
  storageNavGroup: document.getElementById('storageNavGroup'),
  billingNavBtn: document.getElementById('billingNavBtn'),
  billingNavChevron: document.getElementById('billingNavChevron'),
  liveNavBtn: document.getElementById('liveNavBtn'),
  liveNavChevron: document.getElementById('liveNavChevron'),
  liveNavGroup: document.getElementById('liveNavGroup'),
  toast: document.getElementById('toast'),
  healthPill: document.getElementById('healthPill'),
  mainBrandName: document.getElementById('mainBrandName'),
  mainBrandInitials: document.getElementById('mainBrandInitials'),
  authUserPill: document.getElementById('authUserPill'),
  logoutBtn: document.getElementById('logoutBtn'),
  overviewBillingPanel: document.getElementById('overviewBillingPanel'),
  overviewBillingTitle: document.getElementById('overviewBillingTitle'),
  overviewBillingRangePresets: document.getElementById('overviewBillingRangePresets'),
  overviewBillingCards: document.getElementById('overviewBillingCards'),
  overviewBillingOrgHint: document.getElementById('overviewBillingOrgHint'),
  overviewBillingOrgCards: document.getElementById('overviewBillingOrgCards'),
  overviewBillingProductHint: document.getElementById('overviewBillingProductHint'),
  overviewBillingProductCards: document.getElementById('overviewBillingProductCards'),
  exportAllVendorsBtn: document.getElementById('exportAllVendorsBtn'),
  importAllVendorsBtn: document.getElementById('importAllVendorsBtn'),
  importAllVendorsInput: document.getElementById('importAllVendorsInput'),
  importSingleVendorInput: document.getElementById('importSingleVendorInput'),
  storageEmbedRoot: document.getElementById('storageEmbedRoot'),
  pricingEmbedRoot: document.getElementById('pricingEmbedRoot'),
  billingRangePresets: document.getElementById('billingRangePresets'),
  billingRangeStart: document.getElementById('billingRangeStart'),
  billingRangeEnd: document.getElementById('billingRangeEnd'),
  billingNavGroup: document.getElementById('billingNavGroup'),
  refreshBillingBtn: document.getElementById('refreshBillingBtn'),
  exportBillingBtn: document.getElementById('exportBillingBtn'),
  billingSyncStatus: document.getElementById('billingSyncStatus'),
  billingSummaryTitle: document.getElementById('billingSummaryTitle'),
  billingTotalsCards: document.getElementById('billingTotalsCards'),
  billingBudgetStatusPanel: document.getElementById('billingBudgetStatusPanel'),
  billingBudgetStatusHint: document.getElementById('billingBudgetStatusHint'),
  billingBudgetStatusCards: document.getElementById('billingBudgetStatusCards'),
  billingOrgTotalsHint: document.getElementById('billingOrgTotalsHint'),
  billingOrgTotalsCards: document.getElementById('billingOrgTotalsCards'),
  billingProductTotalsHint: document.getElementById('billingProductTotalsHint'),
  billingProductTotalsCards: document.getElementById('billingProductTotalsCards'),
  billingAccountSummaryBody: document.getElementById('billingAccountSummaryBody'),
  billingTrendPanel: document.getElementById('billingTrendPanel'),
  toggleBillingTrendBtn: document.getElementById('toggleBillingTrendBtn'),
  billingTrendWrap: document.getElementById('billingTrendWrap'),
  billingTrendChart: document.getElementById('billingTrendChart'),
  billingTrendSummary: document.getElementById('billingTrendSummary'),
  billingTrendFootnote: document.getElementById('billingTrendFootnote'),
  billingScopeHint: document.getElementById('billingScopeHint'),
  billingResourceBody: document.getElementById('billingResourceBody'),
  billingTypeProviderFilter: document.getElementById('billingTypeProviderFilter'),
  billingTypeResourceFilter: document.getElementById('billingTypeResourceFilter'),
  billingTypeTagFilter: document.getElementById('billingTypeTagFilter'),
  billingSearchInput: document.getElementById('billingSearchInput'),
  billingTypeSort: document.getElementById('billingTypeSort'),
  billingAccountSort: document.getElementById('billingAccountSort'),
  toggleBillingSnapshotsBtn: document.getElementById('toggleBillingSnapshotsBtn'),
  billingSnapshotsWrap: document.getElementById('billingSnapshotsWrap'),
  startBillingBackfillBtn: document.getElementById('startBillingBackfillBtn'),
  refreshBillingBackfillStatusBtn: document.getElementById('refreshBillingBackfillStatusBtn'),
  billingBackfillProvider: document.getElementById('billingBackfillProvider'),
  billingBackfillMonths: document.getElementById('billingBackfillMonths'),
  billingBackfillOnlyMissing: document.getElementById('billingBackfillOnlyMissing'),
  billingBackfillDelayMs: document.getElementById('billingBackfillDelayMs'),
  billingBackfillRetries: document.getElementById('billingBackfillRetries'),
  billingBackfillSummary: document.getElementById('billingBackfillSummary'),
  billingBackfillStateText: document.getElementById('billingBackfillStateText'),
  billingBackfillFailuresBody: document.getElementById('billingBackfillFailuresBody'),
  billingBudgetYear: document.getElementById('billingBudgetYear'),
  refreshBillingBudgetBtn: document.getElementById('refreshBillingBudgetBtn'),
  saveBillingBudgetBtn: document.getElementById('saveBillingBudgetBtn'),
  billingBudgetStatus: document.getElementById('billingBudgetStatus'),
  billingBudgetBody: document.getElementById('billingBudgetBody'),
  refreshCloudMetricsBtn: document.getElementById('refreshCloudMetricsBtn'),
  cloudMetricsSyncStatus: document.getElementById('cloudMetricsSyncStatus'),
  cloudMetricsProviderTabs: document.getElementById('cloudMetricsProviderTabs'),
  cloudMetricsScopeHint: document.getElementById('cloudMetricsScopeHint'),
  cloudMetricsTypeBody: document.getElementById('cloudMetricsTypeBody'),
  refreshCloudDatabaseBtn: document.getElementById('refreshCloudDatabaseBtn'),
  syncCloudDatabaseBtn: document.getElementById('syncCloudDatabaseBtn'),
  cloudDatabaseSyncStatus: document.getElementById('cloudDatabaseSyncStatus'),
  cloudDatabaseProviderTabs: document.getElementById('cloudDatabaseProviderTabs'),
  cloudDatabaseSearchInput: document.getElementById('cloudDatabaseSearchInput'),
  clearCloudDatabaseFiltersBtn: document.getElementById('clearCloudDatabaseFiltersBtn'),
  cloudDatabaseScopeHint: document.getElementById('cloudDatabaseScopeHint'),
  cloudDatabaseSummaryCards: document.getElementById('cloudDatabaseSummaryCards'),
  cloudDatabaseBody: document.getElementById('cloudDatabaseBody'),
  refreshGrafanaCloudBtn: document.getElementById('refreshGrafanaCloudBtn'),
  syncGrafanaCloudBtn: document.getElementById('syncGrafanaCloudBtn'),
  grafanaCloudSyncStatus: document.getElementById('grafanaCloudSyncStatus'),
  grafanaCloudRangePresets: document.getElementById('grafanaCloudRangePresets'),
  grafanaCloudVendorFilter: document.getElementById('grafanaCloudVendorFilter'),
  grafanaCloudRangeStart: document.getElementById('grafanaCloudRangeStart'),
  grafanaCloudRangeEnd: document.getElementById('grafanaCloudRangeEnd'),
  grafanaCloudSummaryCards: document.getElementById('grafanaCloudSummaryCards'),
  grafanaCloudScopeHint: document.getElementById('grafanaCloudScopeHint'),
  grafanaCloudDetailsHint: document.getElementById('grafanaCloudDetailsHint'),
  grafanaCloudProductCards: document.getElementById('grafanaCloudProductCards'),
  grafanaCloudMetricsBody: document.getElementById('grafanaCloudMetricsBody'),
  grafanaCloudDimensionBody: document.getElementById('grafanaCloudDimensionBody'),
  grafanaCloudDailyBody: document.getElementById('grafanaCloudDailyBody'),
  refreshLiveBtn: document.getElementById('refreshLiveBtn'),
  liveSearchInput: document.getElementById('liveSearchInput'),
  liveGroupFilterWrap: document.getElementById('liveGroupFilterWrap'),
  liveGroupSearchInput: document.getElementById('liveGroupSearchInput'),
  liveGroupFilter: document.getElementById('liveGroupFilter'),
  clearLiveFiltersBtn: document.getElementById('clearLiveFiltersBtn'),
  liveScopeHint: document.getElementById('liveScopeHint'),
  liveBody: document.getElementById('liveBody'),
  syncFirewallBtn: document.getElementById('syncFirewallBtn'),
  refreshFirewallBtn: document.getElementById('refreshFirewallBtn'),
  firewallSearchInput: document.getElementById('firewallSearchInput'),
  firewallHostFilter: document.getElementById('firewallHostFilter'),
  firewallSyncStatus: document.getElementById('firewallSyncStatus'),
  firewallCards: document.getElementById('firewallCards'),
  firewallSummaryBody: document.getElementById('firewallSummaryBody'),
  firewallIssuesBody: document.getElementById('firewallIssuesBody'),
  refreshVpnBtn: document.getElementById('refreshVpnBtn'),
  vpnSearchInput: document.getElementById('vpnSearchInput'),
  vpnHostFilter: document.getElementById('vpnHostFilter'),
  vpnSortSelect: document.getElementById('vpnSortSelect'),
  clearVpnFiltersBtn: document.getElementById('clearVpnFiltersBtn'),
  vpnScopeHint: document.getElementById('vpnScopeHint'),
  vpnBody: document.getElementById('vpnBody'),
  syncAwsTagsBtn: document.getElementById('syncAwsTagsBtn'),
  refreshTagsBtn: document.getElementById('refreshTagsBtn'),
  tagsSearchInput: document.getElementById('tagsSearchInput'),
  tagsBody: document.getElementById('tagsBody'),
  ipMapInput: document.getElementById('ipMapInput'),
  ipMapStatus: document.getElementById('ipMapStatus'),
  saveIpMapBtn: document.getElementById('saveIpMapBtn'),
  reloadIpMapBtn: document.getElementById('reloadIpMapBtn'),
  exportIpMapBtn: document.getElementById('exportIpMapBtn'),
  scanIpDiscoveryBtn: document.getElementById('scanIpDiscoveryBtn'),
  ipDiscoverySearchInput: document.getElementById('ipDiscoverySearchInput'),
  ipDiscoveryBody: document.getElementById('ipDiscoveryBody'),
  ipDiscoveryStatus: document.getElementById('ipDiscoveryStatus'),
  refreshAdminUsersBtn: document.getElementById('refreshAdminUsersBtn'),
  refreshAppConfigBtn: document.getElementById('refreshAppConfigBtn'),
  appConfigForm: document.getElementById('appConfigForm'),
  appConfigLoginBrandName: document.getElementById('appConfigLoginBrandName'),
  appConfigLoginBrandInitials: document.getElementById('appConfigLoginBrandInitials'),
  appConfigMainBrandName: document.getElementById('appConfigMainBrandName'),
  appConfigMainBrandInitials: document.getElementById('appConfigMainBrandInitials'),
  appConfigEnvOverrides: document.getElementById('appConfigEnvOverrides'),
  saveAppConfigBtn: document.getElementById('saveAppConfigBtn'),
  exportAppConfigBtn: document.getElementById('exportAppConfigBtn'),
  importAppConfigBtn: document.getElementById('importAppConfigBtn'),
  importAppConfigInput: document.getElementById('importAppConfigInput'),
  appConfigStatus: document.getElementById('appConfigStatus'),
  adminUserForm: document.getElementById('adminUserForm'),
  adminUserFormTitle: document.getElementById('adminUserFormTitle'),
  adminUserFormStatus: document.getElementById('adminUserFormStatus'),
  adminUserUsername: document.getElementById('adminUserUsername'),
  adminUserPassword: document.getElementById('adminUserPassword'),
  adminUserIsAdmin: document.getElementById('adminUserIsAdmin'),
  adminUserPermissions: document.getElementById('adminUserPermissions'),
  adminUserSubmitBtn: document.getElementById('adminUserSubmitBtn'),
  adminUserCancelBtn: document.getElementById('adminUserCancelBtn'),
  adminUsersBody: document.getElementById('adminUsersBody'),
  refreshDbBackupConfigBtn: document.getElementById('refreshDbBackupConfigBtn'),
  dbBackupForm: document.getElementById('dbBackupForm'),
  dbBackupEnabled: document.getElementById('dbBackupEnabled'),
  dbBackupBucket: document.getElementById('dbBackupBucket'),
  dbBackupPrefix: document.getElementById('dbBackupPrefix'),
  dbBackupRegion: document.getElementById('dbBackupRegion'),
  dbBackupEndpoint: document.getElementById('dbBackupEndpoint'),
  dbBackupAccessKeyId: document.getElementById('dbBackupAccessKeyId'),
  dbBackupSecretAccessKey: document.getElementById('dbBackupSecretAccessKey'),
  dbBackupSessionToken: document.getElementById('dbBackupSessionToken'),
  dbBackupIntervalMs: document.getElementById('dbBackupIntervalMs'),
  dbBackupRetentionDays: document.getElementById('dbBackupRetentionDays'),
  dbBackupLifecycleRuleId: document.getElementById('dbBackupLifecycleRuleId'),
  dbBackupTmpDir: document.getElementById('dbBackupTmpDir'),
  dbBackupForcePathStyle: document.getElementById('dbBackupForcePathStyle'),
  dbBackupRunOnStartup: document.getElementById('dbBackupRunOnStartup'),
  dbBackupApplyLifecycle: document.getElementById('dbBackupApplyLifecycle'),
  dbBackupClearAccessKeyId: document.getElementById('dbBackupClearAccessKeyId'),
  dbBackupClearSecretAccessKey: document.getElementById('dbBackupClearSecretAccessKey'),
  dbBackupClearSessionToken: document.getElementById('dbBackupClearSessionToken'),
  runDbBackupNowBtn: document.getElementById('runDbBackupNowBtn'),
  dbBackupConfigStatus: document.getElementById('dbBackupConfigStatus'),
  dbBackupRuntimeStatus: document.getElementById('dbBackupRuntimeStatus'),
  createApiKeyBtn: document.getElementById('createApiKeyBtn'),
  apiKeysBody: document.getElementById('apiKeysBody'),
  apiKeyOutput: document.getElementById('apiKeyOutput')
};

const VIEW_LABELS = {
  dashboard: 'Dashboard',
  storage: 'Storage',
  'ip-address': 'IP Address',
  pricing: 'Pricing',
  billing: 'Billing',
  tags: 'Tags',
  'cloud-metrics': 'Cloud Metrics',
  'cloud-database': 'Cloud Database',
  'grafana-cloud': 'Grafana-Cloud',
  live: 'Live View (VSAx)',
  firewall: 'Firewall',
  vpn: 'VPN',
  security: 'Security',
  vendors: 'Vendor onboarding',
  'admin-settings': 'App config',
  'admin-users': 'User access',
  'admin-api-keys': 'API keys',
  'admin-backup': 'Backup',
  apidocs: 'API Docs'
};
const PROVIDER_DISPLAY_LABELS = Object.freeze({
  azure: 'AZURE',
  aws: 'AWS',
  gcp: 'GCP',
  sendgrid: 'SENDGRID',
  'grafana-cloud': 'GRAFANA-CLOUD',
  rackspace: 'RACKSPACE',
  wasabi: 'WASABI-WACM',
  'wasabi-wacm': 'WASABI-WACM',
  'wasabi-main': 'WASABI-MAIN',
  private: 'PRIVATE',
  vsax: 'VSAX',
  other: 'OTHER'
});
const ADMIN_ONLY_VIEWS = new Set(['vendors', 'admin-settings', 'admin-users', 'admin-api-keys', 'admin-backup']);

function showToast(message, isError = false) {
  dom.toast.textContent = message;
  dom.toast.style.background = isError ? 'rgba(130, 32, 32, 0.95)' : 'rgba(17, 29, 38, 0.95)';
  dom.toast.classList.add('show');
  window.setTimeout(() => {
    dom.toast.classList.remove('show');
  }, 2600);
}

function setBillingSyncState({ running = false, message = '', error = false, success = false } = {}) {
  if (!dom.billingSyncStatus) {
    return;
  }
  dom.billingSyncStatus.textContent = message || '';
  dom.billingSyncStatus.classList.toggle('working', running);
  dom.billingSyncStatus.classList.toggle('error', !running && error);
  dom.billingSyncStatus.classList.toggle('success', !running && success);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      'content-type': 'application/json',
      ...(options.headers || {})
    }
  });

  const text = await response.text();
  let payload = null;

  try {
    payload = text ? JSON.parse(text) : null;
  } catch (_error) {
    payload = { error: text || 'Invalid JSON response.' };
  }

  if (response.status === 401) {
    window.location.assign('/login');
    throw new Error('Authentication required.');
  }

  if (!response.ok) {
    throw new Error(payload?.error || `Request failed with ${response.status}.`);
  }

  return payload;
}

function setAuthUserPill(user) {
  state.authUser = user || null;
  if (!dom.authUserPill) {
    return;
  }
  if (!user?.username) {
    dom.authUserPill.textContent = 'Signed in';
    return;
  }
  dom.authUserPill.textContent = user.isAdmin ? `${user.username} (Admin)` : user.username;
}

function applyMainBranding(branding) {
  const name = String(branding?.name || '').trim() || 'CloudStudio';
  const initials = String(branding?.initials || '').trim().toUpperCase() || 'CS';
  if (dom.mainBrandName) {
    dom.mainBrandName.textContent = name;
  }
  if (dom.mainBrandInitials) {
    dom.mainBrandInitials.textContent = initials;
    dom.mainBrandInitials.setAttribute('aria-label', `${name} initials`);
    dom.mainBrandInitials.setAttribute('title', `${name} (${initials})`);
  }
}

function normalizeViewNameForAccess(value) {
  const text = String(value || '').trim().toLowerCase();
  if (!text) {
    return '';
  }
  if (text === 'ipmap') {
    return 'ip-address';
  }
  if (text === 'utilization' || text === 'cloudmetrics') {
    return 'cloud-metrics';
  }
  if (text === 'clouddatabase') {
    return 'cloud-database';
  }
  if (text === 'grafanacloud') {
    return 'grafana-cloud';
  }
  if (text === 'billing-backfill' || text === 'billing-budget') {
    return 'billing';
  }
  if (text === 'storageunified') {
    return 'storage-unified';
  }
  if (text === 'storageazure') {
    return 'storage-azure';
  }
  if (text === 'storageaws') {
    return 'storage-aws';
  }
  if (text === 'storagegcp') {
    return 'storage-gcp';
  }
  if (text === 'storagewasabi') {
    return 'storage-wasabi';
  }
  if (text === 'storagevsax') {
    return 'storage-vsax';
  }
  if (text === 'storageother') {
    return 'storage-other';
  }
  return text;
}

function canEditIpAddress() {
  return Boolean(state.authUser?.isAdmin);
}

function applyIpAddressWriteAccess() {
  const canWrite = canEditIpAddress();
  if (dom.ipMapInput) {
    dom.ipMapInput.readOnly = !canWrite;
  }
  if (dom.saveIpMapBtn) {
    dom.saveIpMapBtn.hidden = !canWrite;
    dom.saveIpMapBtn.style.display = canWrite ? '' : 'none';
  }
  renderIpDiscoveryTable();
}

function getAllowedViewSet() {
  const authUser = state.authUser || null;
  const available = listViewNames().map((view) => normalizeViewNameForAccess(view)).filter(Boolean);
  const all = new Set(available);

  if (authUser?.isAdmin) {
    return all;
  }

  const rawAllowed = Array.isArray(authUser?.allowedViews) ? authUser.allowedViews : [];
  const normalizedAllowed = rawAllowed.map((view) => normalizeViewNameForAccess(view)).filter(Boolean);
  if (!normalizedAllowed.length) {
    return new Set();
  }
  return new Set(normalizedAllowed);
}

function hasClientViewAccess(viewName) {
  const normalized = normalizeViewNameForAccess(viewName);
  if (!normalized) {
    return false;
  }
  const allowed = getAllowedViewSet();
  if (allowed.has(normalized)) {
    return true;
  }
  if (normalized === 'storage') {
    return [
      'storage-unified',
      'storage-azure',
      'storage-aws',
      'storage-gcp',
      'storage-wasabi',
      'storage-vsax',
      'storage-other'
    ].some((providerView) => allowed.has(providerView));
  }
  return false;
}

function getFirstAllowedView() {
  for (const view of listViewNames()) {
    if (hasClientViewAccess(view)) {
      return view;
    }
  }
  return 'dashboard';
}

function applyUserViewAccess() {
  dom.navItems.forEach((item) => {
    const view = String(item.dataset.view || '').trim();
    const allowed = hasClientViewAccess(view);
    item.hidden = !allowed;
    item.style.display = allowed ? '' : 'none';
  });

  dom.views.forEach((view) => {
    const viewName = String(view.dataset.view || '').trim();
    const allowed = hasClientViewAccess(viewName);
    view.hidden = !allowed;
    view.style.display = allowed ? '' : 'none';
  });

  if (dom.billingNavBtn && dom.billingNavGroup) {
    dom.billingNavGroup.hidden = dom.billingNavBtn.hidden;
    if (dom.billingNavBtn.hidden) {
      setBillingNavExpanded(false, { persist: false });
    }
  }

  if (dom.storageNavBtn && dom.storageNavGroup) {
    dom.storageNavGroup.hidden = dom.storageNavBtn.hidden;
    if (dom.storageNavBtn.hidden) {
      setStorageNavExpanded(false, { persist: false });
    }
  }

  if (dom.liveNavBtn && dom.liveNavGroup) {
    dom.liveNavGroup.hidden = dom.liveNavBtn.hidden;
    if (dom.liveNavBtn.hidden) {
      setLiveNavExpanded(false, { persist: false });
    }
  }

  if (dom.adminNavGroup) {
    const adminItems = Array.from(dom.adminNavGroup.querySelectorAll('.nav-item[data-view]'));
    const hasVisibleAdminItem = adminItems.some((item) => !item.hidden);
    if (dom.adminNavBtn) {
      dom.adminNavBtn.hidden = !hasVisibleAdminItem;
    }
    dom.adminNavGroup.hidden = !hasVisibleAdminItem;
    if (!hasVisibleAdminItem) {
      setAdminNavExpanded(false, { persist: false });
    }
  }

  renderStorageNavGroup();
  applyIpAddressWriteAccess();
}

function getViewLabel(viewName) {
  return VIEW_LABELS[String(viewName || '').trim()] || String(viewName || '').trim();
}

function getProviderDisplayLabel(providerRaw, fallback = 'Provider') {
  const normalized = String(providerRaw || '').trim().toLowerCase();
  if (!normalized) {
    return fallback;
  }
  return PROVIDER_DISPLAY_LABELS[normalized] || normalized.toUpperCase();
}

async function loadAuthSession() {
  const payload = await api('/api/auth/me');
  applyMainBranding(payload?.branding?.main);
  if (!payload?.authenticated || !payload?.user) {
    window.location.assign('/login');
    return;
  }
  setAuthUserPill(payload.user);
  applyUserViewAccess();
}

async function logout() {
  await api('/api/auth/logout', { method: 'POST' });
  window.location.assign('/login');
}

function formatNumber(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return '-';
  }
  return numeric.toLocaleString();
}

function formatCurrency(value, currency = 'USD') {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return '-';
  }

  try {
    return new Intl.NumberFormat(undefined, {
      style: 'currency',
      currency,
      maximumFractionDigits: 2
    }).format(numeric);
  } catch (_error) {
    return `${currency} ${numeric.toFixed(2)}`;
  }
}

function formatBytes(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return '-';
  }

  if (numeric === 0) {
    return '0 B';
  }

  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const idx = Math.min(units.length - 1, Math.floor(Math.log(numeric) / Math.log(1024)));
  const scaled = numeric / 1024 ** idx;
  return `${scaled.toFixed(scaled >= 100 ? 0 : scaled >= 10 ? 1 : 2)} ${units[idx]}`;
}

function formatScaledNumber(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return '-';
  }
  const abs = Math.abs(numeric);
  const maximumFractionDigits = abs >= 100 ? 0 : abs >= 10 ? 1 : abs >= 1 ? 2 : 4;
  return numeric.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits
  });
}

function formatPercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return '-';
  }
  return `${formatScaledNumber(numeric)}%`;
}

function formatSignedPercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return '-';
  }
  const prefix = numeric > 0 ? '+' : numeric < 0 ? '-' : '';
  return `${prefix}${formatScaledNumber(Math.abs(numeric))}%`;
}

function isGrafanaDetailMetricRow(row = {}) {
  return Boolean(row?.raw && typeof row.raw === 'object' && row.raw.isDetailMetric);
}

function formatGrafanaMetricValue(value, kind = 'usage', unit = null, currency = 'USD') {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return '-';
  }
  if (String(kind || '').trim().toLowerCase() === 'cost') {
    return formatCurrency(numeric, currency);
  }
  const normalizedUnit = String(unit || '').trim().toLowerCase();
  if (['tib', 'tb', 'gb', 'mb', 'kb', 'b'].includes(normalizedUnit)) {
    return `${formatScaledNumber(numeric)} ${normalizedUnit.toUpperCase()}`;
  }
  if (['series', 'users', 'hours', 'host-hours', 'executions'].includes(normalizedUnit)) {
    return `${formatScaledNumber(numeric)} ${normalizedUnit}`;
  }
  return formatScaledNumber(numeric);
}

function detectCloudMetricKind(unit, metricName = '') {
  const unitText = String(unit || '').trim().toLowerCase();
  const metricText = String(metricName || '').trim().toLowerCase();
  if (unitText === 'percent' || metricText.includes('percent')) {
    return 'percent';
  }
  if (
    unitText === 'bytespersecond' ||
    unitText === 'bytepersecond' ||
    metricText.includes('bytes/sec') ||
    metricText.includes('byte/sec')
  ) {
    return 'bytes-per-second';
  }
  if (
    unitText === 'bitspersecond' ||
    unitText === 'bitpersecond' ||
    metricText.includes('bits/sec') ||
    metricText.includes('bit/sec')
  ) {
    return 'bits-per-second';
  }
  if (unitText === 'bytes' || unitText === 'byte') {
    return 'bytes';
  }
  if (unitText === 'countpersecond' || unitText === 'count/second') {
    return 'count-per-second';
  }
  return 'raw';
}

function resolveCloudMetricScale(metric = {}) {
  const kind = detectCloudMetricKind(metric?.unit, metric?.name);
  const values = [metric?.value, metric?.average, metric?.minimum, metric?.maximum, metric?.total]
    .map((item) => Number(item))
    .filter((item) => Number.isFinite(item));
  const reference = values.length ? Math.max(...values.map((item) => Math.abs(item))) : 0;

  if (kind === 'percent') {
    return {
      kind,
      unitLabel: '%',
      divisor: 1,
      multiplier: 1
    };
  }

  if (kind === 'bytes') {
    const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    const index = reference > 0 ? Math.min(units.length - 1, Math.floor(Math.log(reference) / Math.log(1024))) : 0;
    return {
      kind,
      unitLabel: units[index],
      divisor: 1024 ** index,
      multiplier: 1
    };
  }

  if (kind === 'bytes-per-second') {
    const units = ['bps', 'Kbps', 'Mbps', 'Gbps', 'Tbps'];
    const referenceBits = reference * 8;
    const index =
      referenceBits > 0 ? Math.min(units.length - 1, Math.floor(Math.log(referenceBits) / Math.log(1000))) : 0;
    return {
      kind,
      unitLabel: units[index],
      divisor: 1000 ** index,
      multiplier: 8
    };
  }

  if (kind === 'bits-per-second') {
    const units = ['bps', 'Kbps', 'Mbps', 'Gbps', 'Tbps'];
    const index = reference > 0 ? Math.min(units.length - 1, Math.floor(Math.log(reference) / Math.log(1000))) : 0;
    return {
      kind,
      unitLabel: units[index],
      divisor: 1000 ** index,
      multiplier: 1
    };
  }

  if (kind === 'count-per-second') {
    return {
      kind,
      unitLabel: '/s',
      divisor: 1,
      multiplier: 1
    };
  }

  return {
    kind: 'raw',
    unitLabel: String(metric?.unit || '').trim() || '-',
    divisor: 1,
    multiplier: 1
  };
}

function formatCloudMetricValue(value, scale = {}) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return '-';
  }
  const kind = String(scale?.kind || 'raw');
  if (kind === 'percent') {
    return `${formatScaledNumber(numeric)}%`;
  }
  if (kind === 'raw') {
    return formatScaledNumber(numeric);
  }
  const divisor = Number(scale?.divisor);
  const multiplier = Number(scale?.multiplier);
  const safeDivisor = Number.isFinite(divisor) && divisor > 0 ? divisor : 1;
  const safeMultiplier = Number.isFinite(multiplier) ? multiplier : 1;
  return formatScaledNumber((numeric * safeMultiplier) / safeDivisor);
}

function formatDateTime(value) {
  if (!value) {
    return '-';
  }
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) {
    return String(value);
  }
  return dt.toLocaleString();
}

function escapeHtml(value) {
  return String(value || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function toFileSlug(value, fallback = 'scope') {
  const slug = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return slug || fallback;
}

function parseDispositionFilename(disposition) {
  const text = String(disposition || '').trim();
  if (!text) {
    return '';
  }

  const utf8Match = text.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    try {
      return decodeURIComponent(utf8Match[1].trim());
    } catch (_error) {
      return utf8Match[1].trim();
    }
  }

  const basicMatch = text.match(/filename=\"?([^\";]+)\"?/i);
  if (basicMatch?.[1]) {
    return basicMatch[1].trim();
  }
  return '';
}

function normalizeResourceRefKey(value) {
  return String(value || '').trim();
}

function billingResourceGroupKey(row = {}) {
  const provider = String(row.provider || '').trim().toLowerCase();
  const resourceType = String(row.resourceType || '').trim().toLowerCase();
  const currency = String(row.currency || '').trim().toUpperCase();
  return `${provider}::${resourceType}::${currency}`;
}

function getExpandedBillingGroupSet() {
  return new Set(Array.isArray(state.billing.expandedResourceKeys) ? state.billing.expandedResourceKeys : []);
}

function setExpandedBillingGroupSet(setLike) {
  state.billing.expandedResourceKeys = Array.from(setLike || []);
}

function getBillingResourceDetailGroupMap() {
  const map = new Map();
  const groups = Array.isArray(state.billing.resourceDetailGroups) ? state.billing.resourceDetailGroups : [];
  for (const group of groups) {
    const key = billingResourceGroupKey(group);
    if (!key) {
      continue;
    }
    map.set(key, group);
  }
  return map;
}

function parseTagsPromptInput(rawInput) {
  const text = String(rawInput || '').trim();
  if (!text) {
    return {};
  }

  let parsedJson = null;
  try {
    parsedJson = JSON.parse(text);
  } catch (_error) {
    parsedJson = null;
  }
  if (parsedJson && typeof parsedJson === 'object' && !Array.isArray(parsedJson)) {
    const tags = {};
    for (const [rawKey, rawValue] of Object.entries(parsedJson)) {
      const key = String(rawKey || '').trim();
      if (!key) {
        continue;
      }
      tags[key] = String(rawValue === null || rawValue === undefined ? '' : rawValue).trim();
    }
    return tags;
  }

  const tags = {};
  const pairs = text
    .split(',')
    .map((chunk) => chunk.trim())
    .filter(Boolean);
  for (const pair of pairs) {
    const idx = pair.indexOf('=');
    if (idx <= 0) {
      continue;
    }
    const key = pair.slice(0, idx).trim();
    const value = pair.slice(idx + 1).trim();
    if (!key) {
      continue;
    }
    tags[key] = value;
  }
  return tags;
}

function formatTagsForPrompt(tags = {}) {
  const entries = Object.entries(tags || {}).filter(([key]) => String(key || '').trim());
  if (!entries.length) {
    return '';
  }
  return entries.map(([key, value]) => `${key}=${String(value || '')}`).join(', ');
}

function renderTagChips(tags = {}) {
  const entries = Object.entries(tags || {}).filter(([key]) => String(key || '').trim());
  if (!entries.length) {
    return '<span class="muted">No tags</span>';
  }
  return `<div class="tag-list">${entries
    .map(
      ([key, value]) =>
        `<span class="tag-chip"><strong>${escapeHtml(key)}</strong>=${escapeHtml(String(value || ''))}</span>`
    )
    .join('')}</div>`;
}

function parseIpAliasRows(rawText) {
  const lines = String(rawText || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const rows = [];
  const invalid = [];

  for (const line of lines) {
    const delimiterIdx = line.indexOf(',');
    if (delimiterIdx <= 0) {
      invalid.push(line);
      continue;
    }

    const ipAddress = line.slice(0, delimiterIdx).trim().toLowerCase();
    const serverName = line.slice(delimiterIdx + 1).trim();
    if (!ipAddress || !serverName) {
      invalid.push(line);
      continue;
    }

    rows.push({ ipAddress, serverName });
  }

  return { rows, invalid };
}

function setIpMapStatus(message, isError = false) {
  if (!dom.ipMapStatus) {
    return;
  }
  dom.ipMapStatus.textContent = message || '';
  dom.ipMapStatus.style.color = isError ? '#ffb8b8' : '';
}

function setIpDiscoveryStatus(message, isError = false) {
  if (!dom.ipDiscoveryStatus) {
    return;
  }
  dom.ipDiscoveryStatus.textContent = message || '';
  dom.ipDiscoveryStatus.style.color = isError ? '#ffb8b8' : '';
}

async function loadIpMap() {
  const payload = await api('/api/ip-address');
  const aliases = Array.isArray(payload.aliases) ? payload.aliases : [];
  const lines = aliases.map((row) => `${row.ip_address || row.ipAddress || ''}, ${row.server_name || row.serverName || ''}`.trim());
  const defaults = ['1.1.1.1, CloudFlare', '8.8.8.8, Google'];
  if (dom.ipMapInput) {
    dom.ipMapInput.value = (lines.length ? lines : defaults).join('\n');
  }
  setIpMapStatus(aliases.length ? `Loaded ${aliases.length} row(s).` : 'No rows found. Default sample loaded.');
}

async function saveIpMap() {
  if (!canEditIpAddress()) {
    throw new Error('IP address map is read-only for this user.');
  }
  const parsed = parseIpAliasRows(dom.ipMapInput?.value || '');
  if (!parsed.rows.length) {
    throw new Error('Enter at least one valid row in format: IP, Server name');
  }

  const payload = await api('/api/ip-address/import', {
    method: 'POST',
    body: JSON.stringify({
      rows: parsed.rows,
      replace: true
    })
  });

  const aliases = Array.isArray(payload.aliases) ? payload.aliases : [];
  const lines = aliases.map((row) => `${row.ip_address || row.ipAddress || ''}, ${row.server_name || row.serverName || ''}`.trim());
  if (dom.ipMapInput) {
    dom.ipMapInput.value = lines.join('\n');
  }

  if (parsed.invalid.length) {
    setIpMapStatus(`Saved ${aliases.length} row(s). Skipped ${parsed.invalid.length} invalid line(s).`, false);
  } else {
    setIpMapStatus(`Saved ${aliases.length} row(s).`, false);
  }
  showToast('IP address map saved.');
}

async function exportIpMapCsv() {
  const response = await fetch('/api/ip-address/export/csv');
  if (response.status === 401) {
    window.location.assign('/login');
    throw new Error('Authentication required.');
  }
  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(text || `Export failed (${response.status}).`);
  }

  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = 'ip-aliases.csv';
  link.style.display = 'none';
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => {
    URL.revokeObjectURL(url);
  }, 1200);
  setIpMapStatus('CSV exported.', false);
}

function renderIpDiscoveryTable() {
  if (!dom.ipDiscoveryBody) {
    return;
  }

  const searchText = String(state.ipDiscovery.searchText || '')
    .trim()
    .toLowerCase();
  const canWrite = canEditIpAddress();
  const rows = Array.isArray(state.ipDiscovery.rows) ? state.ipDiscovery.rows : [];
  const filteredRows = searchText
    ? rows.filter((row) => {
      const providers = Array.isArray(row?.providers) ? row.providers.join(' ') : '';
      const sources = Array.isArray(row?.sources) ? row.sources.join(' ') : '';
      const sampleRefs = Array.isArray(row?.sampleRefs) ? row.sampleRefs.join(' ') : '';
      const suggested = String(row?.suggestedServerName || '');
      const haystack = `${row?.ipAddress || ''} ${suggested} ${providers} ${sources} ${sampleRefs}`.toLowerCase();
      return haystack.includes(searchText);
    })
    : rows;

  if (!filteredRows.length) {
    const emptyLabel = rows.length
      ? 'No rows match the current search.'
      : state.ipDiscovery.loading
        ? 'Scanning source data...'
        : 'No unmapped IPs found.';
    dom.ipDiscoveryBody.innerHTML = `<tr><td colspan="5" class="muted">${escapeHtml(emptyLabel)}</td></tr>`;
    return;
  }

  dom.ipDiscoveryBody.innerHTML = filteredRows
    .map((row) => {
      const ipAddress = String(row.ipAddress || '').trim().toLowerCase();
      const suggestedServerName = String(row.suggestedServerName || '').trim();
      const providers = Array.isArray(row.providers)
        ? row.providers.map((provider) => getProviderDisplayLabel(provider, provider.toUpperCase())).join(', ')
        : '-';
      const sources = Array.isArray(row.sources) ? row.sources : [];
      const sourcePreview = sources.slice(0, 2).join(' | ');
      const sourceSuffix = sources.length > 2 ? ` +${sources.length - 2} more` : '';
      const sourceText = `${sourcePreview}${sourceSuffix}`.trim() || '-';
      return `
        <tr data-ip-address="${escapeHtml(ipAddress)}">
          <td><code>${escapeHtml(ipAddress)}</code></td>
          <td>
            <input
              class="ip-discovery-name-input"
              data-ip-address="${escapeHtml(ipAddress)}"
              type="text"
              maxlength="128"
              value="${escapeHtml(suggestedServerName)}"
              ${canWrite ? '' : 'disabled'}
            />
          </td>
          <td>${escapeHtml(providers)}</td>
          <td title="${escapeHtml(sources.join(' | '))}">${escapeHtml(sourceText)}</td>
          <td>
            ${canWrite
          ? `<button class="btn secondary ip-discovery-inherit-btn js-ip-discovery-inherit" data-ip-address="${escapeHtml(
            ipAddress
          )}" type="button">Inherit</button>`
          : '<span class="muted">Read only</span>'
        }
          </td>
        </tr>
      `;
    })
    .join('');
}

async function loadIpDiscovery() {
  state.ipDiscovery.loading = true;
  if (dom.scanIpDiscoveryBtn) {
    dom.scanIpDiscoveryBtn.disabled = true;
  }
  if (!state.ipDiscovery.rows.length) {
    renderIpDiscoveryTable();
  }
  setIpDiscoveryStatus('Scanning source data for unmapped IPs...');
  try {
    const payload = await api('/api/ip-address/discovery/unmapped');
    state.ipDiscovery.rows = Array.isArray(payload?.rows) ? payload.rows : [];
    renderIpDiscoveryTable();
    const uniqueUnmapped = Number(payload?.uniqueUnmapped || state.ipDiscovery.rows.length || 0);
    const uniqueDiscovered = Number(payload?.uniqueDiscovered || uniqueUnmapped || 0);
    const alreadyMapped = Number(payload?.alreadyMapped || 0);
    const truncated = Boolean(payload?.truncated);
    const statusParts = [
      `Unmapped: ${uniqueUnmapped.toLocaleString()}`,
      `Discovered: ${uniqueDiscovered.toLocaleString()}`,
      `Already mapped: ${alreadyMapped.toLocaleString()}`
    ];
    if (truncated) {
      statusParts.push('Result truncated');
    }
    setIpDiscoveryStatus(statusParts.join(' | '), false);
  } catch (error) {
    setIpDiscoveryStatus(error?.message || 'IP discovery scan failed.', true);
    throw error;
  } finally {
    state.ipDiscovery.loading = false;
    if (dom.scanIpDiscoveryBtn) {
      dom.scanIpDiscoveryBtn.disabled = false;
    }
    renderIpDiscoveryTable();
  }
}

async function inheritDiscoveredIp(ipAddressRaw, serverNameRaw) {
  if (!canEditIpAddress()) {
    throw new Error('IP address map is read-only for this user.');
  }
  const ipAddress = String(ipAddressRaw || '')
    .trim()
    .toLowerCase();
  const serverName = String(serverNameRaw || '').trim();
  if (!ipAddress) {
    throw new Error('IP address is required.');
  }
  if (!serverName) {
    throw new Error('System name is required before inherit.');
  }

  await api('/api/ip-address/discovery/inherit', {
    method: 'POST',
    body: JSON.stringify({
      ipAddress,
      serverName
    })
  });

  state.ipDiscovery.rows = state.ipDiscovery.rows.filter((row) => String(row?.ipAddress || '').trim().toLowerCase() !== ipAddress);
  renderIpDiscoveryTable();
  setIpDiscoveryStatus(`Inherited ${ipAddress} as "${serverName}".`, false);
  showToast(`Inherited ${ipAddress}.`);
  await loadIpMap();
}

function setDrawer(open) {
  dom.drawer.classList.toggle('drawer-open', open);
  dom.workspace.classList.toggle('drawer-open', open);
}

function listViewNames() {
  return dom.views.map((view) => view.dataset.view).filter(Boolean);
}

function readSavedView() {
  try {
    return localStorage.getItem(ACTIVE_VIEW_STORAGE_KEY);
  } catch (_error) {
    return null;
  }
}

function saveActiveView(viewName) {
  try {
    localStorage.setItem(ACTIVE_VIEW_STORAGE_KEY, viewName);
  } catch (_error) {
    // Ignore storage errors (private mode, blocked storage, etc).
  }
}

function readSavedBillingScope() {
  try {
    return localStorage.getItem(BILLING_SCOPE_STORAGE_KEY);
  } catch (_error) {
    return null;
  }
}

function saveBillingScope(scopeId) {
  try {
    localStorage.setItem(BILLING_SCOPE_STORAGE_KEY, scopeId);
  } catch (_error) {
    // Ignore storage errors (private mode, blocked storage, etc).
  }
}

function readSavedBillingPreset() {
  try {
    return localStorage.getItem(BILLING_PRESET_STORAGE_KEY);
  } catch (_error) {
    return null;
  }
}

function saveBillingPreset(preset) {
  try {
    localStorage.setItem(BILLING_PRESET_STORAGE_KEY, preset);
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedOverviewBillingPreset() {
  try {
    return localStorage.getItem(OVERVIEW_BILLING_PRESET_STORAGE_KEY);
  } catch (_error) {
    return null;
  }
}

function saveOverviewBillingPreset(preset) {
  try {
    localStorage.setItem(OVERVIEW_BILLING_PRESET_STORAGE_KEY, String(preset || '').trim().toLowerCase());
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedBillingSearchText() {
  try {
    return String(localStorage.getItem(BILLING_SEARCH_STORAGE_KEY) || '').trim();
  } catch (_error) {
    return '';
  }
}

function saveBillingSearchText(searchText) {
  try {
    localStorage.setItem(BILLING_SEARCH_STORAGE_KEY, String(searchText || ''));
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedBillingAccountSort() {
  try {
    return String(localStorage.getItem(BILLING_ACCOUNT_SORT_STORAGE_KEY) || '').trim();
  } catch (_error) {
    return '';
  }
}

function saveBillingAccountSort(sortValue) {
  try {
    localStorage.setItem(BILLING_ACCOUNT_SORT_STORAGE_KEY, String(sortValue || '').trim().toLowerCase());
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedBillingNavExpanded() {
  try {
    const value = localStorage.getItem(BILLING_NAV_EXPANDED_STORAGE_KEY);
    if (value === null) {
      return false;
    }
    return value === '1';
  } catch (_error) {
    return false;
  }
}

function saveBillingNavExpanded(expanded) {
  try {
    localStorage.setItem(BILLING_NAV_EXPANDED_STORAGE_KEY, expanded ? '1' : '0');
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedStorageNavExpanded() {
  try {
    const value = readUserScopedLocalStorage(STORAGE_NAV_EXPANDED_STORAGE_KEY, {
      fallbackToLegacy: false
    });
    if (value === null) {
      return false;
    }
    return value === '1';
  } catch (_error) {
    return false;
  }
}

function saveStorageNavExpanded(expanded) {
  try {
    writeUserScopedLocalStorage(STORAGE_NAV_EXPANDED_STORAGE_KEY, expanded ? '1' : '0');
  } catch (_error) {
    // Ignore storage errors.
  }
}

function normalizeStorageProvider(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return ['unified', 'azure', 'aws', 'gcp', 'wasabi', 'vsax', 'other'].includes(normalized) ? normalized : 'unified';
}

function readSavedStorageProvider() {
  try {
    return normalizeStorageProvider(
      readUserScopedLocalStorage(STORAGE_NAV_PROVIDER_STORAGE_KEY, {
        fallbackToLegacy: false
      })
    );
  } catch (_error) {
    return 'unified';
  }
}

function saveStorageProvider(provider) {
  const normalized = normalizeStorageProvider(provider);
  try {
    writeUserScopedLocalStorage(STORAGE_NAV_PROVIDER_STORAGE_KEY, normalized);
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedAdminNavExpanded() {
  try {
    const value = localStorage.getItem(ADMIN_NAV_EXPANDED_STORAGE_KEY);
    if (value === null) {
      return false;
    }
    return value === '1';
  } catch (_error) {
    return false;
  }
}

function saveAdminNavExpanded(expanded) {
  try {
    localStorage.setItem(ADMIN_NAV_EXPANDED_STORAGE_KEY, expanded ? '1' : '0');
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedBillingSnapshotsCollapsed() {
  try {
    const value = localStorage.getItem(BILLING_SNAPSHOTS_COLLAPSED_STORAGE_KEY);
    if (value === null) {
      return true;
    }
    return value === '1';
  } catch (_error) {
    return true;
  }
}

function saveBillingSnapshotsCollapsed(collapsed) {
  try {
    localStorage.setItem(BILLING_SNAPSHOTS_COLLAPSED_STORAGE_KEY, collapsed ? '1' : '0');
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedBillingTrendCollapsed() {
  try {
    const value = localStorage.getItem(BILLING_TREND_COLLAPSED_STORAGE_KEY);
    if (value === null) {
      return true;
    }
    return value === '1';
  } catch (_error) {
    return true;
  }
}

function saveBillingTrendCollapsed(collapsed) {
  try {
    localStorage.setItem(BILLING_TREND_COLLAPSED_STORAGE_KEY, collapsed ? '1' : '0');
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedCloudMetricsProvider() {
  try {
    return localStorage.getItem(CLOUD_METRICS_PROVIDER_STORAGE_KEY);
  } catch (_error) {
    return null;
  }
}

function saveCloudMetricsProvider(provider) {
  try {
    localStorage.setItem(CLOUD_METRICS_PROVIDER_STORAGE_KEY, String(provider || '').trim().toLowerCase());
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedCloudDatabaseProvider() {
  try {
    return localStorage.getItem(CLOUD_DATABASE_PROVIDER_STORAGE_KEY);
  } catch (_error) {
    return null;
  }
}

function saveCloudDatabaseProvider(provider) {
  try {
    localStorage.setItem(CLOUD_DATABASE_PROVIDER_STORAGE_KEY, String(provider || '').trim().toLowerCase());
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedCloudDatabaseSearchText() {
  try {
    return String(localStorage.getItem(CLOUD_DATABASE_SEARCH_STORAGE_KEY) || '').trim();
  } catch (_error) {
    return '';
  }
}

function saveCloudDatabaseSearchText(searchText) {
  try {
    localStorage.setItem(CLOUD_DATABASE_SEARCH_STORAGE_KEY, String(searchText || '').trim());
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedGrafanaCloudPreset() {
  try {
    return localStorage.getItem(GRAFANA_CLOUD_PRESET_STORAGE_KEY);
  } catch (_error) {
    return null;
  }
}

function saveGrafanaCloudPreset(preset) {
  try {
    localStorage.setItem(GRAFANA_CLOUD_PRESET_STORAGE_KEY, String(preset || '').trim().toLowerCase());
  } catch (_error) {
    // Ignore storage errors.
  }
}

function normalizeLiveGroupName(value) {
  const normalized = String(value || '').trim();
  return normalized || 'all';
}

function normalizeLivePinnedGroupName(value) {
  const normalized = String(value || '').trim();
  return normalized || null;
}

function readSavedLiveGroupName() {
  try {
    return normalizeLiveGroupName(localStorage.getItem(LIVE_GROUP_STORAGE_KEY));
  } catch (_error) {
    return 'all';
  }
}

function saveLiveGroupName(groupName) {
  try {
    localStorage.setItem(LIVE_GROUP_STORAGE_KEY, normalizeLiveGroupName(groupName));
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedLivePinnedGroupName() {
  try {
    return normalizeLivePinnedGroupName(localStorage.getItem(LIVE_PINNED_GROUP_STORAGE_KEY));
  } catch (_error) {
    return null;
  }
}

function saveLivePinnedGroupName(groupName) {
  try {
    const normalized = normalizeLivePinnedGroupName(groupName);
    if (normalized) {
      localStorage.setItem(LIVE_PINNED_GROUP_STORAGE_KEY, normalized);
    } else {
      localStorage.removeItem(LIVE_PINNED_GROUP_STORAGE_KEY);
    }
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedLiveSearchText() {
  try {
    return String(localStorage.getItem(LIVE_SEARCH_STORAGE_KEY) || '').trim();
  } catch (_error) {
    return '';
  }
}

function readSavedLiveGroupSearchText() {
  try {
    return String(localStorage.getItem(LIVE_GROUP_SEARCH_STORAGE_KEY) || '').trim();
  } catch (_error) {
    return '';
  }
}

function saveLiveSearchText(searchText) {
  try {
    localStorage.setItem(LIVE_SEARCH_STORAGE_KEY, String(searchText || ''));
  } catch (_error) {
    // Ignore storage errors.
  }
}

function saveLiveGroupSearchText(searchText) {
  try {
    localStorage.setItem(LIVE_GROUP_SEARCH_STORAGE_KEY, String(searchText || ''));
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedLiveNavExpanded() {
  try {
    const value = localStorage.getItem(LIVE_NAV_EXPANDED_STORAGE_KEY);
    if (value === null) {
      return false;
    }
    return value === '1';
  } catch (_error) {
    return false;
  }
}

function saveLiveNavExpanded(expanded) {
  try {
    localStorage.setItem(LIVE_NAV_EXPANDED_STORAGE_KEY, expanded ? '1' : '0');
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedFirewallSearchText() {
  try {
    return String(localStorage.getItem(FIREWALL_SEARCH_STORAGE_KEY) || '').trim();
  } catch (_error) {
    return '';
  }
}

function saveFirewallSearchText(value) {
  try {
    localStorage.setItem(FIREWALL_SEARCH_STORAGE_KEY, String(value || ''));
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedFirewallHostFilter() {
  try {
    return String(localStorage.getItem(FIREWALL_HOST_STORAGE_KEY) || 'all').trim() || 'all';
  } catch (_error) {
    return 'all';
  }
}

function saveFirewallHostFilter(value) {
  try {
    localStorage.setItem(FIREWALL_HOST_STORAGE_KEY, String(value || 'all').trim() || 'all');
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedVpnSearchText() {
  try {
    return String(localStorage.getItem(VPN_SEARCH_STORAGE_KEY) || '').trim();
  } catch (_error) {
    return '';
  }
}

function saveVpnSearchText(value) {
  try {
    localStorage.setItem(VPN_SEARCH_STORAGE_KEY, String(value || ''));
  } catch (_error) {
    // Ignore storage errors.
  }
}

function readSavedVpnHostFilter() {
  try {
    return String(localStorage.getItem(VPN_HOST_STORAGE_KEY) || 'all').trim() || 'all';
  } catch (_error) {
    return 'all';
  }
}

function saveVpnHostFilter(value) {
  try {
    localStorage.setItem(VPN_HOST_STORAGE_KEY, String(value || 'all').trim() || 'all');
  } catch (_error) {
    // Ignore storage errors.
  }
}

function normalizeVpnSortValue(value) {
  const normalized = String(value || '').trim().toLowerCase();
  if (['status', 'vpn_number', 'gateway', 'host'].includes(normalized)) {
    return normalized;
  }
  return 'status';
}

function readSavedVpnSort() {
  try {
    return normalizeVpnSortValue(localStorage.getItem(VPN_SORT_STORAGE_KEY));
  } catch (_error) {
    return 'status';
  }
}

function saveVpnSort(value) {
  try {
    localStorage.setItem(VPN_SORT_STORAGE_KEY, normalizeVpnSortValue(value));
  } catch (_error) {
    // Ignore storage errors.
  }
}

function getLastMonthRange(referenceDate = new Date()) {
  const now = referenceDate;
  const monthStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  const lastMonthStart = new Date(Date.UTC(monthStart.getUTCFullYear(), monthStart.getUTCMonth() - 1, 1));
  const lastMonthEnd = new Date(Date.UTC(monthStart.getUTCFullYear(), monthStart.getUTCMonth(), 0));
  return {
    periodStart: lastMonthStart.toISOString().slice(0, 10),
    periodEnd: lastMonthEnd.toISOString().slice(0, 10)
  };
}

function getMonthToDateRange(referenceDate = new Date()) {
  const now = referenceDate;
  const monthStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  const today = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  return {
    periodStart: monthStart.toISOString().slice(0, 10),
    periodEnd: today.toISOString().slice(0, 10)
  };
}

function getFullMonthRange(monthCount, referenceDate = new Date()) {
  const safeMonths = Math.max(1, Number(monthCount || 1));
  const currentMonthStart = new Date(Date.UTC(referenceDate.getUTCFullYear(), referenceDate.getUTCMonth(), 1));
  const rangeStart = new Date(Date.UTC(currentMonthStart.getUTCFullYear(), currentMonthStart.getUTCMonth() - safeMonths, 1));
  const rangeEnd = new Date(Date.UTC(currentMonthStart.getUTCFullYear(), currentMonthStart.getUTCMonth(), 0));
  return {
    periodStart: rangeStart.toISOString().slice(0, 10),
    periodEnd: rangeEnd.toISOString().slice(0, 10)
  };
}

function getBillingRangeForPreset(preset, referenceDate = new Date()) {
  const normalized = String(preset || '').trim().toLowerCase();
  switch (normalized) {
    case 'month_to_date':
      return getMonthToDateRange(referenceDate);
    case '3_months':
      return getFullMonthRange(3, referenceDate);
    case '6_months':
      return getFullMonthRange(6, referenceDate);
    case '1_year':
      return getFullMonthRange(12, referenceDate);
    case 'last_month':
    default:
      return getLastMonthRange(referenceDate);
  }
}

function resolveRangePresetFromValues(periodStart, periodEnd) {
  const start = String(periodStart || '').trim();
  const end = String(periodEnd || '').trim();
  if (!start || !end) {
    return 'custom';
  }
  const presets = ['month_to_date', 'last_month', '3_months', '6_months', '1_year'];
  for (const preset of presets) {
    const range = getBillingRangeForPreset(preset);
    if (range.periodStart === start && range.periodEnd === end) {
      return preset;
    }
  }
  return 'custom';
}

function getDateOnlyDiffDays(startDateText, endDateText) {
  const start = new Date(`${String(startDateText || '').trim()}T00:00:00.000Z`);
  const end = new Date(`${String(endDateText || '').trim()}T00:00:00.000Z`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    return 0;
  }
  return Math.max(0, Math.round((end.getTime() - start.getTime()) / (24 * 60 * 60 * 1000))) + 1;
}

function ensureBillingRangeDefaults() {
  if (!dom.billingRangeStart || !dom.billingRangeEnd) {
    return;
  }
  const startValue = String(dom.billingRangeStart.value || '').trim();
  const endValue = String(dom.billingRangeEnd.value || '').trim();
  if (startValue && endValue) {
    state.billing.rangePreset = resolveRangePresetFromValues(startValue, endValue);
    renderBillingRangePresetButtons();
    return;
  }
  const savedPreset = String(readSavedBillingPreset() || '').trim().toLowerCase();
  const preset = ['month_to_date', 'last_month', '3_months', '6_months', '1_year'].includes(savedPreset)
    ? savedPreset
    : 'last_month';
  const range = getBillingRangeForPreset(preset);
  dom.billingRangeStart.value = range.periodStart;
  dom.billingRangeEnd.value = range.periodEnd;
  state.billing.rangePreset = preset;
  renderBillingRangePresetButtons();
}

function normalizeBillingScopeId(scopeId) {
  const value = String(scopeId || '').trim();
  if (!value || value === 'all') {
    return 'all';
  }
  if (value.startsWith('vendor:') || value.startsWith('provider:')) {
    return value;
  }
  return 'all';
}

function resolveViewName(candidate) {
  if (candidate === 'ipmap') {
    candidate = 'ip-address';
  }
  if (candidate === 'utilization' || candidate === 'cloudmetrics') {
    candidate = 'cloud-metrics';
  }
  if (candidate === 'clouddatabase') {
    candidate = 'cloud-database';
  }
  if (candidate === 'grafanacloud') {
    candidate = 'grafana-cloud';
  }
  if (candidate === 'admin') {
    candidate = 'vendors';
  }
  const available = new Set(listViewNames());
  if (available.has(candidate)) {
    return candidate;
  }
  return 'dashboard';
}

function isBillingRelatedView(viewName) {
  const value = String(viewName || '').trim();
  return value === 'billing' || value === 'billing-backfill' || value === 'billing-budget';
}

function isStorageRelatedView(viewName) {
  const value = String(viewName || '').trim();
  return value === 'storage';
}

function isLiveRelatedView(viewName) {
  const value = String(viewName || '').trim();
  return value === 'live';
}

function isAdminRelatedView(viewName) {
  const value = String(viewName || '').trim();
  return (
    value === 'vendors' ||
    value === 'admin-settings' ||
    value === 'admin-users' ||
    value === 'admin-api-keys' ||
    value === 'admin-backup'
  );
}

function getActiveViewName() {
  const active = dom.views.find((view) => view.classList.contains('active'));
  return String(active?.dataset?.view || 'dashboard').trim() || 'dashboard';
}

function setBillingNavExpanded(expanded, options = {}) {
  const canShow = !(dom.billingNavBtn && dom.billingNavBtn.hidden);
  const next = canShow ? Boolean(expanded) : false;
  state.billing.navExpanded = next;
  if (dom.billingNavGroup) {
    dom.billingNavGroup.classList.toggle('collapsed', !next);
  }
  if (dom.billingNavBtn) {
    dom.billingNavBtn.classList.toggle('nav-open', next);
  }
  if (dom.billingNavChevron) {
    dom.billingNavChevron.textContent = next ? '▾' : '▸';
  }
  if (options.persist !== false) {
    saveBillingNavExpanded(next);
  }
}

function setStorageNavExpanded(expanded, options = {}) {
  const canShow = !(dom.storageNavBtn && dom.storageNavBtn.hidden);
  const next = canShow ? Boolean(expanded) : false;
  state.storage.navExpanded = next;
  if (dom.storageNavGroup) {
    dom.storageNavGroup.classList.toggle('collapsed', !next);
  }
  if (dom.storageNavBtn) {
    dom.storageNavBtn.classList.toggle('nav-open', next);
  }
  if (dom.storageNavChevron) {
    dom.storageNavChevron.textContent = next ? '▾' : '▸';
  }
  if (options.persist !== false) {
    saveStorageNavExpanded(next);
  }
}

function setLiveNavExpanded(expanded, options = {}) {
  const canShow = !(dom.liveNavBtn && dom.liveNavBtn.hidden);
  const next = canShow ? Boolean(expanded) : false;
  state.live.navExpanded = next;
  if (dom.liveNavGroup) {
    dom.liveNavGroup.classList.toggle('collapsed', !next);
  }
  if (dom.liveNavBtn) {
    dom.liveNavBtn.classList.toggle('nav-open', next);
  }
  if (dom.liveNavChevron) {
    dom.liveNavChevron.textContent = next ? '▾' : '▸';
  }
  if (options.persist !== false) {
    saveLiveNavExpanded(next);
  }
}

function setAdminNavExpanded(expanded, options = {}) {
  const canShow = !(dom.adminNavBtn && dom.adminNavBtn.hidden);
  const next = canShow ? Boolean(expanded) : false;
  state.admin.navExpanded = next;
  if (dom.adminNavGroup) {
    dom.adminNavGroup.classList.toggle('collapsed', !next);
  }
  if (dom.adminNavBtn) {
    dom.adminNavBtn.classList.toggle('open', next);
  }
  if (dom.adminNavChevron) {
    dom.adminNavChevron.textContent = next ? '▾' : '▸';
  }
  if (options.persist !== false) {
    saveAdminNavExpanded(next);
  }
}

function setBillingSnapshotsCollapsed(collapsed, options = {}) {
  const next = Boolean(collapsed);
  state.billing.snapshotsCollapsed = next;
  if (dom.billingSnapshotsWrap) {
    dom.billingSnapshotsWrap.classList.toggle('collapsed', next);
  }
  if (dom.toggleBillingSnapshotsBtn) {
    dom.toggleBillingSnapshotsBtn.textContent = next ? 'Show snapshots' : 'Hide snapshots';
    dom.toggleBillingSnapshotsBtn.setAttribute('aria-expanded', next ? 'false' : 'true');
  }
  if (options.persist !== false) {
    saveBillingSnapshotsCollapsed(next);
  }
}

function setBillingTrendCollapsed(collapsed, options = {}) {
  const next = Boolean(collapsed);
  state.billing.trendCollapsed = next;
  if (dom.billingTrendWrap) {
    dom.billingTrendWrap.classList.toggle('collapsed', next);
  }
  if (dom.toggleBillingTrendBtn) {
    dom.toggleBillingTrendBtn.textContent = next ? 'Show trend' : 'Hide trend';
    dom.toggleBillingTrendBtn.setAttribute('aria-expanded', next ? 'false' : 'true');
  }
  if (options.persist !== false) {
    saveBillingTrendCollapsed(next);
  }
}

function normalizeOverviewBillingPreset(preset) {
  const normalized = String(preset || '').trim().toLowerCase();
  return normalized === 'month_to_date' ? 'month_to_date' : 'last_month';
}

function renderOverviewBillingPresetButtons() {
  if (!dom.overviewBillingRangePresets) {
    return;
  }
  const activePreset = normalizeOverviewBillingPreset(state.overviewBilling.rangePreset);
  dom.overviewBillingRangePresets.querySelectorAll('[data-overview-billing-preset]').forEach((button) => {
    if (!(button instanceof HTMLElement)) {
      return;
    }
    const preset = normalizeOverviewBillingPreset(button.dataset.overviewBillingPreset || '');
    button.classList.toggle('active', preset === activePreset);
  });
}

function applyOverviewBillingPreset(preset, options = {}) {
  const normalized = normalizeOverviewBillingPreset(preset);
  const changed = normalized !== normalizeOverviewBillingPreset(state.overviewBilling.rangePreset);
  state.overviewBilling.rangePreset = normalized;
  renderOverviewBillingPresetButtons();
  if (options.persist !== false) {
    saveOverviewBillingPreset(normalized);
  }
  return changed;
}

function renderBillingRangePresetButtons() {
  if (!dom.billingRangePresets) {
    return;
  }
  const activePreset = String(state.billing.rangePreset || 'custom').trim().toLowerCase();
  dom.billingRangePresets.querySelectorAll('[data-billing-preset]').forEach((button) => {
    if (!(button instanceof HTMLElement)) {
      return;
    }
    const preset = String(button.dataset.billingPreset || '').trim().toLowerCase();
    button.classList.toggle('active', preset === activePreset);
  });
}

async function fetchText(path) {
  const response = await fetch(path, {
    headers: {
      accept: 'text/plain,text/html,text/css,*/*'
    }
  });

  if (response.status === 401) {
    window.location.assign('/login');
    throw new Error('Authentication required.');
  }

  if (!response.ok) {
    throw new Error(`Failed to load ${path} (${response.status}).`);
  }

  return response.text();
}

function extractBodyMarkup(htmlText) {
  const parser = new DOMParser();
  const doc = parser.parseFromString(String(htmlText || ''), 'text/html');
  doc.querySelectorAll('script').forEach((node) => node.remove());
  return (doc.body?.innerHTML || '').trim();
}

function scopeEmbeddedCss(cssText, scopeSelector) {
  const source = String(cssText || '');
  return source.replace(/(^|})\s*([^@{}][^{}]*)\{/g, (match, boundary, selectorGroup) => {
    const scopedSelectors = selectorGroup
      .split(',')
      .map((chunk) => {
        let selector = chunk.trim();
        if (!selector) {
          return selector;
        }

        if (/^(from|to|\d+%)$/i.test(selector)) {
          return selector;
        }

        selector = selector.replace(/:root/g, scopeSelector);
        selector = selector.replace(/\bhtml(?=[\s.#:[>+~]|$)/g, scopeSelector);
        selector = selector.replace(/\bbody(?=[\s.#:[>+~]|$)/g, scopeSelector);

        if (selector.includes(scopeSelector)) {
          return selector;
        }

        return `${scopeSelector} ${selector}`;
      })
      .join(', ');

    return `${boundary} ${scopedSelectors}{`;
  });
}

function ensureStorageScopedStyles(rawCss) {
  if (state.storageEmbed.stylesInjected) {
    return;
  }

  const styleId = 'cloudstudio-storage-embedded-style';
  let styleNode = document.getElementById(styleId);
  if (!styleNode) {
    styleNode = document.createElement('style');
    styleNode.id = styleId;
    styleNode.textContent = scopeEmbeddedCss(rawCss, '.storage-embed');
    document.head.appendChild(styleNode);
  }

  state.storageEmbed.stylesInjected = true;
}

function ensurePricingScopedStyles(rawCss) {
  if (state.pricingEmbed.stylesInjected) {
    return;
  }

  const styleId = 'cloudstudio-pricing-embedded-style';
  let styleNode = document.getElementById(styleId);
  if (!styleNode) {
    styleNode = document.createElement('style');
    styleNode.id = styleId;
    styleNode.textContent = scopeEmbeddedCss(rawCss, '.pricing-embed');
    document.head.appendChild(styleNode);
  }

  state.pricingEmbed.stylesInjected = true;
}

async function ensureStorageWorkspace() {
  if (state.storageEmbed.initialized) {
    return;
  }

  if (state.storageEmbed.initPromise) {
    return state.storageEmbed.initPromise;
  }

  const root = dom.storageEmbedRoot;
  if (!root) {
    return;
  }

  state.storageEmbed.initPromise = (async () => {
    root.classList.add('loading-state');
    root.innerHTML = '<p class="muted">Loading storage workspace...</p>';

    const [storageIndexHtml, storageCss] = await Promise.all([fetchText('/apps/storage/index.html'), fetchText('/apps/storage/styles.css')]);
    ensureStorageScopedStyles(storageCss);

    const markup = extractBodyMarkup(storageIndexHtml);
    if (!markup) {
      throw new Error('Storage workspace markup is empty.');
    }

    root.innerHTML = markup;
    root.classList.remove('loading-state');
    root.setAttribute('data-theme', 'dark');

    window.CLOUDSTUDIO_STORAGE_EMBEDDED = true;
    window.CLOUDSTUDIO_STORAGE_BASE_PATH = '/apps/storage';
    window.CLOUDSTUDIO_STORAGE_FORCE_THEME = 'dark';
    window.CLOUDSTUDIO_STORAGE_THEME_TARGET = root;
    window.CLOUDSTUDIO_STORAGE_CLASS_TARGET = root;

    if (!state.storageEmbed.moduleLoaded) {
      await import('/apps/storage/app.js?embedded=6');
      state.storageEmbed.moduleLoaded = true;
    }

    state.storageEmbed.initialized = true;
  })()
    .catch((error) => {
      root.classList.remove('loading-state');
      root.innerHTML = `<div class="storage-load-error">${escapeHtml(error.message || 'Storage workspace failed to load.')}</div>`;
      throw error;
    })
    .finally(() => {
      state.storageEmbed.initPromise = null;
    });

  return state.storageEmbed.initPromise;
}

async function ensurePricingWorkspace() {
  if (state.pricingEmbed.initialized) {
    return;
  }

  if (state.pricingEmbed.initPromise) {
    return state.pricingEmbed.initPromise;
  }

  const root = dom.pricingEmbedRoot;
  if (!root) {
    return;
  }

  state.pricingEmbed.initPromise = (async () => {
    root.classList.add('loading-state');
    root.innerHTML = '<p class="muted">Loading pricing workspace...</p>';

    const [pricingIndexHtml, pricingCss] = await Promise.all([fetchText('/apps/pricing/index.html'), fetchText('/apps/pricing/styles.css')]);
    ensurePricingScopedStyles(pricingCss);

    const markup = extractBodyMarkup(pricingIndexHtml);
    if (!markup) {
      throw new Error('Pricing workspace markup is empty.');
    }

    root.innerHTML = markup;
    root.classList.remove('loading-state');
    root.setAttribute('data-theme', 'dark');

    window.CLOUDSTUDIO_PRICING_EMBEDDED = true;
    window.CLOUDSTUDIO_PRICING_BASE_PATH = '/apps/pricing';
    window.CLOUDSTUDIO_PRICING_THEME_TARGET = root;
    window.CLOUDSTUDIO_PRICING_CLASS_TARGET = root;

    if (!state.pricingEmbed.moduleLoaded) {
      await import('/apps/pricing/app.js?embedded=2');
      state.pricingEmbed.moduleLoaded = true;
    }

    state.pricingEmbed.initialized = true;
  })()
    .catch((error) => {
      root.classList.remove('loading-state');
      root.innerHTML = `<div class="storage-load-error">${escapeHtml(error.message || 'Pricing workspace failed to load.')}</div>`;
      throw error;
    })
    .finally(() => {
      state.pricingEmbed.initPromise = null;
    });

  return state.pricingEmbed.initPromise;
}

function setActiveView(viewName, options = {}) {
  const preferredView = resolveViewName(viewName);
  const resolvedView = hasClientViewAccess(preferredView) ? preferredView : getFirstAllowedView();
  const billingRelated = isBillingRelatedView(resolvedView);
  const storageRelated = isStorageRelatedView(resolvedView);
  const liveRelated = isLiveRelatedView(resolvedView);
  const adminRelated = isAdminRelatedView(resolvedView);

  dom.navItems.forEach((item) => {
    const itemView = String(item.dataset.view || '').trim();
    const isActive = itemView === resolvedView || (itemView === 'billing' && billingRelated);
    item.classList.toggle('active', isActive);
  });

  dom.views.forEach((view) => {
    view.classList.toggle('active', view.dataset.view === resolvedView);
  });

  if (options.persist !== false) {
    saveActiveView(resolvedView);
  }

  if (window.matchMedia('(max-width: 860px)').matches) {
    setDrawer(false);
  }

  if (!billingRelated) {
    setBillingNavExpanded(false);
  }
  if (!storageRelated) {
    setStorageNavExpanded(false);
  }
  if (!liveRelated) {
    setLiveNavExpanded(false);
  }
  if (!adminRelated) {
    setAdminNavExpanded(false);
  } else {
    setAdminNavExpanded(true);
  }

  renderStorageNavGroup();
  renderLiveNavGroup();

  if (resolvedView === 'storage') {
    setStorageNavExpanded(true);
    void ensureStorageWorkspace().catch((error) => {
      showToast(error.message || 'Storage workspace failed to load.', true);
    });
    setStorageEmbedProviderPreference(state.storage.selectedProvider, { persist: false });
  }

  if (resolvedView === 'pricing') {
    void ensurePricingWorkspace().catch((error) => {
      showToast(error.message || 'Pricing workspace failed to load.', true);
    });
  }

  if (resolvedView === 'ip-address') {
    void Promise.all([loadIpMap(), loadIpDiscovery()]).catch((error) => {
      showToast(error.message || 'IP address module failed to load.', true);
    });
  }

  if (resolvedView === 'tags') {
    void loadTags().catch((error) => {
      showToast(error.message || 'Tag module failed to load.', true);
    });
  }

  if (resolvedView === 'cloud-metrics') {
    void loadCloudMetrics().catch((error) => {
      showToast(error.message || 'Cloud metrics failed to load.', true);
    });
  }

  if (resolvedView === 'cloud-database') {
    void loadCloudDatabase().catch((error) => {
      showToast(error.message || 'Cloud Database failed to load.', true);
    });
  }

  if (resolvedView === 'grafana-cloud') {
    void loadGrafanaCloud().catch((error) => {
      showToast(error.message || 'Grafana-Cloud module failed to load.', true);
    });
  }

  if (resolvedView === 'live') {
    void loadLive({ refresh: true }).catch((error) => {
      showToast(error.message || 'Live View (VSAx) failed to load.', true);
    });
  }

  if (resolvedView === 'firewall') {
    void loadFirewall().catch((error) => {
      showToast(error.message || 'Firewall module failed to load.', true);
    });
  }

  if (resolvedView === 'vpn') {
    void loadVpn().catch((error) => {
      showToast(error.message || 'VPN module failed to load.', true);
    });
  }

  if (resolvedView === 'security') {
    void loadSecurity().catch((error) => {
      showToast(error.message || 'Security module failed to load.', true);
    });
  }

  if (resolvedView === 'admin-users') {
    void loadAdminUsers().catch((error) => {
      showToast(error.message || 'Admin users failed to load.', true);
    });
  }

  if (resolvedView === 'admin-settings') {
    void loadAppConfig().catch((error) => {
      showToast(error.message || 'App config failed to load.', true);
    });
  }

  if (resolvedView === 'admin-api-keys') {
    void loadApiKeys().catch((error) => {
      showToast(error.message || 'API keys failed to load.', true);
    });
  }

  if (resolvedView === 'admin-backup') {
    void loadDbBackupConfig().catch((error) => {
      showToast(error.message || 'DB backup config failed to load.', true);
    });
  }

  if (resolvedView === 'billing-backfill') {
    void loadBillingBackfillStatus().catch((error) => {
      showToast(error.message || 'Billing backfill status failed to load.', true);
    });
  }

  if (resolvedView === 'billing-budget') {
    void loadBillingBudget().catch((error) => {
      showToast(error.message || 'Billing budget failed to load.', true);
    });
  }

  if (resolvedView === 'billing') {
    void loadBillingAutoSyncStatus().catch(() => {
      // ignore periodic status errors
    });
  }

  return resolvedView;
}

function buildStatusBadge(status) {
  const key = String(status || '').toLowerCase();
  const safe = ['healthy', 'warning', 'open'].includes(key) ? key : 'warning';
  return `<span class="status-badge ${safe}">${status || safe}</span>`;
}

function renderServiceStatusPanel(rows) {
  if (!dom.healthPill) {
    return;
  }

  const safeRows = Array.isArray(rows) ? rows : [];
  const rendered = safeRows
    .map((row) => {
      const label = String(row?.label || 'Service');
      const isUp = Boolean(row?.isUp);
      const state = isUp ? 'up' : 'down';
      const stateLabel = isUp ? 'Up' : 'Down';
      return `
        <div class="service-status-item ${state}">
          <span class="service-status-dot ${state}" aria-hidden="true"></span>
          <span class="service-status-label">${escapeHtml(label)}</span>
          <span class="service-status-value">${stateLabel}</span>
        </div>
      `;
    })
    .join('');

  dom.healthPill.innerHTML = `
    <p class="drawer-service-title">Service Modules</p>
    ${rendered}
  `;

  const ariaSummary = safeRows
    .map((row) => `${row?.label || 'Service'} ${row?.isUp ? 'up' : 'down'}`)
    .join(' | ');
  dom.healthPill.setAttribute('aria-label', ariaSummary || 'Service status');
}

async function loadServices() {
  const payload = await api('/api/services');
  state.services = payload.services;

  const storageOk = Boolean(payload.services?.storage?.running);
  const priceOk = Boolean(payload.services?.price?.running);
  const ipAddressOk = Boolean(payload.services?.ipAddress?.running);
  const billingOk = Boolean(payload.services?.billing?.running);
  const tagOk = Boolean(payload.services?.tag?.running);
  const grafanaCloudOk = Boolean(payload.services?.grafanaCloud?.running);
  const firewallOk = Boolean(payload.services?.firewall?.running);
  const cloudMetricsState = payload.cloudMetrics || payload.services?.cloudMetrics || null;
  const cloudMetricsOk = cloudMetricsState
    ? cloudMetricsState.lastStatus !== 'error' && (cloudMetricsState.configured !== false || cloudMetricsState.enabled === false)
    : true;
  renderServiceStatusPanel([
    { label: 'Storage', isUp: storageOk },
    { label: 'Pricing', isUp: priceOk },
    { label: 'IP Address', isUp: ipAddressOk },
    { label: 'Billing', isUp: billingOk },
    { label: 'Tag', isUp: tagOk },
    { label: 'Grafana-Cloud', isUp: grafanaCloudOk },
    { label: 'Firewall', isUp: firewallOk },
    { label: 'Cloud Metrics', isUp: cloudMetricsOk },
    { label: 'Cloud Database', isUp: cloudMetricsOk }
  ]);
}

function renderOverviewCards(data) {
  const cardsHost = document.getElementById('overviewCards');
  const totals = data?.totals || {};
  const cards = [
    { label: 'Accounts', value: formatNumber(totals.accountCount || 0) },
    { label: 'Resources', value: formatNumber(totals.resourceCount || 0) },
    { label: 'Storage', value: formatBytes(totals.storageBytes || 0) },
    { label: 'Egress (24h)', value: formatBytes(totals.egressBytes24h || 0) },
    { label: 'Ingress (24h)', value: formatBytes(totals.ingressBytes24h || 0) },
    { label: 'Txns (24h)', value: formatNumber(totals.transactions24h || 0) }
  ];

  cardsHost.innerHTML = cards
    .map(
      (card) => `
      <article class="card">
        <p class="label">${card.label}</p>
        <p class="value">${card.value}</p>
      </article>
    `
    )
    .join('');
}

function summarizeTotalsByCurrencyFromRows(rows = [], amountField = 'totalAmount', currencyField = 'currency') {
  const currencyTotals = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const currency = String(row?.[currencyField] || 'USD').trim().toUpperCase() || 'USD';
    const amount = Number(row?.[amountField] || 0);
    if (!Number.isFinite(amount)) {
      continue;
    }
    currencyTotals.set(currency, (currencyTotals.get(currency) || 0) + amount);
  }
  return Array.from(currencyTotals.entries()).map(([currency, totalAmount]) => ({
    currency,
    totalAmount
  }));
}

function buildProviderTotalsFromAccountPayload(payload = {}) {
  const inputRows = Array.isArray(payload?.providerTotals) ? payload.providerTotals : [];
  const map = new Map();
  for (const row of inputRows) {
    const provider = String(row?.provider || '').trim().toLowerCase();
    if (!provider) {
      continue;
    }
    const providerLabel = getProviderDisplayLabel(row?.providerLabel || provider, 'PROVIDER');
    const currency = String(row?.currency || 'USD').trim().toUpperCase() || 'USD';
    const amount = Number(row?.totalAmount || 0);
    const key = provider;
    const current = map.get(key) || {
      provider,
      providerLabel,
      currencyTotals: new Map()
    };
    current.providerLabel = providerLabel;
    current.currencyTotals.set(currency, (current.currencyTotals.get(currency) || 0) + (Number.isFinite(amount) ? amount : 0));
    map.set(key, current);
  }
  return Array.from(map.values())
    .map((item) => ({
      provider: item.provider,
      providerLabel: item.providerLabel,
      totals: Array.from(item.currencyTotals.entries()).map(([currency, totalAmount]) => ({
        currency,
        totalAmount
      }))
    }))
    .sort((left, right) => left.providerLabel.localeCompare(right.providerLabel));
}

function getBillingDetailRowsFromPayload(payload = {}) {
  const groups = Array.isArray(payload?.resourceDetails) ? payload.resourceDetails : [];
  const rows = [];
  for (const group of groups) {
    const details = Array.isArray(group?.details) ? group.details : [];
    for (const detail of details) {
      rows.push({
        ...detail,
        provider: detail?.provider || group?.provider || '',
        currency: detail?.currency || group?.currency || 'USD',
        resourceType: detail?.resourceType || group?.resourceType || ''
      });
    }
  }
  return rows;
}

function renderOverviewBillingOrgTotals(detailRows = [], periodStart = '-', periodEnd = '-') {
  if (!dom.overviewBillingOrgCards) {
    return;
  }

  if (dom.overviewBillingOrgHint) {
    dom.overviewBillingOrgHint.textContent = `All providers | ${periodStart} to ${periodEnd} | tag key: org`;
  }

  if (!detailRows.length) {
    dom.overviewBillingOrgCards.innerHTML = `
      <article class="card">
        <p class="label">Org totals</p>
        <p class="value">-</p>
        <p class="muted">No billing line items found for selected period.</p>
      </article>
    `;
    return;
  }

  const orgTotals = summarizeBillingOrgTotals(detailRows);
  if (!orgTotals.length) {
    dom.overviewBillingOrgCards.innerHTML = `
      <article class="card">
        <p class="label">Org totals</p>
        <p class="value">-</p>
        <p class="muted">No billable line items available for org totals.</p>
      </article>
    `;
    return;
  }

  dom.overviewBillingOrgCards.innerHTML = orgTotals
    .map(
      (row) => `
      <article
        class="card billing-org-card billing-tag-filter-card"
        data-billing-tag-key="org"
        data-billing-tag-value="${escapeHtml(String(row.tagValue || '').trim().toLowerCase())}"
        data-billing-tag-scope="all"
        role="button"
        tabindex="0"
        title="Filter billing by ${escapeHtml(row.label)}"
      >
        <p class="label">${escapeHtml(row.label)}</p>
        <p class="value">${escapeHtml(formatBillingTotalsByCurrency(row.totals))}</p>
        <p class="muted">${escapeHtml(formatNumber(row.lineItemCount))} line item(s) • ${escapeHtml(
        formatNumber(row.resourceCount)
      )} resource(s)</p>
      </article>
    `
    )
    .join('');
}

function renderOverviewBillingProductTotals(detailRows = [], periodStart = '-', periodEnd = '-') {
  if (!dom.overviewBillingProductCards) {
    return;
  }

  if (dom.overviewBillingProductHint) {
    dom.overviewBillingProductHint.textContent = `All providers | ${periodStart} to ${periodEnd} | tag key: product`;
  }

  if (!detailRows.length) {
    dom.overviewBillingProductCards.innerHTML = `
      <article class="card">
        <p class="label">Product totals</p>
        <p class="value">-</p>
        <p class="muted">No billing line items found for selected period.</p>
      </article>
    `;
    return;
  }

  const productTotals = summarizeBillingProductTotals(detailRows);
  if (!productTotals.length) {
    dom.overviewBillingProductCards.innerHTML = `
      <article class="card">
        <p class="label">Product totals</p>
        <p class="value">-</p>
        <p class="muted">No billable line items available for product totals.</p>
      </article>
    `;
    return;
  }

  dom.overviewBillingProductCards.innerHTML = productTotals
    .map(
      (row) => `
      <article
        class="card billing-org-card billing-tag-filter-card"
        data-billing-tag-key="product"
        data-billing-tag-value="${escapeHtml(String(row.tagValue || '').trim().toLowerCase())}"
        data-billing-tag-scope="all"
        role="button"
        tabindex="0"
        title="Filter billing by ${escapeHtml(row.label)}"
      >
        <p class="label">${escapeHtml(row.label)}</p>
        <p class="value">${escapeHtml(formatBillingTotalsByCurrency(row.totals))}</p>
        <p class="muted">${escapeHtml(formatNumber(row.lineItemCount))} line item(s) • ${escapeHtml(
        formatNumber(row.resourceCount)
      )} resource(s)</p>
      </article>
    `
    )
    .join('');
}

function renderOverviewBillingSummary(accountsPayload = {}, detailsPayload = {}, options = {}) {
  if (!dom.overviewBillingPanel) {
    return;
  }

  const periodStart = String(accountsPayload?.period?.periodStart || '').trim() || getLastMonthRange().periodStart;
  const periodEnd = String(accountsPayload?.period?.periodEnd || '').trim() || getLastMonthRange().periodEnd;
  const preset = normalizeOverviewBillingPreset(options?.preset || state.overviewBilling.rangePreset);
  const presetLabel = preset === 'month_to_date' ? 'month-to-date' : 'last month';
  if (dom.overviewBillingTitle) {
    dom.overviewBillingTitle.textContent = `Provider/account totals (${presetLabel}: ${periodStart} to ${periodEnd})`;
  }

  const accountRows = Array.isArray(accountsPayload?.accounts) ? accountsPayload.accounts : [];
  const providerRows = buildProviderTotalsFromAccountPayload(accountsPayload);
  const grandTotals = Array.isArray(accountsPayload?.grandTotals)
    ? accountsPayload.grandTotals
    : summarizeTotalsByCurrencyFromRows(accountRows);

  if (dom.overviewBillingCards) {
    const cards = [
      {
        label: 'Providers',
        value: formatNumber(providerRows.length)
      },
      {
        label: 'Accounts',
        value: formatNumber(accountRows.length)
      },
      {
        label: preset === 'month_to_date' ? 'Month-to-date total' : 'Last month total',
        value: formatBillingTotalsByCurrency(grandTotals)
      }
    ];
    for (const providerRow of providerRows) {
      cards.push({
        label: `${providerRow.providerLabel} total`,
        value: formatBillingTotalsByCurrency(providerRow.totals)
      });
    }
    dom.overviewBillingCards.innerHTML = cards
      .map(
        (card) => `
          <article class="card">
            <p class="label">${escapeHtml(card.label)}</p>
            <p class="value">${escapeHtml(card.value)}</p>
          </article>
        `
      )
      .join('');
  }

  const detailRows = getBillingDetailRowsFromPayload(detailsPayload);
  renderOverviewBillingOrgTotals(detailRows, periodStart, periodEnd);
  renderOverviewBillingProductTotals(detailRows, periodStart, periodEnd);
}

async function loadOverview() {
  const data = await api('/api/platform/overview');
  renderOverviewCards(data);

  const providersBody = document.getElementById('overviewProvidersBody');
  const providers = Array.isArray(data.providers) ? data.providers : [];
  providersBody.innerHTML = providers
    .map(
      (row) => `
      <tr>
        <td>${row.provider}</td>
        <td>${formatNumber(row.accountCount)}</td>
        <td>${formatNumber(row.resourceCount)}</td>
        <td>${formatBytes(row.storageBytes)}</td>
        <td>${formatBytes(row.egressBytes24h)}</td>
        <td>${formatBytes(row.ingressBytes24h)}</td>
        <td>${formatNumber(row.transactions24h)}</td>
        <td>${formatDateTime(row.lastSyncAt)}</td>
      </tr>
    `
    )
    .join('');

  if (dom.overviewBillingPanel) {
    if (!hasClientViewAccess('billing')) {
      dom.overviewBillingPanel.hidden = true;
    } else {
      dom.overviewBillingPanel.hidden = false;
      renderOverviewBillingPresetButtons();
      const overviewPreset = normalizeOverviewBillingPreset(state.overviewBilling.rangePreset);
      const selectedRange = getBillingRangeForPreset(overviewPreset);
      const query = new URLSearchParams({
        periodStart: selectedRange.periodStart,
        periodEnd: selectedRange.periodEnd
      });
      let accountsPayload = null;
      let detailsPayload = null;
      try {
        [accountsPayload, detailsPayload] = await Promise.all([
          api(`/api/billing/accounts?${query.toString()}`),
          api(`/api/billing?${query.toString()}&limit=1`)
        ]);
      } catch (error) {
        if (dom.overviewBillingCards) {
          dom.overviewBillingCards.innerHTML = `
            <article class="card">
              <p class="label">Billing summary</p>
              <p class="value">-</p>
              <p class="muted">${escapeHtml(error?.message || 'Billing summary unavailable.')}</p>
            </article>
          `;
        }
        renderOverviewBillingOrgTotals([], selectedRange.periodStart, selectedRange.periodEnd);
        renderOverviewBillingProductTotals([], selectedRange.periodStart, selectedRange.periodEnd);
        return;
      }

      const detailRows = getBillingDetailRowsFromPayload(detailsPayload || {});
      if (detailRows.length) {
        try {
          await resolveTagsForDetailRows(detailRows);
        } catch (error) {
          console.warn('[dashboard] Billing org tags resolve skipped:', error?.message || String(error));
        }
      }
      renderOverviewBillingSummary(accountsPayload || {}, detailsPayload || {}, { preset: overviewPreset });
    }
  }
}

function parseCredentialsInput(raw) {
  const trimmed = String(raw || '').trim();
  if (!trimmed) {
    return null;
  }
  return JSON.parse(trimmed);
}

function isVendorHiddenInClient(vendor = {}) {
  const parse = (value) => {
    if (value === undefined || value === null || value === '') {
      return null;
    }
    if (typeof value === 'boolean') {
      return value;
    }
    if (typeof value === 'number') {
      return value !== 0;
    }
    const normalized = String(value).trim().toLowerCase();
    if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) {
      return true;
    }
    if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) {
      return false;
    }
    return null;
  };
  const direct = parse(vendor?.hidden);
  if (direct !== null) {
    return direct;
  }
  const metadata = vendor?.metadata && typeof vendor.metadata === 'object' && !Array.isArray(vendor.metadata) ? vendor.metadata : {};
  return parse(metadata.hidden) === true;
}

function readBillingRangeFromInputs() {
  const periodStart = String(dom.billingRangeStart?.value || '').trim();
  const periodEnd = String(dom.billingRangeEnd?.value || '').trim();
  if (!periodStart && !periodEnd) {
    return null;
  }
  if (!periodStart || !periodEnd) {
    throw new Error('Set both billing range dates: From and To.');
  }
  return {
    periodStart,
    periodEnd
  };
}

function resolveBillingScopeFilter() {
  const scopeId = normalizeBillingScopeId(state.billing.selectedScopeId);
  if (scopeId.startsWith('vendor:')) {
    return {
      scopeId,
      vendorId: scopeId.slice('vendor:'.length)
    };
  }
  if (scopeId.startsWith('provider:')) {
    return {
      scopeId,
      provider: scopeId.slice('provider:'.length)
    };
  }
  return { scopeId: 'all' };
}

function formatBillingScopeLabel(scope) {
  if (!scope || scope.scopeId === 'all') {
    return 'All providers';
  }
  if (scope.vendorId) {
    const account = state.billing.accounts.find((row) => row.scopeId === scope.scopeId);
    if (!account) {
      return 'Selected account';
    }
    return `${getProviderDisplayLabel(account.providerLabel || account.provider || '', 'PROVIDER')} • ${account.accountName || account.accountId || 'Account'
      }`;
  }
  if (scope.provider) {
    return `${getProviderDisplayLabel(scope.provider, 'PROVIDER')} provider`;
  }
  return 'All providers';
}

function resolveBillingProviderScopeId(scopeId) {
  const normalized = normalizeBillingScopeId(scopeId);
  if (normalized.startsWith('provider:')) {
    return normalized;
  }
  if (normalized.startsWith('vendor:')) {
    const account = state.billing.accounts.find((row) => String(row.scopeId || '') === normalized);
    const provider = String(account?.provider || '').trim().toLowerCase();
    if (provider) {
      return `provider:${provider}`;
    }
  }
  return '';
}

function getSelectedBillingProvider() {
  const selectedScopeId = normalizeBillingScopeId(state.billing.selectedScopeId);
  const providerScopeId = resolveBillingProviderScopeId(selectedScopeId);
  if (providerScopeId.startsWith('provider:')) {
    return providerScopeId.slice('provider:'.length);
  }
  return '';
}

function summarizeBillingTotalsByCurrency(rows = []) {
  const totalsByCurrency = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const currency = String(row?.currency || 'USD').trim().toUpperCase() || 'USD';
    const amount = Number(row?.totalAmount || 0);
    if (!Number.isFinite(amount)) {
      continue;
    }
    totalsByCurrency.set(currency, (totalsByCurrency.get(currency) || 0) + amount);
  }
  return Array.from(totalsByCurrency.entries()).map(([currency, totalAmount]) => ({
    currency,
    totalAmount
  }));
}

function buildBillingProviderSummaries() {
  const accountRows = Array.isArray(state.billing.accounts) ? state.billing.accounts : [];
  const providerTotals = Array.isArray(state.billing.providerTotals) ? state.billing.providerTotals : [];
  const map = new Map();

  const providerCandidates = new Set();
  for (const provider of Array.isArray(state.vendorProviders) ? state.vendorProviders : []) {
    const normalized = String(provider || '').trim().toLowerCase();
    if (normalized) {
      providerCandidates.add(normalized);
    }
  }
  for (const row of providerTotals) {
    const normalized = String(row?.provider || '').trim().toLowerCase();
    if (normalized) {
      providerCandidates.add(normalized);
    }
  }
  for (const row of accountRows) {
    const normalized = String(row?.provider || '').trim().toLowerCase();
    if (normalized) {
      providerCandidates.add(normalized);
    }
  }

  for (const provider of providerCandidates) {
    map.set(provider, {
      provider,
      providerLabel: getProviderDisplayLabel(provider, 'PROVIDER'),
      accountCount: 0,
      rows: [],
      currencyTotals: new Map()
    });
  }

  for (const row of accountRows) {
    const provider = String(row.provider || '').trim().toLowerCase();
    if (!provider) {
      continue;
    }
    const current = map.get(provider);
    if (!current) {
      continue;
    }
    current.providerLabel = getProviderDisplayLabel(row.providerLabel || current.providerLabel || provider, 'PROVIDER');
    current.rows.push(row);
    current.accountCount += 1;

    const currency = String(row.currency || 'USD').trim().toUpperCase() || 'USD';
    const amount = Number(row.totalAmount || 0);
    current.currencyTotals.set(currency, (current.currencyTotals.get(currency) || 0) + (Number.isFinite(amount) ? amount : 0));
  }

  for (const row of providerTotals) {
    const provider = String(row?.provider || '').trim().toLowerCase();
    if (!provider) {
      continue;
    }
    let current = map.get(provider);
    if (!current) {
      current = {
        provider,
        providerLabel: getProviderDisplayLabel(row?.providerLabel || provider, 'PROVIDER'),
        accountCount: 0,
        rows: [],
        currencyTotals: new Map()
      };
      map.set(provider, current);
    }
    current.providerLabel = getProviderDisplayLabel(row?.providerLabel || current.providerLabel || provider, 'PROVIDER');
    if (Number.isFinite(Number(row?.accountCount))) {
      current.accountCount = Math.max(current.accountCount, Number(row.accountCount));
    }
    const currency = String(row?.currency || 'USD').trim().toUpperCase() || 'USD';
    const totalAmount = Number(row?.totalAmount || 0);
    current.currencyTotals.set(currency, Number.isFinite(totalAmount) ? totalAmount : 0);
  }

  return Array.from(map.values())
    .map((summary) => ({
      provider: summary.provider,
      providerLabel: summary.providerLabel,
      accountCount: summary.accountCount,
      totals: Array.from(summary.currencyTotals.entries()).map(([currency, totalAmount]) => ({
        currency,
        totalAmount
      })),
      rows: summary.rows
        .slice()
        .sort((left, right) => {
          const leftAmount = Number(left?.totalAmount || 0);
          const rightAmount = Number(right?.totalAmount || 0);
          if (rightAmount !== leftAmount) {
            return rightAmount - leftAmount;
          }
          return String(left?.accountName || '').localeCompare(String(right?.accountName || ''));
        })
    }))
    .sort((left, right) => left.providerLabel.localeCompare(right.providerLabel));
}

function formatBillingTotalsByCurrency(totals = []) {
  const rows = Array.isArray(totals) ? totals : [];
  return (
    rows
      .map((item) => formatCurrency(item?.totalAmount, item?.currency))
      .filter((value) => value && value !== '-')
      .join(' / ') || '-'
  );
}

function getBillingDetailRowsForOrgTotals() {
  const groups = Array.isArray(state.billing.resourceDetailGroups) ? state.billing.resourceDetailGroups : [];
  const rows = [];
  for (const group of groups) {
    const groupDetails = Array.isArray(group?.details) ? group.details : [];
    for (const detail of groupDetails) {
      rows.push({
        ...detail,
        provider: detail?.provider || group?.provider || '',
        currency: detail?.currency || group?.currency || 'USD',
        resourceType: detail?.resourceType || group?.resourceType || ''
      });
    }
  }
  return rows;
}

function resolveBillingTagValueForDetail(detail = {}, tagKey = 'org') {
  const normalizedTagKey = String(tagKey || '').trim().toLowerCase();
  if (!normalizedTagKey) {
    return '';
  }
  const resourceRef = String(detail?.resourceRef || '').trim();
  if (!resourceRef) {
    return '';
  }
  const entries = getBillingTagEntriesForResource(resourceRef);
  if (!entries.length) {
    return '';
  }
  const tagEntry = entries.find((entry) => entry.keyLower === normalizedTagKey);
  if (!tagEntry) {
    return '';
  }
  const value = String(tagEntry.tagValue || '').trim().toLowerCase();
  if (!value || value === 'null') {
    return '';
  }
  return value;
}

function getSortableBillingAmount(totals = []) {
  const rows = Array.isArray(totals) ? totals : [];
  const usd = rows.find((row) => String(row?.currency || '').trim().toUpperCase() === 'USD');
  if (usd && Number.isFinite(Number(usd.totalAmount))) {
    return Number(usd.totalAmount);
  }
  const first = rows.find((row) => Number.isFinite(Number(row?.totalAmount)));
  if (first) {
    return Number(first.totalAmount);
  }
  return 0;
}

function summarizeBillingTagTotals(detailRows = [], tagKey = 'org') {
  const normalizedTagKey = String(tagKey || '').trim().toLowerCase();
  if (!normalizedTagKey) {
    return [];
  }
  const buckets = new Map();
  for (const detail of Array.isArray(detailRows) ? detailRows : []) {
    const amount = Number(detail?.amount || 0);
    if (!Number.isFinite(amount)) {
      continue;
    }
    const currency = String(detail?.currency || 'USD').trim().toUpperCase() || 'USD';
    const resourceRef = String(detail?.resourceRef || '').trim();
    const tagValue = resolveBillingTagValueForDetail(detail, normalizedTagKey);
    const key = tagValue || `__${normalizedTagKey}_null__`;
    const current = buckets.get(key) || {
      tagValue,
      lineItemCount: 0,
      resourceRefs: new Set(),
      currencyTotals: new Map()
    };
    current.lineItemCount += 1;
    if (resourceRef) {
      current.resourceRefs.add(resourceRef);
    }
    current.currencyTotals.set(currency, (current.currencyTotals.get(currency) || 0) + amount);
    buckets.set(key, current);
  }

  return Array.from(buckets.values())
    .map((bucket) => ({
      tagValue: bucket.tagValue,
      label: bucket.tagValue ? `${normalizedTagKey}=${bucket.tagValue}` : `${normalizedTagKey} = null`,
      lineItemCount: bucket.lineItemCount,
      resourceCount: bucket.resourceRefs.size,
      totals: Array.from(bucket.currencyTotals.entries()).map(([currency, totalAmount]) => ({
        currency,
        totalAmount
      }))
    }))
    .sort((left, right) => {
      const amountDiff = getSortableBillingAmount(right.totals) - getSortableBillingAmount(left.totals);
      if (amountDiff !== 0) {
        return amountDiff;
      }
      return String(left.label || '').localeCompare(String(right.label || ''));
    });
}

function summarizeBillingOrgTotals(detailRows = []) {
  return summarizeBillingTagTotals(detailRows, 'org');
}

function summarizeBillingProductTotals(detailRows = []) {
  return summarizeBillingTagTotals(detailRows, 'product');
}

async function applyBillingTagSummaryFilter(tagKey, tagValue, options = {}) {
  if (!hasClientViewAccess('billing')) {
    showToast('You do not have access to Billing.', true);
    return;
  }
  const normalizedKey = String(tagKey || '').trim().toLowerCase();
  if (!normalizedKey) {
    return;
  }
  const normalizedValue = String(tagValue || '').trim().toLowerCase();

  if (options.scope === 'all') {
    setBillingScope('all');
  }

  state.billing.typeResourceFilter = 'all';
  state.billing.searchText = '';
  saveBillingSearchText('');
  if (dom.billingSearchInput) {
    dom.billingSearchInput.value = '';
  }

  state.billing.typeTagFilter = normalizedValue
    ? buildBillingTypeTagFilterValue(normalizedKey, normalizedValue)
    : normalizedKey === 'org'
      ? 'org_null'
      : buildBillingTypeTagNullFilterValue(normalizedKey);

  setActiveView('billing');
  await loadBilling();
}

function setBillingOrgTotalsLoading(message = 'Loading org totals from tags...') {
  if (!dom.billingOrgTotalsCards) {
    return;
  }
  dom.billingOrgTotalsCards.innerHTML = `
    <article class="card">
      <p class="label">Org totals</p>
      <p class="value">-</p>
      <p class="muted">${escapeHtml(message)}</p>
    </article>
  `;
}

function setBillingProductTotalsLoading(message = 'Loading product totals from tags...') {
  if (!dom.billingProductTotalsCards) {
    return;
  }
  dom.billingProductTotalsCards.innerHTML = `
    <article class="card">
      <p class="label">Product totals</p>
      <p class="value">-</p>
      <p class="muted">${escapeHtml(message)}</p>
    </article>
  `;
}

function renderBillingOrgTotals() {
  if (!dom.billingOrgTotalsCards) {
    return;
  }
  const detailRows = getBillingDetailRowsForOrgTotals();
  const periodStart = state.billing.period?.periodStart || '-';
  const periodEnd = state.billing.period?.periodEnd || '-';
  const scopeLabel = formatBillingScopeLabel(resolveBillingScopeFilter());
  if (dom.billingOrgTotalsHint) {
    dom.billingOrgTotalsHint.textContent = `${scopeLabel} | ${periodStart} to ${periodEnd} | tag key: org`;
  }

  if (!detailRows.length) {
    dom.billingOrgTotalsCards.innerHTML = `
      <article class="card">
        <p class="label">Org totals</p>
        <p class="value">-</p>
        <p class="muted">No billing line items found for this range.</p>
      </article>
    `;
    return;
  }

  const orgTotals = summarizeBillingOrgTotals(detailRows);
  if (!orgTotals.length) {
    dom.billingOrgTotalsCards.innerHTML = `
      <article class="card">
        <p class="label">Org totals</p>
        <p class="value">-</p>
        <p class="muted">No billable line items available for org totals.</p>
      </article>
    `;
    return;
  }

  dom.billingOrgTotalsCards.innerHTML = orgTotals
    .map(
      (row) => `
      <article
        class="card billing-org-card billing-tag-filter-card"
        data-billing-tag-key="org"
        data-billing-tag-value="${escapeHtml(String(row.tagValue || '').trim().toLowerCase())}"
        data-billing-tag-scope="current"
        role="button"
        tabindex="0"
        title="Filter billing by ${escapeHtml(row.label)}"
      >
        <p class="label">${escapeHtml(row.label)}</p>
        <p class="value">${escapeHtml(formatBillingTotalsByCurrency(row.totals))}</p>
        <p class="muted">${escapeHtml(formatNumber(row.lineItemCount))} line item(s) • ${escapeHtml(
        formatNumber(row.resourceCount)
      )} resource(s)</p>
      </article>
    `
    )
    .join('');
}

function renderBillingProductTotals() {
  if (!dom.billingProductTotalsCards) {
    return;
  }
  const detailRows = getBillingDetailRowsForOrgTotals();
  const periodStart = state.billing.period?.periodStart || '-';
  const periodEnd = state.billing.period?.periodEnd || '-';
  const scopeLabel = formatBillingScopeLabel(resolveBillingScopeFilter());
  if (dom.billingProductTotalsHint) {
    dom.billingProductTotalsHint.textContent = `${scopeLabel} | ${periodStart} to ${periodEnd} | tag key: product`;
  }

  if (!detailRows.length) {
    dom.billingProductTotalsCards.innerHTML = `
      <article class="card">
        <p class="label">Product totals</p>
        <p class="value">-</p>
        <p class="muted">No billing line items found for this range.</p>
      </article>
    `;
    return;
  }

  const productTotals = summarizeBillingProductTotals(detailRows);
  if (!productTotals.length) {
    dom.billingProductTotalsCards.innerHTML = `
      <article class="card">
        <p class="label">Product totals</p>
        <p class="value">-</p>
        <p class="muted">No billable line items available for product totals.</p>
      </article>
    `;
    return;
  }

  dom.billingProductTotalsCards.innerHTML = productTotals
    .map(
      (row) => `
      <article
        class="card billing-org-card billing-tag-filter-card"
        data-billing-tag-key="product"
        data-billing-tag-value="${escapeHtml(String(row.tagValue || '').trim().toLowerCase())}"
        data-billing-tag-scope="current"
        role="button"
        tabindex="0"
        title="Filter billing by ${escapeHtml(row.label)}"
      >
        <p class="label">${escapeHtml(row.label)}</p>
        <p class="value">${escapeHtml(formatBillingTotalsByCurrency(row.totals))}</p>
        <p class="muted">${escapeHtml(formatNumber(row.lineItemCount))} line item(s) • ${escapeHtml(
        formatNumber(row.resourceCount)
      )} resource(s)</p>
      </article>
    `
    )
    .join('');
}

function parseBillingTypeFilterValue(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized || 'all';
}

function parseBillingTypeTagFilterValue(value) {
  const raw = String(value || '').trim();
  if (!raw || raw === 'all') {
    return { mode: 'all', value: 'all' };
  }
  if (raw === 'tagged') {
    return { mode: 'tagged', value: 'tagged' };
  }
  if (raw === 'untagged') {
    return { mode: 'untagged', value: 'untagged' };
  }
  if (raw === 'org_null') {
    return { mode: 'key_null', value: 'org_null', key: 'org', keyLower: 'org' };
  }
  if (raw.startsWith('key_null:')) {
    let key = '';
    try {
      key = decodeURIComponent(raw.slice('key_null:'.length) || '').trim().toLowerCase();
    } catch (_error) {
      return { mode: 'all', value: 'all' };
    }
    if (!key) {
      return { mode: 'all', value: 'all' };
    }
    return {
      mode: 'key_null',
      value: buildBillingTypeTagNullFilterValue(key),
      key,
      keyLower: key
    };
  }
  if (!raw.startsWith('kv:')) {
    return { mode: 'all', value: 'all' };
  }
  const parts = raw.split(':');
  if (parts.length < 3) {
    return { mode: 'all', value: 'all' };
  }
  let key = '';
  let tagValue = '';
  try {
    key = decodeURIComponent(parts[1] || '').trim().toLowerCase();
    tagValue = decodeURIComponent(parts.slice(2).join(':') || '').trim().toLowerCase();
  } catch (_error) {
    return { mode: 'all', value: 'all' };
  }
  if (!key) {
    return { mode: 'all', value: 'all' };
  }
  const canonical = buildBillingTypeTagFilterValue(key, tagValue);
  return {
    mode: 'kv',
    value: canonical,
    key,
    tagValue,
    keyLower: key,
    tagValueLower: tagValue
  };
}

function buildBillingTypeTagFilterValue(key, tagValue) {
  const normalizedKey = String(key || '').trim().toLowerCase();
  const normalizedValue = String(tagValue || '').trim().toLowerCase();
  return `kv:${encodeURIComponent(normalizedKey)}:${encodeURIComponent(normalizedValue)}`;
}

function buildBillingTypeTagNullFilterValue(key) {
  const normalizedKey = String(key || '').trim().toLowerCase();
  return normalizedKey ? `key_null:${encodeURIComponent(normalizedKey)}` : 'all';
}

function parseBillingTypeSortValue(value) {
  const allowed = new Set([
    'amount_desc',
    'amount_asc',
    'untagged_desc',
    'type_asc',
    'type_desc',
    'provider_asc',
    'provider_desc'
  ]);
  const normalized = String(value || '').trim().toLowerCase();
  return allowed.has(normalized) ? normalized : 'amount_desc';
}

function parseBillingAccountSortValue(value) {
  const allowed = new Set([
    'provider_asc',
    'provider_desc',
    'account_asc',
    'account_desc',
    'total_desc',
    'total_asc',
    'budget_desc',
    'budget_asc'
  ]);
  const normalized = String(value || '').trim().toLowerCase();
  return allowed.has(normalized) ? normalized : 'provider_asc';
}

function sortBillingAccountRows(rows = [], sortValue = 'provider_asc') {
  const safeRows = Array.isArray(rows) ? rows.slice() : [];
  const normalizedSort = parseBillingAccountSortValue(sortValue);
  safeRows.sort((left, right) => {
    const providerLeft = String(left?.provider || '').trim().toLowerCase();
    const providerRight = String(right?.provider || '').trim().toLowerCase();
    const accountLeft = String(left?.accountName || '').trim().toLowerCase();
    const accountRight = String(right?.accountName || '').trim().toLowerCase();
    const totalLeft = Number(left?.totalAmount || 0);
    const totalRight = Number(right?.totalAmount || 0);
    const budgetLeft = Number(left?.budgetDelta || 0);
    const budgetRight = Number(right?.budgetDelta || 0);
    const accountIdLeft = String(left?.accountId || '').trim().toLowerCase();
    const accountIdRight = String(right?.accountId || '').trim().toLowerCase();

    const compareProvider = providerLeft.localeCompare(providerRight);
    const compareAccount = accountLeft.localeCompare(accountRight);
    const compareAccountId = accountIdLeft.localeCompare(accountIdRight);

    if (normalizedSort === 'provider_asc') {
      if (compareProvider !== 0) {
        return compareProvider;
      }
      if (totalRight !== totalLeft) {
        return totalRight - totalLeft;
      }
      return compareAccount || compareAccountId;
    }
    if (normalizedSort === 'provider_desc') {
      if (compareProvider !== 0) {
        return -compareProvider;
      }
      if (totalRight !== totalLeft) {
        return totalRight - totalLeft;
      }
      return compareAccount || compareAccountId;
    }
    if (normalizedSort === 'account_asc') {
      if (compareAccount !== 0) {
        return compareAccount;
      }
      if (compareProvider !== 0) {
        return compareProvider;
      }
      return compareAccountId;
    }
    if (normalizedSort === 'account_desc') {
      if (compareAccount !== 0) {
        return -compareAccount;
      }
      if (compareProvider !== 0) {
        return compareProvider;
      }
      return compareAccountId;
    }
    if (normalizedSort === 'total_desc') {
      if (totalRight !== totalLeft) {
        return totalRight - totalLeft;
      }
      if (compareProvider !== 0) {
        return compareProvider;
      }
      return compareAccount || compareAccountId;
    }
    if (normalizedSort === 'total_asc') {
      if (totalLeft !== totalRight) {
        return totalLeft - totalRight;
      }
      if (compareProvider !== 0) {
        return compareProvider;
      }
      return compareAccount || compareAccountId;
    }
    if (normalizedSort === 'budget_desc') {
      if (budgetRight !== budgetLeft) {
        return budgetRight - budgetLeft;
      }
      if (compareProvider !== 0) {
        return compareProvider;
      }
      return compareAccount || compareAccountId;
    }
    if (budgetLeft !== budgetRight) {
      return budgetLeft - budgetRight;
    }
    if (compareProvider !== 0) {
      return compareProvider;
    }
    return compareAccount || compareAccountId;
  });
  return safeRows;
}

function getBillingResourceRows() {
  return Array.isArray(state.billing.resourceBreakdownRows) ? state.billing.resourceBreakdownRows : [];
}

function renderBillingTypeProviderOptions() {
  if (!dom.billingTypeProviderFilter) {
    return;
  }
  const selectedProvider = getSelectedBillingProvider();
  if (selectedProvider) {
    state.billing.typeProviderFilter = selectedProvider;
    dom.billingTypeProviderFilter.innerHTML = `<option value="${escapeHtml(selectedProvider)}">${escapeHtml(
      getProviderDisplayLabel(selectedProvider, 'PROVIDER')
    )}</option>`;
    dom.billingTypeProviderFilter.value = selectedProvider;
    dom.billingTypeProviderFilter.disabled = true;
    return;
  }

  dom.billingTypeProviderFilter.disabled = false;
  const providers = Array.from(
    new Set(
      getBillingResourceRows()
        .map((row) => String(row?.provider || '').trim().toLowerCase())
        .filter(Boolean)
    )
  ).sort((left, right) => left.localeCompare(right));

  const current = parseBillingTypeFilterValue(state.billing.typeProviderFilter);
  const next = current === 'all' || providers.includes(current) ? current : 'all';
  state.billing.typeProviderFilter = next;

  dom.billingTypeProviderFilter.innerHTML = [
    '<option value="all">All providers</option>',
    ...providers.map(
      (provider) =>
        `<option value="${escapeHtml(provider)}">${escapeHtml(getProviderDisplayLabel(provider, 'PROVIDER'))}</option>`
    )
  ].join('');
  dom.billingTypeProviderFilter.value = next;
}

function renderBillingTypeResourceOptions() {
  if (!dom.billingTypeResourceFilter) {
    return;
  }
  const providerFilter = parseBillingTypeFilterValue(state.billing.typeProviderFilter);
  const rows = providerFilter === 'all'
    ? getBillingResourceRows()
    : getBillingResourceRows().filter((row) => String(row?.provider || '').trim().toLowerCase() === providerFilter);

  const types = Array.from(
    new Set(
      rows
        .map((row) => String(row?.resourceType || '').trim())
        .filter(Boolean)
    )
  ).sort((left, right) => left.localeCompare(right));

  const current = parseBillingTypeFilterValue(state.billing.typeResourceFilter);
  const normalizedTypes = types.map((type) => type.toLowerCase());
  const next = current === 'all' || normalizedTypes.includes(current) ? current : 'all';
  state.billing.typeResourceFilter = next;

  dom.billingTypeResourceFilter.innerHTML = [
    '<option value="all">All bill types</option>',
    ...types.map((type) => `<option value="${escapeHtml(type.toLowerCase())}">${escapeHtml(type)}</option>`)
  ].join('');
  dom.billingTypeResourceFilter.value = next;
}

function getBillingTagEntriesForResource(resourceRef) {
  const tagInfo = getCachedTagsForResource(resourceRef);
  if (!tagInfo?.tags || typeof tagInfo.tags !== 'object') {
    return [];
  }
  return Object.entries(tagInfo.tags)
    .map(([rawKey, rawValue]) => {
      const key = String(rawKey || '').trim();
      const tagValue = String(rawValue === null || rawValue === undefined ? '' : rawValue).trim();
      if (!key) {
        return null;
      }
      return {
        key,
        tagValue,
        keyLower: key.toLowerCase(),
        tagValueLower: tagValue.toLowerCase()
      };
    })
    .filter(Boolean);
}

function getBillingTagStatsForDetailRows(detailRows = []) {
  const rows = Array.isArray(detailRows) ? detailRows : [];
  let taggedCount = 0;
  let untaggedCount = 0;

  for (const detail of rows) {
    const resourceRef = String(detail?.resourceRef || '').trim();
    if (!resourceRef) {
      untaggedCount += 1;
      continue;
    }
    const entries = getBillingTagEntriesForResource(resourceRef);
    if (entries.length) {
      taggedCount += 1;
    } else {
      untaggedCount += 1;
    }
  }

  return {
    taggedCount,
    untaggedCount,
    totalCount: taggedCount + untaggedCount
  };
}

function filterBillingDetailRowsByTag(detailRows = [], tagFilter = { mode: 'all' }) {
  if (tagFilter.mode === 'all') {
    return Array.isArray(detailRows) ? detailRows : [];
  }

  const rows = Array.isArray(detailRows) ? detailRows : [];
  return rows.filter((detail) => {
    const resourceRef = String(detail?.resourceRef || '').trim();
    if (tagFilter.mode === 'untagged' && !resourceRef) {
      return true;
    }
    if (tagFilter.mode === 'key_null' && !resourceRef) {
      return true;
    }
    if (!resourceRef) {
      return false;
    }
    const entries = getBillingTagEntriesForResource(resourceRef);
    if (tagFilter.mode === 'tagged') {
      return entries.length > 0;
    }
    if (tagFilter.mode === 'untagged') {
      return entries.length === 0;
    }
    if (tagFilter.mode === 'key_null') {
      if (!entries.length) {
        return true;
      }
      const keyName = String(tagFilter.keyLower || '').trim().toLowerCase();
      if (!keyName) {
        return false;
      }
      const keyEntry = entries.find((entry) => entry.keyLower === keyName);
      if (!keyEntry) {
        return true;
      }
      const keyValue = String(keyEntry.tagValueLower || '').trim();
      return !keyValue || keyValue === 'null';
    }
    if (tagFilter.mode === 'kv') {
      if (!entries.length) {
        return false;
      }
      return entries.some(
        (entry) => entry.keyLower === tagFilter.keyLower && entry.tagValueLower === tagFilter.tagValueLower
      );
    }
    return false;
  });
}

function renderBillingTypeTagOptions() {
  if (!dom.billingTypeTagFilter) {
    return;
  }

  const providerFilter = parseBillingTypeFilterValue(state.billing.typeProviderFilter);
  const resourceTypeFilter = parseBillingTypeFilterValue(state.billing.typeResourceFilter);
  const detailMap = getBillingResourceDetailGroupMap();

  const tagPairs = new Map();
  const tagKeys = new Set();
  const rows = getBillingResourceRows().filter((row) => {
    const provider = String(row?.provider || '').trim().toLowerCase();
    const resourceType = String(row?.resourceType || '').trim().toLowerCase();
    const providerOk = providerFilter === 'all' || provider === providerFilter;
    const typeOk = resourceTypeFilter === 'all' || resourceType === resourceTypeFilter;
    return providerOk && typeOk;
  });

  for (const row of rows) {
    const groupKey = billingResourceGroupKey(row);
    const group = detailMap.get(groupKey);
    const detailRows = Array.isArray(group?.details) ? group.details : [];
    for (const detail of detailRows) {
      const resourceRef = String(detail?.resourceRef || '').trim();
      if (!resourceRef) {
        continue;
      }
      const entries = getBillingTagEntriesForResource(resourceRef);
      for (const entry of entries) {
        if (entry?.keyLower) {
          tagKeys.add(entry.keyLower);
        }
        const optionValue = buildBillingTypeTagFilterValue(entry.keyLower, entry.tagValueLower);
        if (!tagPairs.has(optionValue)) {
          tagPairs.set(optionValue, `${entry.keyLower}=${entry.tagValueLower}`);
        }
      }
    }
  }

  const current = parseBillingTypeTagFilterValue(state.billing.typeTagFilter);
  const nullTagOptions = Array.from(tagKeys)
    .sort((left, right) => left.localeCompare(right))
    .map((key) => [buildBillingTypeTagNullFilterValue(key), `${key} = null`]);
  if (
    current.mode === 'key_null' &&
    current.value !== 'org_null' &&
    current.keyLower &&
    !nullTagOptions.some(([value]) => value === current.value)
  ) {
    nullTagOptions.unshift([current.value, `${current.keyLower} = null`]);
  }
  const optionValues = new Set(['all', 'tagged', 'untagged', 'org_null', ...tagPairs.keys(), ...nullTagOptions.map((row) => row[0])]);
  const next = optionValues.has(current.value) ? current.value : 'all';
  state.billing.typeTagFilter = next;

  const tagOptions = Array.from(tagPairs.entries()).sort((left, right) =>
    String(left[1] || '').toLowerCase().localeCompare(String(right[1] || '').toLowerCase())
  );

  dom.billingTypeTagFilter.innerHTML = [
    '<option value="all">All tags</option>',
    '<option value="tagged">Tagged only</option>',
    '<option value="untagged">Untagged only</option>',
    '<option value="org_null">org = null</option>',
    ...nullTagOptions
      .filter(([value]) => value !== 'key_null:org')
      .map(([value, label]) => `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`),
    ...tagOptions.map(([value, label]) => `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`)
  ].join('');
  dom.billingTypeTagFilter.value = next;
}

function normalizeBillingSearchText(value) {
  return String(value || '').trim();
}

function tokenizeBillingSearchText(value) {
  return normalizeBillingSearchText(value)
    .toLowerCase()
    .split(/\s+/)
    .map((token) => token.trim())
    .filter(Boolean);
}

function billingTextIncludesAllTokens(text, tokens = []) {
  if (!tokens.length) {
    return true;
  }
  const haystack = String(text || '').toLowerCase();
  return tokens.every((token) => haystack.includes(token));
}

function buildBillingAccountNameMap() {
  const map = new Map();
  const rows = Array.isArray(state.billing.accounts) ? state.billing.accounts : [];
  for (const row of rows) {
    const provider = String(row?.provider || '').trim().toLowerCase();
    const accountId = String(row?.accountId || '').trim();
    if (!provider || !accountId) {
      continue;
    }
    const key = `${provider}::${accountId}`;
    const accountName = String(row?.accountName || '').trim();
    if (accountName && !map.has(key)) {
      map.set(key, accountName);
    }
  }
  return map;
}

function buildBillingDetailSearchText(detail, row, accountNameMap) {
  const provider = String(detail?.provider || row?.provider || '').trim().toLowerCase();
  const accountId = String(detail?.accountId || '').trim();
  const accountName = accountId ? accountNameMap.get(`${provider}::${accountId}`) || '' : '';
  const resourceRef = String(detail?.resourceRef || '').trim();
  const tagEntries = resourceRef ? getBillingTagEntriesForResource(resourceRef) : [];
  const tagText = tagEntries.map((entry) => `${entry.keyLower}=${entry.tagValueLower}`).join(' ');
  return [
    row?.provider,
    getProviderDisplayLabel(row?.provider, ''),
    row?.resourceType,
    detail?.detailName,
    detail?.itemType,
    detail?.sectionType,
    detail?.invoiceId,
    detail?.invoiceDate,
    detail?.coverageStartDate,
    detail?.coverageEndDate,
    detail?.accountId,
    accountName,
    detail?.resourceRef,
    tagText
  ]
    .map((value) => String(value || '').trim())
    .filter(Boolean)
    .join(' ');
}

function buildBillingRowSearchText(row, accountNameMap, detailRows = []) {
  const accountText = (Array.isArray(detailRows) ? detailRows : [])
    .map((detail) => {
      const provider = String(detail?.provider || row?.provider || '').trim().toLowerCase();
      const accountId = String(detail?.accountId || '').trim();
      if (!provider || !accountId) {
        return '';
      }
      return `${accountId} ${accountNameMap.get(`${provider}::${accountId}`) || ''}`.trim();
    })
    .filter(Boolean)
    .join(' ');
  return [
    row?.provider,
    getProviderDisplayLabel(row?.provider, ''),
    row?.resourceType,
    row?.currency,
    accountText
  ]
    .map((value) => String(value || '').trim())
    .filter(Boolean)
    .join(' ');
}

function getFilteredSortedBillingResourceRows() {
  const providerFilter = parseBillingTypeFilterValue(state.billing.typeProviderFilter);
  const resourceTypeFilter = parseBillingTypeFilterValue(state.billing.typeResourceFilter);
  const tagFilter = parseBillingTypeTagFilterValue(state.billing.typeTagFilter);
  const sortBy = parseBillingTypeSortValue(state.billing.typeSort);
  const searchTokens = tokenizeBillingSearchText(state.billing.searchText);
  const detailMap = getBillingResourceDetailGroupMap();
  const accountNameMap = buildBillingAccountNameMap();

  const filtered = getBillingResourceRows().filter((row) => {
    const provider = String(row?.provider || '').trim().toLowerCase();
    const resourceType = String(row?.resourceType || '').trim().toLowerCase();
    const providerOk = providerFilter === 'all' || provider === providerFilter;
    const typeOk = resourceTypeFilter === 'all' || resourceType === resourceTypeFilter;
    if (!providerOk || !typeOk) {
      return false;
    }
    if (tagFilter.mode === 'all') {
      return true;
    }
    const group = detailMap.get(billingResourceGroupKey(row));
    const detailRows = Array.isArray(group?.details) ? group.details : [];
    return filterBillingDetailRowsByTag(detailRows, tagFilter).length > 0;
  });

  const normalized = filtered.map((row) => {
    const group = detailMap.get(billingResourceGroupKey(row));
    const detailRows = Array.isArray(group?.details) ? group.details : [];
    const tagMatchedDetails =
      tagFilter.mode === 'all' ? detailRows : filterBillingDetailRowsByTag(detailRows, tagFilter);
    const rowSearchText = buildBillingRowSearchText(row, accountNameMap, tagMatchedDetails);
    const rowMatchesSearch = billingTextIncludesAllTokens(rowSearchText, searchTokens);
    const searchMatchedDetails = searchTokens.length
      ? tagMatchedDetails.filter((detail) =>
        billingTextIncludesAllTokens(buildBillingDetailSearchText(detail, row, accountNameMap), searchTokens)
      )
      : tagMatchedDetails;

    if (searchTokens.length && !rowMatchesSearch && !searchMatchedDetails.length) {
      return null;
    }

    let visibleDetails = tagMatchedDetails;
    if (searchTokens.length) {
      if (searchMatchedDetails.length) {
        visibleDetails = searchMatchedDetails;
      } else if (rowMatchesSearch) {
        visibleDetails = tagMatchedDetails;
      } else {
        visibleDetails = [];
      }
    }

    const tagStats = getBillingTagStatsForDetailRows(visibleDetails);
    const matchedAmount = visibleDetails.reduce((sum, detail) => {
      const amount = Number(detail?.amount || 0);
      return Number.isFinite(amount) ? sum + amount : sum;
    }, 0);

    return {
      ...row,
      totalAmount:
        (tagFilter.mode === 'all' && !searchTokens.length) || (rowMatchesSearch && !visibleDetails.length)
          ? Number(row?.totalAmount || 0)
          : Number.isFinite(matchedAmount)
            ? matchedAmount
            : 0,
      snapshotCount: visibleDetails.length || Number(row?.snapshotCount || 0),
      __matchedDetails: visibleDetails,
      __taggedCount: tagStats.taggedCount,
      __untaggedCount: tagStats.untaggedCount
    };
  }).filter(Boolean);

  if (tagFilter.mode !== 'all' || searchTokens.length) {
    const providerCurrencyTotals = new Map();
    for (const row of normalized) {
      const provider = String(row?.provider || '').trim().toLowerCase();
      const currency = String(row?.currency || 'USD').trim().toUpperCase() || 'USD';
      const totalAmount = Number(row?.totalAmount || 0);
      const key = `${provider}::${currency}`;
      providerCurrencyTotals.set(key, (providerCurrencyTotals.get(key) || 0) + totalAmount);
    }
    for (const row of normalized) {
      const provider = String(row?.provider || '').trim().toLowerCase();
      const currency = String(row?.currency || 'USD').trim().toUpperCase() || 'USD';
      const key = `${provider}::${currency}`;
      const providerTotal = Number(providerCurrencyTotals.get(key) || 0);
      row.sharePercent = providerTotal > 0 ? (Number(row?.totalAmount || 0) / providerTotal) * 100 : 0;
    }
  }

  const sorted = normalized.slice();
  sorted.sort((left, right) => {
    const leftAmount = Number(left?.totalAmount || 0);
    const rightAmount = Number(right?.totalAmount || 0);
    const leftUntaggedCount = Number(left?.__untaggedCount || 0);
    const rightUntaggedCount = Number(right?.__untaggedCount || 0);
    const leftProvider = String(left?.provider || '').toLowerCase();
    const rightProvider = String(right?.provider || '').toLowerCase();
    const leftType = String(left?.resourceType || '').toLowerCase();
    const rightType = String(right?.resourceType || '').toLowerCase();

    switch (sortBy) {
      case 'amount_asc':
        if (leftAmount !== rightAmount) {
          return leftAmount - rightAmount;
        }
        break;
      case 'amount_desc':
        if (leftAmount !== rightAmount) {
          return rightAmount - leftAmount;
        }
        break;
      case 'untagged_desc':
        if (leftUntaggedCount !== rightUntaggedCount) {
          return rightUntaggedCount - leftUntaggedCount;
        }
        if (leftAmount !== rightAmount) {
          return rightAmount - leftAmount;
        }
        break;
      case 'type_asc':
        if (leftType !== rightType) {
          return leftType.localeCompare(rightType);
        }
        break;
      case 'type_desc':
        if (leftType !== rightType) {
          return rightType.localeCompare(leftType);
        }
        break;
      case 'provider_asc':
        if (leftProvider !== rightProvider) {
          return leftProvider.localeCompare(rightProvider);
        }
        break;
      case 'provider_desc':
        if (leftProvider !== rightProvider) {
          return rightProvider.localeCompare(leftProvider);
        }
        break;
      default:
        break;
    }

    if (rightAmount !== leftAmount) {
      return rightAmount - leftAmount;
    }
    if (leftProvider !== rightProvider) {
      return leftProvider.localeCompare(rightProvider);
    }
    return leftType.localeCompare(rightType);
  });

  return sorted;
}

async function resolveTagsForDetailRows(detailRows = []) {
  const resources = detailRows
    .filter((row) => String(row?.resourceRef || '').trim())
    .map((row) => ({
      provider: row.provider,
      resourceRef: row.resourceRef,
      vendorId: row.vendorId || null,
      accountId: row.accountId || null
    }));

  if (!resources.length) {
    return;
  }

  const uniqueByRef = new Map();
  for (const item of resources) {
    const key = normalizeResourceRefKey(item.resourceRef);
    if (!key) {
      continue;
    }
    if (!uniqueByRef.has(key)) {
      uniqueByRef.set(key, item);
    }
  }
  const pending = Array.from(uniqueByRef.values()).filter((item) => {
    const key = normalizeResourceRefKey(item.resourceRef);
    return !state.billing.tagsByResourceRef[key];
  });
  if (!pending.length) {
    return;
  }

  const payload = await api('/api/tags/resolve', {
    method: 'POST',
    body: JSON.stringify({
      resources: pending
    })
  });
  const rows = Array.isArray(payload?.rows) ? payload.rows : [];
  for (const row of rows) {
    const key = normalizeResourceRefKey(row?.resourceRef);
    if (!key) {
      continue;
    }
    state.billing.tagsByResourceRef[key] = {
      tags: row?.tags || {},
      provider: row?.provider || '',
      vendorId: row?.vendorId || null,
      accountId: row?.accountId || null,
      source: row?.source || 'local',
      warning: row?.warning || null
    };
  }
}

function getCachedTagsForResource(resourceRef) {
  const key = normalizeResourceRefKey(resourceRef);
  if (!key) {
    return null;
  }
  return state.billing.tagsByResourceRef[key] || null;
}

function getBillingDetailGroupForRow(row) {
  const detailMap = getBillingResourceDetailGroupMap();
  return detailMap.get(billingResourceGroupKey(row)) || null;
}

function renderBillingDetailRows(group, groupKey, filteredDetailRows = null) {
  const detailRows = Array.isArray(filteredDetailRows)
    ? filteredDetailRows
    : Array.isArray(group?.details)
      ? group.details
      : [];
  if (!detailRows.length) {
    return `
      <tr class="billing-detail-row">
        <td colspan="9" class="muted">No detailed line items found for the current filters.</td>
      </tr>
    `;
  }

  return `
    <tr class="billing-detail-row" data-billing-detail-group="${escapeHtml(groupKey)}">
      <td colspan="7">
        <div class="billing-detail-wrap">
          <table class="billing-detail-table">
            <thead>
              <tr>
                <th>Line item</th>
                <th>Account</th>
                <th>Item type</th>
                <th>Coverage</th>
                <th>Invoice</th>
                <th>Resource reference</th>
                <th>Amount</th>
                <th>Tags</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              ${detailRows
      .map((detail) => {
        const resourceRef = String(detail?.resourceRef || '').trim();
        const tagInfo = resourceRef ? getCachedTagsForResource(resourceRef) : null;
        const coverageStart = String(detail?.coverageStartDate || '').trim().slice(0, 10);
        const coverageEnd = String(detail?.coverageEndDate || '').trim().slice(0, 10);
        const coverageText =
          coverageStart || coverageEnd ? `${coverageStart || '?'} to ${coverageEnd || '?'}` : '-';
        const invoiceDate = String(detail?.invoiceDate || '').trim().slice(0, 10);
        const invoiceId = String(detail?.invoiceId || '').trim();
        const invoiceText = invoiceId
          ? `${invoiceId}${invoiceDate ? ` (${invoiceDate})` : ''}`
          : invoiceDate || '-';
        return `
                    <tr>
                      <td>${escapeHtml(detail.detailName || '-')}</td>
                      <td>${escapeHtml(String(detail?.accountId || '-'))}</td>
                      <td>${escapeHtml(String(detail?.itemType || detail?.sectionType || '-'))}</td>
                      <td>${escapeHtml(coverageText)}</td>
                      <td>${escapeHtml(invoiceText)}</td>
                      <td class="billing-detail-resource-ref">${escapeHtml(resourceRef || '-')}</td>
                      <td>${escapeHtml(formatCurrency(detail.amount, detail.currency || group.currency || 'USD'))}</td>
                      <td>${tagInfo ? renderTagChips(tagInfo.tags || {}) : '<span class="muted">No tags</span>'}</td>
                      <td>
                        ${resourceRef
            ? `<button type="button" class="btn secondary tag-edit-btn" data-tag-edit-resource="${escapeHtml(
              resourceRef
            )}" data-tag-provider="${escapeHtml(detail.provider || group.provider)}" data-tag-vendor-id="${escapeHtml(
              detail.vendorId || ''
            )}" data-tag-account-id="${escapeHtml(detail.accountId || '')}">Edit tags</button>`
            : '<span class="muted">N/A</span>'
          }
                      </td>
                    </tr>
                  `;
      })
      .join('')}
            </tbody>
          </table>
        </div>
      </td>
    </tr>
  `;
}

function renderBillingResourceBreakdown() {
  if (!dom.billingResourceBody) {
    return;
  }

  if (dom.billingSearchInput && dom.billingSearchInput.value !== state.billing.searchText) {
    dom.billingSearchInput.value = state.billing.searchText;
  }

  renderBillingTypeProviderOptions();
  renderBillingTypeResourceOptions();
  renderBillingTypeTagOptions();
  if (dom.billingTypeSort) {
    const sortValue = parseBillingTypeSortValue(state.billing.typeSort);
    state.billing.typeSort = sortValue;
    dom.billingTypeSort.value = sortValue;
  }

  const rows = getFilteredSortedBillingResourceRows();
  const scopedProvider = getSelectedBillingProvider();
  const allowResourceTypeExport = Boolean(scopedProvider);
  if (!rows.length) {
    const searchText = normalizeBillingSearchText(state.billing.searchText);
    dom.billingResourceBody.innerHTML = `
      <tr>
        <td colspan="7" class="muted">${searchText
        ? `No bill types found for current filters and search: "${escapeHtml(searchText)}".`
        : 'No bill types found for the current filters.'
      }</td>
      </tr>
    `;
    return;
  }

  const expandedSet = getExpandedBillingGroupSet();
  const markup = [];
  for (const row of rows) {
    const groupKey = billingResourceGroupKey(row);
    const group = getBillingDetailGroupForRow(row);
    const visibleDetailRows = Array.isArray(row?.__matchedDetails)
      ? row.__matchedDetails
      : Array.isArray(group?.details)
        ? group.details
        : [];
    const detailCount = visibleDetailRows.length;
    const canExpand = detailCount > 0;
    const expanded = canExpand && expandedSet.has(groupKey);

    markup.push(`
      <tr data-billing-group-key="${escapeHtml(groupKey)}">
        <td class="billing-expand-cell">
          ${canExpand
        ? `<button type="button" class="billing-expand-btn" data-billing-toggle="${escapeHtml(groupKey)}">${expanded ? '-' : '+'
        }</button>`
        : ''
      }
        </td>
        <td>${escapeHtml(getProviderDisplayLabel(row.provider, 'PROVIDER'))}</td>
        <td>
          <div class="billing-resource-type-cell">
            <span>${escapeHtml(row.resourceType)}</span>
            ${allowResourceTypeExport && String(row.provider || '').trim().toLowerCase() === scopedProvider
        ? `<button type="button" class="btn secondary billing-resource-export-btn" data-billing-export-resource="${escapeHtml(
          row.resourceType
        )}">Export</button>`
        : ''
      }
          </div>
        </td>
        <td>${escapeHtml(formatCurrency(row.totalAmount, row.currency))}</td>
        <td>${escapeHtml(row.currency)}</td>
        <td>${escapeHtml(`${Number(row.sharePercent || 0).toFixed(1)}%`)}</td>
        <td>${escapeHtml(formatNumber(row.snapshotCount))}</td>
      </tr>
    `);

    if (expanded) {
      markup.push(renderBillingDetailRows(group, groupKey, visibleDetailRows));
    }
  }

  dom.billingResourceBody.innerHTML = markup.join('');
}

function setLivePinnedGroupName(groupName, options = {}) {
  const normalized = normalizeLivePinnedGroupName(groupName);
  state.live.pinnedGroupName = normalized;
  if (normalized) {
    state.live.selectedGroupName = normalized;
    saveLiveGroupName(normalized);
  }
  if (options.persist !== false) {
    saveLivePinnedGroupName(normalized);
  }
}

const STORAGE_PROVIDER_NAV_ITEMS = Object.freeze([
  { provider: 'unified', label: 'Unified', permission: 'storage-unified', meta: 'Cross-provider summary' },
  { provider: 'azure', label: 'Azure', permission: 'storage-azure', meta: 'Subscriptions + storage accounts' },
  { provider: 'aws', label: 'AWS', permission: 'storage-aws', meta: 'S3 + EFS + security' },
  { provider: 'gcp', label: 'GCP', permission: 'storage-gcp', meta: 'Cloud Storage scope' },
  { provider: 'wasabi', label: 'Wasabi', permission: 'storage-wasabi', meta: 'Wasabi accounts + buckets' },
  { provider: 'vsax', label: 'VSAx', permission: 'storage-vsax', meta: 'Group inventory + usage' },
  { provider: 'other', label: 'Other', permission: 'storage-other', meta: 'Future provider adapters' }
]);

function getVisibleStorageProviderNavItems() {
  const allowed = getAllowedViewSet();
  const providerScopedItems = STORAGE_PROVIDER_NAV_ITEMS.filter((item) => allowed.has(item.permission));
  if (providerScopedItems.length) {
    return providerScopedItems;
  }
  if (allowed.has('storage')) {
    return [...STORAGE_PROVIDER_NAV_ITEMS];
  }
  return [];
}

function setStorageEmbedProviderPreference(provider, options = {}) {
  const normalized = normalizeStorageProvider(provider);
  const persist = options.persist !== false;
  state.storage.selectedProvider = normalized;

  if (persist) {
    saveStorageProvider(normalized);
  }

  try {
    const storageProviderKey = 'cloudstudio.storage.activeProvider';
    const scopedStorageProviderKey = getUserScopedLocalStorageKey(storageProviderKey);
    if (scopedStorageProviderKey) {
      localStorage.setItem(scopedStorageProviderKey, normalized);
    }
  } catch (_error) {
    // Ignore storage write errors.
  }

  if (window.CLOUDSTUDIO_STORAGE_EMBED_API?.setProvider) {
    window.CLOUDSTUDIO_STORAGE_EMBED_API.setProvider(normalized);
  } else {
    window.dispatchEvent(
      new CustomEvent('cloudstudio-storage-provider-select', {
        detail: {
          provider: normalized
        }
      })
    );
  }

  renderStorageNavGroup();
}

function renderStorageNavGroup() {
  if (!dom.storageNavGroup) {
    return;
  }

  const items = getVisibleStorageProviderNavItems();
  if (!items.length) {
    dom.storageNavGroup.innerHTML = '';
    return;
  }

  const activeView = getActiveViewName();
  const storageViewActive = activeView === 'storage';
  const providerSet = new Set(items.map((item) => item.provider));
  if (!providerSet.has(state.storage.selectedProvider)) {
    state.storage.selectedProvider = items[0].provider;
  }

  dom.storageNavGroup.innerHTML = items
    .map((item) => {
      const active = storageViewActive && state.storage.selectedProvider === item.provider;
      return `
      <button type="button" class="billing-nav-item billing-nav-item-subtab ${active ? 'active' : ''}" data-storage-provider="${escapeHtml(
        item.provider
      )}">
        <span class="billing-nav-item-title">
          <span class="billing-nav-item-name">${escapeHtml(item.label)}</span>
        </span>
        <span class="billing-nav-item-meta">${escapeHtml(item.meta)}</span>
      </button>
    `;
    })
    .join('');
}

function renderLiveNavGroup() {
  if (!dom.liveNavGroup) {
    return;
  }

  const activeView = getActiveViewName();
  const liveViewActive = activeView === 'live';
  const pinnedGroupName = normalizeLivePinnedGroupName(state.live.pinnedGroupName);
  const lkDataCenterPinned = pinnedGroupName && pinnedGroupName.toLowerCase() === 'lkdatacenter';

  dom.liveNavGroup.innerHTML = `
    <button
      type="button"
      class="billing-nav-item billing-nav-item-subtab ${liveViewActive && lkDataCenterPinned ? 'active' : ''}"
      data-live-pinned-group="LKDataCenter"
    >
      <span class="billing-nav-item-title">
        <span class="billing-nav-item-name">LKDataCenter</span>
        <span class="billing-nav-item-total">Pinned</span>
      </span>
      <span class="billing-nav-item-meta">VSAx group only</span>
    </button>
  `;
}

function renderBillingNavGroup() {
  if (!dom.billingNavGroup) {
    return;
  }

  const activeView = getActiveViewName();
  const billingViewActive = activeView === 'billing';
  const budgetViewActive = activeView === 'billing-budget';
  const backfillViewActive = activeView === 'billing-backfill';
  const selectedScopeId = normalizeBillingScopeId(state.billing.selectedScopeId);
  const selectedProviderScopeId = resolveBillingProviderScopeId(selectedScopeId);
  const providerSummaries = buildBillingProviderSummaries();
  const allProvidersTotal = formatBillingTotalsByCurrency(state.billing.grandTotals);

  const buttons = [
    `
      <button type="button" class="billing-nav-item ${billingViewActive && selectedScopeId === 'all' ? 'active' : ''}" data-billing-scope="all">
        <span class="billing-nav-item-title">
          <span class="billing-nav-item-name">All providers</span>
          <span class="billing-nav-item-total">${escapeHtml(allProvidersTotal)}</span>
        </span>
        <span class="billing-nav-item-meta">Combined total for selected period</span>
      </button>
    `
  ];

  for (const providerSummary of providerSummaries) {
    const providerScopeId = `provider:${providerSummary.provider}`;
    buttons.push(`
      <button type="button" class="billing-nav-item billing-nav-item-provider ${billingViewActive && (selectedScopeId === providerScopeId || selectedProviderScopeId === providerScopeId) ? 'active' : ''}" data-billing-scope="${escapeHtml(providerScopeId)}">
        <span class="billing-nav-item-title">
          <span class="billing-nav-item-name">${escapeHtml(providerSummary.providerLabel)}</span>
          <span class="billing-nav-item-total">${escapeHtml(formatBillingTotalsByCurrency(providerSummary.totals))}</span>
        </span>
        <span class="billing-nav-item-meta">${escapeHtml(formatNumber(providerSummary.accountCount))} account(s)</span>
      </button>
    `);
  }

  buttons.push(`
    <button type="button" class="billing-nav-item billing-nav-item-subtab ${budgetViewActive ? 'active' : ''}" data-billing-open-view="billing-budget">
      <span class="billing-nav-item-title">
        <span class="billing-nav-item-name">Budget</span>
        <span class="billing-nav-item-total">Edit</span>
      </span>
      <span class="billing-nav-item-meta">Jan-Dec account budget plan</span>
    </button>
  `);

  buttons.push(`
    <button type="button" class="billing-nav-item billing-nav-item-subtab ${backfillViewActive ? 'active' : ''}" data-billing-open-view="billing-backfill">
      <span class="billing-nav-item-title">
        <span class="billing-nav-item-name">Backfill</span>
        <span class="billing-nav-item-total">Run</span>
      </span>
      <span class="billing-nav-item-meta">Historical billing pull</span>
    </button>
  `);

  dom.billingNavGroup.innerHTML = buttons.join('');
}

function summarizeBillingBudgetStatus(rows = []) {
  const summaryByCurrency = new Map();
  const accountRows = Array.isArray(rows) ? rows : [];

  for (const row of accountRows) {
    if (!row?.budgetConfigured) {
      continue;
    }
    const currency = String(row.budgetCurrency || row.currency || 'USD').trim().toUpperCase() || 'USD';
    const current = summaryByCurrency.get(currency) || {
      currency,
      actualAmount: 0,
      budgetAmount: 0,
      accountCount: 0
    };
    current.actualAmount += Number(row.totalAmount || 0);
    current.budgetAmount += Number(row.budgetAmount || 0);
    current.accountCount += 1;
    summaryByCurrency.set(currency, current);
  }

  return Array.from(summaryByCurrency.values()).map((row) => {
    const delta = Number(row.actualAmount || 0) - Number(row.budgetAmount || 0);
    let status = 'on-target';
    if (delta > 0.005) {
      status = 'over';
    } else if (delta < -0.005) {
      status = 'under';
    }
    return {
      ...row,
      delta,
      status
    };
  });
}

function renderBillingBudgetStatus(visibleAccountRows = []) {
  if (!dom.billingBudgetStatusCards) {
    return;
  }
  const periodStart = state.billing.period?.periodStart || '-';
  const periodEnd = state.billing.period?.periodEnd || '-';
  if (dom.billingBudgetStatusHint) {
    dom.billingBudgetStatusHint.textContent = `Under/over budget for selected period ${periodStart} to ${periodEnd}.`;
  }

  const summaries = summarizeBillingBudgetStatus(visibleAccountRows);
  if (!summaries.length) {
    dom.billingBudgetStatusCards.innerHTML = `
      <article class="card">
        <p class="label">Budget status</p>
        <p class="value">-</p>
        <p class="muted">No account budgets configured for this scope and period.</p>
      </article>
    `;
    return;
  }

  dom.billingBudgetStatusCards.innerHTML = summaries
    .map((summary) => {
      const deltaAbs = Math.abs(Number(summary.delta || 0));
      const arrow = summary.status === 'over' ? '↑' : summary.status === 'under' ? '↓' : '→';
      const statusText =
        summary.status === 'over'
          ? `Over budget by ${formatCurrency(deltaAbs, summary.currency)}`
          : summary.status === 'under'
            ? `Under budget by ${formatCurrency(deltaAbs, summary.currency)}`
            : 'On budget';
      return `
        <article class="card">
          <p class="label">${escapeHtml(summary.currency)} budget status</p>
          <p class="billing-budget-delta ${escapeHtml(summary.status)}">
            <span>${escapeHtml(arrow)}</span>
            <span>${escapeHtml(statusText)}</span>
          </p>
          <p class="billing-budget-detail">
            Actual ${escapeHtml(formatCurrency(summary.actualAmount, summary.currency))}
            | Budget ${escapeHtml(formatCurrency(summary.budgetAmount, summary.currency))}
            | ${escapeHtml(formatNumber(summary.accountCount))} account(s)
          </p>
        </article>
      `;
    })
    .join('');
}

function renderBillingAccountSummary() {
  if (!dom.billingAccountSummaryBody) {
    return;
  }

  const accountRows = Array.isArray(state.billing.accounts) ? state.billing.accounts : [];
  const periodStart = state.billing.period?.periodStart || '-';
  const periodEnd = state.billing.period?.periodEnd || '-';
  const lastMonth = getLastMonthRange();
  const isLastMonth = periodStart === lastMonth.periodStart && periodEnd === lastMonth.periodEnd;
  const providerSummaries = buildBillingProviderSummaries();
  const selectedScopeId = normalizeBillingScopeId(state.billing.selectedScopeId);
  const selectedProvider = getSelectedBillingProvider();
  const rawVisibleAccountRows = selectedProvider
    ? accountRows.filter((row) => String(row.provider || '').trim().toLowerCase() === selectedProvider)
    : accountRows;
  state.billing.accountSort = parseBillingAccountSortValue(state.billing.accountSort);
  if (dom.billingAccountSort) {
    dom.billingAccountSort.value = state.billing.accountSort;
  }
  const visibleAccountRows = sortBillingAccountRows(rawVisibleAccountRows, state.billing.accountSort);
  const visibleProviderSummaries = selectedProvider
    ? providerSummaries.filter((summary) => summary.provider === selectedProvider)
    : providerSummaries;
  const visibleGrandTotals = selectedProvider
    ? summarizeBillingTotalsByCurrency(visibleAccountRows)
    : state.billing.grandTotals;
  renderBillingBudgetStatus(visibleAccountRows);
  if (dom.billingSummaryTitle) {
    dom.billingSummaryTitle.textContent = isLastMonth
      ? `Provider/account totals (last month: ${periodStart} to ${periodEnd})`
      : `Provider/account totals (${periodStart} to ${periodEnd})`;
  }

  if (dom.billingTotalsCards) {
    const cards = [
      {
        label: 'Providers',
        value: formatNumber(visibleProviderSummaries.length)
      },
      {
        label: 'Accounts',
        value: formatNumber(visibleAccountRows.length)
      },
      {
        label: isLastMonth ? 'Last month total' : 'Selected total',
        value: formatBillingTotalsByCurrency(visibleGrandTotals)
      }
    ];
    for (const summary of visibleProviderSummaries) {
      if (!summary.totals.length && !summary.accountCount) {
        continue;
      }
      cards.push({
        label: `${summary.providerLabel} total`,
        value: formatBillingTotalsByCurrency(summary.totals)
      });
    }
    dom.billingTotalsCards.innerHTML = cards
      .map(
        (card) => `
        <article class="card">
          <p class="label">${escapeHtml(card.label)}</p>
          <p class="value">${escapeHtml(card.value)}</p>
        </article>
      `
      )
      .join('');
  }

  if (!accountRows.length) {
    dom.billingAccountSummaryBody.innerHTML = `
      <tr>
        <td colspan="7" class="muted">No provider/account billing totals found for this period.</td>
      </tr>
    `;
    return;
  }

  if (!visibleAccountRows.length) {
    dom.billingAccountSummaryBody.innerHTML = `
      <tr>
        <td colspan="7" class="muted">No accounts found for the selected provider.</td>
      </tr>
    `;
    return;
  }

  dom.billingAccountSummaryBody.innerHTML = visibleAccountRows
    .map((row) => {
      const selected = selectedScopeId === String(row.scopeId || '');
      const scopeId = String(row.scopeId || '').trim();
      const budgetStatusRaw = String(row.budgetStatus || '').trim().toLowerCase();
      const budgetConfigured = Boolean(row.budgetConfigured);
      const budgetDelta = Number(row.budgetDelta || 0);
      const budgetCurrency = String(row.budgetCurrency || row.currency || 'USD').trim().toUpperCase() || 'USD';
      const budgetDeltaAbs = Math.abs(budgetDelta);

      let budgetClass = 'unset';
      let budgetArrow = '•';
      let budgetHeadline = 'Budget not set';
      if (budgetConfigured) {
        if (budgetStatusRaw === 'over') {
          budgetClass = 'over';
          budgetArrow = '↑';
          budgetHeadline = `Over ${formatCurrency(budgetDeltaAbs, budgetCurrency)}`;
        } else if (budgetStatusRaw === 'under') {
          budgetClass = 'under';
          budgetArrow = '↓';
          budgetHeadline = `Under ${formatCurrency(budgetDeltaAbs, budgetCurrency)}`;
        } else {
          budgetClass = 'on-target';
          budgetArrow = '→';
          budgetHeadline = 'On target';
        }
      }

      const budgetDetail = budgetConfigured
        ? `Budget ${formatCurrency(row.budgetAmount || 0, budgetCurrency)}`
        : `Set account budget in Billing -> Budget`;

      return `
        <tr class="billing-account-row ${selected ? 'active-row' : ''}" ${scopeId ? `data-billing-account-scope="${escapeHtml(scopeId)}"` : ''}>
          <td>${escapeHtml(getProviderDisplayLabel(row.providerLabel || row.provider || '', 'PROVIDER'))}</td>
          <td>${escapeHtml(row.accountName || '-')}</td>
          <td>${escapeHtml(row.accountId || '-')}</td>
          <td>${escapeHtml(formatCurrency(row.totalAmount, row.currency))}</td>
          <td>
            <div class="billing-budget-delta billing-budget-delta-table ${escapeHtml(budgetClass)}">
              <span>${escapeHtml(budgetArrow)}</span>
              <span>${escapeHtml(budgetHeadline)}</span>
            </div>
            <div class="billing-budget-table-detail muted">${escapeHtml(budgetDetail)}</div>
          </td>
          <td>${escapeHtml(row.currency || 'USD')}</td>
          <td>${escapeHtml(formatNumber(row.snapshotCount || 0))}</td>
        </tr>
      `;
    })
    .join('');
}

async function loadBillingAccounts(range = null) {
  const query = new URLSearchParams();
  if (range?.periodStart && range?.periodEnd) {
    query.set('periodStart', range.periodStart);
    query.set('periodEnd', range.periodEnd);
  }

  const suffix = query.toString();
  const payload = await api(suffix ? `/api/billing/accounts?${suffix}` : '/api/billing/accounts');
  state.billing.accounts = Array.isArray(payload.accounts) ? payload.accounts : [];
  state.billing.providerTotals = Array.isArray(payload.providerTotals) ? payload.providerTotals : [];
  state.billing.grandTotals = Array.isArray(payload.grandTotals) ? payload.grandTotals : [];
  state.billing.period = payload.period || range || null;

  const providerScopesFromAccounts = state.billing.accounts
    .map((row) => String(row.provider || '').trim().toLowerCase())
    .filter(Boolean)
    .map((provider) => `provider:${provider}`);
  const providerScopesFromTotals = state.billing.providerTotals
    .map((row) => String(row.provider || '').trim().toLowerCase())
    .filter(Boolean)
    .map((provider) => `provider:${provider}`);
  const accountScopes = state.billing.accounts
    .map((row) => String(row.scopeId || '').trim())
    .filter(Boolean);
  const availableScopes = new Set(['all', ...providerScopesFromAccounts, ...providerScopesFromTotals, ...accountScopes]);
  const currentScope = normalizeBillingScopeId(state.billing.selectedScopeId);
  if (!availableScopes.has(currentScope)) {
    state.billing.selectedScopeId = 'all';
  }

  renderBillingNavGroup();
  renderBillingAccountSummary();
}

function setBillingScope(scopeId, options = {}) {
  const normalized = normalizeBillingScopeId(scopeId);
  state.billing.selectedScopeId = normalized;
  const scopedProvider = getSelectedBillingProvider();
  state.billing.typeProviderFilter = scopedProvider || 'all';
  state.billing.typeResourceFilter = 'all';
  state.billing.typeTagFilter = 'all';
  if (options.persist !== false) {
    saveBillingScope(normalized);
  }
  renderBillingNavGroup();
  renderBillingAccountSummary();
}

function normalizeBillingBudgetYear(value) {
  const numeric = Number(value);
  if (!Number.isInteger(numeric) || numeric < 2000 || numeric > 9999) {
    return new Date().getUTCFullYear();
  }
  return numeric;
}

function setBillingBudgetStatus(message, isError = false) {
  if (!dom.billingBudgetStatus) {
    return;
  }
  dom.billingBudgetStatus.textContent = String(message || '').trim();
  dom.billingBudgetStatus.classList.toggle('negative', Boolean(isError && message));
}

function getBudgetMonthsTemplate() {
  return {
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
  };
}

function normalizeBillingBudgetRows(rows = []) {
  return (Array.isArray(rows) ? rows : []).map((row) => {
    const months = getBudgetMonthsTemplate();
    const sourceMonths = row?.months && typeof row.months === 'object' ? row.months : {};
    for (let month = 1; month <= 12; month += 1) {
      const rawValue = Object.prototype.hasOwnProperty.call(sourceMonths, String(month))
        ? sourceMonths[String(month)]
        : sourceMonths[month];
      if (rawValue === '' || rawValue === null || rawValue === undefined) {
        continue;
      }
      const numeric = Number(rawValue);
      if (!Number.isFinite(numeric) || numeric < 0) {
        continue;
      }
      months[month] = numeric;
    }
    return {
      scopeId: String(row?.scopeId || '').trim(),
      vendorId: String(row?.vendorId || '').trim() || null,
      provider: String(row?.provider || '').trim().toLowerCase(),
      providerLabel: String(row?.providerLabel || '').trim() || '',
      accountId: String(row?.accountId || '').trim() || null,
      accountName: String(row?.accountName || '').trim() || 'Account',
      currency: String(row?.currency || 'USD').trim().toUpperCase() || 'USD',
      months
    };
  });
}

function calculateBillingBudgetAnnualTotal(months = {}) {
  let total = 0;
  for (let month = 1; month <= 12; month += 1) {
    const numeric = Number(months?.[month]);
    if (!Number.isFinite(numeric) || numeric < 0) {
      continue;
    }
    total += numeric;
  }
  return total;
}

function renderBillingBudgetTable() {
  if (!dom.billingBudgetBody) {
    return;
  }
  const rows = Array.isArray(state.billing.budgetRows) ? state.billing.budgetRows : [];
  if (!rows.length) {
    dom.billingBudgetBody.innerHTML = `
      <tr>
        <td colspan="17" class="muted">No billing accounts found. Add billing vendors first.</td>
      </tr>
    `;
    return;
  }

  const monthColumns = Array.from({ length: 12 }, (_, index) => index + 1);
  dom.billingBudgetBody.innerHTML = rows
    .map((row) => {
      const providerLabel = row.providerLabel || getProviderDisplayLabel(row.provider, 'PROVIDER');
      const annualTotal = calculateBillingBudgetAnnualTotal(row.months);
      const monthCells = monthColumns
        .map((month) => {
          const numeric = Number(row.months?.[month]);
          const value = Number.isFinite(numeric) && numeric >= 0 ? numeric.toFixed(2) : '';
          return `
            <td>
              <input
                type="number"
                min="0"
                step="0.01"
                inputmode="decimal"
                value="${escapeHtml(value)}"
                data-budget-input="true"
                data-budget-scope="${escapeHtml(row.scopeId)}"
                data-budget-month="${month}"
              />
            </td>
          `;
        })
        .join('');

      return `
        <tr data-budget-row="${escapeHtml(row.scopeId)}" data-budget-currency="${escapeHtml(row.currency)}">
          <td>${escapeHtml(providerLabel)}</td>
          <td>${escapeHtml(row.accountName || '-')}</td>
          <td>${escapeHtml(row.accountId || '-')}</td>
          <td>${escapeHtml(row.currency || 'USD')}</td>
          ${monthCells}
          <td class="billing-budget-annual" data-budget-annual-cell="true">${escapeHtml(
        formatCurrency(annualTotal, row.currency)
      )}</td>
        </tr>
      `;
    })
    .join('');
}

function updateBillingBudgetAnnualForRow(rowElement) {
  if (!(rowElement instanceof HTMLElement)) {
    return;
  }
  const currency = String(rowElement.dataset.budgetCurrency || 'USD').trim().toUpperCase() || 'USD';
  const inputs = Array.from(rowElement.querySelectorAll('input[data-budget-input]'));
  let total = 0;
  for (const input of inputs) {
    if (!(input instanceof HTMLInputElement)) {
      continue;
    }
    const numeric = Number(input.value);
    if (!Number.isFinite(numeric) || numeric < 0) {
      continue;
    }
    total += numeric;
  }
  const annualCell = rowElement.querySelector('[data-budget-annual-cell]');
  if (annualCell instanceof HTMLElement) {
    annualCell.textContent = formatCurrency(total, currency);
  }
}

function handleBillingBudgetInput(event) {
  const target = event.target;
  if (!(target instanceof HTMLInputElement)) {
    return;
  }
  if (!target.matches('input[data-budget-input]')) {
    return;
  }
  const scopeId = String(target.dataset.budgetScope || '').trim();
  const month = Number(target.dataset.budgetMonth || 0);
  if (!scopeId || !Number.isInteger(month) || month < 1 || month > 12) {
    return;
  }
  const row = (Array.isArray(state.billing.budgetRows) ? state.billing.budgetRows : []).find(
    (entry) => String(entry?.scopeId || '').trim() === scopeId
  );
  if (!row) {
    return;
  }
  const raw = String(target.value || '').trim();
  if (!raw) {
    row.months[month] = null;
  } else {
    const numeric = Number(raw);
    if (!Number.isFinite(numeric) || numeric < 0) {
      return;
    }
    row.months[month] = numeric;
  }
  const rowElement = target.closest('[data-budget-row]');
  updateBillingBudgetAnnualForRow(rowElement);
}

async function loadBillingBudget(options = {}) {
  if (!hasClientViewAccess('billing')) {
    return;
  }
  const requestedYear = options?.year ?? dom.billingBudgetYear?.value ?? state.billing.budgetYear;
  const year = normalizeBillingBudgetYear(requestedYear);
  state.billing.budgetYear = year;
  if (dom.billingBudgetYear) {
    dom.billingBudgetYear.value = String(year);
  }
  setBillingBudgetStatus(`Loading account budgets for ${year}...`);
  const payload = await api(`/api/billing/budgets/accounts?year=${encodeURIComponent(String(year))}`);
  state.billing.budgetYear = normalizeBillingBudgetYear(payload?.year || year);
  state.billing.budgetRows = normalizeBillingBudgetRows(payload?.rows || []);
  state.billing.budgetSupportedTargets = Array.isArray(payload?.supportedTargetTypes)
    ? payload.supportedTargetTypes
    : ['account', 'org', 'product'];
  if (dom.billingBudgetYear) {
    dom.billingBudgetYear.value = String(state.billing.budgetYear);
  }
  renderBillingBudgetTable();
  setBillingBudgetStatus(
    `Loaded ${formatNumber(state.billing.budgetRows.length)} account budget row(s) for ${state.billing.budgetYear}.`
  );
}

async function saveBillingBudget() {
  if (!hasClientViewAccess('billing')) {
    return;
  }
  const year = normalizeBillingBudgetYear(dom.billingBudgetYear?.value || state.billing.budgetYear);
  state.billing.budgetYear = year;
  if (dom.billingBudgetYear) {
    dom.billingBudgetYear.value = String(year);
  }

  const budgets = (Array.isArray(state.billing.budgetRows) ? state.billing.budgetRows : []).map((row) => {
    const months = {};
    for (let month = 1; month <= 12; month += 1) {
      const numeric = Number(row?.months?.[month]);
      if (!Number.isFinite(numeric) || numeric < 0) {
        continue;
      }
      months[month] = numeric;
    }
    return {
      scopeId: row.scopeId,
      vendorId: row.vendorId || null,
      provider: row.provider || null,
      accountId: row.accountId || null,
      accountName: row.accountName || null,
      currency: row.currency || 'USD',
      months
    };
  });

  setBillingBudgetStatus(`Saving account budgets for ${year}...`);
  const payload = await api('/api/billing/budgets/accounts/save', {
    method: 'POST',
    body: JSON.stringify({
      year,
      budgets
    })
  });
  state.billing.budgetRows = normalizeBillingBudgetRows(payload?.rows || []);
  renderBillingBudgetTable();
  setBillingBudgetStatus(`Saved ${formatNumber(payload?.savedEntries || 0)} budget row(s) for ${year}.`);
  showToast('Billing budgets saved.');

  const range = readBillingRangeFromInputs();
  await loadBillingAccounts(range);
}

function buildTimestampFileSuffix() {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, '0');
  const dd = String(now.getDate()).padStart(2, '0');
  const hh = String(now.getHours()).padStart(2, '0');
  const min = String(now.getMinutes()).padStart(2, '0');
  return `${yyyy}${mm}${dd}-${hh}${min}`;
}

function sanitizeFileSegment(value, fallback = 'vendor') {
  const cleaned = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return cleaned || fallback;
}

function downloadJsonFile(filename, payload) {
  const blob = new Blob([`${JSON.stringify(payload, null, 2)}\n`], { type: 'application/json;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 500);
}

async function readJsonFileFromInput(inputElement) {
  if (!(inputElement instanceof HTMLInputElement)) {
    throw new Error('File input is not available.');
  }
  const file = inputElement.files?.[0];
  if (!file) {
    throw new Error('Select a JSON file first.');
  }
  const text = await file.text();
  if (!text.trim()) {
    throw new Error('Selected file is empty.');
  }
  try {
    return JSON.parse(text);
  } catch (_error) {
    throw new Error('Selected file is not valid JSON.');
  }
}

function setAppConfigStatus(message, isError = false) {
  if (!dom.appConfigStatus) {
    return;
  }
  dom.appConfigStatus.textContent = String(message || '').trim();
  dom.appConfigStatus.classList.toggle('negative', Boolean(isError && message));
}

function normalizeEnvOverridesMap(input) {
  const source = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
  const output = {};
  Object.entries(source).forEach(([rawKey, rawValue]) => {
    const key = String(rawKey || '')
      .trim()
      .toUpperCase();
    if (!key) {
      return;
    }
    const value = String(rawValue === null || rawValue === undefined ? '' : rawValue).trim();
    if (!value) {
      return;
    }
    output[key] = value;
  });
  return Object.fromEntries(Object.entries(output).sort(([left], [right]) => left.localeCompare(right)));
}

function formatEnvOverridesForTextarea(input) {
  const rows = [];
  const normalized = normalizeEnvOverridesMap(input);
  const serializeValue = (value) => {
    const text = String(value === null || value === undefined ? '' : value);
    if (!text.includes('\n')) {
      return text;
    }
    const trimmed = text.trim();
    if (
      (trimmed.startsWith("'") && trimmed.endsWith("'")) ||
      (trimmed.startsWith('"') && trimmed.endsWith('"'))
    ) {
      return trimmed;
    }
    return `'${text}'`;
  };
  Object.entries(normalized).forEach(([key, value]) => {
    rows.push(`${key}=${serializeValue(value)}`);
  });
  return rows.join('\n');
}

function parseEnvOverridesFromTextarea(rawText) {
  const lines = String(rawText || '').split(/\r?\n/);
  const output = {};
  const keyPattern = /^([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$/;

  let currentKey = '';
  let currentValueLines = [];
  let multilineQuote = '';

  function startsMultilineQuote(value) {
    const text = String(value || '').trimStart();
    const first = text.charAt(0);
    if (first !== "'" && first !== '"') {
      return '';
    }

    let escaped = false;
    for (let index = 1; index < text.length; index += 1) {
      const char = text.charAt(index);
      if (escaped) {
        escaped = false;
        continue;
      }
      if (char === '\\') {
        escaped = true;
        continue;
      }
      if (char === first) {
        return '';
      }
    }
    return first;
  }

  function closesMultilineQuote(line, quote) {
    if (!quote) {
      return false;
    }
    const text = String(line || '');
    let index = text.length - 1;
    while (index >= 0 && /\s/.test(text.charAt(index))) {
      index -= 1;
    }
    if (index < 0 || text.charAt(index) !== quote) {
      return false;
    }

    let slashCount = 0;
    let backtrack = index - 1;
    while (backtrack >= 0 && text.charAt(backtrack) === '\\') {
      slashCount += 1;
      backtrack -= 1;
    }
    return slashCount % 2 === 0;
  }

  function flushCurrent() {
    if (!currentKey) {
      return;
    }
    output[currentKey] = currentValueLines.join('\n');
    currentKey = '';
    currentValueLines = [];
    multilineQuote = '';
  }

  lines.forEach((rawLine) => {
    const line = String(rawLine || '');
    const trimmed = line.trim();
    if (!currentKey && (!trimmed || trimmed.startsWith('#'))) {
      return;
    }

    if (currentKey && multilineQuote) {
      currentValueLines.push(line);
      if (closesMultilineQuote(line, multilineQuote)) {
        flushCurrent();
      }
      return;
    }

    const match = line.match(keyPattern);
    if (match) {
      flushCurrent();
      currentKey = String(match[1] || '').trim().toUpperCase();
      const initialValue = String(match[2] || '');
      currentValueLines = [initialValue];
      multilineQuote = startsMultilineQuote(initialValue);
      return;
    }

    if (currentKey) {
      if (!trimmed || trimmed.startsWith('#')) {
        return;
      }
      currentValueLines.push(line);
    }
  });

  flushCurrent();
  return normalizeEnvOverridesMap(output);
}

function normalizeRuntimeConfigPayload(input = {}) {
  const runtimeConfig = input && typeof input === 'object' ? input : {};
  const branding = runtimeConfig.branding && typeof runtimeConfig.branding === 'object' ? runtimeConfig.branding : {};
  return {
    branding: {
      login: {
        name: String(branding?.login?.name || '').trim() || 'CloudStudio',
        initials: String(branding?.login?.initials || '').trim().toUpperCase() || 'CS'
      },
      main: {
        name: String(branding?.main?.name || '').trim() || 'CloudStudio',
        initials: String(branding?.main?.initials || '').trim().toUpperCase() || 'CS'
      }
    },
    envOverrides: normalizeEnvOverridesMap(runtimeConfig.envOverrides || runtimeConfig.env || {})
  };
}

function applyAppConfigToForm(runtimeConfig = {}) {
  const normalized = normalizeRuntimeConfigPayload(runtimeConfig);
  state.admin.runtimeConfig = normalized;
  if (dom.appConfigLoginBrandName) dom.appConfigLoginBrandName.value = normalized.branding.login.name;
  if (dom.appConfigLoginBrandInitials) dom.appConfigLoginBrandInitials.value = normalized.branding.login.initials;
  if (dom.appConfigMainBrandName) dom.appConfigMainBrandName.value = normalized.branding.main.name;
  if (dom.appConfigMainBrandInitials) dom.appConfigMainBrandInitials.value = normalized.branding.main.initials;
  if (dom.appConfigEnvOverrides) dom.appConfigEnvOverrides.value = formatEnvOverridesForTextarea(normalized.envOverrides);
}

function buildRuntimeConfigPayloadFromForm() {
  return normalizeRuntimeConfigPayload({
    branding: {
      login: {
        name: dom.appConfigLoginBrandName?.value,
        initials: dom.appConfigLoginBrandInitials?.value
      },
      main: {
        name: dom.appConfigMainBrandName?.value,
        initials: dom.appConfigMainBrandInitials?.value
      }
    },
    envOverrides: parseEnvOverridesFromTextarea(dom.appConfigEnvOverrides?.value || '')
  });
}

async function loadAppConfig() {
  if (!hasClientViewAccess('admin-settings')) {
    return;
  }
  const payload = await api('/api/admin/app-config');
  applyAppConfigToForm(payload?.runtimeConfig || {});
  setAppConfigStatus('Loaded app config from database.');
}

async function saveAppConfig(event) {
  event?.preventDefault?.();
  if (!hasClientViewAccess('admin-settings')) {
    return;
  }
  setAppConfigStatus('Saving app config...');
  const runtimeConfig = buildRuntimeConfigPayloadFromForm();
  const payload = await api('/api/admin/app-config', {
    method: 'PUT',
    body: JSON.stringify({ runtimeConfig })
  });
  applyAppConfigToForm(payload?.runtimeConfig || runtimeConfig);
  applyMainBranding(payload?.runtimeConfig?.branding?.main || runtimeConfig?.branding?.main || {});
  await loadServices();
  setAppConfigStatus('App config saved and reloaded.');
  showToast('App config saved.');
}

async function exportAppConfig() {
  if (!hasClientViewAccess('admin-settings')) {
    return;
  }
  const payload = await api('/api/admin/app-config/export');
  const suffix = buildTimestampFileSuffix();
  downloadJsonFile(`cloudstudio-app-config-${suffix}.json`, payload);
  showToast('App config exported.');
}

function openImportAppConfigPicker() {
  if (!dom.importAppConfigInput) {
    return;
  }
  dom.importAppConfigInput.value = '';
  dom.importAppConfigInput.click();
}

async function importAppConfigFromFile() {
  if (!hasClientViewAccess('admin-settings')) {
    return;
  }
  const imported = await readJsonFileFromInput(dom.importAppConfigInput);
  setAppConfigStatus('Importing app config...');
  const result = await api('/api/admin/app-config/import', {
    method: 'POST',
    body: JSON.stringify({ payload: imported })
  });
  applyAppConfigToForm(result?.runtimeConfig || {});
  applyMainBranding(result?.runtimeConfig?.branding?.main || {});
  await Promise.all([loadServices(), loadVendors(), loadDbBackupConfig()]);
  await refreshBillingAfterVendorChange();
  const created = Number(result?.vendorImport?.createdCount || 0);
  const updated = Number(result?.vendorImport?.updatedCount || 0);
  const failed = Number(result?.vendorImport?.failedCount || 0);
  const vendorSummary =
    result?.vendorImport && (created > 0 || updated > 0 || failed > 0)
      ? ` Vendors: created ${formatNumber(created)}, updated ${formatNumber(updated)}, failed ${formatNumber(failed)}.`
      : '';
  setAppConfigStatus(`App config import completed.${vendorSummary}`, failed > 0);
  showToast(failed > 0 ? 'App config imported with some vendor failures.' : 'App config imported.', failed > 0);
}

async function refreshBillingAfterVendorChange() {
  try {
    ensureBillingRangeDefaults();
    await loadBillingAccounts(readBillingRangeFromInputs());
  } catch (_error) {
    // Ignore billing nav refresh failures on vendor updates.
  }
}

async function exportAllVendors() {
  const payload = await api('/api/vendors/export');
  const suffix = buildTimestampFileSuffix();
  downloadJsonFile(`cloudstudio-vendors-${suffix}.json`, payload);
  showToast(`Exported ${formatNumber(payload?.total || 0)} vendor config(s).`);
}

async function exportSingleVendor(vendorId) {
  const payload = await api(`/api/vendors/${encodeURIComponent(vendorId)}/export`);
  const vendor = payload?.vendor || {};
  const suffix = buildTimestampFileSuffix();
  const provider = sanitizeFileSegment(vendor.provider, 'provider');
  const name = sanitizeFileSegment(vendor.name, 'vendor');
  downloadJsonFile(`cloudstudio-vendor-${provider}-${name}-${suffix}.json`, payload);
  showToast(`Exported vendor ${vendor.name || vendorId}.`);
}

function openImportAllVendorsPicker() {
  if (!dom.importAllVendorsInput) {
    return;
  }
  dom.importAllVendorsInput.value = '';
  dom.importAllVendorsInput.click();
}

function openImportSingleVendorPicker(vendorId) {
  if (!dom.importSingleVendorInput) {
    return;
  }
  state.vendors.importTargetId = String(vendorId || '').trim() || null;
  dom.importSingleVendorInput.value = '';
  dom.importSingleVendorInput.click();
}

async function importAllVendorsFromFile() {
  const payload = await readJsonFileFromInput(dom.importAllVendorsInput);
  const result = await api('/api/vendors/import', {
    method: 'POST',
    body: JSON.stringify({ payload })
  });
  showToast(
    `Vendor import complete. Created ${formatNumber(result?.createdCount || 0)}, updated ${formatNumber(
      result?.updatedCount || 0
    )}, failed ${formatNumber(result?.failedCount || 0)}.`,
    Boolean(result?.failedCount)
  );
  await loadVendors();
  await refreshBillingAfterVendorChange();
}

async function importSingleVendorFromFile() {
  const vendorId = String(state.vendors.importTargetId || '').trim();
  if (!vendorId) {
    throw new Error('No target vendor selected for import.');
  }
  try {
    const payload = await readJsonFileFromInput(dom.importSingleVendorInput);
    const result = await api(`/api/vendors/${encodeURIComponent(vendorId)}/import`, {
      method: 'POST',
      body: JSON.stringify({ payload })
    });
    const vendorName = result?.vendor?.name || vendorId;
    showToast(`Vendor ${vendorName} imported.`);
    await loadVendors();
    await refreshBillingAfterVendorChange();
  } finally {
    state.vendors.importTargetId = null;
  }
}

async function loadVendors() {
  const payload = await api('/api/vendors');
  const body = document.getElementById('vendorsBody');
  const vendors = (Array.isArray(payload.vendors) ? payload.vendors : []).map((vendor) => ({
    ...vendor,
    hidden: isVendorHiddenInClient(vendor)
  }));
  state.vendorProviders = Array.from(
    new Set(
      vendors
        .map((vendor) => String(vendor?.provider || '').trim().toLowerCase())
        .filter(Boolean)
    )
  ).sort((left, right) => left.localeCompare(right));
  body.innerHTML = vendors
    .map(
      (vendor) => `
      <tr data-vendor-id="${vendor.id}" class="${vendor.hidden ? 'vendor-hidden-row' : ''}">
        <td>${escapeHtml(vendor.name || '')}</td>
        <td>${escapeHtml(getProviderDisplayLabel(vendor.provider, vendor.provider || 'Provider'))}</td>
        <td>${escapeHtml(vendor.cloudType || '-')}</td>
        <td>${escapeHtml(vendor.authMethod || '-')}</td>
        <td>${vendor.hasCredentials ? 'configured' : 'none'}</td>
        <td>${vendor.hidden ? '<span class="vendor-visibility-hidden">Hidden</span>' : '<span class="vendor-visibility-visible">Visible</span>'}</td>
        <td>
          <button class="btn secondary js-test-vendor" data-vendor-id="${vendor.id}">Test</button>
          <button class="btn secondary js-export-vendor" data-vendor-id="${vendor.id}">Export</button>
          <button class="btn secondary js-import-vendor" data-vendor-id="${vendor.id}">Import</button>
          <button class="btn secondary js-toggle-vendor-hidden" data-vendor-id="${vendor.id}" data-vendor-hidden="${vendor.hidden ? '1' : '0'
        }">${vendor.hidden ? 'Unhide' : 'Hide'}</button>
          <button class="btn danger js-delete-vendor" data-vendor-id="${vendor.id}">Delete</button>
        </td>
      </tr>
    `
    )
    .join('');

  renderBillingNavGroup();
  renderBillingAccountSummary();
}

async function submitVendorForm(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const fd = new FormData(form);

  let credentials = null;
  try {
    credentials = parseCredentialsInput(fd.get('credentials'));
  } catch (error) {
    showToast(`Credentials JSON is invalid: ${error.message}`, true);
    return;
  }

  const provider = String(fd.get('provider') || 'other');
  const authDefaults = {
    azure: 'service_principal',
    aws: 'api_key',
    gcp: 'service_account_or_export',
    'grafana-cloud': 'api_key',
    sendgrid: 'api_key',
    rackspace: 'api_key',
    private: 'private_api',
    wasabi: 'api_key',
    'wasabi-main': 'api_key',
    other: 'custom'
  };

  const payload = {
    name: fd.get('name'),
    provider,
    cloudType: fd.get('cloudType'),
    authMethod: String(fd.get('authMethod') || '').trim() || authDefaults[provider] || 'unknown',
    accountId: fd.get('accountId'),
    projectId: fd.get('projectId'),
    credentials,
    metadata: {
      source: 'cloudstudio-ui'
    }
  };

  await api('/api/vendors', {
    method: 'POST',
    body: JSON.stringify(payload)
  });

  form.reset();
  showToast('Vendor saved.');
  await loadVendors();
  await refreshBillingAfterVendorChange();
}

async function handleVendorTableClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }

  const vendorId = target.dataset.vendorId;
  if (!vendorId) {
    return;
  }

  if (target.classList.contains('js-delete-vendor')) {
    if (!window.confirm('Delete this vendor configuration?')) {
      return;
    }

    await api(`/api/vendors/${vendorId}`, { method: 'DELETE' });
    showToast('Vendor deleted.');
    await loadVendors();
    await refreshBillingAfterVendorChange();
    return;
  }

  if (target.classList.contains('js-export-vendor')) {
    await exportSingleVendor(vendorId);
    return;
  }

  if (target.classList.contains('js-import-vendor')) {
    openImportSingleVendorPicker(vendorId);
    return;
  }

  if (target.classList.contains('js-test-vendor')) {
    const result = await api(`/api/vendors/${vendorId}/test`, { method: 'POST' });
    showToast(`Test ok: ${result.provider} ${formatCurrency(result.preview.amount, result.preview.currency)}`);
    return;
  }

  if (target.classList.contains('js-toggle-vendor-hidden')) {
    const currentlyHidden = String(target.dataset.vendorHidden || '').trim() === '1';
    const nextHidden = !currentlyHidden;
    await api(`/api/vendors/${encodeURIComponent(vendorId)}`, {
      method: 'PUT',
      body: JSON.stringify({
        hidden: nextHidden
      })
    });
    showToast(nextHidden ? 'Vendor hidden.' : 'Vendor unhidden.');
    await loadVendors();
    await refreshBillingAfterVendorChange();
  }
}

function setAdminUserFormStatus(message, isError = false) {
  if (!dom.adminUserFormStatus) {
    return;
  }
  dom.adminUserFormStatus.textContent = String(message || '').trim();
  dom.adminUserFormStatus.classList.toggle('negative', Boolean(isError && message));
}

function getAdminViewCatalog() {
  if (Array.isArray(state.admin.viewCatalog) && state.admin.viewCatalog.length) {
    return state.admin.viewCatalog;
  }
  return Object.entries(VIEW_LABELS).map(([id, label]) => ({ id, label }));
}

function collectSelectedAdminViews() {
  if (!dom.adminUserPermissions) {
    return [];
  }
  const selected = Array.from(dom.adminUserPermissions.querySelectorAll('input[data-admin-view]'))
    .filter((input) => input instanceof HTMLInputElement && input.checked)
    .map((input) => String(input.getAttribute('data-admin-view') || '').trim())
    .filter(Boolean);
  return Array.from(new Set(selected));
}

function renderAdminPermissionGrid(selectedViews = null) {
  if (!dom.adminUserPermissions) {
    return;
  }
  const isAdmin = Boolean(dom.adminUserIsAdmin?.checked);
  const selectedSet = new Set(
    Array.isArray(selectedViews)
      ? selectedViews.map((view) => normalizeViewNameForAccess(view)).filter(Boolean)
      : collectSelectedAdminViews().map((view) => normalizeViewNameForAccess(view)).filter(Boolean)
  );
  const options = getAdminViewCatalog()
    .map((entry) => ({
      id: normalizeViewNameForAccess(entry.id),
      label: entry.label || getViewLabel(entry.id)
    }))
    .filter((entry) => entry.id);

  dom.adminUserPermissions.innerHTML = options
    .map((entry) => {
      const checked = isAdmin || (!ADMIN_ONLY_VIEWS.has(entry.id) && selectedSet.has(entry.id));
      const disabled = isAdmin || ADMIN_ONLY_VIEWS.has(entry.id);
      return `
        <label class="permission-grid-item">
          <input
            type="checkbox"
            data-admin-view="${escapeHtml(entry.id)}"
            ${checked ? 'checked' : ''}
            ${disabled ? 'disabled' : ''}
          />
          <span>${escapeHtml(entry.label)}</span>
        </label>
      `;
    })
    .join('');
}

function resetAdminUserForm() {
  state.admin.editingUsername = null;
  if (dom.adminUserForm) {
    dom.adminUserForm.reset();
  }
  if (dom.adminUserUsername) {
    dom.adminUserUsername.disabled = false;
  }
  if (dom.adminUserPassword) {
    dom.adminUserPassword.required = true;
    dom.adminUserPassword.placeholder = '';
  }
  if (dom.adminUserFormTitle) {
    dom.adminUserFormTitle.textContent = 'Add user';
  }
  if (dom.adminUserSubmitBtn) {
    dom.adminUserSubmitBtn.textContent = 'Create user';
  }
  if (dom.adminUserCancelBtn) {
    dom.adminUserCancelBtn.disabled = true;
  }
  const defaultViews = getAdminViewCatalog()
    .map((entry) => normalizeViewNameForAccess(entry.id))
    .filter((view) => view && !ADMIN_ONLY_VIEWS.has(view));
  renderAdminPermissionGrid(defaultViews);
  setAdminUserFormStatus('');
}

function renderAdminUsersTable() {
  if (!dom.adminUsersBody) {
    return;
  }
  const users = Array.isArray(state.admin.users) ? state.admin.users : [];
  if (!users.length) {
    dom.adminUsersBody.innerHTML = `
      <tr>
        <td colspan="5" class="muted">No users configured.</td>
      </tr>
    `;
    return;
  }

  dom.adminUsersBody.innerHTML = users
    .map((user) => {
      const tabs = Array.isArray(user.allowedViews)
        ? user.allowedViews.map((view) => getViewLabel(view)).join(', ')
        : '-';
      return `
        <tr>
          <td>${escapeHtml(user.username || '-')}</td>
          <td>${escapeHtml(user.isAdmin ? 'Admin' : 'User')}</td>
          <td>${escapeHtml(tabs || '-')}</td>
          <td>${escapeHtml(formatDateTime(user.updatedAt || user.createdAt || null))}</td>
          <td>
            <button
              type="button"
              class="btn secondary js-admin-user-edit"
              data-admin-username="${escapeHtml(user.username || '')}"
            >
              Edit
            </button>
            <button
              type="button"
              class="btn danger js-admin-user-delete"
              data-admin-username="${escapeHtml(user.username || '')}"
            >
              Delete
            </button>
          </td>
        </tr>
      `;
    })
    .join('');
}

function startAdminUserEdit(usernameRaw) {
  const username = String(usernameRaw || '').trim().toLowerCase();
  if (!username) {
    return;
  }
  const user = (state.admin.users || []).find((entry) => String(entry?.username || '').trim().toLowerCase() === username);
  if (!user) {
    return;
  }
  state.admin.editingUsername = user.username;
  if (dom.adminUserUsername) {
    dom.adminUserUsername.value = user.username;
    dom.adminUserUsername.disabled = true;
  }
  if (dom.adminUserPassword) {
    dom.adminUserPassword.value = '';
    dom.adminUserPassword.required = false;
    dom.adminUserPassword.placeholder = 'Leave empty to keep current password';
  }
  if (dom.adminUserIsAdmin) {
    dom.adminUserIsAdmin.checked = Boolean(user.isAdmin);
  }
  renderAdminPermissionGrid(Array.isArray(user.allowedViews) ? user.allowedViews : []);
  if (dom.adminUserFormTitle) {
    dom.adminUserFormTitle.textContent = `Edit user: ${user.username}`;
  }
  if (dom.adminUserSubmitBtn) {
    dom.adminUserSubmitBtn.textContent = 'Save user';
  }
  if (dom.adminUserCancelBtn) {
    dom.adminUserCancelBtn.disabled = false;
  }
  setAdminUserFormStatus('');
}

async function loadAdminUsers() {
  if (!hasClientViewAccess('admin-users')) {
    return;
  }
  const payload = await api('/api/admin/users');
  state.admin.users = Array.isArray(payload?.users) ? payload.users : [];
  state.admin.viewCatalog = Array.isArray(payload?.viewCatalog) ? payload.viewCatalog : [];
  renderAdminUsersTable();
  if (!state.admin.editingUsername) {
    resetAdminUserForm();
  }
}

async function submitAdminUserForm(event) {
  event.preventDefault();
  if (!hasClientViewAccess('admin-users')) {
    return;
  }
  const isEditing = Boolean(state.admin.editingUsername);
  const username = String(dom.adminUserUsername?.value || '').trim().toLowerCase();
  const password = String(dom.adminUserPassword?.value || '');
  const isAdmin = Boolean(dom.adminUserIsAdmin?.checked);
  const allowedViews = collectSelectedAdminViews();

  if (!username) {
    setAdminUserFormStatus('Username is required.', true);
    return;
  }
  if (!isEditing && !password) {
    setAdminUserFormStatus('Password is required for new users.', true);
    return;
  }
  if (!isAdmin && !allowedViews.length) {
    setAdminUserFormStatus('Select at least one tab permission.', true);
    return;
  }

  const payload = {
    isAdmin,
    allowedViews
  };
  if (!isEditing) {
    payload.username = username;
    payload.password = password;
  } else if (password) {
    payload.password = password;
  }

  setAdminUserFormStatus(isEditing ? 'Saving user...' : 'Creating user...');
  if (isEditing) {
    await api(`/api/admin/users/${encodeURIComponent(state.admin.editingUsername)}`, {
      method: 'PUT',
      body: JSON.stringify(payload)
    });
    showToast('User updated.');
  } else {
    await api('/api/admin/users', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
    showToast('User created.');
  }

  const editedSelf =
    isEditing &&
    String(state.admin.editingUsername || '').trim().toLowerCase() === String(state.authUser?.username || '').trim().toLowerCase();
  resetAdminUserForm();
  await loadAdminUsers();
  if (editedSelf) {
    await loadAuthSession();
    setActiveView(getActiveViewName(), { persist: true });
  }
}

async function handleAdminUsersTableClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const username = String(target.dataset.adminUsername || '').trim().toLowerCase();
  if (!username) {
    return;
  }

  if (target.classList.contains('js-admin-user-edit')) {
    startAdminUserEdit(username);
    return;
  }

  if (target.classList.contains('js-admin-user-delete')) {
    if (!window.confirm(`Delete user "${username}"?`)) {
      return;
    }
    await api(`/api/admin/users/${encodeURIComponent(username)}`, {
      method: 'DELETE'
    });
    showToast('User deleted.');
    if (state.admin.editingUsername && state.admin.editingUsername.toLowerCase() === username) {
      resetAdminUserForm();
    }
    await loadAdminUsers();
  }
}

async function handleBillingScopeClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const viewButton = target.closest('[data-billing-open-view]');
  if (viewButton instanceof HTMLElement) {
    const viewName = String(viewButton.dataset.billingOpenView || '').trim();
    if (viewName) {
      setBillingNavExpanded(true);
      setActiveView(viewName);
    }
    return;
  }
  const button = target.closest('[data-billing-scope]');
  if (!(button instanceof HTMLElement)) {
    return;
  }
  const scopeId = String(button.dataset.billingScope || 'all').trim() || 'all';
  setBillingNavExpanded(true);
  setBillingScope(scopeId);
  setActiveView('billing');
  await loadBilling();
}

async function handleLiveNavGroupClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const button = target.closest('[data-live-pinned-group]');
  if (!(button instanceof HTMLElement)) {
    return;
  }
  const groupName = normalizeLivePinnedGroupName(button.dataset.livePinnedGroup);
  if (!groupName) {
    return;
  }
  setLivePinnedGroupName(groupName, { persist: true });
  setLiveNavExpanded(true);
  renderLiveNavGroup();
  setActiveView('live');
  await loadLive({ refresh: true });
}

async function handleBillingAccountSummaryClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const row = target.closest('[data-billing-account-scope]');
  if (!(row instanceof HTMLElement)) {
    return;
  }
  const scopeId = String(row.dataset.billingAccountScope || '').trim();
  if (!scopeId) {
    return;
  }
  setBillingScope(scopeId);
  await loadBilling();
}

function handleBillingTypeProviderChange(event) {
  const target = event.target;
  if (!(target instanceof HTMLSelectElement)) {
    return;
  }
  state.billing.typeProviderFilter = parseBillingTypeFilterValue(target.value);
  renderBillingResourceBreakdown();
}

function handleBillingTypeResourceChange(event) {
  const target = event.target;
  if (!(target instanceof HTMLSelectElement)) {
    return;
  }
  state.billing.typeResourceFilter = parseBillingTypeFilterValue(target.value);
  renderBillingResourceBreakdown();
}

function handleBillingTypeTagChange(event) {
  const target = event.target;
  if (!(target instanceof HTMLSelectElement)) {
    return;
  }
  state.billing.typeTagFilter = parseBillingTypeTagFilterValue(target.value).value;
  renderBillingResourceBreakdown();
}

function handleBillingTypeSortChange(event) {
  const target = event.target;
  if (!(target instanceof HTMLSelectElement)) {
    return;
  }
  state.billing.typeSort = parseBillingTypeSortValue(target.value);
  renderBillingResourceBreakdown();
}

function handleBillingAccountSortChange(event) {
  const target = event.target;
  if (!(target instanceof HTMLSelectElement)) {
    return;
  }
  state.billing.accountSort = parseBillingAccountSortValue(target.value);
  saveBillingAccountSort(state.billing.accountSort);
  renderBillingAccountSummary();
}

function handleBillingSearchInput(event) {
  const target = event.target;
  if (!(target instanceof HTMLInputElement)) {
    return;
  }
  state.billing.searchText = normalizeBillingSearchText(target.value);
  saveBillingSearchText(state.billing.searchText);
  renderBillingResourceBreakdown();
}

async function handleBillingTagSummaryClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const card = target.closest('[data-billing-tag-key]');
  if (!(card instanceof HTMLElement)) {
    return;
  }
  const tagKey = String(card.dataset.billingTagKey || '').trim().toLowerCase();
  if (!tagKey) {
    return;
  }
  const tagValue = String(card.dataset.billingTagValue || '').trim().toLowerCase();
  const scope = String(card.dataset.billingTagScope || '').trim().toLowerCase() || 'current';
  await applyBillingTagSummaryFilter(tagKey, tagValue, { scope });
}

async function handleBillingTagSummaryKeydown(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  if (event.key !== 'Enter' && event.key !== ' ') {
    return;
  }
  const card = target.closest('[data-billing-tag-key]');
  if (!(card instanceof HTMLElement)) {
    return;
  }
  event.preventDefault();
  const tagKey = String(card.dataset.billingTagKey || '').trim().toLowerCase();
  if (!tagKey) {
    return;
  }
  const tagValue = String(card.dataset.billingTagValue || '').trim().toLowerCase();
  const scope = String(card.dataset.billingTagScope || '').trim().toLowerCase() || 'current';
  await applyBillingTagSummaryFilter(tagKey, tagValue, { scope });
}

function applyBillingPreset(preset, options = {}) {
  const normalized = String(preset || '').trim().toLowerCase();
  const allowed = new Set(['month_to_date', 'last_month', '3_months', '6_months', '1_year']);
  if (!allowed.has(normalized)) {
    return false;
  }
  const range = getBillingRangeForPreset(normalized);
  if (dom.billingRangeStart) {
    dom.billingRangeStart.value = range.periodStart;
  }
  if (dom.billingRangeEnd) {
    dom.billingRangeEnd.value = range.periodEnd;
  }
  state.billing.rangePreset = normalized;
  renderBillingRangePresetButtons();
  if (options.persist !== false) {
    saveBillingPreset(normalized);
  }
  return true;
}

async function handleBillingPresetClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const button = target.closest('[data-billing-preset]');
  if (!(button instanceof HTMLElement)) {
    return;
  }
  const preset = String(button.dataset.billingPreset || '').trim();
  const changed = applyBillingPreset(preset);
  if (!changed) {
    return;
  }
  await loadBilling();
}

async function handleOverviewBillingPresetClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const button = target.closest('[data-overview-billing-preset]');
  if (!(button instanceof HTMLElement)) {
    return;
  }
  const preset = String(button.dataset.overviewBillingPreset || '').trim();
  const changed = applyOverviewBillingPreset(preset);
  if (!changed) {
    return;
  }
  await loadOverview();
}

async function handleBillingRangeChange() {
  const periodStart = String(dom.billingRangeStart?.value || '').trim();
  const periodEnd = String(dom.billingRangeEnd?.value || '').trim();
  if ((periodStart && !periodEnd) || (!periodStart && periodEnd)) {
    setBillingSyncState({
      message: 'Select both From and To dates.',
      error: false,
      success: false
    });
    return;
  }
  state.billing.rangePreset = resolveRangePresetFromValues(periodStart, periodEnd);
  renderBillingRangePresetButtons();
  if (state.billing.rangePreset !== 'custom') {
    saveBillingPreset(state.billing.rangePreset);
  }
  await loadBilling();
}

async function promptAndSaveResourceTags(input = {}) {
  const provider = String(input.provider || '').trim().toLowerCase();
  const resourceRef = String(input.resourceRef || '').trim();
  const vendorId = input.vendorId ? String(input.vendorId).trim() : null;
  const accountId = input.accountId ? String(input.accountId).trim() : null;
  if (!resourceRef) {
    return;
  }

  const existing = getCachedTagsForResource(resourceRef);
  const seed = formatTagsForPrompt(existing?.tags || {});
  const text = window.prompt(
    'Edit tags as key=value pairs separated by commas (or JSON object):',
    seed
  );
  if (text === null) {
    return;
  }
  const tags = parseTagsPromptInput(text);
  const response = await api('/api/tags', {
    method: 'PUT',
    body: JSON.stringify({
      provider,
      resourceRef,
      vendorId,
      accountId,
      tags
    })
  });

  const saved = response?.row || null;
  const key = normalizeResourceRefKey(resourceRef);
  if (key) {
    state.billing.tagsByResourceRef[key] = {
      tags: saved?.tags || tags,
      provider: saved?.provider || provider,
      vendorId: saved?.vendorId || vendorId,
      accountId: saved?.accountId || accountId,
      source: saved?.source || (response?.cloudSynced ? 'cloud' : 'local'),
      warning: response?.warning || null
    };
  }
  if (response?.warning) {
    showToast(response.warning, false);
  } else {
    showToast('Tags updated.');
  }
  renderBillingResourceBreakdown();
  renderBillingOrgTotals();
  renderBillingProductTotals();
  await loadTags();
}

async function handleBillingResourceBodyClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }

  const exportBtn = target.closest('[data-billing-export-resource]');
  if (exportBtn instanceof HTMLElement) {
    const resourceType = String(exportBtn.dataset.billingExportResource || '').trim();
    if (!resourceType) {
      return;
    }
    await exportBillingCsv({ resourceType });
    return;
  }

  const toggleBtn = target.closest('[data-billing-toggle]');
  if (toggleBtn instanceof HTMLElement) {
    const groupKey = String(toggleBtn.dataset.billingToggle || '').trim();
    if (!groupKey) {
      return;
    }
    const expanded = getExpandedBillingGroupSet();
    if (expanded.has(groupKey)) {
      expanded.delete(groupKey);
      setExpandedBillingGroupSet(expanded);
      renderBillingResourceBreakdown();
      return;
    }

    const detailMap = getBillingResourceDetailGroupMap();
    const group = detailMap.get(groupKey);
    if (group?.details?.length) {
      await resolveTagsForDetailRows(group.details);
    }
    expanded.add(groupKey);
    setExpandedBillingGroupSet(expanded);
    renderBillingResourceBreakdown();
    return;
  }

  const editBtn = target.closest('[data-tag-edit-resource]');
  if (editBtn instanceof HTMLElement) {
    await promptAndSaveResourceTags({
      provider: editBtn.dataset.tagProvider,
      resourceRef: editBtn.dataset.tagEditResource,
      vendorId: editBtn.dataset.tagVendorId,
      accountId: editBtn.dataset.tagAccountId
    });
  }
}

async function handleTagsTableClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const editBtn = target.closest('.js-tag-table-edit');
  if (!(editBtn instanceof HTMLElement)) {
    return;
  }
  await promptAndSaveResourceTags({
    provider: editBtn.dataset.tagProvider,
    resourceRef: editBtn.dataset.tagEditResource,
    vendorId: editBtn.dataset.tagVendorId,
    accountId: editBtn.dataset.tagAccountId
  });
}

function buildBillingTrendSeries(snapshotRows = []) {
  const rows = Array.isArray(snapshotRows) ? snapshotRows : [];
  const monthCurrencyTotals = new Map();
  const currencyTotals = new Map();

  for (const row of rows) {
    const monthKey = String(row?.periodStart || '').slice(0, 7);
    const currency = String(row?.currency || 'USD').trim().toUpperCase() || 'USD';
    const amount = Number(row?.amount || 0);
    if (!monthKey || !Number.isFinite(amount)) {
      continue;
    }

    let byCurrency = monthCurrencyTotals.get(monthKey);
    if (!byCurrency) {
      byCurrency = new Map();
      monthCurrencyTotals.set(monthKey, byCurrency);
    }
    byCurrency.set(currency, (byCurrency.get(currency) || 0) + amount);
    currencyTotals.set(currency, (currencyTotals.get(currency) || 0) + Math.abs(amount));
  }

  const selectedCurrency =
    Array.from(currencyTotals.entries()).sort((left, right) => right[1] - left[1])[0]?.[0] || 'USD';
  const points = Array.from(monthCurrencyTotals.keys())
    .sort((left, right) => left.localeCompare(right))
    .map((monthKey) => {
      const byCurrency = monthCurrencyTotals.get(monthKey);
      return {
        monthKey,
        amount: Number(byCurrency?.get(selectedCurrency) || 0)
      };
    });

  return {
    currency: selectedCurrency,
    points,
    hasMixedCurrencies: currencyTotals.size > 1
  };
}

function formatTrendMonthLabel(monthKey) {
  const date = new Date(`${String(monthKey || '').trim()}-01T00:00:00.000Z`);
  if (Number.isNaN(date.getTime())) {
    return monthKey;
  }
  return date.toLocaleDateString(undefined, { month: 'short', year: 'numeric' });
}

function renderBillingTrend(range, snapshotRows) {
  if (
    !dom.billingTrendPanel ||
    !dom.billingTrendChart ||
    !dom.billingTrendSummary ||
    !dom.billingTrendFootnote ||
    !dom.billingTrendWrap
  ) {
    return;
  }

  const periodStart = String(range?.periodStart || '').trim();
  const periodEnd = String(range?.periodEnd || '').trim();
  const rangeDays = getDateOnlyDiffDays(periodStart, periodEnd);
  const shouldShow = rangeDays > 35;
  dom.billingTrendPanel.style.display = shouldShow ? '' : 'none';
  if (!shouldShow) {
    return;
  }
  setBillingTrendCollapsed(state.billing.trendCollapsed, { persist: false });

  const trend = buildBillingTrendSeries(snapshotRows);
  const points = trend.points;
  if (points.length < 2) {
    dom.billingTrendSummary.textContent = 'Not enough monthly snapshots to draw trend yet.';
    dom.billingTrendChart.innerHTML = '';
    dom.billingTrendFootnote.textContent = 'Tip: use a wider period or wait for more daily sync cycles.';
    return;
  }

  const values = points.map((point) => Number(point.amount || 0));
  const maxValue = Math.max(...values, 1);
  const width = 480;
  const height = 120;
  const padX = 18;
  const padY = 12;
  const innerWidth = width - padX * 2;
  const innerHeight = height - padY * 2;

  const chartPoints = points.map((point, index) => {
    const x =
      points.length > 1
        ? padX + (innerWidth * index) / (points.length - 1)
        : padX + innerWidth / 2;
    const y = padY + innerHeight - (Math.max(0, Number(point.amount || 0)) / maxValue) * innerHeight;
    return { ...point, x, y };
  });

  const linePath = chartPoints
    .map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
    .join(' ');
  const firstPoint = chartPoints[0];
  const lastPoint = chartPoints[chartPoints.length - 1];
  const areaPath = `${linePath} L ${lastPoint.x.toFixed(2)} ${(padY + innerHeight).toFixed(2)} L ${firstPoint.x.toFixed(
    2
  )} ${(padY + innerHeight).toFixed(2)} Z`;

  const grid = [0, 0.25, 0.5, 0.75, 1]
    .map((ratio) => {
      const y = padY + innerHeight - innerHeight * ratio;
      return `<line class="trend-grid" x1="${padX}" y1="${y.toFixed(2)}" x2="${(padX + innerWidth).toFixed(2)}" y2="${y.toFixed(2)}"></line>`;
    })
    .join('');

  const circles = chartPoints
    .map(
      (point) =>
        `<circle class="trend-point" cx="${point.x.toFixed(2)}" cy="${point.y.toFixed(2)}" r="2.8"><title>${escapeHtml(
          `${formatTrendMonthLabel(point.monthKey)}: ${formatCurrency(point.amount, trend.currency)}`
        )}</title></circle>`
    )
    .join('');

  dom.billingTrendChart.innerHTML = `
    ${grid}
    <path class="trend-area" d="${areaPath}"></path>
    <path class="trend-line" d="${linePath}"></path>
    ${circles}
  `;

  const firstValue = Number(values[0] || 0);
  const lastValue = Number(values[values.length - 1] || 0);
  const delta = lastValue - firstValue;
  const direction = delta > 0 ? 'up' : delta < 0 ? 'down' : 'flat';
  const percent = firstValue !== 0 ? Math.abs((delta / firstValue) * 100) : null;
  const startLabel = formatTrendMonthLabel(points[0].monthKey);
  const endLabel = formatTrendMonthLabel(points[points.length - 1].monthKey);

  if (direction === 'flat') {
    dom.billingTrendSummary.textContent = `Flat trend from ${startLabel} to ${endLabel}.`;
  } else if (percent !== null && Number.isFinite(percent)) {
    dom.billingTrendSummary.textContent = `${direction === 'up' ? 'Up' : 'Down'} ${percent.toFixed(
      1
    )}% from ${startLabel} to ${endLabel}.`;
  } else {
    dom.billingTrendSummary.textContent = `${direction === 'up' ? 'Up' : 'Down'} from ${startLabel} to ${endLabel}.`;
  }

  dom.billingTrendFootnote.textContent = trend.hasMixedCurrencies
    ? `Chart uses dominant currency (${trend.currency}) from selected scope.`
    : `Chart currency: ${trend.currency}.`;
}

async function loadBillingAutoSyncStatus() {
  const payload = await api('/api/billing/auto-sync-status');
  const running = Boolean(payload?.running);
  let message = 'Automatic billing sync runs every 24 hours.';
  let error = false;
  let success = false;

  if (running) {
    const started = payload?.lastStartedAt ? formatDateTime(payload.lastStartedAt) : 'now';
    message = `Auto-sync in progress (started ${started}).`;
  } else if (payload?.lastStatus === 'error') {
    error = true;
    message = payload?.lastError
      ? `Auto-sync error: ${payload.lastError}`
      : 'Auto-sync failed on the previous run.';
  } else if (payload?.lastStatus === 'ok') {
    success = true;
    const finished = payload?.lastFinishedAt ? formatDateTime(payload.lastFinishedAt) : 'recently';
    const nextRun = payload?.nextRunAt ? formatDateTime(payload.nextRunAt) : 'about 24h';
    message = `Auto-sync OK (${finished}). Next run: ${nextRun}.`;
  } else if (payload?.nextRunAt) {
    message = `Automatic billing sync every 24h. Next run: ${formatDateTime(payload.nextRunAt)}.`;
  }

  setBillingSyncState({
    running,
    message,
    error,
    success
  });
}

function renderBillingBackfillStatus(status = {}) {
  if (!dom.billingBackfillSummary || !dom.billingBackfillStateText || !dom.billingBackfillFailuresBody) {
    return;
  }

  const running = Boolean(status?.running);
  const progress = status?.progress || {};
  const summary = status?.summary || {};
  const completed = Number(progress.completedVendorMonths || 0);
  const total = Number(progress.totalVendorMonths || 0);
  const completionText = total > 0 ? `${completed}/${total}` : '-';

  const cards = [
    {
      label: 'Status',
      value: running ? 'Running' : summary?.ok === false ? 'Completed with issues' : summary?.ok ? 'Completed' : 'Idle'
    },
    {
      label: 'Progress',
      value: completionText
    },
    {
      label: 'Successful',
      value: formatNumber(progress.successVendorMonths || summary.successVendorMonths || 0)
    },
    {
      label: 'Failed',
      value: formatNumber(progress.failedVendorMonths || summary.failedVendorMonths || 0)
    }
  ];

  dom.billingBackfillSummary.innerHTML = cards
    .map(
      (card) => `
      <article class="card">
        <p class="label">${escapeHtml(card.label)}</p>
        <p class="value">${escapeHtml(card.value)}</p>
      </article>
    `
    )
    .join('');

  if (running) {
    const current = [
      progress.currentVendorName || progress.currentVendorId || '-',
      progress.currentPeriodStart && progress.currentPeriodEnd ? `${progress.currentPeriodStart} to ${progress.currentPeriodEnd}` : '-'
    ];
    dom.billingBackfillStateText.textContent = `Backfill running. Current: ${current[0]} | ${current[1]}`;
  } else if (status?.error) {
    dom.billingBackfillStateText.textContent = `Backfill error: ${status.error}`;
  } else if (status?.finishedAt) {
    dom.billingBackfillStateText.textContent = `Last finished: ${formatDateTime(status.finishedAt)}`;
  } else {
    dom.billingBackfillStateText.textContent = 'No backfill has been started yet.';
  }

  const failures = Array.isArray(status?.failuresPreview) ? status.failuresPreview : [];
  if (!failures.length) {
    dom.billingBackfillFailuresBody.innerHTML = `
      <tr>
        <td colspan="4" class="muted">No failure rows to display.</td>
      </tr>
    `;
    return;
  }

  dom.billingBackfillFailuresBody.innerHTML = failures
    .map(
      (row) => `
      <tr>
        <td>${escapeHtml(row.vendorId || '-')}</td>
        <td>${escapeHtml(getProviderDisplayLabel(row.provider, 'PROVIDER'))}</td>
        <td>${escapeHtml(`${row.periodStart || '-'} to ${row.periodEnd || '-'}`)}</td>
        <td>${escapeHtml(row.error || '-')}</td>
      </tr>
    `
    )
    .join('');
}

async function loadBillingBackfillStatus() {
  const payload = await api('/api/billing/backfill/status');
  state.billing.backfillStatus = payload || null;
  renderBillingBackfillStatus(payload || {});
}

async function startBillingBackfill() {
  const provider = String(dom.billingBackfillProvider?.value || 'all').trim().toLowerCase();
  const lookbackMonths = Number(dom.billingBackfillMonths?.value || 48);
  const onlyMissing = String(dom.billingBackfillOnlyMissing?.value || 'true').trim().toLowerCase() !== 'false';
  const delayMs = Number(dom.billingBackfillDelayMs?.value || 1200);
  const retries = Number(dom.billingBackfillRetries?.value || 4);

  const payload = await api('/api/billing/backfill', {
    method: 'POST',
    body: JSON.stringify({
      provider: provider === 'all' ? null : provider,
      lookbackMonths,
      onlyMissing,
      delayMs,
      retries
    })
  });

  showToast('Billing backfill started.');
  state.billing.backfillStatus = payload?.status || null;
  renderBillingBackfillStatus(payload?.status || {});
}

async function loadBilling() {
  ensureBillingRangeDefaults();
  setBillingOrgTotalsLoading();
  setBillingProductTotalsLoading();
  const query = new URLSearchParams({ limit: '300' });
  const range = readBillingRangeFromInputs();
  if (range) {
    state.billing.rangePreset = resolveRangePresetFromValues(range.periodStart, range.periodEnd);
    renderBillingRangePresetButtons();
  }
  if (range) {
    query.set('periodStart', range.periodStart);
    query.set('periodEnd', range.periodEnd);
  }
  await loadBillingAccounts(range);

  const scope = resolveBillingScopeFilter();
  if (scope.vendorId) {
    query.set('vendorId', scope.vendorId);
  } else if (scope.provider) {
    query.set('provider', scope.provider);
  }

  const payload = await api(`/api/billing?${query.toString()}`);
  const rows = Array.isArray(payload.snapshots) ? payload.snapshots : [];
  state.billing.latestSnapshots = rows;
  state.billing.resourceBreakdownRows = Array.isArray(payload.resourceBreakdown) ? payload.resourceBreakdown : [];
  state.billing.resourceDetailGroups = Array.isArray(payload.resourceDetails) ? payload.resourceDetails : [];
  const orgDetailRows = getBillingDetailRowsForOrgTotals();
  if (orgDetailRows.length) {
    try {
      await resolveTagsForDetailRows(orgDetailRows);
    } catch (error) {
      console.warn('[billing] Org totals tag resolve skipped:', error?.message || String(error));
    }
  }
  renderBillingOrgTotals();
  renderBillingProductTotals();
  const detailGroupKeys = new Set(state.billing.resourceDetailGroups.map((row) => billingResourceGroupKey(row)));
  const expanded = getExpandedBillingGroupSet();
  const nextExpanded = new Set(Array.from(expanded).filter((key) => detailGroupKeys.has(key)));
  setExpandedBillingGroupSet(nextExpanded);
  const body = document.getElementById('billingBody');
  const scopeLabel = formatBillingScopeLabel(scope);
  if (dom.billingScopeHint) {
    const periodStart = state.billing.period?.periodStart || range?.periodStart || '-';
    const periodEnd = state.billing.period?.periodEnd || range?.periodEnd || '-';
    dom.billingScopeHint.textContent = `${scopeLabel} | ${periodStart} to ${periodEnd}`;
  }
  renderBillingTrend(range || state.billing.period || null, rows);
  renderBillingResourceBreakdown();
  void loadBillingAutoSyncStatus().catch(() => {
    // ignore sync status errors during data load
  });

  if (!rows.length) {
    body.innerHTML = `
      <tr>
        <td colspan="6" class="muted">No billing snapshots found for this period.</td>
      </tr>
    `;
    return;
  }

  body.innerHTML = rows
    .map(
      (row) => `
      <tr>
        <td>${escapeHtml(formatDateTime(row.pulledAt))}</td>
        <td>${escapeHtml(row.provider)}</td>
        <td>${escapeHtml(`${row.periodStart} to ${row.periodEnd}`)}</td>
        <td>${escapeHtml(formatCurrency(row.amount, row.currency))}</td>
        <td>${escapeHtml(row.currency)}</td>
        <td>${escapeHtml(row.source)}</td>
      </tr>
    `
    )
    .join('');
}

function buildBillingExportFileName(scope = {}, range = {}, options = {}) {
  const periodStart = String(range?.periodStart || state.billing.period?.periodStart || '').trim() || 'start';
  const periodEnd = String(range?.periodEnd || state.billing.period?.periodEnd || '').trim() || 'end';
  const scopePart = scope.vendorId
    ? `vendor-${toFileSlug(scope.vendorId, 'vendor')}`
    : scope.provider
      ? `provider-${toFileSlug(scope.provider, 'provider')}`
      : 'all-providers';
  if (options.resourceType) {
    return `billing-${scopePart}-resource-${toFileSlug(options.resourceType, 'resource')}-${periodStart}-to-${periodEnd}.csv`;
  }
  return `billing-${scopePart}-${periodStart}-to-${periodEnd}.csv`;
}

async function exportBillingCsv(options = {}) {
  const range = readBillingRangeFromInputs();
  const scope = resolveBillingScopeFilter();
  const resourceType = String(options.resourceType || '').trim();
  if (resourceType && !scope.provider && !scope.vendorId) {
    throw new Error('Select a provider tab to export a single resource type.');
  }

  const query = new URLSearchParams();
  if (range?.periodStart && range?.periodEnd) {
    query.set('periodStart', range.periodStart);
    query.set('periodEnd', range.periodEnd);
  }
  if (scope.vendorId) {
    query.set('vendorId', scope.vendorId);
  } else if (scope.provider) {
    query.set('provider', scope.provider);
  }
  if (resourceType) {
    query.set('resourceType', resourceType);
  }

  showToast(resourceType ? `Exporting ${resourceType} bill CSV...` : 'Exporting billing CSV...');
  const response = await fetch(`/api/billing/export/csv?${query.toString()}`, {
    headers: {
      accept: 'text/csv,*/*'
    }
  });

  if (response.status === 401) {
    window.location.assign('/login');
    throw new Error('Authentication required.');
  }
  if (!response.ok) {
    const raw = await response.text().catch(() => '');
    let message = `Billing export failed (${response.status}).`;
    if (raw) {
      try {
        const parsed = JSON.parse(raw);
        message = parsed?.error || message;
      } catch (_error) {
        message = raw;
      }
    }
    throw new Error(message);
  }

  const blob = await response.blob();
  const disposition = response.headers.get('content-disposition');
  const filename =
    parseDispositionFilename(disposition) || buildBillingExportFileName(scope, range || {}, { resourceType });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.style.display = 'none';
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => {
    URL.revokeObjectURL(url);
  }, 1200);
  showToast('Billing CSV exported.');
}

async function syncBillingFromProviders() {
  const startedAt = Date.now();
  let progressTimer = null;
  const range = readBillingRangeFromInputs();
  const payload = range
    ? {
      periodStart: range.periodStart,
      periodEnd: range.periodEnd
    }
    : {};
  setBillingSyncState({
    running: true,
    message: 'Pulling provider bills. This can take 30 to 60 seconds.'
  });
  progressTimer = window.setInterval(() => {
    const elapsedSec = Math.max(1, Math.floor((Date.now() - startedAt) / 1000));
    setBillingSyncState({
      running: true,
      message: `Pulling provider bills... ${elapsedSec}s elapsed.`
    });
  }, 1000);

  try {
    const result = await api('/api/billing/sync', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
    const rangeLabel = result?.period?.periodStart && result?.period?.periodEnd
      ? ` (${result.period.periodStart} to ${result.period.periodEnd})`
      : '';
    const elapsedSec = Math.max(1, Math.round((Date.now() - startedAt) / 1000));
    setBillingSyncState({
      running: false,
      success: true,
      message: `Billing sync completed in ${elapsedSec}s${rangeLabel}.`
    });
    showToast(`Billing sync complete${rangeLabel}: ${result.summary.ok}/${result.summary.vendors} succeeded.`);
    await loadBilling();
    await loadOverview();
  } catch (error) {
    const elapsedSec = Math.max(1, Math.round((Date.now() - startedAt) / 1000));
    setBillingSyncState({
      running: false,
      error: true,
      message: `Billing sync failed after ${elapsedSec}s: ${error?.message || 'Request failed.'}`
    });
    throw error;
  } finally {
    if (progressTimer) {
      window.clearInterval(progressTimer);
    }
  }
}

function resolveAwsTagSyncContext() {
  const scope = resolveBillingScopeFilter();
  if (scope?.vendorId) {
    const selected = (Array.isArray(state.billing.accounts) ? state.billing.accounts : []).find(
      (row) => String(row?.scopeId || '') === String(scope.scopeId || '')
    );
    if (selected && String(selected.provider || '').trim().toLowerCase() === 'aws') {
      return {
        vendorId: scope.vendorId,
        accountId: String(selected.accountId || '').trim(),
        accountLabel: selected.accountName || selected.accountId || scope.vendorId
      };
    }
  }

  const firstAws = (Array.isArray(state.billing.accounts) ? state.billing.accounts : []).find(
    (row) => String(row?.provider || '').trim().toLowerCase() === 'aws'
  );
  if (firstAws) {
    const scopeId = String(firstAws.scopeId || '').trim();
    return {
      vendorId: scopeId.startsWith('vendor:') ? scopeId.slice('vendor:'.length) : null,
      accountId: String(firstAws.accountId || '').trim(),
      accountLabel: firstAws.accountName || firstAws.accountId || 'AWS account'
    };
  }

  return {
    vendorId: null,
    accountId: '',
    accountLabel: 'AWS account'
  };
}

async function syncAwsTags() {
  const context = resolveAwsTagSyncContext();
  const promptMessage = `AWS account ID to sync tags (default: ${context.accountLabel}). Leave blank to use selected/default AWS vendor.`;
  const input = window.prompt(promptMessage, context.accountId || '');
  if (input === null) {
    return;
  }

  const accountId = String(input || '').trim();
  const payload = {
    includeKnownResources: true,
    maxPages: 500
  };
  if (context.vendorId) {
    payload.vendorId = context.vendorId;
  }
  if (accountId) {
    payload.accountId = accountId;
  }

  if (dom.syncAwsTagsBtn) {
    dom.syncAwsTagsBtn.disabled = true;
    dom.syncAwsTagsBtn.textContent = 'Syncing...';
  }
  setBillingSyncState({
    running: true,
    message: 'Syncing AWS resource tags...'
  });

  try {
    const result = await api('/api/tags/sync/aws', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
    const summary = result?.summary || {};
    showToast(
      `AWS tags synced: ${formatNumber(summary.syncedResources || 0)} resources (${formatNumber(
        summary.taggedResources || 0
      )} tagged).`
    );
    setBillingSyncState({
      running: false,
      success: true,
      message: `AWS tag sync complete: ${formatNumber(summary.syncedResources || 0)} resources.`
    });
    await loadTags();
  } finally {
    if (dom.syncAwsTagsBtn) {
      dom.syncAwsTagsBtn.disabled = false;
      dom.syncAwsTagsBtn.textContent = 'Sync AWS tags';
    }
  }
}

async function loadTags() {
  if (!dom.tagsBody) {
    return;
  }
  const payload = await api('/api/tags?limit=1500');
  const rows = Array.isArray(payload?.rows) ? payload.rows : [];

  state.tags.rows = rows;

  for (const row of rows) {
    const key = normalizeResourceRefKey(row?.resourceRef);
    if (!key) {
      continue;
    }
    state.billing.tagsByResourceRef[key] = {
      tags: row?.tags || {},
      provider: row?.provider || '',
      vendorId: row?.vendorId || null,
      accountId: row?.accountId || null,
      source: row?.source || 'local',
      warning: null
    };
  }

  if (state.billing.resourceBreakdownRows.length) {
    renderBillingResourceBreakdown();
    renderBillingOrgTotals();
    renderBillingProductTotals();
  }

  renderTagsTable();
}

function renderTagsTable() {
  if (!dom.tagsBody) {
    return;
  }
  const query = state.tags.searchText.trim().toLowerCase();
  const rows = state.tags.rows.filter((row) => {
    if (!query) return true;
    const provider = getProviderDisplayLabel(row.provider, 'PROVIDER').toLowerCase();
    const resourceRef = (row.resourceRef || '').toLowerCase();
    const accountId = (row.accountId || '').toLowerCase();
    return provider.includes(query) || resourceRef.includes(query) || accountId.includes(query);
  });

  if (!rows.length) {
    dom.tagsBody.innerHTML = `
      <tr>
        <td colspan="7" class="muted">${state.tags.rows.length
        ? 'No tags match your search.'
        : 'No tags found yet. Expand billing rows and sync/edit tags to populate this table.'
      }</td>
      </tr>
    `;
    return;
  }

  dom.tagsBody.innerHTML = rows
    .map(
      (row) => `
      <tr>
        <td>${escapeHtml(getProviderDisplayLabel(row.provider, 'PROVIDER'))}</td>
        <td class="billing-detail-resource-ref">${escapeHtml(row.resourceRef || '-')}</td>
        <td>${escapeHtml(row.accountId || '-')}</td>
        <td>${renderTagChips(row.tags || {})}</td>
        <td>${escapeHtml(row.source || 'local')}</td>
        <td>${escapeHtml(formatDateTime(row.updatedAt))}</td>
        <td>
          <button
            type="button"
            class="btn secondary tag-edit-btn js-tag-table-edit"
            data-tag-edit-resource="${escapeHtml(row.resourceRef || '')}"
            data-tag-provider="${escapeHtml(row.provider || '')}"
            data-tag-vendor-id="${escapeHtml(row.vendorId || '')}"
            data-tag-account-id="${escapeHtml(row.accountId || '')}"
          >
            Edit tags
          </button>
        </td>
      </tr>
    `
    )
    .join('');
}

function handleTagsSearchInput(event) {
  state.tags.searchText = event.target.value || '';
  renderTagsTable();
}

function normalizeGrafanaCloudPreset(preset) {
  const normalized = String(preset || '').trim().toLowerCase();
  const allowed = new Set(['month_to_date', 'last_month', '3_months', '6_months', '1_year']);
  return allowed.has(normalized) ? normalized : 'custom';
}

function renderGrafanaCloudRangePresetButtons() {
  if (!dom.grafanaCloudRangePresets) {
    return;
  }
  const activePreset = normalizeGrafanaCloudPreset(state.grafanaCloud.rangePreset);
  dom.grafanaCloudRangePresets.querySelectorAll('[data-grafana-cloud-preset]').forEach((button) => {
    if (!(button instanceof HTMLElement)) {
      return;
    }
    const preset = normalizeGrafanaCloudPreset(button.dataset.grafanaCloudPreset || '');
    button.classList.toggle('active', preset !== 'custom' && preset === activePreset);
  });
}

function applyGrafanaCloudPreset(preset, options = {}) {
  const normalized = normalizeGrafanaCloudPreset(preset);
  if (normalized === 'custom') {
    return false;
  }
  const range = getBillingRangeForPreset(normalized);
  const currentStart = String(dom.grafanaCloudRangeStart?.value || state.grafanaCloud.periodStart || '').trim();
  const currentEnd = String(dom.grafanaCloudRangeEnd?.value || state.grafanaCloud.periodEnd || '').trim();
  const changed =
    normalized !== normalizeGrafanaCloudPreset(state.grafanaCloud.rangePreset) ||
    currentStart !== range.periodStart ||
    currentEnd !== range.periodEnd;

  if (dom.grafanaCloudRangeStart) {
    dom.grafanaCloudRangeStart.value = range.periodStart;
  }
  if (dom.grafanaCloudRangeEnd) {
    dom.grafanaCloudRangeEnd.value = range.periodEnd;
  }
  state.grafanaCloud.periodStart = range.periodStart;
  state.grafanaCloud.periodEnd = range.periodEnd;
  state.grafanaCloud.rangePreset = normalized;
  renderGrafanaCloudRangePresetButtons();
  if (options.persist !== false) {
    saveGrafanaCloudPreset(normalized);
  }
  return changed;
}

function ensureGrafanaCloudRangeDefaults() {
  if (!dom.grafanaCloudRangeStart || !dom.grafanaCloudRangeEnd) {
    return;
  }
  const startValue = String(dom.grafanaCloudRangeStart.value || '').trim();
  const endValue = String(dom.grafanaCloudRangeEnd.value || '').trim();
  if (startValue && endValue) {
    state.grafanaCloud.periodStart = startValue;
    state.grafanaCloud.periodEnd = endValue;
    state.grafanaCloud.rangePreset = resolveRangePresetFromValues(startValue, endValue);
    renderGrafanaCloudRangePresetButtons();
    return;
  }
  const preset = normalizeGrafanaCloudPreset(state.grafanaCloud.rangePreset);
  if (preset !== 'custom') {
    applyGrafanaCloudPreset(preset, { persist: false });
    return;
  }
  applyGrafanaCloudPreset('month_to_date', { persist: false });
}

function setGrafanaCloudSyncState({ running = false, message = '', error = false, success = false } = {}) {
  if (!dom.grafanaCloudSyncStatus) {
    return;
  }
  dom.grafanaCloudSyncStatus.textContent = message || '';
  dom.grafanaCloudSyncStatus.classList.toggle('working', Boolean(running));
  dom.grafanaCloudSyncStatus.classList.toggle('error', !running && Boolean(error));
  dom.grafanaCloudSyncStatus.classList.toggle('success', !running && Boolean(success));
}

function renderGrafanaCloudVendorFilter() {
  if (!dom.grafanaCloudVendorFilter) {
    return;
  }
  const vendors = Array.isArray(state.grafanaCloud.vendors) ? state.grafanaCloud.vendors : [];
  const selected = String(state.grafanaCloud.selectedVendorId || 'all').trim() || 'all';
  const hasSelectedVendor = selected === 'all' || vendors.some((vendor) => String(vendor?.id || '').trim() === selected);
  const resolved = hasSelectedVendor ? selected : 'all';
  state.grafanaCloud.selectedVendorId = resolved;
  dom.grafanaCloudVendorFilter.innerHTML = [
    '<option value="all">All Grafana vendors</option>',
    ...vendors
      .map((vendor) => {
        const id = String(vendor?.id || '').trim();
        if (!id) {
          return '';
        }
        const label = String(vendor?.name || vendor?.accountId || id).trim() || id;
        return `<option value="${escapeHtml(id)}">${escapeHtml(label)}</option>`;
      })
      .filter(Boolean)
  ].join('');
  dom.grafanaCloudVendorFilter.value = resolved;
}

function formatGrafanaUsage(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return '-';
  }
  return formatScaledNumber(numeric);
}

function renderGrafanaCloudSummaryCards(payload = {}) {
  if (!dom.grafanaCloudSummaryCards) {
    return;
  }
  const allRows = Array.isArray(state.grafanaCloud.rows) ? state.grafanaCloud.rows : [];
  const rows = allRows.filter((row) => !isGrafanaDetailMetricRow(row));
  const detailRows = allRows.length - rows.length;
  const vendors = Array.isArray(state.grafanaCloud.vendors) ? state.grafanaCloud.vendors : [];
  const amountTotal = rows.reduce((sum, row) => sum + (Number.isFinite(Number(row?.amountDue)) ? Number(row.amountDue) : 0), 0);
  const ingestTotal = rows.reduce(
    (sum, row) => sum + (Number.isFinite(Number(row?.ingestUsage)) ? Number(row.ingestUsage) : 0),
    0
  );
  const queryTotal = rows.reduce(
    (sum, row) => sum + (Number.isFinite(Number(row?.queryUsage)) ? Number(row.queryUsage) : 0),
    0
  );
  const latestDate = rows.reduce((latest, row) => {
    const usageDate = String(row?.usageDate || '').trim();
    return usageDate && (!latest || usageDate > latest) ? usageDate : latest;
  }, '');
  const currency = String(rows.find((row) => row?.currency)?.currency || 'USD').trim().toUpperCase() || 'USD';

  dom.grafanaCloudSummaryCards.innerHTML = `
    <article class="card">
      <p class="label">Configured vendors</p>
      <p class="value">${formatNumber(vendors.length)}</p>
    </article>
    <article class="card">
      <p class="label">Daily rows</p>
      <p class="value">${formatNumber(rows.length)}</p>
    </article>
    <article class="card">
      <p class="label">Detail metric rows</p>
      <p class="value">${formatNumber(detailRows)}</p>
    </article>
    <article class="card">
      <p class="label">Amount due</p>
      <p class="value">${formatCurrency(amountTotal, currency)}</p>
    </article>
    <article class="card">
      <p class="label">Ingest usage</p>
      <p class="value">${formatGrafanaUsage(ingestTotal)}</p>
    </article>
    <article class="card">
      <p class="label">Query usage</p>
      <p class="value">${formatGrafanaUsage(queryTotal)}</p>
    </article>
    <article class="card">
      <p class="label">Latest usage date</p>
      <p class="value">${latestDate || '-'}</p>
    </article>
  `;

  if (dom.grafanaCloudScopeHint) {
    const periodStart = String(payload?.periodStart || state.grafanaCloud.periodStart || '').trim() || '-';
    const periodEnd = String(payload?.periodEnd || state.grafanaCloud.periodEnd || '').trim() || '-';
    const selectedVendorId = String(state.grafanaCloud.selectedVendorId || 'all').trim();
    const selectedVendor =
      selectedVendorId !== 'all'
        ? vendors.find((vendor) => String(vendor?.id || '').trim() === selectedVendorId)
        : null;
    const scopeLabel = selectedVendor ? selectedVendor.name || selectedVendor.accountId || selectedVendorId : 'All Grafana vendors';
    dom.grafanaCloudScopeHint.textContent = `${scopeLabel} | ${periodStart} to ${periodEnd} | ${formatNumber(
      rows.length
    )} billing row(s), ${formatNumber(detailRows)} detail row(s).`;
  }
}

function renderGrafanaCloudDetails(payload = {}) {
  if (!dom.grafanaCloudProductCards || !dom.grafanaCloudMetricsBody) {
    return;
  }
  const details = payload?.details && typeof payload.details === 'object' ? payload.details : state.grafanaCloud.details;
  const products = Array.isArray(details?.products) ? details.products : [];
  const metrics = Array.isArray(details?.metrics) ? details.metrics : [];
  const compareRange = details?.compareRange && typeof details.compareRange === 'object' ? details.compareRange : null;

  if (dom.grafanaCloudDetailsHint) {
    const compareLabel =
      compareRange?.periodStart && compareRange?.periodEnd
        ? `Compared to ${compareRange.periodStart} to ${compareRange.periodEnd}.`
        : 'Previous range comparison unavailable.';
    dom.grafanaCloudDetailsHint.textContent = `${formatNumber(metrics.length)} metric(s) across ${formatNumber(
      products.length
    )} product(s). ${compareLabel}`;
  }

  if (!metrics.length) {
    dom.grafanaCloudProductCards.innerHTML = `
      <article class="card">
        <p class="label">Usage details</p>
        <p class="value">-</p>
        <p class="muted">Run "Sync now" to load detailed Grafana usage dimensions.</p>
      </article>
    `;
    dom.grafanaCloudMetricsBody.innerHTML = `
      <tr>
        <td colspan="9" class="muted">No Grafana product metrics found in the selected range yet.</td>
      </tr>
    `;
    return;
  }

  dom.grafanaCloudProductCards.innerHTML = products
    .map((product) => {
      const productMetrics = metrics.filter((metric) => metric.productKey === product.productKey);
      const primaryCost =
        productMetrics.find((metric) => metric.metricKind === 'cost' && metric.isPrimaryCost) ||
        productMetrics.find((metric) => metric.metricKind === 'cost') ||
        null;
      const highlightUsage = productMetrics.find((metric) => metric.metricKind === 'usage') || null;
      const deltaPercent = Number(primaryCost?.deltaPercent);
      const deltaClass = Number.isFinite(deltaPercent) ? (deltaPercent >= 0 ? 'up' : 'down') : 'flat';
      const deltaLabel = Number.isFinite(deltaPercent) ? formatSignedPercent(deltaPercent) : '-';
      return `
        <article class="card grafana-product-card">
          <p class="label">${escapeHtml(product.productLabel || product.productKey || 'Product')}</p>
          <p class="value">${primaryCost ? formatGrafanaMetricValue(primaryCost.value, 'cost', 'USD', 'USD') : '-'}</p>
          <p class="muted">${escapeHtml(primaryCost?.metricLabel || 'Cost metric')}</p>
          <p class="muted grafana-product-secondary">${highlightUsage
          ? `${escapeHtml(highlightUsage.metricLabel)}: ${escapeHtml(
            formatGrafanaMetricValue(highlightUsage.value, highlightUsage.metricKind, highlightUsage.metricUnit)
          )}`
          : 'No usage metric'
        }</p>
          <p class="muted grafana-delta ${deltaClass}">Delta: ${escapeHtml(deltaLabel)}</p>
        </article>
      `;
    })
    .join('');

  dom.grafanaCloudMetricsBody.innerHTML = metrics
    .map((metric) => {
      const deltaPercent = Number(metric?.deltaPercent);
      const deltaClass = Number.isFinite(deltaPercent) ? (deltaPercent >= 0 ? 'up' : 'down') : 'flat';
      return `
        <tr>
          <td>${escapeHtml(metric?.productLabel || metric?.productKey || '-')}</td>
          <td>${escapeHtml(metric?.metricLabel || metric?.metricKey || '-')}</td>
          <td>${escapeHtml(
        formatGrafanaMetricValue(metric?.value, metric?.metricKind, metric?.metricUnit, metric?.metricKind === 'cost' ? 'USD' : 'USD')
      )}</td>
          <td>${escapeHtml(String(metric?.metricUnit || metric?.metricKind || '-'))}</td>
          <td>${escapeHtml(
        metric?.previousValue === null || metric?.previousValue === undefined
          ? '-'
          : formatGrafanaMetricValue(
            metric.previousValue,
            metric?.metricKind,
            metric?.metricUnit,
            metric?.metricKind === 'cost' ? 'USD' : 'USD'
          )
      )}</td>
          <td><span class="grafana-delta ${deltaClass}">${escapeHtml(formatSignedPercent(metric?.deltaPercent))}</span></td>
          <td>${formatNumber(metric?.rowCount || 0)}</td>
          <td>${escapeHtml(metric?.lastUsageDate || '-')}</td>
          <td>${escapeHtml(metric?.metricKey || '-')}</td>
        </tr>
      `;
    })
    .join('');
}

function renderGrafanaCloudDimensionTable(payload = {}) {
  if (!dom.grafanaCloudDimensionBody) {
    return;
  }
  const rows = Array.isArray(payload?.summary?.byDimension) ? payload.summary.byDimension : [];
  if (!rows.length) {
    dom.grafanaCloudDimensionBody.innerHTML = `
      <tr>
        <td colspan="8" class="muted">No Grafana daily ingest rows in this range yet.</td>
      </tr>
    `;
    return;
  }
  const fallbackCurrency = String(state.grafanaCloud.rows.find((item) => item?.currency)?.currency || 'USD').trim().toUpperCase() || 'USD';
  dom.grafanaCloudDimensionBody.innerHTML = rows
    .slice(0, 1000)
    .map(
      (row) => `
        <tr>
          <td>${escapeHtml(row?.usageDate || '-')}</td>
          <td>${escapeHtml(row?.dimensionName || row?.dimensionKey || '-')}</td>
          <td>${formatCurrency(row?.amountDue || 0, fallbackCurrency)}</td>
          <td>${formatGrafanaUsage(row?.ingestUsage)}</td>
          <td>${formatGrafanaUsage(row?.queryUsage)}</td>
          <td>${formatGrafanaUsage(row?.totalUsage)}</td>
          <td>${formatNumber(row?.rowCount || 0)}</td>
          <td>${escapeHtml(row?.dimensionKey || '-')}</td>
        </tr>
      `
    )
    .join('');
}

function renderGrafanaCloudDailyTable() {
  if (!dom.grafanaCloudDailyBody) {
    return;
  }
  const rows = (Array.isArray(state.grafanaCloud.rows) ? state.grafanaCloud.rows : []).filter(
    (row) => !isGrafanaDetailMetricRow(row)
  );
  const vendorNameById = new Map(
    (Array.isArray(state.grafanaCloud.vendors) ? state.grafanaCloud.vendors : []).map((vendor) => [
      String(vendor?.id || '').trim(),
      String(vendor?.name || vendor?.accountId || vendor?.id || '').trim()
    ])
  );
  if (!rows.length) {
    dom.grafanaCloudDailyBody.innerHTML = `
      <tr>
        <td colspan="11" class="muted">No Grafana ingest rows found for the selected scope.</td>
      </tr>
    `;
    return;
  }
  const sorted = [...rows].sort((left, right) => {
    const dateCmp = String(right?.usageDate || '').localeCompare(String(left?.usageDate || ''));
    if (dateCmp !== 0) {
      return dateCmp;
    }
    return Number(right?.amountDue || 0) - Number(left?.amountDue || 0);
  });
  dom.grafanaCloudDailyBody.innerHTML = sorted
    .slice(0, 5000)
    .map((row) => {
      const vendorName = vendorNameById.get(String(row?.vendorId || '').trim()) || row?.vendorId || '-';
      const estimated = row?.isEstimated ? 'Estimated' : 'Actual';
      return `
        <tr>
          <td>${escapeHtml(row?.usageDate || '-')}</td>
          <td>${escapeHtml(vendorName)}</td>
          <td>${escapeHtml(row?.stackName || row?.stackSlug || '-')}</td>
          <td>${escapeHtml(row?.dimensionName || row?.dimensionKey || '-')}</td>
          <td>${formatGrafanaUsage(row?.ingestUsage)}</td>
          <td>${formatGrafanaUsage(row?.queryUsage)}</td>
          <td>${formatGrafanaUsage(row?.totalUsage)}</td>
          <td>${formatCurrency(row?.amountDue || 0, row?.currency || 'USD')}</td>
          <td>${escapeHtml(row?.unit || '-')}</td>
          <td>${escapeHtml(estimated)}</td>
          <td>${escapeHtml(formatDateTime(row?.fetchedAt || row?.updatedAt || null))}</td>
        </tr>
      `;
    })
    .join('');
}

function applyGrafanaCloudPayload(payload = {}) {
  state.grafanaCloud.vendors = Array.isArray(payload?.vendors) ? payload.vendors : [];
  state.grafanaCloud.rows = Array.isArray(payload?.rows) ? payload.rows : [];
  state.grafanaCloud.summary = payload?.summary && typeof payload.summary === 'object' ? payload.summary : null;
  state.grafanaCloud.details = payload?.details && typeof payload.details === 'object' ? payload.details : null;
  state.grafanaCloud.sync = payload?.sync && typeof payload.sync === 'object' ? payload.sync : null;
  state.grafanaCloud.periodStart = String(payload?.periodStart || state.grafanaCloud.periodStart || '').trim();
  state.grafanaCloud.periodEnd = String(payload?.periodEnd || state.grafanaCloud.periodEnd || '').trim();
  renderGrafanaCloudVendorFilter();
  renderGrafanaCloudSummaryCards(payload);
  renderGrafanaCloudDetails(payload);
  renderGrafanaCloudDimensionTable(payload);
  renderGrafanaCloudDailyTable();

  const syncStatus = state.grafanaCloud.sync || {};
  if (syncStatus.running) {
    setGrafanaCloudSyncState({
      running: true,
      message: 'Grafana-Cloud sync in progress...'
    });
    return;
  }
  if (syncStatus.lastStatus === 'error') {
    setGrafanaCloudSyncState({
      error: true,
      message: syncStatus.lastError ? `Sync error: ${syncStatus.lastError}` : 'Latest sync failed.'
    });
    return;
  }
  const nextRun = syncStatus?.nextRunAt ? formatDateTime(syncStatus.nextRunAt) : null;
  setGrafanaCloudSyncState({
    success: true,
    message: nextRun ? `Daily auto-sync active. Next run: ${nextRun}.` : 'Grafana-Cloud data loaded.'
  });
}

function getGrafanaCloudQueryString() {
  ensureGrafanaCloudRangeDefaults();
  const params = new URLSearchParams();
  const vendorId = String(state.grafanaCloud.selectedVendorId || 'all').trim();
  const periodStart = String(dom.grafanaCloudRangeStart?.value || state.grafanaCloud.periodStart || '').trim();
  const periodEnd = String(dom.grafanaCloudRangeEnd?.value || state.grafanaCloud.periodEnd || '').trim();
  if (vendorId && vendorId !== 'all') {
    params.set('vendorId', vendorId);
  }
  if (periodStart) {
    params.set('periodStart', periodStart);
  }
  if (periodEnd) {
    params.set('periodEnd', periodEnd);
  }
  return params.toString();
}

async function loadGrafanaCloud() {
  ensureGrafanaCloudRangeDefaults();
  const queryString = getGrafanaCloudQueryString();
  const suffix = queryString ? `?${queryString}` : '';
  const payload = await api(`/api/platform/grafana-cloud/daily-ingest${suffix}`);
  applyGrafanaCloudPayload(payload);
}

async function syncGrafanaCloud() {
  ensureGrafanaCloudRangeDefaults();
  const vendorId = String(state.grafanaCloud.selectedVendorId || 'all').trim();
  const periodStart = String(dom.grafanaCloudRangeStart?.value || state.grafanaCloud.periodStart || '').trim();
  const periodEnd = String(dom.grafanaCloudRangeEnd?.value || state.grafanaCloud.periodEnd || '').trim();
  if (dom.syncGrafanaCloudBtn) {
    dom.syncGrafanaCloudBtn.disabled = true;
    dom.syncGrafanaCloudBtn.textContent = 'Syncing...';
  }
  setGrafanaCloudSyncState({
    running: true,
    message: 'Syncing Grafana-Cloud billed usage...'
  });
  try {
    const payload = await api('/api/platform/grafana-cloud/sync', {
      method: 'POST',
      body: JSON.stringify({
        vendorId: vendorId === 'all' ? null : vendorId,
        periodStart,
        periodEnd
      })
    });
    const succeeded = Number(payload?.summary?.succeeded || 0);
    const failed = Number(payload?.summary?.failed || 0);
    const parts = [`Sync complete (${succeeded} vendor(s) succeeded)`];
    if (failed > 0) {
      parts.push(`${failed} failed`);
    }
    showToast(parts.join(', ') + '.');
    await loadGrafanaCloud();
  } finally {
    if (dom.syncGrafanaCloudBtn) {
      dom.syncGrafanaCloudBtn.disabled = false;
      dom.syncGrafanaCloudBtn.textContent = 'Sync now';
    }
  }
}

async function handleGrafanaCloudVendorChange() {
  if (!dom.grafanaCloudVendorFilter) {
    return;
  }
  state.grafanaCloud.selectedVendorId = String(dom.grafanaCloudVendorFilter.value || 'all').trim() || 'all';
  await loadGrafanaCloud();
}

async function handleGrafanaCloudPresetClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const button = target.closest('[data-grafana-cloud-preset]');
  if (!(button instanceof HTMLElement)) {
    return;
  }
  const preset = String(button.dataset.grafanaCloudPreset || '').trim();
  const changed = applyGrafanaCloudPreset(preset);
  if (!changed) {
    return;
  }
  await loadGrafanaCloud();
}

async function handleGrafanaCloudRangeChange() {
  const periodStart = String(dom.grafanaCloudRangeStart?.value || '').trim();
  const periodEnd = String(dom.grafanaCloudRangeEnd?.value || '').trim();
  if ((periodStart && !periodEnd) || (!periodStart && periodEnd)) {
    setGrafanaCloudSyncState({
      message: 'Select both From and To dates.',
      error: false,
      success: false
    });
    return;
  }
  state.grafanaCloud.periodStart = periodStart;
  state.grafanaCloud.periodEnd = periodEnd;
  state.grafanaCloud.rangePreset = resolveRangePresetFromValues(periodStart, periodEnd);
  renderGrafanaCloudRangePresetButtons();
  if (state.grafanaCloud.rangePreset !== 'custom') {
    saveGrafanaCloudPreset(state.grafanaCloud.rangePreset);
  }
  await loadGrafanaCloud();
}

function normalizeCloudMetricsProvider(value) {
  const text = String(value || '').trim().toLowerCase();
  if (['azure', 'aws', 'gcp', 'rackspace', 'wasabi', 'private', 'other'].includes(text)) {
    return text;
  }
  return 'azure';
}

function setCloudMetricsSyncState({ running = false, message = '', error = false, success = false } = {}) {
  if (!dom.cloudMetricsSyncStatus) {
    return;
  }
  dom.cloudMetricsSyncStatus.textContent = message || '';
  dom.cloudMetricsSyncStatus.classList.toggle('working', Boolean(running));
  dom.cloudMetricsSyncStatus.classList.toggle('error', !running && Boolean(error));
  dom.cloudMetricsSyncStatus.classList.toggle('success', !running && Boolean(success));
}

function buildCloudMetricsTypeKey(provider, group) {
  const providerKey = normalizeCloudMetricsProvider(provider);
  const typeKey = String(group?.resourceType || '').trim().toLowerCase();
  return `${providerKey}:${typeKey}`;
}

function buildCloudMetricsResourceKey(provider, group, resource) {
  const typeKey = buildCloudMetricsTypeKey(provider, group);
  const resourceKey = String(resource?.resourceId || resource?.resourceName || '').trim().toLowerCase();
  return `${typeKey}:${resourceKey}`;
}

function getCloudMetricsProviderLabel(providerId) {
  const selected = (Array.isArray(state.cloudMetrics.providers) ? state.cloudMetrics.providers : []).find(
    (entry) => String(entry?.id || '').trim().toLowerCase() === String(providerId || '').trim().toLowerCase()
  );
  if (selected?.label) {
    return String(selected.label);
  }
  const text = String(providerId || '').trim();
  return text ? text.toUpperCase() : 'Provider';
}

function setSelectedCloudMetricsProvider(provider, options = {}) {
  const normalized = normalizeCloudMetricsProvider(provider);
  state.cloudMetrics.selectedProvider = normalized;
  if (options.persist !== false) {
    saveCloudMetricsProvider(normalized);
  }
}

function renderCloudMetricsProviderTabs() {
  if (!dom.cloudMetricsProviderTabs) {
    return;
  }
  const providers = Array.isArray(state.cloudMetrics.providers) ? state.cloudMetrics.providers : [];
  if (!providers.length) {
    dom.cloudMetricsProviderTabs.innerHTML = '';
    return;
  }
  const selected = normalizeCloudMetricsProvider(state.cloudMetrics.selectedProvider);
  dom.cloudMetricsProviderTabs.innerHTML = providers
    .map((provider) => {
      const id = normalizeCloudMetricsProvider(provider?.id);
      const active = id === selected;
      const supported = provider?.supported !== false;
      const configured = provider?.configured !== false;
      const count = Number(provider?.resourceCount || 0);
      const titleParts = [];
      if (!supported) {
        titleParts.push('Read-only placeholder until provider sync is implemented.');
      }
      if (!configured) {
        titleParts.push('Provider credentials are not configured yet.');
      }
      if (provider?.lastSyncAt) {
        titleParts.push(`Last sync: ${formatDateTime(provider.lastSyncAt)}`);
      }
      return `
        <button
          type="button"
          class="billing-preset-btn cloud-metrics-provider-btn ${active ? 'active' : ''} ${!supported ? 'muted-btn' : ''}"
          data-cloud-metrics-provider="${escapeHtml(id)}"
          title="${escapeHtml(titleParts.join(' '))}"
        >
          ${escapeHtml(provider?.label || id.toUpperCase())}
          <span class="cloud-metrics-provider-meta">${count > 0 ? formatNumber(count) : '-'}</span>
        </button>
      `;
    })
    .join('');
}

function renderCloudMetricsRows() {
  if (!dom.cloudMetricsTypeBody) {
    return;
  }
  const provider = normalizeCloudMetricsProvider(state.cloudMetrics.selectedProvider);
  const groups = Array.isArray(state.cloudMetrics.typeGroups) ? state.cloudMetrics.typeGroups : [];
  if (!groups.length) {
    dom.cloudMetricsTypeBody.innerHTML = `
      <tr>
        <td colspan="5" class="muted">No metrics are stored yet for ${escapeHtml(getCloudMetricsProviderLabel(provider))}. Click Refresh to sync.</td>
      </tr>
    `;
    return;
  }

  const rows = [];
  for (const group of groups) {
    const typeKey = buildCloudMetricsTypeKey(provider, group);
    const typeExpanded = state.cloudMetrics.expandedTypeKeys.includes(typeKey);
    rows.push(`
      <tr>
        <td class="billing-expand-cell">
          <button type="button" class="billing-expand-btn" data-cloud-metrics-type="${escapeHtml(typeKey)}">${typeExpanded ? '-' : '+'}</button>
        </td>
        <td>${escapeHtml(group?.resourceTypeLabel || group?.resourceType || 'unknown')}</td>
        <td>${formatNumber(group?.resourceCount || 0)}</td>
        <td>${formatNumber(group?.metricCount || 0)}</td>
        <td>${escapeHtml(formatDateTime(group?.lastSyncAt || null))}</td>
      </tr>
    `);

    if (!typeExpanded) {
      continue;
    }

    const resources = Array.isArray(group?.resources) ? group.resources : [];
    const detailRows = resources
      .map((resource) => {
        const resourceKey = buildCloudMetricsResourceKey(provider, group, resource);
        const resourceExpanded = state.cloudMetrics.expandedResourceKeys.includes(resourceKey);
        const metricError = String(resource?.metadata?.error || '').trim();
        const metricStatus = metricError ? buildStatusBadge('warning') : buildStatusBadge('healthy');
        const metrics = Array.isArray(resource?.metrics) ? resource.metrics : [];

        const metricRows = metrics
          .map((metric) => {
            const scale = resolveCloudMetricScale(metric);
            return `
              <tr>
                <td>${escapeHtml(metric?.name || '-')}</td>
                <td>${escapeHtml(metric?.namespace || '-')}</td>
                <td>${formatCloudMetricValue(metric?.value, scale)}</td>
                <td>${escapeHtml(scale?.unitLabel || '-')}</td>
                <td>${formatCloudMetricValue(metric?.average, scale)}</td>
                <td>${formatCloudMetricValue(metric?.minimum, scale)}</td>
                <td>${formatCloudMetricValue(metric?.maximum, scale)}</td>
                <td>${formatCloudMetricValue(metric?.total, scale)}</td>
                <td>${escapeHtml(formatDateTime(metric?.timestamp))}</td>
              </tr>
            `
          })
          .join('');

        return `
          <tr>
            <td class="billing-expand-cell">
              <button type="button" class="billing-expand-btn" data-cloud-metrics-resource="${escapeHtml(resourceKey)}">${resourceExpanded ? '-' : '+'}</button>
            </td>
            <td>${escapeHtml(resource?.resourceName || resource?.resourceId || 'Resource')}</td>
            <td>${escapeHtml(resource?.accountId || resource?.subscriptionId || '-')}</td>
            <td>${escapeHtml(resource?.location || '-')}</td>
            <td>${formatNumber(resource?.metricCount || 0)}</td>
            <td>${escapeHtml(formatDateTime(resource?.fetchedAt || null))}</td>
            <td>${metricStatus}</td>
          </tr>
          ${resourceExpanded
            ? `
            <tr class="billing-detail-row">
              <td colspan="7">
                <div class="billing-detail-wrap">
                  ${metricError
              ? `<p class="muted negative">Metric read warning: ${escapeHtml(metricError)}</p>`
              : ''
            }
                  ${metrics.length
              ? `
                    <table class="billing-detail-table cloud-metrics-metric-table">
                      <thead>
                        <tr>
                          <th>Metric</th>
                          <th>Namespace</th>
                          <th>Value</th>
                          <th>Unit</th>
                          <th>Avg</th>
                          <th>Min</th>
                          <th>Max</th>
                          <th>Total</th>
                          <th>Timestamp</th>
                        </tr>
                      </thead>
                      <tbody>${metricRows}</tbody>
                    </table>
                  `
              : '<p class="muted">No metric values returned for this resource in the latest window.</p>'
            }
                </div>
              </td>
            </tr>
          `
            : ''
          }
        `;
      })
      .join('');

    rows.push(`
      <tr class="billing-detail-row">
        <td colspan="5">
          <div class="billing-detail-wrap">
            <table class="billing-detail-table cloud-metrics-resource-table">
              <thead>
                <tr>
                  <th class="billing-expand-cell"></th>
                  <th>Resource</th>
                  <th>Account</th>
                  <th>Location</th>
                  <th>Metrics</th>
                  <th>Last sync</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                ${detailRows || '<tr><td colspan="7" class="muted">No resources found for this type.</td></tr>'}
              </tbody>
            </table>
          </div>
        </td>
      </tr>
    `);
  }

  dom.cloudMetricsTypeBody.innerHTML = rows.join('');
}

function applyCloudMetricsPayload(payload = {}, options = {}) {
  const providers = Array.isArray(payload?.providers) ? payload.providers : [];
  state.cloudMetrics.providers = providers;
  const preferredProvider = normalizeCloudMetricsProvider(payload?.provider || options.provider || state.cloudMetrics.selectedProvider || 'azure');
  const available = new Set(providers.map((entry) => normalizeCloudMetricsProvider(entry?.id)));
  const selectedProvider = available.has(preferredProvider)
    ? preferredProvider
    : normalizeCloudMetricsProvider(providers[0]?.id || preferredProvider || 'azure');
  setSelectedCloudMetricsProvider(selectedProvider, { persist: options.persist !== false });

  state.cloudMetrics.typeGroups = Array.isArray(payload?.typeGroups) ? payload.typeGroups : [];
  state.cloudMetrics.syncStatus = payload?.syncStatus || null;
  state.cloudMetrics.expandedTypeKeys = state.cloudMetrics.expandedTypeKeys.filter((key) =>
    state.cloudMetrics.typeGroups.some((group) => buildCloudMetricsTypeKey(selectedProvider, group) === key)
  );
  state.cloudMetrics.expandedResourceKeys = state.cloudMetrics.expandedResourceKeys.filter((key) =>
    state.cloudMetrics.typeGroups.some((group) => {
      const resources = Array.isArray(group?.resources) ? group.resources : [];
      return resources.some((resource) => buildCloudMetricsResourceKey(selectedProvider, group, resource) === key);
    })
  );

  renderCloudMetricsProviderTabs();
  renderCloudMetricsRows();

  if (dom.cloudMetricsScopeHint) {
    const providerLabel = getCloudMetricsProviderLabel(selectedProvider);
    const totalResources = Number(payload?.totalResources || 0);
    const totalMetrics = Number(payload?.totalMetrics || 0);
    const lastSync = payload?.lastSyncAt ? formatDateTime(payload.lastSyncAt) : 'Not synced yet';
    dom.cloudMetricsScopeHint.textContent = `${providerLabel} | ${formatNumber(totalResources)} resources | ${formatNumber(
      totalMetrics
    )} metric values | Last sync ${lastSync}.`;
  }
}

async function loadCloudMetrics(options = {}) {
  const selectedProvider = normalizeCloudMetricsProvider(
    options.provider || state.cloudMetrics.selectedProvider || readSavedCloudMetricsProvider() || 'azure'
  );
  const payload = await api(`/api/platform/cloud-metrics?provider=${encodeURIComponent(selectedProvider)}`);
  applyCloudMetricsPayload(payload, {
    provider: selectedProvider,
    persist: options.persist !== false
  });

  const syncStatus = payload?.syncStatus || null;
  if (syncStatus?.running) {
    setCloudMetricsSyncState({
      running: true,
      message: `Sync in progress for ${getCloudMetricsProviderLabel(selectedProvider)}...`
    });
    return;
  }
  if (syncStatus?.lastStatus === 'error') {
    setCloudMetricsSyncState({
      error: true,
      message: syncStatus?.lastError ? `Sync error: ${syncStatus.lastError}` : 'Latest sync failed.'
    });
    return;
  }
  const nextRun = syncStatus?.nextRunAt ? formatDateTime(syncStatus.nextRunAt) : null;
  setCloudMetricsSyncState({
    success: true,
    message: nextRun ? `Auto-sync every 5 minutes. Next run: ${nextRun}.` : 'Cloud metrics loaded.'
  });
}

async function syncCloudMetrics() {
  const selectedProvider = normalizeCloudMetricsProvider(state.cloudMetrics.selectedProvider || 'azure');
  if (dom.refreshCloudMetricsBtn) {
    dom.refreshCloudMetricsBtn.disabled = true;
    dom.refreshCloudMetricsBtn.textContent = 'Syncing...';
  }
  setCloudMetricsSyncState({
    running: true,
    message: `Syncing ${getCloudMetricsProviderLabel(selectedProvider)} metrics...`
  });
  try {
    const payload = await api('/api/platform/cloud-metrics/sync', {
      method: 'POST',
      body: JSON.stringify({
        provider: selectedProvider
      })
    });
    applyCloudMetricsPayload(payload, {
      provider: selectedProvider,
      persist: true
    });
    setCloudMetricsSyncState({
      success: true,
      message: 'Cloud metrics sync completed.'
    });
    showToast('Cloud metrics sync completed.');
  } finally {
    if (dom.refreshCloudMetricsBtn) {
      dom.refreshCloudMetricsBtn.disabled = false;
      dom.refreshCloudMetricsBtn.textContent = 'Refresh';
    }
  }
}

function handleCloudMetricsProviderTabsClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const button = target.closest('[data-cloud-metrics-provider]');
  if (!(button instanceof HTMLElement)) {
    return;
  }
  const provider = normalizeCloudMetricsProvider(button.dataset.cloudMetricsProvider || '');
  if (!provider) {
    return;
  }
  state.cloudMetrics.expandedTypeKeys = [];
  state.cloudMetrics.expandedResourceKeys = [];
  setSelectedCloudMetricsProvider(provider, { persist: true });
  void loadCloudMetrics({
    provider,
    persist: true
  }).catch((error) => {
    showToast(error.message || 'Failed to switch cloud metrics provider.', true);
  });
}

function handleCloudMetricsTypeBodyClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const typeBtn = target.closest('[data-cloud-metrics-type]');
  if (typeBtn instanceof HTMLElement) {
    const typeKey = String(typeBtn.dataset.cloudMetricsType || '').trim();
    if (!typeKey) {
      return;
    }
    const expanded = state.cloudMetrics.expandedTypeKeys.includes(typeKey);
    state.cloudMetrics.expandedTypeKeys = expanded
      ? state.cloudMetrics.expandedTypeKeys.filter((key) => key !== typeKey)
      : [...state.cloudMetrics.expandedTypeKeys, typeKey];
    if (expanded) {
      state.cloudMetrics.expandedResourceKeys = state.cloudMetrics.expandedResourceKeys.filter((key) => !key.startsWith(`${typeKey}:`));
    }
    renderCloudMetricsRows();
    return;
  }

  const resourceBtn = target.closest('[data-cloud-metrics-resource]');
  if (resourceBtn instanceof HTMLElement) {
    const resourceKey = String(resourceBtn.dataset.cloudMetricsResource || '').trim();
    if (!resourceKey) {
      return;
    }
    const expanded = state.cloudMetrics.expandedResourceKeys.includes(resourceKey);
    state.cloudMetrics.expandedResourceKeys = expanded
      ? state.cloudMetrics.expandedResourceKeys.filter((key) => key !== resourceKey)
      : [...state.cloudMetrics.expandedResourceKeys, resourceKey];
    renderCloudMetricsRows();
  }
}

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

function getCloudDatabaseProviderLabel(providerId) {
  const provider = normalizeCloudDatabaseProvider(providerId, 'all');
  if (provider === 'all') {
    return 'All providers';
  }
  const selected = (Array.isArray(state.cloudDatabase.providers) ? state.cloudDatabase.providers : []).find(
    (entry) => normalizeCloudDatabaseProvider(entry?.id) === provider
  );
  if (selected?.label) {
    return String(selected.label);
  }
  return getProviderDisplayLabel(provider, provider.toUpperCase());
}

function setSelectedCloudDatabaseProvider(provider, options = {}) {
  const normalized = normalizeCloudDatabaseProvider(provider, 'all');
  state.cloudDatabase.selectedProvider = normalized;
  if (options.persist !== false) {
    saveCloudDatabaseProvider(normalized);
  }
}

function setCloudDatabaseSyncState({ running = false, message = '', error = false, success = false } = {}) {
  if (!dom.cloudDatabaseSyncStatus) {
    return;
  }
  dom.cloudDatabaseSyncStatus.textContent = message || '';
  dom.cloudDatabaseSyncStatus.classList.toggle('working', Boolean(running));
  dom.cloudDatabaseSyncStatus.classList.toggle('error', !running && Boolean(error));
  dom.cloudDatabaseSyncStatus.classList.toggle('success', !running && Boolean(success));
}

function buildCloudDatabaseRowKey(row = {}) {
  const provider = normalizeCloudDatabaseProvider(row?.provider, 'other');
  const resourceKey = String(row?.resourceId || row?.resourceName || '').trim().toLowerCase();
  const accountKey = String(row?.accountId || '-').trim().toLowerCase();
  return `${provider}:${accountKey}:${resourceKey}`;
}

function mapCloudDatabaseStatusToBadge(statusRaw) {
  const status = String(statusRaw || '').trim().toLowerCase();
  if (status === 'online') {
    return buildStatusBadge('healthy');
  }
  if (status === 'issue') {
    return buildStatusBadge('warning');
  }
  return buildStatusBadge('open');
}

function formatCloudDatabaseStorageValue(storage = {}) {
  if (!storage || typeof storage !== 'object') {
    return '-';
  }
  const value = Number(storage.value);
  if (!Number.isFinite(value)) {
    return '-';
  }
  const unit = String(storage.unit || '').trim().toLowerCase();
  if (unit === 'bytes' || unit === 'byte') {
    return formatBytes(value);
  }
  if (unit === 'percent') {
    return formatPercent(value);
  }
  if (unit) {
    return `${formatScaledNumber(value)} ${storage.unit}`;
  }
  return formatScaledNumber(value);
}

function filterCloudDatabaseRows() {
  const provider = normalizeCloudDatabaseProvider(state.cloudDatabase.selectedProvider, 'all');
  const searchText = String(state.cloudDatabase.searchText || '').trim().toLowerCase();
  const rows = Array.isArray(state.cloudDatabase.rows) ? state.cloudDatabase.rows : [];

  const filtered = rows.filter((row) => {
    if (provider !== 'all' && normalizeCloudDatabaseProvider(row?.provider, 'other') !== provider) {
      return false;
    }
    if (!searchText) {
      return true;
    }
    const haystack = [
      row?.providerLabel,
      row?.engine,
      row?.resourceName,
      row?.resourceTypeLabel,
      row?.resourceType,
      row?.accountId,
      row?.location,
      row?.statusLabel,
      row?.statusNote,
      row?.securitySummary
    ]
      .map((value) => String(value || '').trim().toLowerCase())
      .filter(Boolean)
      .join(' | ');
    return haystack.includes(searchText);
  });

  state.cloudDatabase.filteredRows = filtered;
  return filtered;
}

function renderCloudDatabaseProviderTabs() {
  if (!dom.cloudDatabaseProviderTabs) {
    return;
  }
  const providers = Array.isArray(state.cloudDatabase.providers) ? state.cloudDatabase.providers : [];
  const totalResources = Array.isArray(state.cloudDatabase.rows) ? state.cloudDatabase.rows.length : 0;
  const selected = normalizeCloudDatabaseProvider(state.cloudDatabase.selectedProvider, 'all');
  const options = [
    {
      id: 'all',
      label: 'All providers',
      resourceCount: totalResources,
      issueCount: providers.reduce((sum, entry) => sum + Number(entry?.issueCount || 0), 0)
    },
    ...providers
  ];

  dom.cloudDatabaseProviderTabs.innerHTML = options
    .map((provider) => {
      const id = normalizeCloudDatabaseProvider(provider?.id, 'all');
      const label = String(provider?.label || getCloudDatabaseProviderLabel(id) || id.toUpperCase());
      const active = id === selected;
      const resourceCount = Number(provider?.resourceCount || 0);
      const issueCount = Number(provider?.issueCount || 0);
      const titleParts = [];
      if (provider?.lastSyncAt) {
        titleParts.push(`Last sync: ${formatDateTime(provider.lastSyncAt)}`);
      }
      if (Number.isFinite(issueCount) && issueCount > 0) {
        titleParts.push(`Issues: ${formatNumber(issueCount)}`);
      }
      return `
        <button
          type="button"
          class="billing-preset-btn cloud-metrics-provider-btn ${active ? 'active' : ''}"
          data-cloud-database-provider="${escapeHtml(id)}"
          title="${escapeHtml(titleParts.join(' | '))}"
        >
          ${escapeHtml(label)}
          <span class="cloud-metrics-provider-meta">${formatNumber(resourceCount)}</span>
        </button>
      `;
    })
    .join('');
}

function renderCloudDatabaseSummaryCards(payload = {}) {
  if (!dom.cloudDatabaseSummaryCards) {
    return;
  }
  const summary = payload?.summary && typeof payload.summary === 'object' ? payload.summary : state.cloudDatabase.summary || {};
  const filteredCount = Number(payload?.filteredResources || state.cloudDatabase.filteredRows?.length || 0);
  const totalCount = Number(payload?.totalResources || state.cloudDatabase.rows?.length || 0);
  const lastSyncAt = payload?.lastSyncAt || state.cloudDatabase.lastSyncAt;
  const cards = [
    { label: 'Databases (filtered)', value: formatNumber(filteredCount) },
    { label: 'Databases (total)', value: formatNumber(totalCount) },
    { label: 'Online', value: formatNumber(summary?.onlineCount || 0) },
    { label: 'Issues', value: formatNumber(summary?.issueCount || 0) },
    { label: 'Unknown', value: formatNumber(summary?.unknownCount || 0) },
    { label: 'Transactions (24h)', value: formatNumber(summary?.totalTransactions24h || 0) },
    { label: 'Last sync', value: escapeHtml(formatDateTime(lastSyncAt)) }
  ];
  dom.cloudDatabaseSummaryCards.innerHTML = cards
    .map(
      (card) => `
        <article class="card">
          <p class="label">${card.label}</p>
          <p class="value">${card.value}</p>
        </article>
      `
    )
    .join('');
}

function renderCloudDatabaseRows() {
  if (!dom.cloudDatabaseBody) {
    return;
  }

  const rows = filterCloudDatabaseRows();
  if (!rows.length) {
    const hasRows = Array.isArray(state.cloudDatabase.rows) && state.cloudDatabase.rows.length > 0;
    dom.cloudDatabaseBody.innerHTML = `
      <tr>
        <td colspan="11" class="muted">
          ${hasRows ? 'No database rows match the current filters.' : 'No cloud database resources discovered yet. Click Sync now.'}
        </td>
      </tr>
    `;
    return;
  }

  const htmlRows = [];
  for (const row of rows) {
    const rowKey = buildCloudDatabaseRowKey(row);
    const expanded = state.cloudDatabase.expandedRowKeys.includes(rowKey);
    const storageUsed = formatCloudDatabaseStorageValue(row?.storageUsed);
    const storageAllocated = formatCloudDatabaseStorageValue(row?.storageAllocated);
    const storageSummary = storageAllocated !== '-' ? `${storageUsed} / ${storageAllocated}` : storageUsed;
    const securitySignals = Array.isArray(row?.securitySignals) ? row.securitySignals : [];
    const securityBadge =
      securitySignals.find((signal) => String(signal?.severity || '').trim().toLowerCase() === 'warning') !== undefined
        ? buildStatusBadge('warning')
        : securitySignals.length
          ? buildStatusBadge('healthy')
          : buildStatusBadge('open');

    const metrics = Array.isArray(row?.metrics) ? row.metrics : [];
    const metricRows = metrics
      .map((metric) => {
        const scale = resolveCloudMetricScale(metric);
        return `
          <tr>
            <td>${escapeHtml(metric?.name || '-')}</td>
            <td>${formatCloudMetricValue(metric?.value, scale)}</td>
            <td>${escapeHtml(scale?.unitLabel || '-')}</td>
            <td>${formatCloudMetricValue(metric?.average, scale)}</td>
            <td>${formatCloudMetricValue(metric?.minimum, scale)}</td>
            <td>${formatCloudMetricValue(metric?.maximum, scale)}</td>
            <td>${formatCloudMetricValue(metric?.total, scale)}</td>
            <td>${escapeHtml(formatDateTime(metric?.timestamp))}</td>
          </tr>
        `;
      })
      .join('');

    const securityLines = securitySignals.length
      ? securitySignals
        .map((signal) => {
          const severity = String(signal?.severity || '').trim().toLowerCase();
          const className = severity === 'warning' ? 'status-badge warning' : 'status-badge healthy';
          const label = String(signal?.label || signal?.key || 'signal').trim();
          return `<span class="${className}">${escapeHtml(label)}</span>`;
        })
        .join(' ')
      : '<span class="muted">No security signals collected.</span>';

    htmlRows.push(`
      <tr>
        <td class="billing-expand-cell">
          <button type="button" class="billing-expand-btn" data-cloud-database-row="${escapeHtml(rowKey)}">${expanded ? '-' : '+'}</button>
        </td>
        <td>${escapeHtml(row?.providerLabel || getCloudDatabaseProviderLabel(row?.provider))}</td>
        <td>${escapeHtml(row?.engine || '-')}</td>
        <td>${escapeHtml(row?.resourceName || '-')}</td>
        <td>${formatNumber(row?.transactions24h || 0)}</td>
        <td>${escapeHtml(storageSummary)}</td>
        <td>${escapeHtml(formatPercent(row?.cpuPercent))}</td>
        <td>${escapeHtml(formatPercent(row?.memoryPercent))}</td>
        <td>${securityBadge}</td>
        <td>${mapCloudDatabaseStatusToBadge(row?.status)}</td>
        <td>${escapeHtml(formatDateTime(row?.fetchedAt || row?.updatedAt || null))}</td>
      </tr>
    `);

    if (!expanded) {
      continue;
    }

    htmlRows.push(`
      <tr class="billing-detail-row">
        <td colspan="11">
          <div class="billing-detail-wrap">
            <p class="muted">
              <strong>Type:</strong> ${escapeHtml(row?.resourceTypeLabel || row?.resourceType || '-')}
              &nbsp;|&nbsp; <strong>Account:</strong> ${escapeHtml(row?.accountId || '-')}
              &nbsp;|&nbsp; <strong>Location:</strong> ${escapeHtml(row?.location || '-')}
              &nbsp;|&nbsp; <strong>Storage used %:</strong> ${escapeHtml(formatPercent(row?.storageUsedPercent))}
            </p>
            <p class="muted"><strong>Status note:</strong> ${escapeHtml(row?.statusNote || '-')}</p>
            <p class="muted"><strong>Security:</strong> ${securityLines}</p>
            ${metrics.length
        ? `
              <table class="billing-detail-table cloud-metrics-metric-table">
                <thead>
                  <tr>
                    <th>Metric</th>
                    <th>Value</th>
                    <th>Unit</th>
                    <th>Avg</th>
                    <th>Min</th>
                    <th>Max</th>
                    <th>Total</th>
                    <th>Timestamp</th>
                  </tr>
                </thead>
                <tbody>${metricRows}</tbody>
              </table>
            `
        : '<p class="muted">No metric values returned for this resource in the latest fetch window.</p>'
      }
          </div>
        </td>
      </tr>
    `);
  }

  dom.cloudDatabaseBody.innerHTML = htmlRows.join('');
}

function applyCloudDatabasePayload(payload = {}, options = {}) {
  state.cloudDatabase.providers = Array.isArray(payload?.providers) ? payload.providers : [];
  state.cloudDatabase.rows = Array.isArray(payload?.rows) ? payload.rows : [];
  state.cloudDatabase.summary = payload?.summary && typeof payload.summary === 'object' ? payload.summary : null;
  state.cloudDatabase.syncStatus = payload?.syncStatus && typeof payload.syncStatus === 'object' ? payload.syncStatus : null;
  state.cloudDatabase.lastSyncAt = payload?.lastSyncAt || null;

  const requestedProvider = normalizeCloudDatabaseProvider(
    payload?.provider || options.provider || state.cloudDatabase.selectedProvider || 'all',
    'all'
  );
  const availableProviders = new Set(
    ['all', ...state.cloudDatabase.providers.map((entry) => normalizeCloudDatabaseProvider(entry?.id, 'all'))]
  );
  const selectedProvider = availableProviders.has(requestedProvider) ? requestedProvider : 'all';
  setSelectedCloudDatabaseProvider(selectedProvider, {
    persist: options.persist !== false
  });

  const rowKeys = new Set(state.cloudDatabase.rows.map((row) => buildCloudDatabaseRowKey(row)));
  state.cloudDatabase.expandedRowKeys = state.cloudDatabase.expandedRowKeys.filter((key) => rowKeys.has(key));

  renderCloudDatabaseProviderTabs();
  renderCloudDatabaseSummaryCards(payload);
  renderCloudDatabaseRows();

  if (dom.cloudDatabaseScopeHint) {
    const providerLabel = getCloudDatabaseProviderLabel(selectedProvider);
    const filteredResources = Number(payload?.filteredResources || state.cloudDatabase.filteredRows.length || 0);
    const totalResources = Number(payload?.totalResources || state.cloudDatabase.rows.length || 0);
    const lastSyncAt = payload?.lastSyncAt ? formatDateTime(payload.lastSyncAt) : 'Not synced yet';
    dom.cloudDatabaseScopeHint.textContent = `${providerLabel} | ${formatNumber(filteredResources)} shown / ${formatNumber(
      totalResources
    )} discovered | Last sync ${lastSyncAt}.`;
  }
}

async function loadCloudDatabase(options = {}) {
  const selectedProvider = normalizeCloudDatabaseProvider(
    options.provider || state.cloudDatabase.selectedProvider || readSavedCloudDatabaseProvider() || 'all',
    'all'
  );
  const payload = await api(`/api/platform/cloud-database?provider=${encodeURIComponent(selectedProvider)}`);
  applyCloudDatabasePayload(payload, {
    provider: selectedProvider,
    persist: options.persist !== false
  });

  const syncStatus = payload?.syncStatus || null;
  if (syncStatus?.running) {
    setCloudDatabaseSyncState({
      running: true,
      message: `Sync in progress for ${getCloudDatabaseProviderLabel(selectedProvider)}...`
    });
    return;
  }
  if (syncStatus?.lastStatus === 'error') {
    setCloudDatabaseSyncState({
      error: true,
      message: syncStatus?.lastError ? `Sync error: ${syncStatus.lastError}` : 'Latest sync failed.'
    });
    return;
  }
  const nextRun = syncStatus?.nextRunAt ? formatDateTime(syncStatus.nextRunAt) : null;
  setCloudDatabaseSyncState({
    success: true,
    message: nextRun ? `Auto-sync every 5 minutes. Next run: ${nextRun}.` : 'Cloud Database loaded.'
  });
}

async function syncCloudDatabase() {
  const selectedProvider = normalizeCloudDatabaseProvider(state.cloudDatabase.selectedProvider || 'all', 'all');
  if (dom.syncCloudDatabaseBtn) {
    dom.syncCloudDatabaseBtn.disabled = true;
    dom.syncCloudDatabaseBtn.textContent = 'Syncing...';
  }
  if (dom.refreshCloudDatabaseBtn) {
    dom.refreshCloudDatabaseBtn.disabled = true;
  }
  setCloudDatabaseSyncState({
    running: true,
    message: `Syncing ${getCloudDatabaseProviderLabel(selectedProvider)} database metrics...`
  });
  try {
    const payload = await api('/api/platform/cloud-database/sync', {
      method: 'POST',
      body: JSON.stringify({
        provider: selectedProvider
      })
    });
    applyCloudDatabasePayload(payload, {
      provider: selectedProvider,
      persist: true
    });
    setCloudDatabaseSyncState({
      success: true,
      message: 'Cloud Database sync completed.'
    });
    showToast('Cloud Database sync completed.');
  } finally {
    if (dom.syncCloudDatabaseBtn) {
      dom.syncCloudDatabaseBtn.disabled = false;
      dom.syncCloudDatabaseBtn.textContent = 'Sync now';
    }
    if (dom.refreshCloudDatabaseBtn) {
      dom.refreshCloudDatabaseBtn.disabled = false;
    }
  }
}

function handleCloudDatabaseProviderTabsClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const button = target.closest('[data-cloud-database-provider]');
  if (!(button instanceof HTMLElement)) {
    return;
  }
  const provider = normalizeCloudDatabaseProvider(button.dataset.cloudDatabaseProvider || '', 'all');
  setSelectedCloudDatabaseProvider(provider, { persist: true });
  state.cloudDatabase.expandedRowKeys = [];
  void loadCloudDatabase({
    provider,
    persist: true
  }).catch((error) => {
    showToast(error.message || 'Failed to switch Cloud Database provider.', true);
  });
}

function handleCloudDatabaseBodyClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const button = target.closest('[data-cloud-database-row]');
  if (!(button instanceof HTMLElement)) {
    return;
  }
  const rowKey = String(button.dataset.cloudDatabaseRow || '').trim();
  if (!rowKey) {
    return;
  }
  const expanded = state.cloudDatabase.expandedRowKeys.includes(rowKey);
  state.cloudDatabase.expandedRowKeys = expanded
    ? state.cloudDatabase.expandedRowKeys.filter((key) => key !== rowKey)
    : [...state.cloudDatabase.expandedRowKeys, rowKey];
  renderCloudDatabaseRows();
}

function handleCloudDatabaseSearchInput() {
  state.cloudDatabase.searchText = String(dom.cloudDatabaseSearchInput?.value || '').trim();
  saveCloudDatabaseSearchText(state.cloudDatabase.searchText);
  renderCloudDatabaseRows();
}

function clearCloudDatabaseFilters() {
  state.cloudDatabase.searchText = '';
  saveCloudDatabaseSearchText(state.cloudDatabase.searchText);
  if (dom.cloudDatabaseSearchInput) {
    dom.cloudDatabaseSearchInput.value = '';
  }
  renderCloudDatabaseRows();
}

function renderLiveGroupFilterOptions() {
  if (!dom.liveGroupFilter) {
    return;
  }

  const groups = Array.isArray(state.live.availableGroups) ? state.live.availableGroups : [];
  const selectedGroupName = normalizeLiveGroupName(state.live.selectedGroupName);
  const groupSearchText = String(state.live.groupSearchText || '').trim().toLowerCase();
  let visibleGroups = groups.filter((groupName) => {
    if (!groupSearchText) {
      return true;
    }
    return String(groupName || '').toLowerCase().includes(groupSearchText);
  });
  if (selectedGroupName !== 'all' && groups.includes(selectedGroupName) && !visibleGroups.includes(selectedGroupName)) {
    visibleGroups = [selectedGroupName, ...visibleGroups];
  }

  const options = [
    { value: 'all', label: 'All groups' },
    ...visibleGroups.map((groupName) => ({
      value: groupName,
      label: groupName
    }))
  ];

  dom.liveGroupFilter.innerHTML = options
    .map((option) => `<option value="${escapeHtml(option.value)}">${escapeHtml(option.label)}</option>`)
    .join('');

  const availableValues = new Set(options.map((option) => option.value));
  const resolvedValue = availableValues.has(selectedGroupName) ? selectedGroupName : 'all';
  dom.liveGroupFilter.value = resolvedValue;
  state.live.selectedGroupName = resolvedValue;
}

function getEffectiveLiveGroupName() {
  return normalizeLivePinnedGroupName(state.live.pinnedGroupName) || normalizeLiveGroupName(state.live.selectedGroupName);
}

function applyLivePinnedModeUi() {
  const pinnedGroupName = normalizeLivePinnedGroupName(state.live.pinnedGroupName);
  const isPinned = Boolean(pinnedGroupName);
  if (dom.liveGroupFilterWrap) {
    dom.liveGroupFilterWrap.hidden = isPinned;
    dom.liveGroupFilterWrap.style.display = isPinned ? 'none' : '';
  }
}

function getFilteredLiveRows() {
  const selectedGroupName = getEffectiveLiveGroupName();
  const searchText = String(state.live.searchText || '').trim().toLowerCase();
  const rawRows = Array.isArray(state.live.rows) ? state.live.rows : [];

  return rawRows.filter((row) => {
    const groupName = String(row?.groupName || '').trim();
    const groupMatch = selectedGroupName === 'all' || groupName === selectedGroupName;
    if (!groupMatch) {
      return false;
    }
    if (!searchText) {
      return true;
    }
    const haystack = [
      row?.groupName,
      row?.resourceName,
      row?.resourceType,
      row?.internalIp,
      row?.publicIp,
      row?.status,
      row?.note
    ]
      .map((value) => String(value || '').toLowerCase())
      .join(' ');
    return haystack.includes(searchText);
  });
}

function renderLiveRows() {
  if (!dom.liveBody) {
    return;
  }

  const filteredRows = getFilteredLiveRows();
  state.live.filteredRows = filteredRows;

  if (!filteredRows.length) {
    dom.liveBody.innerHTML = `
      <tr class="live-empty-row">
        <td colspan="10">No VSAx data matches the selected filters.</td>
      </tr>
    `;
  } else {
    dom.liveBody.innerHTML = filteredRows
      .map(
        (row) => `
        <tr>
          <td>${escapeHtml(row.groupName || '-')}</td>
          <td>${escapeHtml(row.resourceName || '-')}</td>
          <td>${escapeHtml(formatPercent(row.cpuUsagePercent))}</td>
          <td>${escapeHtml(formatPercent(row.memoryUsagePercent))}</td>
          <td>${escapeHtml(formatPercent(row.diskUsagePercent))}</td>
          <td>${escapeHtml(row.internalIp || '-')}</td>
          <td>${escapeHtml(row.publicIp || '-')}</td>
          <td>${buildStatusBadge(row.status || 'healthy')}</td>
          <td>${formatDateTime(row.updatedAt)}</td>
          <td>${escapeHtml(row.note || '-')}</td>
        </tr>
      `
      )
      .join('');
  }

  if (dom.liveScopeHint) {
    const pinnedGroupName = normalizeLivePinnedGroupName(state.live.pinnedGroupName);
    const selectedGroupName = getEffectiveLiveGroupName();
    const groupLabel = selectedGroupName === 'all' ? 'All groups' : selectedGroupName;
    const modeText = pinnedGroupName ? `Pinned group (${pinnedGroupName})` : groupLabel;
    dom.liveScopeHint.textContent = `${filteredRows.length}/${state.live.rows.length} device(s) | ${groupLabel}`;
    if (pinnedGroupName) {
      dom.liveScopeHint.textContent = `${filteredRows.length}/${state.live.rows.length} device(s) | ${modeText}`;
    }
  }
}

async function loadLive(options = {}) {
  const refresh = Boolean(options.refresh);
  const selectedGroupName = getEffectiveLiveGroupName();
  const query = new URLSearchParams();
  if (selectedGroupName !== 'all') {
    query.set('groupNames', selectedGroupName);
  }
  if (refresh) {
    query.set('refresh', 'true');
  }
  const suffix = query.toString() ? `?${query.toString()}` : '';
  const payload = await api(`/api/platform/live${suffix}`);
  const rawRows = Array.isArray(payload.rows) ? payload.rows : [];
  const rows = rawRows
    .filter((row) => {
      const provider = String(row?.provider || '').trim().toLowerCase();
      const resourceType = String(row?.resourceType || '').trim().toLowerCase();
      if (resourceType === 'vsax-device') {
        return true;
      }
      if (provider === 'vsax' && resourceType.includes('device')) {
        return true;
      }
      if (provider === 'private' && resourceType === 'vsax-device') {
        return true;
      }
      return false;
    })
    .map((row) => ({
      ...row,
      groupName: String(row?.groupName || row?.accountId || '').trim() || null
    }));
  const availableGroupsRaw = Array.isArray(payload.availableGroups) ? payload.availableGroups : [];
  const availableGroups = Array.from(
    new Set(
      [...availableGroupsRaw, ...rows.map((row) => row.groupName)]
        .map((groupName) => String(groupName || '').trim())
        .filter(Boolean)
    )
  ).sort((left, right) => String(left || '').localeCompare(String(right || ''), undefined, { sensitivity: 'base', numeric: true }));

  state.live.rows = rows;
  state.live.availableGroups = availableGroups;

  if (selectedGroupName !== 'all' && !availableGroups.includes(selectedGroupName)) {
    state.live.selectedGroupName = 'all';
    saveLiveGroupName('all');
  } else {
    state.live.selectedGroupName = selectedGroupName;
  }

  renderLiveGroupFilterOptions();
  applyLivePinnedModeUi();
  if (dom.liveSearchInput && dom.liveSearchInput.value !== String(state.live.searchText || '')) {
    dom.liveSearchInput.value = String(state.live.searchText || '');
  }
  renderLiveRows();
}

function handleLiveGroupFilterChange() {
  if (!dom.liveGroupFilter) {
    return;
  }
  if (normalizeLivePinnedGroupName(state.live.pinnedGroupName)) {
    return;
  }
  state.live.selectedGroupName = normalizeLiveGroupName(dom.liveGroupFilter.value);
  saveLiveGroupName(state.live.selectedGroupName);
  void loadLive({ refresh: true }).catch((error) => {
    showToast(error.message || 'Failed to load VSAx live data.', true);
  });
}

function handleLiveSearchInput() {
  state.live.searchText = String(dom.liveSearchInput?.value || '').trim();
  saveLiveSearchText(state.live.searchText);
  renderLiveRows();
}

function handleLiveGroupSearchInput() {
  state.live.groupSearchText = String(dom.liveGroupSearchInput?.value || '').trim();
  saveLiveGroupSearchText(state.live.groupSearchText);
  renderLiveGroupFilterOptions();
  renderLiveRows();
}

function clearLiveFilters() {
  if (!normalizeLivePinnedGroupName(state.live.pinnedGroupName)) {
    state.live.selectedGroupName = 'all';
    saveLiveGroupName('all');
  }
  state.live.groupSearchText = '';
  saveLiveGroupSearchText('');
  if (dom.liveGroupSearchInput) {
    dom.liveGroupSearchInput.value = '';
  }
  state.live.searchText = '';
  saveLiveSearchText('');
  if (dom.liveSearchInput) {
    dom.liveSearchInput.value = '';
  }
  void loadLive({ refresh: true }).catch((error) => {
    showToast(error.message || 'Failed to reload VSAx live data.', true);
  });
}

function buildVpnStatusPill(row = {}) {
  const isOnline = Boolean(row?.isOnline);
  const statusText = isOnline ? 'online' : 'issue';
  return `
    <span class="vpn-status-pill">
      <span class="vpn-status-dot ${isOnline ? 'online' : 'issue'}" aria-hidden="true"></span>
      ${escapeHtml(statusText)}
    </span>
  `;
}

function renderFirewallSyncStatus() {
  if (!dom.firewallSyncStatus) {
    return;
  }
  const sync = state.firewall.sync || {};
  const parts = [];
  if (sync.running) {
    parts.push('Sync running...');
  } else if (sync.lastStatus) {
    parts.push(`Last status: ${String(sync.lastStatus).toUpperCase()}`);
  }
  if (sync.lastFinishedAt) {
    parts.push(`Updated ${formatDateTime(sync.lastFinishedAt)}`);
  }
  if (sync.nextRunAt) {
    parts.push(`Next run ${formatDateTime(sync.nextRunAt)}`);
  }
  dom.firewallSyncStatus.textContent = parts.join(' | ') || 'No firewall sync data yet.';
}

function renderFirewallCards() {
  if (!dom.firewallCards) {
    return;
  }
  const hosts = Array.isArray(state.firewall.hosts) ? state.firewall.hosts : [];
  const totalHosts = hosts.length;
  const totalServices = hosts.reduce((sum, row) => sum + Number(row?.serviceCount || 0), 0);
  const totalVpn = hosts.reduce((sum, row) => sum + Number(row?.vpnCount || 0), 0);
  const totalIssues = hosts.reduce((sum, row) => sum + Number(row?.issueCount || 0), 0);

  const cards = [
    { label: 'Hosts', value: formatNumber(totalHosts) },
    { label: 'Services', value: formatNumber(totalServices) },
    { label: 'VPN services', value: formatNumber(totalVpn) },
    { label: 'Issues', value: formatNumber(totalIssues) }
  ];

  dom.firewallCards.innerHTML = cards
    .map(
      (card) => `
      <article class="card">
        <p class="label">${escapeHtml(card.label)}</p>
        <p class="value">${escapeHtml(card.value)}</p>
      </article>
    `
    )
    .join('');
}

function renderFirewallHostFilterOptions() {
  if (!dom.firewallHostFilter) {
    return;
  }
  const hosts = Array.from(new Set((state.firewall.hosts || []).map((row) => String(row?.hostId || '').trim()).filter(Boolean))).sort((left, right) =>
    left.localeCompare(right, undefined, { sensitivity: 'base', numeric: true })
  );
  const selected = String(state.firewall.hostFilter || 'all').trim() || 'all';
  const options = [{ value: 'all', label: 'All hosts' }, ...hosts.map((hostId) => ({ value: hostId, label: hostId }))];

  dom.firewallHostFilter.innerHTML = options
    .map((option) => `<option value="${escapeHtml(option.value)}">${escapeHtml(option.label)}</option>`)
    .join('');

  const allowed = new Set(options.map((option) => option.value));
  dom.firewallHostFilter.value = allowed.has(selected) ? selected : 'all';
  state.firewall.hostFilter = dom.firewallHostFilter.value;
}

function getFilteredFirewallIssueRows() {
  const hostFilter = String(state.firewall.hostFilter || 'all').trim();
  const searchText = String(state.firewall.searchText || '').trim().toLowerCase();
  const rows = Array.isArray(state.firewall.issueRows) ? state.firewall.issueRows : [];
  return rows.filter((row) => {
    const hostMatch = hostFilter === 'all' || String(row?.hostId || '') === hostFilter;
    if (!hostMatch) {
      return false;
    }
    if (!searchText) {
      return true;
    }
    const haystack = [
      row?.hostId,
      row?.serviceDescription,
      row?.gatewayIp,
      row?.summaryText,
      row?.stateLabel
    ]
      .map((value) => String(value || '').toLowerCase())
      .join(' ');
    return haystack.includes(searchText);
  });
}

function renderFirewallSummaryRows() {
  if (!dom.firewallSummaryBody) {
    return;
  }
  const hostFilter = String(state.firewall.hostFilter || 'all').trim();
  const rows = (state.firewall.hosts || []).filter((row) => hostFilter === 'all' || String(row?.hostId || '') === hostFilter);
  if (!rows.length) {
    dom.firewallSummaryBody.innerHTML = `
      <tr class="live-empty-row">
        <td colspan="7">No firewall host stats available.</td>
      </tr>
    `;
    return;
  }
  dom.firewallSummaryBody.innerHTML = rows
    .map(
      (row) => `
      <tr>
        <td>${escapeHtml(row.hostId || '-')}</td>
        <td>${formatNumber(row.serviceCount || 0)}</td>
        <td>${formatNumber(row.vpnCount || 0)}</td>
        <td>${formatNumber(row.healthyCount || 0)}</td>
        <td>${formatNumber(row.issueCount || 0)}</td>
        <td>${formatNumber(row.vpnTransmittingCount || 0)}</td>
        <td>${formatDateTime(row.updatedAt)}</td>
      </tr>
    `
    )
    .join('');
}

function renderFirewallIssueRows() {
  if (!dom.firewallIssuesBody) {
    return;
  }
  const rows = getFilteredFirewallIssueRows().slice(0, 1000);
  if (!rows.length) {
    dom.firewallIssuesBody.innerHTML = `
      <tr class="live-empty-row">
        <td colspan="6">No matching firewall service issues.</td>
      </tr>
    `;
    return;
  }
  dom.firewallIssuesBody.innerHTML = rows
    .map(
      (row) => `
      <tr>
        <td>${escapeHtml(row.hostId || '-')}</td>
        <td>${escapeHtml(row.serviceDescription || '-')}</td>
        <td>${escapeHtml(row.gatewayIp || '-')}</td>
        <td>${buildStatusBadge(row.stateLabel || 'unknown')}</td>
        <td>${escapeHtml(row.summaryText || '-')}</td>
        <td>${formatDateTime(row.updatedAt)}</td>
      </tr>
    `
    )
    .join('');
}

function renderFirewallView() {
  renderFirewallSyncStatus();
  renderFirewallCards();
  renderFirewallHostFilterOptions();
  renderFirewallSummaryRows();
  renderFirewallIssueRows();
}

async function loadFirewall(options = {}) {
  const refresh = Boolean(options.refresh);
  const query = new URLSearchParams();
  if (refresh) {
    query.set('refresh', 'true');
  }
  const suffix = query.toString() ? `?${query.toString()}` : '';
  const payload = await api(`/api/platform/firewall${suffix}`);

  state.firewall.hosts = Array.isArray(payload.hosts) ? payload.hosts : [];
  state.firewall.services = Array.isArray(payload.services) ? payload.services : [];
  state.firewall.issueRows = Array.isArray(payload.issueRows) ? payload.issueRows : [];
  state.firewall.sync = payload.sync && typeof payload.sync === 'object' ? payload.sync : null;
  state.firewall.config = payload.config && typeof payload.config === 'object' ? payload.config : null;

  renderFirewallView();
}

async function syncFirewallNow() {
  const payload = await api('/api/platform/firewall/sync', {
    method: 'POST',
    body: JSON.stringify({})
  });
  const summary = payload?.summary || payload?.sync?.lastSummary || null;
  if (summary?.hosts?.length) {
    showToast(`Firewall sync completed for ${summary.hosts.length} host(s).`);
  } else {
    showToast('Firewall sync completed.');
  }
  await loadFirewall();
  await loadVpn();
}

function renderVpnHostFilterOptions() {
  if (!dom.vpnHostFilter) {
    return;
  }
  const hosts = Array.from(
    new Set((state.vpn.rows || []).map((row) => String(row?.hostId || '').trim()).filter(Boolean))
  ).sort((left, right) => left.localeCompare(right, undefined, { sensitivity: 'base', numeric: true }));
  const selected = String(state.vpn.hostFilter || 'all').trim() || 'all';
  const options = [{ value: 'all', label: 'All hosts' }, ...hosts.map((hostId) => ({ value: hostId, label: hostId }))];

  dom.vpnHostFilter.innerHTML = options
    .map((option) => `<option value="${escapeHtml(option.value)}">${escapeHtml(option.label)}</option>`)
    .join('');
  const available = new Set(options.map((option) => option.value));
  dom.vpnHostFilter.value = available.has(selected) ? selected : 'all';
  state.vpn.hostFilter = dom.vpnHostFilter.value;
}

function getFilteredVpnRows() {
  const hostFilter = String(state.vpn.hostFilter || 'all').trim();
  const searchText = String(state.vpn.searchText || '').trim().toLowerCase();
  const rows = Array.isArray(state.vpn.rows) ? state.vpn.rows : [];
  return rows.filter((row) => {
    const hostMatch = hostFilter === 'all' || String(row?.hostId || '') === hostFilter;
    if (!hostMatch) {
      return false;
    }
    if (!searchText) {
      return true;
    }
    const haystack = [
      row?.hostId,
      row?.vpnNumber,
      row?.gatewayIp,
      row?.serviceDescription,
      (row?.accountTags || []).join(','),
      row?.notes,
      row?.summaryText
    ]
      .map((value) => String(value || '').toLowerCase())
      .join(' ');
    return haystack.includes(searchText);
  });
}

function renderVpnRows() {
  if (!dom.vpnBody) {
    return;
  }
  const rows = getFilteredVpnRows();
  const totalRows = Array.isArray(state.vpn.rows) ? state.vpn.rows.length : 0;

  if (!rows.length) {
    dom.vpnBody.innerHTML = `
      <tr class="live-empty-row">
        <td colspan="10">No VPN rows match the selected filters.</td>
      </tr>
    `;
  } else {
    dom.vpnBody.innerHTML = rows
      .map((row) => {
        const gatewayIp = String(row?.gatewayIp || '').trim();
        const gatewayEscaped = escapeHtml(gatewayIp || '');
        const saveDisabled = gatewayIp ? '' : 'disabled';
        const transmittingText = row?.transmitting === null || row?.transmitting === undefined ? '-' : row.transmitting ? 'Yes' : 'No';
        return `
          <tr data-vpn-gateway-ip="${gatewayEscaped}">
            <td>${escapeHtml(row.hostId || '-')}</td>
            <td><input class="vpn-meta-input" data-vpn-field="vpn-number" value="${escapeHtml(row.vpnNumber || '')}" placeholder="VPN number" /></td>
            <td>${gatewayEscaped || '-'}</td>
            <td>${escapeHtml(row.serviceDescription || '-')}</td>
            <td>${buildVpnStatusPill(row)}</td>
            <td>${escapeHtml(transmittingText)}</td>
            <td><input class="vpn-meta-input tags" data-vpn-field="account-tags" value="${escapeHtml((row.accountTags || []).join(', '))}" placeholder="tag1, tag2" /></td>
            <td><input class="vpn-meta-input" data-vpn-field="notes" value="${escapeHtml(row.notes || '')}" placeholder="Notes" /></td>
            <td>${formatDateTime(row.updatedAt)}</td>
            <td><button type="button" class="btn secondary tag-edit-btn" data-vpn-save="${gatewayEscaped}" ${saveDisabled}>Save</button></td>
          </tr>
        `;
      })
      .join('');
  }

  if (dom.vpnScopeHint) {
    const hostLabel = String(state.vpn.hostFilter || 'all') === 'all' ? 'All hosts' : state.vpn.hostFilter;
    dom.vpnScopeHint.textContent = `${rows.length}/${totalRows} VPN service(s) | ${hostLabel}`;
  }
}

function renderVpnView() {
  renderVpnHostFilterOptions();
  if (dom.vpnSortSelect) {
    dom.vpnSortSelect.value = normalizeVpnSortValue(state.vpn.sortBy);
  }
  renderVpnRows();
}

async function loadVpn(options = {}) {
  const refresh = Boolean(options.refresh);
  const query = new URLSearchParams();
  query.set('sortBy', normalizeVpnSortValue(state.vpn.sortBy));
  if (refresh) {
    query.set('refresh', 'true');
  }
  const payload = await api(`/api/platform/vpn?${query.toString()}`);
  state.vpn.rows = Array.isArray(payload.rows) ? payload.rows : [];
  renderVpnView();
}

async function saveVpnMetadataForRow(rowElement) {
  const row = rowElement instanceof HTMLElement ? rowElement : null;
  if (!row) {
    return;
  }
  const gatewayIp = String(row.getAttribute('data-vpn-gateway-ip') || '').trim();
  if (!gatewayIp) {
    throw new Error('Gateway IP is required to save VPN metadata.');
  }
  const vpnNumberInput = row.querySelector('input[data-vpn-field=\"vpn-number\"]');
  const accountTagsInput = row.querySelector('input[data-vpn-field=\"account-tags\"]');
  const notesInput = row.querySelector('input[data-vpn-field=\"notes\"]');

  const payload = await api('/api/platform/vpn/metadata', {
    method: 'PUT',
    body: JSON.stringify({
      gatewayIp,
      vpnNumber: vpnNumberInput instanceof HTMLInputElement ? vpnNumberInput.value : '',
      accountTags: accountTagsInput instanceof HTMLInputElement ? accountTagsInput.value : '',
      notes: notesInput instanceof HTMLInputElement ? notesInput.value : ''
    })
  });

  const metadata = payload?.metadata || {};
  const normalizedGateway = String(metadata.gatewayIp || gatewayIp).trim().toLowerCase();
  state.vpn.rows = state.vpn.rows.map((item) => {
    if (String(item?.gatewayIp || '').trim().toLowerCase() !== normalizedGateway) {
      return item;
    }
    return {
      ...item,
      vpnNumber: metadata.vpnNumber || '',
      accountTags: Array.isArray(metadata.accountTags) ? metadata.accountTags : [],
      notes: metadata.notes || '',
      metadataUpdatedAt: metadata.updatedAt || item.metadataUpdatedAt || null
    };
  });
  renderVpnRows();
  showToast(`Saved VPN metadata for ${metadata.gatewayIp || gatewayIp}.`);
}

function handleFirewallHostFilterChange() {
  if (!dom.firewallHostFilter) {
    return;
  }
  state.firewall.hostFilter = String(dom.firewallHostFilter.value || 'all').trim() || 'all';
  saveFirewallHostFilter(state.firewall.hostFilter);
  renderFirewallSummaryRows();
  renderFirewallIssueRows();
}

function handleFirewallSearchInput() {
  state.firewall.searchText = String(dom.firewallSearchInput?.value || '').trim();
  saveFirewallSearchText(state.firewall.searchText);
  renderFirewallIssueRows();
}

function handleVpnSearchInput() {
  state.vpn.searchText = String(dom.vpnSearchInput?.value || '').trim();
  saveVpnSearchText(state.vpn.searchText);
  renderVpnRows();
}

function handleVpnHostFilterChange() {
  if (!dom.vpnHostFilter) {
    return;
  }
  state.vpn.hostFilter = String(dom.vpnHostFilter.value || 'all').trim() || 'all';
  saveVpnHostFilter(state.vpn.hostFilter);
  renderVpnRows();
}

function handleVpnSortChange() {
  if (!dom.vpnSortSelect) {
    return;
  }
  state.vpn.sortBy = normalizeVpnSortValue(dom.vpnSortSelect.value || 'status');
  saveVpnSort(state.vpn.sortBy);
  void loadVpn().catch((error) => {
    showToast(error.message || 'Failed to sort VPN rows.', true);
  });
}

function clearVpnFilters() {
  state.vpn.hostFilter = 'all';
  state.vpn.searchText = '';
  if (dom.vpnHostFilter) {
    dom.vpnHostFilter.value = 'all';
  }
  if (dom.vpnSearchInput) {
    dom.vpnSearchInput.value = '';
  }
  saveVpnHostFilter('all');
  saveVpnSearchText('');
  renderVpnRows();
}

async function handleVpnTableClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const button = target.closest('[data-vpn-save]');
  if (!(button instanceof HTMLElement)) {
    return;
  }
  const row = button.closest('tr');
  if (!(row instanceof HTMLElement)) {
    return;
  }
  button.setAttribute('disabled', 'disabled');
  try {
    await saveVpnMetadataForRow(row);
  } finally {
    button.removeAttribute('disabled');
  }
}

async function loadSecurity() {
  const payload = await api('/api/platform/security');
  const rows = Array.isArray(payload.findings) ? payload.findings : [];
  const body = document.getElementById('securityBody');

  body.innerHTML = rows
    .map(
      (row) => `
      <tr>
        <td>${row.provider}</td>
        <td>${buildStatusBadge(row.severity || 'medium')}</td>
        <td>${row.resourceName || '-'}</td>
        <td>${row.rule}</td>
        <td>${buildStatusBadge(row.status || 'open')}</td>
        <td>${formatDateTime(row.observedAt)}</td>
      </tr>
    `
    )
    .join('');
}

function setDbBackupConfigStatus(message, isError = false) {
  if (!dom.dbBackupConfigStatus) {
    return;
  }
  dom.dbBackupConfigStatus.textContent = String(message || '').trim();
  dom.dbBackupConfigStatus.classList.toggle('negative', Boolean(isError && message));
}

function renderDbBackupRuntimeStatus(status = null) {
  if (!dom.dbBackupRuntimeStatus) {
    return;
  }
  if (!status || typeof status !== 'object') {
    dom.dbBackupRuntimeStatus.textContent = '';
    dom.dbBackupRuntimeStatus.classList.remove('negative');
    return;
  }

  const parts = [];
  if (status.enabled) {
    parts.push('Scheduler: enabled');
    if (status.bucket) {
      parts.push(`bucket=${status.bucket}`);
    }
    if (status.nextRunAt) {
      parts.push(`next=${formatDateTime(status.nextRunAt)}`);
    }
    if (status.lastStatus === 'degraded') {
      parts.push('state=degraded (using local queue)');
    }
  } else {
    parts.push(`Scheduler: disabled (${status.disabledReason || 'not configured'})`);
  }

  if (status.lastSuccessAt) {
    parts.push(`last success=${formatDateTime(status.lastSuccessAt)}`);
  }
  if (status.lastKey) {
    parts.push(`last object=${status.lastKey}`);
  }
  if (status.compressionEnabled !== undefined) {
    parts.push(`compression=${status.compressionEnabled ? `gzip(level ${status.compressionLevel || 6})` : 'off'}`);
  }
  if (Number.isFinite(Number(status.queuedFiles)) && Number(status.queuedFiles) > 0) {
    parts.push(`queued=${formatNumber(status.queuedFiles)} (${formatBytes(status.queuedBytes || 0)})`);
  }
  if (status.queueLastError) {
    parts.push(`queue warning=${status.queueLastError}`);
  }
  if (status.lastLifecycleError) {
    parts.push(`lifecycle warning=${status.lastLifecycleError}`);
  }

  dom.dbBackupRuntimeStatus.textContent = parts.join(' | ');
  const isError = status.lastStatus === 'error' && Boolean(status.lastError);
  dom.dbBackupRuntimeStatus.classList.toggle('negative', isError);
}

function applyDbBackupConfigToForm(config = {}) {
  if (!dom.dbBackupForm) {
    return;
  }
  if (dom.dbBackupEnabled) dom.dbBackupEnabled.checked = Boolean(config.enabled);
  if (dom.dbBackupBucket) dom.dbBackupBucket.value = String(config.bucket || '');
  if (dom.dbBackupPrefix) dom.dbBackupPrefix.value = String(config.prefix || '');
  if (dom.dbBackupRegion) dom.dbBackupRegion.value = String(config.region || '');
  if (dom.dbBackupEndpoint) dom.dbBackupEndpoint.value = String(config.endpoint || '');
  if (dom.dbBackupAccessKeyId) dom.dbBackupAccessKeyId.value = String(config.accessKeyId || '');
  if (dom.dbBackupSecretAccessKey) dom.dbBackupSecretAccessKey.value = '';
  if (dom.dbBackupSessionToken) dom.dbBackupSessionToken.value = '';
  if (dom.dbBackupIntervalMs) dom.dbBackupIntervalMs.value = String(config.intervalMs || 3600000);
  if (dom.dbBackupRetentionDays) dom.dbBackupRetentionDays.value = String(config.retentionDays || 15);
  if (dom.dbBackupLifecycleRuleId) dom.dbBackupLifecycleRuleId.value = String(config.lifecycleRuleId || '');
  if (dom.dbBackupTmpDir) dom.dbBackupTmpDir.value = String(config.tempDir || '');
  if (dom.dbBackupForcePathStyle) dom.dbBackupForcePathStyle.checked = Boolean(config.forcePathStyle);
  if (dom.dbBackupRunOnStartup) dom.dbBackupRunOnStartup.checked = Boolean(config.runOnStartup);
  if (dom.dbBackupApplyLifecycle) dom.dbBackupApplyLifecycle.checked = Boolean(config.applyLifecycle);
  if (dom.dbBackupClearAccessKeyId) dom.dbBackupClearAccessKeyId.checked = false;
  if (dom.dbBackupClearSecretAccessKey) dom.dbBackupClearSecretAccessKey.checked = false;
  if (dom.dbBackupClearSessionToken) dom.dbBackupClearSessionToken.checked = false;

  const meta = [];
  if (config.hasAccessKeyId) {
    meta.push('Saved access key ID present');
  }
  if (config.hasSecretAccessKey) {
    meta.push('Saved secret key present');
  }
  if (config.hasSessionToken) {
    meta.push('Saved session token present');
  }
  setDbBackupConfigStatus(meta.length ? meta.join(' | ') : 'No saved backup credentials.');
}

async function loadDbBackupConfig() {
  if (!dom.dbBackupForm) {
    return;
  }
  const payload = await api('/api/admin/db-backup/config');
  applyDbBackupConfigToForm(payload?.uiConfig || {});
  renderDbBackupRuntimeStatus(payload?.status || null);
}

function buildDbBackupConfigPayloadFromForm() {
  const payload = {
    enabled: Boolean(dom.dbBackupEnabled?.checked),
    bucket: String(dom.dbBackupBucket?.value || '').trim(),
    prefix: String(dom.dbBackupPrefix?.value || '').trim(),
    region: String(dom.dbBackupRegion?.value || '').trim(),
    endpoint: String(dom.dbBackupEndpoint?.value || '').trim(),
    intervalMs: Number(dom.dbBackupIntervalMs?.value || 3600000),
    retentionDays: Number(dom.dbBackupRetentionDays?.value || 15),
    lifecycleRuleId: String(dom.dbBackupLifecycleRuleId?.value || '').trim(),
    tempDir: String(dom.dbBackupTmpDir?.value || '').trim(),
    forcePathStyle: Boolean(dom.dbBackupForcePathStyle?.checked),
    runOnStartup: Boolean(dom.dbBackupRunOnStartup?.checked),
    applyLifecycle: Boolean(dom.dbBackupApplyLifecycle?.checked)
  };

  const clearAccessKeyId = Boolean(dom.dbBackupClearAccessKeyId?.checked);
  const accessKeyId = String(dom.dbBackupAccessKeyId?.value || '').trim();
  if (accessKeyId || clearAccessKeyId) {
    payload.accessKeyId = accessKeyId;
    payload.clearAccessKeyId = clearAccessKeyId;
  }

  const clearSecretAccessKey = Boolean(dom.dbBackupClearSecretAccessKey?.checked);
  const secretAccessKey = String(dom.dbBackupSecretAccessKey?.value || '').trim();
  if (secretAccessKey || clearSecretAccessKey) {
    payload.secretAccessKey = secretAccessKey;
    payload.clearSecretAccessKey = clearSecretAccessKey;
  }

  const clearSessionToken = Boolean(dom.dbBackupClearSessionToken?.checked);
  const sessionToken = String(dom.dbBackupSessionToken?.value || '').trim();
  if (sessionToken || clearSessionToken) {
    payload.sessionToken = sessionToken;
    payload.clearSessionToken = clearSessionToken;
  }

  return payload;
}

async function saveDbBackupConfig(event) {
  event.preventDefault();
  if (!dom.dbBackupForm) {
    return;
  }
  setDbBackupConfigStatus('Saving DB backup config...');
  const payload = buildDbBackupConfigPayloadFromForm();
  const response = await api('/api/admin/db-backup/config', {
    method: 'PUT',
    body: JSON.stringify({ config: payload })
  });
  applyDbBackupConfigToForm(response?.uiConfig || {});
  renderDbBackupRuntimeStatus(response?.status || null);
  setDbBackupConfigStatus('DB backup config saved.');
  showToast('DB backup config saved.');
}

async function runDbBackupNow() {
  setDbBackupConfigStatus('Running DB backup now...');
  const response = await api('/api/admin/db-backup/run', { method: 'POST' });
  renderDbBackupRuntimeStatus(response?.status || null);
  if (response?.ok) {
    setDbBackupConfigStatus('DB backup completed.');
    showToast('DB backup completed.');
    return;
  }
  if (response?.skipped) {
    setDbBackupConfigStatus(response?.result?.reason || 'DB backup skipped.');
    showToast(response?.result?.reason || 'DB backup skipped.', false);
    return;
  }
  setDbBackupConfigStatus('DB backup run failed.', true);
}

async function loadApiKeys() {
  if (!dom.apiKeysBody) {
    return;
  }
  const payload = await api('/api/keys');
  const rows = Array.isArray(payload.keys) ? payload.keys : [];
  const body = dom.apiKeysBody;

  body.innerHTML = rows
    .map(
      (row) => `
      <tr>
        <td>${row.name}</td>
        <td>${row.keyPrefix}</td>
        <td>${formatDateTime(row.createdAt)}</td>
        <td>${formatDateTime(row.lastUsedAt)}</td>
        <td>${buildStatusBadge(row.isActive ? 'healthy' : 'warning')}</td>
        <td>
          <button
            type="button"
            class="btn danger"
            data-api-key-delete="${escapeHtml(row.id || '')}"
          >
            Delete
          </button>
        </td>
      </tr>
    `
    )
    .join('');
}

async function createApiKey() {
  if (!dom.apiKeyOutput) {
    return;
  }
  const name = window.prompt('API key name', 'Third-party integration');
  if (!name) {
    return;
  }

  const payload = await api('/api/keys', {
    method: 'POST',
    body: JSON.stringify({ name })
  });

  dom.apiKeyOutput.textContent = payload?.key?.apiKey || '';
  showToast('API key created. Copy it now, it is shown once.');
  await loadApiKeys();
}

async function handleApiKeysTableClick(event) {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  const button = target.closest('[data-api-key-delete]');
  if (!(button instanceof HTMLElement)) {
    return;
  }
  const keyId = String(button.dataset.apiKeyDelete || '').trim();
  if (!keyId) {
    return;
  }
  const confirmed = window.confirm('Delete this API key? This cannot be undone.');
  if (!confirmed) {
    return;
  }
  await api(`/api/keys/${encodeURIComponent(keyId)}`, { method: 'DELETE' });
  if (dom.apiKeyOutput) {
    dom.apiKeyOutput.textContent = '';
  }
  showToast('API key deleted.');
  await loadApiKeys();
}

function bindEvents() {
  dom.menuBtn.addEventListener('click', () => {
    const open = !dom.drawer.classList.contains('drawer-open');
    setDrawer(open);
  });

  dom.navItems.forEach((item) => {
    item.addEventListener('click', (event) => {
      const view = String(item.dataset.view || '').trim();
      if (view === 'billing') {
        const target = event.target;
        const clickedChevron = target instanceof HTMLElement && target.closest('#billingNavChevron');
        const activeView = getActiveViewName();
        if (clickedChevron || activeView === 'billing' || activeView === 'billing-backfill' || activeView === 'billing-budget') {
          setBillingNavExpanded(!state.billing.navExpanded);
          return;
        }
        setBillingNavExpanded(true);
        setActiveView('billing');
        void loadBilling().catch((error) => {
          showToast(error.message || 'Failed to load billing view.', true);
        });
        return;
      }
      if (view === 'storage') {
        const target = event.target;
        const clickedChevron = target instanceof HTMLElement && target.closest('#storageNavChevron');
        const activeView = getActiveViewName();
        if (clickedChevron || activeView === 'storage') {
          setStorageNavExpanded(!state.storage.navExpanded);
          return;
        }
        setStorageNavExpanded(true);
        setActiveView('storage');
        setStorageEmbedProviderPreference(state.storage.selectedProvider, { persist: false });
        return;
      }
      if (view === 'live') {
        const target = event.target;
        const clickedChevron = target instanceof HTMLElement && target.closest('#liveNavChevron');
        const activeView = getActiveViewName();
        if (clickedChevron || activeView === 'live') {
          setLiveNavExpanded(!state.live.navExpanded);
          return;
        }
        setLivePinnedGroupName(null, { persist: true });
        setLiveNavExpanded(true);
        renderLiveNavGroup();
        setActiveView('live');
        void loadLive({ refresh: true }).catch((error) => {
          showToast(error.message || 'Failed to load VSAx live view.', true);
        });
        return;
      }
      setActiveView(view);
    });
  });

  if (dom.adminNavBtn) {
    dom.adminNavBtn.addEventListener('click', (event) => {
      const activeView = getActiveViewName();
      const target = event.target;
      const clickedChevron = target instanceof HTMLElement && target.closest('#adminNavChevron');
      if (clickedChevron || isAdminRelatedView(activeView)) {
        setAdminNavExpanded(!state.admin.navExpanded);
        return;
      }
      setAdminNavExpanded(true);
      setActiveView('vendors');
    });
  }

  document.getElementById('refreshOverviewBtn').addEventListener('click', runSafe(loadOverview));
  if (dom.overviewBillingRangePresets) {
    dom.overviewBillingRangePresets.addEventListener('click', runSafe(handleOverviewBillingPresetClick));
  }
  if (dom.exportAllVendorsBtn) {
    dom.exportAllVendorsBtn.addEventListener('click', runSafe(exportAllVendors));
  }
  if (dom.importAllVendorsBtn) {
    dom.importAllVendorsBtn.addEventListener('click', openImportAllVendorsPicker);
  }
  if (dom.importAllVendorsInput) {
    dom.importAllVendorsInput.addEventListener('change', runSafe(importAllVendorsFromFile));
  }
  if (dom.importSingleVendorInput) {
    dom.importSingleVendorInput.addEventListener('change', runSafe(importSingleVendorFromFile));
  }
  document.getElementById('refreshVendorsBtn').addEventListener('click', runSafe(loadVendors));
  document.getElementById('refreshBillingBtn').addEventListener('click', runSafe(loadBilling));
  if (dom.exportBillingBtn) {
    dom.exportBillingBtn.addEventListener('click', runSafe(() => exportBillingCsv()));
  }
  if (dom.refreshCloudMetricsBtn) {
    dom.refreshCloudMetricsBtn.addEventListener('click', runSafe(syncCloudMetrics));
  }
  if (dom.cloudMetricsProviderTabs) {
    dom.cloudMetricsProviderTabs.addEventListener('click', runSafe(handleCloudMetricsProviderTabsClick));
  }
  if (dom.cloudMetricsTypeBody) {
    dom.cloudMetricsTypeBody.addEventListener('click', runSafe(handleCloudMetricsTypeBodyClick));
  }
  if (dom.refreshCloudDatabaseBtn) {
    dom.refreshCloudDatabaseBtn.addEventListener('click', runSafe(loadCloudDatabase));
  }
  if (dom.syncCloudDatabaseBtn) {
    dom.syncCloudDatabaseBtn.addEventListener('click', runSafe(syncCloudDatabase));
  }
  if (dom.cloudDatabaseProviderTabs) {
    dom.cloudDatabaseProviderTabs.addEventListener('click', runSafe(handleCloudDatabaseProviderTabsClick));
  }
  if (dom.cloudDatabaseBody) {
    dom.cloudDatabaseBody.addEventListener('click', runSafe(handleCloudDatabaseBodyClick));
  }
  if (dom.cloudDatabaseSearchInput) {
    dom.cloudDatabaseSearchInput.addEventListener('input', handleCloudDatabaseSearchInput);
  }
  if (dom.clearCloudDatabaseFiltersBtn) {
    dom.clearCloudDatabaseFiltersBtn.addEventListener('click', clearCloudDatabaseFilters);
  }
  if (dom.refreshGrafanaCloudBtn) {
    dom.refreshGrafanaCloudBtn.addEventListener('click', runSafe(loadGrafanaCloud));
  }
  if (dom.syncGrafanaCloudBtn) {
    dom.syncGrafanaCloudBtn.addEventListener('click', runSafe(syncGrafanaCloud));
  }
  if (dom.grafanaCloudVendorFilter) {
    dom.grafanaCloudVendorFilter.addEventListener('change', runSafe(handleGrafanaCloudVendorChange));
  }
  if (dom.grafanaCloudRangePresets) {
    dom.grafanaCloudRangePresets.addEventListener('click', runSafe(handleGrafanaCloudPresetClick));
  }
  if (dom.grafanaCloudRangeStart) {
    dom.grafanaCloudRangeStart.addEventListener('change', runSafe(handleGrafanaCloudRangeChange));
  }
  if (dom.grafanaCloudRangeEnd) {
    dom.grafanaCloudRangeEnd.addEventListener('change', runSafe(handleGrafanaCloudRangeChange));
  }
  if (dom.refreshLiveBtn) {
    dom.refreshLiveBtn.addEventListener(
      'click',
      runSafe(async () => {
        await loadLive({ refresh: true });
      })
    );
  }
  if (dom.liveGroupFilter) {
    dom.liveGroupFilter.addEventListener('change', handleLiveGroupFilterChange);
  }
  if (dom.liveGroupSearchInput) {
    dom.liveGroupSearchInput.addEventListener('input', handleLiveGroupSearchInput);
  }
  if (dom.liveSearchInput) {
    dom.liveSearchInput.addEventListener('input', handleLiveSearchInput);
  }
  if (dom.clearLiveFiltersBtn) {
    dom.clearLiveFiltersBtn.addEventListener('click', clearLiveFilters);
  }
  if (dom.refreshFirewallBtn) {
    dom.refreshFirewallBtn.addEventListener('click', runSafe(() => loadFirewall()));
  }
  if (dom.syncFirewallBtn) {
    dom.syncFirewallBtn.addEventListener('click', runSafe(syncFirewallNow));
  }
  if (dom.firewallHostFilter) {
    dom.firewallHostFilter.addEventListener('change', handleFirewallHostFilterChange);
  }
  if (dom.firewallSearchInput) {
    dom.firewallSearchInput.addEventListener('input', handleFirewallSearchInput);
  }
  if (dom.refreshVpnBtn) {
    dom.refreshVpnBtn.addEventListener('click', runSafe(() => loadVpn()));
  }
  if (dom.vpnSearchInput) {
    dom.vpnSearchInput.addEventListener('input', handleVpnSearchInput);
  }
  if (dom.vpnHostFilter) {
    dom.vpnHostFilter.addEventListener('change', handleVpnHostFilterChange);
  }
  if (dom.vpnSortSelect) {
    dom.vpnSortSelect.addEventListener('change', handleVpnSortChange);
  }
  if (dom.clearVpnFiltersBtn) {
    dom.clearVpnFiltersBtn.addEventListener('click', clearVpnFilters);
  }
  if (dom.vpnBody) {
    dom.vpnBody.addEventListener('click', runSafe(handleVpnTableClick));
  }
  document.getElementById('refreshSecurityBtn').addEventListener('click', runSafe(loadSecurity));

  document.getElementById('vendorForm').addEventListener('submit', runSafe(submitVendorForm));
  document.getElementById('vendorsBody').addEventListener('click', runSafe(handleVendorTableClick));
  if (dom.refreshAppConfigBtn) {
    dom.refreshAppConfigBtn.addEventListener('click', runSafe(loadAppConfig));
  }
  if (dom.appConfigForm) {
    dom.appConfigForm.addEventListener('submit', runSafe(saveAppConfig));
  }
  if (dom.exportAppConfigBtn) {
    dom.exportAppConfigBtn.addEventListener('click', runSafe(exportAppConfig));
  }
  if (dom.importAppConfigBtn) {
    dom.importAppConfigBtn.addEventListener('click', openImportAppConfigPicker);
  }
  if (dom.importAppConfigInput) {
    dom.importAppConfigInput.addEventListener('change', runSafe(importAppConfigFromFile));
  }
  if (dom.refreshAdminUsersBtn) {
    dom.refreshAdminUsersBtn.addEventListener('click', runSafe(loadAdminUsers));
  }
  if (dom.adminUserForm) {
    dom.adminUserForm.addEventListener('submit', runSafe(submitAdminUserForm));
  }
  if (dom.adminUserCancelBtn) {
    dom.adminUserCancelBtn.addEventListener('click', () => {
      resetAdminUserForm();
    });
  }
  if (dom.adminUserIsAdmin) {
    dom.adminUserIsAdmin.addEventListener('change', () => {
      renderAdminPermissionGrid();
    });
  }
  if (dom.adminUsersBody) {
    dom.adminUsersBody.addEventListener('click', runSafe(handleAdminUsersTableClick));
  }
  if (dom.billingNavGroup) {
    dom.billingNavGroup.addEventListener('click', runSafe(handleBillingScopeClick));
  }
  if (dom.storageNavGroup) {
    dom.storageNavGroup.addEventListener(
      'click',
      runSafe(async (event) => {
        const target = event.target;
        if (!(target instanceof HTMLElement)) {
          return;
        }
        const button = target.closest('[data-storage-provider]');
        if (!(button instanceof HTMLElement)) {
          return;
        }
        const provider = normalizeStorageProvider(button.dataset.storageProvider || 'unified');
        setStorageEmbedProviderPreference(provider, { persist: true });
        setStorageNavExpanded(true);
        setActiveView('storage');
      })
    );
  }
  if (dom.liveNavGroup) {
    dom.liveNavGroup.addEventListener('click', runSafe(handleLiveNavGroupClick));
  }
  if (dom.billingAccountSummaryBody) {
    dom.billingAccountSummaryBody.addEventListener('click', runSafe(handleBillingAccountSummaryClick));
  }
  if (dom.billingTypeProviderFilter) {
    dom.billingTypeProviderFilter.addEventListener('change', runSafe(handleBillingTypeProviderChange));
  }
  if (dom.billingTypeResourceFilter) {
    dom.billingTypeResourceFilter.addEventListener('change', runSafe(handleBillingTypeResourceChange));
  }
  if (dom.billingTypeTagFilter) {
    dom.billingTypeTagFilter.addEventListener('change', runSafe(handleBillingTypeTagChange));
  }
  if (dom.billingSearchInput) {
    dom.billingSearchInput.addEventListener('input', handleBillingSearchInput);
  }
  if (dom.billingTypeSort) {
    dom.billingTypeSort.addEventListener('change', runSafe(handleBillingTypeSortChange));
  }
  if (dom.billingAccountSort) {
    dom.billingAccountSort.addEventListener('change', runSafe(handleBillingAccountSortChange));
  }
  if (dom.billingResourceBody) {
    dom.billingResourceBody.addEventListener('click', runSafe(handleBillingResourceBodyClick));
  }
  if (dom.billingOrgTotalsCards) {
    dom.billingOrgTotalsCards.addEventListener('click', runSafe(handleBillingTagSummaryClick));
    dom.billingOrgTotalsCards.addEventListener('keydown', runSafe(handleBillingTagSummaryKeydown));
  }
  if (dom.billingProductTotalsCards) {
    dom.billingProductTotalsCards.addEventListener('click', runSafe(handleBillingTagSummaryClick));
    dom.billingProductTotalsCards.addEventListener('keydown', runSafe(handleBillingTagSummaryKeydown));
  }
  if (dom.overviewBillingOrgCards) {
    dom.overviewBillingOrgCards.addEventListener('click', runSafe(handleBillingTagSummaryClick));
    dom.overviewBillingOrgCards.addEventListener('keydown', runSafe(handleBillingTagSummaryKeydown));
  }
  if (dom.overviewBillingProductCards) {
    dom.overviewBillingProductCards.addEventListener('click', runSafe(handleBillingTagSummaryClick));
    dom.overviewBillingProductCards.addEventListener('keydown', runSafe(handleBillingTagSummaryKeydown));
  }
  if (dom.refreshTagsBtn) {
    dom.refreshTagsBtn.addEventListener('click', runSafe(loadTags));
  }
  if (dom.tagsSearchInput) {
    dom.tagsSearchInput.addEventListener('input', handleTagsSearchInput);
  }
  if (dom.syncAwsTagsBtn) {
    dom.syncAwsTagsBtn.addEventListener('click', runSafe(syncAwsTags));
  }
  if (dom.tagsBody) {
    dom.tagsBody.addEventListener('click', runSafe(handleTagsTableClick));
  }
  if (dom.billingRangePresets) {
    dom.billingRangePresets.addEventListener('click', runSafe(handleBillingPresetClick));
  }

  if (dom.billingRangeStart) {
    dom.billingRangeStart.addEventListener('change', runSafe(handleBillingRangeChange));
  }
  if (dom.billingRangeEnd) {
    dom.billingRangeEnd.addEventListener('change', runSafe(handleBillingRangeChange));
  }
  if (dom.toggleBillingSnapshotsBtn) {
    dom.toggleBillingSnapshotsBtn.addEventListener('click', () => {
      setBillingSnapshotsCollapsed(!state.billing.snapshotsCollapsed);
    });
  }
  if (dom.toggleBillingTrendBtn) {
    dom.toggleBillingTrendBtn.addEventListener('click', () => {
      setBillingTrendCollapsed(!state.billing.trendCollapsed);
    });
  }
  if (dom.startBillingBackfillBtn) {
    dom.startBillingBackfillBtn.addEventListener('click', runSafe(startBillingBackfill));
  }
  if (dom.refreshBillingBackfillStatusBtn) {
    dom.refreshBillingBackfillStatusBtn.addEventListener('click', runSafe(loadBillingBackfillStatus));
  }
  if (dom.refreshBillingBudgetBtn) {
    dom.refreshBillingBudgetBtn.addEventListener('click', runSafe(loadBillingBudget));
  }
  if (dom.saveBillingBudgetBtn) {
    dom.saveBillingBudgetBtn.addEventListener('click', runSafe(saveBillingBudget));
  }
  if (dom.billingBudgetYear) {
    dom.billingBudgetYear.addEventListener(
      'change',
      runSafe(() =>
        loadBillingBudget({
          year: dom.billingBudgetYear?.value
        })
      )
    );
  }
  if (dom.billingBudgetBody) {
    dom.billingBudgetBody.addEventListener('input', handleBillingBudgetInput);
  }

  if (dom.createApiKeyBtn) {
    dom.createApiKeyBtn.addEventListener('click', runSafe(createApiKey));
  }
  if (dom.apiKeysBody) {
    dom.apiKeysBody.addEventListener('click', runSafe(handleApiKeysTableClick));
  }
  if (dom.refreshDbBackupConfigBtn) {
    dom.refreshDbBackupConfigBtn.addEventListener('click', runSafe(loadDbBackupConfig));
  }
  if (dom.dbBackupForm) {
    dom.dbBackupForm.addEventListener('submit', runSafe(saveDbBackupConfig));
  }
  if (dom.runDbBackupNowBtn) {
    dom.runDbBackupNowBtn.addEventListener('click', runSafe(runDbBackupNow));
  }

  if (dom.reloadIpMapBtn) {
    dom.reloadIpMapBtn.addEventListener('click', runSafe(loadIpMap));
  }
  if (dom.saveIpMapBtn) {
    dom.saveIpMapBtn.addEventListener('click', runSafe(saveIpMap));
  }
  if (dom.exportIpMapBtn) {
    dom.exportIpMapBtn.addEventListener('click', runSafe(exportIpMapCsv));
  }
  if (dom.scanIpDiscoveryBtn) {
    dom.scanIpDiscoveryBtn.addEventListener('click', runSafe(loadIpDiscovery));
  }
  if (dom.ipDiscoverySearchInput) {
    dom.ipDiscoverySearchInput.addEventListener('input', () => {
      state.ipDiscovery.searchText = String(dom.ipDiscoverySearchInput?.value || '').trim();
      renderIpDiscoveryTable();
    });
  }
  if (dom.ipDiscoveryBody) {
    dom.ipDiscoveryBody.addEventListener(
      'click',
      runSafe(async (event) => {
        const target = event.target;
        if (!(target instanceof HTMLElement)) {
          return;
        }
        const inheritBtn = target.closest('.js-ip-discovery-inherit');
        if (!inheritBtn) {
          return;
        }
        const ipAddress = String(inheritBtn.dataset.ipAddress || '').trim().toLowerCase();
        if (!ipAddress) {
          return;
        }
        let serverName = '';
        const inputs = dom.ipDiscoveryBody?.querySelectorAll('.ip-discovery-name-input') || [];
        for (const input of inputs) {
          if (!(input instanceof HTMLInputElement)) {
            continue;
          }
          if (String(input.dataset.ipAddress || '').trim().toLowerCase() === ipAddress) {
            serverName = input.value;
            break;
          }
        }
        await inheritDiscoveredIp(ipAddress, serverName);
      })
    );
  }

  dom.logoutBtn.addEventListener('click', runSafe(logout));
}

function runSafe(fn) {
  return async (...args) => {
    try {
      await fn(...args);
    } catch (error) {
      showToast(error.message || 'Request failed.', true);
    }
  };
}

async function init() {
  bindEvents();
  setDrawer(!window.matchMedia('(max-width: 860px)').matches);
  await loadAuthSession();
  state.billing.selectedScopeId = normalizeBillingScopeId(readSavedBillingScope());
  state.billing.navExpanded = readSavedBillingNavExpanded();
  state.storage.navExpanded = readSavedStorageNavExpanded();
  state.storage.selectedProvider = readSavedStorageProvider();
  state.admin.navExpanded = readSavedAdminNavExpanded();
  state.live.navExpanded = readSavedLiveNavExpanded();
  state.billing.snapshotsCollapsed = readSavedBillingSnapshotsCollapsed();
  state.billing.trendCollapsed = readSavedBillingTrendCollapsed();
  state.cloudMetrics.selectedProvider = normalizeCloudMetricsProvider(readSavedCloudMetricsProvider() || 'azure');
  state.cloudDatabase.selectedProvider = normalizeCloudDatabaseProvider(readSavedCloudDatabaseProvider() || 'all', 'all');
  state.cloudDatabase.searchText = readSavedCloudDatabaseSearchText();
  state.live.selectedGroupName = normalizeLiveGroupName(readSavedLiveGroupName());
  state.live.pinnedGroupName = normalizeLivePinnedGroupName(readSavedLivePinnedGroupName());
  state.live.groupSearchText = readSavedLiveGroupSearchText();
  state.live.searchText = readSavedLiveSearchText();
  state.firewall.searchText = readSavedFirewallSearchText();
  state.firewall.hostFilter = readSavedFirewallHostFilter();
  state.vpn.searchText = readSavedVpnSearchText();
  state.vpn.hostFilter = readSavedVpnHostFilter();
  state.vpn.sortBy = readSavedVpnSort();
  state.billing.searchText = readSavedBillingSearchText();
  state.billing.accountSort = parseBillingAccountSortValue(readSavedBillingAccountSort() || state.billing.accountSort);
  const savedPreset = String(readSavedBillingPreset() || '').trim().toLowerCase();
  if (['month_to_date', 'last_month', '3_months', '6_months', '1_year'].includes(savedPreset)) {
    state.billing.rangePreset = savedPreset;
  }
  const savedOverviewPreset = String(readSavedOverviewBillingPreset() || '').trim().toLowerCase();
  if (['month_to_date', 'last_month'].includes(savedOverviewPreset)) {
    state.overviewBilling.rangePreset = savedOverviewPreset;
  }
  const savedGrafanaCloudPreset = String(readSavedGrafanaCloudPreset() || '').trim().toLowerCase();
  if (['month_to_date', 'last_month', '3_months', '6_months', '1_year'].includes(savedGrafanaCloudPreset)) {
    state.grafanaCloud.rangePreset = savedGrafanaCloudPreset;
  }
  renderOverviewBillingPresetButtons();
  renderGrafanaCloudRangePresetButtons();
  if (dom.liveSearchInput) {
    dom.liveSearchInput.value = state.live.searchText;
  }
  if (dom.liveGroupSearchInput) {
    dom.liveGroupSearchInput.value = state.live.groupSearchText;
  }
  if (dom.firewallSearchInput) {
    dom.firewallSearchInput.value = state.firewall.searchText;
  }
  if (dom.vpnSearchInput) {
    dom.vpnSearchInput.value = state.vpn.searchText;
  }
  if (dom.vpnSortSelect) {
    dom.vpnSortSelect.value = normalizeVpnSortValue(state.vpn.sortBy);
  }
  if (dom.billingSearchInput) {
    dom.billingSearchInput.value = state.billing.searchText;
  }
  if (dom.cloudDatabaseSearchInput) {
    dom.cloudDatabaseSearchInput.value = state.cloudDatabase.searchText;
  }
  ensureBillingRangeDefaults();
  setBillingSnapshotsCollapsed(state.billing.snapshotsCollapsed, { persist: false });
  setBillingTrendCollapsed(state.billing.trendCollapsed, { persist: false });
  setBillingNavExpanded(state.billing.navExpanded, { persist: false });
  setStorageNavExpanded(state.storage.navExpanded, { persist: false });
  setLiveNavExpanded(state.live.navExpanded, { persist: false });
  setAdminNavExpanded(state.admin.navExpanded, { persist: false });
  renderStorageNavGroup();
  renderLiveNavGroup();
  setActiveView(readSavedView(), { persist: false });
  await loadServices();
  const startupLoads = [];
  if (hasClientViewAccess('dashboard')) {
    startupLoads.push(loadOverview());
  }
  if (hasClientViewAccess('vendors')) {
    startupLoads.push(loadVendors());
  }
  if (hasClientViewAccess('billing')) {
    startupLoads.push(loadBilling(), loadBillingBackfillStatus());
  }
  if (hasClientViewAccess('admin-api-keys')) {
    startupLoads.push(loadApiKeys());
  }
  if (hasClientViewAccess('admin-settings')) {
    startupLoads.push(loadAppConfig());
  }
  if (hasClientViewAccess('admin-backup')) {
    startupLoads.push(loadDbBackupConfig());
  }
  if (hasClientViewAccess('tags')) {
    startupLoads.push(loadTags());
  }
  if (hasClientViewAccess('admin-users')) {
    startupLoads.push(loadAdminUsers());
  }
  if (hasClientViewAccess('cloud-metrics')) {
    startupLoads.push(
      loadCloudMetrics({
        provider: state.cloudMetrics.selectedProvider,
        persist: false
      })
    );
  }
  if (hasClientViewAccess('cloud-database')) {
    startupLoads.push(
      loadCloudDatabase({
        provider: state.cloudDatabase.selectedProvider,
        persist: false
      })
    );
  }
  if (hasClientViewAccess('grafana-cloud')) {
    startupLoads.push(loadGrafanaCloud());
  }
  if (hasClientViewAccess('live')) {
    startupLoads.push(loadLive({ refresh: false }));
  }
  if (hasClientViewAccess('firewall')) {
    startupLoads.push(loadFirewall());
  }
  if (hasClientViewAccess('vpn')) {
    startupLoads.push(loadVpn());
  }
  if (hasClientViewAccess('security')) {
    startupLoads.push(loadSecurity());
  }
  await Promise.all(startupLoads);

  if (hasClientViewAccess('billing')) {
    await loadBillingAutoSyncStatus();

    window.setInterval(() => {
      void loadBillingAutoSyncStatus().catch(() => {
        // ignore background polling failures
      });
    }, 60_000);

    window.setInterval(() => {
      const activeView = getActiveViewName();
      if (activeView === 'billing-backfill' || state.billing.backfillStatus?.running) {
        void loadBillingBackfillStatus().catch(() => {
          // ignore background polling failures
        });
      }
    }, 5000);
  }

  if (hasClientViewAccess('cloud-metrics')) {
    window.setInterval(() => {
      const activeView = getActiveViewName();
      if (activeView === 'cloud-metrics') {
        void loadCloudMetrics({
          provider: state.cloudMetrics.selectedProvider,
          persist: false
        }).catch(() => {
          // ignore background polling failures
        });
      }
    }, 60_000);
  }

  if (hasClientViewAccess('cloud-database')) {
    window.setInterval(() => {
      const activeView = getActiveViewName();
      if (activeView === 'cloud-database') {
        void loadCloudDatabase({
          provider: state.cloudDatabase.selectedProvider,
          persist: false
        }).catch(() => {
          // ignore background polling failures
        });
      }
    }, 60_000);
  }

  if (hasClientViewAccess('grafana-cloud')) {
    window.setInterval(() => {
      if (getActiveViewName() === 'grafana-cloud') {
        void loadGrafanaCloud().catch(() => {
          // ignore background polling failures
        });
      }
    }, 60_000);
  }

  if (hasClientViewAccess('firewall') || hasClientViewAccess('vpn')) {
    window.setInterval(() => {
      const activeView = getActiveViewName();
      if (activeView === 'firewall') {
        void loadFirewall().catch(() => {
          // ignore background polling failures
        });
      }
      if (activeView === 'vpn') {
        void loadVpn().catch(() => {
          // ignore background polling failures
        });
      }
    }, 60_000);
  }
}

runSafe(init)();
