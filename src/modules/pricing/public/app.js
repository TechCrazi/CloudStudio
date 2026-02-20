const PRICE_APP_BASE_PATH =
  typeof window.CLOUDSTUDIO_PRICING_BASE_PATH === "string" &&
  window.CLOUDSTUDIO_PRICING_BASE_PATH.trim()
    ? window.CLOUDSTUDIO_PRICING_BASE_PATH.trim()
    : window.location.pathname.startsWith("/apps/pricing")
    ? "/apps/pricing"
    : "";
const PRICE_CLASS_TARGET =
  window.CLOUDSTUDIO_PRICING_CLASS_TARGET instanceof HTMLElement
    ? window.CLOUDSTUDIO_PRICING_CLASS_TARGET
    : document.body;
const PRICE_EMBEDDED = Boolean(window.CLOUDSTUDIO_PRICING_EMBEDDED);

function resolvePriceApiPath(url) {
  if (typeof url !== "string") {
    return url;
  }
  if (!PRICE_APP_BASE_PATH) {
    return url;
  }
  return url.startsWith("/api/") ? `${PRICE_APP_BASE_PATH}${url}` : url;
}

function togglePricingFocusClass(className, enabled) {
  if (!PRICE_CLASS_TARGET || !className) {
    return;
  }
  PRICE_CLASS_TARGET.classList.toggle(className, Boolean(enabled));
}

const form = document.getElementById("pricing-form");
const formNote = document.getElementById("form-note");
const delta = document.getElementById("delta");
const exportButton = document.getElementById("export-csv");
const authStatus = document.getElementById("auth-status");
const authForm = document.getElementById("auth-form");
const authUsernameInput = document.getElementById("auth-username");
const authPasswordInput = document.getElementById("auth-password");
const authOpenLoginButton = document.getElementById("auth-open-login");
const authImportGuestButton = document.getElementById("auth-import-guest");
const authSaveDbButton = document.getElementById("auth-save-db");
const authLogoutButton = document.getElementById("auth-logout");
const authNote = document.getElementById("auth-note");
const modeInput = document.getElementById("mode-input");
const pricingFocusInput = document.getElementById("pricing-focus");
const modeTabs = document.querySelectorAll(".mode-tab");
const adminModeTab = document.getElementById("admin-mode-tab");
const formTitle = document.getElementById("form-title");
const formSubtitle = document.getElementById("form-subtitle");
const resultsTitle = document.getElementById("results-title");
const resultsSubtitle = document.getElementById("results-subtitle");
const cpuLabel = document.getElementById("cpu-label");
const vmCountLabel = document.getElementById("vm-count-label");
const egressLabel = document.getElementById("egress-label");
const osDiskLabel = document.getElementById("os-disk-label");
const dataDiskLabel = document.getElementById("data-disk-label");
const workloadField = document.getElementById("workload-field");
const sqlEditionField = document.getElementById("sql-edition-field");
const sqlRateField = document.getElementById("sql-rate-field");
const awsTitle = document.getElementById("aws-title");
const azureTitle = document.getElementById("azure-title");
const gcpTitle = document.getElementById("gcp-title");
const cpuSelect = form.querySelector("[name='cpu']");
const workloadSelect = form.querySelector("[name='workload']");
const awsInstanceSelect = document.getElementById("aws-instance");
const azureInstanceSelect = document.getElementById("azure-instance");
const gcpInstanceSelect = document.getElementById("gcp-instance");
const awsVpcSelect = document.getElementById("aws-vpc-flavor");
const awsFirewallSelect = document.getElementById("aws-firewall-flavor");
const awsLbSelect = document.getElementById("aws-lb-flavor");
const azureVpcSelect = document.getElementById("azure-vnet-flavor");
const azureFirewallSelect = document.getElementById("azure-firewall-flavor");
const azureLbSelect = document.getElementById("azure-lb-flavor");
const gcpVpcSelect = document.getElementById("gcp-vpc-flavor");
const gcpFirewallSelect = document.getElementById("gcp-firewall-flavor");
const gcpLbSelect = document.getElementById("gcp-lb-flavor");
const diskTierSelect = form.querySelector("[name='diskTier']");
const sqlEditionSelect = form.querySelector("[name='sqlEdition']");
const sqlRateInput = form.querySelector("[name='sqlLicenseRate']");
const osDiskInput = form.querySelector("[name='osDiskGb']");
const vmCountInput = form.querySelector("[name='vmCount']");
const regionSelect = form.querySelector("[name='regionKey']");
const pricingProviderSelect = form.querySelector("[name='pricingProvider']");
const hoursInput = form.querySelector("[name='hours']");
const egressInput = form.querySelector("[name='egressTb']");
const interVlanInput = form.querySelector("[name='interVlanTb']");
const intraVlanInput = form.querySelector("[name='intraVlanTb']");
const interRegionInput = form.querySelector("[name='interRegionTb']");
const storageIopsInput = form.querySelector("[name='storageIops']");
const storageThroughputInput = form.querySelector(
  "[name='storageThroughputMbps']"
);
const storageRequestInput = form.querySelector(
  "[name='storageRequestUnitsMillion']"
);
const storageOperationInput = form.querySelector(
  "[name='storageOperationUnitsMillion']"
);
const dataDiskInput = form.querySelector("[name='dataDiskTb']");
const backupEnabledInput = form.querySelector("[name='backupEnabled']");
const drPercentInput = form.querySelector("[name='drPercent']");
const awsObjectStorageInput = form.querySelector(
  "[name='awsObjectStorageRate']"
);
const azureObjectStorageInput = form.querySelector(
  "[name='azureObjectStorageRate']"
);
const gcpObjectStorageInput = form.querySelector(
  "[name='gcpObjectStorageRate']"
);
const storageRateSection = document.getElementById("storage-rate-section");
const storageRateFields = document.getElementById("storage-rate-fields");
const hoursField = hoursInput?.closest("label");
const cpuField = cpuSelect?.closest("label");
const vmCountField = vmCountInput?.closest("label");
const diskTierField = diskTierSelect?.closest("label");
const osDiskField = osDiskInput?.closest("label");
const dataDiskField = dataDiskInput?.closest("label");
const backupField = backupEnabledInput?.closest("label");
const egressField = egressInput?.closest("label");
const interVlanField = interVlanInput?.closest("label");
const intraVlanField = intraVlanInput?.closest("label");
const interRegionField = interRegionInput?.closest("label");
const storageIopsField = storageIopsInput?.closest("label");
const storageThroughputField = storageThroughputInput?.closest("label");
const storageRequestField = storageRequestInput?.closest("label");
const storageOperationField = storageOperationInput?.closest("label");
const drField = drPercentInput?.closest("label");
const networkSection = document.getElementById("network-section");
const networkFields = document.getElementById("network-fields");
const networkAddonFocusInput = document.getElementById("network-addon-focus");
const networkAddonTabs = document.getElementById("network-addon-tabs");
const networkAddonTabButtons = document.querySelectorAll(".network-addon-tab");
const networkAddonGroups = document.querySelectorAll(".network-addon-group");
const scenarioNameInput = document.getElementById("scenario-name");
const scenarioList = document.getElementById("scenario-list");
const scenarioNote = document.getElementById("scenario-note");
const scenarioDelta = document.getElementById("scenario-delta");
const scenarioComponentDelta = document.getElementById(
  "scenario-component-delta"
);
const saveScenarioButton = document.getElementById("save-scenario");
const loadScenarioButton = document.getElementById("load-scenario");
const cloneScenarioButton = document.getElementById("clone-scenario");
const compareScenarioButton = document.getElementById("compare-scenario");
const deleteScenarioButton = document.getElementById("delete-scenario");
const importScenarioButton = document.getElementById("import-scenario");
const importScenarioInput = document.getElementById("import-scenario-file");
const importScenarioCsvButton = document.getElementById(
  "import-scenario-csv"
);
const importScenarioCsvInput = document.getElementById(
  "import-scenario-csv-file"
);
const awsInstanceFilter = document.getElementById("aws-instance-filter");
const azureInstanceFilter = document.getElementById("azure-instance-filter");
const gcpInstanceFilter = document.getElementById("gcp-instance-filter");
const awsCard = document.getElementById("aws-card");
const azureCard = document.getElementById("azure-card");
const gcpCard = document.getElementById("gcp-card");
const compareGrid = document.getElementById("compare-grid");
const vendorGrid = document.getElementById("vendor-grid");
const networkFocusPanel = document.getElementById("network-focus-panel");
const storageFocusPanel = document.getElementById("storage-focus-panel");
const networkFocusTable = document.getElementById("network-focus-table");
const storageFocusTable = document.getElementById("storage-focus-table");
const networkProviderCards = document.getElementById("network-provider-cards");
const storageProviderCards = document.getElementById("storage-provider-cards");
const networkInsightPanel = document.getElementById("network-focus-insight");
const networkInsightChart = document.getElementById("network-insight-chart");
const networkInsightNote = document.getElementById("network-insight-note");
const storageInsightPanel = document.getElementById("storage-focus-insight");
const storageInsightChart = document.getElementById("storage-insight-chart");
const storageInsightNote = document.getElementById("storage-insight-note");
const networkResultTabs = document.querySelectorAll("[data-network-result]");
const storageResultTabs = document.querySelectorAll("[data-storage-result]");
const disclaimer = document.querySelector(".disclaimer");
const defaultDisclaimerText = disclaimer
  ? disclaimer.textContent.replace(/\s+/g, " ").trim()
  : "";
const vendorCardTemplate = document.getElementById("vendor-card-template");
const privateOptionTemplate = document.getElementById("private-option-template");
const privateCompareTemplate = document.getElementById(
  "private-compare-template"
);
const privateCompareContainer = document.getElementById("private-compare-cards");
const viewTabs = document.getElementById("vm-view-tabs");
const viewTabButtons = document.querySelectorAll(".view-tab");
const privateViewTab = viewTabs?.querySelector("[data-view='private']");
const cloudPanel = document.getElementById("cloud-panel");
const privatePanel = document.getElementById("private-panel");
const scenariosPanel = document.getElementById("scenarios-panel");
const layout = document.querySelector(".layout");
const formCard = document.querySelector(".form-card");
const privateSaveNote = document.getElementById("private-save-note");
const privateProvidersList = document.getElementById(
  "private-providers-list"
);
const privateProviderTemplate = document.getElementById(
  "private-provider-template"
);
const addPrivateProviderButton = document.getElementById(
  "add-private-provider"
);
const exportPrivateProvidersButton = document.getElementById(
  "export-private-providers"
);
const importPrivateProvidersButton = document.getElementById(
  "import-private-providers"
);
const importPrivateProvidersInput = document.getElementById(
  "import-private-providers-file"
);
const resultsTabs = document.getElementById("results-tabs");
const resultsTabButtons = document.querySelectorAll(".results-tab");
const pricingPanel = document.getElementById("pricing-panel");
const savedComparePanel = document.getElementById("saved-compare-panel");
const vsaxComparePanel = document.getElementById("vsax-compare-panel");
const vsaxCompareGroupSelect = document.getElementById("vsax-compare-group");
const vsaxCompareRegionSelect = document.getElementById("vsax-compare-region");
const vsaxComparePricingProviderSelect = document.getElementById(
  "vsax-compare-pricing-provider"
);
const vsaxCompareRefreshButton = document.getElementById(
  "vsax-compare-refresh"
);
const vsaxCompareExportCsvButton = document.getElementById(
  "vsax-compare-export-csv"
);
const vsaxCompareExportExcelButton = document.getElementById(
  "vsax-compare-export-excel"
);
const vsaxCompareExportPdfButton = document.getElementById(
  "vsax-compare-export-pdf"
);
const vsaxCompareNote = document.getElementById("vsax-compare-note");
const vsaxCompareSummary = document.getElementById("vsax-compare-summary");
const vsaxCompareTable = document.getElementById("vsax-compare-table");
const billingPanel = document.getElementById("billing-panel");
const dataTransferPanel = document.getElementById("data-transfer-panel");
const dataTransferExportButton = document.getElementById("data-transfer-export");
const dataTransferImportButton = document.getElementById("data-transfer-import");
const dataTransferImportInput = document.getElementById(
  "data-transfer-import-file"
);
const dataTransferScope = document.getElementById("data-transfer-scope");
const dataTransferNote = document.getElementById("data-transfer-note");
const dataVersionSelect = document.getElementById("data-version-select");
const dataVersionRollbackButton = document.getElementById("data-version-rollback");
const dataVersionRefreshButton = document.getElementById("data-version-refresh");
const dataVersionNote = document.getElementById("data-version-note");
const adminPanel = document.getElementById("admin-panel");
const adminRefreshUsersButton = document.getElementById("admin-refresh-users");
const adminAddUserForm = document.getElementById("admin-add-user-form");
const adminAddUsernameInput = document.getElementById("admin-add-username");
const adminAddPasswordInput = document.getElementById("admin-add-password");
const adminAddRoleSelect = document.getElementById("admin-add-role");
const adminUpdatePasswordForm = document.getElementById(
  "admin-update-password-form"
);
const adminUpdateUsernameSelect = document.getElementById("admin-update-username");
const adminUpdatePasswordInput = document.getElementById("admin-update-password");
const adminNote = document.getElementById("admin-note");
const adminUsersTable = document.getElementById("admin-users-table");
const billingProviderTabs = document.querySelectorAll("[data-billing-provider]");
const billingImportButton = document.getElementById("billing-import-csv");
const billingExportButton = document.getElementById("billing-export-csv");
const billingImportInput = document.getElementById("billing-import-file");
const billingClearButton = document.getElementById("billing-clear");
const billingClearAllButton = document.getElementById("billing-clear-all");
const billingNote = document.getElementById("billing-note");
const billingFormatHint = document.getElementById("billing-format-hint");
const billingMonthFilter = document.getElementById("billing-month-filter");
const billingProductFilter = document.getElementById("billing-product-filter");
const billingChartGroup = document.getElementById("billing-chart-group");
const billingTagService = document.getElementById("billing-tag-service");
const billingTagProductApp = document.getElementById("billing-tag-product-app");
const billingTagCustom = document.getElementById("billing-tag-custom");
const billingApplyTagsButton = document.getElementById("billing-apply-tags");
const billingClearTagsButton = document.getElementById("billing-clear-tags");
const billingBulkCount = document.getElementById("billing-bulk-count");
const billingBulkProductApp = document.getElementById("billing-bulk-product-app");
const billingBulkTags = document.getElementById("billing-bulk-tags");
const billingBulkSelectVisible = document.getElementById(
  "billing-bulk-select-visible"
);
const billingBulkClearSelection = document.getElementById(
  "billing-bulk-clear-selection"
);
const billingBulkApply = document.getElementById("billing-bulk-apply");
const billingBulkClearTags = document.getElementById("billing-bulk-clear-tags");
const billingSummary = document.getElementById("billing-summary");
const billingChart = document.getElementById("billing-chart");
const billingTable = document.getElementById("billing-table");
const savedCompareTable = document.getElementById("saved-compare-table");
const savedCompareNote = document.getElementById("saved-compare-note");
const savedCompareRefresh = document.getElementById("saved-compare-refresh");
const savedCompareScenarioList = document.getElementById(
  "saved-compare-scenarios"
);
const savedComparePrivateList = document.getElementById(
  "saved-compare-private"
);
const savedComparePrivateRun = document.getElementById(
  "saved-compare-private-run"
);
const savedComparePrivateTable = document.getElementById(
  "saved-compare-private-table"
);
const savedComparePrivateNote = document.getElementById(
  "saved-compare-private-note"
);
const insightPanel = document.getElementById("insight-panel");
const insightChart = document.getElementById("insight-chart");
const insightNote = document.getElementById("insight-note");
const qualityPanel = document.getElementById("quality-panel");
const qualityMeta = document.getElementById("quality-meta");
const qualityList = document.getElementById("quality-list");
const unitEconPanel = document.getElementById("unit-econ-panel");
const unitEconTable = document.getElementById("unit-econ-table");
const unitEconNote = document.getElementById("unit-econ-note");
const recommendPanel = document.getElementById("recommend-panel");
const recommendList = document.getElementById("recommend-list");
const recommendNote = document.getElementById("recommend-note");
const recommendProviderFilter = document.getElementById(
  "recommend-provider-filter"
);
const recommendLimitInput = document.getElementById("recommend-limit");
const runRecommendationsButton = document.getElementById(
  "run-recommendations"
);
const commitPanel = document.getElementById("commit-panel");
const commitNote = document.getElementById("commit-note");
const commitDiscountInputs = {
  aws: document.querySelector("[data-commit-discount='aws']"),
  azure: document.querySelector("[data-commit-discount='azure']"),
  gcp: document.querySelector("[data-commit-discount='gcp']"),
};
const commitTypeInputs = {
  aws: document.querySelector("[data-commit-type='aws']"),
  azure: document.querySelector("[data-commit-type='azure']"),
  gcp: document.querySelector("[data-commit-type='gcp']"),
};
const commitFields = {
  aws: {
    base: {
      compute: document.querySelector("[data-commit='aws-base-compute']"),
      control: document.querySelector("[data-commit='aws-base-control']"),
      storage: document.querySelector("[data-commit='aws-base-storage']"),
      backup: document.querySelector("[data-commit='aws-base-backup']"),
      egress: document.querySelector("[data-commit='aws-base-egress']"),
      network: document.querySelector("[data-commit='aws-base-network']"),
      sql: document.querySelector("[data-commit='aws-base-sql']"),
      windows: document.querySelector("[data-commit='aws-base-windows']"),
      dr: document.querySelector("[data-commit='aws-base-dr']"),
      total: document.querySelector("[data-commit='aws-base-total']"),
    },
    commit: {
      compute: document.querySelector("[data-commit='aws-commit-compute']"),
      control: document.querySelector("[data-commit='aws-commit-control']"),
      storage: document.querySelector("[data-commit='aws-commit-storage']"),
      backup: document.querySelector("[data-commit='aws-commit-backup']"),
      egress: document.querySelector("[data-commit='aws-commit-egress']"),
      network: document.querySelector("[data-commit='aws-commit-network']"),
      sql: document.querySelector("[data-commit='aws-commit-sql']"),
      windows: document.querySelector("[data-commit='aws-commit-windows']"),
      dr: document.querySelector("[data-commit='aws-commit-dr']"),
      savings: document.querySelector("[data-commit='aws-commit-savings']"),
      total: document.querySelector("[data-commit='aws-commit-total']"),
    },
    note: document.querySelector("[data-commit='aws-note']"),
  },
  azure: {
    base: {
      compute: document.querySelector("[data-commit='azure-base-compute']"),
      control: document.querySelector("[data-commit='azure-base-control']"),
      storage: document.querySelector("[data-commit='azure-base-storage']"),
      backup: document.querySelector("[data-commit='azure-base-backup']"),
      egress: document.querySelector("[data-commit='azure-base-egress']"),
      network: document.querySelector("[data-commit='azure-base-network']"),
      sql: document.querySelector("[data-commit='azure-base-sql']"),
      windows: document.querySelector("[data-commit='azure-base-windows']"),
      dr: document.querySelector("[data-commit='azure-base-dr']"),
      total: document.querySelector("[data-commit='azure-base-total']"),
    },
    commit: {
      compute: document.querySelector("[data-commit='azure-commit-compute']"),
      control: document.querySelector("[data-commit='azure-commit-control']"),
      storage: document.querySelector("[data-commit='azure-commit-storage']"),
      backup: document.querySelector("[data-commit='azure-commit-backup']"),
      egress: document.querySelector("[data-commit='azure-commit-egress']"),
      network: document.querySelector("[data-commit='azure-commit-network']"),
      sql: document.querySelector("[data-commit='azure-commit-sql']"),
      windows: document.querySelector("[data-commit='azure-commit-windows']"),
      dr: document.querySelector("[data-commit='azure-commit-dr']"),
      savings: document.querySelector("[data-commit='azure-commit-savings']"),
      total: document.querySelector("[data-commit='azure-commit-total']"),
    },
    note: document.querySelector("[data-commit='azure-note']"),
  },
  gcp: {
    base: {
      compute: document.querySelector("[data-commit='gcp-base-compute']"),
      control: document.querySelector("[data-commit='gcp-base-control']"),
      storage: document.querySelector("[data-commit='gcp-base-storage']"),
      backup: document.querySelector("[data-commit='gcp-base-backup']"),
      egress: document.querySelector("[data-commit='gcp-base-egress']"),
      network: document.querySelector("[data-commit='gcp-base-network']"),
      sql: document.querySelector("[data-commit='gcp-base-sql']"),
      windows: document.querySelector("[data-commit='gcp-base-windows']"),
      dr: document.querySelector("[data-commit='gcp-base-dr']"),
      total: document.querySelector("[data-commit='gcp-base-total']"),
    },
    commit: {
      compute: document.querySelector("[data-commit='gcp-commit-compute']"),
      control: document.querySelector("[data-commit='gcp-commit-control']"),
      storage: document.querySelector("[data-commit='gcp-commit-storage']"),
      backup: document.querySelector("[data-commit='gcp-commit-backup']"),
      egress: document.querySelector("[data-commit='gcp-commit-egress']"),
      network: document.querySelector("[data-commit='gcp-commit-network']"),
      sql: document.querySelector("[data-commit='gcp-commit-sql']"),
      windows: document.querySelector("[data-commit='gcp-commit-windows']"),
      dr: document.querySelector("[data-commit='gcp-commit-dr']"),
      savings: document.querySelector("[data-commit='gcp-commit-savings']"),
      total: document.querySelector("[data-commit='gcp-commit-total']"),
    },
    note: document.querySelector("[data-commit='gcp-note']"),
  },
};
const commitInsightFields = {
  aws: {
    base: document.querySelector("[data-commit-insight-base='aws']"),
    commit: document.querySelector("[data-commit-insight-commit='aws']"),
    save: document.querySelector("[data-commit-insight-save='aws']"),
    baseBar: document.querySelector("[data-commit-insight-bar='aws-base']"),
    commitBar: document.querySelector("[data-commit-insight-bar='aws-commit']"),
  },
  azure: {
    base: document.querySelector("[data-commit-insight-base='azure']"),
    commit: document.querySelector("[data-commit-insight-commit='azure']"),
    save: document.querySelector("[data-commit-insight-save='azure']"),
    baseBar: document.querySelector("[data-commit-insight-bar='azure-base']"),
    commitBar: document.querySelector("[data-commit-insight-bar='azure-commit']"),
  },
  gcp: {
    base: document.querySelector("[data-commit-insight-base='gcp']"),
    commit: document.querySelector("[data-commit-insight-commit='gcp']"),
    save: document.querySelector("[data-commit-insight-save='gcp']"),
    baseBar: document.querySelector("[data-commit-insight-bar='gcp-base']"),
    commitBar: document.querySelector("[data-commit-insight-bar='gcp-commit']"),
  },
};
const vendorSubtabs = document.getElementById("vendor-subtabs");
const vendorSubtabButtons = document.querySelectorAll(".vendor-subtab");
const vendorRegionPanel = document.getElementById("vendor-region-panel");
const vendorRegionPicker = document.getElementById("vendor-region-picker");
const vendorRegionTable = document.getElementById("region-compare-table");
const vendorRegionNote = document.getElementById("region-compare-note");
const runRegionCompareButton = document.getElementById("run-region-compare");

const SQL_DEFAULTS = {
  none: 0,
  standard: 0.35,
  enterprise: 0.5,
};
const DEFAULT_RATE_EPSILON = 0.0001;
const DISK_TIER_LABELS = {
  premium: "Premium SSD",
  max: "Max performance",
};
const SCENARIO_STORAGE_KEY = "cloud-price-scenarios";
const SAVED_COMPARE_SCENARIOS_KEY = "cloud-price-saved-compare-scenarios";
const SAVED_COMPARE_PRIVATE_KEY = "cloud-price-saved-compare-private";
const PRIVATE_STORAGE_KEY = "cloud-price-private";
const PRIVATE_PROVIDERS_KEY = "cloud-price-private-providers";
const PRIVATE_COMPARE_KEY = "cloud-price-private-compare";
const PRICING_PANEL_STORAGE_KEY = "cloudstudio-pricing-active-panel";
const VSAX_COMPARE_PREFS_KEY = "cloudstudio-pricing-vsax-compare-prefs";
const PRIVATE_COMPARE_SLOTS = 2;
const MAX_VENDOR_OPTIONS = 4;
const DEFAULT_VSAX_GROUP_NAME = "LKDataCenter";
const BILLING_IMPORT_KEY = "cloud-price-billing-import";
const BILLING_TAGS_KEY = "cloud-price-billing-tags";
const BILLING_DETAIL_TAGS_KEY = "cloud-price-billing-detail-tags";
const BILLING_MONTH_UNKNOWN_KEY = "unknown";
const AUTH_SYNC_STORAGE_KEYS = [
  SCENARIO_STORAGE_KEY,
  SAVED_COMPARE_SCENARIOS_KEY,
  SAVED_COMPARE_PRIVATE_KEY,
  PRIVATE_STORAGE_KEY,
  PRIVATE_PROVIDERS_KEY,
  PRIVATE_COMPARE_KEY,
  VSAX_COMPARE_PREFS_KEY,
  BILLING_IMPORT_KEY,
  BILLING_TAGS_KEY,
  BILLING_DETAIL_TAGS_KEY,
];
const AUTH_SYNC_STORAGE_PREFIX = "cloud-price-";
const AUTH_SYNC_STORAGE_KEY_SET = new Set(AUTH_SYNC_STORAGE_KEYS);
const AUTH_STATE_VERSION = 1;
const AUTH_SYNC_DEBOUNCE_MS = 1200;
const DATA_HISTORY_STORAGE_PREFIX = "cps-state-history";
const DATA_HISTORY_MAX_ENTRIES = 50;
const BILLING_UNTAGGED_FILTER = "__untagged__";
const SCENARIO_SCHEMA_VERSION = 2;
const QUALITY_WARNING_LIMIT = 8;
const BILLING_IMPORT_PROVIDERS = ["aws", "azure", "gcp", "rackspace"];
const COMMITMENT_TYPE_DEFAULTS = {
  aws: "aws-savings-plan",
  azure: "azure-reservation",
  gcp: "gcp-cud-1y",
};
const COMMITMENT_TYPE_PROFILES = {
  "aws-savings-plan": {
    label: "AWS Savings Plan",
    recommendedDiscount: 25,
  },
  "aws-reserved-instance": {
    label: "AWS Reserved Instance",
    recommendedDiscount: 30,
  },
  "azure-reservation": {
    label: "Azure Reservation",
    recommendedDiscount: 28,
  },
  "azure-savings-plan": {
    label: "Azure Savings Plan",
    recommendedDiscount: 22,
  },
  "gcp-cud-1y": {
    label: "GCP CUD 1-year",
    recommendedDiscount: 20,
  },
  "gcp-cud-3y": {
    label: "GCP CUD 3-year",
    recommendedDiscount: 40,
  },
};
let sqlRateTouched = false;
let sizeOptions = null;
let lastPricing = null;
let currentMode = "vm";
let activePanel = "vm";
let currentView = "compare";
let currentResultsTab = "pricing";
let currentVendorView = "options";
let currentNetworkResult = "vpc";
let currentStorageResult = "object";
let currentBillingProvider = "aws";
let savedCompareRows = [];
let savedComparePrivateRows = [];
let savedCompareScenarioSelections = null;
let savedComparePrivateSelections = null;
let privateProviderStore = { activeId: null, providers: [] };
let privateCompareSelections = [];
let privateProviderCards = new Map();
let vsaxCapacityCatalog = { loadedAt: null, groups: [] };
let vsaxCompareState = {
  loading: false,
  selectedGroupName: "",
  regionKey: "us-east",
  pricingProvider: "api",
  data: null,
};
const vendorOptionState = {
  aws: [],
  azure: [],
  gcp: [],
  private: [],
};
let scenarioStore = [];
let billingImportStore = buildEmptyBillingImportStore();
let billingTagStore = { aws: {}, azure: {}, gcp: {}, rackspace: {} };
let billingDetailTagStore = { aws: {}, azure: {}, gcp: {}, rackspace: {} };
let authSession = null;
let authSyncTimer = null;
let authSyncInFlight = false;
let pendingGuestImportState = null;
let isAdminUser = false;
let adminUsers = [];
let authSyncedStateCache = {};
let authDataHistoryCache = {};
const billingProductFilterSelections = {
  aws: "",
  azure: "",
  gcp: "",
  rackspace: "",
  unified: "",
};
const billingMonthSelections = {
  aws: "all",
  azure: "all",
  gcp: "all",
  rackspace: "all",
  unified: "all",
};
const billingChartGroupSelections = {
  aws: "service",
  azure: "service",
  gcp: "service",
  rackspace: "service",
  unified: "service",
};
const billingExpandedServices = {
  aws: new Set(),
  azure: new Set(),
  gcp: new Set(),
  rackspace: new Set(),
  unified: new Set(),
};
const billingSelectedDetailKeys = {
  aws: new Set(),
  azure: new Set(),
  gcp: new Set(),
  rackspace: new Set(),
  unified: new Set(),
};
const instancePools = {
  aws: [],
  azure: [],
  gcp: [],
};

function isAuthenticatedUserSession() {
  return Boolean(authSession?.authenticated && authSession?.user?.username);
}

function isPricingSyncedStorageKey(keyRaw) {
  const key = String(keyRaw || "").trim();
  if (!key) {
    return false;
  }
  return (
    key === PRICING_PANEL_STORAGE_KEY ||
    AUTH_SYNC_STORAGE_KEY_SET.has(key) ||
    key.startsWith(AUTH_SYNC_STORAGE_PREFIX)
  );
}

function listPricingStorageKeys(options = {}) {
  const managedOnly = options.managedOnly === true;

  if (isAuthenticatedUserSession()) {
    const keys = Object.keys(authSyncedStateCache || {});
    return managedOnly ? keys.filter((key) => isPricingSyncedStorageKey(key)) : keys;
  }

  const keys = [];
  try {
    for (let index = 0; index < localStorage.length; index += 1) {
      const key = localStorage.key(index);
      if (!key) {
        continue;
      }
      keys.push(key);
    }
  } catch (_error) {
    return [];
  }

  return managedOnly ? keys.filter((key) => isPricingSyncedStorageKey(key)) : keys;
}

function readPricingStorageValue(keyRaw) {
  const key = String(keyRaw || "").trim();
  if (!key) {
    return null;
  }

  if (isAuthenticatedUserSession() && isPricingSyncedStorageKey(key)) {
    return Object.prototype.hasOwnProperty.call(authSyncedStateCache, key)
      ? authSyncedStateCache[key]
      : null;
  }

  try {
    return localStorage.getItem(key);
  } catch (_error) {
    return null;
  }
}

function writePricingStorageValue(keyRaw, valueRaw, options = {}) {
  const key = String(keyRaw || "").trim();
  if (!key) {
    return false;
  }
  const value = String(valueRaw === undefined || valueRaw === null ? "" : valueRaw);
  const shouldSync = options.sync !== false;
  const managed = isPricingSyncedStorageKey(key);

  if (isAuthenticatedUserSession() && managed) {
    authSyncedStateCache[key] = value;
    if (shouldSync) {
      scheduleSyncedStatePush();
    }
    return true;
  }

  try {
    localStorage.setItem(key, value);
    if (managed && shouldSync) {
      scheduleSyncedStatePush();
    }
    return true;
  } catch (_error) {
    return false;
  }
}

function removePricingStorageValue(keyRaw, options = {}) {
  const key = String(keyRaw || "").trim();
  if (!key) {
    return false;
  }
  const shouldSync = options.sync !== false;
  const managed = isPricingSyncedStorageKey(key);

  if (isAuthenticatedUserSession() && managed) {
    delete authSyncedStateCache[key];
    if (shouldSync) {
      scheduleSyncedStatePush();
    }
    return true;
  }

  try {
    localStorage.removeItem(key);
    if (managed && shouldSync) {
      scheduleSyncedStatePush();
    }
    return true;
  } catch (_error) {
    return false;
  }
}

function collectPricingSyncedStorageState(options = {}) {
  const forceLocal = options.forceLocal === true;
  const state = {};
  let keys = [];
  if (forceLocal) {
    try {
      for (let index = 0; index < localStorage.length; index += 1) {
        const key = localStorage.key(index);
        if (key && isPricingSyncedStorageKey(key)) {
          keys.push(key);
        }
      }
    } catch (_error) {
      keys = [];
    }
  } else {
    keys = listPricingStorageKeys({ managedOnly: true });
  }
  keys.forEach((key) => {
    const value = forceLocal
      ? (() => {
          try {
            return localStorage.getItem(key);
          } catch (_error) {
            return null;
          }
        })()
      : readPricingStorageValue(key);
    if (typeof value === "string") {
      state[key] = value;
    }
  });
  return state;
}

function clearManagedPricingLocalStorage() {
  let keys = [];
  try {
    for (let index = 0; index < localStorage.length; index += 1) {
      const key = localStorage.key(index);
      if (key) {
        keys.push(key);
      }
    }
  } catch (_error) {
    return;
  }
  keys = keys.filter((key) => isPricingSyncedStorageKey(key));
  keys.forEach((key) => {
    try {
      localStorage.removeItem(key);
    } catch (_error) {
      // Ignore storage errors.
    }
  });
}
const PRIVATE_FLAVORS = [
  { key: "8-16", vcpu: 8, ram: 16 },
  { key: "12-24", vcpu: 12, ram: 24 },
  { key: "16-32", vcpu: 16, ram: 32 },
  { key: "24-48", vcpu: 24, ram: 48 },
  { key: "48-64", vcpu: 48, ram: 64 },
  { key: "64-128", vcpu: 64, ram: 128 },
  { key: "128-512", vcpu: 128, ram: 512 },
];
const DEFAULT_PRIVATE_CONFIG = {
  enabled: false,
  vmwareMonthly: 0,
  windowsLicenseMonthly: 0,
  nodeCount: 2,
  storagePerTb: 0,
  networkMonthly: 0,
  firewallMonthly: 0,
  loadBalancerMonthly: 0,
  nodeCpu: 1,
  nodeRam: 128,
  nodeStorageTb: 2,
  vmOsDiskGb: 256,
  sanUsableTb: 0,
  sanTotalMonthly: 0,
  vsaxGroupName: DEFAULT_VSAX_GROUP_NAME,
};
const SCENARIO_CSV_FIELDS = [
  { key: "name", label: "Scenario", type: "string" },
  { key: "mode", label: "Mode", type: "string" },
  { key: "pricingFocus", label: "Pricing_Focus", type: "string" },
  { key: "networkAddonFocus", label: "Network_Addon_Focus", type: "string" },
  { key: "interVlanTb", label: "Inter_VLAN_TB", type: "number" },
  { key: "intraVlanTb", label: "Intra_VLAN_TB", type: "number" },
  { key: "interRegionTb", label: "Inter_Region_TB", type: "number" },
  { key: "storageIops", label: "Storage_IOPS", type: "number" },
  { key: "storageThroughputMbps", label: "Storage_Throughput_MBps", type: "number" },
  {
    key: "storageRequestUnitsMillion",
    label: "Storage_Request_Units_Million",
    type: "number",
  },
  {
    key: "storageOperationUnitsMillion",
    label: "Storage_Operation_Units_Million",
    type: "number",
  },
  { key: "workload", label: "Workload", type: "string" },
  { key: "regionKey", label: "Region_Key", type: "string" },
  { key: "pricingProvider", label: "Pricing_Provider", type: "string" },
  { key: "cpu", label: "vCPU", type: "number" },
  { key: "vmCount", label: "VM_Count", type: "number" },
  { key: "awsInstanceType", label: "AWS_Instance", type: "string" },
  { key: "azureInstanceType", label: "Azure_Instance", type: "string" },
  { key: "gcpInstanceType", label: "GCP_Instance", type: "string" },
  { key: "diskTier", label: "Disk_Tier", type: "string" },
  { key: "osDiskGb", label: "OS_Disk_GB", type: "number" },
  { key: "dataDiskTb", label: "Data_Disk_TB", type: "number" },
  { key: "egressTb", label: "Egress_TB", type: "number" },
  { key: "hours", label: "Hours", type: "number" },
  { key: "backupEnabled", label: "Backups_Enabled", type: "boolean" },
  { key: "drPercent", label: "DR_Percent", type: "number" },
  { key: "sqlEdition", label: "SQL_Edition", type: "string" },
  { key: "sqlLicenseRate", label: "SQL_License_Rate", type: "number" },
  { key: "awsVpcFlavor", label: "AWS_VPC", type: "string" },
  { key: "awsFirewallFlavor", label: "AWS_Firewall", type: "string" },
  { key: "awsLoadBalancerFlavor", label: "AWS_Load_Balancer", type: "string" },
  { key: "azureVpcFlavor", label: "Azure_VNet", type: "string" },
  { key: "azureFirewallFlavor", label: "Azure_Firewall", type: "string" },
  { key: "azureLoadBalancerFlavor", label: "Azure_Load_Balancer", type: "string" },
  { key: "gcpVpcFlavor", label: "GCP_VPC", type: "string" },
  { key: "gcpFirewallFlavor", label: "GCP_Firewall", type: "string" },
  { key: "gcpLoadBalancerFlavor", label: "GCP_Load_Balancer", type: "string" },
  {
    key: "awsNetworkVpcFlavor",
    label: "AWS_Network_VPC_Flavor",
    type: "string",
  },
  { key: "awsNetworkVpcCount", label: "AWS_Network_VPC_Count", type: "number" },
  { key: "awsNetworkVpcDataTb", label: "AWS_Network_VPC_Data_TB", type: "number" },
  {
    key: "awsNetworkGatewayFlavor",
    label: "AWS_Network_Gateway_Flavor",
    type: "string",
  },
  {
    key: "awsNetworkGatewayCount",
    label: "AWS_Network_Gateway_Count",
    type: "number",
  },
  {
    key: "awsNetworkGatewayDataTb",
    label: "AWS_Network_Gateway_Data_TB",
    type: "number",
  },
  {
    key: "awsNetworkLoadBalancerFlavor",
    label: "AWS_Network_LB_Flavor",
    type: "string",
  },
  {
    key: "awsNetworkLoadBalancerCount",
    label: "AWS_Network_LB_Count",
    type: "number",
  },
  {
    key: "awsNetworkLoadBalancerDataTb",
    label: "AWS_Network_LB_Data_TB",
    type: "number",
  },
  {
    key: "azureNetworkVpcFlavor",
    label: "Azure_Network_VPC_Flavor",
    type: "string",
  },
  {
    key: "azureNetworkVpcCount",
    label: "Azure_Network_VPC_Count",
    type: "number",
  },
  {
    key: "azureNetworkVpcDataTb",
    label: "Azure_Network_VPC_Data_TB",
    type: "number",
  },
  {
    key: "azureNetworkGatewayFlavor",
    label: "Azure_Network_Gateway_Flavor",
    type: "string",
  },
  {
    key: "azureNetworkGatewayCount",
    label: "Azure_Network_Gateway_Count",
    type: "number",
  },
  {
    key: "azureNetworkGatewayDataTb",
    label: "Azure_Network_Gateway_Data_TB",
    type: "number",
  },
  {
    key: "azureNetworkLoadBalancerFlavor",
    label: "Azure_Network_LB_Flavor",
    type: "string",
  },
  {
    key: "azureNetworkLoadBalancerCount",
    label: "Azure_Network_LB_Count",
    type: "number",
  },
  {
    key: "azureNetworkLoadBalancerDataTb",
    label: "Azure_Network_LB_Data_TB",
    type: "number",
  },
  {
    key: "gcpNetworkVpcFlavor",
    label: "GCP_Network_VPC_Flavor",
    type: "string",
  },
  {
    key: "gcpNetworkVpcCount",
    label: "GCP_Network_VPC_Count",
    type: "number",
  },
  {
    key: "gcpNetworkVpcDataTb",
    label: "GCP_Network_VPC_Data_TB",
    type: "number",
  },
  {
    key: "gcpNetworkGatewayFlavor",
    label: "GCP_Network_Gateway_Flavor",
    type: "string",
  },
  {
    key: "gcpNetworkGatewayCount",
    label: "GCP_Network_Gateway_Count",
    type: "number",
  },
  {
    key: "gcpNetworkGatewayDataTb",
    label: "GCP_Network_Gateway_Data_TB",
    type: "number",
  },
  {
    key: "gcpNetworkLoadBalancerFlavor",
    label: "GCP_Network_LB_Flavor",
    type: "string",
  },
  {
    key: "gcpNetworkLoadBalancerCount",
    label: "GCP_Network_LB_Count",
    type: "number",
  },
  {
    key: "gcpNetworkLoadBalancerDataTb",
    label: "GCP_Network_LB_Data_TB",
    type: "number",
  },
  { key: "awsObjectStorageRate", label: "AWS_Object_Storage_TB", type: "number" },
  { key: "azureObjectStorageRate", label: "Azure_Object_Storage_TB", type: "number" },
  { key: "gcpObjectStorageRate", label: "GCP_Object_Storage_TB", type: "number" },
  {
    key: "awsStorageAccountCount",
    label: "AWS_Storage_Account_Count",
    type: "number",
  },
  { key: "awsStorageDrEnabled", label: "AWS_Storage_DR", type: "boolean" },
  { key: "awsStorageDrDeltaTb", label: "AWS_Storage_DR_Delta_TB", type: "number" },
  { key: "awsStorageObjectTb", label: "AWS_Storage_Object_TB", type: "number" },
  { key: "awsStorageFileTb", label: "AWS_Storage_File_TB", type: "number" },
  { key: "awsStorageTableTb", label: "AWS_Storage_Table_TB", type: "number" },
  { key: "awsStorageQueueTb", label: "AWS_Storage_Queue_TB", type: "number" },
  {
    key: "azureStorageAccountCount",
    label: "Azure_Storage_Account_Count",
    type: "number",
  },
  { key: "azureStorageDrEnabled", label: "Azure_Storage_DR", type: "boolean" },
  { key: "azureStorageDrDeltaTb", label: "Azure_Storage_DR_Delta_TB", type: "number" },
  { key: "azureStorageObjectTb", label: "Azure_Storage_Object_TB", type: "number" },
  { key: "azureStorageFileTb", label: "Azure_Storage_File_TB", type: "number" },
  { key: "azureStorageTableTb", label: "Azure_Storage_Table_TB", type: "number" },
  { key: "azureStorageQueueTb", label: "Azure_Storage_Queue_TB", type: "number" },
  {
    key: "gcpStorageAccountCount",
    label: "GCP_Storage_Account_Count",
    type: "number",
  },
  { key: "gcpStorageDrEnabled", label: "GCP_Storage_DR", type: "boolean" },
  { key: "gcpStorageDrDeltaTb", label: "GCP_Storage_DR_Delta_TB", type: "number" },
  { key: "gcpStorageObjectTb", label: "GCP_Storage_Object_TB", type: "number" },
  { key: "gcpStorageFileTb", label: "GCP_Storage_File_TB", type: "number" },
  { key: "gcpStorageTableTb", label: "GCP_Storage_Table_TB", type: "number" },
  { key: "gcpStorageQueueTb", label: "GCP_Storage_Queue_TB", type: "number" },
  { key: "privateEnabled", label: "Private_Enabled", type: "boolean" },
  { key: "privateVmwareMonthly", label: "Private_VMware_Monthly", type: "number" },
  { key: "privateWindowsLicenseMonthly", label: "Private_Windows_License", type: "number" },
  { key: "privateNodeCount", label: "Private_Node_Count", type: "number" },
  { key: "privateStoragePerTb", label: "Private_SAN_per_TB", type: "number" },
  { key: "privateNetworkMonthly", label: "Private_Network_Monthly", type: "number" },
  { key: "privateFirewallMonthly", label: "Private_Firewall_Monthly", type: "number" },
  { key: "privateLoadBalancerMonthly", label: "Private_Load_Balancer", type: "number" },
  { key: "privateNodeCpu", label: "Private_Node_CPU", type: "number" },
  { key: "privateNodeRam", label: "Private_Node_RAM", type: "number" },
  { key: "privateNodeStorageTb", label: "Private_Node_Storage_TB", type: "number" },
  { key: "privateVmOsDiskGb", label: "Private_VM_OS_GB", type: "number" },
  { key: "privateSanUsableTb", label: "Private_SAN_Usable_TB", type: "number" },
  { key: "privateSanTotalMonthly", label: "Private_SAN_Total_Monthly", type: "number" },
];
const PRIVATE_PROVIDER_CSV_FIELDS = [
  { key: "name", label: "Provider", type: "string" },
  { key: "enabled", label: "Enabled", type: "boolean" },
  { key: "vmwareMonthly", label: "VMware_Monthly", type: "number" },
  { key: "windowsLicenseMonthly", label: "Windows_License_Monthly", type: "number" },
  { key: "nodeCount", label: "Node_Count", type: "number" },
  { key: "nodeCpu", label: "Node_CPU", type: "number" },
  { key: "nodeRam", label: "Node_RAM", type: "number" },
  { key: "nodeStorageTb", label: "Node_Storage_TB", type: "number" },
  { key: "vmOsDiskGb", label: "VM_OS_GB", type: "number" },
  { key: "sanUsableTb", label: "SAN_Usable_TB", type: "number" },
  { key: "sanTotalMonthly", label: "SAN_Total_Monthly", type: "number" },
  { key: "vsaxGroupName", label: "VSAx_Group", type: "string" },
  { key: "storagePerTb", label: "SAN_per_TB", type: "number" },
  { key: "networkMonthly", label: "Network_Monthly", type: "number" },
  { key: "firewallMonthly", label: "Firewall_Monthly", type: "number" },
  { key: "loadBalancerMonthly", label: "Load_Balancer_Monthly", type: "number" },
];
let sqlState = {
  edition: sqlEditionSelect.value,
  rate: sqlRateInput.value,
};
const K8S_OS_DISK_MIN_GB = 32;
const K8S_MIN_NODE_COUNT = 3;

const MODE_COPY = {
  vm: {
    formTitle: "Workload inputs",
    formSubtitle:
      "Windows only, no local or temp disks, disk tier selectable, network >= 10 Gbps.",
    resultsTitle: "Price comparison",
    resultsSubtitle: "Live compute rates + estimated storage, egress, and SQL.",
    cpuLabel: "vCPU count (min 8)",
    countLabel: "VM count",
    egressLabel: "Egress (TB / month per VM)",
    awsTitle: "AWS",
    azureTitle: "Azure",
    gcpTitle: "GCP",
    privateTitle: "Private",
  },
  k8s: {
    formTitle: "Kubernetes inputs",
    formSubtitle:
      "Premium managed Kubernetes tiers (Linux nodes). Disk tier selectable for OS disks.",
    resultsTitle: "Kubernetes price comparison",
    resultsSubtitle:
      "Node compute rates + control plane fees + storage and egress.",
    cpuLabel: "Node vCPU count (min 8)",
    countLabel: "Node count (min 3)",
    egressLabel: "Egress (TB / month per cluster)",
    awsTitle: "EKS",
    azureTitle: "AKS",
    gcpTitle: "GKE",
    privateTitle: "Private",
  },
  network: {
    formTitle: "Network pricing inputs",
    formSubtitle:
      "Public-cloud networking only (VPC/VNet, gateway, load balancer, inter/intra VLAN, inter-region transfer, and egress).",
    resultsTitle: "Network pricing",
    resultsSubtitle:
      "VPC/VNet, VPC/VPN gateway, and load balancer pricing across AWS, Azure, and GCP.",
    cpuLabel: "vCPU count (min 8)",
    countLabel: "VM count",
    egressLabel: "Egress (TB / month)",
    awsTitle: "AWS",
    azureTitle: "Azure",
    gcpTitle: "GCP",
    privateTitle: "Private",
  },
  storage: {
    formTitle: "Storage pricing inputs",
    formSubtitle:
      "Public storage services only (object, file, table, queue, and DR replication delta).",
    resultsTitle: "Storage pricing",
    resultsSubtitle:
      "Shared storage pricing across AWS, Azure, and GCP.",
    cpuLabel: "vCPU count (min 8)",
    countLabel: "VM count",
    egressLabel: "Egress (TB / month)",
    awsTitle: "AWS",
    azureTitle: "Azure",
    gcpTitle: "GCP",
    privateTitle: "Private",
  },
};

const COMMIT_COMPONENTS = [
  { key: "compute", field: "computeMonthly" },
  { key: "control", field: "controlPlaneMonthly" },
  { key: "storage", field: "storageMonthly" },
  { key: "backup", field: "backupMonthly" },
  { key: "egress", field: "egressMonthly" },
  { key: "network", field: "networkMonthly" },
  { key: "sql", field: "sqlMonthly" },
  { key: "windows", field: "windowsLicenseMonthly" },
  { key: "dr", field: "drMonthly" },
];

const RESULTS_TAB_COPY = {
  saved: {
    title: "Saved Compare",
    subtitle: "Run saved scenarios in a multi-provider dashboard.",
  },
  insight: {
    title: "Insight",
    subtitle: "Cost-driver breakdown across compute, storage, egress, and licenses.",
  },
  commit: {
    title: "Cloud Commit",
    subtitle:
      "Apply per-provider discounts to compute only and compare committed totals.",
  },
};

const SCENARIO_BREAKDOWN_COMPONENTS = [
  { label: "Compute", field: "computeMonthly" },
  { label: "Control plane", field: "controlPlaneMonthly" },
  { label: "Storage", field: "storageMonthly" },
  { label: "Backups", field: "backupMonthly" },
  { label: "Network", field: "networkMonthly" },
  { label: "Inter-region", field: "interRegionMonthly" },
  { label: "Egress", field: "egressMonthly" },
  { label: "Licenses", field: "licenseMonthly" },
  { label: "DR", field: "drMonthly" },
];

const NETWORK_HARDCODED_RATES = {
  egress: { aws: 0.09, azure: 0.087, gcp: 0.12 },
  interVlan: { aws: 0.01, azure: 0.01, gcp: 0.01 },
  intraVlan: { aws: 0, azure: 0, gcp: 0 },
  interRegion: { aws: 0.02, azure: 0.02, gcp: 0.02 },
  addonData: {
    aws: { gateway: 0.02, firewall: 0.01, loadBalancer: 0.008 },
    azure: { gateway: 0.018, firewall: 0.016, loadBalancer: 0.008 },
    gcp: { gateway: 0.018, firewall: 0.01, loadBalancer: 0.01 },
  },
};

const STORAGE_HARDCODED_RATES = {
  aws: { object: 0.023, file: 0.3, table: 0.25, queue: 0, replication: 0.02 },
  azure: {
    object: 0.018,
    file: 0.16,
    table: 0.06,
    queue: 0.06,
    replication: 0.02,
  },
  gcp: { object: 0.02, file: 0.3, table: 0.17, queue: 0.08, replication: 0.02 },
};

const fields = {
  aws: {
    status: document.getElementById("aws-status"),
    family: document.getElementById("aws-family"),
    instance: document.getElementById("aws-instance"),
    shape: document.getElementById("aws-shape"),
    region: document.getElementById("aws-region"),
    hourly: document.getElementById("aws-hourly"),
    network: document.getElementById("aws-network"),
    tiers: {
      onDemand: {
        total: document.getElementById("aws-od-total"),
        rate: document.getElementById("aws-od-rate"),
      },
      year1: {
        total: document.getElementById("aws-1y-total"),
        rate: document.getElementById("aws-1y-rate"),
      },
      year3: {
        total: document.getElementById("aws-3y-total"),
        rate: document.getElementById("aws-3y-rate"),
      },
    },
    savings: document.getElementById("aws-savings"),
    breakdown: document.getElementById("aws-breakdown"),
    note: document.getElementById("aws-note"),
  },
  azure: {
    status: document.getElementById("azure-status"),
    family: document.getElementById("azure-family"),
    instance: document.getElementById("azure-instance"),
    shape: document.getElementById("azure-shape"),
    region: document.getElementById("azure-region"),
    hourly: document.getElementById("azure-hourly"),
    network: document.getElementById("azure-network"),
    tiers: {
      onDemand: {
        total: document.getElementById("azure-od-total"),
        rate: document.getElementById("azure-od-rate"),
      },
      year1: {
        total: document.getElementById("azure-1y-total"),
        rate: document.getElementById("azure-1y-rate"),
      },
      year3: {
        total: document.getElementById("azure-3y-total"),
        rate: document.getElementById("azure-3y-rate"),
      },
    },
    savings: document.getElementById("azure-savings"),
    breakdown: document.getElementById("azure-breakdown"),
    note: document.getElementById("azure-note"),
  },
  gcp: {
    status: document.getElementById("gcp-status"),
    family: document.getElementById("gcp-family"),
    instance: document.getElementById("gcp-instance"),
    shape: document.getElementById("gcp-shape"),
    region: document.getElementById("gcp-region"),
    hourly: document.getElementById("gcp-hourly"),
    network: document.getElementById("gcp-network"),
    tiers: {
      onDemand: {
        total: document.getElementById("gcp-od-total"),
        rate: document.getElementById("gcp-od-rate"),
      },
      year1: {
        total: document.getElementById("gcp-1y-total"),
        rate: document.getElementById("gcp-1y-rate"),
      },
      year3: {
        total: document.getElementById("gcp-3y-total"),
        rate: document.getElementById("gcp-3y-rate"),
      },
    },
    savings: document.getElementById("gcp-savings"),
    breakdown: document.getElementById("gcp-breakdown"),
    note: document.getElementById("gcp-note"),
  },
};

const currency = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 2,
});

const rateFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 4,
});

function formatMoney(value) {
  if (!Number.isFinite(value)) {
    return "N/A";
  }
  return currency.format(value);
}

function formatRate(value) {
  if (!Number.isFinite(value)) {
    return "N/A";
  }
  return `${rateFormatter.format(value)}/hr`;
}

function formatMonthly(value) {
  if (!Number.isFinite(value)) {
    return "N/A";
  }
  return `${currency.format(value)}/mo`;
}

function escapeVsaxCompareHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatVsaxNumeric(value, maximumFractionDigits = 2) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "-";
  }
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits,
  }).format(number);
}

function formatVsaxDisk(valueGb) {
  const gb = Number(valueGb);
  if (!Number.isFinite(gb) || gb < 0) {
    return "-";
  }
  if (gb >= 1024) {
    return `${formatVsaxNumeric(gb / 1024, 2)} TB`;
  }
  return `${formatVsaxNumeric(gb, 1)} GB`;
}

function formatVsaxFlavorShape(providerData) {
  const vcpu = Number(providerData?.vcpu);
  const memoryGb = Number(providerData?.memoryGb);
  if (!Number.isFinite(vcpu) && !Number.isFinite(memoryGb)) {
    return "-";
  }
  const vcpuText = Number.isFinite(vcpu)
    ? `${formatVsaxNumeric(vcpu, 0)} vCPU`
    : "-";
  const ramText = Number.isFinite(memoryGb)
    ? `${formatVsaxNumeric(memoryGb, 1)} GB`
    : "-";
  return `${vcpuText} / ${ramText}`;
}

function setVsaxCompareNote(message, isError = false) {
  if (!vsaxCompareNote) {
    return;
  }
  vsaxCompareNote.textContent = String(message || "");
  vsaxCompareNote.classList.toggle("negative", Boolean(isError));
}

function loadVsaxComparePreferences() {
  const raw = readPricingStorageValue(VSAX_COMPARE_PREFS_KEY);
  if (!raw) {
    return;
  }
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") {
      return;
    }
    const selectedGroupName = String(parsed.selectedGroupName || "").trim();
    const regionKey = String(parsed.regionKey || "").trim();
    const pricingProvider = String(parsed.pricingProvider || "").trim();
    if (selectedGroupName) {
      vsaxCompareState.selectedGroupName = selectedGroupName;
    }
    if (regionKey) {
      vsaxCompareState.regionKey = regionKey;
    }
    if (pricingProvider === "api" || pricingProvider === "retail") {
      vsaxCompareState.pricingProvider = pricingProvider;
    }
  } catch (_error) {
    // Ignore invalid preference payloads.
  }
}

function persistVsaxComparePreferences() {
  writePricingStorageValue(
    VSAX_COMPARE_PREFS_KEY,
    JSON.stringify({
      selectedGroupName: vsaxCompareState.selectedGroupName || "",
      regionKey: vsaxCompareState.regionKey || "us-east",
      pricingProvider: vsaxCompareState.pricingProvider || "api",
    })
  );
}

function populateVsaxCompareRegionOptions() {
  if (!(vsaxCompareRegionSelect instanceof HTMLSelectElement)) {
    return;
  }
  const regionOptions = Array.from(regionSelect?.options || []);
  if (!regionOptions.length) {
    return;
  }
  const previous = String(vsaxCompareState.regionKey || "").trim();
  vsaxCompareRegionSelect.innerHTML = "";
  regionOptions.forEach((option) => {
    const next = document.createElement("option");
    next.value = option.value;
    next.textContent = option.textContent;
    vsaxCompareRegionSelect.appendChild(next);
  });
  const validSelection = Array.from(vsaxCompareRegionSelect.options).some(
    (option) => option.value === previous
  )
    ? previous
    : String(vsaxCompareRegionSelect.options[0]?.value || "us-east");
  vsaxCompareState.regionKey = validSelection || "us-east";
  vsaxCompareRegionSelect.value = vsaxCompareState.regionKey;
}

function renderVsaxCompareSummary(data) {
  if (!vsaxCompareSummary) {
    return;
  }
  const summary = data?.summary || {};
  const awsMonthlyTotal = Number(summary.awsMonthlyTotal);
  const azureMonthlyTotal = Number(summary.azureMonthlyTotal);
  const systemCount = Number(summary.systemCount) || 0;
  const awsPricedCount = Number(summary.awsPricedCount) || 0;
  const azurePricedCount = Number(summary.azurePricedCount) || 0;
  const delta = Number(summary.monthlyDelta);
  const selectedGroup = String(data?.groupName || "All groups").trim() || "All groups";

  const cards = [
    {
      title: "Selected group",
      value: selectedGroup,
      hint: `${systemCount} system(s)`,
    },
    {
      title: "AWS total",
      value: Number.isFinite(awsMonthlyTotal) ? formatMonthly(awsMonthlyTotal) : "N/A",
      hint: `${awsPricedCount}/${systemCount} priced`,
    },
    {
      title: "Azure total",
      value: Number.isFinite(azureMonthlyTotal) ? formatMonthly(azureMonthlyTotal) : "N/A",
      hint: `${azurePricedCount}/${systemCount} priced`,
    },
    {
      title: "Azure - AWS",
      value: Number.isFinite(delta) ? formatMonthly(delta) : "N/A",
      hint:
        Number.isFinite(delta) && delta >= 0
          ? "Azure estimate higher"
          : "Azure estimate lower",
    },
  ];

  vsaxCompareSummary.innerHTML = cards
    .map(
      (card) => `
        <article class="billing-summary-card">
          <h4>${escapeVsaxCompareHtml(card.title)}</h4>
          <strong>${escapeVsaxCompareHtml(card.value)}</strong>
          <p>${escapeVsaxCompareHtml(card.hint)}</p>
        </article>
      `
    )
    .join("");
}

function renderVsaxCompareTable(data) {
  if (!vsaxCompareTable) {
    return;
  }
  const rows = Array.isArray(data?.systems) ? data.systems : [];
  if (!rows.length) {
    vsaxCompareTable.innerHTML =
      '<p class="billing-empty">No VSAx systems found for the selected group.</p>';
    return;
  }
  const body = rows
    .map((row) => {
      const systemName = String(row?.systemName || row?.deviceId || "Unnamed").trim();
      const aws = row?.aws || {};
      const azure = row?.azure || {};
      const awsFlavor = formatVsaxFlavorShape(aws);
      const azureFlavor = formatVsaxFlavorShape(azure);
      const notes = Array.isArray(row?.notes) ? row.notes.filter(Boolean).join(" | ") : "";
      return `
        <tr>
          <td>${escapeVsaxCompareHtml(systemName)}</td>
          <td class="numeric">${escapeVsaxCompareHtml(formatVsaxNumeric(row?.metrics?.vcpu, 0))}</td>
          <td class="numeric">${escapeVsaxCompareHtml(
            Number.isFinite(Number(row?.metrics?.memoryGb))
              ? `${formatVsaxNumeric(row.metrics.memoryGb, 1)} GB`
              : "-"
          )}</td>
          <td class="numeric">${escapeVsaxCompareHtml(formatVsaxDisk(row?.metrics?.diskGb))}</td>
          <td>${escapeVsaxCompareHtml(aws.instanceType || "-")}</td>
          <td>${escapeVsaxCompareHtml(awsFlavor)}</td>
          <td class="numeric">${escapeVsaxCompareHtml(
            Number.isFinite(Number(aws.hourlyRate)) ? formatRate(Number(aws.hourlyRate)) : "N/A"
          )}</td>
          <td class="numeric">${escapeVsaxCompareHtml(
            Number.isFinite(Number(aws.computeMonthly))
              ? formatMonthly(Number(aws.computeMonthly))
              : "N/A"
          )}</td>
          <td class="numeric">${escapeVsaxCompareHtml(
            Number.isFinite(Number(aws.storageMonthly))
              ? formatMonthly(Number(aws.storageMonthly))
              : "N/A"
          )}</td>
          <td class="numeric">${escapeVsaxCompareHtml(
            Number.isFinite(Number(aws.monthlyTotal))
              ? formatMonthly(Number(aws.monthlyTotal))
              : "N/A"
          )}</td>
          <td>${escapeVsaxCompareHtml(azure.instanceType || "-")}</td>
          <td>${escapeVsaxCompareHtml(azureFlavor)}</td>
          <td class="numeric">${escapeVsaxCompareHtml(
            Number.isFinite(Number(azure.hourlyRate)) ? formatRate(Number(azure.hourlyRate)) : "N/A"
          )}</td>
          <td class="numeric">${escapeVsaxCompareHtml(
            Number.isFinite(Number(azure.computeMonthly))
              ? formatMonthly(Number(azure.computeMonthly))
              : "N/A"
          )}</td>
          <td class="numeric">${escapeVsaxCompareHtml(
            Number.isFinite(Number(azure.storageMonthly))
              ? formatMonthly(Number(azure.storageMonthly))
              : "N/A"
          )}</td>
          <td class="numeric">${escapeVsaxCompareHtml(
            Number.isFinite(Number(azure.monthlyTotal))
              ? formatMonthly(Number(azure.monthlyTotal))
              : "N/A"
          )}</td>
          <td>${escapeVsaxCompareHtml(notes || "-")}</td>
        </tr>
      `;
    })
    .join("");

  vsaxCompareTable.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>System</th>
          <th class="numeric">vCPU</th>
          <th class="numeric">RAM</th>
          <th class="numeric">Disk</th>
          <th>AWS instance</th>
          <th>AWS flavor (vCPU/RAM)</th>
          <th class="numeric">AWS hourly</th>
          <th class="numeric">AWS compute</th>
          <th class="numeric">AWS storage</th>
          <th class="numeric">AWS monthly</th>
          <th>Azure instance</th>
          <th>Azure flavor (vCPU/RAM)</th>
          <th class="numeric">Azure hourly</th>
          <th class="numeric">Azure compute</th>
          <th class="numeric">Azure storage</th>
          <th class="numeric">Azure monthly</th>
          <th>Notes</th>
        </tr>
      </thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

function sanitizeVsaxCompareFilename(value) {
  return String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .toLowerCase();
}

function getVsaxCompareExportData() {
  const data = vsaxCompareState.data;
  if (!data || !Array.isArray(data.systems) || !data.systems.length) {
    return null;
  }
  const groupLabel = String(data.groupName || "vsax").trim() || "vsax";
  const safeGroup = sanitizeVsaxCompareFilename(groupLabel) || "vsax";
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  return {
    data,
    groupLabel,
    safeGroup,
    stamp,
  };
}

function downloadVsaxCompareBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

function buildVsaxCompareExportRows(data) {
  const rows = Array.isArray(data?.systems) ? data.systems : [];
  return rows.map((row) => ({
    Group: row?.groupName || data?.groupName || "",
    System: row?.systemName || row?.deviceId || "",
    Device_ID: row?.deviceId || "",
    vCPU: row?.metrics?.vcpu ?? "",
    RAM_GB: row?.metrics?.memoryGb ?? "",
    Disk_GB: row?.metrics?.diskGb ?? "",
    OS_Disk_GB: row?.metrics?.osDiskGb ?? "",
    Data_Disk_GB: row?.metrics?.dataDiskGb ?? "",
    AWS_Instance: row?.aws?.instanceType || "",
    AWS_Flavor_Shape: formatVsaxFlavorShape(row?.aws),
    AWS_Flavor_vCPU: row?.aws?.vcpu ?? "",
    AWS_Flavor_RAM_GB: row?.aws?.memoryGb ?? "",
    AWS_Hourly: row?.aws?.hourlyRate ?? "",
    AWS_Compute_Monthly: row?.aws?.computeMonthly ?? "",
    AWS_Storage_Monthly: row?.aws?.storageMonthly ?? "",
    AWS_Monthly: row?.aws?.monthlyTotal ?? "",
    AWS_Source: row?.aws?.source || "",
    AWS_Status: row?.aws?.status || "",
    Azure_Instance: row?.azure?.instanceType || "",
    Azure_Flavor_Shape: formatVsaxFlavorShape(row?.azure),
    Azure_Flavor_vCPU: row?.azure?.vcpu ?? "",
    Azure_Flavor_RAM_GB: row?.azure?.memoryGb ?? "",
    Azure_Hourly: row?.azure?.hourlyRate ?? "",
    Azure_Compute_Monthly: row?.azure?.computeMonthly ?? "",
    Azure_Storage_Monthly: row?.azure?.storageMonthly ?? "",
    Azure_Monthly: row?.azure?.monthlyTotal ?? "",
    Azure_Source: row?.azure?.source || "",
    Azure_Status: row?.azure?.status || "",
    Notes: Array.isArray(row?.notes) ? row.notes.join(" | ") : "",
    Last_Sync_At: row?.lastSyncAt || "",
  }));
}

function buildVsaxCompareCsv(data) {
  const rows = buildVsaxCompareExportRows(data);
  if (!rows.length) {
    return "";
  }
  const headers = Object.keys(rows[0]);
  const lines = [headers.map((value) => escapeCsv(value)).join(",")];
  rows.forEach((row) => {
    lines.push(headers.map((header) => escapeCsv(row[header])).join(","));
  });
  return lines.join("\n");
}

function buildVsaxCompareExcelHtml(data) {
  const summary = data?.summary || {};
  const rows = buildVsaxCompareExportRows(data);
  const headers = rows.length ? Object.keys(rows[0]) : [];
  const headerHtml = headers
    .map((header) => `<th>${escapeVsaxCompareHtml(header)}</th>`)
    .join("");
  const bodyHtml = rows
    .map((row) => {
      const cells = headers
        .map((header) => `<td>${escapeVsaxCompareHtml(row[header])}</td>`)
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");
  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>VSAx Price Compare</title>
    <style>
      body { font-family: Arial, sans-serif; font-size: 12px; color: #111; }
      h1 { font-size: 18px; margin: 0 0 8px; }
      .meta { margin: 0 0 4px; }
      .summary { margin: 10px 0 14px; }
      .summary strong { margin-right: 12px; }
      table { border-collapse: collapse; width: 100%; }
      th, td { border: 1px solid #c7ced8; padding: 6px 8px; vertical-align: top; }
      th { background: #e9eef5; text-align: left; }
    </style>
  </head>
  <body>
    <h1>CloudStudio - Price Compare VSAx</h1>
    <p class="meta">Generated at: ${escapeVsaxCompareHtml(data?.generatedAt || new Date().toISOString())}</p>
    <p class="meta">Group: ${escapeVsaxCompareHtml(data?.groupName || "-")} | Region: ${escapeVsaxCompareHtml(
      data?.regionLabel || data?.regionKey || "-"
    )} | Source: ${escapeVsaxCompareHtml(data?.pricingProvider || "-")}</p>
    <div class="summary">
      <strong>Systems: ${escapeVsaxCompareHtml(summary.systemCount ?? 0)}</strong>
      <strong>AWS Total: ${escapeVsaxCompareHtml(
        Number.isFinite(Number(summary.awsMonthlyTotal))
          ? formatMonthly(Number(summary.awsMonthlyTotal))
          : "N/A"
      )}</strong>
      <strong>Azure Total: ${escapeVsaxCompareHtml(
        Number.isFinite(Number(summary.azureMonthlyTotal))
          ? formatMonthly(Number(summary.azureMonthlyTotal))
          : "N/A"
      )}</strong>
    </div>
    <table>
      <thead><tr>${headerHtml}</tr></thead>
      <tbody>${bodyHtml}</tbody>
    </table>
  </body>
</html>`;
}

function toPdfAscii(value) {
  return String(value || "").replace(/[^\x20-\x7E]/g, "?");
}

function escapePdfText(value) {
  return toPdfAscii(value)
    .replace(/\\/g, "\\\\")
    .replace(/\(/g, "\\(")
    .replace(/\)/g, "\\)");
}

function chunkPdfLines(lines, maxLinesPerPage) {
  const pages = [];
  let index = 0;
  while (index < lines.length) {
    pages.push(lines.slice(index, index + maxLinesPerPage));
    index += maxLinesPerPage;
  }
  return pages.length ? pages : [[]];
}

function buildSimpleTextPdf(lines) {
  const maxLinesPerPage = 58;
  const pages = chunkPdfLines(lines, maxLinesPerPage);
  const objects = [];
  const addObject = (content) => {
    const number = objects.length + 1;
    objects.push({ number, content });
    return number;
  };

  const catalogObject = addObject("");
  const pagesObject = addObject("");
  const fontObject = addObject("<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>");

  const pageObjectNumbers = [];
  pages.forEach((pageLines) => {
    const streamLines = [
      "BT",
      "/F1 8.5 Tf",
      "32 780 Td",
      "11 TL",
    ];
    pageLines.forEach((line, lineIndex) => {
      streamLines.push(`(${escapePdfText(line)}) Tj`);
      if (lineIndex < pageLines.length - 1) {
        streamLines.push("T*");
      }
    });
    streamLines.push("ET");
    const stream = streamLines.join("\n");
    const contentObject = addObject(
      `<< /Length ${stream.length} >>\nstream\n${stream}\nendstream`
    );
    const pageObject = addObject(
      `<< /Type /Page /Parent ${pagesObject} 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 ${fontObject} 0 R >> >> /Contents ${contentObject} 0 R >>`
    );
    pageObjectNumbers.push(pageObject);
  });

  objects[catalogObject - 1].content = `<< /Type /Catalog /Pages ${pagesObject} 0 R >>`;
  objects[pagesObject - 1].content = `<< /Type /Pages /Kids [${pageObjectNumbers
    .map((number) => `${number} 0 R`)
    .join(" ")}] /Count ${pageObjectNumbers.length} >>`;

  let pdf = "%PDF-1.4\n";
  const offsets = [0];
  objects.forEach((object) => {
    offsets[object.number] = pdf.length;
    pdf += `${object.number} 0 obj\n${object.content}\nendobj\n`;
  });
  const xrefOffset = pdf.length;
  pdf += `xref\n0 ${objects.length + 1}\n`;
  pdf += "0000000000 65535 f \n";
  for (let i = 1; i <= objects.length; i += 1) {
    pdf += `${String(offsets[i]).padStart(10, "0")} 00000 n \n`;
  }
  pdf += `trailer\n<< /Size ${objects.length + 1} /Root ${catalogObject} 0 R >>\n`;
  pdf += `startxref\n${xrefOffset}\n%%EOF`;

  return new Blob([pdf], { type: "application/pdf" });
}

function buildVsaxComparePdf(data) {
  const summary = data?.summary || {};
  const rows = Array.isArray(data?.systems) ? data.systems : [];
  const lines = [];
  const formatPdfNumber = (value, digits = 2) => {
    const number = Number(value);
    if (!Number.isFinite(number)) {
      return "N/A";
    }
    return number.toLocaleString("en-US", {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  };
  const formatPdfRate = (value) => {
    const number = Number(value);
    if (!Number.isFinite(number)) {
      return "N/A";
    }
    return `$${formatPdfNumber(number, 3)}/hr`;
  };
  const formatPdfMonthly = (value) => {
    const number = Number(value);
    if (!Number.isFinite(number)) {
      return "N/A";
    }
    return `$${formatPdfNumber(number, 2)}/mo`;
  };
  const formatPdfFlavor = (providerData) => {
    const vcpu = Number(providerData?.vcpu);
    const ram = Number(providerData?.memoryGb);
    if (!Number.isFinite(vcpu) && !Number.isFinite(ram)) {
      return "N/A";
    }
    const vcpuLabel = Number.isFinite(vcpu) ? `${formatPdfNumber(vcpu, 0)} vCPU` : "-";
    const ramLabel = Number.isFinite(ram) ? `${formatPdfNumber(ram, 1)} GB` : "-";
    return `${vcpuLabel} / ${ramLabel}`;
  };
  const separator = "-".repeat(120);

  lines.push("CloudStudio - Price Compare VSAx");
  lines.push(`Generated: ${data?.generatedAt || new Date().toISOString()}`);
  lines.push(
    `Group: ${data?.groupName || "-"} | Region: ${
      data?.regionLabel || data?.regionKey || "-"
    } | Source: ${data?.pricingProvider || "-"}`
  );
  lines.push(
    `Systems: ${summary.systemCount || 0} | AWS Total: ${
      Number.isFinite(Number(summary.awsMonthlyTotal))
        ? formatMonthly(Number(summary.awsMonthlyTotal))
        : "N/A"
    } | Azure Total: ${
      Number.isFinite(Number(summary.azureMonthlyTotal))
        ? formatMonthly(Number(summary.azureMonthlyTotal))
        : "N/A"
    }`
  );
  lines.push(separator);
  rows.forEach((row) => {
    const systemName = String(row?.systemName || row?.deviceId || "Unnamed");
    const vcpu = Number.isFinite(Number(row?.metrics?.vcpu))
      ? formatPdfNumber(row?.metrics?.vcpu, 0)
      : "N/A";
    const ram = Number.isFinite(Number(row?.metrics?.memoryGb))
      ? `${formatPdfNumber(row?.metrics?.memoryGb, 1)} GB`
      : "N/A";
    const disk = Number.isFinite(Number(row?.metrics?.diskGb))
      ? `${formatPdfNumber(row?.metrics?.diskGb, 1)} GB`
      : "N/A";
    lines.push(`System: ${systemName}`);
    lines.push(`  Shape: ${vcpu} vCPU | ${ram} RAM | ${disk} Disk`);
    lines.push(
      `  AWS: ${row?.aws?.instanceType || "-"} | Flavor ${formatPdfFlavor(row?.aws)} | Hourly ${formatPdfRate(
        row?.aws?.hourlyRate
      )} | Compute ${formatPdfMonthly(row?.aws?.computeMonthly)} | Storage ${formatPdfMonthly(
        row?.aws?.storageMonthly
      )} | Total ${formatPdfMonthly(row?.aws?.monthlyTotal)}`
    );
    lines.push(
      `  Azure: ${row?.azure?.instanceType || "-"} | Flavor ${formatPdfFlavor(
        row?.azure
      )} | Hourly ${formatPdfRate(row?.azure?.hourlyRate)} | Compute ${formatPdfMonthly(
        row?.azure?.computeMonthly
      )} | Storage ${formatPdfMonthly(row?.azure?.storageMonthly)} | Total ${formatPdfMonthly(
        row?.azure?.monthlyTotal
      )}`
    );
    if (Array.isArray(row?.notes) && row.notes.length) {
      lines.push(`  Notes: ${row.notes.join(" | ")}`);
    }
    lines.push(separator);
  });
  return buildSimpleTextPdf(lines);
}

function handleVsaxCompareExportCsv() {
  const exportData = getVsaxCompareExportData();
  if (!exportData) {
    setVsaxCompareNote("Run VSAx compare first, then export.", true);
    return;
  }
  const csv = buildVsaxCompareCsv(exportData.data);
  if (!csv) {
    setVsaxCompareNote("No rows available for CSV export.", true);
    return;
  }
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const filename = `vsax-price-compare-${exportData.safeGroup}-${exportData.stamp}.csv`;
  downloadVsaxCompareBlob(blob, filename);
  setVsaxCompareNote("CSV exported.");
}

function handleVsaxCompareExportExcel() {
  const exportData = getVsaxCompareExportData();
  if (!exportData) {
    setVsaxCompareNote("Run VSAx compare first, then export.", true);
    return;
  }
  const html = buildVsaxCompareExcelHtml(exportData.data);
  const blob = new Blob([html], {
    type: "application/vnd.ms-excel;charset=utf-8;",
  });
  const filename = `vsax-price-compare-${exportData.safeGroup}-${exportData.stamp}.xls`;
  downloadVsaxCompareBlob(blob, filename);
  setVsaxCompareNote("Excel file exported.");
}

function handleVsaxCompareExportPdf() {
  const exportData = getVsaxCompareExportData();
  if (!exportData) {
    setVsaxCompareNote("Run VSAx compare first, then export.", true);
    return;
  }
  const blob = buildVsaxComparePdf(exportData.data);
  const filename = `vsax-price-compare-${exportData.safeGroup}-${exportData.stamp}.pdf`;
  downloadVsaxCompareBlob(blob, filename);
  setVsaxCompareNote("PDF exported.");
}

function renderVsaxComparePanel(data = null) {
  const payload = data || vsaxCompareState.data;
  const availableGroups = Array.isArray(payload?.availableGroups)
    ? payload.availableGroups
    : [];

  if (vsaxCompareGroupSelect instanceof HTMLSelectElement) {
    const previous =
      String(vsaxCompareState.selectedGroupName || "").trim() ||
      String(vsaxCompareGroupSelect.value || "").trim();
    vsaxCompareGroupSelect.innerHTML = "";
    availableGroups.forEach((groupName) => {
      const option = document.createElement("option");
      option.value = groupName;
      option.textContent = groupName;
      vsaxCompareGroupSelect.appendChild(option);
    });
    const fallback = availableGroups[0] || "";
    const selected = availableGroups.includes(previous) ? previous : fallback;
    vsaxCompareState.selectedGroupName = selected;
    vsaxCompareGroupSelect.value = selected;
    vsaxCompareGroupSelect.disabled = vsaxCompareState.loading || !availableGroups.length;
  }

  if (vsaxCompareRegionSelect instanceof HTMLSelectElement) {
    vsaxCompareRegionSelect.value = vsaxCompareState.regionKey || "us-east";
    vsaxCompareRegionSelect.disabled = vsaxCompareState.loading;
  }
  if (vsaxComparePricingProviderSelect instanceof HTMLSelectElement) {
    vsaxComparePricingProviderSelect.value =
      vsaxCompareState.pricingProvider || "api";
    vsaxComparePricingProviderSelect.disabled = vsaxCompareState.loading;
  }
  if (vsaxCompareRefreshButton) {
    vsaxCompareRefreshButton.disabled = vsaxCompareState.loading;
    vsaxCompareRefreshButton.textContent = vsaxCompareState.loading
      ? "Loading..."
      : "Refresh";
  }
  const hasRows = Array.isArray(payload?.systems) && payload.systems.length > 0;
  if (vsaxCompareExportCsvButton) {
    vsaxCompareExportCsvButton.disabled = vsaxCompareState.loading || !hasRows;
  }
  if (vsaxCompareExportExcelButton) {
    vsaxCompareExportExcelButton.disabled = vsaxCompareState.loading || !hasRows;
  }
  if (vsaxCompareExportPdfButton) {
    vsaxCompareExportPdfButton.disabled = vsaxCompareState.loading || !hasRows;
  }

  renderVsaxCompareSummary(payload);
  renderVsaxCompareTable(payload);
}

async function refreshVsaxCompareData(options = {}) {
  const force = options.force === true;
  if (!force && activePanel !== "vsax-compare") {
    return;
  }
  if (vsaxCompareState.loading) {
    return;
  }

  const selectedGroup = String(vsaxCompareState.selectedGroupName || "").trim();
  const regionKey = String(vsaxCompareState.regionKey || "us-east").trim() || "us-east";
  const pricingProvider =
    vsaxCompareState.pricingProvider === "api" ? "api" : "retail";
  const diskTier =
    diskTierSelect instanceof HTMLSelectElement && diskTierSelect.value
      ? diskTierSelect.value
      : "max";

  const params = new URLSearchParams();
  if (selectedGroup) {
    params.set("groupName", selectedGroup);
  }
  params.set("regionKey", regionKey);
  params.set("pricingProvider", pricingProvider);
  params.set("diskTier", diskTier);

  vsaxCompareState.loading = true;
  renderVsaxComparePanel();
  setVsaxCompareNote("Loading VSAx systems and calculating AWS/Azure pricing...");
  try {
    const payload = await requestJson(`/api/vsax/price-compare?${params.toString()}`);
    vsaxCompareState.data = payload;
    vsaxCompareState.selectedGroupName = String(payload?.groupName || selectedGroup).trim();
    vsaxCompareState.regionKey = String(payload?.regionKey || regionKey).trim() || "us-east";
    vsaxCompareState.pricingProvider = pricingProvider;
    persistVsaxComparePreferences();
    renderVsaxComparePanel(payload);
    setVsaxCompareNote(
      `Updated ${Number(payload?.summary?.systemCount || 0)} system(s) from ${vsaxCompareState.selectedGroupName || "selected group"}.`
    );
  } catch (error) {
    setVsaxCompareNote(error?.message || "Could not load VSAx price compare data.", true);
  } finally {
    vsaxCompareState.loading = false;
    renderVsaxComparePanel();
  }
}

function isDefaultSqlRate(rate, edition) {
  const defaultRate = SQL_DEFAULTS[edition] ?? 0;
  if (!Number.isFinite(rate)) {
    return false;
  }
  return Math.abs(rate - defaultRate) <= DEFAULT_RATE_EPSILON;
}

function normalizeNetworkAddonFocus(value) {
  if (value === "firewall") {
    return "gateway";
  }
  if (value === "loadBalancer" || value === "gateway") {
    return value;
  }
  return "vpc";
}

function isApiSource(source) {
  if (typeof source !== "string" || !source) {
    return false;
  }
  return (
    source.includes("api") ||
    source.includes("price-list") ||
    source.includes("cloud-billing") ||
    source.includes("pricing-page")
  );
}

function isFallbackSource(source) {
  return (
    !source ||
    source === "fallback-default" ||
    source === "missing" ||
    source === "static" ||
    source === "unknown"
  );
}

function formatSourceDetail(source) {
  if (typeof source !== "string" || !source) {
    return "unknown";
  }
  if (source === "public-snapshot") {
    return "retail snapshot";
  }
  if (source === "manual") {
    return "manual input";
  }
  if (isApiSource(source)) {
    return "API";
  }
  if (isFallbackSource(source)) {
    return "fallback";
  }
  return source;
}

function summarizeItemSources(items) {
  if (!Array.isArray(items) || !items.length) {
    return "none";
  }
  const api = items.filter((item) => isApiSource(item?.source)).length;
  const fallback = items.filter((item) => isFallbackSource(item?.source)).length;
  if (api === items.length) {
    return "API";
  }
  if (fallback === items.length) {
    return "fallback";
  }
  return "mixed";
}

function formatRateNumber(value, max = 4) {
  if (!Number.isFinite(value)) {
    return "0";
  }
  return value.toFixed(max).replace(/\.?0+$/, "");
}

function getNetworkCardSourceLabel(input, provider) {
  const items = Array.isArray(provider?.networkAddons?.items)
    ? provider.networkAddons.items
    : [];
  const selected = [
    {
      addonKey: "vpc",
      flavor: input?.networkVpcFlavor,
      count: input?.networkVpcCount,
    },
    {
      addonKey: "gateway",
      flavor: input?.networkGatewayFlavor,
      count: input?.networkGatewayCount,
    },
    {
      addonKey: "loadBalancer",
      flavor: input?.networkLoadBalancerFlavor,
      count: input?.networkLoadBalancerCount,
    },
  ].filter(
    (entry) =>
      entry.flavor && entry.flavor !== "none" && Number.isFinite(entry.count) && entry.count > 0
  );
  const selectedUsesFallback = selected.some((entry) => {
    const item = items.find((networkItem) => networkItem.addonKey === entry.addonKey);
    return !isApiSource(item?.source);
  });
  const trafficUsesFallback =
    (input?.egressTb || 0) > 0 ||
    (input?.interVlanTb || 0) > 0 ||
    (input?.intraVlanTb || 0) > 0 ||
    (input?.interRegionTb || 0) > 0;
  return selectedUsesFallback || trafficUsesFallback ? "HARDCODED" : "API";
}

function getStorageCardSourceLabel(input, storageServices) {
  const sources = storageServices?.sources || {};
  const activeChecks = [
    { used: (input?.objectTb || 0) > 0, key: "object" },
    { used: (input?.fileTb || 0) > 0, key: "file" },
    { used: (input?.tableTb || 0) > 0, key: "table" },
    { used: (input?.queueTb || 0) > 0, key: "queue" },
    {
      used: Boolean(input?.drEnabled) && (input?.drDeltaTb || 0) > 0,
      key: "replication",
    },
  ];
  const fallbackUsed = activeChecks.some(
    (entry) => entry.used && isFallbackSource(sources[entry.key])
  );
  return fallbackUsed ? "HARDCODED" : "API";
}

function buildNetworkDisclaimerText() {
  const egress = NETWORK_HARDCODED_RATES.egress;
  const inter = NETWORK_HARDCODED_RATES.interVlan;
  const intra = NETWORK_HARDCODED_RATES.intraVlan;
  const interRegion = NETWORK_HARDCODED_RATES.interRegion;
  const addon = NETWORK_HARDCODED_RATES.addonData;
  return [
    "Source badge: API = live provider API/price list; HARDCODED = static or fallback values were used.",
    "Hardcoded rates ($/GB):",
    `Egress AWS ${formatRateNumber(egress.aws)}, Azure ${formatRateNumber(
      egress.azure
    )}, GCP ${formatRateNumber(egress.gcp)}.`,
    `Inter-VLAN AWS ${formatRateNumber(inter.aws)}, Azure ${formatRateNumber(
      inter.azure
    )}, GCP ${formatRateNumber(inter.gcp)}.`,
    `Intra-VLAN AWS ${formatRateNumber(intra.aws)}, Azure ${formatRateNumber(
      intra.azure
    )}, GCP ${formatRateNumber(intra.gcp)}.`,
    `Inter-region AWS ${formatRateNumber(interRegion.aws)}, Azure ${formatRateNumber(
      interRegion.azure
    )}, GCP ${formatRateNumber(interRegion.gcp)}.`,
    `Add-on data transfer: AWS gateway ${formatRateNumber(
      addon.aws.gateway
    )}, firewall ${formatRateNumber(addon.aws.firewall)}, LB ${formatRateNumber(
      addon.aws.loadBalancer
    )}; Azure gateway ${formatRateNumber(
      addon.azure.gateway
    )}, firewall ${formatRateNumber(addon.azure.firewall)}, LB ${formatRateNumber(
      addon.azure.loadBalancer
    )}; GCP gateway ${formatRateNumber(
      addon.gcp.gateway
    )}, firewall ${formatRateNumber(addon.gcp.firewall)}, LB ${formatRateNumber(
      addon.gcp.loadBalancer
    )}.`,
  ].join(" ");
}

function buildStorageDisclaimerText(data) {
  const fallbackUsedByProvider = [];
  const providerMap = [
    { key: "aws", label: "AWS" },
    { key: "azure", label: "Azure" },
    { key: "gcp", label: "GCP" },
  ];
  providerMap.forEach(({ key, label }) => {
    const services = data?.[key]?.storageServices || {};
    const sources = services.sources || {};
    const input = data?.input || {};
    const prefix = key;
    const used = [];
    if ((input[`${prefix}StorageObjectTb`] || 0) > 0 && isFallbackSource(sources.object)) {
      used.push("object");
    }
    if ((input[`${prefix}StorageFileTb`] || 0) > 0 && isFallbackSource(sources.file)) {
      used.push("file");
    }
    if ((input[`${prefix}StorageTableTb`] || 0) > 0 && isFallbackSource(sources.table)) {
      used.push("table");
    }
    if ((input[`${prefix}StorageQueueTb`] || 0) > 0 && isFallbackSource(sources.queue)) {
      used.push("queue");
    }
    if (
      input[`${prefix}StorageDrEnabled`] &&
      (input[`${prefix}StorageDrDeltaTb`] || 0) > 0 &&
      isFallbackSource(sources.replication)
    ) {
      used.push("replication");
    }
    if (used.length) {
      fallbackUsedByProvider.push(`${label}: ${used.join(", ")}`);
    }
  });
  const defaults = STORAGE_HARDCODED_RATES;
  const fallbackSummary = fallbackUsedByProvider.length
    ? `Fallback used for ${fallbackUsedByProvider.join(" | ")}.`
    : "No active fallback components detected in current inputs.";
  return [
    "Source badge: API = live provider API/price list; HARDCODED = fallback-default values were used.",
    fallbackSummary,
    "Hardcoded fallback rates ($/GB-month):",
    `AWS object ${formatRateNumber(defaults.aws.object)}, file ${formatRateNumber(
      defaults.aws.file
    )}, table ${formatRateNumber(defaults.aws.table)}, queue ${formatRateNumber(
      defaults.aws.queue
    )}, replication ${formatRateNumber(defaults.aws.replication)}.`,
    `Azure object ${formatRateNumber(defaults.azure.object)}, file ${formatRateNumber(
      defaults.azure.file
    )}, table ${formatRateNumber(defaults.azure.table)}, queue ${formatRateNumber(
      defaults.azure.queue
    )}, replication ${formatRateNumber(defaults.azure.replication)}.`,
    `GCP object ${formatRateNumber(defaults.gcp.object)}, file ${formatRateNumber(
      defaults.gcp.file
    )}, table ${formatRateNumber(defaults.gcp.table)}, queue ${formatRateNumber(
      defaults.gcp.queue
    )}, replication ${formatRateNumber(defaults.gcp.replication)}.`,
    "Conversion note: 1 TB = 1024 GB.",
  ].join(" ");
}

function updateDisclaimerText(data) {
  if (!disclaimer) {
    return;
  }
  if (currentMode === "network") {
    disclaimer.textContent = buildNetworkDisclaimerText();
    return;
  }
  if (currentMode === "storage") {
    disclaimer.textContent = buildStorageDisclaimerText(data);
    return;
  }
  disclaimer.textContent = defaultDisclaimerText;
}

function updateNetworkAddonFocusUi() {
  const focus = normalizeNetworkAddonFocus(
    networkAddonFocusInput?.value
  );
  networkAddonTabButtons.forEach((button) => {
    button.classList.toggle(
      "active",
      button.dataset.networkFocus === focus
    );
  });
  const isNetworkMode = currentMode === "network";
  networkAddonGroups.forEach((group) => {
    if (!(group instanceof HTMLElement)) {
      return;
    }
    const groupFocus = group.dataset.networkAddon;
    const hideGroup = isNetworkMode && groupFocus !== focus;
    group.classList.toggle("is-hidden", hideGroup);
  });
}

function setNetworkAddonFocus(focus, options = {}) {
  if (!networkAddonFocusInput) {
    return;
  }
  networkAddonFocusInput.value = normalizeNetworkAddonFocus(focus);
  updateNetworkAddonFocusUi();
  currentNetworkResult = networkAddonFocusInput.value;
  networkResultTabs.forEach((button) => {
    button.classList.toggle(
      "active",
      button.dataset.networkResult === currentNetworkResult
    );
  });
  if (!options.silent && currentMode === "network") {
    handleCompare();
  }
}

function setNetworkFocusView(showInsight) {
  if (networkInsightPanel) {
    networkInsightPanel.classList.toggle("is-hidden", !showInsight);
  }
}

function setStorageFocusView(showInsight) {
  if (storageInsightPanel) {
    storageInsightPanel.classList.toggle("is-hidden", !showInsight);
  }
}

function setNetworkResultTab(tab, options = {}) {
  const nextTab =
    tab === "insight" ? "insight" : normalizeNetworkAddonFocus(tab);
  currentNetworkResult = nextTab;
  networkResultTabs.forEach((button) => {
    button.classList.toggle(
      "active",
      button.dataset.networkResult === currentNetworkResult
    );
  });
  setResultsTab("pricing", { silent: true });
  if (currentNetworkResult !== "insight") {
    setNetworkAddonFocus(currentNetworkResult, { silent: true });
  }
  setNetworkFocusView(currentNetworkResult === "insight");
  if (currentNetworkResult === "insight") {
    renderFocusInsight(lastPricing, "network");
  } else if (lastPricing && currentMode === "network") {
    renderNetworkFocusTable(lastPricing);
  }
  if (!options.silent) {
    // Keep focus-tab navigation responsive: switching to Insight should
    // not force a fresh compare that can reset focus state.
    if (currentNetworkResult === "insight") {
      if (!lastPricing) {
        handleCompare();
      }
      return;
    }
    handleCompare();
  }
}

function setStorageResultTab(tab, options = {}) {
  currentStorageResult =
    tab === "insight" ? "insight" : tab === "performance" ? "performance" : "object";
  storageResultTabs.forEach((button) => {
    button.classList.toggle(
      "active",
      button.dataset.storageResult === currentStorageResult
    );
  });
  setResultsTab("pricing", { silent: true });
  setStorageFocusView(currentStorageResult === "insight");
  if (currentStorageResult === "insight") {
    renderFocusInsight(lastPricing, "storage");
  } else if (lastPricing && currentMode === "storage") {
    renderStorageFocusTable(lastPricing);
  }
  if (!options.silent) {
    // Keep focus-tab navigation responsive: switching to Insight should
    // not force a fresh compare that can reset focus state.
    if (currentStorageResult === "insight") {
      if (!lastPricing) {
        handleCompare();
      }
      return;
    }
    handleCompare();
  }
}

function setMode(mode) {
  const wasK8s = currentMode === "k8s";
  const nextMode =
    mode === "k8s"
      ? "k8s"
      : mode === "network"
      ? "network"
      : mode === "storage"
      ? "storage"
      : mode === "saved"
      ? "saved"
      : "vm";
  const leavingVm = currentMode === "vm" && nextMode !== "vm";
  if (leavingVm) {
    sqlState = {
      edition: sqlEditionSelect.value,
      rate: sqlRateInput.value,
    };
  }
  currentMode = nextMode;
  modeInput.value = currentMode === "k8s" ? "k8s" : "vm";
  if (pricingFocusInput) {
    pricingFocusInput.value =
      currentMode === "network"
        ? "network"
        : currentMode === "storage"
        ? "storage"
        : "all";
  }
  const copy = MODE_COPY[currentMode] || MODE_COPY.vm;
  formTitle.textContent = copy.formTitle;
  formSubtitle.textContent = copy.formSubtitle;
  cpuLabel.textContent = copy.cpuLabel;
  vmCountLabel.textContent = copy.countLabel;
  egressLabel.textContent = copy.egressLabel;
  awsTitle.textContent = copy.awsTitle;
  azureTitle.textContent = copy.azureTitle;
  gcpTitle.textContent = copy.gcpTitle;
  updateResultsHeading();

  const isK8s = currentMode === "k8s";
  const isNetwork = currentMode === "network";
  const isStorage = currentMode === "storage";
  const isPublicOnlyMode = isNetwork || isStorage;
  togglePricingFocusClass("focus-network", isNetwork);
  togglePricingFocusClass("focus-storage", isStorage);
  const hideWorkload = isK8s || isNetwork || isStorage;
  const hideSql = isK8s || isNetwork || isStorage;
  workloadField.classList.toggle("is-hidden", hideWorkload);
  sqlEditionField.classList.toggle("is-hidden", hideSql);
  sqlRateField.classList.toggle("is-hidden", hideSql);
  sqlEditionSelect.disabled = hideSql;
  sqlRateInput.disabled = hideSql;
  if (isK8s) {
    sqlEditionSelect.value = "none";
    sqlRateInput.value = "0";
    osDiskLabel.textContent = `OS disk (GB, min ${K8S_OS_DISK_MIN_GB})`;
    osDiskInput.min = K8S_OS_DISK_MIN_GB.toString();
    const currentOs = Number.parseFloat(osDiskInput.value);
    if (!wasK8s || !Number.isFinite(currentOs) || currentOs < K8S_OS_DISK_MIN_GB) {
      osDiskInput.value = K8S_OS_DISK_MIN_GB.toString();
    }
    vmCountInput.min = K8S_MIN_NODE_COUNT.toString();
    const currentCount = Number.parseInt(vmCountInput.value, 10);
    if (!Number.isFinite(currentCount) || currentCount < K8S_MIN_NODE_COUNT) {
      vmCountInput.value = K8S_MIN_NODE_COUNT.toString();
    }
    dataDiskLabel.textContent = "Shared storage (TB)";
  } else {
    if (hideSql) {
      sqlEditionSelect.value = "none";
      sqlRateInput.value = "0";
    } else {
      sqlEditionSelect.value = sqlState.edition || "none";
      sqlRateInput.value = sqlState.rate || "0";
    }
    osDiskLabel.textContent = "OS disk (GB)";
    osDiskInput.min = "0";
    vmCountInput.min = "1";
    const currentCount = Number.parseInt(vmCountInput.value, 10);
    if (!Number.isFinite(currentCount) || currentCount < 1) {
      vmCountInput.value = "1";
    }
    dataDiskLabel.textContent = isStorage
      ? "Shared storage (TB)"
      : "Data disk (TB)";
    if (isStorage) {
      osDiskInput.value = "0";
    }
    if (isNetwork && dataDiskInput) {
      osDiskInput.value = "0";
      dataDiskInput.value = "0";
    }
  }

  if (cpuField) {
    cpuField.classList.toggle("is-hidden", isNetwork || isStorage);
  }
  if (vmCountField) {
    vmCountField.classList.toggle("is-hidden", isNetwork || isStorage);
  }
  if (diskTierField) {
    diskTierField.classList.toggle("is-hidden", isNetwork || isStorage);
  }
  if (osDiskField) {
    osDiskField.classList.toggle("is-hidden", isNetwork || isStorage);
  }
  if (dataDiskField) {
    dataDiskField.classList.toggle("is-hidden", isNetwork || isStorage);
  }
  if (backupField) {
    backupField.classList.toggle("is-hidden", isNetwork || isStorage);
  }
  if (egressField) {
    egressField.classList.toggle("is-hidden", isStorage);
  }
  if (interVlanField) {
    interVlanField.classList.toggle("is-hidden", !isNetwork);
  }
  if (intraVlanField) {
    intraVlanField.classList.toggle("is-hidden", !isNetwork);
  }
  if (interRegionField) {
    interRegionField.classList.toggle("is-hidden", !isNetwork);
  }
  if (storageIopsField) {
    storageIopsField.classList.toggle("is-hidden", !isStorage);
  }
  if (storageThroughputField) {
    storageThroughputField.classList.toggle("is-hidden", !isStorage);
  }
  if (storageRequestField) {
    storageRequestField.classList.toggle("is-hidden", !isStorage);
  }
  if (storageOperationField) {
    storageOperationField.classList.toggle("is-hidden", !isStorage);
  }
  if (drField) {
    drField.classList.toggle("is-hidden", isNetwork || isStorage);
  }
  if (hoursField) {
    hoursField.classList.toggle("is-hidden", isStorage);
  }
  if (networkSection) {
    networkSection.classList.toggle("is-hidden", isStorage || isNetwork);
  }
  if (networkFields) {
    networkFields.classList.toggle("is-hidden", isStorage || isNetwork);
  }
  if (networkAddonTabs) {
    networkAddonTabs.classList.add("is-hidden");
  }
  if (storageRateSection) {
    storageRateSection.classList.add("is-hidden");
  }
  if (storageRateFields) {
    storageRateFields.classList.add("is-hidden");
  }
  if (isStorage && backupEnabledInput) {
    backupEnabledInput.checked = false;
  }
  if ((isStorage || isNetwork) && drPercentInput) {
    drPercentInput.value = "0";
  }
  if (isStorage && egressInput) {
    egressInput.value = "0";
  }
  if (isNetwork && backupEnabledInput) {
    backupEnabledInput.checked = false;
  }
  if (networkAddonFocusInput) {
    if (isNetwork) {
      setNetworkAddonFocus(networkAddonFocusInput.value || "vpc", {
        silent: true,
      });
    } else {
      updateNetworkAddonFocusUi();
    }
  }
  if (isNetwork) {
    setNetworkFocusView(currentNetworkResult === "insight");
  }
  if (isStorage) {
    setStorageFocusView(currentStorageResult === "insight");
  }
  networkResultTabs.forEach((button) => {
    const key = button.dataset.networkResult;
    button.classList.toggle(
      "active",
      key === currentNetworkResult
    );
  });
  storageResultTabs.forEach((button) => {
    const key = button.dataset.storageResult;
    button.classList.toggle(
      "active",
      key === currentStorageResult
    );
  });
  if (privateViewTab) {
    privateViewTab.classList.toggle("is-hidden", isPublicOnlyMode);
  }
  if (privateCompareContainer) {
    privateCompareContainer.classList.toggle("is-hidden", isPublicOnlyMode);
  }
  if (isPublicOnlyMode && currentView === "private") {
    currentView = "compare";
  }
  if (isPublicOnlyMode) {
    setView("compare");
  }
  if (pricingProviderSelect) {
    if (isPublicOnlyMode) {
      pricingProviderSelect.value = "api";
      pricingProviderSelect.disabled = true;
    } else {
      pricingProviderSelect.disabled = false;
    }
  }
  if (disclaimer) {
    disclaimer.classList.remove("is-hidden");
  }

  if (sizeOptions) {
    updateCpuOptions();
    updateInstanceOptions();
  }
  updateViewTabsVisibility();
}

function updateResultsHeading() {
  if (!resultsTitle || !resultsSubtitle) {
    return;
  }
  if (activePanel === "private") {
    resultsTitle.textContent = "Private cloud profiles";
    resultsSubtitle.textContent =
      "Create and save private provider profiles for VM comparisons.";
    return;
  }
  if (activePanel === "scenarios") {
    resultsTitle.textContent = "Private/Public Cloud";
    resultsSubtitle.textContent =
      "Compare saved public scenarios against private cloud providers.";
    return;
  }
  if (activePanel === "saved") {
    resultsTitle.textContent = RESULTS_TAB_COPY.saved.title;
    resultsSubtitle.textContent = RESULTS_TAB_COPY.saved.subtitle;
    return;
  }
  if (activePanel === "vsax-compare") {
    resultsTitle.textContent = "Price Compare VSAx";
    resultsSubtitle.textContent =
      "Select a VSAx group to compare Windows system capacities against AWS and Azure monthly pricing.";
    return;
  }
  if (activePanel === "billing") {
    resultsTitle.textContent = "Billing Import";
    resultsSubtitle.textContent =
      "Import provider billing CSVs and visualize cost allocation by service.";
    return;
  }
  if (activePanel === "data-transfer") {
    resultsTitle.textContent = "User Data";
    resultsSubtitle.textContent =
      "Export/import your full workspace data for guest and signed-in use.";
    return;
  }
  if (activePanel === "admin") {
    resultsTitle.textContent = "Admin";
    resultsSubtitle.textContent =
      "Manage users: add/remove accounts and update passwords.";
    return;
  }
  if (currentResultsTab === "saved") {
    if (currentMode === "network") {
      resultsTitle.textContent = "Network Saved Compare";
      resultsSubtitle.textContent =
        "Run saved network scenarios in a multi-provider dashboard.";
      return;
    }
    if (currentMode === "storage") {
      resultsTitle.textContent = "Storage Saved Compare";
      resultsSubtitle.textContent =
        "Run saved storage scenarios in a multi-provider dashboard.";
      return;
    }
    resultsTitle.textContent = RESULTS_TAB_COPY.saved.title;
    resultsSubtitle.textContent = RESULTS_TAB_COPY.saved.subtitle;
    return;
  }
  if (currentResultsTab === "insight") {
    if (currentMode === "network") {
      resultsTitle.textContent = "Network Insight";
      resultsSubtitle.textContent =
        "Breakdown of networking costs: VPC/VNet, VPC/VPN gateway, load balancer, VLAN transfer, and egress.";
      return;
    }
    if (currentMode === "storage") {
      resultsTitle.textContent = "Storage Insight";
      resultsSubtitle.textContent =
        "Breakdown of storage service costs and DR replication by provider.";
      return;
    }
    resultsTitle.textContent = RESULTS_TAB_COPY.insight.title;
    resultsSubtitle.textContent = RESULTS_TAB_COPY.insight.subtitle;
    return;
  }
  if (currentResultsTab === "commit") {
    resultsTitle.textContent = RESULTS_TAB_COPY.commit.title;
    resultsSubtitle.textContent = RESULTS_TAB_COPY.commit.subtitle;
    return;
  }
  const copy = MODE_COPY[currentMode] || MODE_COPY.vm;
  resultsTitle.textContent = copy.resultsTitle;
  resultsSubtitle.textContent = copy.resultsSubtitle;
}

function updateResultsTabsVisibility() {
  if (!resultsTabs) {
    return;
  }
  const isFocusMode = currentMode === "network" || currentMode === "storage";
  const showTabs =
    activePanel !== "private" &&
    activePanel !== "scenarios" &&
    activePanel !== "saved" &&
    activePanel !== "vsax-compare" &&
    activePanel !== "billing" &&
    activePanel !== "data-transfer" &&
    activePanel !== "admin";
  resultsTabs.classList.toggle("is-hidden", !showTabs || isFocusMode);
  if (!showTabs) {
    currentResultsTab = "pricing";
    if (pricingPanel) {
      pricingPanel.classList.add("is-hidden");
    }
    if (savedComparePanel) {
      savedComparePanel.classList.toggle("is-hidden", activePanel !== "saved");
    }
    if (insightPanel) {
      insightPanel.classList.add("is-hidden");
    }
    if (commitPanel) {
      commitPanel.classList.add("is-hidden");
    }
    updateResultsHeading();
  }
}

function setResultsTab(tab, options = {}) {
  const isFocusMode = currentMode === "network" || currentMode === "storage";
  const nextTab =
    !isFocusMode &&
    (tab === "saved" || tab === "insight" || tab === "commit")
      ? tab
      : "pricing";
  currentResultsTab = nextTab;
  resultsTabButtons.forEach((button) => {
    button.classList.toggle(
      "active",
      button.dataset.results === nextTab
    );
  });
  if (pricingPanel) {
    pricingPanel.classList.toggle("is-hidden", nextTab !== "pricing");
  }
  if (savedComparePanel) {
    savedComparePanel.classList.toggle("is-hidden", nextTab !== "saved");
  }
  if (insightPanel) {
    insightPanel.classList.toggle("is-hidden", nextTab !== "insight");
  }
  if (commitPanel) {
    commitPanel.classList.toggle("is-hidden", nextTab !== "commit");
  }
  if (!isFocusMode) {
    networkResultTabs.forEach((button) => {
      const key = button.dataset.networkResult;
      button.classList.toggle(
        "active",
        (nextTab === "insight" && key === "insight") ||
          (nextTab === "pricing" && key === currentNetworkResult)
      );
    });
    storageResultTabs.forEach((button) => {
      const key = button.dataset.storageResult;
      button.classList.toggle(
        "active",
        (nextTab === "insight" && key === "insight") ||
          (nextTab === "pricing" && key === currentStorageResult)
      );
    });
  }
  updateResultsHeading();
  updateViewTabsVisibility();
  updateVendorSubtabs();
  if (nextTab === "pricing") {
    setView(currentView);
  }
  if (options.silent) {
    return;
  }
  if (nextTab === "saved") {
    refreshSavedCompare();
  }
  if (nextTab === "insight") {
    renderInsight(lastPricing);
  }
  if (nextTab === "commit") {
    renderCommit(lastPricing);
  }
}

function readSavedPanel() {
  return readPricingStorageValue(PRICING_PANEL_STORAGE_KEY);
}

function savePanel(panel) {
  writePricingStorageValue(PRICING_PANEL_STORAGE_KEY, panel);
}

function setPanel(panel) {
  let nextPanel =
    panel === "private"
      ? "private"
      : panel === "scenarios"
      ? "scenarios"
      : panel === "billing"
      ? "billing"
      : panel === "vsax-compare"
      ? "vsax-compare"
      : panel === "data-transfer"
      ? "data-transfer"
      : panel === "admin"
      ? "admin"
      : panel === "k8s"
      ? "k8s"
      : panel === "network"
      ? "network"
      : panel === "storage"
      ? "storage"
      : panel === "saved"
      ? "saved"
      : "vm";
  if (nextPanel === "admin" && (PRICE_EMBEDDED || !isAdminUser)) {
    nextPanel = "vm";
  }
  if (nextPanel === "billing") {
    nextPanel = "vm";
  }
  activePanel = nextPanel;
  savePanel(nextPanel);
  modeTabs.forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.mode === nextPanel);
  });
  if (
    nextPanel === "private" ||
    nextPanel === "scenarios" ||
    nextPanel === "saved" ||
    nextPanel === "vsax-compare" ||
    nextPanel === "billing" ||
    nextPanel === "data-transfer" ||
    nextPanel === "admin"
  ) {
    if (cloudPanel) {
      cloudPanel.classList.add("is-hidden");
    }
    if (privatePanel) {
      privatePanel.classList.toggle("is-hidden", nextPanel !== "private");
    }
    if (scenariosPanel) {
      scenariosPanel.classList.toggle("is-hidden", nextPanel !== "scenarios");
    }
    if (savedComparePanel) {
      savedComparePanel.classList.toggle("is-hidden", nextPanel !== "saved");
    }
    if (vsaxComparePanel) {
      vsaxComparePanel.classList.toggle(
        "is-hidden",
        nextPanel !== "vsax-compare"
      );
    }
    if (billingPanel) {
      billingPanel.classList.toggle("is-hidden", nextPanel !== "billing");
    }
    if (dataTransferPanel) {
      dataTransferPanel.classList.toggle(
        "is-hidden",
        nextPanel !== "data-transfer"
      );
    }
    if (adminPanel) {
      adminPanel.classList.toggle("is-hidden", nextPanel !== "admin");
    }
    if (formCard) {
      formCard.classList.add("is-hidden");
    }
    if (layout) {
      layout.classList.add("single");
    }
    if (pricingPanel) {
      pricingPanel.classList.add("is-hidden");
    }
    if (insightPanel) {
      insightPanel.classList.add("is-hidden");
    }
    if (commitPanel) {
      commitPanel.classList.add("is-hidden");
    }
    updateResultsTabsVisibility();
    updateResultsHeading();
    updateViewTabsVisibility();
    if (nextPanel === "saved") {
      refreshSavedCompare();
    }
    if (nextPanel === "vsax-compare") {
      refreshVsaxCompareData({ force: true });
    }
    if (nextPanel === "billing") {
      setBillingProvider(currentBillingProvider);
    }
    if (nextPanel === "data-transfer") {
      renderDataTransferScope(authSession?.user || null);
    }
    if (nextPanel === "admin") {
      loadAdminUsers();
    }
    return;
  }
  if (cloudPanel) {
    cloudPanel.classList.remove("is-hidden");
  }
  if (privatePanel) {
    privatePanel.classList.add("is-hidden");
  }
  if (scenariosPanel) {
    scenariosPanel.classList.add("is-hidden");
  }
  if (savedComparePanel) {
    savedComparePanel.classList.add("is-hidden");
  }
  if (vsaxComparePanel) {
    vsaxComparePanel.classList.add("is-hidden");
  }
  if (billingPanel) {
    billingPanel.classList.add("is-hidden");
  }
  if (dataTransferPanel) {
    dataTransferPanel.classList.add("is-hidden");
  }
  if (adminPanel) {
    adminPanel.classList.add("is-hidden");
  }
  if (formCard) {
    formCard.classList.remove("is-hidden");
  }
  if (layout) {
    layout.classList.remove("single");
  }
  setMode(nextPanel);
  if (nextPanel === "network" || nextPanel === "storage") {
    currentResultsTab = "pricing";
  }
  updateResultsTabsVisibility();
  setResultsTab(currentResultsTab, { silent: true });
  setView(currentView);
}

function updateViewTabsVisibility() {
  if (!viewTabs) {
    return;
  }
  const showTabs =
    activePanel !== "private" &&
    activePanel !== "scenarios" &&
    activePanel !== "saved" &&
    activePanel !== "vsax-compare" &&
    activePanel !== "billing" &&
    activePanel !== "data-transfer" &&
    activePanel !== "admin" &&
    currentResultsTab === "pricing" &&
    currentMode !== "network" &&
    currentMode !== "storage";
  viewTabs.classList.toggle("is-hidden", !showTabs);
  if (
    !showTabs &&
    (activePanel === "private" ||
      activePanel === "scenarios" ||
      activePanel === "saved" ||
      activePanel === "vsax-compare" ||
      activePanel === "billing" ||
      activePanel === "data-transfer" ||
      activePanel === "admin") &&
    currentView !== "compare"
  ) {
    currentView = "compare";
    setView("compare");
  }
}

function setView(view) {
  const isFocusMode = currentMode === "network" || currentMode === "storage";
  if (isFocusMode) {
    const showNetworkPanel = currentMode === "network";
    const showStoragePanel = currentMode === "storage";
    currentView = "compare";
    viewTabButtons.forEach((button) => {
      button.classList.toggle("active", button.dataset.view === "compare");
    });
    if (compareGrid) {
      compareGrid.classList.add("is-hidden");
    }
    if (vendorGrid) {
      vendorGrid.classList.add("is-hidden");
    }
    if (delta) {
      delta.classList.add("is-hidden");
    }
    if (scenarioDelta) {
      scenarioDelta.classList.add("is-hidden");
    }
    if (networkFocusPanel) {
      networkFocusPanel.classList.toggle("is-hidden", !showNetworkPanel);
    }
    if (storageFocusPanel) {
      storageFocusPanel.classList.toggle("is-hidden", !showStoragePanel);
    }
    if (showNetworkPanel) {
      setNetworkFocusView(currentNetworkResult === "insight");
    }
    if (showStoragePanel) {
      setStorageFocusView(currentStorageResult === "insight");
    }
    updateVendorSubtabs();
    return;
  }
  if (networkFocusPanel) {
    networkFocusPanel.classList.add("is-hidden");
  }
  if (storageFocusPanel) {
    storageFocusPanel.classList.add("is-hidden");
  }
  const privateViewBlocked =
    currentMode === "network" || currentMode === "storage";
  const nextView =
    view === "aws" || view === "azure" || view === "gcp"
      ? view
      : view === "private" && !privateViewBlocked
      ? view
      : "compare";
  currentView = nextView;
  viewTabButtons.forEach((button) => {
    button.classList.toggle("active", button.dataset.view === nextView);
  });
  const showAll = nextView === "compare";
  if (awsCard) {
    awsCard.classList.toggle("is-hidden", !(showAll || nextView === "aws"));
  }
  if (azureCard) {
    azureCard.classList.toggle("is-hidden", !(showAll || nextView === "azure"));
  }
  if (gcpCard) {
    gcpCard.classList.toggle("is-hidden", !(showAll || nextView === "gcp"));
  }
  if (compareGrid) {
    compareGrid.classList.toggle("single", !showAll);
    compareGrid.classList.toggle("is-hidden", !showAll);
  }
  if (vendorGrid) {
    vendorGrid.classList.toggle("is-hidden", showAll);
  }
  delta.classList.toggle("is-hidden", !showAll);
  if (scenarioDelta) {
    scenarioDelta.classList.toggle("is-hidden", !showAll);
  }
  updateVendorSubtabs();
}

function updateVendorSubtabs() {
  if (!vendorSubtabs || !vendorRegionPanel) {
    return;
  }
  const isProviderView =
    currentView === "aws" || currentView === "azure" || currentView === "gcp";
  const showSubtabs = currentResultsTab === "pricing" && isProviderView;
  vendorSubtabs.classList.toggle("is-hidden", !showSubtabs);
  if (!showSubtabs) {
    vendorRegionPanel.classList.add("is-hidden");
    return;
  }
  setVendorSubtab(currentVendorView, { silent: true });
}

function setVendorSubtab(view, options = {}) {
  const nextView = view === "regions" ? "regions" : "options";
  currentVendorView = nextView;
  vendorSubtabButtons.forEach((button) => {
    button.classList.toggle(
      "active",
      button.dataset.vendorView === nextView
    );
  });
  const isProviderView =
    currentView === "aws" || currentView === "azure" || currentView === "gcp";
  if (!isProviderView || currentResultsTab !== "pricing") {
    return;
  }
  const showOptions = nextView === "options";
  if (vendorGrid) {
    vendorGrid.classList.toggle("is-hidden", !showOptions);
  }
  if (vendorRegionPanel) {
    vendorRegionPanel.classList.toggle("is-hidden", showOptions);
  }
  if (!options.silent && nextView === "regions") {
    runRegionCompare();
  }
}

function getProviderLabelForMode(providerKey, mode) {
  const copy = MODE_COPY[mode] || MODE_COPY.vm;
  if (providerKey === "aws") {
    return copy.awsTitle;
  }
  if (providerKey === "azure") {
    return copy.azureTitle;
  }
  if (providerKey === "gcp") {
    return copy.gcpTitle;
  }
  return copy.privateTitle || "Private";
}

function getProviderLabel(providerKey) {
  return getProviderLabelForMode(providerKey, currentMode);
}

function updateTier(target, tierData, options = {}) {
  if (!tierData || !Number.isFinite(tierData.hourlyRate) || !tierData.totals) {
    target.total.textContent = "N/A";
    target.rate.textContent = "Rate unavailable";
    return;
  }
  target.total.textContent = formatMoney(tierData.totals.total);
  if (options.showMonthlyRate) {
    target.rate.textContent = `Compute ${formatMonthly(
      tierData.totals.computeMonthly
    )}`;
  } else {
    target.rate.textContent = formatRate(tierData.hourlyRate);
  }
}

function formatSavings(label, onDemandTotal, reservedTotal) {
  if (!Number.isFinite(onDemandTotal) || !Number.isFinite(reservedTotal)) {
    return `${label} N/A`;
  }
  const diff = onDemandTotal - reservedTotal;
  if (diff === 0) {
    return `${label} no change`;
  }
  const verb = diff > 0 ? "saves" : "adds";
  return `${label} ${verb} ${formatMoney(Math.abs(diff))}/mo`;
}

function setStatus(element, status, message) {
  element.textContent = status;
  element.classList.toggle("error", status === "error");
  element.title = message || "";
}

function syncInstanceSelect(select, instance) {
  if (!(select instanceof HTMLSelectElement)) {
    select.textContent = instance?.type || "-";
    return;
  }
  if (!instance?.type) {
    if (select.options.length) {
      select.selectedIndex = 0;
    }
    return;
  }
  const value = instance.type;
  const existing = Array.from(select.options).find(
    (option) => option.value === value
  );
  if (!existing) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = `${value} — ${instance.vcpu} vCPU / ${instance.memory} GB`;
    select.appendChild(option);
  }
  select.value = value;
}

function buildProviderFieldsFromCard(card) {
  const instanceSelect = card.querySelector("[data-field='instanceSelect']");
  const instance = instanceSelect || card.querySelector("[data-field='instance']");
  return {
    family: card.querySelector("[data-field='family']"),
    instance,
    shape: card.querySelector("[data-field='shape']"),
    region: card.querySelector("[data-field='region']"),
    hourly: card.querySelector("[data-field='hourly']"),
    network: card.querySelector("[data-field='network']"),
    status: card.querySelector("[data-field='status']"),
    tiers: {
      onDemand: {
        total: card.querySelector("[data-field='od-total']"),
        rate: card.querySelector("[data-field='od-rate']"),
      },
      year1: {
        total: card.querySelector("[data-field='1y-total']"),
        rate: card.querySelector("[data-field='1y-rate']"),
      },
      year3: {
        total: card.querySelector("[data-field='3y-total']"),
        rate: card.querySelector("[data-field='3y-rate']"),
      },
    },
    savings: card.querySelector("[data-field='savings']"),
    breakdown: card.querySelector("[data-field='breakdown']"),
    note: card.querySelector("[data-field='note']"),
  };
}

function buildProviderSourceConfidence(provider, options = {}, totals = null) {
  const pricingFocus =
    options.pricingFocus === "network"
      ? "network"
      : options.pricingFocus === "storage"
      ? "storage"
      : "all";
  const computeSource = formatSourceDetail(
    provider?.pricingTiers?.onDemand?.source || provider?.source
  );
  const networkSource = summarizeItemSources(provider?.networkAddons?.items || []);
  const egressUsesFallback =
    Number.isFinite(totals?.egressMonthly) && totals.egressMonthly > 0;
  if (pricingFocus === "network") {
    return `Source confidence: compute excluded, network add-ons ${networkSource}, traffic transfer ${egressUsesFallback ? "fallback" : "none"}.`;
  }
  if (pricingFocus === "storage") {
    const sourceValues = Object.values(provider?.storageServices?.sources || {});
    const storageSource = sourceValues.length
      ? summarizeItemSources(sourceValues.map((value) => ({ source: value })))
      : "none selected";
    return `Source confidence: compute excluded, storage services ${storageSource}.`;
  }
  const reserved1Source = formatSourceDetail(provider?.pricingTiers?.reserved1yr?.source);
  const reserved3Source = formatSourceDetail(provider?.pricingTiers?.reserved3yr?.source);
  const storageSource =
    options.mode === "k8s"
      ? formatSourceDetail(
          options.sharedStorageSources?.[options.providerKey] || "fallback-default"
        )
      : "disk-tier model";
  return `Source confidence: compute ${computeSource}, 1-year ${reserved1Source}, 3-year ${reserved3Source}, network add-ons ${networkSource}, traffic transfer ${egressUsesFallback ? "fallback" : "none"}, storage ${storageSource}.`;
}

function updateProvider(target, provider, region, options = {}) {
  const pricingFocus =
    options.pricingFocus === "network"
      ? "network"
      : options.pricingFocus === "storage"
      ? "storage"
      : "all";
  const statusOverride =
    pricingFocus === "all"
      ? null
      : options.pricingProvider === "api"
      ? "api"
      : "retail";
  target.family.textContent =
    pricingFocus === "network"
      ? "Networking"
      : pricingFocus === "storage"
      ? "Shared storage"
      : provider.family || "-";
  if (target.instance) {
    syncInstanceSelect(target.instance, provider.instance);
  }
  const vcpu = provider.instance?.vcpu;
  const memory = provider.instance?.memory;
  if (Number.isFinite(vcpu) && Number.isFinite(memory)) {
    target.shape.textContent = `${vcpu} vCPU / ${memory} GB`;
  } else if (Number.isFinite(vcpu)) {
    target.shape.textContent = `${vcpu} vCPU`;
  } else {
    target.shape.textContent = "-";
  }
  target.region.textContent = region?.location || "-";
  const networkGbps = provider.instance?.networkGbps;
  if (pricingFocus === "network") {
    const selectedAddons = Array.isArray(provider.networkAddons?.items)
      ? provider.networkAddons.items
      : [];
    target.network.textContent = selectedAddons.length
      ? selectedAddons.map((item) => item.label).join(" + ")
      : "None selected";
  } else if (pricingFocus === "storage") {
    target.network.textContent = "-";
  } else if (Number.isFinite(networkGbps)) {
    target.network.textContent = `>= ${networkGbps} Gbps`;
  } else if (provider.instance?.networkLabel) {
    target.network.textContent = provider.instance.networkLabel;
  } else {
    target.network.textContent = "-";
  }

  setStatus(
    target.status,
    statusOverride || provider.status,
    pricingFocus === "all" ? provider.message : null
  );

  const onDemandTier = provider.pricingTiers?.onDemand;
  const hourlyRate = onDemandTier?.hourlyRate ?? provider.hourlyRate;
  if (options.showMonthlyRate && onDemandTier?.totals?.computeMonthly) {
    target.hourly.textContent = formatMonthly(
      onDemandTier.totals.computeMonthly
    );
  } else {
    target.hourly.textContent = formatRate(hourlyRate);
  }

  updateTier(target.tiers.onDemand, onDemandTier, {
    showMonthlyRate: options.showMonthlyRate,
  });
  updateTier(target.tiers.year1, provider.pricingTiers?.reserved1yr, {
    showMonthlyRate: options.showMonthlyRate,
  });
  updateTier(target.tiers.year3, provider.pricingTiers?.reserved3yr, {
    showMonthlyRate: options.showMonthlyRate,
  });

  const breakdownTotals = onDemandTier?.totals ?? provider.totals;
  if (!breakdownTotals || !Number.isFinite(breakdownTotals.total)) {
    target.breakdown.textContent =
      pricingFocus === "all" ? "Compute rate unavailable." : "Rate unavailable.";
  } else {
    const dataLabel = options.mode === "k8s" ? "Shared" : "Data";
    const storageInfo = provider.storage
      ? `(OS ${provider.storage.osDiskGb} GB + ${dataLabel} ${provider.storage.dataDiskTb} TB)`
      : "";
    const backupInfo = provider.backup
      ? provider.backup.enabled
        ? `(Snapshots ${Math.round(
            provider.backup.snapshotGb
          )} GB, ${provider.backup.retentionDays}d @ ${
            provider.backup.dailyDeltaPercent
          }%)`
        : "(Disabled)"
      : "";
    const drInfo = provider.dr ? `(DR ${provider.dr.percent}%)` : "";
    const showSql = options.mode !== "k8s" && pricingFocus === "all";
    const sqlIncluded = showSql && provider.sqlNote
      ? provider.sqlNote.toLowerCase().includes("included")
      : false;
    const sqlLine = showSql
      ? sqlIncluded
        ? "SQL included"
        : `SQL ${formatMoney(breakdownTotals.sqlMonthly)}`
      : null;
    const windowsLine =
      Number.isFinite(breakdownTotals.windowsLicenseMonthly) &&
      breakdownTotals.windowsLicenseMonthly > 0
        ? `Windows ${formatMoney(breakdownTotals.windowsLicenseMonthly)}`
        : null;
    const countLabel = options.mode === "k8s" ? "Nodes" : "VMs";
    const vmLabel =
      pricingFocus === "all" && options.vmCount && options.vmCount > 1
        ? `${countLabel} ${options.vmCount}`
        : null;
    const controlPlaneMonthly = breakdownTotals.controlPlaneMonthly;
    let controlPlaneLine = null;
    if (
      Number.isFinite(controlPlaneMonthly) &&
      controlPlaneMonthly > 0
    ) {
      const perHost =
        options.mode === "k8s" && options.vmCount
          ? controlPlaneMonthly / options.vmCount
          : null;
      const perHostLabel = Number.isFinite(perHost)
        ? ` (${formatMoney(perHost)}/host)`
        : "";
      controlPlaneLine = `Control plane ${formatMoney(
        controlPlaneMonthly
      )}${perHostLabel}`;
    }
    const networkItems = Array.isArray(provider.networkAddons?.items)
      ? provider.networkAddons.items
      : [];
    let networkLine = null;
    if (networkItems.length) {
      const labels = networkItems.map((item) => item.label);
      const networkMonthly = breakdownTotals.networkMonthly;
      networkLine = `Network ${formatMoney(networkMonthly)} (${labels.join(
        " + "
      )})`;
    }
    const defaultBreakdownLines = [
      `Compute ${formatMoney(breakdownTotals.computeMonthly)}`,
      controlPlaneLine,
      `Storage ${formatMoney(breakdownTotals.storageMonthly)} ${storageInfo}`.trim(),
      `Backups ${formatMoney(breakdownTotals.backupMonthly)} ${backupInfo}`.trim(),
      `DR ${formatMoney(breakdownTotals.drMonthly)} ${drInfo}`.trim(),
      networkLine,
      `Egress ${formatMoney(breakdownTotals.egressMonthly)}`,
      windowsLine,
      sqlLine,
    ].filter(Boolean);
    const networkBreakdownLine =
      networkLine || `Network ${formatMoney(breakdownTotals.networkMonthly)}`;
    const storageBreakdownLines = [
      `Storage ${formatMoney(breakdownTotals.storageMonthly)} ${storageInfo}`.trim(),
      `Backups ${formatMoney(breakdownTotals.backupMonthly)} ${backupInfo}`.trim(),
    ].filter(Boolean);
    const breakdownLines =
      pricingFocus === "network"
        ? [networkBreakdownLine]
        : pricingFocus === "storage"
        ? storageBreakdownLines
        : defaultBreakdownLines;
    if (vmLabel) {
      breakdownLines.unshift(vmLabel);
    }
    target.breakdown.textContent = breakdownLines.join(" | ");
  }

  const onDemandTotal = onDemandTier?.totals?.total;
  const year1Total = provider.pricingTiers?.reserved1yr?.totals?.total;
  const year3Total = provider.pricingTiers?.reserved3yr?.totals?.total;
  target.savings.textContent = [
    formatSavings("1-year", onDemandTotal, year1Total),
    formatSavings("3-year", onDemandTotal, year3Total),
  ].join(" | ");
  const year1Diff = onDemandTotal - year1Total;
  const year3Diff = onDemandTotal - year3Total;
  target.savings.classList.toggle(
    "negative",
    (Number.isFinite(year1Diff) && year1Diff < 0) ||
      (Number.isFinite(year3Diff) && year3Diff < 0)
  );

  const noteParts = [];
  if (provider.message && pricingFocus === "all") {
    noteParts.push(provider.message);
  }
  if (provider.networkAddons?.note && pricingFocus !== "storage") {
    noteParts.push(provider.networkAddons.note);
  }
  if (options.mode !== "k8s" && pricingFocus === "all" && provider.sqlNote) {
    noteParts.push(provider.sqlNote);
  }
  if (
    pricingFocus === "all" &&
    options.showReservationNote &&
    provider.reservationNote
  ) {
    noteParts.push(provider.reservationNote);
  }
  if (provider && provider.status !== "error") {
    noteParts.push(buildProviderSourceConfidence(provider, options, breakdownTotals));
  }
  target.note.textContent = noteParts.join(" ");
}

function sortSizesByResources(sizes) {
  return [...sizes].sort((a, b) => {
    if (a.vcpu === b.vcpu) {
      return a.memory - b.memory;
    }
    return a.vcpu - b.vcpu;
  });
}

function getProviderSelect(providerKey) {
  if (providerKey === "aws") {
    return awsInstanceSelect;
  }
  if (providerKey === "azure") {
    return azureInstanceSelect;
  }
  if (providerKey === "gcp") {
    return gcpInstanceSelect;
  }
  return null;
}

function buildAutoInstanceTypes(providerKey, sizes) {
  const sorted = sortSizesByResources(sizes);
  const selected = [];
  const current = getProviderSelect(providerKey)?.value;
  if (current && sorted.some((size) => size.type === current)) {
    selected.push(current);
  }
  for (const size of sorted) {
    if (selected.length >= MAX_VENDOR_OPTIONS) {
      break;
    }
    if (!selected.includes(size.type)) {
      selected.push(size.type);
    }
  }
  return selected;
}

function resolveVendorInstanceTypes(providerKey, sizes) {
  if (!sizes.length) {
    vendorOptionState[providerKey] = [];
    return [];
  }
  const available = new Set(sizes.map((size) => size.type));
  const stored = Array.isArray(vendorOptionState[providerKey])
    ? vendorOptionState[providerKey]
    : [];
  const resolved = stored.filter((type) => available.has(type));
  const autoTypes = buildAutoInstanceTypes(providerKey, sizes);
  autoTypes.forEach((type) => {
    if (resolved.length >= MAX_VENDOR_OPTIONS) {
      return;
    }
    if (!resolved.includes(type)) {
      resolved.push(type);
    }
  });
  vendorOptionState[providerKey] = resolved.slice(0, MAX_VENDOR_OPTIONS);
  return vendorOptionState[providerKey];
}

function buildPrivateOptionDefaults() {
  const primaryProvider = getPrimaryPrivateProvider();
  const primaryConfig = normalizePrivateConfig(
    primaryProvider?.config || buildDefaultPrivateConfig()
  );
  const osDefault = Number.parseFloat(primaryConfig?.vmOsDiskGb);
  const osDisk =
    Number.isFinite(osDefault) && osDefault > 0
      ? osDefault
      : Number.parseFloat(osDiskInput?.value) || 256;
  const dataTb = Number.parseFloat(dataDiskInput?.value);
  const dataGb = Number.isFinite(dataTb) ? dataTb * 1024 : 1024;
  return PRIVATE_FLAVORS.slice(0, MAX_VENDOR_OPTIONS).map((flavor) => ({
    vcpu: flavor.vcpu,
    ram: flavor.ram,
    osDiskGb: osDisk,
    dataDiskGb: dataGb,
    providerId: primaryProvider?.id || "",
  }));
}

function resolvePrivateOptions() {
  const defaults = buildPrivateOptionDefaults();
  const stored = Array.isArray(vendorOptionState.private)
    ? vendorOptionState.private
    : [];
  const options = defaults.map((def, index) => ({
    ...def,
    ...(stored[index] || {}),
  }));
  vendorOptionState.private = options;
  return options;
}

function createVendorOptionCard(providerKey, optionIndex, sizes, selectedType) {
  if (!vendorCardTemplate?.content?.firstElementChild) {
    return null;
  }
  const card = vendorCardTemplate.content.firstElementChild.cloneNode(true);
  card.dataset.provider = providerKey;
  card.dataset.option = (optionIndex + 1).toString();
  const title = card.querySelector("[data-field='title']");
  if (title) {
    title.textContent = `${getProviderLabel(providerKey)} Option ${optionIndex + 1}`;
  }
  const instanceSelect = card.querySelector("[data-field='instanceSelect']");
  if (instanceSelect instanceof HTMLSelectElement) {
    setInstanceOptions(instanceSelect, sizes, selectedType);
  }
  return {
    element: card,
    fields: buildProviderFieldsFromCard(card),
    instanceSelect,
    providerKey,
    optionIndex,
  };
}

function createPrivateOptionCard(optionIndex, option) {
  if (!privateOptionTemplate?.content?.firstElementChild) {
    return null;
  }
  const card = privateOptionTemplate.content.firstElementChild.cloneNode(true);
  card.dataset.provider = "private";
  card.dataset.option = (optionIndex + 1).toString();
  const title = card.querySelector("[data-field='title']");
  if (title) {
    title.textContent = `Private Option ${optionIndex + 1}`;
  }
  const vcpuInput = card.querySelector("[data-field='spec-vcpu']");
  const ramInput = card.querySelector("[data-field='spec-ram']");
  const osInput = card.querySelector("[data-field='spec-os']");
  const dataInput = card.querySelector("[data-field='spec-data']");
  const providerSelect = card.querySelector("[data-field='providerSelect']");
  const selectedProviderId =
    option.providerId ||
    getPrimaryPrivateProvider()?.id ||
    privateProviderStore.providers[0]?.id ||
    "";
  fillPrivateProviderSelect(providerSelect, selectedProviderId);
  if (providerSelect instanceof HTMLSelectElement) {
    providerSelect.disabled = privateProviderStore.providers.length < 2;
  }
  if (vcpuInput) {
    vcpuInput.value = option.vcpu;
  }
  if (ramInput) {
    ramInput.value = option.ram;
  }
  if (osInput) {
    osInput.value = option.osDiskGb;
  }
  if (dataInput) {
    dataInput.value = option.dataDiskGb;
  }
  return {
    element: card,
    fields: buildProviderFieldsFromCard(card),
    specInputs: {
      vcpuInput,
      ramInput,
      osInput,
      dataInput,
    },
    providerSelect,
    providerId: selectedProviderId,
    providerKey: "private",
    optionIndex,
  };
}

function buildVendorPayload(basePayload, cardState) {
  let payload = { ...basePayload };
  if (cardState.providerKey === "aws") {
    payload.awsInstanceType = cardState.instanceSelect?.value || "";
  }
  if (cardState.providerKey === "azure") {
    payload.azureInstanceType = cardState.instanceSelect?.value || "";
  }
  if (cardState.providerKey === "gcp") {
    payload.gcpInstanceType = cardState.instanceSelect?.value || "";
  }
  if (cardState.providerKey === "private") {
    const selectedProviderId =
      cardState.providerSelect?.value || cardState.providerId || "";
    const selectedProvider = selectedProviderId
      ? getPrivateProviderById(selectedProviderId)
      : null;
    if (selectedProvider?.config) {
      payload = applyPrivateConfigToPayload(payload, selectedProvider.config, {
        forceEnable: true,
      });
    } else {
      payload.privateEnabled = false;
    }
    const vcpu = Number.parseFloat(cardState.specInputs.vcpuInput?.value);
    const ram = Number.parseFloat(cardState.specInputs.ramInput?.value);
    const osDiskGb = Number.parseFloat(cardState.specInputs.osInput?.value);
    const dataDiskGb = Number.parseFloat(cardState.specInputs.dataInput?.value);
    payload.cpu = Number.isFinite(vcpu) ? vcpu : payload.cpu;
    payload.privateVmMemory = Number.isFinite(ram) ? ram : null;
    payload.osDiskGb = Number.isFinite(osDiskGb) ? osDiskGb : payload.osDiskGb;
    if (Number.isFinite(dataDiskGb) && dataDiskGb >= 0) {
      payload.dataDiskTb = dataDiskGb / 1024;
    }
    payload.privateVmOsDiskGb = payload.osDiskGb;
  }
  return payload;
}

async function fetchVendorCard(cardState, basePayload) {
  const payload = buildVendorPayload(basePayload, cardState);
  if (cardState.providerKey === "private" && !payload.privateEnabled) {
    updateProvider(
      cardState.fields,
      {
        status: "manual",
        message:
          privateProviderStore.providers.length > 0
            ? "Select a private provider profile."
            : "Create and save a private provider profile first.",
        family: "Private cloud",
        instance: {},
        pricingTiers: {},
      },
      { location: "Private DC" },
      {
        showMonthlyRate: false,
        showReservationNote: false,
        vmCount: basePayload.vmCount,
        mode: basePayload.mode || currentMode,
        providerKey: "private",
        pricingFocus: basePayload.pricingFocus,
        pricingProvider: basePayload.pricingProvider,
      }
    );
    return null;
  }
  const data = await comparePricing(payload);
  const providerKey = cardState.providerKey;
  let providerData = data[providerKey];
  if (providerKey === "private") {
    const selectedProvider = getPrivateProviderById(
      cardState.providerSelect?.value || cardState.providerId || ""
    );
    if (selectedProvider) {
      providerData = {
        ...providerData,
        family: `Private cloud (${selectedProvider.name})`,
      };
    }
  }
  const vmCount = data.input?.vmCount ?? payload.vmCount;
  const mode = data.input?.mode ?? payload.mode ?? "vm";
  updateProvider(cardState.fields, providerData, data.region[providerKey], {
    showMonthlyRate: false,
    showReservationNote: providerKey === "aws",
    vmCount,
    mode,
    providerKey,
    sharedStorageSources: data.notes?.sharedStorageSources || null,
    pricingFocus: data.input?.pricingFocus,
    pricingProvider: data.input?.pricingProvider,
  });
  return providerData;
}

async function fetchVendorOptions() {
  if (!vendorGrid) {
    return;
  }
  const basePayload = serializeForm(form);
  const providerKey = currentView;
  vendorGrid.innerHTML = "";
  const cards = [];
  if (providerKey === "private") {
    const options = resolvePrivateOptions();
    options.forEach((option, index) => {
      const cardState = createPrivateOptionCard(index, option);
      if (!cardState) {
        return;
      }
      vendorGrid.appendChild(cardState.element);
      cards.push(cardState);
      const onChange = async () => {
        cardState.providerId = cardState.providerSelect?.value || "";
        vendorOptionState.private[index] = {
          vcpu: Number.parseFloat(cardState.specInputs.vcpuInput?.value),
          ram: Number.parseFloat(cardState.specInputs.ramInput?.value),
          osDiskGb: Number.parseFloat(cardState.specInputs.osInput?.value),
          dataDiskGb: Number.parseFloat(cardState.specInputs.dataInput?.value),
          providerId: cardState.providerId,
        };
        try {
          await fetchVendorCard(cardState, serializeForm(form));
        } catch (error) {
          formNote.textContent =
            error?.message || "Could not fetch pricing. Try again.";
        }
      };
      Object.values(cardState.specInputs).forEach((input) => {
        if (input) {
          input.addEventListener("change", onChange);
        }
      });
      if (cardState.providerSelect) {
        cardState.providerSelect.addEventListener("change", onChange);
      }
    });
  } else {
    const sizes = instancePools[providerKey] || [];
    const selections = resolveVendorInstanceTypes(providerKey, sizes);
    selections.forEach((instanceType, index) => {
      const cardState = createVendorOptionCard(
        providerKey,
        index,
        sizes,
        instanceType
      );
      if (!cardState) {
        return;
      }
      vendorGrid.appendChild(cardState.element);
      cards.push(cardState);
      if (cardState.instanceSelect) {
        cardState.instanceSelect.addEventListener("change", async () => {
          vendorOptionState[providerKey][index] =
            cardState.instanceSelect.value;
          try {
            await fetchVendorCard(cardState, serializeForm(form));
          } catch (error) {
            formNote.textContent =
              error?.message || "Could not fetch pricing. Try again.";
          }
        });
      }
    });
  }
  if (!cards.length) {
    formNote.textContent = "No matching flavors for that CPU selection.";
    return;
  }
  await Promise.all(
    cards.map((cardState) => fetchVendorCard(cardState, basePayload))
  );
  formNote.textContent = "Vendor options loaded.";
}

function updateDelta(aws, azure, gcp, privateProvider) {
  const providers = [
    {
      name: getProviderLabel("aws"),
      total:
        aws.pricingTiers?.onDemand?.totals?.total ?? aws.totals?.total,
    },
    {
      name: getProviderLabel("azure"),
      total:
        azure.pricingTiers?.onDemand?.totals?.total ??
        azure.totals?.total,
    },
    {
      name: getProviderLabel("gcp"),
      total:
        gcp?.pricingTiers?.onDemand?.totals?.total ??
        gcp?.totals?.total,
    },
  ];
  const allowPrivateCompare = currentMode === "vm" || currentMode === "k8s";
  if (allowPrivateCompare && privateProvider?.enabled) {
    providers.push({
      name: getProviderLabel("private"),
      total:
        privateProvider.pricingTiers?.onDemand?.totals?.total ??
        privateProvider.totals?.total,
    });
  }

  const available = providers.filter((item) =>
    Number.isFinite(item.total)
  );
  if (available.length < 2) {
    delta.textContent =
      "Waiting for at least two provider rates to compare totals.";
    delta.classList.remove("negative");
    return;
  }

  available.sort((a, b) => a.total - b.total);
  const lowest = available[0];
  const highest = available[available.length - 1];
  const spread = highest.total - lowest.total;

  if (spread < 0.01) {
    delta.textContent =
      "All providers are estimated at the same monthly cost.";
    delta.classList.remove("negative");
    return;
  }

  const comparisons = available.slice(1).map((item) => {
    const diff = item.total - lowest.total;
    if (diff < 0.01) {
      return `${item.name} same`;
    }
    return `${item.name} +${formatMoney(diff)}/mo`;
  });

  delta.textContent = `Lowest: ${lowest.name} ${formatMonthly(
    lowest.total
  )}. ${comparisons.join(" | ")}.`;

  const awsTotal =
    aws.pricingTiers?.onDemand?.totals?.total ?? aws.totals?.total;
  delta.classList.toggle(
    "negative",
    Number.isFinite(awsTotal) && awsTotal > lowest.total
  );
}

function buildOptionHtml(options, selectedKey) {
  return (options || [])
    .map((option) => {
      const selected = option.key === selectedKey ? " selected" : "";
      return `<option value="${option.key}"${selected}>${option.label}</option>`;
    })
    .join("");
}

function getFocusAddonOptions(providerKey, addonKey) {
  const providerAddons = sizeOptions?.networkAddons?.providers?.[providerKey];
  if (!providerAddons) {
    return [{ key: "none", label: "None" }];
  }
  if (addonKey !== "gateway") {
    return providerAddons[addonKey] || [{ key: "none", label: "None" }];
  }
  const explicit = providerAddons.gateway;
  if (Array.isArray(explicit) && explicit.length) {
    return explicit;
  }
  const vpc = providerAddons.vpc || [];
  const derived = vpc.filter((option) =>
    /gateway|vpn|transit/i.test(option.label || option.key || "")
  );
  if (!derived.find((option) => option.key === "none")) {
    derived.unshift({ key: "none", label: "None" });
  }
  return derived.length ? derived : [{ key: "none", label: "None" }];
}

function renderNetworkProviderCards(data) {
  if (!networkProviderCards) {
    return;
  }
  const input = data?.input || {};
  const providers = [
    { key: "aws", label: "AWS", provider: data?.aws },
    { key: "azure", label: "Azure", provider: data?.azure },
    { key: "gcp", label: "GCP", provider: data?.gcp },
  ];
  const html = providers
    .map(({ key, label, provider }) => {
      const prefix = key;
      const vpcOptions = getFocusAddonOptions(key, "vpc");
      const gatewayOptions = getFocusAddonOptions(key, "gateway");
      const lbOptions = getFocusAddonOptions(key, "loadBalancer");
      const vpcDefault = vpcOptions[0]?.key || "none";
      const gatewayDefault = gatewayOptions[0]?.key || "none";
      const lbDefault = lbOptions[0]?.key || "none";
      const vpcValue = input[`${prefix}NetworkVpcFlavor`] || vpcDefault;
      const gatewayValue =
        input[`${prefix}NetworkGatewayFlavor`] || gatewayDefault;
      const lbValue =
        input[`${prefix}NetworkLoadBalancerFlavor`] || lbDefault;
      const vpcCount = Number.isFinite(input[`${prefix}NetworkVpcCount`])
        ? input[`${prefix}NetworkVpcCount`]
        : 1;
      const gatewayCount = Number.isFinite(
        input[`${prefix}NetworkGatewayCount`]
      )
        ? input[`${prefix}NetworkGatewayCount`]
        : 1;
      const lbCount = Number.isFinite(input[`${prefix}NetworkLoadBalancerCount`])
        ? input[`${prefix}NetworkLoadBalancerCount`]
        : 1;
      const vpcData = Number.isFinite(input[`${prefix}NetworkVpcDataTb`])
        ? input[`${prefix}NetworkVpcDataTb`]
        : 0;
      const gatewayData = Number.isFinite(input[`${prefix}NetworkGatewayDataTb`])
        ? input[`${prefix}NetworkGatewayDataTb`]
        : 0;
      const lbData = Number.isFinite(input[`${prefix}NetworkLoadBalancerDataTb`])
        ? input[`${prefix}NetworkLoadBalancerDataTb`]
        : 0;
      const items = Array.isArray(provider?.networkAddons?.items)
        ? provider.networkAddons.items
        : [];
      const addonMonthly = (addonKey) =>
        items
          .filter((item) => item.addonKey === addonKey)
          .reduce(
            (sum, item) => sum + (Number.isFinite(item.monthlyTotal) ? item.monthlyTotal : 0),
            0
          );
      const vpcMonthly = addonMonthly("vpc");
      const gatewayMonthly = addonMonthly("gateway");
      const lbMonthly = addonMonthly("loadBalancer");
      const providerTotal = provider?.totals?.total || 0;
      const sourceLabel = getNetworkCardSourceLabel(
        {
          networkVpcFlavor: vpcValue,
          networkVpcCount: vpcCount,
          networkGatewayFlavor: gatewayValue,
          networkGatewayCount: gatewayCount,
          networkLoadBalancerFlavor: lbValue,
          networkLoadBalancerCount: lbCount,
          egressTb: input.egressTb || 0,
          interVlanTb: input.interVlanTb || 0,
          intraVlanTb: input.intraVlanTb || 0,
          interRegionTb: input.interRegionTb || 0,
        },
        provider
      );
      const addonSource = summarizeItemSources(
        (items || []).map((item) => ({ source: item?.source }))
      );
      const trafficSource =
        (input.egressTb || 0) > 0 ||
        (input.interVlanTb || 0) > 0 ||
        (input.intraVlanTb || 0) > 0 ||
        (input.interRegionTb || 0) > 0
          ? "fallback"
          : "none";
      const sourceDetail = `Source detail: add-ons ${addonSource}; transfer ${trafficSource}.`;
      return `
        <article class="focus-provider-card">
          <div class="focus-provider-head">
            <h4>${label}</h4>
            <span class="status-pill ${
              sourceLabel === "HARDCODED" ? "hardcoded" : ""
            }">${sourceLabel}</span>
          </div>
          <p class="subtle">API pricing for VPC/VNet, VPC/VPN gateway, and load balancer.</p>
          <p class="subtle">${sourceDetail}</p>
          <div class="row3">
            <label>
              VPC / VNet flavor
              <select name="${prefix}NetworkVpcFlavor" form="pricing-form">${buildOptionHtml(
                vpcOptions,
                vpcValue
              )}</select>
            </label>
            <label>
              Count
              <input type="number" name="${prefix}NetworkVpcCount" form="pricing-form" min="0" step="1" value="${vpcCount}" />
            </label>
            <label>
              Data (TB)
              <input type="number" name="${prefix}NetworkVpcDataTb" form="pricing-form" min="0" step="0.1" value="${vpcData}" />
            </label>
          </div>
          <div class="row3">
            <label>
              VPC/VPN gateway
              <select name="${prefix}NetworkGatewayFlavor" form="pricing-form">${buildOptionHtml(
                gatewayOptions,
                gatewayValue
              )}</select>
            </label>
            <label>
              Count
              <input type="number" name="${prefix}NetworkGatewayCount" form="pricing-form" min="0" step="1" value="${gatewayCount}" />
            </label>
            <label>
              Data (TB)
              <input type="number" name="${prefix}NetworkGatewayDataTb" form="pricing-form" min="0" step="0.1" value="${gatewayData}" />
            </label>
          </div>
          <div class="row3">
            <label>
              Load balancer
              <select name="${prefix}NetworkLoadBalancerFlavor" form="pricing-form">${buildOptionHtml(
                lbOptions,
                lbValue
              )}</select>
            </label>
            <label>
              Count
              <input type="number" name="${prefix}NetworkLoadBalancerCount" form="pricing-form" min="0" step="1" value="${lbCount}" />
            </label>
            <label>
              Data (TB)
              <input type="number" name="${prefix}NetworkLoadBalancerDataTb" form="pricing-form" min="0" step="0.1" value="${lbData}" />
            </label>
          </div>
          <div class="focus-provider-summary">
            <div><span>VPC / VNet monthly</span><strong>${formatMoney(vpcMonthly)}</strong></div>
            <div><span>VPC/VPN gateway monthly</span><strong>${formatMoney(gatewayMonthly)}</strong></div>
            <div><span>LB monthly</span><strong>${formatMoney(lbMonthly)}</strong></div>
            <div><span>Total networking</span><strong>${formatMoney(providerTotal)}</strong></div>
          </div>
        </article>`;
    })
    .join("");
  networkProviderCards.innerHTML = html;
}

function renderStorageProviderCards(data) {
  if (!storageProviderCards) {
    return;
  }
  const input = data?.input || {};
  const providers = [
    { key: "aws", label: "AWS", provider: data?.aws },
    { key: "azure", label: "Azure", provider: data?.azure },
    { key: "gcp", label: "GCP", provider: data?.gcp },
  ];
  const html = providers
    .map(({ key, label, provider }) => {
      const prefix = key;
      const accounts = Number.isFinite(input[`${prefix}StorageAccountCount`])
        ? input[`${prefix}StorageAccountCount`]
        : 1;
      const drEnabled = Boolean(input[`${prefix}StorageDrEnabled`]);
      const drDelta = Number.isFinite(input[`${prefix}StorageDrDeltaTb`])
        ? input[`${prefix}StorageDrDeltaTb`]
        : 0;
      const objectTb = Number.isFinite(input[`${prefix}StorageObjectTb`])
        ? input[`${prefix}StorageObjectTb`]
        : 0;
      const fileTb = Number.isFinite(input[`${prefix}StorageFileTb`])
        ? input[`${prefix}StorageFileTb`]
        : 0;
      const tableTb = Number.isFinite(input[`${prefix}StorageTableTb`])
        ? input[`${prefix}StorageTableTb`]
        : 0;
      const queueTb = Number.isFinite(input[`${prefix}StorageQueueTb`])
        ? input[`${prefix}StorageQueueTb`]
        : 0;
      const breakdown = provider?.storageServices || {};
      const total = provider?.totals?.total || 0;
      const sourceLabel = getStorageCardSourceLabel(
        {
          objectTb,
          fileTb,
          tableTb,
          queueTb,
          drEnabled,
          drDeltaTb: drDelta,
        },
        breakdown
      );
      const sourceMap = breakdown?.sources || {};
      const activeStorageKeys = [
        objectTb > 0 ? "object" : null,
        fileTb > 0 ? "file" : null,
        tableTb > 0 ? "table" : null,
        queueTb > 0 ? "queue" : null,
        drEnabled && drDelta > 0 ? "replication" : null,
      ].filter(Boolean);
      const fallbackKeys = activeStorageKeys.filter((keyName) =>
        isFallbackSource(sourceMap[keyName])
      );
      const sourceDetail = fallbackKeys.length
        ? `Source detail: fallback for ${fallbackKeys.join(", ")}.`
        : "Source detail: API for active services.";
      return `
        <article class="focus-provider-card">
          <div class="focus-provider-head">
            <h4>${label}</h4>
            <span class="status-pill ${
              sourceLabel === "HARDCODED" ? "hardcoded" : ""
            }">${sourceLabel}</span>
          </div>
          <p class="subtle">API pricing for object, file, table, queue, and DR delta replication.</p>
          <p class="subtle">${sourceDetail}</p>
          <div class="row1">
            <label>
              Storage accounts
              <input type="number" name="${prefix}StorageAccountCount" form="pricing-form" min="1" step="1" value="${accounts}" />
            </label>
          </div>
          <div class="row3">
            <label>
              Object (TB)
              <input type="number" name="${prefix}StorageObjectTb" form="pricing-form" min="0" step="0.1" value="${objectTb}" />
            </label>
            <label>
              File (TB)
              <input type="number" name="${prefix}StorageFileTb" form="pricing-form" min="0" step="0.1" value="${fileTb}" />
            </label>
            <label>
              Table (TB)
              <input type="number" name="${prefix}StorageTableTb" form="pricing-form" min="0" step="0.1" value="${tableTb}" />
            </label>
          </div>
          <div class="row3">
            <label>
              Queue (TB)
              <input type="number" name="${prefix}StorageQueueTb" form="pricing-form" min="0" step="0.1" value="${queueTb}" />
            </label>
            <label>
              DR replication
              <input type="checkbox" name="${prefix}StorageDrEnabled" form="pricing-form" ${
                drEnabled ? "checked" : ""
              } />
            </label>
            <label>
              DR delta (TB)
              <input type="number" name="${prefix}StorageDrDeltaTb" form="pricing-form" min="0" step="0.1" value="${drDelta}" />
            </label>
          </div>
          <div class="focus-provider-summary">
            <div><span>Object monthly</span><strong>${formatMoney(
              breakdown.objectMonthly || 0
            )}</strong></div>
            <div><span>File monthly</span><strong>${formatMoney(
              breakdown.fileMonthly || 0
            )}</strong></div>
            <div><span>Table monthly</span><strong>${formatMoney(
              breakdown.tableMonthly || 0
            )}</strong></div>
            <div><span>Queue monthly</span><strong>${formatMoney(
              breakdown.queueMonthly || 0
            )}</strong></div>
            <div><span>Replication monthly</span><strong>${formatMoney(
              breakdown.replicationMonthly || 0
            )}</strong></div>
            <div><span>Total storage</span><strong>${formatMoney(total)}</strong></div>
          </div>
        </article>`;
    })
    .join("");
  storageProviderCards.innerHTML = html;
}

function renderNetworkFocusTable(data) {
  if (!networkFocusTable) {
    return;
  }
  renderNetworkProviderCards(data);
  const focus = normalizeNetworkAddonFocus(
    data?.input?.networkAddonFocus || currentNetworkResult
  );
  const addonLabel =
    focus === "gateway"
      ? "VPC/VPN gateway"
      : focus === "loadBalancer"
      ? "Load balancer"
      : "VPC / VNet";
  const providers = [
    { label: "AWS", provider: data.aws },
    { label: "Azure", provider: data.azure },
    { label: "GCP", provider: data.gcp },
  ];
  const rows = providers.map(({ label, provider }) => {
    const item = Array.isArray(provider?.networkAddons?.items)
      ? provider.networkAddons.items.find((entry) => entry.addonKey === focus)
      : null;
    const totals = provider?.totals || {};
    return `
      <tr>
        <td>${label}</td>
        <td>${item?.label || "None"}</td>
        <td>${item?.count || 0}</td>
        <td>${item?.dataTb || 0}</td>
        <td>${formatMoney(item?.monthlyTotal || 0)}</td>
        <td>${formatMoney(totals.interVlanMonthly || 0)}</td>
        <td>${formatMoney(totals.intraVlanMonthly || 0)}</td>
        <td>${formatMoney(totals.interRegionMonthly || 0)}</td>
        <td>${formatMoney(totals.egressMonthly || 0)}</td>
        <td><strong>${formatMoney(totals.total || 0)}</strong></td>
      </tr>`;
  });
  networkFocusTable.innerHTML = `
    <table class="focus-table">
      <thead>
        <tr>
          <th>Provider</th>
          <th>${addonLabel} option</th>
          <th>Count</th>
          <th>Data (TB)</th>
          <th>${addonLabel} monthly</th>
          <th>Inter-VLAN</th>
          <th>Intra-VLAN</th>
          <th>Inter-region</th>
          <th>Egress</th>
          <th>Total networking</th>
        </tr>
      </thead>
      <tbody>
        ${rows.join("")}
      </tbody>
    </table>`;
}

function renderStorageFocusTable(data) {
  if (!storageFocusTable) {
    return;
  }
  renderStorageProviderCards(data);
  const isPerformance = currentStorageResult === "performance";
  const providers = [
    { label: "AWS", provider: data.aws },
    { label: "Azure", provider: data.azure },
    { label: "GCP", provider: data.gcp },
  ];
  const rows = providers.map(({ label, provider }) => {
    const totals = provider?.totals || {};
    const breakdown = provider?.storageServices || {};
    if (isPerformance) {
      return `
        <tr>
          <td>${label}</td>
          <td>${breakdown.drEnabled ? "Enabled" : "Disabled"}</td>
          <td>${breakdown.drDeltaTb || 0}</td>
          <td>${formatMoney(breakdown.replicationMonthly || totals.egressMonthly || 0)}</td>
          <td><strong>${formatMoney(totals.total || 0)}</strong></td>
        </tr>`;
    }
    return `
      <tr>
        <td>${label}</td>
        <td>${formatMoney(breakdown.objectMonthly || 0)}</td>
        <td>${formatMoney(breakdown.fileMonthly || 0)}</td>
        <td>${formatMoney(breakdown.tableMonthly || 0)}</td>
        <td>${formatMoney(breakdown.queueMonthly || 0)}</td>
        <td><strong>${formatMoney(totals.total || 0)}</strong></td>
      </tr>`;
  });
  storageFocusTable.innerHTML = `
    <table class="focus-table">
      <thead>
        <tr>
          <th>Provider</th>
          ${
            isPerformance
              ? "<th>DR replication</th><th>DR delta (TB)</th><th>Replication monthly</th><th>Total storage</th>"
              : "<th>Object</th><th>File</th><th>Table</th><th>Queue</th><th>Total storage</th>"
          }
        </tr>
      </thead>
      <tbody>
        ${rows.join("")}
      </tbody>
    </table>`;
}

async function comparePricing(payload) {
  const response = await fetch(resolvePriceApiPath("/api/compare"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error("Pricing request failed.");
  }
  return response.json();
}

function buildRegionChecklist() {
  if (!vendorRegionPicker || !regionSelect) {
    return;
  }
  vendorRegionPicker.innerHTML = "";
  const options = Array.from(regionSelect.options);
  const defaults = new Set();
  if (regionSelect.value) {
    defaults.add(regionSelect.value);
  }
  options.forEach((option) => {
    if (defaults.size < 3) {
      defaults.add(option.value);
    }
  });
  options.forEach((option) => {
    const label = document.createElement("label");
    label.className = "region-option";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = option.value;
    checkbox.checked = defaults.has(option.value);
    const text = document.createElement("span");
    text.textContent = option.textContent;
    label.appendChild(checkbox);
    label.appendChild(text);
    vendorRegionPicker.appendChild(label);
  });
}

function getSelectedRegionKeys() {
  if (!vendorRegionPicker) {
    return [];
  }
  const inputs = vendorRegionPicker.querySelectorAll(
    "input[type='checkbox']"
  );
  return Array.from(inputs)
    .filter((input) => input.checked)
    .map((input) => input.value);
}

function getRegionLabel(regionKey) {
  if (!regionSelect) {
    return regionKey;
  }
  const option = Array.from(regionSelect.options).find(
    (item) => item.value === regionKey
  );
  return option ? option.textContent : regionKey;
}

function getPrimaryInstanceType(providerKey) {
  const stored = Array.isArray(vendorOptionState[providerKey])
    ? vendorOptionState[providerKey]
    : [];
  if (stored.length) {
    return stored[0];
  }
  const select = getProviderSelect(providerKey);
  return select?.value || "";
}

async function runRegionCompare() {
  if (!vendorRegionTable || !vendorRegionNote) {
    return;
  }
  const providerKey = currentView;
  if (
    providerKey !== "aws" &&
    providerKey !== "azure" &&
    providerKey !== "gcp"
  ) {
    return;
  }
  const selectedRegions = getSelectedRegionKeys();
  if (selectedRegions.length < 3 || selectedRegions.length > 5) {
    vendorRegionNote.textContent = "Select 3 to 5 regions to compare.";
    vendorRegionNote.classList.add("negative");
    return;
  }
  vendorRegionNote.classList.remove("negative");
  vendorRegionNote.textContent = "Running region compare...";
  const basePayload = serializeForm(form);
  const instanceType = getPrimaryInstanceType(providerKey);
  const results = await Promise.all(
    selectedRegions.map(async (regionKey) => {
      const payload = { ...basePayload, regionKey };
      if (providerKey === "aws") {
        payload.awsInstanceType = instanceType;
      }
      if (providerKey === "azure") {
        payload.azureInstanceType = instanceType;
      }
      if (providerKey === "gcp") {
        payload.gcpInstanceType = instanceType;
      }
      try {
        const data = await comparePricing(payload);
        return { regionKey, data };
      } catch (error) {
        return {
          regionKey,
          error: error?.message || "Pricing request failed.",
        };
      }
    })
  );
  renderRegionCompareTable(results, providerKey);
  vendorRegionNote.textContent = "Region compare updated.";
}

function renderRegionCompareTable(rows, providerKey) {
  if (!vendorRegionTable) {
    return;
  }
  vendorRegionTable.innerHTML = "";
  if (!rows.length) {
    vendorRegionTable.textContent = "No region results yet.";
    return;
  }
  const table = document.createElement("table");
  const head = document.createElement("thead");
  const headRow = document.createElement("tr");
  ["Region", "On-demand", "1-year", "3-year", "Compute rate"].forEach(
    (label) => {
      const cell = document.createElement("th");
      cell.textContent = label;
      headRow.appendChild(cell);
    }
  );
  head.appendChild(headRow);
  table.appendChild(head);
  const body = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    const regionCell = document.createElement("td");
    const location =
      row.data?.region?.[providerKey]?.location ||
      getRegionLabel(row.regionKey);
    regionCell.textContent = location;
    tr.appendChild(regionCell);

    if (row.error) {
      const errorCell = document.createElement("td");
      errorCell.textContent = row.error;
      errorCell.colSpan = 4;
      tr.appendChild(errorCell);
      body.appendChild(tr);
      return;
    }

    const provider = row.data?.[providerKey];
    const onDemandTier = provider?.pricingTiers?.onDemand;
    const onDemandTotal = onDemandTier?.totals?.total;
    const year1Total = provider?.pricingTiers?.reserved1yr?.totals?.total;
    const year3Total = provider?.pricingTiers?.reserved3yr?.totals?.total;
    const hourlyRate = onDemandTier?.hourlyRate ?? provider?.hourlyRate;

    [onDemandTotal, year1Total, year3Total].forEach((value) => {
      const cell = document.createElement("td");
      cell.textContent = formatMonthly(value);
      tr.appendChild(cell);
    });
    const rateCell = document.createElement("td");
    rateCell.textContent = formatRate(hourlyRate);
    tr.appendChild(rateCell);
    body.appendChild(tr);
  });
  table.appendChild(body);
  vendorRegionTable.appendChild(table);
}

async function refreshSavedCompare(options = {}) {
  if (!savedCompareTable || !savedCompareNote) {
    return;
  }
  if (!scenarioStore.length) {
    savedCompareRows = [];
    renderSavedCompareTable([]);
    savedCompareNote.textContent = "No saved scenarios yet.";
    savedCompareNote.classList.remove("negative");
    return;
  }
  const scenarioIds = Array.isArray(options.scenarioIds)
    ? options.scenarioIds
    : null;
  const selectedScenarios = scenarioIds
    ? scenarioStore.filter((scenario) => scenarioIds.includes(scenario.id))
    : scenarioStore;
  if (scenarioIds && !selectedScenarios.length) {
    savedCompareRows = [];
    renderSavedCompareTable([]);
    savedCompareNote.textContent = "Select scenarios to compare.";
    savedCompareNote.classList.add("negative");
    return;
  }
  savedCompareNote.textContent = `Running ${selectedScenarios.length} scenarios...`;
  savedCompareNote.classList.remove("negative");
  const rows = [];
  for (const scenario of selectedScenarios) {
    try {
      const data = await comparePricing(scenario.input);
      rows.push({ scenario, data });
    } catch (error) {
      rows.push({
        scenario,
        error: error?.message || "Pricing request failed.",
      });
    }
  }
  savedCompareRows = rows;
  renderSavedCompareTable(rows);
  const hasError = rows.some((row) => row.error);
  const successfulRows = rows.filter((row) => row.data && !row.error);
  savedCompareNote.textContent = hasError
    ? `Saved compare updated with errors (${successfulRows.length}/${rows.length} succeeded).`
    : `Saved compare updated (${successfulRows.length} scenarios).`;
  savedCompareNote.classList.toggle("negative", hasError);
}

function renderSavedCompareTable(rows) {
  if (!savedCompareTable) {
    return;
  }
  savedCompareTable.innerHTML = "";
  if (!rows.length) {
    savedCompareTable.textContent = "No saved scenarios to display.";
    return;
  }
  const table = document.createElement("table");
  const head = document.createElement("thead");
  const headRow = document.createElement("tr");
  [
    "Scenario",
    "Mode",
    "Region",
    "AWS",
    "Azure",
    "GCP",
    "Private",
    "Status",
    "Actions",
  ].forEach((label) => {
    const cell = document.createElement("th");
    cell.textContent = label;
    headRow.appendChild(cell);
  });
  head.appendChild(headRow);
  table.appendChild(head);
  const body = document.createElement("tbody");
  const totals = { aws: 0, azure: 0, gcp: 0, private: 0, count: 0 };
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    const input = row.data?.input || row.scenario.input || {};
    const mode = getScenarioDisplayMode(input);
    const regionLabel = getRegionLabel(input.regionKey || "");
    const awsTotal = row.data ? getScenarioProviderTotal(row.data, "aws") : null;
    const azureTotal = row.data
      ? getScenarioProviderTotal(row.data, "azure")
      : null;
    const gcpTotal = row.data ? getScenarioProviderTotal(row.data, "gcp") : null;
    const privateTotal = row.data
      ? getScenarioProviderTotal(row.data, "private")
      : null;
    if (Number.isFinite(awsTotal)) {
      totals.aws += awsTotal;
    }
    if (Number.isFinite(azureTotal)) {
      totals.azure += azureTotal;
    }
    if (Number.isFinite(gcpTotal)) {
      totals.gcp += gcpTotal;
    }
    if (Number.isFinite(privateTotal)) {
      totals.private += privateTotal;
    }
    if (
      Number.isFinite(awsTotal) ||
      Number.isFinite(azureTotal) ||
      Number.isFinite(gcpTotal) ||
      Number.isFinite(privateTotal)
    ) {
      totals.count += 1;
    }

    [
      row.scenario.name,
      mode,
      regionLabel,
      formatMonthly(awsTotal),
      formatMonthly(azureTotal),
      formatMonthly(gcpTotal),
      formatMonthly(privateTotal),
      row.error ? row.error : "OK",
    ].forEach((value) => {
      const cell = document.createElement("td");
      cell.textContent = value || "-";
      tr.appendChild(cell);
    });
    const actionCell = document.createElement("td");
    const exportJsonButton = document.createElement("button");
    exportJsonButton.type = "button";
    exportJsonButton.className = "table-action";
    exportJsonButton.textContent = "Export JSON";
    exportJsonButton.addEventListener("click", () => {
      handleExportScenario(row.scenario, savedCompareNote);
    });

    const exportCsvButton = document.createElement("button");
    exportCsvButton.type = "button";
    exportCsvButton.className = "table-action";
    exportCsvButton.textContent = "Export CSV";
    exportCsvButton.addEventListener("click", () => {
      handleExportScenarioCsv(row.scenario, savedCompareNote);
    });

    const deleteButton = document.createElement("button");
    deleteButton.type = "button";
    deleteButton.className = "table-action";
    deleteButton.textContent = "Delete";
    deleteButton.addEventListener("click", () => {
      const confirmDelete = window.confirm(
        `Delete scenario "${row.scenario.name}"?`
      );
      if (!confirmDelete) {
        return;
      }
      const deletedName = deleteScenarioById(row.scenario.id);
      savedCompareRows = savedCompareRows.filter(
        (item) => item.scenario.id !== row.scenario.id
      );
      renderSavedCompareTable(savedCompareRows);
      if (savedCompareNote) {
        savedCompareNote.textContent = deletedName
          ? `Deleted "${deletedName}".`
          : "Scenario deleted.";
        savedCompareNote.classList.remove("negative");
      }
    });
    actionCell.appendChild(exportJsonButton);
    actionCell.appendChild(exportCsvButton);
    actionCell.appendChild(deleteButton);
    tr.appendChild(actionCell);
    body.appendChild(tr);
  });
  if (totals.count > 1) {
    const totalRow = document.createElement("tr");
    totalRow.className = "saved-compare-total";
    [
      "Portfolio total",
      "-",
      `${totals.count} scenarios`,
      formatMonthly(totals.aws),
      formatMonthly(totals.azure),
      formatMonthly(totals.gcp),
      formatMonthly(totals.private),
      "-",
      "-",
    ].forEach((value) => {
      const cell = document.createElement("td");
      cell.textContent = value || "-";
      totalRow.appendChild(cell);
    });
    body.appendChild(totalRow);
  }
  table.appendChild(body);
  savedCompareTable.appendChild(table);
}

function getSavedCompareRow(id) {
  return savedCompareRows.find(
    (row) => row.scenario?.id === id && row.data && !row.error
  );
}

function getPrivateSharedNetworkMonthly(config) {
  if (!config || typeof config !== "object") {
    return 0;
  }
  const network = Number.parseFloat(config.networkMonthly);
  const firewall = Number.parseFloat(config.firewallMonthly);
  const loadBalancer = Number.parseFloat(config.loadBalancerMonthly);
  const total =
    (Number.isFinite(network) ? network : 0) +
    (Number.isFinite(firewall) ? firewall : 0) +
    (Number.isFinite(loadBalancer) ? loadBalancer : 0);
  return total > 0 ? total : 0;
}

function applySharedPrivateNetworkOnce(rows, privateProviders) {
  if (!Array.isArray(rows) || !rows.length || !Array.isArray(privateProviders)) {
    return;
  }
  privateProviders.forEach((provider) => {
    const sharedNetworkMonthly = getPrivateSharedNetworkMonthly(provider?.config);
    if (!Number.isFinite(sharedNetworkMonthly) || sharedNetworkMonthly <= 0) {
      return;
    }
    const applicableRows = rows
      .map((row) => {
        const breakdown = row?.privateBreakdowns?.[provider.id];
        if (!breakdown || row?.privateErrors?.[provider.id]) {
          return null;
        }
        const total = Number.isFinite(breakdown.total) ? breakdown.total : null;
        const networkMonthly = Number.isFinite(breakdown.networkMonthly)
          ? breakdown.networkMonthly
          : 0;
        if (!Number.isFinite(total) || networkMonthly <= 0) {
          return null;
        }
        const baseTotal = Math.max(0, total - networkMonthly);
        return {
          row,
          baseTotal,
        };
      })
      .filter(Boolean);
    if (applicableRows.length <= 1) {
      return;
    }
    const baseSum = applicableRows.reduce(
      (sum, entry) => sum + entry.baseTotal,
      0
    );
    let allocated = 0;
    applicableRows.forEach((entry, index) => {
      const isLast = index === applicableRows.length - 1;
      const share = isLast
        ? Math.max(0, sharedNetworkMonthly - allocated)
        : baseSum > 0
        ? (sharedNetworkMonthly * entry.baseTotal) / baseSum
        : sharedNetworkMonthly / applicableRows.length;
      allocated += share;
      const breakdown = entry.row.privateBreakdowns[provider.id];
      const nextTotal = entry.baseTotal + share;
      entry.row.privateBreakdowns[provider.id] = {
        ...breakdown,
        networkMonthly: share,
        total: nextTotal,
      };
      entry.row.privateTotals[provider.id] = nextTotal;
    });
  });
}

async function runSavedPrivateCompare() {
  if (!savedComparePrivateTable || !savedComparePrivateNote) {
    return;
  }
  const scenarioIds = resolveScenarioSelections();
  if (!scenarioIds.length) {
    savedComparePrivateRows = [];
    renderSavedPrivateCompareTable([], []);
    savedComparePrivateNote.textContent =
      "Select scenarios to compare against private providers.";
    savedComparePrivateNote.classList.add("negative");
    return;
  }
  const privateIds = resolvePrivateSelections();
  if (!privateIds.length) {
    savedComparePrivateRows = [];
    renderSavedPrivateCompareTable([], []);
    savedComparePrivateNote.textContent =
      "Select at least one private provider.";
    savedComparePrivateNote.classList.add("negative");
    return;
  }
  const scenarios = scenarioStore.filter((scenario) =>
    scenarioIds.includes(scenario.id)
  );
  const privateProviders = privateIds
    .map((id) => getPrivateProviderById(id))
    .filter(Boolean);
  if (!privateProviders.length) {
    savedComparePrivateRows = [];
    renderSavedPrivateCompareTable([], []);
    savedComparePrivateNote.textContent = "No private providers selected.";
    savedComparePrivateNote.classList.add("negative");
    return;
  }
  savedComparePrivateNote.textContent = `Running ${scenarios.length} scenarios across ${privateProviders.length} providers...`;
  savedComparePrivateNote.classList.remove("negative");
  const rows = [];
  for (const scenario of scenarios) {
    const input = scenario.input || {};
    let data = getSavedCompareRow(scenario.id)?.data || null;
    let errorMessage = "";
    if (!data) {
      try {
        data = await comparePricing(input);
      } catch (error) {
        errorMessage = error?.message || "Pricing request failed.";
      }
    }
    const row = {
      scenario,
      data,
      error: errorMessage,
      privateTotals: {},
      privateBreakdowns: {},
      privateErrors: {},
    };
    const privateResults = await Promise.all(
      privateProviders.map(async (provider) => {
        const payload = applyPrivateConfigToPayload(
          input,
          provider.config,
          { forceEnable: true }
        );
        try {
          const privateData = await comparePricing(payload);
          const totals = getScenarioProviderTotals(privateData, "private");
          return {
            id: provider.id,
            total: Number.isFinite(totals?.total) ? totals.total : null,
            totals,
          };
        } catch (error) {
          return {
            id: provider.id,
            error: error?.message || "Private pricing failed.",
          };
        }
      })
    );
    privateResults.forEach((result) => {
      if (result.error) {
        row.privateErrors[result.id] = result.error;
        row.privateTotals[result.id] = null;
        row.privateBreakdowns[result.id] = null;
      } else {
        row.privateTotals[result.id] = result.total;
        row.privateBreakdowns[result.id] = result.totals || null;
      }
    });
    rows.push(row);
  }
  applySharedPrivateNetworkOnce(rows, privateProviders);
  savedComparePrivateRows = rows;
  renderSavedPrivateCompareTable(rows, privateProviders);
  const hasError = rows.some(
    (row) =>
      row.error ||
      Object.values(row.privateErrors).some((value) => value)
  );
  const hasSharedPrivateNetwork =
    scenarios.length > 1 &&
    privateProviders.some(
      (provider) => getPrivateSharedNetworkMonthly(provider?.config) > 0
    );
  savedComparePrivateNote.textContent = hasError
    ? "Private vs public compare completed with errors."
    : hasSharedPrivateNetwork
    ? "Private vs public compare updated. Shared private networking is charged once per provider."
    : "Private vs public compare updated.";
  savedComparePrivateNote.classList.toggle("negative", hasError);
}

function renderSavedPrivateCompareTable(rows, privateProviders) {
  if (!savedComparePrivateTable) {
    return;
  }
  savedComparePrivateTable.innerHTML = "";
  if (!rows.length) {
    savedComparePrivateTable.textContent = "No private compare results yet.";
    return;
  }
  const table = document.createElement("table");
  const head = document.createElement("thead");
  const headRow = document.createElement("tr");
  const privateHeaders = privateProviders.map((provider) => provider.name);
  [
    "Scenario",
    "Mode / Component",
    "Region",
    "AWS",
    "Azure",
    "GCP",
    ...privateHeaders,
    "Status",
  ].forEach((label) => {
    const cell = document.createElement("th");
    cell.textContent = label;
    headRow.appendChild(cell);
  });
  head.appendChild(headRow);
  table.appendChild(head);
  const body = document.createElement("tbody");
  const totals = {
    aws: 0,
    azure: 0,
    gcp: 0,
    private: {},
  };
  privateProviders.forEach((provider) => {
    totals.private[provider.id] = 0;
  });
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.className = "saved-compare-summary";
    const input = row.data?.input || row.scenario.input || {};
    const mode = getScenarioDisplayMode(input);
    const regionLabel = getRegionLabel(input.regionKey || "");
    const awsTotal = row.data ? getScenarioProviderTotal(row.data, "aws") : null;
    const azureTotal = row.data
      ? getScenarioProviderTotal(row.data, "azure")
      : null;
    const gcpTotal = row.data ? getScenarioProviderTotal(row.data, "gcp") : null;
    [
      row.scenario.name,
      mode,
      regionLabel,
      formatMonthly(awsTotal),
      formatMonthly(azureTotal),
      formatMonthly(gcpTotal),
    ].forEach((value) => {
      const cell = document.createElement("td");
      cell.textContent = value || "-";
      tr.appendChild(cell);
    });
    if (Number.isFinite(awsTotal)) {
      totals.aws += awsTotal;
    }
    if (Number.isFinite(azureTotal)) {
      totals.azure += azureTotal;
    }
    if (Number.isFinite(gcpTotal)) {
      totals.gcp += gcpTotal;
    }
    privateProviders.forEach((provider) => {
      const cell = document.createElement("td");
      const total = row.privateTotals[provider.id];
      if (row.privateErrors[provider.id]) {
        cell.textContent = "ERR";
      } else {
        cell.textContent = formatMonthly(total);
      }
      tr.appendChild(cell);
      if (Number.isFinite(total)) {
        totals.private[provider.id] += total;
      }
    });
    const statusCell = document.createElement("td");
    const hasPrivateError = Object.values(row.privateErrors).some(Boolean);
    statusCell.textContent = row.error
      ? row.error
      : hasPrivateError
      ? "Partial"
      : "OK";
    tr.appendChild(statusCell);
    body.appendChild(tr);

    const awsTotals = row.data
      ? getScenarioProviderTotals(row.data, "aws")
      : null;
    const azureTotals = row.data
      ? getScenarioProviderTotals(row.data, "azure")
      : null;
    const gcpTotals = row.data
      ? getScenarioProviderTotals(row.data, "gcp")
      : null;
    const privateTotals = row.privateBreakdowns || {};

    SCENARIO_BREAKDOWN_COMPONENTS.forEach((component) => {
      const componentValues = [
        getScenarioComponentValue(awsTotals, component.field),
        getScenarioComponentValue(azureTotals, component.field),
        getScenarioComponentValue(gcpTotals, component.field),
        ...privateProviders.map((provider) =>
          getScenarioComponentValue(
            privateTotals[provider.id],
            component.field
          )
        ),
      ];
      if (!componentValues.some((value) => Number.isFinite(value))) {
        return;
      }
      const detailRow = document.createElement("tr");
      detailRow.className = "saved-compare-breakdown";
      const scenarioCell = document.createElement("td");
      scenarioCell.textContent = "";
      detailRow.appendChild(scenarioCell);
      const componentCell = document.createElement("td");
      componentCell.textContent = component.label;
      componentCell.className = "saved-compare-component";
      detailRow.appendChild(componentCell);
      const regionCell = document.createElement("td");
      regionCell.textContent = "";
      detailRow.appendChild(regionCell);
      [
        getScenarioComponentValue(awsTotals, component.field),
        getScenarioComponentValue(azureTotals, component.field),
        getScenarioComponentValue(gcpTotals, component.field),
      ].forEach((value) => {
        const cell = document.createElement("td");
        cell.textContent = formatMonthly(value);
        detailRow.appendChild(cell);
      });
      privateProviders.forEach((provider) => {
        const cell = document.createElement("td");
        const value = getScenarioComponentValue(
          privateTotals[provider.id],
          component.field
        );
        cell.textContent = formatMonthly(value);
        detailRow.appendChild(cell);
      });
      const statusDetailCell = document.createElement("td");
      statusDetailCell.textContent = "";
      detailRow.appendChild(statusDetailCell);
      body.appendChild(detailRow);
    });
  });
  const totalRow = document.createElement("tr");
  totalRow.className = "saved-compare-total";
  [
    "Total",
    "-",
    "-",
    formatMonthly(totals.aws),
    formatMonthly(totals.azure),
    formatMonthly(totals.gcp),
  ].forEach((value) => {
    const cell = document.createElement("td");
    cell.textContent = value || "-";
    totalRow.appendChild(cell);
  });
  privateProviders.forEach((provider) => {
    const cell = document.createElement("td");
    cell.textContent = formatMonthly(totals.private[provider.id]);
    totalRow.appendChild(cell);
  });
  const statusCell = document.createElement("td");
  statusCell.textContent = "-";
  totalRow.appendChild(statusCell);
  body.appendChild(totalRow);
  table.appendChild(body);
  savedComparePrivateTable.appendChild(table);
}

function buildSavedCompareCsv(rows) {
  const headers = [
    "Scenario",
    "Mode",
    "Region",
    "AWS_On_Demand",
    "Azure_On_Demand",
    "GCP_On_Demand",
    "Private_On_Demand",
    "Status",
  ];
  const lines = [headers.join(",")];
  const totals = { aws: 0, azure: 0, gcp: 0, private: 0, count: 0 };
  rows.forEach((row) => {
    const input = row.data?.input || row.scenario.input || {};
    const regionLabel = getRegionLabel(input.regionKey || "");
    const awsTotal = row.data ? getScenarioProviderTotal(row.data, "aws") : "";
    const azureTotal = row.data ? getScenarioProviderTotal(row.data, "azure") : "";
    const gcpTotal = row.data ? getScenarioProviderTotal(row.data, "gcp") : "";
    const privateTotal = row.data
      ? getScenarioProviderTotal(row.data, "private")
      : "";
    const line = [
      row.scenario.name,
      input.mode || "vm",
      regionLabel,
      awsTotal,
      azureTotal,
      gcpTotal,
      privateTotal,
      row.error ? row.error : "OK",
    ];
    if (Number.isFinite(awsTotal)) {
      totals.aws += awsTotal;
    }
    if (Number.isFinite(azureTotal)) {
      totals.azure += azureTotal;
    }
    if (Number.isFinite(gcpTotal)) {
      totals.gcp += gcpTotal;
    }
    if (Number.isFinite(privateTotal)) {
      totals.private += privateTotal;
    }
    if (
      Number.isFinite(awsTotal) ||
      Number.isFinite(azureTotal) ||
      Number.isFinite(gcpTotal) ||
      Number.isFinite(privateTotal)
    ) {
      totals.count += 1;
    }
    lines.push(line.map((value) => escapeCsv(value)).join(","));
  });
  if (totals.count > 1) {
    lines.push(
      [
        "Portfolio total",
        "-",
        `${totals.count} scenarios`,
        totals.aws,
        totals.azure,
        totals.gcp,
        totals.private,
        "-",
      ]
        .map((value) => escapeCsv(value))
        .join(",")
    );
  }
  return lines.join("\n");
}

function deleteScenarioById(id) {
  const scenario = getScenarioById(id);
  if (!scenario) {
    return null;
  }
  scenarioStore = scenarioStore.filter((item) => item.id !== id);
  persistScenarioStore(scenarioStore);
  renderScenarioList();
  if (scenarioList && scenarioList.value === id) {
    scenarioList.value = "";
  }
  if (scenarioNameInput && scenarioNameInput.value === scenario.name) {
    scenarioNameInput.value = "";
  }
  return scenario.name;
}

function buildInsightBuckets(totals, focus = "all") {
  if (!totals || !Number.isFinite(totals.total)) {
    return null;
  }
  if (focus === "network") {
    const networkAddons = totals.networkMonthly || 0;
    const transfer =
      (totals.interVlanMonthly || 0) +
      (totals.intraVlanMonthly || 0) +
      (totals.interRegionMonthly || 0);
    const egress = totals.egressMonthly || 0;
    const total = networkAddons + transfer + egress;
    return {
      compute: networkAddons,
      storage: transfer,
      egress,
      licenses: 0,
      total,
      labels: {
        compute: "Network add-ons",
        storage: "VLAN + Inter-region",
        egress: "Egress",
        licenses: "Other",
      },
    };
  }
  if (focus === "storage") {
    const capacity = totals.storageMonthly || 0;
    const replication = totals.egressMonthly || 0;
    const total = capacity + replication;
    return {
      compute: capacity,
      storage: replication,
      egress: 0,
      licenses: 0,
      total,
      labels: {
        compute: "Storage services",
        storage: "DR replication",
        egress: "Other",
        licenses: "Other",
      },
    };
  }
  const compute =
    (totals.computeMonthly || 0) +
    (totals.controlPlaneMonthly || 0) +
    (totals.networkMonthly || 0) +
    (totals.drMonthly || 0);
  const storage = (totals.storageMonthly || 0) + (totals.backupMonthly || 0);
  const egress = totals.egressMonthly || 0;
  const licenses =
    (totals.sqlMonthly || 0) + (totals.windowsLicenseMonthly || 0);
  const total = compute + storage + egress + licenses;
  return {
    compute,
    storage,
    egress,
    licenses,
    total,
    labels: {
      compute: "Compute",
      storage: "Storage",
      egress: "Egress",
      licenses: "Licenses",
    },
  };
}

function renderInsightTo(targetChart, targetNote, data, focusOverride = null) {
  if (!targetChart || !targetNote) {
    return;
  }
  targetChart.innerHTML = "";
  if (!data) {
    targetNote.textContent = "Run a comparison to generate insights.";
    return;
  }
  const pricingFocus = focusOverride || data.input?.pricingFocus || "all";
  const mode = data.input?.mode || "vm";
  const providers = [
    { key: "aws", label: getProviderLabelForMode("aws", mode), data: data.aws },
    {
      key: "azure",
      label: getProviderLabelForMode("azure", mode),
      data: data.azure,
    },
    { key: "gcp", label: getProviderLabelForMode("gcp", mode), data: data.gcp },
  ];
  if (data.private?.enabled) {
    providers.push({
      key: "private",
      label: getProviderLabelForMode("private", mode),
      data: data.private,
    });
  }
  const cards = providers
    .map((provider) => {
      const totals =
        provider.data?.pricingTiers?.onDemand?.totals || provider.data?.totals;
      const buckets = buildInsightBuckets(totals, pricingFocus);
      if (!buckets) {
        return null;
      }
      return { provider, buckets };
    })
    .filter(Boolean);
  if (!cards.length) {
    targetNote.textContent = "No pricing data available for insights.";
    return;
  }
  cards.forEach(({ provider, buckets }) => {
    const card = document.createElement("div");
    card.className = "insight-card";
    const header = document.createElement("div");
    header.className = "insight-card-header";
    const title = document.createElement("h4");
    title.textContent = provider.label;
    const total = document.createElement("span");
    total.textContent = formatMonthly(buckets.total);
    header.appendChild(title);
    header.appendChild(total);
    const bar = document.createElement("div");
    bar.className = "insight-bar";
    const segments = [
      { key: "compute", value: buckets.compute },
      { key: "storage", value: buckets.storage },
      { key: "egress", value: buckets.egress },
      { key: "licenses", value: buckets.licenses },
    ];
    segments.forEach((segment) => {
      const span = document.createElement("span");
      span.className = `insight-segment ${segment.key}`;
      const width =
        buckets.total > 0 ? (segment.value / buckets.total) * 100 : 0;
      span.style.width = `${Math.max(0, width)}%`;
      bar.appendChild(span);
    });
    const metrics = document.createElement("div");
    metrics.className = "insight-metrics";
    const metricItems = [
      [buckets.labels?.compute || "Compute", buckets.compute],
      [buckets.labels?.storage || "Storage", buckets.storage],
      [buckets.labels?.egress || "Egress", buckets.egress],
      [buckets.labels?.licenses || "Licenses", buckets.licenses],
    ];
    metricItems.forEach(([label, value]) => {
      const item = document.createElement("div");
      const text = document.createElement("span");
      text.textContent = `${label}: `;
      const amount = document.createElement("strong");
      amount.textContent = formatMonthly(value);
      item.appendChild(text);
      item.appendChild(amount);
      metrics.appendChild(item);
    });
    card.appendChild(header);
    card.appendChild(bar);
    card.appendChild(metrics);
    targetChart.appendChild(card);
  });
  if (pricingFocus === "network") {
    targetNote.textContent =
      "Networking focus breakdown uses on-demand totals (network add-ons, inter/intra VLAN, inter-region transfer, egress).";
    return;
  }
  if (pricingFocus === "storage") {
    targetNote.textContent =
      "Storage focus breakdown uses on-demand totals (storage services + DR replication).";
    return;
  }
  targetNote.textContent =
    "Breakdown uses on-demand totals (compute, storage, egress, licenses).";
}

function renderInsight(data) {
  renderInsightTo(insightChart, insightNote, data);
}

function renderFocusInsight(data, focus) {
  if (focus === "network") {
    renderInsightTo(networkInsightChart, networkInsightNote, data, "network");
    return;
  }
  if (focus === "storage") {
    renderInsightTo(storageInsightChart, storageInsightNote, data, "storage");
  }
}

function formatDateTime(value) {
  if (!value) {
    return "n/a";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "n/a";
  }
  return date.toLocaleString();
}

function getProviderRowsForPanels(data) {
  if (!data) {
    return [];
  }
  const mode = data.input?.mode || "vm";
  const rows = [
    { key: "aws", label: getProviderLabelForMode("aws", mode), provider: data.aws },
    {
      key: "azure",
      label: getProviderLabelForMode("azure", mode),
      provider: data.azure,
    },
    { key: "gcp", label: getProviderLabelForMode("gcp", mode), provider: data.gcp },
  ];
  if (data.private?.enabled && data.input?.pricingFocus === "all") {
    const privateName = getPrimaryPrivateProvider()?.name;
    rows.push({
      key: "private",
      label: privateName ? `Private (${privateName})` : "Private",
      provider: data.private,
    });
  }
  return rows;
}

function getProviderOnDemandTotals(provider) {
  return provider?.pricingTiers?.onDemand?.totals || provider?.totals || null;
}

function getNormalizationInfo(data, providerKey) {
  const input = data?.input || {};
  const focus = input.pricingFocus || "all";
  if (focus === "network") {
    const providerPrefix =
      providerKey === "aws" ? "aws" : providerKey === "azure" ? "azure" : "gcp";
    const totalTb =
      (Number.isFinite(input[`${providerPrefix}NetworkVpcDataTb`])
        ? input[`${providerPrefix}NetworkVpcDataTb`]
        : 0) +
      (Number.isFinite(input[`${providerPrefix}NetworkGatewayDataTb`])
        ? input[`${providerPrefix}NetworkGatewayDataTb`]
        : 0) +
      (Number.isFinite(input[`${providerPrefix}NetworkLoadBalancerDataTb`])
        ? input[`${providerPrefix}NetworkLoadBalancerDataTb`]
        : 0) +
      (Number.isFinite(input.interVlanTb) ? input.interVlanTb : 0) +
      (Number.isFinite(input.intraVlanTb) ? input.intraVlanTb : 0) +
      (Number.isFinite(input.interRegionTb) ? input.interRegionTb : 0) +
      (Number.isFinite(input.egressTb) ? input.egressTb : 0);
    if (totalTb > 0) {
      return { divisor: totalTb, label: "TB transfer" };
    }
    return { divisor: null, label: "TB transfer" };
  }
  if (focus === "storage") {
    const providerPrefix =
      providerKey === "aws" ? "aws" : providerKey === "azure" ? "azure" : "gcp";
    const objectTb = Number.isFinite(input[`${providerPrefix}StorageObjectTb`])
      ? input[`${providerPrefix}StorageObjectTb`]
      : 0;
    const fileTb = Number.isFinite(input[`${providerPrefix}StorageFileTb`])
      ? input[`${providerPrefix}StorageFileTb`]
      : 0;
    const tableTb = Number.isFinite(input[`${providerPrefix}StorageTableTb`])
      ? input[`${providerPrefix}StorageTableTb`]
      : 0;
    const queueTb = Number.isFinite(input[`${providerPrefix}StorageQueueTb`])
      ? input[`${providerPrefix}StorageQueueTb`]
      : 0;
    const drDeltaTb =
      input[`${providerPrefix}StorageDrEnabled`] &&
      Number.isFinite(input[`${providerPrefix}StorageDrDeltaTb`])
        ? input[`${providerPrefix}StorageDrDeltaTb`]
        : 0;
    const totalTb = objectTb + fileTb + tableTb + queueTb + drDeltaTb;
    if (totalTb > 0) {
      return { divisor: totalTb, label: "TB storage" };
    }
    return { divisor: null, label: "TB storage" };
  }
  const count = Number.isFinite(input.vmCount) ? input.vmCount : 0;
  const unitLabel = input.mode === "k8s" ? "node" : "VM";
  if (count > 0) {
    return { divisor: count, label: unitLabel };
  }
  return { divisor: null, label: unitLabel };
}

function renderDataQualityPanel(data) {
  if (!qualityMeta || !qualityList) {
    return;
  }
  if (!data) {
    qualityMeta.textContent = "Waiting for pricing...";
    qualityList.innerHTML = "";
    return;
  }
  const cacheMeta = data.notes?.cacheMeta || {};
  const refreshStatus = cacheMeta.refreshStatus || "unknown";
  qualityMeta.textContent = `Generated ${formatDateTime(
    cacheMeta.generatedAt
  )} | refresh: ${refreshStatus}`;
  const lines = [];
  if (data.notes?.cacheWarning) {
    lines.push(data.notes.cacheWarning);
  }
  if (Array.isArray(cacheMeta.staleCaches) && cacheMeta.staleCaches.length) {
    lines.push(`Stale caches: ${cacheMeta.staleCaches.join(", ")}.`);
  }
  const providerRows = getProviderRowsForPanels(data);
  providerRows.forEach(({ label, provider }) => {
    const computeSource = formatSourceDetail(
      provider?.pricingTiers?.onDemand?.source || provider?.source
    );
    const networkSource = summarizeItemSources(provider?.networkAddons?.items || []);
    const storageSourceValues = Object.values(provider?.storageServices?.sources || {});
    const storageSource = storageSourceValues.length
      ? summarizeItemSources(
          storageSourceValues.map((source) => ({ source }))
        )
      : "none";
    lines.push(
      `${label}: compute ${computeSource}, network ${networkSource}, storage ${storageSource}.`
    );
  });
  qualityList.innerHTML = "";
  lines.slice(0, QUALITY_WARNING_LIMIT).forEach((line) => {
    const item = document.createElement("li");
    item.textContent = line;
    qualityList.appendChild(item);
  });
  if (!lines.length) {
    const item = document.createElement("li");
    item.textContent = "No quality warnings for this result set.";
    qualityList.appendChild(item);
  }
}

function renderUnitEconomics(data) {
  if (!unitEconTable || !unitEconNote) {
    return;
  }
  if (!data) {
    unitEconTable.innerHTML = "";
    unitEconNote.textContent = "Per-provider normalized costs.";
    return;
  }
  const rows = getProviderRowsForPanels(data).map(({ key, label, provider }) => {
    const totals = getProviderOnDemandTotals(provider);
    const info = getNormalizationInfo(data, key);
    const total = Number.isFinite(totals?.total) ? totals.total : null;
    const normalized =
      Number.isFinite(total) && Number.isFinite(info.divisor) && info.divisor > 0
        ? total / info.divisor
        : null;
    const computeBase = Number.isFinite(totals?.computeMonthly)
      ? totals.computeMonthly
      : 0;
    const controlPlane = Number.isFinite(totals?.controlPlaneMonthly)
      ? totals.controlPlaneMonthly
      : 0;
    const compute = computeBase + controlPlane;
    const storage = Number.isFinite(totals?.storageMonthly)
      ? totals.storageMonthly
      : 0;
    const backups = Number.isFinite(totals?.backupMonthly)
      ? totals.backupMonthly
      : 0;
    const dr = Number.isFinite(totals?.drMonthly) ? totals.drMonthly : 0;
    const network = Number.isFinite(totals?.networkMonthly)
      ? totals.networkMonthly
      : 0;
    const egress = Number.isFinite(totals?.egressMonthly)
      ? totals.egressMonthly
      : 0;
    const licenses =
      (Number.isFinite(totals?.sqlMonthly) ? totals.sqlMonthly : 0) +
      (Number.isFinite(totals?.windowsLicenseMonthly)
        ? totals.windowsLicenseMonthly
        : 0);
    const trackedTotal =
      compute + storage + backups + dr + network + egress + licenses;
    let other = Number.isFinite(total) ? total - trackedTotal : 0;
    if (Math.abs(other) < 0.01) {
      other = 0;
    }
    const denominator = Number.isFinite(total) && total > 0 ? total : null;
    const share = (value) =>
      denominator ? `${((value / denominator) * 100).toFixed(1)}%` : "n/a";
    return {
      label,
      total,
      normalized,
      unit: info.label,
      computeShare: share(compute),
      storageShare: share(storage),
      backupShare: share(backups),
      drShare: share(dr),
      networkShare: share(network),
      egressShare: share(egress),
      licenseShare: share(licenses),
      otherShare: share(other),
    };
  });
  unitEconTable.innerHTML = `
    <thead>
      <tr>
        <th>Provider</th>
        <th>Total / month</th>
        <th>Normalized</th>
        <th>Compute</th>
        <th>Storage</th>
        <th>Backups</th>
        <th>DR</th>
        <th>Network</th>
        <th>Egress</th>
        <th>Licenses</th>
        <th>Other</th>
      </tr>
    </thead>
    <tbody>
      ${rows
        .map(
          (row) => `
        <tr>
          <td>${row.label}</td>
          <td>${formatMonthly(row.total)}</td>
          <td>${
            Number.isFinite(row.normalized)
              ? `${formatMoney(row.normalized)}/${row.unit}`
              : "n/a"
          }</td>
          <td>${row.computeShare}</td>
          <td>${row.storageShare}</td>
          <td>${row.backupShare}</td>
          <td>${row.drShare}</td>
          <td>${row.networkShare}</td>
          <td>${row.egressShare}</td>
          <td>${row.licenseShare}</td>
          <td>${row.otherShare}</td>
        </tr>`
        )
        .join("")}
    </tbody>
  `;
  const focus = data.input?.pricingFocus || "all";
  if (focus === "network") {
    unitEconNote.textContent =
      "Normalized by per-provider network TB transfer input.";
    return;
  }
  if (focus === "storage") {
    unitEconNote.textContent =
      "Normalized by per-provider storage + replication TB input.";
    return;
  }
  unitEconNote.textContent =
    data.input?.mode === "k8s"
      ? "Normalized per node count. Shares include backups/DR and reconcile with an Other residual."
      : "Normalized per VM count. Shares include backups/DR and reconcile with an Other residual.";
}

function buildRecommendations(data) {
  const focus = data?.input?.pricingFocus || "all";
  const providerFilter = recommendProviderFilter?.value || "all";
  const parsedLimit = Number.parseInt(recommendLimitInput?.value || "3", 10);
  const topN = Number.isFinite(parsedLimit)
    ? Math.min(10, Math.max(1, parsedLimit))
    : 3;
  const providerRows = getProviderRowsForPanels(data).filter(
    ({ key }) =>
      key !== "private" &&
      (providerFilter === "all" || providerFilter === key)
  );
  const items = [];
  providerRows.forEach(({ key, label, provider }) => {
    const onDemand = provider?.pricingTiers?.onDemand?.totals?.total;
    const reserved1 = provider?.pricingTiers?.reserved1yr?.totals?.total;
    const reserved3 = provider?.pricingTiers?.reserved3yr?.totals?.total;
    if (
      focus === "all" &&
      Number.isFinite(onDemand) &&
      Number.isFinite(reserved1) &&
      reserved1 < onDemand
    ) {
      items.push({
        key: `${key}-1y`,
        title: `${label}: use 1-year commitment`,
        impact: onDemand - reserved1,
        detail: `Estimated monthly savings ${formatMoney(onDemand - reserved1)}.`,
      });
    }
    if (
      focus === "all" &&
      Number.isFinite(onDemand) &&
      Number.isFinite(reserved3) &&
      reserved3 < onDemand
    ) {
      items.push({
        key: `${key}-3y`,
        title: `${label}: use 3-year commitment`,
        impact: onDemand - reserved3,
        detail: `Estimated monthly savings ${formatMoney(onDemand - reserved3)}.`,
      });
    }
    const totals = getProviderOnDemandTotals(provider);
    if (!totals || !Number.isFinite(totals.total) || totals.total <= 0) {
      return;
    }
    if (focus === "network") {
      const transfer =
        (totals.interVlanMonthly || 0) +
        (totals.intraVlanMonthly || 0) +
        (totals.interRegionMonthly || 0) +
        (totals.egressMonthly || 0);
      if (transfer > 0 && transfer / totals.total >= 0.3) {
        items.push({
          key: `${key}-network-transfer`,
          title: `${label}: optimize transfer-heavy traffic`,
          impact: transfer * 0.15,
          detail:
            "Inter/intra/inter-region + egress represent a large share of networking spend.",
        });
      }
      return;
    }
    if (focus === "storage") {
      const replication = totals.egressMonthly || 0;
      if (replication > 0 && replication / totals.total >= 0.25) {
        items.push({
          key: `${key}-storage-repl`,
          title: `${label}: tune DR delta replication`,
          impact: replication * 0.2,
          detail:
            "Replication delta is a material part of storage total; reducing delta volume can lower spend.",
        });
      }
      return;
    }
    const storageAndBackup =
      (totals.storageMonthly || 0) + (totals.backupMonthly || 0);
    if (storageAndBackup > 0 && storageAndBackup / totals.total >= 0.35) {
      items.push({
        key: `${key}-storage-tier`,
        title: `${label}: validate storage tier/perf settings`,
        impact: storageAndBackup * 0.12,
        detail:
          "Storage and backups are a major cost driver; validate required performance profile.",
      });
    }
    const egressAndInterRegion =
      (totals.egressMonthly || 0) + (totals.interRegionMonthly || 0);
    if (egressAndInterRegion > 0 && egressAndInterRegion / totals.total >= 0.2) {
      items.push({
        key: `${key}-egress`,
        title: `${label}: reduce inter-region/egress traffic`,
        impact: egressAndInterRegion * 0.15,
        detail:
          "Traffic costs are significant; localize traffic paths where possible.",
      });
    }
  });
  if (providerRows.length > 1) {
    const priced = providerRows
      .map(({ label, provider }) => ({
        label,
        total: provider?.pricingTiers?.onDemand?.totals?.total,
      }))
      .filter((entry) => Number.isFinite(entry.total));
    priced.sort((a, b) => a.total - b.total);
    if (priced.length > 1) {
      const cheapest = priced[0];
      priced.slice(1).forEach((entry) => {
        items.push({
          key: `provider-delta-${entry.label}`,
          title: `Compare ${entry.label} against ${cheapest.label}`,
          impact: entry.total - cheapest.total,
          detail: `${entry.label} is ${formatMoney(
            entry.total - cheapest.total
          )}/mo above the current lowest provider.`,
        });
      });
    }
  }
  items.sort((a, b) => b.impact - a.impact);
  return items.slice(0, topN);
}

function renderRecommendations(data) {
  if (!recommendList || !recommendNote) {
    return;
  }
  recommendList.innerHTML = "";
  if (!data) {
    recommendNote.textContent = "Run pricing first to generate recommendations.";
    return;
  }
  const recommendations = buildRecommendations(data);
  if (!recommendations.length) {
    recommendNote.textContent =
      "No high-confidence recommendations for the current filter.";
    return;
  }
  recommendNote.textContent = `Showing ${recommendations.length} recommendation(s).`;
  recommendations.forEach((item) => {
    const card = document.createElement("article");
    card.className = "recommend-item";
    const title = document.createElement("h5");
    title.textContent = item.title;
    const detail = document.createElement("p");
    detail.className = "meta";
    detail.textContent = item.detail;
    const impact = document.createElement("strong");
    impact.textContent = `Potential impact: ${formatMoney(item.impact)}/mo`;
    card.appendChild(title);
    card.appendChild(detail);
    card.appendChild(impact);
    recommendList.appendChild(card);
  });
}

function clampPercent(value) {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.min(Math.max(value, 0), 100);
}

function getCommitDiscount(providerKey) {
  const input = commitDiscountInputs[providerKey];
  const raw = Number.parseFloat(input?.value);
  const percent = clampPercent(raw);
  if (input && Number.isFinite(percent)) {
    input.value = percent.toString();
  }
  return percent;
}

function setCommitField(field, value) {
  if (!field) {
    return;
  }
  field.textContent = value;
}

function renderCommit(data) {
  if (!commitPanel || !commitNote) {
    return;
  }
  if (!data) {
    commitNote.textContent =
      "Run a comparison to generate cloud commit totals.";
    return;
  }
  const mode = data.input?.mode || "vm";
  const providerKeys = ["aws", "azure", "gcp"];
  const summaries = providerKeys.map((providerKey) => {
    const provider = data[providerKey];
    const totals =
      provider?.pricingTiers?.onDemand?.totals || provider?.totals;
    const onDemandTotal = Number.isFinite(totals?.total) ? totals.total : null;
    const computeMonthly = Number.isFinite(totals?.computeMonthly)
      ? totals.computeMonthly
      : null;
    const discountPercent = getCommitDiscount(providerKey);
    const discountAmount =
      Number.isFinite(computeMonthly) && Number.isFinite(discountPercent)
        ? (computeMonthly * discountPercent) / 100
        : null;
    const committedTotal =
      Number.isFinite(onDemandTotal) && Number.isFinite(discountAmount)
        ? onDemandTotal - discountAmount
        : null;
    return {
      providerKey,
      provider,
      totals,
      onDemandTotal,
      computeMonthly,
      discountAmount,
      committedTotal,
    };
  });
  const maxTotal = summaries.reduce((max, item) => {
    if (Number.isFinite(item.onDemandTotal)) {
      return Math.max(max, item.onDemandTotal);
    }
    return max;
  }, 0);

  summaries.forEach((summary) => {
    const {
      providerKey,
      totals,
      onDemandTotal,
      discountAmount,
      committedTotal,
    } = summary;

    const fields = commitFields[providerKey];
    COMMIT_COMPONENTS.forEach((component) => {
      const rawValue = Number.isFinite(totals?.[component.field])
        ? totals[component.field]
        : null;
      const committedValue =
        component.key === "compute" &&
        Number.isFinite(rawValue) &&
        Number.isFinite(discountAmount)
          ? rawValue - discountAmount
          : rawValue;
      setCommitField(
        fields?.base?.[component.key],
        formatMonthly(rawValue)
      );
      setCommitField(
        fields?.commit?.[component.key],
        formatMonthly(committedValue)
      );
    });
    setCommitField(fields?.base?.total, formatMonthly(onDemandTotal));
    setCommitField(
      fields?.commit?.savings,
      formatMonthly(
        Number.isFinite(discountAmount) ? -discountAmount : null
      )
    );
    setCommitField(fields?.commit?.total, formatMonthly(committedTotal));
    if (fields?.note) {
      const region = data.region?.[providerKey]?.location || "";
      const label = getProviderLabelForMode(providerKey, mode);
      fields.note.textContent = region
        ? `${label} ${region}. Discount applies to compute only.`
        : "Discount applies to compute only.";
    }
    const insightFields = commitInsightFields[providerKey];
    if (insightFields) {
      const savings =
        Number.isFinite(onDemandTotal) && Number.isFinite(committedTotal)
          ? onDemandTotal - committedTotal
          : null;
      const baseWidth =
        Number.isFinite(onDemandTotal) && maxTotal > 0
          ? (onDemandTotal / maxTotal) * 100
          : 0;
      const commitWidth =
        Number.isFinite(committedTotal) && maxTotal > 0
          ? (committedTotal / maxTotal) * 100
          : 0;
      if (insightFields.baseBar) {
        insightFields.baseBar.style.width = `${Math.max(0, baseWidth)}%`;
      }
      if (insightFields.commitBar) {
        insightFields.commitBar.style.width = `${Math.max(0, commitWidth)}%`;
      }
      setCommitField(
        insightFields.base,
        `On-demand ${formatMonthly(onDemandTotal)}`
      );
      setCommitField(
        insightFields.commit,
        `Committed ${formatMonthly(committedTotal)}`
      );
      setCommitField(
        insightFields.save,
        `Savings ${formatMonthly(savings)}`
      );
    }
  });
  commitNote.textContent =
    "Discounts apply to compute only. Storage, egress, network, SQL, and DR remain unchanged. Savings are visualized below.";
}

function setAuthNote(message, isError = false) {
  if (!authNote) {
    return;
  }
  authNote.textContent = message || "";
  authNote.classList.toggle("negative", isError);
}

function setAdminNote(message, isError = false) {
  if (!adminNote) {
    return;
  }
  adminNote.textContent = message || "";
  adminNote.classList.toggle("negative", isError);
}

function setDataTransferNote(message, isError = false) {
  if (!dataTransferNote) {
    return;
  }
  dataTransferNote.textContent = message || "";
  dataTransferNote.classList.toggle("negative", isError);
}

function setDataVersionNote(message, isError = false) {
  if (!dataVersionNote) {
    return;
  }
  dataVersionNote.textContent = message || "";
  dataVersionNote.classList.toggle("negative", isError);
}

function getDataHistoryUserKey(user = authSession?.user || null) {
  const username = String(user?.username || "")
    .trim()
    .toLowerCase();
  return username || "__guest__";
}

function getDataHistoryStorageKey(user = authSession?.user || null) {
  return `${DATA_HISTORY_STORAGE_PREFIX}:${getDataHistoryUserKey(user)}`;
}

function normalizeDataHistoryEntry(entry) {
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
    return null;
  }
  const id = String(entry.id || "").trim();
  const savedAt = String(entry.savedAt || "").trim();
  const state = normalizeSyncedStatePayload(entry.state);
  if (!id || !savedAt || !state) {
    return null;
  }
  return { id, savedAt, state };
}

function loadDataHistory(user = authSession?.user || null) {
  if (isAuthenticatedUserSession()) {
    const cacheKey = getDataHistoryUserKey(user);
    const cached = authDataHistoryCache[cacheKey];
    if (!Array.isArray(cached)) {
      return [];
    }
    return cached
      .map((entry) => normalizeDataHistoryEntry(entry))
      .filter(Boolean);
  }

  const raw = readPricingStorageValue(getDataHistoryStorageKey(user));
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed
      .map((entry) => normalizeDataHistoryEntry(entry))
      .filter(Boolean);
  } catch (_error) {
    return [];
  }
}

function persistDataHistory(history, user = authSession?.user || null) {
  const key = getDataHistoryStorageKey(user);
  const cacheKey = getDataHistoryUserKey(user);
  let entries = Array.isArray(history)
    ? history
        .map((entry) => normalizeDataHistoryEntry(entry))
        .filter(Boolean)
        .slice(-DATA_HISTORY_MAX_ENTRIES)
    : [];

  if (isAuthenticatedUserSession()) {
    authDataHistoryCache[cacheKey] = entries;
    return entries;
  }

  while (entries.length >= 0) {
    const saved = writePricingStorageValue(key, JSON.stringify(entries), {
      sync: false,
    });
    if (saved) {
      return entries;
    }
    if (!entries.length) {
      break;
    }
    entries = entries.slice(1);
  }
  removePricingStorageValue(key, { sync: false });
  return [];
}

function renderDataVersionHistory(user = authSession?.user || null, options = {}) {
  if (!dataVersionSelect) {
    return;
  }
  const selected = String(options.selectedId || dataVersionSelect.value || "");
  const history = loadDataHistory(user);
  dataVersionSelect.innerHTML = "";
  if (!history.length) {
    const empty = document.createElement("option");
    empty.value = "";
    empty.textContent = "No versions available";
    dataVersionSelect.appendChild(empty);
    dataVersionSelect.disabled = true;
    if (dataVersionRollbackButton) {
      dataVersionRollbackButton.disabled = true;
    }
    return;
  }
  history
    .slice()
    .reverse()
    .forEach((entry) => {
      const option = document.createElement("option");
      option.value = entry.id;
      const keyCount = Object.keys(entry.state.localStorage || {}).length;
      option.textContent = `${formatDateTime(entry.savedAt)} (${keyCount} key${
        keyCount === 1 ? "" : "s"
      })`;
      dataVersionSelect.appendChild(option);
    });
  const hasSelected = history.some((entry) => entry.id === selected);
  dataVersionSelect.value = hasSelected ? selected : dataVersionSelect.options[0].value;
  dataVersionSelect.disabled = false;
  if (dataVersionRollbackButton) {
    dataVersionRollbackButton.disabled = false;
  }
}

function recordDataHistoryVersion(stateRaw = null, options = {}) {
  const user = options.user || authSession?.user || null;
  const state = cloneSyncedStatePayload(stateRaw || buildSyncedStatePayload());
  if (!state) {
    return false;
  }
  const history = loadDataHistory(user);
  const latest = history.length ? history[history.length - 1] : null;
  if (latest?.state && areSyncedStatePayloadsEqual(latest.state, state)) {
    return false;
  }
  const nextEntry = {
    id: `ver-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`,
    savedAt: new Date().toISOString(),
    state,
  };
  const nextHistory = persistDataHistory([...history, nextEntry], user);
  if (activePanel === "data-transfer") {
    renderDataVersionHistory(user, { selectedId: nextEntry.id });
  }
  return nextHistory.length > 0;
}

function refreshDataVersionHistory(options = {}) {
  const user = options.user || authSession?.user || null;
  renderDataVersionHistory(user, options);
  const versionCount = loadDataHistory(user).length;
  setDataVersionNote(
    versionCount
      ? `Loaded ${versionCount} version${versionCount === 1 ? "" : "s"}.`
      : "No versions available yet."
  );
}

async function handleDataVersionRollback() {
  const selectedId = String(dataVersionSelect?.value || "").trim();
  if (!selectedId) {
    setDataVersionNote("Select a version to roll back.", true);
    return;
  }
  const history = loadDataHistory(authSession?.user || null);
  const entry = history.find((item) => item.id === selectedId);
  if (!entry?.state) {
    setDataVersionNote("Selected version was not found.", true);
    return;
  }
  const approved = window.confirm(
    "Roll back to this version? Current saved workspace data will be replaced."
  );
  if (!approved) {
    setDataVersionNote("Rollback canceled.");
    return;
  }
  try {
    applySyncedStatePayload(entry.state);
    reloadPersistentStateFromLocalStorage();
    setPendingGuestImportState(null);
    setAuthUi(authSession?.user || null, {
      loginEnabled: authSession?.loginEnabled !== false,
    });
    if (authSession?.authenticated) {
      await pushSyncedStateToServer(entry.state);
    }
    recordDataHistoryVersion(entry.state);
    renderDataVersionHistory(authSession?.user || null);
    setDataVersionNote(`Rolled back to ${formatDateTime(entry.savedAt)}.`);
    if (
      activePanel !== "private" &&
      activePanel !== "scenarios" &&
      activePanel !== "saved" &&
      activePanel !== "vsax-compare" &&
      activePanel !== "billing" &&
      activePanel !== "data-transfer" &&
      activePanel !== "admin"
    ) {
      await handleCompare();
    }
  } catch (error) {
    setDataVersionNote(error?.message || "Rollback failed.", true);
  }
}

function renderDataTransferScope(user = null) {
  if (!dataTransferScope) {
    return;
  }
  const username = String(user?.username || "").trim();
  if (username) {
    dataTransferScope.textContent = `Signed in as ${username}. Imported data is also synced to your DB profile.`;
    return;
  }
  dataTransferScope.textContent =
    "Guest mode: data transfer works browser-locally for this device/session.";
  if (activePanel === "data-transfer") {
    renderDataVersionHistory(user || null);
  }
}

function renderAdminUsersTable() {
  if (!adminUsersTable) {
    return;
  }
  if (!Array.isArray(adminUsers) || !adminUsers.length) {
    adminUsersTable.innerHTML =
      '<p class="billing-empty">No users available. Add a user to get started.</p>';
    return;
  }
  const adminCount = adminUsers.reduce(
    (count, user) => count + (Boolean(user?.isAdmin) ? 1 : 0),
    0
  );
  const rows = adminUsers
    .map((user) => {
      const username = String(user?.username || "").trim();
      if (!username) {
        return "";
      }
      const isAdmin = Boolean(user?.isAdmin);
      const roleClass = isAdmin ? "" : " user";
      const roleValue = isAdmin ? "user" : "admin";
      const roleLabel = isAdmin ? "Set regular" : "Set admin";
      const roleAction =
        isAdmin && adminCount <= 1
          ? "<span>Last admin</span>"
          : `<button type="button" class="table-action" data-admin-role="${encodeURIComponent(
              username
            )}" data-admin-role-value="${roleValue}">${roleLabel}</button>`;
      const deleteAction = isAdmin
        ? "<span>Protected</span>"
        : `<button type="button" class="table-action" data-admin-delete="${encodeURIComponent(
            username
          )}">Remove</button>`;
      return `<tr>
        <td>${escapeMarkup(username)}</td>
        <td><span class="admin-role${roleClass}">${isAdmin ? "Admin" : "Regular"}</span></td>
        <td>${escapeMarkup(formatDateTime(user?.updatedAt))}</td>
        <td>${escapeMarkup(formatDateTime(user?.createdAt))}</td>
        <td><div class="table-actions">${roleAction}${deleteAction}</div></td>
      </tr>`;
    })
    .filter(Boolean)
    .join("");
  adminUsersTable.innerHTML = `<table>
    <thead>
      <tr>
        <th>Username</th>
        <th>Role</th>
        <th>Updated</th>
        <th>Created</th>
        <th>Actions</th>
      </tr>
    </thead>
    <tbody>${rows}</tbody>
  </table>`;
}

function syncAdminUserSelector() {
  if (!adminUpdateUsernameSelect) {
    return;
  }
  const selected = String(adminUpdateUsernameSelect.value || "");
  adminUpdateUsernameSelect.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "Select user";
  adminUpdateUsernameSelect.appendChild(placeholder);
  adminUsers.forEach((user) => {
    const username = String(user?.username || "").trim();
    if (!username) {
      return;
    }
    const option = document.createElement("option");
    option.value = username;
    option.textContent = username;
    adminUpdateUsernameSelect.appendChild(option);
  });
  if (
    selected &&
    adminUsers.some((user) => String(user?.username || "").trim() === selected)
  ) {
    adminUpdateUsernameSelect.value = selected;
  }
}

function updateAdminTabVisibility() {
  if (adminModeTab) {
    adminModeTab.classList.toggle("is-hidden", PRICE_EMBEDDED || !isAdminUser);
  }
  if (PRICE_EMBEDDED || !isAdminUser) {
    adminUsers = [];
    renderAdminUsersTable();
    syncAdminUserSelector();
    if (activePanel === "admin") {
      setPanel("vm");
    }
  }
}

async function loadAdminUsers(options = {}) {
  if (PRICE_EMBEDDED || !isAdminUser) {
    return;
  }
  if (!options.silent) {
    setAdminNote("Loading users...");
  }
  try {
    const payload = await requestJson("/api/admin/users");
    adminUsers = Array.isArray(payload?.users) ? payload.users : [];
    renderAdminUsersTable();
    syncAdminUserSelector();
    if (!options.keepNote) {
      setAdminNote(`Loaded ${adminUsers.length} user(s).`);
    }
  } catch (error) {
    setAdminNote(error?.message || "Could not load users.", true);
  }
}

async function handleAdminAddUserSubmit(event) {
  event.preventDefault();
  if (!isAdminUser) {
    return;
  }
  const username = String(adminAddUsernameInput?.value || "").trim();
  const password = String(adminAddPasswordInput?.value || "");
  const role = String(adminAddRoleSelect?.value || "user")
    .trim()
    .toLowerCase();
  const isAdmin = role === "admin";
  if (!username || !password) {
    setAdminNote("Username and password are required.", true);
    return;
  }
  setAdminNote(
    `Adding ${isAdmin ? "admin" : "regular"} user "${username}"...`
  );
  try {
    const payload = await requestJson("/api/admin/users", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password, isAdmin }),
    });
    if (adminAddUserForm) {
      adminAddUserForm.reset();
    }
    await loadAdminUsers({ silent: true, keepNote: true });
    setAdminNote(
      `User "${payload?.user?.username || username}" added as ${
        payload?.user?.isAdmin ? "admin" : "regular"
      }.`
    );
  } catch (error) {
    setAdminNote(error?.message || "Could not add user.", true);
  }
}

async function handleAdminUpdatePasswordSubmit(event) {
  event.preventDefault();
  if (!isAdminUser) {
    return;
  }
  const username = String(adminUpdateUsernameSelect?.value || "").trim();
  const password = String(adminUpdatePasswordInput?.value || "");
  if (!username || !password) {
    setAdminNote("Choose a user and enter a new password.", true);
    return;
  }
  setAdminNote(`Updating password for "${username}"...`);
  try {
    await requestJson(`/api/admin/users/${encodeURIComponent(username)}/password`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ password }),
    });
    if (adminUpdatePasswordInput) {
      adminUpdatePasswordInput.value = "";
    }
    await loadAdminUsers({ silent: true, keepNote: true });
    setAdminNote(`Password updated for "${username}".`);
  } catch (error) {
    setAdminNote(error?.message || "Could not update password.", true);
  }
}

async function handleAdminUpdateRole(usernameRaw, roleRaw) {
  if (!isAdminUser) {
    return;
  }
  const username = String(usernameRaw || "").trim();
  const role = String(roleRaw || "")
    .trim()
    .toLowerCase();
  const isAdmin = role === "admin";
  if (!username || (role !== "admin" && role !== "user")) {
    return;
  }
  setAdminNote(`Updating role for "${username}"...`);
  try {
    const payload = await requestJson(
      `/api/admin/users/${encodeURIComponent(username)}/role`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ isAdmin }),
      }
    );
    const currentUser = String(authSession?.user?.username || "")
      .trim()
      .toLowerCase();
    if (currentUser && currentUser === username.toLowerCase()) {
      if (authSession?.user) {
        authSession.user.isAdmin = Boolean(payload?.user?.isAdmin);
      }
      isAdminUser = Boolean(payload?.user?.isAdmin);
      setAuthUi(authSession?.user || null, { loginEnabled: true });
    }
    if (isAdminUser) {
      await loadAdminUsers({ silent: true, keepNote: true });
    }
    setAdminNote(
      `Role updated for "${username}" (${isAdmin ? "admin" : "regular"}).`
    );
  } catch (error) {
    setAdminNote(error?.message || "Could not update role.", true);
  }
}

async function handleAdminDeleteUser(usernameRaw) {
  if (!isAdminUser) {
    return;
  }
  const username = String(usernameRaw || "").trim();
  if (!username) {
    return;
  }
  const shouldDelete = window.confirm(
    `Remove user "${username}"? This will revoke active sessions for that user.`
  );
  if (!shouldDelete) {
    return;
  }
  setAdminNote(`Removing user "${username}"...`);
  try {
    await requestJson(`/api/admin/users/${encodeURIComponent(username)}`, {
      method: "DELETE",
    });
    await loadAdminUsers({ silent: true, keepNote: true });
    setAdminNote(`User "${username}" removed.`);
  } catch (error) {
    setAdminNote(error?.message || "Could not remove user.", true);
  }
}

function setAuthUi(user, options = {}) {
  const loginEnabled = options.loginEnabled !== false;
  const showGuestLogin = !user && loginEnabled;
  const showGuestImport = Boolean(user?.username && pendingGuestImportState);
  updateAdminTabVisibility();
  renderDataTransferScope(user);
  if (authStatus) {
    if (user?.username) {
      authStatus.textContent = `Signed in as ${user.username}. Profile data is stored in the cloud database (not browser cache).`;
    } else if (loginEnabled) {
      authStatus.textContent =
        "Guest mode: data is stored in browser cache and is not synced to the cloud database.";
    } else {
      authStatus.textContent = "Guest user";
    }
  }
  if (authOpenLoginButton) {
    authOpenLoginButton.classList.toggle("is-hidden", !showGuestLogin);
    authOpenLoginButton.disabled = !loginEnabled;
  }
  if (authImportGuestButton) {
    authImportGuestButton.classList.toggle("is-hidden", !showGuestImport);
  }
  if (authSaveDbButton) {
    const showSaveDb = Boolean(user?.username);
    authSaveDbButton.classList.toggle("is-hidden", !showSaveDb);
    authSaveDbButton.disabled = !showSaveDb || authSyncInFlight;
    authSaveDbButton.textContent = authSyncInFlight ? "Saving..." : "Save to DB";
  }
  if (authLogoutButton) {
    authLogoutButton.classList.toggle("is-hidden", !user?.username);
  }
  if (authForm && user?.username) {
    authForm.classList.add("is-hidden");
  }
}

async function requestJson(url, options = {}) {
  const response = await fetch(resolvePriceApiPath(url), options);
  let payload = null;
  try {
    payload = await response.json();
  } catch (error) {
    payload = null;
  }
  if (!response.ok) {
    throw new Error(
      payload?.error ||
        payload?.message ||
        `Request failed (${response.status}).`
    );
  }
  return payload;
}

function isAuthSyncStorageKey(key) {
  return isPricingSyncedStorageKey(key);
}

function collectAuthSyncedLocalStorage() {
  return collectPricingSyncedStorageState();
}

function normalizeSyncedStatePayload(state) {
  if (!state || typeof state !== "object" || Array.isArray(state)) {
    return null;
  }
  const localState = state.localStorage;
  if (!localState || typeof localState !== "object" || Array.isArray(localState)) {
    return null;
  }
  const normalizedLocalState = {};
  Object.entries(localState).forEach(([key, value]) => {
    if (isAuthSyncStorageKey(key) && typeof value === "string") {
      normalizedLocalState[key] = value;
    }
  });
  return {
    version: AUTH_STATE_VERSION,
    localStorage: normalizedLocalState,
  };
}

function cloneSyncedStatePayload(state) {
  const normalized = normalizeSyncedStatePayload(state);
  if (!normalized) {
    return null;
  }
  return {
    version: normalized.version,
    localStorage: { ...normalized.localStorage },
  };
}

function hasSyncedStateData(state) {
  const normalized = normalizeSyncedStatePayload(state);
  return Boolean(
    normalized && Object.keys(normalized.localStorage).length > 0
  );
}

function areSyncedStatePayloadsEqual(left, right) {
  const normalizedLeft = normalizeSyncedStatePayload(left);
  const normalizedRight = normalizeSyncedStatePayload(right);
  if (!normalizedLeft || !normalizedRight) {
    return false;
  }
  const leftEntries = Object.entries(normalizedLeft.localStorage).sort(
    ([a], [b]) => a.localeCompare(b)
  );
  const rightEntries = Object.entries(normalizedRight.localStorage).sort(
    ([a], [b]) => a.localeCompare(b)
  );
  return JSON.stringify(leftEntries) === JSON.stringify(rightEntries);
}

function setPendingGuestImportState(state) {
  const candidate = cloneSyncedStatePayload(state);
  pendingGuestImportState =
    candidate && Object.keys(candidate.localStorage).length > 0
      ? candidate
      : null;
}

function buildSyncedStatePayload() {
  return {
    version: AUTH_STATE_VERSION,
    localStorage: collectAuthSyncedLocalStorage(),
  };
}

function buildUserDataExportPayload() {
  const state = cloneSyncedStatePayload(buildSyncedStatePayload()) || {
    version: AUTH_STATE_VERSION,
    localStorage: {},
  };
  return {
    schema: "cloud-price-user-data",
    version: 1,
    exportedAt: new Date().toISOString(),
    source: {
      authenticated: Boolean(authSession?.authenticated),
      username: String(authSession?.user?.username || "").trim() || null,
    },
    state,
  };
}

function extractSyncedStateFromTransferPayload(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return null;
  }
  const direct = normalizeSyncedStatePayload(payload);
  if (direct) {
    return direct;
  }
  return normalizeSyncedStatePayload(payload.state);
}

function handleExportUserData() {
  try {
    const payload = buildUserDataExportPayload();
    const keyCount = Object.keys(payload.state?.localStorage || {}).length;
    const blob = new Blob([JSON.stringify(payload, null, 2)], {
      type: "application/json;charset=utf-8;",
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    const username =
      String(payload.source?.username || "")
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_-]+/g, "-") || "guest";
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    link.href = url;
    link.download = `cloud-price-user-data-${username}-${stamp}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    setDataTransferNote(`Exported ${keyCount} data key(s).`);
  } catch (error) {
    setDataTransferNote(error?.message || "Could not export user data.", true);
  }
}

async function handleImportUserDataFile(event) {
  const input = event?.target;
  const file = input?.files?.[0];
  if (!file) {
    return;
  }
  setDataTransferNote(`Importing "${file.name}"...`);
  try {
    const raw = await file.text();
    const parsed = JSON.parse(raw);
    const importedState = extractSyncedStateFromTransferPayload(parsed);
    if (!importedState) {
      setDataTransferNote("File is not a valid Cloud Price user data export.", true);
      return;
    }
    const currentState = buildSyncedStatePayload();
    const currentKeyCount = Object.keys(currentState.localStorage || {}).length;
    if (currentKeyCount > 0) {
      const shouldImport = window.confirm(
        "Import will replace your current saved workspace data. Continue?"
      );
      if (!shouldImport) {
        setDataTransferNote("Import canceled.");
        return;
      }
    }
    applySyncedStatePayload(importedState);
    reloadPersistentStateFromLocalStorage();
    setPendingGuestImportState(null);
    setAuthUi(authSession?.user || null, {
      loginEnabled: authSession?.loginEnabled !== false,
    });
    if (authSession?.authenticated) {
      await pushSyncedStateToServer(importedState);
      setDataTransferNote("Import complete. Data synced to your DB profile.");
    } else {
      setDataTransferNote("Import complete. Data applied to guest browser storage.");
    }
    recordDataHistoryVersion(importedState);
    renderDataVersionHistory(authSession?.user || null);
    if (
      activePanel !== "private" &&
      activePanel !== "scenarios" &&
      activePanel !== "saved" &&
      activePanel !== "vsax-compare" &&
      activePanel !== "billing" &&
      activePanel !== "data-transfer" &&
      activePanel !== "admin"
    ) {
      await handleCompare();
    }
  } catch (error) {
    setDataTransferNote(error?.message || "Could not import user data.", true);
  } finally {
    if (input) {
      input.value = "";
    }
  }
}

function applySyncedStatePayload(state) {
  const normalized = normalizeSyncedStatePayload(state);
  if (!normalized) {
    return false;
  }
  const localState = normalized.localStorage || {};
  const managedKeys = new Set([
    ...listPricingStorageKeys({ managedOnly: true }),
    ...Object.keys(localState),
  ]);

  managedKeys.forEach((key) => {
    if (!isPricingSyncedStorageKey(key)) {
      return;
    }
    const nextValue = localState[key];
    if (typeof nextValue === "string") {
      if (isAuthenticatedUserSession()) {
        authSyncedStateCache[key] = nextValue;
      } else {
        writePricingStorageValue(key, nextValue, { sync: false });
      }
      return;
    }
    if (isAuthenticatedUserSession()) {
      delete authSyncedStateCache[key];
    } else {
      removePricingStorageValue(key, { sync: false });
    }
  });
  return true;
}

async function fetchSyncedStateFromServer() {
  if (!authSession?.authenticated) {
    return null;
  }
  const payload = await requestJson("/api/user/state");
  return normalizeSyncedStatePayload(payload?.state);
}

async function loadSyncedStateFromServer() {
  const state = await fetchSyncedStateFromServer();
  if (!state) {
    return { state: null, applied: false };
  }
  return {
    state,
    applied: applySyncedStatePayload(state),
  };
}

async function pushSyncedStateToServer(stateOverride = null, options = {}) {
  const suppressErrorNote = options.suppressErrorNote === true;
  if (!authSession?.authenticated || authSyncInFlight) {
    if (authSaveDbButton) {
      authSaveDbButton.disabled = !authSession?.authenticated || authSyncInFlight;
      authSaveDbButton.textContent = authSyncInFlight ? "Saving..." : "Save to DB";
    }
    return false;
  }
  const statePayload =
    cloneSyncedStatePayload(stateOverride) || buildSyncedStatePayload();
  authSyncInFlight = true;
  if (authSaveDbButton) {
    authSaveDbButton.disabled = true;
    authSaveDbButton.textContent = "Saving...";
  }
  try {
    await requestJson("/api/user/state", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        version: AUTH_STATE_VERSION,
        state: statePayload,
      }),
    });
    return true;
  } catch (error) {
    if (!suppressErrorNote) {
      setAuthNote(error?.message || "Could not sync profile data.", true);
    }
    return false;
  } finally {
    authSyncInFlight = false;
    if (authSaveDbButton) {
      const canSave = Boolean(authSession?.authenticated && authSession?.user?.username);
      authSaveDbButton.disabled = !canSave;
      authSaveDbButton.textContent = "Save to DB";
    }
  }
}

function scheduleSyncedStatePush() {
  recordDataHistoryVersion();
  if (!authSession?.authenticated) {
    return;
  }
  if (authSyncTimer) {
    clearTimeout(authSyncTimer);
  }
  authSyncTimer = setTimeout(() => {
    authSyncTimer = null;
    pushSyncedStateToServer();
  }, AUTH_SYNC_DEBOUNCE_MS);
}

async function handleAuthSaveToDb() {
  if (!authSession?.authenticated || !authSession?.user?.username) {
    setAuthNote("Sign in to save profile data to the cloud database.", true);
    return;
  }
  if (authSyncInFlight) {
    setAuthNote("Profile sync already in progress...");
    return;
  }
  if (authSyncTimer) {
    clearTimeout(authSyncTimer);
    authSyncTimer = null;
  }
  const currentState = buildSyncedStatePayload();
  setAuthNote("Saving profile data to cloud database...");
  const saved = await pushSyncedStateToServer(currentState, {
    suppressErrorNote: true,
  });
  if (saved) {
    setAuthNote("Profile data saved to cloud database.");
    return;
  }
  setAuthNote("Could not save profile data to cloud database.", true);
}

function reloadPersistentStateFromLocalStorage() {
  loadVsaxComparePreferences();
  populateVsaxCompareRegionOptions();
  if (vsaxComparePricingProviderSelect instanceof HTMLSelectElement) {
    vsaxComparePricingProviderSelect.value =
      vsaxCompareState.pricingProvider || "api";
  }
  scenarioStore = loadScenarioStore();
  billingImportStore = loadBillingImportStore();
  billingTagStore = loadBillingTagsStore();
  billingDetailTagStore = loadBillingDetailTagsStore();
  savedCompareScenarioSelections = loadSavedCompareSelections(
    SAVED_COMPARE_SCENARIOS_KEY
  );
  savedComparePrivateSelections = loadSavedCompareSelections(
    SAVED_COMPARE_PRIVATE_KEY
  );
  renderScenarioList();
  privateProviderStore = loadPrivateProviders();
  privateCompareSelections = loadPrivateCompareSelections();
  renderPrivateProviderCards();
  refreshPrivateProviderVsaxGroupOptions();
  renderSavedCompareSelectors();
  setBillingProvider(currentBillingProvider || "aws");
  renderVsaxComparePanel(vsaxCompareState.data);
}

async function initializeAuthSession() {
  authSyncedStateCache = {};
  authDataHistoryCache = {};
  setPendingGuestImportState(null);
  isAdminUser = false;
  setAuthUi(null);
  try {
    const me = await requestJson("/api/auth/me");
    authSession = me;
    if (me?.authenticated && me.user?.username) {
      isAdminUser = Boolean(me.user?.isAdmin);
      setAuthUi(me.user, { loginEnabled: true });
      const { applied } = await loadSyncedStateFromServer();
      if (!applied) {
        const legacyLocalState = normalizeSyncedStatePayload({
          version: AUTH_STATE_VERSION,
          localStorage: collectPricingSyncedStorageState({ forceLocal: true }),
        });
        if (hasSyncedStateData(legacyLocalState)) {
          applySyncedStatePayload(legacyLocalState);
          reloadPersistentStateFromLocalStorage();
          await pushSyncedStateToServer(legacyLocalState, {
            suppressErrorNote: true,
          });
        } else {
          scheduleSyncedStatePush();
        }
      } else {
        reloadPersistentStateFromLocalStorage();
      }
      clearManagedPricingLocalStorage();
      recordDataHistoryVersion(null, { user: me.user });
      if (!PRICE_EMBEDDED && activePanel === "admin") {
        loadAdminUsers({ silent: true });
      }
      return;
    }
    isAdminUser = false;
    setAuthUi(null, { loginEnabled: me?.loginEnabled !== false });
  } catch (error) {
    authSession = null;
    isAdminUser = false;
    setAuthUi(null, { loginEnabled: false });
    setAuthNote(
      error?.message || "Auth service unavailable. Continuing in guest mode.",
      true
    );
  }
}

async function handleAuthLoginSubmit(event) {
  event.preventDefault();
  const username = String(authUsernameInput?.value || "").trim();
  const password = String(authPasswordInput?.value || "");
  const guestSnapshot = buildSyncedStatePayload();
  const guestHadData = hasSyncedStateData(guestSnapshot);
  if (!username || !password) {
    setAuthNote("Enter username and password.", true);
    return;
  }
  setAuthNote("Signing in...");
  try {
    const loginResult = await requestJson("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
    authSession = {
      authenticated: true,
      user: loginResult?.user || { username },
      loginEnabled: true,
    };
    isAdminUser = Boolean(authSession.user?.isAdmin);
    setPendingGuestImportState(null);
    setAuthUi(authSession.user, { loginEnabled: true });
    const { state: serverState, applied } = await loadSyncedStateFromServer();
    const serverHadData = hasSyncedStateData(serverState);
    if (applied) {
      reloadPersistentStateFromLocalStorage();
    } else if (!serverHadData && !guestHadData) {
      scheduleSyncedStatePush();
    }
    if (guestHadData && !serverHadData) {
      applySyncedStatePayload(guestSnapshot);
      reloadPersistentStateFromLocalStorage();
      await pushSyncedStateToServer(guestSnapshot);
      recordDataHistoryVersion(guestSnapshot, { user: authSession.user });
      setAuthNote(
        `Signed in as ${authSession.user.username}. Guest data saved to your DB profile.`
      );
    } else if (
      guestHadData &&
      serverHadData &&
      !areSyncedStatePayloadsEqual(guestSnapshot, serverState)
    ) {
      setPendingGuestImportState(guestSnapshot);
      setAuthUi(authSession.user, { loginEnabled: true });
      setAuthNote(
        `Signed in as ${authSession.user.username}. Server profile loaded. Click "Import Guest Data" to save your guest work to DB.`
      );
    } else {
      recordDataHistoryVersion(null, { user: authSession.user });
      setAuthNote(`Signed in as ${authSession.user.username}.`);
    }
    clearManagedPricingLocalStorage();
    if (authPasswordInput) {
      authPasswordInput.value = "";
    }
    await handleCompare();
  } catch (error) {
    setAuthNote(error?.message || "Login failed.", true);
  }
}

async function handleImportGuestState() {
  if (!authSession?.authenticated || !pendingGuestImportState) {
    return;
  }
  setAuthNote("Importing guest data...");
  try {
    const nextState = cloneSyncedStatePayload(pendingGuestImportState);
    if (!nextState) {
      setPendingGuestImportState(null);
      setAuthUi(authSession.user, { loginEnabled: true });
      setAuthNote("No guest data found to import.");
      return;
    }
    applySyncedStatePayload(nextState);
    reloadPersistentStateFromLocalStorage();
    await pushSyncedStateToServer(nextState);
    recordDataHistoryVersion(nextState, { user: authSession.user });
    setPendingGuestImportState(null);
    setAuthUi(authSession.user, { loginEnabled: true });
    setAuthNote("Guest data imported and saved to your DB profile.");
    await handleCompare();
  } catch (error) {
    setAuthNote(error?.message || "Guest import failed.", true);
  }
}

async function handleAuthLogout() {
  setAuthNote("Signing out...");
  try {
    await requestJson("/api/auth/logout", { method: "POST" });
    authSyncedStateCache = {};
    authDataHistoryCache = {};
    setPendingGuestImportState(null);
    isAdminUser = false;
    authSession = { authenticated: false, user: null, loginEnabled: true };
    setAuthUi(null, { loginEnabled: true });
    if (authForm) {
      authForm.classList.add("is-hidden");
    }
    setAuthNote("Signed out. Guest mode remains browser-local only.");
  } catch (error) {
    setAuthNote(error?.message || "Logout failed.", true);
  }
}

function loadScenarioStore() {
  if (!scenarioList) {
    return [];
  }
  try {
    const raw = readPricingStorageValue(SCENARIO_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    return [];
  }
}

function persistScenarioStore(list) {
  if (!scenarioList) {
    return;
  }
  writePricingStorageValue(SCENARIO_STORAGE_KEY, JSON.stringify(list));
}

function loadPrivateConfig() {
  try {
    const raw = readPricingStorageValue(PRIVATE_STORAGE_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch (error) {
    return null;
  }
}

function persistPrivateConfig(config) {
  writePricingStorageValue(PRIVATE_STORAGE_KEY, JSON.stringify(config));
}

function setPrivateNote(message, isError = false) {
  if (!privateSaveNote) {
    return;
  }
  privateSaveNote.textContent = message;
  privateSaveNote.classList.toggle("negative", isError);
}

function setInlineNote(target, message, isError = false) {
  if (!target) {
    return;
  }
  target.textContent = message;
  target.classList.toggle("negative", isError);
}

function loadPrivateProviders() {
  try {
    const raw = readPricingStorageValue(PRIVATE_PROVIDERS_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    if (parsed && Array.isArray(parsed.providers)) {
      return {
        activeId: parsed.activeId || null,
        providers: parsed.providers,
      };
    }
  } catch (error) {
    // Ignore storage errors.
  }
  const legacy = loadPrivateConfig();
  if (legacy) {
    const id = `prv-${Date.now().toString(36)}-${Math.random()
      .toString(36)
      .slice(2, 7)}`;
    return {
      activeId: id,
      providers: [
        {
          id,
          name: "Private cloud",
          config: legacy,
          updatedAt: new Date().toISOString(),
        },
      ],
    };
  }
  return { activeId: null, providers: [] };
}

function persistPrivateProviders(store) {
  writePricingStorageValue(PRIVATE_PROVIDERS_KEY, JSON.stringify(store));
}

function loadPrivateCompareSelections() {
  try {
    const raw = readPricingStorageValue(PRIVATE_COMPARE_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    return [];
  }
}

function persistPrivateCompareSelections(selections) {
  writePricingStorageValue(
    PRIVATE_COMPARE_KEY,
    JSON.stringify(Array.isArray(selections) ? selections : [])
  );
}

function loadSavedCompareSelections(key) {
  try {
    const raw = readPricingStorageValue(key);
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : null;
  } catch (error) {
    return null;
  }
}

function persistSavedCompareSelections(key, selections) {
  writePricingStorageValue(
    key,
    JSON.stringify(Array.isArray(selections) ? selections : [])
  );
}

function resolveScenarioSelections() {
  const ids = scenarioStore.map((scenario) => scenario.id);
  if (!ids.length) {
    return [];
  }
  let selections = Array.isArray(savedCompareScenarioSelections)
    ? savedCompareScenarioSelections.filter((id) => ids.includes(id))
    : null;
  if (selections === null) {
    selections = [...ids];
  }
  return selections;
}

function resolvePrivateSelections() {
  const ids = privateProviderStore.providers.map((provider) => provider.id);
  if (!ids.length) {
    return [];
  }
  let selections = Array.isArray(savedComparePrivateSelections)
    ? savedComparePrivateSelections.filter((id) => ids.includes(id))
    : null;
  if (selections === null) {
    selections = [...ids];
  }
  return selections;
}

function updateScenarioSelections(nextSelections) {
  savedCompareScenarioSelections = nextSelections;
  persistSavedCompareSelections(
    SAVED_COMPARE_SCENARIOS_KEY,
    savedCompareScenarioSelections
  );
}

function updatePrivateSelections(nextSelections) {
  savedComparePrivateSelections = nextSelections;
  persistSavedCompareSelections(
    SAVED_COMPARE_PRIVATE_KEY,
    savedComparePrivateSelections
  );
}

function syncPrivateCompareSelections() {
  const providerIds = privateProviderStore.providers.map(
    (provider) => provider.id
  );
  let selections = Array.isArray(privateCompareSelections)
    ? [...privateCompareSelections]
    : [];
  selections = selections.filter((id) => providerIds.includes(id));
  providerIds.forEach((id) => {
    if (selections.length >= PRIVATE_COMPARE_SLOTS) {
      return;
    }
    if (!selections.includes(id)) {
      selections.push(id);
    }
  });
  while (selections.length < PRIVATE_COMPARE_SLOTS) {
    selections.push("");
  }
  privateCompareSelections = selections.slice(0, PRIVATE_COMPARE_SLOTS);
  persistPrivateCompareSelections(privateCompareSelections);
  return privateCompareSelections;
}

function getPrivateProviderById(id) {
  return privateProviderStore.providers.find((provider) => provider.id === id);
}

function buildDefaultPrivateConfig() {
  return { ...DEFAULT_PRIVATE_CONFIG };
}

function normalizePrivateConfig(config = {}) {
  const toNumber = (value, fallback) => {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  };
  const normalized = {
    ...DEFAULT_PRIVATE_CONFIG,
    ...config,
  };
  normalized.enabled = Boolean(
    Object.prototype.hasOwnProperty.call(config, "enabled")
      ? config.enabled
      : normalized.enabled
  );
  normalized.vmwareMonthly = toNumber(
    config.vmwareMonthly,
    normalized.vmwareMonthly
  );
  normalized.windowsLicenseMonthly = toNumber(
    config.windowsLicenseMonthly,
    normalized.windowsLicenseMonthly
  );
  normalized.nodeCount = toNumber(config.nodeCount, normalized.nodeCount);
  normalized.nodeCpu = toNumber(config.nodeCpu, normalized.nodeCpu);
  normalized.nodeRam = toNumber(config.nodeRam, normalized.nodeRam);
  normalized.nodeStorageTb = toNumber(
    config.nodeStorageTb,
    normalized.nodeStorageTb
  );
  normalized.vmOsDiskGb = toNumber(config.vmOsDiskGb, normalized.vmOsDiskGb);
  normalized.sanUsableTb = toNumber(
    config.sanUsableTb,
    normalized.sanUsableTb
  );
  normalized.sanTotalMonthly = toNumber(
    config.sanTotalMonthly,
    normalized.sanTotalMonthly
  );
  const resolvedGroupName =
    typeof config.vsaxGroupName === "string"
      ? config.vsaxGroupName.trim()
      : "";
  normalized.vsaxGroupName =
    resolvedGroupName ||
    normalized.vsaxGroupName ||
    DEFAULT_VSAX_GROUP_NAME;
  normalized.networkMonthly = toNumber(
    config.networkMonthly,
    normalized.networkMonthly
  );
  normalized.firewallMonthly = toNumber(
    config.firewallMonthly,
    normalized.firewallMonthly
  );
  normalized.loadBalancerMonthly = toNumber(
    config.loadBalancerMonthly,
    normalized.loadBalancerMonthly
  );
  let storagePerTb = toNumber(config.storagePerTb, normalized.storagePerTb);
  if (
    normalized.sanUsableTb > 0 &&
    normalized.sanTotalMonthly > 0
  ) {
    storagePerTb = normalized.sanTotalMonthly / normalized.sanUsableTb;
  }
  normalized.storagePerTb = storagePerTb;
  return normalized;
}

function getPrimaryPrivateProvider() {
  const selection =
    Array.isArray(privateCompareSelections) && privateCompareSelections.length
      ? privateCompareSelections[0]
      : "";
  if (selection) {
    const provider = getPrivateProviderById(selection);
    if (provider) {
      return provider;
    }
  }
  if (privateProviderStore.activeId) {
    const provider = getPrivateProviderById(privateProviderStore.activeId);
    if (provider) {
      return provider;
    }
  }
  return privateProviderStore.providers[0] || null;
}

function getPrimaryPrivateConfig() {
  const provider = getPrimaryPrivateProvider();
  return normalizePrivateConfig(provider?.config || buildDefaultPrivateConfig());
}

function buildPrivateCardState(card) {
  const fields = {};
  card.querySelectorAll("[data-private-field]").forEach((element) => {
    const key = element.dataset.privateField;
    if (key) {
      fields[key] = element;
    }
  });
  const capacityCounts = {};
  card.querySelectorAll("[data-private-capacity]").forEach((element) => {
    const key = element.dataset.privateCapacity;
    if (key) {
      capacityCounts[key] = element;
    }
  });
  const capacityTotals = {};
  card.querySelectorAll("[data-private-capacity-total]").forEach((element) => {
    const key = element.dataset.privateCapacityTotal;
    if (key) {
      capacityTotals[key] = element;
    }
  });
  const osSizeLabels = card.querySelectorAll(".private-os-size");
  return {
    element: card,
    fields,
    capacityCounts,
    capacityTotals,
    osSizeLabels,
    actions: {
      importVsax: card.querySelector("[data-private-action='import-vsax']"),
      save: card.querySelector("[data-private-action='save']"),
      delete: card.querySelector("[data-private-action='delete']"),
    },
    providerId: card.dataset.providerId || "",
    cardId: card.dataset.cardId || "",
  };
}

function updatePrivateCardTitle(cardState) {
  const nameValue = cardState.fields.name?.value?.trim() || "";
  if (cardState.fields.title) {
    cardState.fields.title.textContent = nameValue || "New provider";
  }
}

function setPrivateVsaxImportNote(cardState, message, isError = false) {
  if (!cardState?.fields?.vsaxImportNote) {
    return;
  }
  cardState.fields.vsaxImportNote.textContent = String(message || "").trim();
  cardState.fields.vsaxImportNote.classList.toggle("negative", Boolean(isError));
}

function buildVsaxGroupOptions() {
  const groups = Array.isArray(vsaxCapacityCatalog?.groups)
    ? vsaxCapacityCatalog.groups
    : [];
  return groups
    .map((entry) => ({
      groupName: String(entry?.groupName || "").trim(),
      deviceCount: Number(entry?.deviceCount || 0),
    }))
    .filter((entry) => entry.groupName);
}

function updatePrivateCardVsaxGroupOptions(cardState, preferredGroupName = "") {
  const select = cardState?.fields?.vsaxGroupName;
  if (!(select instanceof HTMLSelectElement)) {
    return;
  }
  const options = buildVsaxGroupOptions();
  const currentValue = String(
    preferredGroupName || select.value || DEFAULT_VSAX_GROUP_NAME
  ).trim();
  select.innerHTML = "";

  if (!options.length) {
    const fallback = document.createElement("option");
    fallback.value = currentValue || DEFAULT_VSAX_GROUP_NAME;
    fallback.textContent = currentValue || DEFAULT_VSAX_GROUP_NAME;
    select.appendChild(fallback);
    select.value = fallback.value;
    select.disabled = true;
    if (cardState?.actions?.importVsax) {
      cardState.actions.importVsax.disabled = true;
    }
    return;
  }

  options.forEach((entry) => {
    const option = document.createElement("option");
    option.value = entry.groupName;
    option.textContent =
      entry.deviceCount > 0
        ? `${entry.groupName} (${entry.deviceCount})`
        : entry.groupName;
    select.appendChild(option);
  });

  const preferred =
    options.find((entry) => entry.groupName === currentValue)?.groupName ||
    options.find((entry) => entry.groupName === DEFAULT_VSAX_GROUP_NAME)
      ?.groupName ||
    options[0]?.groupName ||
    "";
  select.disabled = false;
  select.value = preferred;
  if (cardState?.actions?.importVsax) {
    cardState.actions.importVsax.disabled = false;
  }
}

function refreshPrivateProviderVsaxGroupOptions() {
  privateProviderCards.forEach((cardState) => {
    const configuredGroup = String(cardState?.fields?.vsaxGroupName?.value || "").trim();
    updatePrivateCardVsaxGroupOptions(cardState, configuredGroup);
  });
}

function formatCapacityValue(value, digits = 2) {
  if (!Number.isFinite(value)) {
    return null;
  }
  const factor = 10 ** Math.max(0, digits);
  return Math.round(value * factor) / factor;
}

function applyVsaxCapacitySummaryToPrivateCard(cardState, summary) {
  const nodeVcpu = Number(summary?.avgVcpuPerDevice);
  const nodeRamGb = Number(summary?.avgMemoryGbPerDevice);
  const nodeStorageTb = Number(summary?.avgStorageTbPerDevice);
  const totalStorageTb = Number(summary?.totalStorageTb);
  const nodeCount = Number(summary?.deviceCount);

  if (cardState?.fields?.nodeCpu && Number.isFinite(nodeVcpu) && nodeVcpu > 0) {
    cardState.fields.nodeCpu.value = Math.max(1, Math.round(nodeVcpu)).toString();
  }
  if (cardState?.fields?.nodeRam && Number.isFinite(nodeRamGb) && nodeRamGb > 0) {
    cardState.fields.nodeRam.value = Math.max(1, Math.round(nodeRamGb)).toString();
  }
  if (
    cardState?.fields?.nodeStorageTb &&
    Number.isFinite(nodeStorageTb) &&
    nodeStorageTb >= 0
  ) {
    const roundedTb = formatCapacityValue(nodeStorageTb, 2);
    cardState.fields.nodeStorageTb.value = String(roundedTb ?? nodeStorageTb);
  }
  if (
    cardState?.fields?.sanUsableTb &&
    Number.isFinite(totalStorageTb) &&
    totalStorageTb >= 0
  ) {
    const roundedTotalTb = formatCapacityValue(totalStorageTb, 2);
    cardState.fields.sanUsableTb.value = String(roundedTotalTb ?? totalStorageTb);
  }
  if (cardState?.fields?.nodeCount && Number.isFinite(nodeCount) && nodeCount > 0) {
    cardState.fields.nodeCount.value = Math.max(2, Math.round(nodeCount)).toString();
  }
  if (cardState?.fields?.vsaxGroupName && summary?.groupName) {
    cardState.fields.vsaxGroupName.value = String(summary.groupName).trim();
  }

  updatePrivateCapacityForCard(cardState);
}

async function loadVsaxCapacityCatalog(options = {}) {
  const silent = options.silent === true;
  try {
    const payload = await requestJson("/api/private/vsax/capacity");
    const groups = Array.isArray(payload?.groups) ? payload.groups : [];
    vsaxCapacityCatalog = {
      loadedAt: payload?.generatedAt || new Date().toISOString(),
      groups: groups,
    };
    refreshPrivateProviderVsaxGroupOptions();
    return vsaxCapacityCatalog;
  } catch (error) {
    if (!silent) {
      setPrivateNote(error?.message || "Could not load VSAx groups.", true);
    }
    return null;
  }
}

async function handleImportVsaxCapacity(cardState) {
  if (cardState?.fields?.vsaxGroupName?.disabled) {
    await loadVsaxCapacityCatalog({ silent: true });
    updatePrivateCardVsaxGroupOptions(
      cardState,
      String(cardState?.fields?.vsaxGroupName?.value || "").trim()
    );
  }
  const groupName = String(cardState?.fields?.vsaxGroupName?.value || "").trim();
  if (!groupName) {
    setPrivateVsaxImportNote(cardState, "Select a VSAx group first.", true);
    return;
  }

  setPrivateVsaxImportNote(cardState, `Importing capacity from ${groupName}...`);
  try {
    const payload = await requestJson(
      `/api/private/vsax/capacity?groupName=${encodeURIComponent(groupName)}`
    );
    const summary = payload?.summary;
    if (!summary || Number(summary?.deviceCount || 0) <= 0) {
      setPrivateVsaxImportNote(
        cardState,
        `No VSAx capacity data found for group "${groupName}".`,
        true
      );
      return;
    }

    applyVsaxCapacitySummaryToPrivateCard(cardState, summary);
    const noteParts = [];
    noteParts.push(`Imported ${Math.round(Number(summary.deviceCount || 0))} device(s) from ${groupName}.`);
    if (Number(summary?.vcpuCoverageCount || 0) <= 0) {
      noteParts.push("vCPU capacity was not provided by VSAx for this group.");
    } else {
      const avgVcpu = formatCapacityValue(Number(summary.avgVcpuPerDevice), 2);
      if (Number.isFinite(avgVcpu)) {
        noteParts.push(`Avg vCPU/device: ${avgVcpu}.`);
      }
    }
    setPrivateVsaxImportNote(cardState, noteParts.join(" "), false);
    setPrivateNote(
      `Imported actual VSAx capacity for "${groupName}". Save provider to persist.`,
      false
    );
  } catch (error) {
    setPrivateVsaxImportNote(
      cardState,
      error?.message || `Could not import VSAx capacity for "${groupName}".`,
      true
    );
  }
}

function applyPrivateConfigToCard(cardState, config, nameValue) {
  const normalized = normalizePrivateConfig(config);
  if (cardState.fields.name && typeof nameValue === "string") {
    cardState.fields.name.value = nameValue;
  }
  if (cardState.fields.enabled) {
    cardState.fields.enabled.checked = normalized.enabled;
  }
  if (cardState.fields.vmwareMonthly) {
    cardState.fields.vmwareMonthly.value = normalized.vmwareMonthly.toString();
  }
  if (cardState.fields.windowsLicenseMonthly) {
    cardState.fields.windowsLicenseMonthly.value =
      normalized.windowsLicenseMonthly.toString();
  }
  if (cardState.fields.nodeCount) {
    cardState.fields.nodeCount.value = normalized.nodeCount.toString();
  }
  if (cardState.fields.storagePerTb) {
    cardState.fields.storagePerTb.value = normalized.storagePerTb.toFixed(4);
  }
  if (cardState.fields.networkMonthly) {
    cardState.fields.networkMonthly.value =
      normalized.networkMonthly.toString();
  }
  if (cardState.fields.firewallMonthly) {
    cardState.fields.firewallMonthly.value =
      normalized.firewallMonthly.toString();
  }
  if (cardState.fields.loadBalancerMonthly) {
    cardState.fields.loadBalancerMonthly.value =
      normalized.loadBalancerMonthly.toString();
  }
  if (cardState.fields.nodeCpu) {
    cardState.fields.nodeCpu.value = normalized.nodeCpu.toString();
  }
  if (cardState.fields.nodeRam) {
    cardState.fields.nodeRam.value = normalized.nodeRam.toString();
  }
  if (cardState.fields.nodeStorageTb) {
    cardState.fields.nodeStorageTb.value =
      normalized.nodeStorageTb.toString();
  }
  if (cardState.fields.vmOsDiskGb) {
    cardState.fields.vmOsDiskGb.value = normalized.vmOsDiskGb.toString();
  }
  if (cardState.fields.sanUsableTb) {
    cardState.fields.sanUsableTb.value =
      normalized.sanUsableTb.toString();
  }
  if (cardState.fields.sanTotalMonthly) {
    cardState.fields.sanTotalMonthly.value =
      normalized.sanTotalMonthly.toString();
  }
  updatePrivateCardVsaxGroupOptions(cardState, normalized.vsaxGroupName);
  setPrivateVsaxImportNote(cardState, "");
  updatePrivateCardTitle(cardState);
}

function readPrivateConfigFromCard(cardState) {
  const toNumber = (value, fallback = 0) => {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  };
  return normalizePrivateConfig({
    enabled: Boolean(cardState.fields.enabled?.checked),
    vmwareMonthly: toNumber(cardState.fields.vmwareMonthly?.value),
    windowsLicenseMonthly: toNumber(
      cardState.fields.windowsLicenseMonthly?.value
    ),
    nodeCount: toNumber(
      cardState.fields.nodeCount?.value,
      DEFAULT_PRIVATE_CONFIG.nodeCount
    ),
    storagePerTb: toNumber(cardState.fields.storagePerTb?.value),
    networkMonthly: toNumber(cardState.fields.networkMonthly?.value),
    firewallMonthly: toNumber(cardState.fields.firewallMonthly?.value),
    loadBalancerMonthly: toNumber(cardState.fields.loadBalancerMonthly?.value),
    nodeCpu: toNumber(
      cardState.fields.nodeCpu?.value,
      DEFAULT_PRIVATE_CONFIG.nodeCpu
    ),
    nodeRam: toNumber(
      cardState.fields.nodeRam?.value,
      DEFAULT_PRIVATE_CONFIG.nodeRam
    ),
    nodeStorageTb: toNumber(cardState.fields.nodeStorageTb?.value),
    vmOsDiskGb: toNumber(
      cardState.fields.vmOsDiskGb?.value,
      DEFAULT_PRIVATE_CONFIG.vmOsDiskGb
    ),
    sanUsableTb: toNumber(cardState.fields.sanUsableTb?.value),
    sanTotalMonthly: toNumber(cardState.fields.sanTotalMonthly?.value),
    vsaxGroupName: String(cardState.fields.vsaxGroupName?.value || "").trim(),
  });
}

function setPrivateCardProviderId(cardState, providerId) {
  if (!providerId) {
    return;
  }
  if (cardState.cardId && privateProviderCards.has(cardState.cardId)) {
    privateProviderCards.delete(cardState.cardId);
  }
  cardState.providerId = providerId;
  cardState.cardId = providerId;
  cardState.element.dataset.providerId = providerId;
  cardState.element.dataset.cardId = providerId;
  privateProviderCards.set(providerId, cardState);
}

function upsertPrivateProvider(id, name, config) {
  const trimmedName = name.trim();
  if (!trimmedName) {
    setPrivateNote("Enter a private provider name.", true);
    return null;
  }
  const existingByName = privateProviderStore.providers.find(
    (item) =>
      item.name.toLowerCase() === trimmedName.toLowerCase() && item.id !== id
  );
  if (existingByName) {
    setPrivateNote("Provider name already exists.", true);
    return null;
  }
  const now = new Date().toISOString();
  let provider = id ? getPrivateProviderById(id) : null;
  if (provider) {
    provider.name = trimmedName;
    provider.config = config;
    provider.updatedAt = now;
  } else {
    const providerId = `prv-${Date.now().toString(36)}-${Math.random()
      .toString(36)
      .slice(2, 7)}`;
    provider = {
      id: providerId,
      name: trimmedName,
      config,
      createdAt: now,
      updatedAt: now,
    };
    privateProviderStore.providers.push(provider);
  }
  privateProviderStore.activeId = provider.id;
  persistPrivateProviders(privateProviderStore);
  syncPrivateCompareSelections();
  renderSavedCompareSelectors();
  return provider;
}

function removePrivateProvider(id) {
  const provider = getPrivateProviderById(id);
  if (!provider) {
    return null;
  }
  privateProviderStore.providers = privateProviderStore.providers.filter(
    (item) => item.id !== id
  );
  if (privateProviderStore.activeId === id) {
    privateProviderStore.activeId =
      privateProviderStore.providers[0]?.id || null;
  }
  persistPrivateProviders(privateProviderStore);
  syncPrivateCompareSelections();
  renderSavedCompareSelectors();
  return provider.name;
}

function handleSavePrivateCard(cardState) {
  updatePrivateCapacityForCard(cardState);
  const config = readPrivateConfigFromCard(cardState);
  const name = cardState.fields.name?.value || "";
  const provider = upsertPrivateProvider(cardState.providerId, name, config);
  if (!provider) {
    return;
  }
  setPrivateCardProviderId(cardState, provider.id);
  applyPrivateConfigToCard(cardState, provider.config, provider.name);
  updatePrivateCapacityForCard(cardState);
  setPrivateNote(
    provider.config.enabled
      ? `Saved "${provider.name}".`
      : `Saved "${provider.name}". Enable private cloud to compare.`,
    false
  );
  if (activePanel !== "private") {
    handleCompare();
  }
}

function handleDeletePrivateCard(cardState) {
  if (cardState.providerId) {
    const confirmed = window.confirm("Delete this private provider?");
    if (!confirmed) {
      return;
    }
    const deletedName = removePrivateProvider(cardState.providerId);
    cardState.element.remove();
    privateProviderCards.delete(cardState.cardId);
    setPrivateNote(
      deletedName
        ? `Deleted "${deletedName}".`
        : "Private provider deleted.",
      false
    );
  } else {
    cardState.element.remove();
    privateProviderCards.delete(cardState.cardId);
    setPrivateNote("Removed unsaved provider.", false);
  }
  if (privateProvidersList && !privateProvidersList.children.length) {
    addPrivateProviderCard();
  }
  if (activePanel !== "private") {
    handleCompare();
  }
}

function createPrivateProviderCard(provider) {
  if (!privateProviderTemplate?.content?.firstElementChild) {
    return null;
  }
  const card = privateProviderTemplate.content.firstElementChild.cloneNode(true);
  const cardState = buildPrivateCardState(card);
  const cardId =
    provider?.id ||
    `draft-${Date.now().toString(36)}-${Math.random()
      .toString(36)
      .slice(2, 7)}`;
  cardState.cardId = cardId;
  card.dataset.cardId = cardId;
  if (provider?.id) {
    cardState.providerId = provider.id;
    card.dataset.providerId = provider.id;
  }
  privateProviderCards.set(cardId, cardState);
  if (provider) {
    applyPrivateConfigToCard(cardState, provider.config, provider.name);
  } else {
    applyPrivateConfigToCard(cardState, buildDefaultPrivateConfig(), "");
  }
  updatePrivateCapacityForCard(cardState);
  updatePrivateCardTitle(cardState);
  if (cardState.fields.name) {
    cardState.fields.name.addEventListener("input", () => {
      updatePrivateCardTitle(cardState);
    });
  }
  if (cardState.fields.vsaxGroupName) {
    cardState.fields.vsaxGroupName.addEventListener("change", () => {
      setPrivateVsaxImportNote(cardState, "");
    });
  }
  const capacityInputs = [
    "nodeCpu",
    "nodeRam",
    "nodeStorageTb",
    "vmOsDiskGb",
    "sanUsableTb",
    "sanTotalMonthly",
    "nodeCount",
  ];
  capacityInputs.forEach((key) => {
    const field = cardState.fields[key];
    if (field) {
      field.addEventListener("input", () => {
        updatePrivateCapacityForCard(cardState);
      });
    }
  });
  if (cardState.actions.save) {
    cardState.actions.save.addEventListener("click", () => {
      handleSavePrivateCard(cardState);
    });
  }
  if (cardState.actions.importVsax) {
    cardState.actions.importVsax.addEventListener("click", async () => {
      await handleImportVsaxCapacity(cardState);
    });
  }
  if (cardState.actions.delete) {
    cardState.actions.delete.addEventListener("click", () => {
      handleDeletePrivateCard(cardState);
    });
  }
  return cardState;
}

function renderPrivateProviderCards() {
  if (!privateProvidersList) {
    return;
  }
  privateProvidersList.innerHTML = "";
  privateProviderCards = new Map();
  if (privateProviderStore.providers.length) {
    privateProviderStore.providers.forEach((provider) => {
      const cardState = createPrivateProviderCard(provider);
      if (cardState) {
        privateProvidersList.appendChild(cardState.element);
      }
    });
  } else {
    const cardState = createPrivateProviderCard();
    if (cardState) {
      privateProvidersList.appendChild(cardState.element);
    }
  }
  syncPrivateCompareSelections();
  renderSavedCompareSelectors();
}

function addPrivateProviderCard() {
  if (!privateProvidersList) {
    return;
  }
  const cardState = createPrivateProviderCard();
  if (!cardState) {
    return;
  }
  privateProvidersList.appendChild(cardState.element);
  updatePrivateCapacityForCard(cardState);
  cardState.element.scrollIntoView({ behavior: "smooth", block: "start" });
}

function getPrivateConfigFromForm() {
  return getPrimaryPrivateConfig();
}

function applyPrivateConfigToPayload(payload, config, options = {}) {
  if (!config) {
    return { ...payload, privateEnabled: false };
  }
  const forceEnable = options.forceEnable !== false;
  const pickNumber = (value, fallback) =>
    Number.isFinite(value) ? value : fallback;
  const vmwareMonthly = pickNumber(
    Number.parseFloat(config.vmwareMonthly),
    payload.privateVmwareMonthly
  );
  const windowsMonthly = pickNumber(
    Number.parseFloat(config.windowsLicenseMonthly),
    payload.privateWindowsLicenseMonthly
  );
  const nodeCount = pickNumber(
    Number.parseFloat(config.nodeCount),
    payload.privateNodeCount
  );
  const storagePerTb = pickNumber(
    Number.parseFloat(config.storagePerTb),
    payload.privateStoragePerTb
  );
  const networkMonthly = pickNumber(
    Number.parseFloat(config.networkMonthly),
    payload.privateNetworkMonthly
  );
  const firewallMonthly = pickNumber(
    Number.parseFloat(config.firewallMonthly),
    payload.privateFirewallMonthly
  );
  const loadBalancerMonthly = pickNumber(
    Number.parseFloat(config.loadBalancerMonthly),
    payload.privateLoadBalancerMonthly
  );
  const nodeCpu = pickNumber(
    Number.parseFloat(config.nodeCpu),
    payload.privateNodeCpu
  );
  const nodeRam = pickNumber(
    Number.parseFloat(config.nodeRam),
    payload.privateNodeRam
  );
  const nodeStorageTb = pickNumber(
    Number.parseFloat(config.nodeStorageTb),
    payload.privateNodeStorageTb
  );
  const vmOsDiskGb = pickNumber(
    Number.parseFloat(config.vmOsDiskGb),
    payload.privateVmOsDiskGb
  );
  const sanUsableTb = pickNumber(
    Number.parseFloat(config.sanUsableTb),
    payload.privateSanUsableTb
  );
  const sanTotalMonthly = pickNumber(
    Number.parseFloat(config.sanTotalMonthly),
    payload.privateSanTotalMonthly
  );
  return {
    ...payload,
    privateEnabled: forceEnable ? true : Boolean(config.enabled),
    privateVmwareMonthly: vmwareMonthly,
    privateWindowsLicenseMonthly: windowsMonthly,
    privateNodeCount: nodeCount,
    privateStoragePerTb: storagePerTb,
    privateNetworkMonthly: networkMonthly,
    privateFirewallMonthly: firewallMonthly,
    privateLoadBalancerMonthly: loadBalancerMonthly,
    privateNodeCpu: nodeCpu,
    privateNodeRam: nodeRam,
    privateNodeStorageTb: nodeStorageTb,
    privateVmOsDiskGb: vmOsDiskGb,
    privateSanUsableTb: sanUsableTb,
    privateSanTotalMonthly: sanTotalMonthly,
  };
}

function fillPrivateProviderSelect(select, selectedId) {
  if (!(select instanceof HTMLSelectElement)) {
    return;
  }
  select.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = privateProviderStore.providers.length
    ? "Select provider"
    : "No providers saved";
  select.appendChild(placeholder);
  privateProviderStore.providers.forEach((provider) => {
    const option = document.createElement("option");
    option.value = provider.id;
    option.textContent = provider.name;
    select.appendChild(option);
  });
  if (selectedId) {
    select.value = selectedId;
  }
}

function createPrivateCompareCard(slotIndex, providerId) {
  if (!privateCompareTemplate?.content?.firstElementChild) {
    return null;
  }
  const card = privateCompareTemplate.content.firstElementChild.cloneNode(true);
  card.dataset.privateSlot = (slotIndex + 1).toString();
  const provider = providerId ? getPrivateProviderById(providerId) : null;
  const title = card.querySelector("[data-field='title']");
  if (title) {
    title.textContent = provider
      ? provider.name
      : `Private ${slotIndex + 1}`;
  }
  const providerSelect = card.querySelector("[data-field='providerSelect']");
  fillPrivateProviderSelect(providerSelect, providerId);
  if (providerSelect) {
    providerSelect.addEventListener("change", () => {
      privateCompareSelections[slotIndex] = providerSelect.value;
      persistPrivateCompareSelections(privateCompareSelections);
      if (currentView === "compare" && currentResultsTab === "pricing") {
        handleCompare();
      }
    });
  }
  return {
    element: card,
    fields: buildProviderFieldsFromCard(card),
    providerId,
    provider,
  };
}

function buildPrivateCompareCards() {
  if (!privateCompareContainer) {
    return [];
  }
  const selections = syncPrivateCompareSelections();
  privateCompareContainer.innerHTML = "";
  return selections
    .map((providerId, index) => {
      const cardState = createPrivateCompareCard(index, providerId);
      if (!cardState) {
        return null;
      }
      privateCompareContainer.appendChild(cardState.element);
      return cardState;
    })
    .filter(Boolean);
}

function setPrivateCompareEmpty(cardState, options) {
  const note =
    privateProviderStore.providers.length > 0
      ? "Select a private provider to compare."
      : "Save a private provider profile to compare.";
  updateProvider(
    cardState.fields,
    {
      status: "manual",
      message: note,
      family: "Private cloud",
      instance: {},
      pricingTiers: {},
    },
    { location: "Private DC" },
    options
  );
}

async function renderPrivateCompareCards(basePayload, baseData) {
  const cards = buildPrivateCompareCards();
  if (!cards.length) {
    return;
  }
  const vmCount = baseData?.input?.vmCount ?? basePayload.vmCount;
  const mode = baseData?.input?.mode ?? basePayload.mode ?? "vm";
  const primaryProviderId = cards[0]?.providerId;
  const primaryProvider =
    primaryProviderId && getPrivateProviderById(primaryProviderId);
  await Promise.all(
    cards.map(async (cardState, index) => {
      const provider = cardState.providerId
        ? getPrivateProviderById(cardState.providerId)
        : null;
      if (!provider) {
        setPrivateCompareEmpty(cardState, {
          showMonthlyRate: false,
          showReservationNote: false,
          vmCount,
          mode,
          providerKey: "private",
          pricingFocus: baseData?.input?.pricingFocus,
          pricingProvider: baseData?.input?.pricingProvider,
        });
        return;
      }
      if (
        index === 0 &&
        primaryProvider &&
        primaryProvider.id === provider.id &&
        baseData?.private
      ) {
        updateProvider(cardState.fields, baseData.private, baseData.region.private, {
          showMonthlyRate: false,
          showReservationNote: false,
          vmCount,
          mode,
          providerKey: "private",
          pricingFocus: baseData?.input?.pricingFocus,
          pricingProvider: baseData?.input?.pricingProvider,
        });
        return;
      }
      const payload = applyPrivateConfigToPayload(basePayload, provider.config, {
        forceEnable: true,
      });
      try {
        const data = await comparePricing(payload);
        updateProvider(cardState.fields, data.private, data.region.private, {
          showMonthlyRate: false,
          showReservationNote: false,
          vmCount,
          mode,
          providerKey: "private",
          pricingFocus: data.input?.pricingFocus,
          pricingProvider: data.input?.pricingProvider,
        });
      } catch (error) {
        updateProvider(
          cardState.fields,
          {
            status: "error",
            message: error?.message || "Private compare failed.",
            family: "Private cloud",
            instance: {},
            pricingTiers: {},
          },
          { location: "Private DC" },
          {
            showMonthlyRate: false,
            showReservationNote: false,
            vmCount,
            mode,
            providerKey: "private",
            pricingFocus: baseData?.input?.pricingFocus,
            pricingProvider: baseData?.input?.pricingProvider,
          }
        );
      }
    })
  );
}

function setScenarioNote(message, isError = false) {
  if (!scenarioNote) {
    return;
  }
  scenarioNote.textContent = message;
  scenarioNote.classList.toggle("negative", isError);
}

function renderScenarioList(selectedId = "") {
  if (!scenarioList) {
    return;
  }
  scenarioList.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = scenarioStore.length
    ? "Select scenario"
    : "No saved scenarios";
  scenarioList.appendChild(placeholder);
  scenarioStore.forEach((scenario) => {
    const option = document.createElement("option");
    option.value = scenario.id;
    option.textContent = scenario.name;
    scenarioList.appendChild(option);
  });
  if (selectedId && scenarioStore.some((scenario) => scenario.id === selectedId)) {
    scenarioList.value = selectedId;
  }
  renderSavedCompareSelectors();
}

function renderSavedCompareSelectors() {
  renderSavedCompareScenarioList();
  renderSavedComparePrivateList();
}

function renderSavedCompareScenarioList() {
  if (!savedCompareScenarioList) {
    return;
  }
  savedCompareScenarioList.innerHTML = "";
  if (!scenarioStore.length) {
    savedCompareScenarioList.textContent = "No saved scenarios.";
    return;
  }
  const selections = resolveScenarioSelections();
  if (Array.isArray(savedCompareScenarioSelections)) {
    updateScenarioSelections(selections);
  }
  scenarioStore.forEach((scenario) => {
    const label = document.createElement("label");
    label.className = "checkbox-field saved-compare-item";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = scenario.id;
    checkbox.checked = selections.includes(scenario.id);
    checkbox.addEventListener("change", () => {
      const nextSelections = Array.isArray(savedCompareScenarioSelections)
        ? [...savedCompareScenarioSelections]
        : resolveScenarioSelections();
      if (checkbox.checked) {
        if (!nextSelections.includes(scenario.id)) {
          nextSelections.push(scenario.id);
        }
      } else {
        const index = nextSelections.indexOf(scenario.id);
        if (index >= 0) {
          nextSelections.splice(index, 1);
        }
      }
      updateScenarioSelections(nextSelections);
    });
    const name = document.createElement("span");
    name.textContent = scenario.name;
    label.appendChild(checkbox);
    label.appendChild(name);
    savedCompareScenarioList.appendChild(label);
  });
}

function renderSavedComparePrivateList() {
  if (!savedComparePrivateList) {
    return;
  }
  savedComparePrivateList.innerHTML = "";
  if (!privateProviderStore.providers.length) {
    savedComparePrivateList.textContent = "No private providers saved.";
    return;
  }
  const selections = resolvePrivateSelections();
  if (Array.isArray(savedComparePrivateSelections)) {
    updatePrivateSelections(selections);
  }
  privateProviderStore.providers.forEach((provider) => {
    const label = document.createElement("label");
    label.className = "checkbox-field saved-compare-item";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = provider.id;
    checkbox.checked = selections.includes(provider.id);
    checkbox.addEventListener("change", () => {
      const nextSelections = Array.isArray(savedComparePrivateSelections)
        ? [...savedComparePrivateSelections]
        : resolvePrivateSelections();
      if (checkbox.checked) {
        if (!nextSelections.includes(provider.id)) {
          nextSelections.push(provider.id);
        }
      } else {
        const index = nextSelections.indexOf(provider.id);
        if (index >= 0) {
          nextSelections.splice(index, 1);
        }
      }
      updatePrivateSelections(nextSelections);
    });
    const name = document.createElement("span");
    name.textContent = provider.name;
    label.appendChild(checkbox);
    label.appendChild(name);
    savedComparePrivateList.appendChild(label);
  });
}

function getScenarioById(id) {
  return scenarioStore.find((scenario) => scenario.id === id);
}

function getScenarioByName(name) {
  return scenarioStore.find(
    (scenario) => scenario.name.toLowerCase() === name.toLowerCase()
  );
}

function buildCloneName(baseName) {
  let name = `${baseName} copy`;
  let index = 2;
  while (getScenarioByName(name)) {
    name = `${baseName} copy ${index}`;
    index += 1;
  }
  return name;
}

function applyScenarioInput(input) {
  if (!input) {
    return;
  }
  const focusPanel =
    input.pricingFocus === "network"
      ? "network"
      : input.pricingFocus === "storage"
      ? "storage"
      : "vm";
  const nextMode = input.mode === "k8s" ? "k8s" : focusPanel;
  setPanel(nextMode);
  if (focusPanel === "network") {
    renderNetworkProviderCards({ input });
  }
  if (focusPanel === "storage") {
    renderStorageProviderCards({ input });
  }
  if (networkAddonFocusInput) {
    setNetworkAddonFocus(input.networkAddonFocus || "vpc", {
      silent: true,
    });
  }
  if (input.workload && workloadSelect) {
    workloadSelect.value = input.workload;
  }
  if (awsInstanceFilter) {
    awsInstanceFilter.value = "";
  }
  if (azureInstanceFilter) {
    azureInstanceFilter.value = "";
  }
  if (gcpInstanceFilter) {
    gcpInstanceFilter.value = "";
  }
  updateCpuOptions();
  if (Number.isFinite(input.cpu)) {
    cpuSelect.value = input.cpu.toString();
  }
  updateInstanceOptions();
  if (input.awsInstanceType) {
    awsInstanceSelect.value = input.awsInstanceType;
  }
  if (input.azureInstanceType) {
    azureInstanceSelect.value = input.azureInstanceType;
  }
  if (input.gcpInstanceType) {
    gcpInstanceSelect.value = input.gcpInstanceType;
  }
  if (input.regionKey && regionSelect) {
    regionSelect.value = input.regionKey;
  }
  if (input.pricingProvider && pricingProviderSelect) {
    pricingProviderSelect.value = input.pricingProvider;
  }
  if (
    (focusPanel === "network" || focusPanel === "storage") &&
    pricingProviderSelect
  ) {
    pricingProviderSelect.value = "api";
  }
  if (input.diskTier && diskTierSelect) {
    diskTierSelect.value = input.diskTier;
  }
  if (input.sqlEdition && sqlEditionSelect) {
    sqlEditionSelect.value = input.sqlEdition;
  }
  if (Number.isFinite(input.sqlLicenseRate)) {
    const rateValue = Number.parseFloat(input.sqlLicenseRate);
    sqlRateInput.value = rateValue.toString();
    const editionValue = sqlEditionSelect.value || "none";
    sqlRateTouched = !isDefaultSqlRate(rateValue, editionValue);
  } else {
    const editionValue = sqlEditionSelect.value || "none";
    const nextRate = SQL_DEFAULTS[editionValue] ?? 0;
    sqlRateInput.value = nextRate.toString();
    sqlRateTouched = false;
  }
  if (Number.isFinite(input.osDiskGb)) {
    osDiskInput.value = input.osDiskGb.toString();
  }
  if (Number.isFinite(input.dataDiskTb) && dataDiskInput) {
    dataDiskInput.value = input.dataDiskTb.toString();
  }
  if (Number.isFinite(input.egressTb) && egressInput) {
    egressInput.value = input.egressTb.toString();
  }
  if (Number.isFinite(input.interVlanTb) && interVlanInput) {
    interVlanInput.value = input.interVlanTb.toString();
  }
  if (Number.isFinite(input.intraVlanTb) && intraVlanInput) {
    intraVlanInput.value = input.intraVlanTb.toString();
  }
  if (Number.isFinite(input.interRegionTb) && interRegionInput) {
    interRegionInput.value = input.interRegionTb.toString();
  }
  if (Number.isFinite(input.storageIops) && storageIopsInput) {
    storageIopsInput.value = input.storageIops.toString();
  }
  if (
    Number.isFinite(input.storageThroughputMbps) &&
    storageThroughputInput
  ) {
    storageThroughputInput.value = input.storageThroughputMbps.toString();
  }
  if (Number.isFinite(input.hours) && hoursInput) {
    hoursInput.value = input.hours.toString();
  }
  if (Number.isFinite(input.vmCount) && vmCountInput) {
    vmCountInput.value = input.vmCount.toString();
  }
  if (Number.isFinite(input.drPercent) && drPercentInput) {
    drPercentInput.value = input.drPercent.toString();
  }
  if (backupEnabledInput) {
    backupEnabledInput.checked = Boolean(input.backupEnabled);
  }
  if (awsVpcSelect && input.awsVpcFlavor) {
    awsVpcSelect.value = input.awsVpcFlavor;
  }
  if (awsFirewallSelect && input.awsFirewallFlavor) {
    awsFirewallSelect.value = input.awsFirewallFlavor;
  }
  if (awsLbSelect && input.awsLoadBalancerFlavor) {
    awsLbSelect.value = input.awsLoadBalancerFlavor;
  }
  if (azureVpcSelect && input.azureVpcFlavor) {
    azureVpcSelect.value = input.azureVpcFlavor;
  }
  if (azureFirewallSelect && input.azureFirewallFlavor) {
    azureFirewallSelect.value = input.azureFirewallFlavor;
  }
  if (azureLbSelect && input.azureLoadBalancerFlavor) {
    azureLbSelect.value = input.azureLoadBalancerFlavor;
  }
  if (gcpVpcSelect && input.gcpVpcFlavor) {
    gcpVpcSelect.value = input.gcpVpcFlavor;
  }
  if (gcpFirewallSelect && input.gcpFirewallFlavor) {
    gcpFirewallSelect.value = input.gcpFirewallFlavor;
  }
  if (gcpLbSelect && input.gcpLoadBalancerFlavor) {
    gcpLbSelect.value = input.gcpLoadBalancerFlavor;
  }
  if (Number.isFinite(input.awsObjectStorageRate) && awsObjectStorageInput) {
    awsObjectStorageInput.value = input.awsObjectStorageRate.toString();
  }
  if (Number.isFinite(input.azureObjectStorageRate) && azureObjectStorageInput) {
    azureObjectStorageInput.value = input.azureObjectStorageRate.toString();
  }
  if (Number.isFinite(input.gcpObjectStorageRate) && gcpObjectStorageInput) {
    gcpObjectStorageInput.value = input.gcpObjectStorageRate.toString();
  }
  const numericFocusFields = [
    "awsNetworkVpcCount",
    "awsNetworkVpcDataTb",
    "awsNetworkGatewayCount",
    "awsNetworkGatewayDataTb",
    "awsNetworkLoadBalancerCount",
    "awsNetworkLoadBalancerDataTb",
    "azureNetworkVpcCount",
    "azureNetworkVpcDataTb",
    "azureNetworkGatewayCount",
    "azureNetworkGatewayDataTb",
    "azureNetworkLoadBalancerCount",
    "azureNetworkLoadBalancerDataTb",
    "gcpNetworkVpcCount",
    "gcpNetworkVpcDataTb",
    "gcpNetworkGatewayCount",
    "gcpNetworkGatewayDataTb",
    "gcpNetworkLoadBalancerCount",
    "gcpNetworkLoadBalancerDataTb",
    "awsStorageAccountCount",
    "awsStorageDrDeltaTb",
    "awsStorageObjectTb",
    "awsStorageFileTb",
    "awsStorageTableTb",
    "awsStorageQueueTb",
    "azureStorageAccountCount",
    "azureStorageDrDeltaTb",
    "azureStorageObjectTb",
    "azureStorageFileTb",
    "azureStorageTableTb",
    "azureStorageQueueTb",
    "gcpStorageAccountCount",
    "gcpStorageDrDeltaTb",
    "gcpStorageObjectTb",
    "gcpStorageFileTb",
    "gcpStorageTableTb",
    "gcpStorageQueueTb",
  ];
  numericFocusFields.forEach((name) => {
    const value = input[name];
    const field = document.querySelector(`[name='${name}']`);
    if (field && Number.isFinite(value)) {
      field.value = value.toString();
    }
  });
  const stringFocusFields = [
    "awsNetworkVpcFlavor",
    "awsNetworkGatewayFlavor",
    "awsNetworkLoadBalancerFlavor",
    "azureNetworkVpcFlavor",
    "azureNetworkGatewayFlavor",
    "azureNetworkLoadBalancerFlavor",
    "gcpNetworkVpcFlavor",
    "gcpNetworkGatewayFlavor",
    "gcpNetworkLoadBalancerFlavor",
  ];
  stringFocusFields.forEach((name) => {
    const value = input[name];
    const field = document.querySelector(`[name='${name}']`);
    if (field && value) {
      field.value = value;
    }
  });
  const booleanFocusFields = [
    "awsStorageDrEnabled",
    "azureStorageDrEnabled",
    "gcpStorageDrEnabled",
  ];
  booleanFocusFields.forEach((name) => {
    const value = input[name];
    const field = document.querySelector(`[name='${name}']`);
    if (field && typeof value === "boolean") {
      field.checked = value;
    }
  });
  applyScenarioPrivateConfig(input);
}

function applyScenarioPrivateConfig(input) {
  if (!input) {
    return;
  }
  const provider = getPrimaryPrivateProvider();
  if (!provider) {
    return;
  }
  const config = normalizePrivateConfig({
    enabled: Boolean(input.privateEnabled),
    vmwareMonthly: input.privateVmwareMonthly,
    windowsLicenseMonthly: input.privateWindowsLicenseMonthly,
    nodeCount: input.privateNodeCount,
    storagePerTb: input.privateStoragePerTb,
    networkMonthly: input.privateNetworkMonthly,
    firewallMonthly: input.privateFirewallMonthly,
    loadBalancerMonthly: input.privateLoadBalancerMonthly,
    nodeCpu: input.privateNodeCpu,
    nodeRam: input.privateNodeRam,
    nodeStorageTb: input.privateNodeStorageTb,
    vmOsDiskGb: input.privateVmOsDiskGb,
    sanUsableTb: input.privateSanUsableTb,
    sanTotalMonthly: input.privateSanTotalMonthly,
  });
  provider.config = config;
  provider.updatedAt = new Date().toISOString();
  privateProviderStore.activeId = provider.id;
  persistPrivateProviders(privateProviderStore);
  const cardState = privateProviderCards.get(provider.id);
  if (cardState) {
    applyPrivateConfigToCard(cardState, config, provider.name);
    updatePrivateCapacityForCard(cardState);
  }
  syncPrivateCompareSelections();
}

function getScenarioProviderTotal(data, providerKey) {
  if (!data) {
    return null;
  }
  if (providerKey === "private" && !data.private?.enabled) {
    return null;
  }
  const provider = data[providerKey];
  const total =
    provider?.pricingTiers?.onDemand?.totals?.total ??
    provider?.totals?.total;
  return Number.isFinite(total) ? total : null;
}

function getScenarioDisplayMode(input) {
  if (!input) {
    return "vm";
  }
  if (input.pricingFocus === "network") {
    return "network";
  }
  if (input.pricingFocus === "storage") {
    return "storage";
  }
  return input.mode || "vm";
}

function getScenarioProviderTotals(data, providerKey) {
  if (!data) {
    return null;
  }
  if (providerKey === "private" && !data.private?.enabled) {
    return null;
  }
  const provider = data[providerKey];
  return provider?.pricingTiers?.onDemand?.totals ?? provider?.totals ?? null;
}

function getScenarioComponentValue(totals, field) {
  if (!totals) {
    return null;
  }
  if (field === "licenseMonthly") {
    const sql = Number.isFinite(totals.sqlMonthly) ? totals.sqlMonthly : 0;
    const windows = Number.isFinite(totals.windowsLicenseMonthly)
      ? totals.windowsLicenseMonthly
      : 0;
    return sql + windows;
  }
  const value = totals[field];
  return Number.isFinite(value) ? value : null;
}

function buildScenarioComparison(currentData, scenarioData, scenarioName) {
  const modeNote =
    currentData?.input?.mode !== scenarioData?.input?.mode
      ? " (mode differs)"
      : "";
  const providerKeys = ["aws", "azure", "gcp", "private"];
  const diffs = [];
  let sumDiff = 0;
  providerKeys.forEach((providerKey) => {
    if (providerKey === "private") {
      if (!currentData?.private?.enabled && !scenarioData?.private?.enabled) {
        return;
      }
    }
    const currentTotal = getScenarioProviderTotal(currentData, providerKey);
    const scenarioTotal = getScenarioProviderTotal(scenarioData, providerKey);
    if (!Number.isFinite(currentTotal) || !Number.isFinite(scenarioTotal)) {
      diffs.push(`${getProviderLabel(providerKey)} N/A`);
      return;
    }
    const diff = scenarioTotal - currentTotal;
    sumDiff += diff;
    const sign = diff > 0 ? "+" : diff < 0 ? "-" : "";
    const label =
      diff === 0
        ? "same"
        : `${sign}${formatMoney(Math.abs(diff))}/mo`;
    diffs.push(`${getProviderLabel(providerKey)} ${label}`);
  });
  return {
    text: `Scenario "${scenarioName}" vs current${modeNote}: ${diffs.join(
      " | "
    )}.`,
    diffTotal: sumDiff,
  };
}

async function loadSizeOptions() {
  const response = await fetch(resolvePriceApiPath("/api/options"));
  if (!response.ok) {
    throw new Error("Options request failed.");
  }
  sizeOptions = await response.json();
  updateCpuOptions();
  updateInstanceOptions();
  updateNetworkAddonOptions();
  updateNetworkAddonFocusUi();
}

function setSelectOptions(select, options, currentValue) {
  select.innerHTML = "";
  options.forEach((value) => {
    const option = document.createElement("option");
    option.value = value.toString();
    option.textContent = value.toString();
    select.appendChild(option);
  });
  if (currentValue && options.includes(currentValue)) {
    select.value = currentValue.toString();
  } else if (options.length) {
    select.value = options[0].toString();
  }
}

function setSelectOptionsWithLabels(select, options, currentValue) {
  if (!(select instanceof HTMLSelectElement)) {
    return;
  }
  select.innerHTML = "";
  options.forEach((optionValue) => {
    const option = document.createElement("option");
    option.value = optionValue.key;
    option.textContent = optionValue.label;
    select.appendChild(option);
  });
  if (
    currentValue &&
    options.some((optionValue) => optionValue.key === currentValue)
  ) {
    select.value = currentValue;
  } else if (options.length) {
    select.value = options[0].key;
  }
}

function getFlavorConfig() {
  if (!sizeOptions) {
    return null;
  }
  if (currentMode === "k8s") {
    return sizeOptions.k8s;
  }
  const workload = workloadSelect.value;
  return sizeOptions.workloads?.[workload] || null;
}

function collectProviderSizes(providerKey, flavorKeys) {
  const provider = sizeOptions?.providers?.[providerKey];
  if (!provider) {
    return [];
  }
  const sizes = [];
  flavorKeys.forEach((flavorKey) => {
    const flavor = provider.flavors?.[flavorKey];
    if (!flavor?.sizes) {
      return;
    }
    flavor.sizes.forEach((size) => {
      sizes.push({ ...size, flavorKey });
    });
  });
  return sizes;
}

function getNetworkAddonLabel(providerKey, addonKey, flavorKey) {
  const options =
    sizeOptions?.networkAddons?.providers?.[providerKey]?.[addonKey];
  if (!Array.isArray(options)) {
    return null;
  }
  const match = options.find((option) => option.key === flavorKey);
  return match?.label || null;
}

function buildCpuOptions() {
  const config = getFlavorConfig();
  if (!config) {
    return [];
  }
  const flavorSets = config.flavors || {};
  const cpuSet = new Set();
  ["aws", "azure", "gcp"].forEach((providerKey) => {
    const sizes = collectProviderSizes(
      providerKey,
      flavorSets[providerKey] || []
    );
    sizes.forEach((size) => {
      if (Number.isFinite(size.vcpu) && size.vcpu >= sizeOptions.minCpu) {
        cpuSet.add(size.vcpu);
      }
    });
  });
  return Array.from(cpuSet).sort((a, b) => a - b);
}

function updateCpuOptions() {
  const cpuOptions = buildCpuOptions();
  const currentValue = Number.parseInt(cpuSelect.value, 10);
  const fallbackValue = Number.isFinite(currentValue)
    ? currentValue
    : sizeOptions?.minCpu;
  const options = cpuOptions.length
    ? cpuOptions
    : sizeOptions?.minCpu
    ? [sizeOptions.minCpu]
    : [];
  setSelectOptions(cpuSelect, options, fallbackValue);
}

function setInstanceOptions(select, sizes, currentValue) {
  select.innerHTML = "";
  if (!sizes.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "No matching flavors";
    select.appendChild(option);
    select.disabled = true;
    return;
  }
  const sorted = [...sizes].sort((a, b) => {
    if (a.vcpu === b.vcpu) {
      return a.memory - b.memory;
    }
    return a.vcpu - b.vcpu;
  });
  sorted.forEach((size) => {
    const option = document.createElement("option");
    option.value = size.type;
    option.textContent = `${size.type} — ${size.vcpu} vCPU / ${size.memory} GB`;
    select.appendChild(option);
  });
  select.disabled = false;
  if (currentValue && sorted.some((size) => size.type === currentValue)) {
    select.value = currentValue;
  } else if (sorted.length) {
    select.value = sorted[0].type;
  }
}

function filterInstanceSizes(sizes, query) {
  if (!query) {
    return sizes;
  }
  const text = query.trim().toLowerCase();
  if (!text) {
    return sizes;
  }
  return sizes.filter((size) => {
    const haystack = `${size.type} ${size.vcpu} ${size.memory}`.toLowerCase();
    return haystack.includes(text);
  });
}

function refreshInstanceSelects() {
  const awsFiltered = filterInstanceSizes(
    instancePools.aws,
    awsInstanceFilter?.value
  );
  const azureFiltered = filterInstanceSizes(
    instancePools.azure,
    azureInstanceFilter?.value
  );
  const gcpFiltered = filterInstanceSizes(
    instancePools.gcp,
    gcpInstanceFilter?.value
  );
  setInstanceOptions(awsInstanceSelect, awsFiltered, awsInstanceSelect.value);
  setInstanceOptions(
    azureInstanceSelect,
    azureFiltered,
    azureInstanceSelect.value
  );
  setInstanceOptions(gcpInstanceSelect, gcpFiltered, gcpInstanceSelect.value);
}

function updateInstanceOptions() {
  const config = getFlavorConfig();
  if (!config) {
    return;
  }
  const flavorSets = config.flavors || {};
  const cpuValue = Number.parseInt(cpuSelect.value, 10);
  const awsSizes = collectProviderSizes(
    "aws",
    flavorSets.aws || []
  ).filter((size) => size.vcpu === cpuValue);
  const azureSizes = collectProviderSizes(
    "azure",
    flavorSets.azure || []
  ).filter((size) => size.vcpu === cpuValue);
  const gcpSizes = collectProviderSizes(
    "gcp",
    flavorSets.gcp || []
  ).filter((size) => size.vcpu === cpuValue);

  instancePools.aws = awsSizes;
  instancePools.azure = azureSizes;
  instancePools.gcp = gcpSizes;
  refreshInstanceSelects();
}

function updatePrivateCapacityForCard(cardState) {
  if (!cardState) {
    return;
  }
  const fields = cardState.fields || {};
  const nodeVcpuCapacityRaw = Number.parseFloat(fields.nodeCpu?.value);
  const nodeVcpuCapacity =
    Number.isFinite(nodeVcpuCapacityRaw) && nodeVcpuCapacityRaw > 0
      ? nodeVcpuCapacityRaw
      : 0;
  const nodeCount = Number.parseFloat(fields.nodeCount?.value);
  const usableNodes =
    Number.isFinite(nodeCount) && nodeCount > 1
      ? nodeCount - 1
      : 1;
  const nodeRam = Number.parseFloat(fields.nodeRam?.value);
  const nodeStorageTb = Number.parseFloat(fields.nodeStorageTb?.value);
  const nodeStorageGb =
    Number.isFinite(nodeStorageTb) && nodeStorageTb > 0
      ? nodeStorageTb * 1024
      : null;
  const vmOsGb = Number.parseFloat(fields.vmOsDiskGb?.value) || 256;
  if (cardState.osSizeLabels?.length) {
    cardState.osSizeLabels.forEach((label) => {
      label.textContent = `${vmOsGb} GB`;
    });
  }
  PRIVATE_FLAVORS.forEach((flavor) => {
    const maxByCpu =
      Number.isFinite(nodeVcpuCapacity) && nodeVcpuCapacity > 0
        ? Math.floor(nodeVcpuCapacity / flavor.vcpu)
        : 0;
    const maxByRam = Number.isFinite(nodeRam)
      ? Math.floor(nodeRam / flavor.ram)
      : 0;
    const maxByStorage =
      Number.isFinite(nodeStorageGb) && nodeStorageGb > 0 && vmOsGb > 0
        ? Math.floor(nodeStorageGb / vmOsGb)
        : Number.POSITIVE_INFINITY;
    const maxCount = Math.max(
      0,
      Math.min(maxByCpu, maxByRam, maxByStorage)
    );
    const target = cardState.capacityCounts?.[flavor.key];
    if (target) {
      target.textContent = Number.isFinite(maxCount)
        ? maxCount.toString()
        : "-";
    }
    const totalTarget = cardState.capacityTotals?.[flavor.key];
    if (totalTarget) {
      const totalCount = Number.isFinite(maxCount)
        ? Math.max(0, Math.floor(maxCount * usableNodes))
        : 0;
      totalTarget.textContent = Number.isFinite(totalCount)
        ? totalCount.toString()
        : "-";
    }
  });

  const sanUsableTb = Number.parseFloat(fields.sanUsableTb?.value);
  const sanTotalMonthly = Number.parseFloat(fields.sanTotalMonthly?.value);
  let perTb = 0;
  if (
    Number.isFinite(sanUsableTb) &&
    sanUsableTb > 0 &&
    Number.isFinite(sanTotalMonthly) &&
    sanTotalMonthly > 0
  ) {
    perTb = sanTotalMonthly / sanUsableTb;
  } else {
    const storedRate = Number.parseFloat(fields.storagePerTb?.value);
    if (Number.isFinite(storedRate) && storedRate > 0) {
      perTb = storedRate;
    }
  }
  if (fields.storagePerTb) {
    fields.storagePerTb.value = perTb.toFixed(4);
  }
  if (fields.sanRate) {
    fields.sanRate.textContent =
      perTb > 0 ? `${formatMoney(perTb)}/TB-mo` : "N/A";
  }
}

function updateNetworkAddonOptions() {
  const networkAddons = sizeOptions?.networkAddons;
  if (!networkAddons) {
    return;
  }
  const providers = networkAddons.providers || {};
  const defaults = networkAddons.defaults || {};
  setSelectOptionsWithLabels(
    awsVpcSelect,
    providers.aws?.vpc || [],
    awsVpcSelect?.value || defaults.aws?.vpc
  );
  setSelectOptionsWithLabels(
    awsFirewallSelect,
    providers.aws?.firewall || [],
    awsFirewallSelect?.value || defaults.aws?.firewall
  );
  setSelectOptionsWithLabels(
    awsLbSelect,
    providers.aws?.loadBalancer || [],
    awsLbSelect?.value || defaults.aws?.loadBalancer
  );
  setSelectOptionsWithLabels(
    azureVpcSelect,
    providers.azure?.vpc || [],
    azureVpcSelect?.value || defaults.azure?.vpc
  );
  setSelectOptionsWithLabels(
    azureFirewallSelect,
    providers.azure?.firewall || [],
    azureFirewallSelect?.value || defaults.azure?.firewall
  );
  setSelectOptionsWithLabels(
    azureLbSelect,
    providers.azure?.loadBalancer || [],
    azureLbSelect?.value || defaults.azure?.loadBalancer
  );
  setSelectOptionsWithLabels(
    gcpVpcSelect,
    providers.gcp?.vpc || [],
    gcpVpcSelect?.value || defaults.gcp?.vpc
  );
  setSelectOptionsWithLabels(
    gcpFirewallSelect,
    providers.gcp?.firewall || [],
    gcpFirewallSelect?.value || defaults.gcp?.firewall
  );
  setSelectOptionsWithLabels(
    gcpLbSelect,
    providers.gcp?.loadBalancer || [],
    gcpLbSelect?.value || defaults.gcp?.loadBalancer
  );
}

function serializeForm(formElement) {
  const data = Object.fromEntries(new FormData(formElement).entries());
  const pricingFocus =
    data.pricingFocus === "network"
      ? "network"
      : data.pricingFocus === "storage"
      ? "storage"
      : "all";
  const networkAddonFocus = normalizeNetworkAddonFocus(
    data.networkAddonFocus
  );
  const mode = data.mode === "k8s" ? "k8s" : "vm";
  const backupEnabled = pricingFocus === "all" && data.backupEnabled === "on";
  const egressTb =
    pricingFocus === "storage" ? 0 : Number.parseFloat(data.egressTb);
  const drPercent =
    pricingFocus === "all" ? Number.parseFloat(data.drPercent) : 0;
  const privateConfig = getPrivateConfigFromForm();
  const sanUsableTb = Number.parseFloat(privateConfig.sanUsableTb);
  const sanTotalMonthly = Number.parseFloat(privateConfig.sanTotalMonthly);
  let privateStoragePerTb = Number.parseFloat(privateConfig.storagePerTb);
  if (
    Number.isFinite(sanUsableTb) &&
    sanUsableTb > 0 &&
    Number.isFinite(sanTotalMonthly) &&
    sanTotalMonthly > 0
  ) {
    privateStoragePerTb = sanTotalMonthly / sanUsableTb;
  }
  const normalizedStoragePerTb = Number.isFinite(privateStoragePerTb)
    ? privateStoragePerTb
    : 0;
  return {
    cpu: Number.parseInt(data.cpu, 10),
    workload: data.workload,
    awsInstanceType: awsInstanceSelect.value,
    azureInstanceType: azureInstanceSelect.value,
    gcpInstanceType: gcpInstanceSelect.value,
    regionKey: data.regionKey,
    pricingProvider: data.pricingProvider,
    pricingFocus,
    networkAddonFocus,
    diskTier: data.diskTier,
    sqlEdition: data.sqlEdition,
    mode,
    osDiskGb: Number.parseFloat(data.osDiskGb),
    dataDiskTb: Number.parseFloat(data.dataDiskTb),
    egressTb,
    interVlanTb: Number.parseFloat(data.interVlanTb),
    intraVlanTb: Number.parseFloat(data.intraVlanTb),
    interRegionTb: Number.parseFloat(data.interRegionTb),
    storageIops: Number.parseFloat(data.storageIops),
    storageThroughputMbps: Number.parseFloat(data.storageThroughputMbps),
    hours: Number.parseFloat(data.hours),
    backupEnabled,
    awsVpcFlavor: data.awsVpcFlavor,
    awsFirewallFlavor: data.awsFirewallFlavor,
    awsLoadBalancerFlavor: data.awsLoadBalancerFlavor,
    awsNetworkVpcFlavor: data.awsNetworkVpcFlavor,
    awsNetworkVpcCount: Number.parseFloat(data.awsNetworkVpcCount),
    awsNetworkVpcDataTb: Number.parseFloat(data.awsNetworkVpcDataTb),
    awsNetworkGatewayFlavor: data.awsNetworkGatewayFlavor,
    awsNetworkGatewayCount: Number.parseFloat(data.awsNetworkGatewayCount),
    awsNetworkGatewayDataTb: Number.parseFloat(data.awsNetworkGatewayDataTb),
    awsNetworkLoadBalancerFlavor: data.awsNetworkLoadBalancerFlavor,
    awsNetworkLoadBalancerCount: Number.parseFloat(
      data.awsNetworkLoadBalancerCount
    ),
    awsNetworkLoadBalancerDataTb: Number.parseFloat(
      data.awsNetworkLoadBalancerDataTb
    ),
    azureVpcFlavor: data.azureVpcFlavor,
    azureFirewallFlavor: data.azureFirewallFlavor,
    azureLoadBalancerFlavor: data.azureLoadBalancerFlavor,
    azureNetworkVpcFlavor: data.azureNetworkVpcFlavor,
    azureNetworkVpcCount: Number.parseFloat(data.azureNetworkVpcCount),
    azureNetworkVpcDataTb: Number.parseFloat(data.azureNetworkVpcDataTb),
    azureNetworkGatewayFlavor: data.azureNetworkGatewayFlavor,
    azureNetworkGatewayCount: Number.parseFloat(data.azureNetworkGatewayCount),
    azureNetworkGatewayDataTb: Number.parseFloat(data.azureNetworkGatewayDataTb),
    azureNetworkLoadBalancerFlavor: data.azureNetworkLoadBalancerFlavor,
    azureNetworkLoadBalancerCount: Number.parseFloat(
      data.azureNetworkLoadBalancerCount
    ),
    azureNetworkLoadBalancerDataTb: Number.parseFloat(
      data.azureNetworkLoadBalancerDataTb
    ),
    gcpVpcFlavor: data.gcpVpcFlavor,
    gcpFirewallFlavor: data.gcpFirewallFlavor,
    gcpLoadBalancerFlavor: data.gcpLoadBalancerFlavor,
    gcpNetworkVpcFlavor: data.gcpNetworkVpcFlavor,
    gcpNetworkVpcCount: Number.parseFloat(data.gcpNetworkVpcCount),
    gcpNetworkVpcDataTb: Number.parseFloat(data.gcpNetworkVpcDataTb),
    gcpNetworkGatewayFlavor: data.gcpNetworkGatewayFlavor,
    gcpNetworkGatewayCount: Number.parseFloat(data.gcpNetworkGatewayCount),
    gcpNetworkGatewayDataTb: Number.parseFloat(data.gcpNetworkGatewayDataTb),
    gcpNetworkLoadBalancerFlavor: data.gcpNetworkLoadBalancerFlavor,
    gcpNetworkLoadBalancerCount: Number.parseFloat(
      data.gcpNetworkLoadBalancerCount
    ),
    gcpNetworkLoadBalancerDataTb: Number.parseFloat(
      data.gcpNetworkLoadBalancerDataTb
    ),
    awsObjectStorageRate: Number.parseFloat(data.awsObjectStorageRate),
    azureObjectStorageRate: Number.parseFloat(data.azureObjectStorageRate),
    gcpObjectStorageRate: Number.parseFloat(data.gcpObjectStorageRate),
    awsStorageAccountCount: Number.parseFloat(data.awsStorageAccountCount),
    awsStorageDrEnabled: data.awsStorageDrEnabled === "on",
    awsStorageDrDeltaTb: Number.parseFloat(data.awsStorageDrDeltaTb),
    awsStorageObjectTb: Number.parseFloat(data.awsStorageObjectTb),
    awsStorageFileTb: Number.parseFloat(data.awsStorageFileTb),
    awsStorageTableTb: Number.parseFloat(data.awsStorageTableTb),
    awsStorageQueueTb: Number.parseFloat(data.awsStorageQueueTb),
    azureStorageAccountCount: Number.parseFloat(data.azureStorageAccountCount),
    azureStorageDrEnabled: data.azureStorageDrEnabled === "on",
    azureStorageDrDeltaTb: Number.parseFloat(data.azureStorageDrDeltaTb),
    azureStorageObjectTb: Number.parseFloat(data.azureStorageObjectTb),
    azureStorageFileTb: Number.parseFloat(data.azureStorageFileTb),
    azureStorageTableTb: Number.parseFloat(data.azureStorageTableTb),
    azureStorageQueueTb: Number.parseFloat(data.azureStorageQueueTb),
    gcpStorageAccountCount: Number.parseFloat(data.gcpStorageAccountCount),
    gcpStorageDrEnabled: data.gcpStorageDrEnabled === "on",
    gcpStorageDrDeltaTb: Number.parseFloat(data.gcpStorageDrDeltaTb),
    gcpStorageObjectTb: Number.parseFloat(data.gcpStorageObjectTb),
    gcpStorageFileTb: Number.parseFloat(data.gcpStorageFileTb),
    gcpStorageTableTb: Number.parseFloat(data.gcpStorageTableTb),
    gcpStorageQueueTb: Number.parseFloat(data.gcpStorageQueueTb),
    vmCount: Number.parseInt(data.vmCount, 10),
    drPercent,
    sqlLicenseRate: Number.parseFloat(data.sqlLicenseRate),
    privateEnabled: Boolean(privateConfig.enabled),
    privateVmwareMonthly: Number.parseFloat(privateConfig.vmwareMonthly),
    privateWindowsLicenseMonthly: Number.parseFloat(
      privateConfig.windowsLicenseMonthly
    ),
    privateNodeCount: Number.parseFloat(privateConfig.nodeCount),
    privateStoragePerTb: normalizedStoragePerTb,
    privateNetworkMonthly: Number.parseFloat(privateConfig.networkMonthly),
    privateFirewallMonthly: Number.parseFloat(privateConfig.firewallMonthly),
    privateLoadBalancerMonthly: Number.parseFloat(
      privateConfig.loadBalancerMonthly
    ),
    privateNodeCpu: Number.parseFloat(privateConfig.nodeCpu),
    privateNodeRam: Number.parseFloat(privateConfig.nodeRam),
    privateNodeStorageTb: Number.parseFloat(privateConfig.nodeStorageTb),
    privateVmOsDiskGb: Number.parseFloat(privateConfig.vmOsDiskGb),
    privateSanUsableTb: sanUsableTb,
    privateSanTotalMonthly: sanTotalMonthly,
  };
}

async function fetchAndRender() {
  const basePayload = serializeForm(form);
  const isPublicOnlyFocus =
    basePayload.pricingFocus === "network" ||
    basePayload.pricingFocus === "storage";
  const selections = syncPrivateCompareSelections();
  const primaryProvider = selections[0]
    ? getPrivateProviderById(selections[0])
    : null;
  const payload = applyPrivateConfigToPayload(
    basePayload,
    isPublicOnlyFocus ? null : primaryProvider?.config,
    { forceEnable: !isPublicOnlyFocus && Boolean(primaryProvider) }
  );
  const data = await comparePricing(payload);
  lastPricing = data;
  const vmCount = data.input?.vmCount ?? payload.vmCount;
  const mode = data.input?.mode ?? payload.mode ?? "vm";
  updateProvider(fields.aws, data.aws, data.region.aws, {
    showMonthlyRate: false,
    showReservationNote: true,
    vmCount,
    mode,
    providerKey: "aws",
    sharedStorageSources: data.notes?.sharedStorageSources || null,
    pricingFocus: data.input?.pricingFocus,
    pricingProvider: data.input?.pricingProvider,
  });
  updateProvider(fields.azure, data.azure, data.region.azure, {
    showMonthlyRate: false,
    showReservationNote: false,
    vmCount,
    mode,
    providerKey: "azure",
    sharedStorageSources: data.notes?.sharedStorageSources || null,
    pricingFocus: data.input?.pricingFocus,
    pricingProvider: data.input?.pricingProvider,
  });
  updateProvider(fields.gcp, data.gcp, data.region.gcp, {
    showMonthlyRate: false,
    showReservationNote: false,
    vmCount,
    mode,
    providerKey: "gcp",
    sharedStorageSources: data.notes?.sharedStorageSources || null,
    pricingFocus: data.input?.pricingFocus,
    pricingProvider: data.input?.pricingProvider,
  });
  updateDelta(
    data.aws,
    data.azure,
    data.gcp,
    isPublicOnlyFocus ? null : data.private
  );
  if (
    activePanel !== "private" &&
    activePanel !== "vsax-compare" &&
    currentResultsTab === "pricing" &&
    currentView === "compare" &&
    !isPublicOnlyFocus
  ) {
    await renderPrivateCompareCards(basePayload, data);
  }
  if (currentMode === "network") {
    renderNetworkFocusTable(data);
    if (currentNetworkResult === "insight") {
      renderFocusInsight(data, "network");
    }
  } else if (currentMode === "storage") {
    renderStorageFocusTable(data);
    if (currentStorageResult === "insight") {
      renderFocusInsight(data, "storage");
    }
  }
  const noteParts = [];
  if (data.notes?.constraints) {
    noteParts.push(data.notes.constraints);
  }
  if (data.notes?.sizeCap) {
    noteParts.push(data.notes.sizeCap);
  }
  const diskTierLabel =
    data.input?.diskTierLabel ||
    DISK_TIER_LABELS[data.input?.diskTier] ||
    DISK_TIER_LABELS[diskTierSelect?.value];
  if (diskTierLabel && data.input?.pricingFocus === "all") {
    noteParts.push(`Disk tier: ${diskTierLabel}.`);
  }
  const networkSummaries = [];
  const input = data.input || {};
  const networkAddonFocus =
    input.pricingFocus === "network"
      ? normalizeNetworkAddonFocus(input.networkAddonFocus)
      : "all";
  const providerKeys = ["aws", "azure", "gcp"];
  providerKeys.forEach((providerKey) => {
    const entries =
      input.pricingFocus === "network"
        ? [
            ["vpc", input[`${providerKey}NetworkVpcFlavor`]],
            ["gateway", input[`${providerKey}NetworkGatewayFlavor`]],
            ["loadBalancer", input[`${providerKey}NetworkLoadBalancerFlavor`]],
          ]
        : [
            ["vpc", input[`${providerKey}VpcFlavor`]],
            ["firewall", input[`${providerKey}FirewallFlavor`]],
            ["loadBalancer", input[`${providerKey}LoadBalancerFlavor`]],
          ];
    const filtered = entries.filter(([addonKey]) =>
      networkAddonFocus === "all" ? true : addonKey === networkAddonFocus
    );
    const labels = filtered
      .map(([addonKey, flavorKey]) =>
        getNetworkAddonLabel(providerKey, addonKey, flavorKey)
      )
      .filter((label) => label && label.toLowerCase() !== "none");
    if (labels.length) {
      const providerLabel = getProviderLabelForMode(providerKey, mode);
      networkSummaries.push(`${providerLabel}: ${labels.join(", ")}`);
    }
  });
  if (networkSummaries.length && input.pricingFocus !== "storage") {
    noteParts.push(`Network add-ons: ${networkSummaries.join(" | ")}.`);
  }
  if (input.pricingFocus === "all" && vmCount && vmCount > 1) {
    const countLabel = mode === "k8s" ? "nodes" : "VMs";
    noteParts.push(`Totals include ${vmCount} ${countLabel}.`);
  }
  formNote.textContent = noteParts.join(" ");
  if (currentResultsTab === "insight") {
    renderInsight(data);
  }
  if (currentResultsTab === "commit") {
    renderCommit(data);
  }
  renderDataQualityPanel(data);
  renderUnitEconomics(data);
  renderRecommendations(data);
  updateDisclaimerText(data);
  setView(currentView);
  return data;
}

async function handleCompare(event) {
  if (event) {
    event.preventDefault();
  }
  if (scenarioDelta) {
    scenarioDelta.classList.add("is-hidden");
    scenarioDelta.textContent = "";
  }
  if (currentResultsTab === "saved") {
    await refreshSavedCompare();
    return;
  }
  const isRegionCompare =
    currentVendorView === "regions" &&
    (currentView === "aws" || currentView === "azure" || currentView === "gcp");
  formNote.textContent =
    currentView === "compare" ||
    currentResultsTab === "insight" ||
    currentResultsTab === "commit"
      ? "Fetching live prices..."
      : isRegionCompare
      ? "Fetching region compare..."
      : "Fetching vendor options...";
  try {
    if (currentResultsTab === "insight" || currentResultsTab === "commit") {
      await fetchAndRender();
    } else if (currentView === "compare") {
      await fetchAndRender();
    } else if (
      currentVendorView === "regions" &&
      (currentView === "aws" || currentView === "azure" || currentView === "gcp")
    ) {
      await runRegionCompare();
    } else {
      await fetchVendorOptions();
    }
  } catch (error) {
    formNote.textContent =
      error?.message || "Could not fetch pricing. Try again.";
  }
}

function escapeCsv(value) {
  if (value === null || value === undefined) {
    return "";
  }
  const text = String(value);
  if (/[",\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function parseCsvRows(text) {
  if (typeof text === "string" && text.charCodeAt(0) === 0xfeff) {
    text = text.slice(1);
  }
  const rows = [];
  let row = [];
  let value = "";
  let inQuotes = false;
  const pushValue = () => {
    row.push(value);
    value = "";
  };
  const pushRow = () => {
    if (row.length || value) {
      pushValue();
      rows.push(row);
      row = [];
    }
  };
  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    if (inQuotes) {
      if (char === '"') {
        if (text[i + 1] === '"') {
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
    if (char === ",") {
      pushValue();
      continue;
    }
    if (char === "\n") {
      pushRow();
      continue;
    }
    if (char === "\r") {
      continue;
    }
    value += char;
  }
  pushRow();
  return rows;
}

function normalizeCsvHeader(value) {
  return String(value || "").trim().toLowerCase();
}

function parseCsvBoolean(value) {
  const text = String(value || "").trim().toLowerCase();
  if (["true", "yes", "1", "on"].includes(text)) {
    return true;
  }
  if (["false", "no", "0", "off"].includes(text)) {
    return false;
  }
  return undefined;
}

function normalizeBillingProvider(provider) {
  if (provider === "unified") {
    return "unified";
  }
  if (provider === "azure") {
    return "azure";
  }
  if (provider === "gcp") {
    return "gcp";
  }
  if (provider === "rackspace") {
    return "rackspace";
  }
  return "aws";
}

function getBillingImportFormatHint(provider) {
  const normalized = normalizeBillingProvider(provider);
  if (normalized === "unified") {
    return "Unified view: combines imported AWS, Azure, GCP, and Rackspace billing datasets.";
  }
  if (normalized === "aws") {
    return "AWS CSV format: Cost Explorer Service view (Cost and usage breakdown by Service). Account labels are auto-derived from filename.";
  }
  if (normalized === "azure") {
    return "Azure CSV format: Cost Analysis Meter view. Account labels are auto-derived from filename.";
  }
  if (normalized === "gcp") {
    return "GCP CSV format: Billing export with service/SKU/cost columns. Account labels are auto-derived from filename.";
  }
  return "Rackspace CSV format: invoice usage export with SERVICE_TYPE and AMOUNT columns. Account labels are auto-derived from filename.";
}

function getBillingProviderDisplayName(provider) {
  const normalized = normalizeBillingProvider(provider);
  if (normalized === "aws") {
    return "AWS";
  }
  if (normalized === "azure") {
    return "Azure";
  }
  if (normalized === "gcp") {
    return "GCP";
  }
  if (normalized === "rackspace") {
    return "Rackspace";
  }
  return "Unified";
}

function normalizeBillingAccountName(value, fallback = "") {
  const text = String(value || "").trim();
  if (text) {
    return text;
  }
  return String(fallback || "").trim();
}

function normalizeBillingTagEntry(entry) {
  const productApp = String(entry?.productApp || "").trim();
  const tags = [];
  const seen = new Set();
  if (Array.isArray(entry?.tags)) {
    entry.tags.forEach((value) => {
      const text = String(value || "").trim();
      if (!text) {
        return;
      }
      const key = text.toLowerCase();
      if (seen.has(key)) {
        return;
      }
      seen.add(key);
      tags.push(text);
    });
  }
  return { productApp, tags };
}

function normalizeBillingProviderTags(providerTags) {
  const normalized = {};
  if (!providerTags || typeof providerTags !== "object") {
    return normalized;
  }
  Object.entries(providerTags).forEach(([serviceName, entry]) => {
    const service = String(serviceName || "").trim();
    if (!service) {
      return;
    }
    const tagEntry = normalizeBillingTagEntry(entry);
    if (!tagEntry.productApp && !tagEntry.tags.length) {
      return;
    }
    normalized[service] = tagEntry;
  });
  return normalized;
}

function normalizeBillingProviderDetailTags(providerTags) {
  const normalized = {};
  if (!providerTags || typeof providerTags !== "object") {
    return normalized;
  }
  Object.entries(providerTags).forEach(([serviceName, detailMap]) => {
    const service = String(serviceName || "").trim();
    if (!service || !detailMap || typeof detailMap !== "object") {
      return;
    }
    const normalizedDetailMap = {};
    Object.entries(detailMap).forEach(([detailName, entry]) => {
      const detail = String(detailName || "").trim();
      if (!detail) {
        return;
      }
      const tagEntry = normalizeBillingTagEntry(entry);
      if (!tagEntry.productApp && !tagEntry.tags.length) {
        return;
      }
      normalizedDetailMap[detail] = tagEntry;
    });
    if (Object.keys(normalizedDetailMap).length) {
      normalized[service] = normalizedDetailMap;
    }
  });
  return normalized;
}

function normalizeBillingTagStore(store) {
  const base = { aws: {}, azure: {}, gcp: {}, rackspace: {} };
  if (!store || typeof store !== "object") {
    return base;
  }
  base.aws = normalizeBillingProviderTags(store.aws);
  base.azure = normalizeBillingProviderTags(store.azure);
  base.gcp = normalizeBillingProviderTags(store.gcp);
  base.rackspace = normalizeBillingProviderTags(store.rackspace);
  return base;
}

function normalizeBillingDetailTagStore(store) {
  const base = { aws: {}, azure: {}, gcp: {}, rackspace: {} };
  if (!store || typeof store !== "object") {
    return base;
  }
  base.aws = normalizeBillingProviderDetailTags(store.aws);
  base.azure = normalizeBillingProviderDetailTags(store.azure);
  base.gcp = normalizeBillingProviderDetailTags(store.gcp);
  base.rackspace = normalizeBillingProviderDetailTags(store.rackspace);
  return base;
}

function normalizeBillingMonthKey(value) {
  const text = String(value || "").trim().toLowerCase();
  if (!text) {
    return BILLING_MONTH_UNKNOWN_KEY;
  }
  if (text === BILLING_MONTH_UNKNOWN_KEY) {
    return BILLING_MONTH_UNKNOWN_KEY;
  }
  if (/^\d{4}-(0[1-9]|1[0-2])$/.test(text)) {
    return text;
  }
  return BILLING_MONTH_UNKNOWN_KEY;
}

function normalizeBillingYearKey(value) {
  const text = String(value || "").trim();
  if (/^\d{4}$/.test(text)) {
    return text;
  }
  return "";
}

function normalizeBillingPeriodValue(value) {
  const text = String(value || "")
    .trim()
    .toLowerCase();
  if (!text || text === "all") {
    return "all";
  }
  if (text === BILLING_MONTH_UNKNOWN_KEY) {
    return BILLING_MONTH_UNKNOWN_KEY;
  }
  if (/^\d{4}-(0[1-9]|1[0-2])$/.test(text)) {
    return text;
  }
  const yearMatch = text.match(/^year:(\d{4})$/);
  if (yearMatch) {
    const year = normalizeBillingYearKey(yearMatch[1]);
    if (year) {
      return `year:${year}`;
    }
  }
  if (/^\d{4}$/.test(text)) {
    return `year:${text}`;
  }
  return "all";
}

function formatBillingMonthLabel(monthKey) {
  const normalized = normalizeBillingMonthKey(monthKey);
  if (normalized === BILLING_MONTH_UNKNOWN_KEY) {
    return "Unknown date";
  }
  const [yearText, monthText] = normalized.split("-");
  const year = Number.parseInt(yearText, 10);
  const month = Number.parseInt(monthText, 10);
  if (!Number.isFinite(year) || !Number.isFinite(month)) {
    return normalized;
  }
  return new Date(year, month - 1, 1).toLocaleString(undefined, {
    month: "long",
    year: "numeric",
  });
}

function formatBillingPeriodLabel(periodValue) {
  const normalized = normalizeBillingPeriodValue(periodValue);
  if (normalized === "all") {
    return "All months";
  }
  if (normalized.startsWith("year:")) {
    return `Yearly ${normalized.slice(5)}`;
  }
  return formatBillingMonthLabel(normalized);
}

function extractBillingYearFromMonthKey(monthKey) {
  const normalized = normalizeBillingMonthKey(monthKey);
  if (normalized === BILLING_MONTH_UNKNOWN_KEY) {
    return "";
  }
  return normalizeBillingYearKey(normalized.split("-")[0]);
}

function getBillingDatasetUsageRange(dataset) {
  if (!dataset || !Array.isArray(dataset.services)) {
    return { min: null, max: null };
  }
  let min = null;
  let max = null;
  dataset.services.forEach((service) => {
    if (Number.isFinite(service?.minDate)) {
      min =
        min === null ? Number(service.minDate) : Math.min(min, Number(service.minDate));
    }
    if (Number.isFinite(service?.maxDate)) {
      max =
        max === null ? Number(service.maxDate) : Math.max(max, Number(service.maxDate));
    }
  });
  return { min, max };
}

function monthKeyFromTimestamp(timestamp) {
  if (!Number.isFinite(timestamp)) {
    return BILLING_MONTH_UNKNOWN_KEY;
  }
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return BILLING_MONTH_UNKNOWN_KEY;
  }
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}`;
}

function inferBillingDatasetMonthKey(dataset) {
  const range = getBillingDatasetUsageRange(dataset);
  if (!Number.isFinite(range.min) && !Number.isFinite(range.max)) {
    return BILLING_MONTH_UNKNOWN_KEY;
  }
  const minKey = monthKeyFromTimestamp(
    Number.isFinite(range.min) ? range.min : range.max
  );
  const maxKey = monthKeyFromTimestamp(
    Number.isFinite(range.max) ? range.max : range.min
  );
  if (minKey === maxKey) {
    return minKey;
  }
  return BILLING_MONTH_UNKNOWN_KEY;
}

function normalizeBillingSourceSignatures(value) {
  const entries = Array.isArray(value) ? value : [];
  const seen = new Set();
  const signatures = [];
  entries.forEach((entry) => {
    const signature = String(entry || "").trim();
    if (!signature || seen.has(signature)) {
      return;
    }
    seen.add(signature);
    signatures.push(signature);
  });
  return signatures;
}

function cloneBillingDataset(dataset) {
  if (!dataset || typeof dataset !== "object" || Array.isArray(dataset)) {
    return null;
  }
  return {
    ...dataset,
    services: Array.isArray(dataset.services)
      ? dataset.services.map((service) => ({
          ...service,
          details: Array.isArray(service.details)
            ? service.details.map((detail) => ({ ...detail }))
            : [],
        }))
      : [],
    sourceFiles: Array.isArray(dataset.sourceFiles) ? [...dataset.sourceFiles] : [],
    sourceAccounts: Array.isArray(dataset.sourceAccounts)
      ? dataset.sourceAccounts.map((account) => ({ ...account }))
      : [],
    sourceSignatures: normalizeBillingSourceSignatures(dataset.sourceSignatures),
  };
}

function normalizeBillingImportDataset(dataset) {
  if (!dataset || typeof dataset !== "object" || Array.isArray(dataset)) {
    return null;
  }
  if (!Array.isArray(dataset.services)) {
    return null;
  }
  return cloneBillingDataset(dataset);
}

async function computeBillingImportSignature(text, provider) {
  const normalizedProvider = normalizeBillingProvider(provider);
  const normalizedText = String(text || "").replace(/\r\n/g, "\n");
  const payload = `${normalizedProvider}\n${normalizedText}`;
  try {
    if (window.crypto?.subtle && typeof TextEncoder !== "undefined") {
      const encoded = new TextEncoder().encode(payload);
      const digest = await window.crypto.subtle.digest("SHA-256", encoded);
      const bytes = new Uint8Array(digest);
      const hash = Array.from(bytes)
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join("");
      return `sha256:${hash}`;
    }
  } catch (error) {
    // Fallback hash below.
  }
  let rolling = 0;
  for (let index = 0; index < payload.length; index += 1) {
    rolling = (rolling * 31 + payload.charCodeAt(index)) >>> 0;
  }
  return `fallback:${normalizedProvider}:${payload.length}:${rolling.toString(16)}`;
}

function normalizeBillingProviderImportEntry(entry, provider = "aws") {
  const normalizedProvider = normalizeBillingProvider(provider);
  const base = { months: {} };
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
    return base;
  }
  if (entry.months && typeof entry.months === "object" && !Array.isArray(entry.months)) {
    Object.entries(entry.months).forEach(([rawMonthKey, dataset]) => {
      const normalizedDataset = normalizeBillingImportDataset(dataset);
      if (!normalizedDataset) {
        return;
      }
      normalizedDataset.provider = normalizedProvider;
      const monthKey = normalizeBillingMonthKey(rawMonthKey);
      const current = base.months[monthKey];
      base.months[monthKey] = current
        ? mergeBillingImportDatasets(normalizedProvider, [
            current,
            normalizedDataset,
          ])
        : normalizedDataset;
    });
    return base;
  }
  const legacyDataset = normalizeBillingImportDataset(entry);
  if (!legacyDataset) {
    return base;
  }
  legacyDataset.provider = normalizedProvider;
  base.months[inferBillingDatasetMonthKey(legacyDataset)] = legacyDataset;
  return base;
}

function buildEmptyBillingImportStore() {
  return {
    aws: { months: {} },
    azure: { months: {} },
    gcp: { months: {} },
    rackspace: { months: {} },
  };
}

function normalizeBillingImportStore(store) {
  const base = buildEmptyBillingImportStore();
  if (!store || typeof store !== "object") {
    return base;
  }
  BILLING_IMPORT_PROVIDERS.forEach((provider) => {
    base[provider] = normalizeBillingProviderImportEntry(store[provider], provider);
  });
  return base;
}

function getBillingProviderImportEntry(provider) {
  const normalizedProvider = normalizeBillingProvider(provider);
  if (normalizedProvider === "unified") {
    return { months: {} };
  }
  const entry = billingImportStore[normalizedProvider];
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
    billingImportStore[normalizedProvider] = { months: {} };
    return billingImportStore[normalizedProvider];
  }
  if (!entry.months || typeof entry.months !== "object" || Array.isArray(entry.months)) {
    entry.months = {};
  }
  return entry;
}

function getBillingProviderMonthMap(provider) {
  return getBillingProviderImportEntry(provider).months;
}

function listBillingMonthKeysForProvider(provider) {
  const months = getBillingProviderMonthMap(provider);
  const keys = Object.keys(months || {}).map((key) => normalizeBillingMonthKey(key));
  const unique = Array.from(new Set(keys));
  return unique.sort((left, right) => {
    if (left === BILLING_MONTH_UNKNOWN_KEY) {
      return 1;
    }
    if (right === BILLING_MONTH_UNKNOWN_KEY) {
      return -1;
    }
    return right.localeCompare(left);
  });
}

function listBillingMonthKeysForUnified() {
  const keys = new Set();
  BILLING_IMPORT_PROVIDERS.forEach((provider) => {
    listBillingMonthKeysForProvider(provider).forEach((monthKey) => {
      keys.add(monthKey);
    });
  });
  return Array.from(keys).sort((left, right) => {
    if (left === BILLING_MONTH_UNKNOWN_KEY) {
      return 1;
    }
    if (right === BILLING_MONTH_UNKNOWN_KEY) {
      return -1;
    }
    return right.localeCompare(left);
  });
}

function listBillingMonthKeys(provider = currentBillingProvider) {
  const normalizedProvider = normalizeBillingProvider(provider);
  if (normalizedProvider === "unified") {
    return listBillingMonthKeysForUnified();
  }
  return listBillingMonthKeysForProvider(normalizedProvider);
}

function listBillingYearKeys(provider = currentBillingProvider) {
  const years = new Set();
  listBillingMonthKeys(provider).forEach((monthKey) => {
    const year = extractBillingYearFromMonthKey(monthKey);
    if (year) {
      years.add(year);
    }
  });
  return Array.from(years).sort((left, right) => right.localeCompare(left));
}

function getBillingCurrentMonth(provider = currentBillingProvider) {
  const key = normalizeBillingProvider(provider);
  return normalizeBillingPeriodValue(billingMonthSelections[key] || "all");
}

function setBillingCurrentMonth(value, provider = currentBillingProvider) {
  const key = normalizeBillingProvider(provider);
  billingMonthSelections[key] = normalizeBillingPeriodValue(value);
}

function getBillingProviderDataByMonth(provider, monthKey = "all", options = {}) {
  const fallbackToAllWhenMissing = options.fallbackToAllWhenMissing !== false;
  const normalizedProvider = normalizeBillingProvider(provider);
  if (normalizedProvider === "unified") {
    return null;
  }
  const monthMap = getBillingProviderMonthMap(normalizedProvider);
  const entries = Object.entries(monthMap)
    .map(([rawMonthKey, dataset]) => ({
      monthKey: normalizeBillingMonthKey(rawMonthKey),
      dataset,
    }))
    .filter(({ dataset }) => dataset && Array.isArray(dataset.services));
  if (!entries.length) {
    return null;
  }
  const normalizedPeriod = normalizeBillingPeriodValue(monthKey);
  if (normalizedPeriod === "all") {
    return mergeBillingImportDatasets(
      normalizedProvider,
      entries.map((entry) => entry.dataset)
    );
  }
  if (normalizedPeriod.startsWith("year:")) {
    const year = normalizeBillingYearKey(normalizedPeriod.slice(5));
    const yearEntries = entries.filter((entry) => entry.monthKey.startsWith(`${year}-`));
    if (yearEntries.length) {
      return mergeBillingImportDatasets(
        normalizedProvider,
        yearEntries.map((entry) => entry.dataset)
      );
    }
  } else {
    const targetMonth = normalizeBillingMonthKey(normalizedPeriod);
    const match = entries.find((entry) => entry.monthKey === targetMonth);
    if (match) {
      return match.dataset;
    }
  }
  if (!fallbackToAllWhenMissing) {
    return null;
  }
  return mergeBillingImportDatasets(
    normalizedProvider,
    entries.map((entry) => entry.dataset)
  );
}

function loadBillingImportStore() {
  try {
    const raw = readPricingStorageValue(BILLING_IMPORT_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    return normalizeBillingImportStore(parsed);
  } catch (error) {
    // Ignore storage errors.
  }
  return buildEmptyBillingImportStore();
}

function persistBillingImportStore(store) {
  writePricingStorageValue(BILLING_IMPORT_KEY, JSON.stringify(store));
}

function loadBillingTagsStore() {
  try {
    const raw = readPricingStorageValue(BILLING_TAGS_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    return normalizeBillingTagStore(parsed);
  } catch (error) {
    return { aws: {}, azure: {}, gcp: {}, rackspace: {} };
  }
}

function persistBillingTagsStore(store) {
  const normalized = normalizeBillingTagStore(store);
  writePricingStorageValue(BILLING_TAGS_KEY, JSON.stringify(normalized));
}

function loadBillingDetailTagsStore() {
  try {
    const raw = readPricingStorageValue(BILLING_DETAIL_TAGS_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    return normalizeBillingDetailTagStore(parsed);
  } catch (error) {
    return { aws: {}, azure: {}, gcp: {}, rackspace: {} };
  }
}

function persistBillingDetailTagsStore(store) {
  const normalized = normalizeBillingDetailTagStore(store);
  writePricingStorageValue(BILLING_DETAIL_TAGS_KEY, JSON.stringify(normalized));
}

function getBillingProviderTagMap(provider) {
  const key = normalizeBillingProvider(provider);
  if (!billingTagStore[key] || typeof billingTagStore[key] !== "object") {
    billingTagStore[key] = {};
  }
  return billingTagStore[key];
}

function getBillingProviderDetailTagMap(provider) {
  const key = normalizeBillingProvider(provider);
  if (
    !billingDetailTagStore[key] ||
    typeof billingDetailTagStore[key] !== "object"
  ) {
    billingDetailTagStore[key] = {};
  }
  return billingDetailTagStore[key];
}

function parseBillingCustomTags(text) {
  const seen = new Set();
  const tags = [];
  String(text || "")
    .split(",")
    .map((value) => value.trim())
    .forEach((value) => {
      if (!value) {
        return;
      }
      const key = value.toLowerCase();
      if (seen.has(key)) {
        return;
      }
      seen.add(key);
      tags.push(value);
    });
  return tags;
}

function getBillingServiceTag(provider, serviceName) {
  const service = String(serviceName || "").trim();
  if (!service) {
    return null;
  }
  const map = getBillingProviderTagMap(provider);
  const entry = normalizeBillingTagEntry(map[service]);
  if (!entry.productApp && !entry.tags.length) {
    return null;
  }
  return entry;
}

function setBillingServiceTag(provider, serviceName, entry) {
  const service = String(serviceName || "").trim();
  if (!service) {
    return;
  }
  const map = getBillingProviderTagMap(provider);
  const normalized = normalizeBillingTagEntry(entry);
  if (!normalized.productApp && !normalized.tags.length) {
    delete map[service];
    return;
  }
  map[service] = normalized;
}

function getBillingDetailTag(provider, serviceName, detailName) {
  const service = String(serviceName || "").trim();
  const detail = String(detailName || "").trim();
  if (!service || !detail) {
    return null;
  }
  const providerMap = getBillingProviderDetailTagMap(provider);
  const serviceMap = providerMap[service];
  if (!serviceMap || typeof serviceMap !== "object") {
    return null;
  }
  const entry = normalizeBillingTagEntry(serviceMap[detail]);
  if (!entry.productApp && !entry.tags.length) {
    return null;
  }
  return entry;
}

function setBillingDetailTag(provider, serviceName, detailName, entry) {
  const service = String(serviceName || "").trim();
  const detail = String(detailName || "").trim();
  if (!service || !detail) {
    return;
  }
  const providerMap = getBillingProviderDetailTagMap(provider);
  if (!providerMap[service] || typeof providerMap[service] !== "object") {
    providerMap[service] = {};
  }
  const normalized = normalizeBillingTagEntry(entry);
  if (!normalized.productApp && !normalized.tags.length) {
    delete providerMap[service][detail];
    if (!Object.keys(providerMap[service]).length) {
      delete providerMap[service];
    }
    return;
  }
  providerMap[service][detail] = normalized;
}

function encodeBillingDetailKey(serviceName, detailName) {
  return `${encodeURIComponent(serviceName || "")}::${encodeURIComponent(
    detailName || ""
  )}`;
}

function decodeBillingDetailKey(value) {
  const [encodedService = "", encodedDetail = ""] = String(value || "").split(
    "::"
  );
  return {
    serviceName: decodeURIComponent(encodedService),
    detailName: decodeURIComponent(encodedDetail),
  };
}

function getBillingCurrentFilter(provider = currentBillingProvider) {
  const key = normalizeBillingProvider(provider);
  return billingProductFilterSelections[key] || "";
}

function setBillingCurrentFilter(value, provider = currentBillingProvider) {
  const key = normalizeBillingProvider(provider);
  billingProductFilterSelections[key] = String(value || "");
}

function getBillingCurrentMonthForProvider(provider = currentBillingProvider) {
  return getBillingCurrentMonth(provider);
}

function setBillingCurrentMonthForProvider(value, provider = currentBillingProvider) {
  setBillingCurrentMonth(value, provider);
}

function getBillingCurrentChartGroup(provider = currentBillingProvider) {
  const key = normalizeBillingProvider(provider);
  return billingChartGroupSelections[key] || "service";
}

function setBillingCurrentChartGroup(value, provider = currentBillingProvider) {
  const key = normalizeBillingProvider(provider);
  const normalized =
    value === "productApp" || value === "tag" ? value : "service";
  billingChartGroupSelections[key] = normalized;
}

function getBillingSelectedDetailSet(provider = currentBillingProvider) {
  const key = normalizeBillingProvider(provider);
  if (!(billingSelectedDetailKeys[key] instanceof Set)) {
    billingSelectedDetailKeys[key] = new Set();
  }
  return billingSelectedDetailKeys[key];
}

function getBillingSelectedDetailCount(provider = currentBillingProvider) {
  const selected = getBillingSelectedDetailSet(provider);
  return selected.size;
}

function updateBillingBulkSelectionSummary(provider = currentBillingProvider) {
  if (!billingBulkCount) {
    return;
  }
  const count = getBillingSelectedDetailCount(provider);
  billingBulkCount.value = `${count} selected`;
}

function getBillingFilteredServices(data, provider = currentBillingProvider) {
  const services = Array.isArray(data?.services) ? data.services : [];
  const filterValue = getBillingCurrentFilter(provider);
  const isUnified = normalizeBillingProvider(provider) === "unified";
  if (!filterValue) {
    return services.map((service) => ({
      ...service,
      details: Array.isArray(service.details) ? service.details : [],
      isPartial: false,
    }));
  }

  const filtered = [];
  services.forEach((service) => {
    const details = Array.isArray(service.details) ? service.details : [];
    const tagProvider = isUnified
      ? normalizeBillingProvider(service.sourceProvider)
      : normalizeBillingProvider(provider);
    const tagServiceName = isUnified
      ? String(service.sourceServiceName || service.name)
      : service.name;
    const entry = getBillingServiceTag(tagProvider, tagServiceName);
    const productApp = entry?.productApp || "";
    if (productApp && filterValue !== BILLING_UNTAGGED_FILTER) {
      if (productApp === filterValue) {
        filtered.push({
          ...service,
          details,
          isPartial: false,
        });
      }
      return;
    }
    const matchingDetails = details.filter((detail) => {
      const detailEntry = getBillingDetailTag(
        tagProvider,
        tagServiceName,
        detail.name
      );
      const detailProductApp = detailEntry?.productApp || "";
      if (filterValue === BILLING_UNTAGGED_FILTER) {
        return !productApp && !detailProductApp;
      }
      return detailProductApp === filterValue;
    });
    if (filterValue === BILLING_UNTAGGED_FILTER) {
      if (productApp) {
        return;
      }
      if (!matchingDetails.length) {
        return;
      }
      const partialCost = matchingDetails.reduce(
        (sum, detail) => sum + (Number.isFinite(detail.cost) ? detail.cost : 0),
        0
      );
      filtered.push({
        ...service,
        cost: partialCost,
        rowCount: matchingDetails.reduce(
          (sum, detail) => sum + (Number.isFinite(detail.rowCount) ? detail.rowCount : 0),
          0
        ),
        detailCount: matchingDetails.length,
        details: matchingDetails,
        isPartial: true,
      });
      return;
    }
    if (!matchingDetails.length) {
      return;
    }
    const partialCost = matchingDetails.reduce(
      (sum, detail) => sum + (Number.isFinite(detail.cost) ? detail.cost : 0),
      0
    );
    filtered.push({
      ...service,
      cost: partialCost,
      rowCount: matchingDetails.reduce(
        (sum, detail) => sum + (Number.isFinite(detail.rowCount) ? detail.rowCount : 0),
        0
      ),
      detailCount: matchingDetails.length,
      details: matchingDetails,
      isPartial: true,
    });
  });
  return filtered;
}

function updateBillingTagEditorFields() {
  if (!billingTagService || !billingTagProductApp || !billingTagCustom) {
    return;
  }
  const serviceName = billingTagService.value;
  if (!serviceName) {
    billingTagProductApp.value = "";
    billingTagCustom.value = "";
    return;
  }
  const entry = getBillingServiceTag(currentBillingProvider, serviceName);
  billingTagProductApp.value = entry?.productApp || "";
  billingTagCustom.value = Array.isArray(entry?.tags)
    ? entry.tags.join(", ")
    : "";
}

function syncBillingControls(data) {
  const normalizedProvider = normalizeBillingProvider(currentBillingProvider);
  const isUnified = normalizedProvider === "unified";
  const hasData = Boolean(data && Array.isArray(data.services) && data.services.length);
  const monthKeys = listBillingMonthKeys(currentBillingProvider);
  const yearKeys = listBillingYearKeys(currentBillingProvider);
  if (billingImportButton) {
    billingImportButton.disabled = isUnified;
  }
  if (billingClearButton) {
    billingClearButton.disabled = isUnified || !hasData;
  }
  if (billingExportButton) {
    billingExportButton.disabled = !hasData;
  }
  if (billingMonthFilter) {
    const monthOptions = [{ key: "all", label: "All months" }];
    yearKeys.forEach((yearKey) => {
      monthOptions.push({
        key: `year:${yearKey}`,
        label: `Yearly ${yearKey}`,
      });
    });
    monthKeys.forEach((monthKey) => {
      monthOptions.push({
        key: monthKey,
        label: formatBillingMonthLabel(monthKey),
      });
    });
    const currentMonth = getBillingCurrentMonth(currentBillingProvider);
    setSelectOptionsWithLabels(billingMonthFilter, monthOptions, currentMonth);
    const selected = billingMonthFilter.value || "all";
    setBillingCurrentMonth(selected, currentBillingProvider);
    billingMonthFilter.disabled = monthOptions.length <= 1;
  }
  if (billingProductFilter) {
    const currentValue = getBillingCurrentFilter(currentBillingProvider);
    const options = [
      { key: "", label: "All product apps" },
      { key: BILLING_UNTAGGED_FILTER, label: "Untagged" },
    ];
    if (hasData) {
      const apps = new Set();
      data.services.forEach((service) => {
        const sourceProvider = isUnified
          ? normalizeBillingProvider(service.sourceProvider)
          : normalizedProvider;
        const sourceServiceName = isUnified
          ? String(service.sourceServiceName || service.name)
          : service.name;
        const tagMap = getBillingProviderTagMap(sourceProvider);
        const detailTagMap = getBillingProviderDetailTagMap(sourceProvider);
        const entry = normalizeBillingTagEntry(tagMap[sourceServiceName]);
        if (entry.productApp) {
          apps.add(entry.productApp);
        }
        const detailTags = detailTagMap[sourceServiceName];
        if (!detailTags || typeof detailTags !== "object") {
          return;
        }
        Object.values(detailTags).forEach((detailEntry) => {
          const normalized = normalizeBillingTagEntry(detailEntry);
          if (normalized.productApp) {
            apps.add(normalized.productApp);
          }
        });
      });
      Array.from(apps)
        .sort((a, b) => a.localeCompare(b))
        .forEach((name) => {
          options.push({ key: name, label: name });
        });
    }
    setSelectOptionsWithLabels(billingProductFilter, options, currentValue);
    const selected = billingProductFilter.value;
    setBillingCurrentFilter(selected, currentBillingProvider);
    billingProductFilter.disabled = !hasData;
  }
  if (billingChartGroup) {
    const chartOptions = [
      { key: "service", label: "Service" },
      { key: "account", label: "Account" },
      { key: "productApp", label: "Product app" },
      { key: "tag", label: "Tag" },
    ];
    const chartGroup = getBillingCurrentChartGroup(currentBillingProvider);
    setSelectOptionsWithLabels(billingChartGroup, chartOptions, chartGroup);
    const selected = billingChartGroup.value || "service";
    setBillingCurrentChartGroup(selected, currentBillingProvider);
    billingChartGroup.disabled = !hasData;
  }
  if (billingTagService) {
    const serviceOptions = [{ key: "", label: "Select service" }];
    if (hasData && !isUnified) {
      data.services.forEach((service) => {
        serviceOptions.push({ key: service.name, label: service.name });
      });
    }
    const currentService = billingTagService.value;
    setSelectOptionsWithLabels(billingTagService, serviceOptions, currentService);
    billingTagService.disabled = !hasData || isUnified;
  }
  if (billingTagProductApp) {
    billingTagProductApp.disabled = !hasData || isUnified;
  }
  if (billingTagCustom) {
    billingTagCustom.disabled = !hasData || isUnified;
  }
  if (billingApplyTagsButton) {
    billingApplyTagsButton.disabled = !hasData || isUnified;
  }
  if (billingClearTagsButton) {
    billingClearTagsButton.disabled = !hasData || isUnified;
  }
  if (billingBulkProductApp) {
    billingBulkProductApp.disabled = !hasData || isUnified;
  }
  if (billingBulkTags) {
    billingBulkTags.disabled = !hasData || isUnified;
  }
  const selectedCount = getBillingSelectedDetailCount(currentBillingProvider);
  if (billingBulkSelectVisible) {
    billingBulkSelectVisible.disabled = !hasData || isUnified;
  }
  if (billingBulkClearSelection) {
    billingBulkClearSelection.disabled = !hasData || isUnified || selectedCount === 0;
  }
  if (billingBulkApply) {
    billingBulkApply.disabled = !hasData || isUnified || selectedCount === 0;
  }
  if (billingBulkClearTags) {
    billingBulkClearTags.disabled = !hasData || isUnified || selectedCount === 0;
  }
  updateBillingBulkSelectionSummary(currentBillingProvider);
  updateBillingTagEditorFields();
}

function parseBillingCurrency(value) {
  let text = String(value ?? "").trim();
  if (!text) {
    return Number.NaN;
  }
  let negative = false;
  if (text.startsWith("(") && text.endsWith(")")) {
    negative = true;
    text = text.slice(1, -1);
  }
  text = text.replace(/[$,%\s]/g, "");
  text = text.replace(/,/g, "");
  text = text.replace(/[A-Za-z]/g, "");
  if (!text) {
    return Number.NaN;
  }
  const parsed = Number.parseFloat(text);
  if (!Number.isFinite(parsed)) {
    return Number.NaN;
  }
  return negative ? -parsed : parsed;
}

function findBillingHeaderIndex(headers, candidates = []) {
  for (const candidate of candidates) {
    const idx = headers.indexOf(candidate);
    if (idx >= 0) {
      return idx;
    }
  }
  for (let idx = 0; idx < headers.length; idx += 1) {
    const header = headers[idx];
    if (candidates.some((candidate) => header.includes(candidate))) {
      return idx;
    }
  }
  return -1;
}

const BILLING_SERVICE_CANDIDATES_BY_PROVIDER = {
  aws: [
    "product/productname",
    "product/servicename",
    "product/servicecode",
    "lineitem/productcode",
    "lineitem/lineitemdescription",
    "lineitem/usagetype",
  ],
  azure: [
    "servicename",
    "metercategory",
    "productname",
    "metername",
    "resourcegroup",
  ],
  gcp: [
    "service description",
    "service.description",
    "service",
    "sku description",
    "sku.description",
  ],
  rackspace: [
    "service_type",
    "impact_type",
    "event_type",
    "res_name",
    "attribute_1",
  ],
};

const BILLING_COST_CANDIDATES_BY_PROVIDER = {
  aws: [
    "lineitem/unblendedcost",
    "lineitem/netunblendedcost",
    "lineitem/blendedcost",
    "lineitem/netamortizedcost",
    "amortizedcost",
    "cost",
    "charge",
    "amount",
  ],
  azure: ["costinbillingcurrency", "pretaxcost", "cost", "charge", "amount"],
  gcp: ["cost", "net cost", "effective cost", "charge", "amount"],
  rackspace: ["amount", "cost", "charge", "rate"],
};

const BILLING_DETAIL_CANDIDATES_BY_PROVIDER = {
  aws: [
    "lineitem/lineitemdescription",
    "lineitem/usagetype",
    "lineitem/resourceid",
    "product/instancetype",
    "product/location",
    "lineitem/operation",
  ],
  azure: [
    "meter",
    "metername",
    "metercategory",
    "meterid",
    "resourcegroup",
    "resourcetype",
    "partnumber",
  ],
  gcp: [
    "sku description",
    "sku.description",
    "sku id",
    "sku.id",
    "project id",
    "project.id",
    "resource name",
  ],
  rackspace: [
    "res_name",
    "impact_type",
    "event_type",
    "res_id",
    "region_id",
    "attribute_1",
    "attribute_2",
    "attribute_3",
    "attribute_4",
    "attribute_5",
    "attribute_6",
    "attribute_7",
    "attribute_8",
  ],
};

const BILLING_CHARGE_TYPE_CANDIDATES_BY_PROVIDER = {
  aws: ["lineitem/lineitemtype", "lineitem/chargetype", "chargetype"],
  azure: ["chargetype", "charge type", "pricingmodel"],
  gcp: ["cost type", "costtype", "charge type"],
  rackspace: ["event_type", "impact_type"],
};

const BILLING_USAGE_DATE_CANDIDATES_BY_PROVIDER = {
  aws: ["lineitem/usagestartdate", "usagedate", "date"],
  azure: ["date", "usagedate", "billingperiodstartdate"],
  gcp: ["usage_start_time", "date", "usage date"],
  rackspace: [
    "event_start_date",
    "event_end_date",
    "bill_start_date",
    "bill_end_date",
    "usage_date",
    "date",
  ],
};

function scoreBillingProviderByHeaders(headers, provider) {
  const normalizedProvider = normalizeBillingProvider(provider);
  const headerSet = new Set(
    (Array.isArray(headers) ? headers : [])
      .map((header) => normalizeCsvHeader(header))
      .filter(Boolean)
  );
  let score = 0;
  BILLING_SERVICE_CANDIDATES_BY_PROVIDER[normalizedProvider].forEach((candidate) => {
    if (headerSet.has(candidate)) {
      score += 2;
    }
  });
  BILLING_COST_CANDIDATES_BY_PROVIDER[normalizedProvider].forEach((candidate) => {
    if (headerSet.has(candidate)) {
      score += 3;
    }
  });
  BILLING_DETAIL_CANDIDATES_BY_PROVIDER[normalizedProvider].forEach((candidate) => {
    if (headerSet.has(candidate)) {
      score += 1;
    }
  });
  BILLING_CHARGE_TYPE_CANDIDATES_BY_PROVIDER[normalizedProvider].forEach(
    (candidate) => {
      if (headerSet.has(candidate)) {
        score += 1;
      }
    }
  );
  BILLING_USAGE_DATE_CANDIDATES_BY_PROVIDER[normalizedProvider].forEach(
    (candidate) => {
      if (headerSet.has(candidate)) {
        score += 2;
      }
    }
  );

  if (normalizedProvider === "aws") {
    const hasAwsPrefix = Array.from(headerSet).some(
      (header) => header.startsWith("lineitem/") || header.startsWith("product/")
    );
    if (hasAwsPrefix) {
      score += 8;
    }
  } else if (normalizedProvider === "azure") {
    if (
      headerSet.has("costinbillingcurrency") ||
      headerSet.has("metercategory") ||
      headerSet.has("billingperiodstartdate")
    ) {
      score += 8;
    }
  } else if (normalizedProvider === "gcp") {
    if (
      headerSet.has("service.description") ||
      headerSet.has("sku.description") ||
      headerSet.has("usage_start_time")
    ) {
      score += 8;
    }
  } else if (normalizedProvider === "rackspace") {
    if (
      headerSet.has("service_type") ||
      headerSet.has("impact_type") ||
      headerSet.has("event_type")
    ) {
      score += 8;
    }
  }
  return score;
}

function detectBillingCsvProvider(rows, headers) {
  const normalizedHeaders = Array.isArray(headers)
    ? headers.map((header) => normalizeCsvHeader(header))
    : [];
  const scores = {
    aws: scoreBillingProviderByHeaders(normalizedHeaders, "aws"),
    azure: scoreBillingProviderByHeaders(normalizedHeaders, "azure"),
    gcp: scoreBillingProviderByHeaders(normalizedHeaders, "gcp"),
    rackspace: scoreBillingProviderByHeaders(normalizedHeaders, "rackspace"),
  };
  const awsMatrix = parseAwsServiceViewMatrix(rows || [], normalizedHeaders);
  if (awsMatrix) {
    scores.aws += 14;
  }
  const ranked = Object.entries(scores)
    .map(([provider, score]) => ({ provider, score }))
    .sort((left, right) => right.score - left.score);
  const top = ranked[0];
  const second = ranked[1];
  const confident =
    top &&
    top.score >= 8 &&
    (!second || top.score >= second.score + 3);
  return {
    provider: confident ? top.provider : null,
    scores,
  };
}

function validateBillingCsvProvider(rows, headers, expectedProvider) {
  const expected = normalizeBillingProvider(expectedProvider);
  const detected = detectBillingCsvProvider(rows, headers);
  if (detected.provider && detected.provider !== expected) {
    return {
      ok: false,
      expected,
      detected: detected.provider,
    };
  }
  return {
    ok: true,
    expected,
    detected: detected.provider,
  };
}

function addBillingCostRowToServiceTotals(
  totalsByService,
  serviceName,
  detailName,
  cost,
  chargeTypeRaw,
  usageTimestamp
) {
  let serviceBucket = totalsByService.get(serviceName);
  if (!serviceBucket) {
    serviceBucket = {
      name: serviceName,
      cost: 0,
      rowCount: 0,
      details: new Map(),
      chargeTypes: new Map(),
      minDate: null,
      maxDate: null,
    };
    totalsByService.set(serviceName, serviceBucket);
  }
  serviceBucket.cost += cost;
  serviceBucket.rowCount += 1;
  let detailBucket = serviceBucket.details.get(detailName);
  if (!detailBucket) {
    detailBucket = {
      name: detailName,
      cost: 0,
      rowCount: 0,
    };
    serviceBucket.details.set(detailName, detailBucket);
  }
  detailBucket.cost += cost;
  detailBucket.rowCount += 1;
  if (chargeTypeRaw) {
    serviceBucket.chargeTypes.set(
      chargeTypeRaw,
      (serviceBucket.chargeTypes.get(chargeTypeRaw) || 0) + 1
    );
  }
  if (Number.isFinite(usageTimestamp)) {
    if (!serviceBucket.minDate || usageTimestamp < serviceBucket.minDate) {
      serviceBucket.minDate = usageTimestamp;
    }
    if (!serviceBucket.maxDate || usageTimestamp > serviceBucket.maxDate) {
      serviceBucket.maxDate = usageTimestamp;
    }
  }
}

function buildBillingServicesFromTotals(totalsByService, totalCost) {
  return Array.from(totalsByService.values())
    .map((bucket) => {
      const details = Array.from(bucket.details.values())
        .map((detail) => ({
          name: detail.name,
          cost: detail.cost,
          rowCount: detail.rowCount,
          share: bucket.cost !== 0 ? (detail.cost / bucket.cost) * 100 : 0,
        }))
        .sort((a, b) => b.cost - a.cost);
      const topChargeTypes = Array.from(bucket.chargeTypes.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([name]) => name);
      return {
        name: bucket.name,
        cost: bucket.cost,
        rowCount: bucket.rowCount,
        share: totalCost !== 0 ? (bucket.cost / totalCost) * 100 : 0,
        detailCount: details.length,
        details,
        topChargeTypes,
        minDate: bucket.minDate,
        maxDate: bucket.maxDate,
      };
    })
    .sort((a, b) => b.cost - a.cost);
}

function escapeMarkup(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function parseAwsServiceViewMatrix(rows, headers) {
  const firstHeader = headers[0] || "";
  const totalCostsColumnIndex = headers.findIndex((header) =>
    header.includes("total costs")
  );
  if (
    firstHeader !== "service" ||
    rows.length < 2 ||
    totalCostsColumnIndex < 0 ||
    headers.length < 3
  ) {
    return null;
  }

  const dataRows = rows.slice(1);
  const summaryRow =
    dataRows.find((row) => normalizeCsvHeader(row[0]) === "service total") ||
    dataRows[0];
  if (!summaryRow) {
    return null;
  }
  const usageRow = dataRows.find((row) =>
    /^\d{4}-\d{2}-\d{2}/.test(String(row[0] || "").trim())
  );
  const usageTimestamp = usageRow
    ? new Date(String(usageRow[0] || "").trim()).getTime()
    : Number.NaN;

  const services = [];
  let totalCost = parseBillingCurrency(summaryRow[totalCostsColumnIndex]);
  for (let col = 1; col < headers.length; col += 1) {
    const normalizedHeader = headers[col];
    if (!normalizedHeader || normalizedHeader.includes("total costs")) {
      continue;
    }
    const rawName = String(rows[0][col] || "").trim();
    const serviceName = rawName.replace(/\s*\((\$|usd)\)\s*$/i, "").trim();
    if (!serviceName) {
      continue;
    }
    let serviceCost = parseBillingCurrency(summaryRow[col]);
    if (!Number.isFinite(serviceCost) && usageRow) {
      serviceCost = parseBillingCurrency(usageRow[col]);
    }
    if (!Number.isFinite(serviceCost)) {
      continue;
    }
    services.push({
      name: serviceName,
      cost: serviceCost,
      rowCount: 1,
      share: 0,
      detailCount: 1,
      details: [
        {
          name: String(summaryRow[0] || "Service total"),
          cost: serviceCost,
          rowCount: 1,
          share: 100,
        },
      ],
      topChargeTypes: [],
      minDate: Number.isFinite(usageTimestamp) ? usageTimestamp : null,
      maxDate: Number.isFinite(usageTimestamp) ? usageTimestamp : null,
    });
  }

  if (!services.length) {
    return null;
  }
  if (!Number.isFinite(totalCost)) {
    totalCost = services.reduce((sum, service) => sum + service.cost, 0);
  }
  services.forEach((service) => {
    service.share = totalCost !== 0 ? (service.cost / totalCost) * 100 : 0;
  });
  services.sort((a, b) => b.cost - a.cost);

  const payload = {
    provider: "aws",
    importedAt: new Date().toISOString(),
    rowCount: services.length,
    serviceCount: services.length,
    totalCost,
    serviceColumn: rows[0][0] || "Service",
    costColumn: rows[0][totalCostsColumnIndex] || "Total costs($)",
    detailColumn: String(summaryRow[0] || "Service total"),
    chargeTypeColumn: "",
    usageDateColumn: usageRow ? rows[0][0] || "Service" : "",
    services,
  };
  const monthKey = inferBillingDatasetMonthKey(payload);
  payload.monthDatasets = {
    [monthKey]: cloneBillingDataset(payload),
  };
  return payload;
}

function parseBillingImportCsv(text, provider) {
  const rows = parseCsvRows(text).filter((row) =>
    row.some((value) => String(value || "").trim() !== "")
  );
  if (rows.length < 2) {
    throw new Error("CSV needs a header and at least one data row.");
  }
  const normalizedProvider = normalizeBillingProvider(provider);
  const headers = rows[0].map((value) => normalizeCsvHeader(value));
  const providerValidation = validateBillingCsvProvider(
    rows,
    headers,
    normalizedProvider
  );
  if (!providerValidation.ok) {
    throw new Error(
      `CSV appears to be ${getBillingProviderDisplayName(
        providerValidation.detected
      )} billing data. Switch to ${getBillingProviderDisplayName(
        providerValidation.expected
      )} tab and import again.`
    );
  }
  if (normalizedProvider === "aws") {
    const matrixParse = parseAwsServiceViewMatrix(rows, headers);
    if (matrixParse) {
      return matrixParse;
    }
  }

  let serviceIndex = findBillingHeaderIndex(
    headers,
    BILLING_SERVICE_CANDIDATES_BY_PROVIDER[normalizedProvider]
  );
  let costIndex = findBillingHeaderIndex(
    headers,
    BILLING_COST_CANDIDATES_BY_PROVIDER[normalizedProvider]
  );
  if (serviceIndex < 0) {
    serviceIndex = findBillingHeaderIndex(headers, [
      "service",
      "product",
      "meter",
      "sku",
      "category",
      "name",
    ]);
  }
  if (costIndex < 0) {
    costIndex = findBillingHeaderIndex(headers, ["cost", "charge", "amount"]);
  }
  let detailIndex = findBillingHeaderIndex(
    headers,
    BILLING_DETAIL_CANDIDATES_BY_PROVIDER[normalizedProvider]
  );
  if (detailIndex < 0) {
    detailIndex = findBillingHeaderIndex(headers, [
      "meter",
      "sku",
      "usage",
      "description",
      "resource",
      "partnumber",
      "item",
      "name",
    ]);
  }
  const chargeTypeIndex = findBillingHeaderIndex(
    headers,
    BILLING_CHARGE_TYPE_CANDIDATES_BY_PROVIDER[normalizedProvider]
  );
  const usageDateIndex = findBillingHeaderIndex(
    headers,
    BILLING_USAGE_DATE_CANDIDATES_BY_PROVIDER[normalizedProvider]
  );
  if (costIndex < 0) {
    throw new Error("Could not find a cost column in CSV.");
  }
  if (serviceIndex < 0) {
    serviceIndex = 0;
  }
  if (detailIndex < 0) {
    detailIndex = serviceIndex;
  }

  const totalsByService = new Map();
  const totalsByMonth = new Map();
  let importedRows = 0;
  let totalCost = 0;
  for (const row of rows.slice(1)) {
    const cost = parseBillingCurrency(row[costIndex]);
    if (!Number.isFinite(cost)) {
      continue;
    }
    const rawService = String(row[serviceIndex] || "").trim();
    const service = rawService || "Uncategorized";
    const rawDetail = String(row[detailIndex] || "").trim();
    const detail = rawDetail || "Line item";
    const usageDateRaw =
      usageDateIndex >= 0 ? String(row[usageDateIndex] || "").trim() : "";
    const chargeTypeRaw =
      chargeTypeIndex >= 0 ? String(row[chargeTypeIndex] || "").trim() : "";
    const parsedDate = usageDateRaw ? new Date(usageDateRaw) : null;
    const usageTimestamp =
      parsedDate && !Number.isNaN(parsedDate.getTime())
        ? parsedDate.getTime()
        : null;
    addBillingCostRowToServiceTotals(
      totalsByService,
      service,
      detail,
      cost,
      chargeTypeRaw,
      usageTimestamp
    );
    const monthKey = normalizeBillingMonthKey(monthKeyFromTimestamp(usageTimestamp));
    let monthBucket = totalsByMonth.get(monthKey);
    if (!monthBucket) {
      monthBucket = {
        totalsByService: new Map(),
        rowCount: 0,
        totalCost: 0,
      };
      totalsByMonth.set(monthKey, monthBucket);
    }
    addBillingCostRowToServiceTotals(
      monthBucket.totalsByService,
      service,
      detail,
      cost,
      chargeTypeRaw,
      usageTimestamp
    );
    monthBucket.rowCount += 1;
    monthBucket.totalCost += cost;
    importedRows += 1;
    totalCost += cost;
  }
  if (!importedRows) {
    throw new Error("No numeric cost rows found in CSV.");
  }

  const services = buildBillingServicesFromTotals(totalsByService, totalCost);
  const payload = {
    provider: normalizedProvider,
    importedAt: new Date().toISOString(),
    rowCount: importedRows,
    serviceCount: services.length,
    totalCost,
    serviceColumn: rows[0][serviceIndex] || "Service",
    costColumn: rows[0][costIndex] || "Cost",
    detailColumn: rows[0][detailIndex] || "Detail",
    chargeTypeColumn: chargeTypeIndex >= 0 ? rows[0][chargeTypeIndex] : "",
    usageDateColumn: usageDateIndex >= 0 ? rows[0][usageDateIndex] : "",
    services,
    sourceDatasetCount: 1,
    sourceFiles: [],
    sourceAccounts: [],
  };
  const monthDatasets = {};
  totalsByMonth.forEach((monthBucket, monthKey) => {
    const monthServices = buildBillingServicesFromTotals(
      monthBucket.totalsByService,
      monthBucket.totalCost
    );
    monthDatasets[monthKey] = {
      provider: normalizedProvider,
      importedAt: payload.importedAt,
      rowCount: monthBucket.rowCount,
      serviceCount: monthServices.length,
      totalCost: monthBucket.totalCost,
      serviceColumn: payload.serviceColumn,
      costColumn: payload.costColumn,
      detailColumn: payload.detailColumn,
      chargeTypeColumn: payload.chargeTypeColumn,
      usageDateColumn: payload.usageDateColumn,
      services: monthServices,
      sourceDatasetCount: 1,
      sourceFiles: [],
      sourceAccounts: [],
    };
  });
  payload.monthDatasets = monthDatasets;
  return payload;
}

function pickMergedBillingColumn(datasets, key, fallback) {
  const values = new Set();
  datasets.forEach((dataset) => {
    const value = String(dataset?.[key] || "").trim();
    if (value) {
      values.add(value);
    }
  });
  if (!values.size) {
    return fallback;
  }
  if (values.size === 1) {
    return Array.from(values)[0];
  }
  return "Mixed";
}

function mergeBillingImportDatasets(provider, datasets = []) {
  const normalizedProvider = normalizeBillingProvider(provider);
  const validDatasets = datasets.filter(
    (dataset) => dataset && Array.isArray(dataset.services)
  );
  if (!validDatasets.length) {
    return null;
  }
  const sourceSignatures = [];
  const seenSourceSignatures = new Set();
  const dedupedDatasets = [];
  validDatasets.forEach((dataset) => {
    const signatures = normalizeBillingSourceSignatures(dataset.sourceSignatures);
    if (
      signatures.length &&
      signatures.every((signature) => seenSourceSignatures.has(signature))
    ) {
      return;
    }
    signatures.forEach((signature) => {
      if (seenSourceSignatures.has(signature)) {
        return;
      }
      seenSourceSignatures.add(signature);
      sourceSignatures.push(signature);
    });
    dedupedDatasets.push({
      ...dataset,
      sourceSignatures: signatures,
    });
  });
  if (!dedupedDatasets.length) {
    return null;
  }

  let totalCost = 0;
  let rowCount = 0;
  let latestImportedAt = 0;
  let sourceDatasetCount = 0;
  const sourceFiles = [];
  const sourceAccountMap = new Map();
  const serviceBuckets = new Map();

  dedupedDatasets.forEach((dataset) => {
    totalCost += Number.isFinite(dataset.totalCost) ? dataset.totalCost : 0;
    rowCount += Number.isFinite(dataset.rowCount) ? dataset.rowCount : 0;
    sourceDatasetCount += Number.isFinite(dataset.sourceDatasetCount)
      ? dataset.sourceDatasetCount
      : 1;
    if (Array.isArray(dataset.sourceFiles) && dataset.sourceFiles.length) {
      dataset.sourceFiles.forEach((name) => {
        const text = String(name || "").trim();
        if (text && !sourceFiles.includes(text)) {
          sourceFiles.push(text);
        }
      });
    }
    const defaultDatasetAccount = normalizeBillingAccountName(
      Array.isArray(dataset.sourceAccounts) && dataset.sourceAccounts.length === 1
        ? dataset.sourceAccounts[0]?.name
        : ""
    );
    if (Array.isArray(dataset.sourceAccounts) && dataset.sourceAccounts.length) {
      dataset.sourceAccounts.forEach((account) => {
        const accountName = normalizeBillingAccountName(account?.name);
        if (!accountName) {
          return;
        }
        if (!sourceAccountMap.has(accountName)) {
          sourceAccountMap.set(accountName, {
            name: accountName,
            cost: 0,
            rowCount: 0,
            fileCount: 0,
            datasetCount: 0,
          });
        }
        const target = sourceAccountMap.get(accountName);
        target.cost += Number.isFinite(account?.cost) ? account.cost : 0;
        target.rowCount += Number.isFinite(account?.rowCount) ? account.rowCount : 0;
        target.fileCount += Number.isFinite(account?.fileCount) ? account.fileCount : 0;
        target.datasetCount += Number.isFinite(account?.datasetCount)
          ? account.datasetCount
          : 1;
      });
    } else if (defaultDatasetAccount) {
      if (!sourceAccountMap.has(defaultDatasetAccount)) {
        sourceAccountMap.set(defaultDatasetAccount, {
          name: defaultDatasetAccount,
          cost: 0,
          rowCount: 0,
          fileCount: 0,
          datasetCount: 0,
        });
      }
      const target = sourceAccountMap.get(defaultDatasetAccount);
      target.cost += Number.isFinite(dataset.totalCost) ? dataset.totalCost : 0;
      target.rowCount += Number.isFinite(dataset.rowCount) ? dataset.rowCount : 0;
      target.fileCount +=
        Array.isArray(dataset.sourceFiles) && dataset.sourceFiles.length
          ? dataset.sourceFiles.length
          : 1;
      target.datasetCount += Number.isFinite(dataset.sourceDatasetCount)
        ? dataset.sourceDatasetCount
        : 1;
    }
    const importedAtMs = Date.parse(dataset.importedAt || "");
    if (Number.isFinite(importedAtMs)) {
      latestImportedAt = Math.max(latestImportedAt, importedAtMs);
    }

    dataset.services.forEach((service) => {
      const serviceName = String(service.name || "").trim() || "Uncategorized";
      const sourceAccountName = normalizeBillingAccountName(
        service.sourceAccountName,
        defaultDatasetAccount
      );
      const bucketKey = `${sourceAccountName}@@${serviceName}`;
      let bucket = serviceBuckets.get(bucketKey);
      if (!bucket) {
        bucket = {
          name: serviceName,
          sourceServiceName: String(
            service.sourceServiceName || serviceName
          ).trim(),
          sourceAccountName,
          cost: 0,
          rowCount: 0,
          minDate: null,
          maxDate: null,
          detailBuckets: new Map(),
          chargeTypeBuckets: new Map(),
        };
        serviceBuckets.set(bucketKey, bucket);
      }

      const serviceCost = Number.isFinite(service.cost) ? service.cost : 0;
      const serviceRows = Number.isFinite(service.rowCount) ? service.rowCount : 0;
      bucket.cost += serviceCost;
      bucket.rowCount += serviceRows;

      if (Number.isFinite(service.minDate)) {
        bucket.minDate =
          bucket.minDate === null
            ? service.minDate
            : Math.min(bucket.minDate, service.minDate);
      }
      if (Number.isFinite(service.maxDate)) {
        bucket.maxDate =
          bucket.maxDate === null
            ? service.maxDate
            : Math.max(bucket.maxDate, service.maxDate);
      }

      if (Array.isArray(service.topChargeTypes)) {
        service.topChargeTypes.forEach((chargeType) => {
          const key = String(chargeType || "").trim();
          if (!key) {
            return;
          }
          const count = bucket.chargeTypeBuckets.get(key) || 0;
          bucket.chargeTypeBuckets.set(key, count + 1);
        });
      }

      const details = Array.isArray(service.details) ? service.details : [];
      details.forEach((detail) => {
        const detailName = String(detail.name || "").trim() || "Line item";
        let detailBucket = bucket.detailBuckets.get(detailName);
        if (!detailBucket) {
          detailBucket = {
            name: detailName,
            cost: 0,
            rowCount: 0,
          };
          bucket.detailBuckets.set(detailName, detailBucket);
        }
        detailBucket.cost += Number.isFinite(detail.cost) ? detail.cost : 0;
        detailBucket.rowCount += Number.isFinite(detail.rowCount)
          ? detail.rowCount
          : 0;
      });
    });
  });

  const services = Array.from(serviceBuckets.values())
    .map((bucket) => {
      const details = Array.from(bucket.detailBuckets.values())
        .map((detail) => ({
          name: detail.name,
          cost: detail.cost,
          rowCount: detail.rowCount,
          share: bucket.cost !== 0 ? (detail.cost / bucket.cost) * 100 : 0,
        }))
        .sort((a, b) => b.cost - a.cost);
      const topChargeTypes = Array.from(bucket.chargeTypeBuckets.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([name]) => name);
      return {
        name: bucket.name,
        sourceServiceName: bucket.sourceServiceName || bucket.name,
        sourceAccountName: bucket.sourceAccountName || "",
        cost: bucket.cost,
        rowCount: bucket.rowCount,
        share: totalCost !== 0 ? (bucket.cost / totalCost) * 100 : 0,
        detailCount: details.length,
        details,
        topChargeTypes,
        minDate: bucket.minDate,
        maxDate: bucket.maxDate,
      };
    })
    .sort((a, b) => b.cost - a.cost);

  return {
    provider: normalizedProvider,
    importedAt: latestImportedAt
      ? new Date(latestImportedAt).toISOString()
      : new Date().toISOString(),
    rowCount,
    serviceCount: services.length,
    totalCost,
    serviceColumn: pickMergedBillingColumn(
      dedupedDatasets,
      "serviceColumn",
      "Service"
    ),
    costColumn: pickMergedBillingColumn(dedupedDatasets, "costColumn", "Cost"),
    detailColumn: pickMergedBillingColumn(
      dedupedDatasets,
      "detailColumn",
      "Detail"
    ),
    chargeTypeColumn: pickMergedBillingColumn(
      dedupedDatasets,
      "chargeTypeColumn",
      ""
    ),
    usageDateColumn: pickMergedBillingColumn(
      dedupedDatasets,
      "usageDateColumn",
      ""
    ),
    services,
    sourceDatasetCount,
    sourceFiles,
    sourceAccounts: Array.from(sourceAccountMap.values()).sort((a, b) =>
      a.name.localeCompare(b.name)
    ),
    sourceSignatures,
  };
}

function setBillingProvider(provider) {
  currentBillingProvider = normalizeBillingProvider(provider);
  billingProviderTabs.forEach((button) => {
    button.classList.toggle(
      "active",
      button.dataset.billingProvider === currentBillingProvider
    );
  });
  if (billingFormatHint) {
    billingFormatHint.textContent = getBillingImportFormatHint(
      currentBillingProvider
    );
  }
  renderBillingImportPanel();
}

function getBillingExpandedServiceSet(provider) {
  const key = normalizeBillingProvider(provider);
  if (!(billingExpandedServices[key] instanceof Set)) {
    billingExpandedServices[key] = new Set();
  }
  return billingExpandedServices[key];
}

function buildUnifiedBillingData(monthKey = "all") {
  const normalizedMonth = String(monthKey || "all")
    .trim()
    .toLowerCase();
  const sources = BILLING_IMPORT_PROVIDERS.map((provider) => ({
    provider,
    data: getBillingProviderDataByMonth(provider, normalizedMonth, {
      fallbackToAllWhenMissing: false,
    }),
  })).filter((entry) => entry.data && Array.isArray(entry.data.services));
  if (!sources.length) {
    return null;
  }
  const services = [];
  let totalCost = 0;
  let rowCount = 0;
  let latestImportedAt = 0;
  let sourceDatasetCount = 0;
  const sourceFiles = [];
  const sourceAccountMap = new Map();
  sources.forEach(({ provider, data }) => {
    const importedAtMs = Date.parse(data.importedAt || "");
    if (Number.isFinite(importedAtMs)) {
      latestImportedAt = Math.max(latestImportedAt, importedAtMs);
    }
    rowCount += Number.isFinite(data.rowCount) ? data.rowCount : 0;
    totalCost += Number.isFinite(data.totalCost) ? data.totalCost : 0;
    sourceDatasetCount += Number.isFinite(data.sourceDatasetCount)
      ? data.sourceDatasetCount
      : 1;
    if (Array.isArray(data.sourceFiles)) {
      data.sourceFiles.forEach((name) => {
        const text = String(name || "").trim();
        if (text && !sourceFiles.includes(text)) {
          sourceFiles.push(text);
        }
      });
    }
    if (Array.isArray(data.sourceAccounts)) {
      data.sourceAccounts.forEach((account) => {
        const accountName = normalizeBillingAccountName(account?.name);
        if (!accountName) {
          return;
        }
        const key = `${provider}@@${accountName}`;
        if (!sourceAccountMap.has(key)) {
          sourceAccountMap.set(key, {
            name: `${getBillingProviderDisplayName(provider)} / ${accountName}`,
            cost: 0,
            rowCount: 0,
            fileCount: 0,
            datasetCount: 0,
          });
        }
        const target = sourceAccountMap.get(key);
        target.cost += Number.isFinite(account?.cost) ? account.cost : 0;
        target.rowCount += Number.isFinite(account?.rowCount) ? account.rowCount : 0;
        target.fileCount += Number.isFinite(account?.fileCount) ? account.fileCount : 0;
        target.datasetCount += Number.isFinite(account?.datasetCount)
          ? account.datasetCount
          : 1;
      });
    }
    const providerLabel = getBillingProviderDisplayName(provider);
    data.services.forEach((service) => {
      const clonedDetails = Array.isArray(service.details)
        ? service.details.map((detail) => ({ ...detail }))
        : [];
      services.push({
        ...service,
        name: service.name,
        details: clonedDetails,
        sourceProvider: provider,
        sourceProviderLabel: providerLabel,
        sourceServiceName: String(service.sourceServiceName || service.name),
        sourceAccountName: service.sourceAccountName || "",
      });
    });
  });
  services.forEach((service) => {
    service.share = totalCost > 0 ? (service.cost / totalCost) * 100 : 0;
  });
  services.sort((a, b) => b.cost - a.cost);
  return {
    provider: "unified",
    importedAt: latestImportedAt
      ? new Date(latestImportedAt).toISOString()
      : new Date().toISOString(),
    rowCount,
    serviceCount: services.length,
    totalCost,
    serviceColumn: "Provider / Service",
    costColumn: "Cost",
    detailColumn: "Detail",
    chargeTypeColumn: "",
    usageDateColumn: "",
    services,
    sourceDatasetCount,
    sourceFiles,
    sourceAccounts: Array.from(sourceAccountMap.values()).sort((a, b) =>
      a.name.localeCompare(b.name)
    ),
  };
}

function getBillingPanelData(provider = currentBillingProvider) {
  const normalized = normalizeBillingProvider(provider);
  const monthKey = getBillingCurrentMonth(normalized);
  if (normalized === "unified") {
    const unified = buildUnifiedBillingData(monthKey);
    if (unified || monthKey === "all") {
      return unified;
    }
    return buildUnifiedBillingData("all");
  }
  return getBillingProviderDataByMonth(normalized, monthKey);
}

function addBillingBucketCost(map, key, amount) {
  const name = String(key || "").trim() || "Untagged";
  const current = map.get(name) || 0;
  map.set(name, current + amount);
}

function buildBillingChartBuckets(services, provider, groupBy) {
  const isUnified = normalizeBillingProvider(provider) === "unified";
  const buckets = new Map();
  if (groupBy === "account") {
    services.forEach((service) => {
      const accountName = normalizeBillingAccountName(
        service.sourceAccountName,
        "Unlabeled"
      );
      const bucketKey = isUnified
        ? `${getBillingProviderDisplayName(service.sourceProvider)} / ${accountName}`
        : accountName;
      addBillingBucketCost(
        buckets,
        bucketKey,
        Number.isFinite(service.cost) ? service.cost : 0
      );
    });
    return buckets;
  }
  if (groupBy === "service") {
    services.forEach((service) => {
      const serviceKey = isUnified
        ? `${getBillingProviderDisplayName(service.sourceProvider)} / ${service.name}`
        : service.name;
      addBillingBucketCost(
        buckets,
        serviceKey,
        Number.isFinite(service.cost) ? service.cost : 0
      );
    });
    return buckets;
  }

  services.forEach((service) => {
    const serviceCost = Number.isFinite(service.cost) ? service.cost : 0;
    const details = Array.isArray(service.details) ? service.details : [];
    const tagProvider = isUnified
      ? normalizeBillingProvider(service.sourceProvider)
      : normalizeBillingProvider(provider);
    const tagServiceName = isUnified
      ? String(service.sourceServiceName || service.name)
      : service.name;
    const serviceTag = getBillingServiceTag(tagProvider, tagServiceName);
    if (serviceTag?.productApp && groupBy === "productApp") {
      addBillingBucketCost(buckets, serviceTag.productApp, serviceCost);
      return;
    }
    if (Array.isArray(serviceTag?.tags) && serviceTag.tags.length && groupBy === "tag") {
      const share = serviceCost / serviceTag.tags.length;
      serviceTag.tags.forEach((tag) => {
        addBillingBucketCost(buckets, tag, share);
      });
      return;
    }
    if (!details.length) {
      addBillingBucketCost(buckets, "Untagged", serviceCost);
      return;
    }

    let distributed = 0;
    details.forEach((detail) => {
      const detailCost = Number.isFinite(detail.cost) ? detail.cost : 0;
      const detailTag = getBillingDetailTag(
        tagProvider,
        tagServiceName,
        detail.name
      );
      if (groupBy === "productApp") {
        addBillingBucketCost(
          buckets,
          detailTag?.productApp || "Untagged",
          detailCost
        );
      } else {
        const tags = Array.isArray(detailTag?.tags) ? detailTag.tags : [];
        if (tags.length) {
          const share = detailCost / tags.length;
          tags.forEach((tag) => {
            addBillingBucketCost(buckets, tag, share);
          });
        } else {
          addBillingBucketCost(buckets, "Untagged", detailCost);
        }
      }
      distributed += detailCost;
    });

    const remainder = serviceCost - distributed;
    if (Math.abs(remainder) >= 0.01) {
      addBillingBucketCost(buckets, "Untagged", remainder);
    }
  });
  return buckets;
}

function renderBillingImportPanel() {
  if (!billingSummary || !billingChart || !billingTable) {
    return;
  }
  const normalizedProvider = normalizeBillingProvider(currentBillingProvider);
  const isUnified = normalizedProvider === "unified";
  const data = getBillingPanelData(currentBillingProvider);
  syncBillingControls(data);
  if (!data) {
    billingSummary.innerHTML = `
      <article class="billing-summary-card">
        <h4>${getBillingProviderDisplayName(currentBillingProvider)}</h4>
        <p>${
          isUnified
            ? "Import at least one provider CSV to populate Unified view."
            : "No CSV imported for this provider yet."
        }</p>
      </article>`;
    billingChart.innerHTML = `<p class="billing-empty">Import a billing CSV to visualize service allocation.</p>`;
    billingTable.innerHTML = "";
    if (billingNote && !billingNote.textContent.trim()) {
      billingNote.textContent = "Import a billing CSV to start allocation analysis.";
    }
    return;
  }

  const activeFilter = getBillingCurrentFilter(currentBillingProvider);
  const monthKey = getBillingCurrentMonth(currentBillingProvider);
  const monthLabel = formatBillingPeriodLabel(monthKey);
  const monthBucketCount = listBillingMonthKeys(currentBillingProvider).length;
  const filterLabel =
    activeFilter === BILLING_UNTAGGED_FILTER
      ? "Untagged"
      : activeFilter || "All product apps";
  const filteredServices = getBillingFilteredServices(
    data,
    currentBillingProvider
  );
  const filteredTotal = filteredServices.reduce(
    (sum, service) => sum + (Number.isFinite(service.cost) ? service.cost : 0),
    0
  );
  const chartGroup = getBillingCurrentChartGroup(currentBillingProvider);
  const chartBuckets = Array.from(
    buildBillingChartBuckets(filteredServices, currentBillingProvider, chartGroup).entries()
  )
    .map(([name, cost]) => ({
      name,
      cost: Number.isFinite(cost) ? cost : 0,
    }))
    .filter((row) => Math.abs(row.cost) >= 0.01)
    .sort((a, b) => b.cost - a.cost);
  const topServices = chartBuckets.slice(0, 20);
  const maxValue = topServices.length
    ? Math.max(...topServices.map((row) => row.cost))
    : 0;
  const chartLabel =
    chartGroup === "account"
      ? "Account"
      : chartGroup === "productApp"
      ? "Product app"
      : chartGroup === "tag"
      ? "Tag"
      : "Service";
  const sourceDatasetCount = Number.isFinite(data.sourceDatasetCount)
    ? data.sourceDatasetCount
    : 1;
  const sourceFileCount = Array.isArray(data.sourceFiles)
    ? data.sourceFiles.length
    : 0;
  const sourceAccountCount = Array.isArray(data.sourceAccounts)
    ? data.sourceAccounts.length
    : 0;
  billingSummary.innerHTML = `
    <article class="billing-summary-card">
      <h4>${getBillingProviderDisplayName(currentBillingProvider)}</h4>
      <p>Total imported</p>
      <strong>${formatMoney(data.totalCost)}</strong>
    </article>
    <article class="billing-summary-card">
      <h4>Period scope</h4>
      <p>${escapeMarkup(monthLabel)}</p>
      <strong>${monthBucketCount} month bucket(s)</strong>
    </article>
    <article class="billing-summary-card">
      <h4>Rows</h4>
      <p>Processed rows</p>
      <strong>${data.rowCount}</strong>
    </article>
    <article class="billing-summary-card">
      <h4>CSV files</h4>
      <p>Imported datasets</p>
      <strong>${sourceDatasetCount} (${sourceFileCount} files tracked)</strong>
    </article>
    <article class="billing-summary-card">
      <h4>Accounts</h4>
      <p>Named sources</p>
      <strong>${sourceAccountCount || "n/a"}</strong>
    </article>
    <article class="billing-summary-card">
      <h4>Services</h4>
      <p>Distinct services</p>
      <strong>${data.serviceCount}</strong>
    </article>
    <article class="billing-summary-card">
      <h4>Filter</h4>
      <p>${escapeMarkup(filterLabel)}</p>
      <strong>${formatMoney(filteredTotal)} (${filteredServices.length} services)</strong>
    </article>
    <article class="billing-summary-card">
      <h4>Chart view</h4>
      <p>Grouped by ${escapeMarkup(chartLabel)}</p>
      <strong>${topServices.length} buckets</strong>
    </article>
    <article class="billing-summary-card">
      <h4>Columns</h4>
      <p>${escapeMarkup(data.serviceColumn)} / ${escapeMarkup(data.costColumn)}</p>
      <strong>${new Date(data.importedAt).toLocaleString()}</strong>
    </article>
    <article class="billing-summary-card">
      <h4>Line-item column</h4>
      <p>${escapeMarkup(data.detailColumn || "Detail")}</p>
      <strong>${data.usageDateColumn ? escapeMarkup(data.usageDateColumn) : "No usage date column"}</strong>
    </article>`;

  billingChart.innerHTML = topServices.length
    ? topServices
        .map((service) => {
          const width = maxValue > 0 ? (service.cost / maxValue) * 100 : 0;
          const share = filteredTotal > 0 ? (service.cost / filteredTotal) * 100 : 0;
          return `
            <div class="billing-bar-row">
              <div class="billing-bar-meta">
                <span class="billing-bar-label">${escapeMarkup(service.name)}</span>
                <span class="billing-bar-value">${formatMoney(service.cost)} (${share.toFixed(1)}%)</span>
              </div>
              <div class="billing-bar-track">
                <div class="billing-bar-fill" style="width: ${Math.max(0, Math.min(100, width))}%"></div>
              </div>
            </div>`;
        })
        .join("")
    : `<p class="billing-empty">No ${escapeMarkup(chartLabel.toLowerCase())} buckets match the current Product App filter.</p>`;

  const expandedServices = getBillingExpandedServiceSet(currentBillingProvider);
  const selectedDetails = getBillingSelectedDetailSet(currentBillingProvider);
  const selectedCount = selectedDetails.size;
  const bulkSelectionBanner =
    !isUnified && selectedCount > 0
      ? `
    <div class="billing-inline-bulk-bar">
      <strong>${selectedCount} line items selected</strong>
      <input
        type="text"
        class="billing-inline-input"
        data-billing-inline-bulk-product-app
        placeholder="Product app"
      />
      <input
        type="text"
        class="billing-inline-input"
        data-billing-inline-bulk-tags
        placeholder="Tags (comma-separated)"
      />
      <button type="button" class="table-action" data-billing-inline-bulk-apply>
        Apply to selected
      </button>
      <button type="button" class="table-action" data-billing-inline-bulk-clear-tags>
        Clear selected tags
      </button>
      <button type="button" class="table-action" data-billing-inline-bulk-clear-selection>
        Clear selection
      </button>
    </div>`
      : "";
  const tableRows = filteredServices
    .slice(0, 100)
    .map((service) => {
      const serviceExpandKey = isUnified
        ? `${normalizeBillingProvider(service.sourceProvider)}@@${String(
            service.sourceServiceName || service.name
          )}`
        : service.name;
      const isExpanded = expandedServices.has(serviceExpandKey);
      const toggleLabel = isExpanded ? "-" : "+";
      const details = Array.isArray(service.details) ? service.details : [];
      const tagProvider = isUnified
        ? normalizeBillingProvider(service.sourceProvider)
        : normalizedProvider;
      const tagServiceName = isUnified
        ? String(service.sourceServiceName || service.name)
        : service.name;
      const tagEntry = getBillingServiceTag(tagProvider, tagServiceName);
      const productApp = tagEntry?.productApp || "—";
      const customTags =
        Array.isArray(tagEntry?.tags) && tagEntry.tags.length
          ? tagEntry.tags.join(", ")
          : "—";
      const serviceShare =
        filteredTotal > 0 ? (service.cost / filteredTotal) * 100 : 0;
      const detailRows = isExpanded
        ? details
            .slice(0, 200)
            .map((detail) => {
              const detailKey = encodeBillingDetailKey(service.name, detail.name);
              const detailChecked = selectedDetails.has(detailKey) ? "checked" : "";
              const detailTag = getBillingDetailTag(
                tagProvider,
                tagServiceName,
                detail.name
              );
              const detailApp = detailTag?.productApp || "";
              const detailTags =
                Array.isArray(detailTag?.tags) && detailTag.tags.length
                  ? detailTag.tags.join(", ")
                  : "";
              if (isUnified) {
                return `
              <tr class="billing-detail-row">
                <td></td>
                <td class="billing-detail-name">${escapeMarkup(detail.name)}</td>
                <td>${escapeMarkup(detailApp || "—")}</td>
                <td>${escapeMarkup(detailTags || "—")}</td>
                <td>${formatMoney(detail.cost)}</td>
                <td>${detail.share.toFixed(2)}%</td>
                <td>${detail.rowCount}</td>
                <td>Line item</td>
              </tr>`;
              }
              return `
              <tr class="billing-detail-row">
                <td>
                  <input
                    type="checkbox"
                    class="billing-detail-select"
                    data-billing-detail-select="${detailKey}"
                    ${detailChecked}
                  />
                </td>
                <td class="billing-detail-name">${escapeMarkup(detail.name)}</td>
                <td>
                  <input
                    type="text"
                    class="billing-inline-input"
                    data-billing-detail-product-app="${detailKey}"
                    value="${escapeMarkup(detailApp)}"
                    placeholder="Product app"
                  />
                </td>
                <td>
                  <input
                    type="text"
                    class="billing-inline-input"
                    data-billing-detail-tags="${detailKey}"
                    value="${escapeMarkup(detailTags)}"
                    placeholder="tag1, tag2"
                  />
                </td>
                <td>${formatMoney(detail.cost)}</td>
                <td>${detail.share.toFixed(2)}%</td>
                <td>${detail.rowCount}</td>
                <td class="billing-inline-actions">
                  <button type="button" class="table-action" data-billing-detail-save="${detailKey}">Save</button>
                  <button type="button" class="table-action" data-billing-detail-clear="${detailKey}">Clear</button>
                </td>
              </tr>`;
            })
            .join("") +
          (() => {
            if (details.length > 200) {
              return `
                <tr class="billing-detail-row">
                  <td></td>
                  <td class="billing-detail-name" colspan="7">
                    Showing top 200 line items by cost (${details.length} total).
                  </td>
                </tr>`;
            }
            return "";
          })() +
          (() => {
            const tags = [];
            if (Array.isArray(service.topChargeTypes) && service.topChargeTypes.length) {
              tags.push(`Charge types: ${service.topChargeTypes.join(", ")}`);
            }
            if (Number.isFinite(service.minDate) && Number.isFinite(service.maxDate)) {
              const start = new Date(service.minDate).toLocaleDateString();
              const end = new Date(service.maxDate).toLocaleDateString();
              tags.push(`Usage range: ${start} - ${end}`);
            }
            if (!tags.length) {
              return "";
            }
            return `
              <tr class="billing-detail-row billing-detail-meta">
                <td></td>
                <td colspan="7">${escapeMarkup(tags.join(" | "))}</td>
              </tr>`;
          })()
        : "";
      return `
        <tr class="billing-service-row">
          <td>
            <button
              type="button"
              class="billing-expand-btn"
              data-billing-toggle="${encodeURIComponent(serviceExpandKey)}"
              title="${isExpanded ? "Collapse" : "Expand"} line items"
            >
              ${toggleLabel}
            </button>
          </td>
          <td>${
            isUnified
              ? `${escapeMarkup(
                  getBillingProviderDisplayName(service.sourceProvider)
                )} / ${
                  service.sourceAccountName
                    ? `${escapeMarkup(service.sourceAccountName)} / `
                    : ""
                }${escapeMarkup(service.name)}`
              : `${
                  service.sourceAccountName
                    ? `${escapeMarkup(service.sourceAccountName)} / `
                    : ""
                }${escapeMarkup(service.name)}`
          }</td>
          <td>${escapeMarkup(productApp)}</td>
          <td>${escapeMarkup(customTags)}</td>
          <td>${formatMoney(service.cost)}</td>
          <td>${serviceShare.toFixed(2)}%</td>
          <td>${service.rowCount}</td>
          <td>${service.detailCount || details.length}${
            service.isPartial ? " (filtered)" : ""
          }</td>
        </tr>
        ${detailRows}`;
    })
    .join("");
  billingTable.innerHTML = `
    ${bulkSelectionBanner}
    <table>
      <thead>
        <tr>
          <th></th>
          <th>Service</th>
          <th>Product app</th>
          <th>Tags</th>
          <th>Cost</th>
          <th>Share</th>
          <th>Rows</th>
          <th>Line items / actions</th>
        </tr>
      </thead>
      <tbody>
        ${tableRows}
      </tbody>
    </table>`;
}

function pruneBillingProviderTags(provider, validServiceNames) {
  const map = getBillingProviderTagMap(provider);
  const allowed = new Set(validServiceNames || []);
  let changed = false;
  Object.keys(map).forEach((serviceName) => {
    if (!allowed.has(serviceName)) {
      delete map[serviceName];
      changed = true;
    }
  });
  return changed;
}

function buildBillingExportCsv(data, services, provider, filterLabel) {
  const rows = [
    [
      "Provider",
      "Account",
      "Service",
      "ProductApp",
      "Tags",
      "Cost",
      "SharePercent",
      "Rows",
      "LineItems",
      "RecordType",
      "LineItem",
      "ImportedAt",
      "FilterProductApp",
      "ServiceColumn",
      "CostColumn",
      "DetailColumn",
      "UsageDateColumn",
    ],
  ];
  const total = services.reduce(
    (sum, service) => sum + (Number.isFinite(service.cost) ? service.cost : 0),
    0
  );
  services.forEach((service) => {
    const sourceProvider =
      normalizeBillingProvider(provider) === "unified"
        ? normalizeBillingProvider(service.sourceProvider)
        : normalizeBillingProvider(provider);
    const sourceServiceName =
      normalizeBillingProvider(provider) === "unified"
        ? String(service.sourceServiceName || service.name)
        : service.name;
    const entry = getBillingServiceTag(sourceProvider, sourceServiceName);
    const share = total > 0 ? (service.cost / total) * 100 : 0;
    rows.push([
      getBillingProviderDisplayName(sourceProvider),
      service.sourceAccountName || "",
      service.name,
      entry?.productApp || "",
      Array.isArray(entry?.tags) ? entry.tags.join("|") : "",
      Number.isFinite(service.cost) ? service.cost.toFixed(2) : "",
      share.toFixed(4),
      service.rowCount || 0,
      service.detailCount || 0,
      "service",
      "",
      data.importedAt || "",
      filterLabel || "All product apps",
      data.serviceColumn || "",
      data.costColumn || "",
      data.detailColumn || "",
      data.usageDateColumn || "",
    ]);
    const details = Array.isArray(service.details) ? service.details : [];
    details.forEach((detail) => {
      const detailEntry = getBillingDetailTag(
        sourceProvider,
        sourceServiceName,
        detail.name
      );
      const detailShare = total > 0 ? (detail.cost / total) * 100 : 0;
      rows.push([
        getBillingProviderDisplayName(sourceProvider),
        service.sourceAccountName || "",
        service.name,
        detailEntry?.productApp || "",
        Array.isArray(detailEntry?.tags) ? detailEntry.tags.join("|") : "",
        Number.isFinite(detail.cost) ? detail.cost.toFixed(2) : "",
        detailShare.toFixed(4),
        detail.rowCount || 0,
        1,
        "line-item",
        detail.name || "",
        data.importedAt || "",
        filterLabel || "All product apps",
        data.serviceColumn || "",
        data.costColumn || "",
        data.detailColumn || "",
        data.usageDateColumn || "",
      ]);
    });
  });
  return rows.map((row) => row.map((value) => escapeCsv(value)).join(",")).join("\n");
}

function handleBillingExportCsv() {
  const data = getBillingPanelData(currentBillingProvider);
  if (!data) {
    setInlineNote(billingNote, "Import a billing CSV first.", true);
    return;
  }
  const filter = getBillingCurrentFilter(currentBillingProvider);
  const filterLabel =
    filter === BILLING_UNTAGGED_FILTER ? "Untagged" : filter || "All product apps";
  const services = getBillingFilteredServices(data, currentBillingProvider);
  if (!services.length) {
    setInlineNote(
      billingNote,
      "No rows match the current Product App filter.",
      true
    );
    return;
  }
  const csv = buildBillingExportCsv(
    data,
    services,
    currentBillingProvider,
    filterLabel
  );
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  const providerLabel = currentBillingProvider.toLowerCase();
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  link.href = url;
  link.download = `billing-${providerLabel}-${stamp}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
  setInlineNote(
    billingNote,
    `${getBillingProviderDisplayName(
      currentBillingProvider
    )} billing CSV exported (${services.length} services).`
  );
}

function handleBillingProductFilterChange() {
  if (!billingProductFilter) {
    return;
  }
  setBillingCurrentFilter(billingProductFilter.value, currentBillingProvider);
  renderBillingImportPanel();
}

function handleBillingMonthFilterChange() {
  if (!billingMonthFilter) {
    return;
  }
  setBillingCurrentMonth(billingMonthFilter.value, currentBillingProvider);
  renderBillingImportPanel();
}

function handleBillingChartGroupChange() {
  if (!billingChartGroup) {
    return;
  }
  setBillingCurrentChartGroup(billingChartGroup.value, currentBillingProvider);
  renderBillingImportPanel();
}

function getVisibleBillingDetailKeys() {
  if (!billingTable) {
    return [];
  }
  return Array.from(
    billingTable.querySelectorAll("[data-billing-detail-select]")
  )
    .map((input) => String(input.dataset.billingDetailSelect || "").trim())
    .filter(Boolean);
}

function handleBillingBulkSelectVisible() {
  const data = getBillingPanelData(currentBillingProvider);
  if (!data) {
    setInlineNote(billingNote, "Import a billing CSV first.", true);
    return;
  }
  const selected = getBillingSelectedDetailSet(currentBillingProvider);
  getVisibleBillingDetailKeys().forEach((key) => {
    selected.add(key);
  });
  renderBillingImportPanel();
  setInlineNote(
    billingNote,
    `Selected ${selected.size} line items for bulk tagging.`
  );
}

function handleBillingBulkClearSelection() {
  const selected = getBillingSelectedDetailSet(currentBillingProvider);
  selected.clear();
  renderBillingImportPanel();
  setInlineNote(billingNote, "Cleared line-item selection.");
}

function handleBillingBulkApply() {
  const data = getBillingPanelData(currentBillingProvider);
  if (!data) {
    setInlineNote(billingNote, "Import a billing CSV first.", true);
    return;
  }
  const productApp = String(billingBulkProductApp?.value || "").trim();
  const tags = parseBillingCustomTags(billingBulkTags?.value || "");
  applyBillingTagsToSelected(productApp, tags, false);
}

function handleBillingBulkClearTags() {
  const data = getBillingPanelData(currentBillingProvider);
  if (!data) {
    setInlineNote(billingNote, "Import a billing CSV first.", true);
    return;
  }
  applyBillingTagsToSelected("", [], true);
}

function applyBillingTagsToSelected(productApp, tags, clear = false) {
  if (normalizeBillingProvider(currentBillingProvider) === "unified") {
    setInlineNote(
      billingNote,
      "Tag editing is disabled in Unified view. Switch to a specific provider.",
      true
    );
    return false;
  }
  const selected = getBillingSelectedDetailSet(currentBillingProvider);
  if (!selected.size) {
    setInlineNote(billingNote, "Select one or more line items first.", true);
    return false;
  }
  const normalizedProductApp = String(productApp || "").trim();
  const normalizedTags = Array.isArray(tags) ? tags : [];
  if (!clear && !normalizedProductApp && !normalizedTags.length) {
    setInlineNote(
      billingNote,
      "Provide a Product app or tags for bulk apply.",
      true
    );
    return false;
  }
  let updated = 0;
  selected.forEach((key) => {
    const { serviceName, detailName } = decodeBillingDetailKey(key);
    if (!serviceName || !detailName) {
      return;
    }
    if (clear) {
      setBillingDetailTag(currentBillingProvider, serviceName, detailName, null);
    } else {
      setBillingDetailTag(currentBillingProvider, serviceName, detailName, {
        productApp: normalizedProductApp,
        tags: normalizedTags,
      });
    }
    updated += 1;
  });
  persistBillingDetailTagsStore(billingDetailTagStore);
  renderBillingImportPanel();
  const action = clear ? "Cleared tags for" : "Applied bulk tags to";
  setInlineNote(
    billingNote,
    `${action} ${updated} line items on ${getBillingProviderDisplayName(
      currentBillingProvider
    )}.`
  );
  return true;
}

function handleBillingApplyTags() {
  if (normalizeBillingProvider(currentBillingProvider) === "unified") {
    setInlineNote(
      billingNote,
      "Tag editing is disabled in Unified view. Switch to a specific provider.",
      true
    );
    return;
  }
  const data = getBillingPanelData(currentBillingProvider);
  if (!data) {
    setInlineNote(billingNote, "Import a billing CSV first.", true);
    return;
  }
  const serviceName = String(billingTagService?.value || "").trim();
  if (!serviceName) {
    setInlineNote(billingNote, "Select a service to tag.", true);
    return;
  }
  const productApp = String(billingTagProductApp?.value || "").trim();
  const tags = parseBillingCustomTags(billingTagCustom?.value || "");
  setBillingServiceTag(currentBillingProvider, serviceName, { productApp, tags });
  persistBillingTagsStore(billingTagStore);
  renderBillingImportPanel();
  setInlineNote(
    billingNote,
    `Saved tags for "${serviceName}" on ${getBillingProviderDisplayName(
      currentBillingProvider
    )}.`
  );
}

function handleBillingClearTags() {
  if (normalizeBillingProvider(currentBillingProvider) === "unified") {
    setInlineNote(
      billingNote,
      "Tag editing is disabled in Unified view. Switch to a specific provider.",
      true
    );
    return;
  }
  const data = getBillingPanelData(currentBillingProvider);
  if (!data) {
    setInlineNote(billingNote, "Import a billing CSV first.", true);
    return;
  }
  const serviceName = String(billingTagService?.value || "").trim();
  if (!serviceName) {
    setInlineNote(billingNote, "Select a service to clear tags.", true);
    return;
  }
  setBillingServiceTag(currentBillingProvider, serviceName, null);
  persistBillingTagsStore(billingTagStore);
  renderBillingImportPanel();
  setInlineNote(
    billingNote,
    `Cleared tags for "${serviceName}" on ${getBillingProviderDisplayName(
      currentBillingProvider
    )}.`
  );
}

async function handleBillingImportFile(event) {
  const files = Array.from(event?.target?.files || []);
  if (!files.length) {
    return;
  }
  if (normalizeBillingProvider(currentBillingProvider) === "unified") {
    setInlineNote(
      billingNote,
      "Import is disabled on Unified. Select AWS, Azure, GCP, or Rackspace.",
      true
    );
    if (billingImportInput) {
      billingImportInput.value = "";
    }
    return;
  }
  try {
    const selectedProvider = normalizeBillingProvider(currentBillingProvider);
    const parsedImportsByProvider = {
      aws: [],
      azure: [],
      gcp: [],
      rackspace: [],
    };
    const importedMonthKeysByProvider = {
      aws: new Set(),
      azure: new Set(),
      gcp: new Set(),
      rackspace: new Set(),
    };
    const reroutedFilesByProvider = {
      aws: 0,
      azure: 0,
      gcp: 0,
      rackspace: 0,
    };
    const failedImports = [];
    for (const file of files) {
      try {
        const text = await file.text();
        const previewRows = parseCsvRows(text).filter((row) =>
          row.some((value) => String(value || "").trim() !== "")
        );
        if (previewRows.length < 2) {
          throw new Error("CSV needs a header and at least one data row.");
        }
        const previewHeaders = previewRows[0].map((value) =>
          normalizeCsvHeader(value)
        );
        const detectedProvider = detectBillingCsvProvider(
          previewRows,
          previewHeaders
        ).provider;
        const targetProvider = normalizeBillingProvider(
          detectedProvider || selectedProvider
        );
        const wasRerouted = targetProvider !== selectedProvider;
        const importSignature = await computeBillingImportSignature(
          text,
          targetProvider
        );
        const parsed = parseBillingImportCsv(text, targetProvider);
        const defaultLabel = String(file.name || "")
          .replace(/\.[^.]+$/, "")
          .trim();
        const sourceAccountName = normalizeBillingAccountName(
          defaultLabel,
          "Unlabeled"
        );
        parsed.sourceFiles = [file.name];
        parsed.sourceDatasetCount = 1;
        parsed.sourceSignatures = [`${importSignature}:all`];
        parsed.sourceAccounts = [
          {
            name: sourceAccountName || "Unlabeled",
            cost: Number.isFinite(parsed.totalCost) ? parsed.totalCost : 0,
            rowCount: Number.isFinite(parsed.rowCount) ? parsed.rowCount : 0,
            fileCount: 1,
            datasetCount: 1,
          },
        ];
        parsed.services = (parsed.services || []).map((service) => ({
          ...service,
          sourceServiceName: String(service.name || "").trim() || service.name,
          sourceAccountName: sourceAccountName || "",
        }));
        const parsedMonthDatasets =
          parsed.monthDatasets &&
          typeof parsed.monthDatasets === "object" &&
          !Array.isArray(parsed.monthDatasets)
            ? parsed.monthDatasets
            : {
                [inferBillingDatasetMonthKey(parsed)]: cloneBillingDataset(parsed),
              };
        const normalizedMonthDatasets = {};
        Object.entries(parsedMonthDatasets).forEach(([rawMonthKey, dataset]) => {
          const normalizedDataset = normalizeBillingImportDataset(dataset);
          if (!normalizedDataset) {
            return;
          }
          const monthKey = normalizeBillingMonthKey(rawMonthKey);
          importedMonthKeysByProvider[targetProvider].add(monthKey);
          normalizedDataset.provider = targetProvider;
          normalizedDataset.sourceFiles = [file.name];
          normalizedDataset.sourceDatasetCount = 1;
          normalizedDataset.sourceSignatures = [`${importSignature}:${monthKey}`];
          normalizedDataset.sourceAccounts = [
            {
              name: sourceAccountName || "Unlabeled",
              cost: Number.isFinite(normalizedDataset.totalCost)
                ? normalizedDataset.totalCost
                : 0,
              rowCount: Number.isFinite(normalizedDataset.rowCount)
                ? normalizedDataset.rowCount
                : 0,
              fileCount: 1,
              datasetCount: 1,
            },
          ];
          normalizedDataset.services = (normalizedDataset.services || []).map(
            (service) => ({
              ...service,
              sourceServiceName:
                String(service.sourceServiceName || service.name || "").trim() ||
                service.name,
              sourceAccountName: sourceAccountName || "",
            })
          );
          if (normalizedMonthDatasets[monthKey]) {
            normalizedMonthDatasets[monthKey] = mergeBillingImportDatasets(
              targetProvider,
              [normalizedMonthDatasets[monthKey], normalizedDataset]
            );
          } else {
            normalizedMonthDatasets[monthKey] = normalizedDataset;
          }
        });
        parsed.monthDatasets = normalizedMonthDatasets;
        parsed.provider = targetProvider;
        parsedImportsByProvider[targetProvider].push(parsed);
        if (wasRerouted) {
          reroutedFilesByProvider[targetProvider] += 1;
        }
      } catch (error) {
        failedImports.push(
          `${file.name}: ${error?.message || "Could not parse CSV"}`
        );
      }
    }
    const totalImportedFiles = BILLING_IMPORT_PROVIDERS.reduce(
      (sum, provider) => sum + parsedImportsByProvider[provider].length,
      0
    );
    if (!totalImportedFiles) {
      throw new Error(
        failedImports.join(" | ") || "Could not parse any selected CSV files."
      );
    }
    const providerSummaries = [];
    BILLING_IMPORT_PROVIDERS.forEach((provider) => {
      const imports = parsedImportsByProvider[provider] || [];
      if (!imports.length) {
        return;
      }
      const providerEntry = getBillingProviderImportEntry(provider);
      let skippedDuplicateBuckets = 0;
      let mergedBuckets = 0;
      imports.forEach((parsed) => {
        const monthDatasets = parsed?.monthDatasets || {};
        Object.entries(monthDatasets).forEach(([monthKey, dataset]) => {
          const normalizedMonthKey = normalizeBillingMonthKey(monthKey);
          const normalizedDataset = normalizeBillingImportDataset(dataset);
          if (!normalizedDataset) {
            return;
          }
          const existingDataset = providerEntry.months[normalizedMonthKey];
          const incomingSignatures = normalizeBillingSourceSignatures(
            normalizedDataset.sourceSignatures
          );
          const existingSignatures = normalizeBillingSourceSignatures(
            existingDataset?.sourceSignatures
          );
          const incomingFiles = Array.isArray(normalizedDataset.sourceFiles)
            ? normalizedDataset.sourceFiles
                .map((name) => String(name || "").trim())
                .filter(Boolean)
            : [];
          const existingFiles = Array.isArray(existingDataset?.sourceFiles)
            ? existingDataset.sourceFiles
                .map((name) => String(name || "").trim())
                .filter(Boolean)
            : [];
          const hasLegacyLikeDuplicate =
            Boolean(existingDataset) &&
            incomingFiles.length > 0 &&
            incomingFiles.every((name) => existingFiles.includes(name)) &&
            Number.isFinite(existingDataset?.rowCount) &&
            Number.isFinite(normalizedDataset.rowCount) &&
            Number(existingDataset.rowCount) === Number(normalizedDataset.rowCount) &&
            Number.isFinite(existingDataset?.totalCost) &&
            Number.isFinite(normalizedDataset.totalCost) &&
            Math.abs(
              Number(existingDataset.totalCost) - Number(normalizedDataset.totalCost)
            ) < 0.01;
          if (
            (incomingSignatures.length &&
              incomingSignatures.every((signature) =>
                existingSignatures.includes(signature)
              )) ||
            hasLegacyLikeDuplicate
          ) {
            skippedDuplicateBuckets += 1;
            return;
          }
          providerEntry.months[normalizedMonthKey] = existingDataset
            ? mergeBillingImportDatasets(provider, [existingDataset, normalizedDataset])
            : normalizedDataset;
          mergedBuckets += 1;
        });
      });
      const merged = getBillingProviderDataByMonth(provider, "all");
      const importedMonthLabels = Array.from(importedMonthKeysByProvider[provider])
        .sort((left, right) => {
          if (left === BILLING_MONTH_UNKNOWN_KEY) {
            return 1;
          }
          if (right === BILLING_MONTH_UNKNOWN_KEY) {
            return -1;
          }
          return right.localeCompare(left);
        })
        .map((monthKey) => formatBillingMonthLabel(monthKey));
      const monthSummary = importedMonthLabels.length
        ? importedMonthLabels.length <= 4
          ? importedMonthLabels.join(", ")
          : `${importedMonthLabels.slice(0, 4).join(", ")} (+${
              importedMonthLabels.length - 4
            } more)`
        : "All months";
      providerSummaries.push({
        provider,
        fileCount: imports.length,
        mergedRows: merged?.rowCount || 0,
        mergedBuckets,
        skippedDuplicateBuckets,
        monthBucketCount: importedMonthKeysByProvider[provider].size,
        monthSummary,
        reroutedFiles: reroutedFilesByProvider[provider] || 0,
      });
      billingExpandedServices[provider] = new Set();
      billingSelectedDetailKeys[provider] = new Set();
    });
    // Keep service/detail tags across imports so monthly files can auto-match by name.
    persistBillingTagsStore(billingTagStore);
    persistBillingDetailTagsStore(billingDetailTagStore);
    persistBillingImportStore(billingImportStore);
    const totalReroutedFiles = providerSummaries.reduce(
      (sum, summary) => sum + summary.reroutedFiles,
      0
    );
    const perProviderMessage = providerSummaries
      .map((summary) => {
        return `${getBillingProviderDisplayName(summary.provider)} imported ${
          summary.fileCount
        } file(s), month buckets updated: ${summary.monthBucketCount} (${
          summary.monthSummary
        }), merged rows: ${summary.mergedRows}${
          summary.skippedDuplicateBuckets
            ? `, skipped duplicates: ${summary.skippedDuplicateBuckets}`
            : ""
        }${
          summary.mergedBuckets === 0 ? ", no new data added" : ""
        }${
          summary.reroutedFiles
            ? `, auto-routed from ${
                getBillingProviderDisplayName(selectedProvider)
              } tab: ${summary.reroutedFiles}`
            : ""
        }`;
      })
      .join(" | ");
    let rerouteNote = "";
    if (totalReroutedFiles === 1) {
      const reroutedTarget = providerSummaries.find(
        (summary) => summary.reroutedFiles > 0
      )?.provider;
      if (reroutedTarget) {
        rerouteNote = ` File was imported in ${getBillingProviderDisplayName(
          reroutedTarget
        )} tab.`;
      }
    } else if (totalReroutedFiles > 1) {
      rerouteNote = ` ${totalReroutedFiles} file(s) were auto-imported into matching provider tabs.`;
    }
    setInlineNote(
      billingNote,
      `${perProviderMessage}.${rerouteNote}${
        failedImports.length
          ? ` Failed: ${failedImports.length} (${failedImports.join(" | ")}).`
          : ""
      }`
    );
    renderBillingImportPanel();
  } catch (error) {
    setInlineNote(
      billingNote,
      error?.message || "Could not import billing CSV.",
      true
    );
  } finally {
    if (billingImportInput) {
      billingImportInput.value = "";
    }
  }
}

function handleBillingClearProvider() {
  if (normalizeBillingProvider(currentBillingProvider) === "unified") {
    setInlineNote(
      billingNote,
      "Clear provider is disabled on Unified. Use Clear all if needed.",
      true
    );
    return;
  }
  billingImportStore[currentBillingProvider] = { months: {} };
  billingTagStore[currentBillingProvider] = {};
  billingDetailTagStore[currentBillingProvider] = {};
  billingSelectedDetailKeys[currentBillingProvider] = new Set();
  persistBillingTagsStore(billingTagStore);
  persistBillingDetailTagsStore(billingDetailTagStore);
  setBillingCurrentMonth("all", currentBillingProvider);
  setBillingCurrentFilter("", currentBillingProvider);
  billingExpandedServices[currentBillingProvider] = new Set();
  persistBillingImportStore(billingImportStore);
  setInlineNote(
    billingNote,
    `Cleared ${getBillingProviderDisplayName(
      currentBillingProvider
    )} billing import.`
  );
  renderBillingImportPanel();
}

function handleBillingClearAll() {
  billingImportStore = buildEmptyBillingImportStore();
  billingTagStore = { aws: {}, azure: {}, gcp: {}, rackspace: {} };
  billingDetailTagStore = { aws: {}, azure: {}, gcp: {}, rackspace: {} };
  billingSelectedDetailKeys.aws = new Set();
  billingSelectedDetailKeys.azure = new Set();
  billingSelectedDetailKeys.gcp = new Set();
  billingSelectedDetailKeys.rackspace = new Set();
  billingSelectedDetailKeys.unified = new Set();
  persistBillingTagsStore(billingTagStore);
  persistBillingDetailTagsStore(billingDetailTagStore);
  setBillingCurrentFilter("", "aws");
  setBillingCurrentFilter("", "azure");
  setBillingCurrentFilter("", "gcp");
  setBillingCurrentFilter("", "rackspace");
  setBillingCurrentFilter("", "unified");
  setBillingCurrentMonth("all", "aws");
  setBillingCurrentMonth("all", "azure");
  setBillingCurrentMonth("all", "gcp");
  setBillingCurrentMonth("all", "rackspace");
  setBillingCurrentMonth("all", "unified");
  billingExpandedServices.aws = new Set();
  billingExpandedServices.azure = new Set();
  billingExpandedServices.gcp = new Set();
  billingExpandedServices.rackspace = new Set();
  billingExpandedServices.unified = new Set();
  persistBillingImportStore(billingImportStore);
  setInlineNote(billingNote, "Cleared billing imports for all providers.");
  renderBillingImportPanel();
}

function buildCsv(data) {
  const input = data.input || {};
  const mode = input.mode || "vm";
  const vmCount = input.vmCount || 1;
  const providers = [
    {
      key: "aws",
      label: getProviderLabelForMode("aws", mode),
      region: data.region?.aws,
      data: data.aws,
    },
    {
      key: "azure",
      label: getProviderLabelForMode("azure", mode),
      region: data.region?.azure,
      data: data.azure,
    },
    {
      key: "gcp",
      label: getProviderLabelForMode("gcp", mode),
      region: data.region?.gcp,
      data: data.gcp,
    },
  ];
  if (data.private?.enabled) {
    providers.push({
      key: "private",
      label: getProviderLabelForMode("private", mode),
      region: data.region?.private,
      data: data.private,
    });
  }
  const tiers = [
    { key: "onDemand", label: "On-demand" },
    { key: "reserved1yr", label: "Reserved 1-year" },
    { key: "reserved3yr", label: "Reserved 3-year" },
  ];

  const rows = [];
  for (const provider of providers) {
    for (const tier of tiers) {
      const tierData = provider.data?.pricingTiers?.[tier.key];
      const totals = tierData?.totals;
      rows.push({
        Mode: mode,
        Provider: provider.label,
        Tier: tier.label,
        Instance: provider.data?.instance?.type || "",
        Region: provider.region?.location || "",
        vCPU: provider.data?.instance?.vcpu ?? "",
        RAM_GB: provider.data?.instance?.memory ?? "",
        VM_Count: vmCount,
        Hourly_Rate: tierData?.hourlyRate ?? "",
        Compute_Monthly: totals?.computeMonthly ?? "",
        Control_Plane_Monthly: totals?.controlPlaneMonthly ?? "",
        Control_Plane_Per_Host:
          Number.isFinite(totals?.controlPlaneMonthly) && vmCount > 0
            ? totals.controlPlaneMonthly / vmCount
            : "",
        Storage_Monthly: totals?.storageMonthly ?? "",
        Backup_Monthly: totals?.backupMonthly ?? "",
        Network_Monthly: totals?.networkMonthly ?? "",
        DR_Monthly: totals?.drMonthly ?? "",
        Egress_Monthly: totals?.egressMonthly ?? "",
        SQL_Monthly: totals?.sqlMonthly ?? "",
        Total_Monthly: totals?.total ?? "",
        Workload: input.workload ?? "",
        Pricing_Focus: input.pricingFocus ?? "all",
        Network_Addon_Focus: input.networkAddonFocus ?? "",
        SQL_Edition: input.sqlEdition ?? "",
        Pricing_Focus: input.pricingFocus ?? "all",
        Network_Addon_Focus: input.networkAddonFocus ?? "",
        SQL_License_Rate: input.sqlLicenseRate ?? "",
        Disk_Tier: input.diskTier ?? "",
        OS_Disk_GB: input.osDiskGb ?? "",
        Data_Disk_TB: input.dataDiskTb ?? "",
        Backups_Enabled: input.backupEnabled ? "Yes" : "No",
        AWS_VPC: input.awsVpcFlavor ?? "",
        AWS_Firewall: input.awsFirewallFlavor ?? "",
        AWS_Load_Balancer: input.awsLoadBalancerFlavor ?? "",
        Azure_VNet: input.azureVpcFlavor ?? "",
        Azure_Firewall: input.azureFirewallFlavor ?? "",
        Azure_Load_Balancer: input.azureLoadBalancerFlavor ?? "",
        GCP_VPC: input.gcpVpcFlavor ?? "",
        GCP_Firewall: input.gcpFirewallFlavor ?? "",
        GCP_Load_Balancer: input.gcpLoadBalancerFlavor ?? "",
        Private_Enabled: input.privateEnabled ? "Yes" : "No",
        Private_VMware_Monthly: input.privateVmwareMonthly ?? "",
        Private_Windows_License_Monthly:
          input.privateWindowsLicenseMonthly ?? "",
        Private_Node_Count: input.privateNodeCount ?? "",
        Private_SAN_per_TB: input.privateStoragePerTb ?? "",
        Private_Network_Monthly: input.privateNetworkMonthly ?? "",
        Private_Firewall_Monthly: input.privateFirewallMonthly ?? "",
        Private_Load_Balancer_Monthly:
          input.privateLoadBalancerMonthly ?? "",
        Windows_License_Monthly: totals?.windowsLicenseMonthly ?? "",
        Backup_Snapshot_GB: provider.data?.backup?.snapshotGb ?? "",
        DR_Percent: input.drPercent ?? "",
        Egress_TB: input.egressTb ?? "",
        Inter_VLAN_TB: input.interVlanTb ?? "",
        Intra_VLAN_TB: input.intraVlanTb ?? "",
        Inter_Region_TB: input.interRegionTb ?? "",
        Storage_IOPS: input.storageIops ?? "",
        Storage_Throughput_MBps: input.storageThroughputMbps ?? "",
        Hours: input.hours ?? "",
        Pricing_Source: provider.data?.source ?? "",
      });
    }
  }

  const headers = Object.keys(rows[0] || {});
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((key) => escapeCsv(row[key])).join(","));
  }
  return lines.join("\n");
}

async function handleExportCsv() {
  try {
    formNote.textContent = "Preparing CSV...";
    const data = lastPricing || (await fetchAndRender());
    const csv = buildCsv(data);
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    const dateStamp = new Date().toISOString().slice(0, 10);
    link.download = `cloud-price-${dateStamp}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    formNote.textContent = "CSV exported.";
  } catch (error) {
    formNote.textContent =
      error?.message || "Could not export CSV. Try again.";
  }
}

function handleSavedCompareExport() {
  if (!savedCompareNote) {
    return;
  }
  if (!savedCompareRows.length) {
    savedCompareNote.textContent = "Run saved compare before exporting.";
    savedCompareNote.classList.add("negative");
    return;
  }
  savedCompareNote.classList.remove("negative");
  savedCompareNote.textContent = "Preparing saved compare CSV...";
  try {
    const csv = buildSavedCompareCsv(savedCompareRows);
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    const dateStamp = new Date().toISOString().slice(0, 10);
    link.download = `cloud-price-saved-${dateStamp}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    savedCompareNote.textContent = "Saved compare CSV exported.";
  } catch (error) {
    savedCompareNote.textContent =
      error?.message || "Could not export saved compare CSV.";
    savedCompareNote.classList.add("negative");
  }
}

function sanitizeScenarioFilename(name) {
  return name
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .toLowerCase();
}

function resolveScenarioForExport(scenario) {
  if (scenario) {
    return scenario;
  }
  const selectedId = scenarioList?.value;
  return selectedId
    ? getScenarioById(selectedId)
    : getScenarioByName(scenarioNameInput?.value || "");
}

function buildCsvFieldIndex(headers, fields) {
  const index = {};
  headers.forEach((header, idx) => {
    const normalized = normalizeCsvHeader(header);
    fields.forEach((field) => {
      if (
        normalized === normalizeCsvHeader(field.label) ||
        normalized === normalizeCsvHeader(field.key)
      ) {
        index[field.key] = idx;
      }
    });
  });
  return index;
}

function buildScenarioCsv(scenario) {
  const headers = SCENARIO_CSV_FIELDS.map((field) => field.label);
  const row = SCENARIO_CSV_FIELDS.map((field) => {
    if (field.key === "name") {
      return escapeCsv(scenario.name || "");
    }
    const value = scenario.input?.[field.key];
    if (field.type === "boolean") {
      return escapeCsv(value ? "true" : "false");
    }
    return escapeCsv(value ?? "");
  });
  return `${headers.join(",")}\n${row.join(",")}`;
}

function parseScenarioCsv(text) {
  const rows = parseCsvRows(text);
  if (rows.length < 2) {
    return [];
  }
  const headers = rows[0];
  const index = buildCsvFieldIndex(headers, SCENARIO_CSV_FIELDS);
  return rows.slice(1).map((row) => {
    const input = {};
    let name = "";
    SCENARIO_CSV_FIELDS.forEach((field) => {
      const idx = index[field.key];
      if (idx === undefined) {
        return;
      }
      const raw = row[idx] ?? "";
      if (field.key === "name") {
        name = raw.trim();
        return;
      }
      if (raw === "") {
        return;
      }
      if (field.type === "number") {
        const parsed = Number.parseFloat(raw);
        if (Number.isFinite(parsed)) {
          input[field.key] = parsed;
        }
        return;
      }
      if (field.type === "boolean") {
        const parsed = parseCsvBoolean(raw);
        if (parsed !== undefined) {
          input[field.key] = parsed;
        }
        return;
      }
      input[field.key] = raw;
    });
    return { name, input };
  }).filter((item) => item.name && Object.keys(item.input).length);
}

function handleExportScenario(scenario, noteTarget) {
  const targetNote = noteTarget || savedCompareNote || scenarioNote;
  const selectedScenario = resolveScenarioForExport(scenario);
  if (!selectedScenario) {
    setInlineNote(targetNote, "Select a scenario to export.", true);
    return;
  }
  const payload = {
    version: 1,
    scenario: {
      name: selectedScenario.name,
      input: selectedScenario.input,
      createdAt: selectedScenario.createdAt,
      updatedAt: selectedScenario.updatedAt,
    },
  };
  const blob = new Blob([JSON.stringify(payload, null, 2)], {
    type: "application/json",
  });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  const safeName =
    sanitizeScenarioFilename(selectedScenario.name) || "scenario";
  link.download = `cloud-price-${safeName}.json`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
  setInlineNote(targetNote, `Exported "${selectedScenario.name}".`);
}

function handleExportScenarioCsv(scenario, noteTarget) {
  const targetNote = noteTarget || savedCompareNote || scenarioNote;
  const selectedScenario = resolveScenarioForExport(scenario);
  if (!selectedScenario) {
    setInlineNote(targetNote, "Select a scenario to export.", true);
    return;
  }
  const csv = buildScenarioCsv(selectedScenario);
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  const safeName =
    sanitizeScenarioFilename(selectedScenario.name) || "scenario";
  link.download = `cloud-price-${safeName}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
  setInlineNote(
    targetNote,
    `Exported "${selectedScenario.name}" as CSV.`
  );
}

async function handleImportScenarioFile(event) {
  const targetNote = savedCompareNote || scenarioNote;
  const file = event?.target?.files?.[0];
  if (!file) {
    return;
  }
  try {
    const text = await file.text();
    const parsed = JSON.parse(text);
    const items = Array.isArray(parsed)
      ? parsed
      : Array.isArray(parsed?.scenarios)
      ? parsed.scenarios
      : parsed?.scenario
      ? [parsed.scenario]
      : parsed?.name && parsed?.input
      ? [parsed]
      : [];
    if (!items.length) {
      setInlineNote(targetNote, "No valid scenarios found in file.", true);
      return;
    }
    let importedCount = 0;
    const now = new Date().toISOString();
    items.forEach((item) => {
      const rawName = item?.name || "";
      const input = item?.input;
      if (!rawName || !input) {
        return;
      }
      const trimmed = rawName.trim();
      if (!trimmed) {
        return;
      }
      const name = getScenarioByName(trimmed)
        ? buildCloneName(trimmed)
        : trimmed;
      const scenarioId = `scn-${Date.now().toString(36)}-${Math.random()
        .toString(36)
        .slice(2, 7)}`;
      scenarioStore.push({
        id: scenarioId,
        name,
        input,
        createdAt: item?.createdAt || now,
        updatedAt: now,
      });
      importedCount += 1;
    });
    if (!importedCount) {
      setInlineNote(targetNote, "No valid scenarios found in file.", true);
      return;
    }
    persistScenarioStore(scenarioStore);
    renderScenarioList();
    setInlineNote(
      targetNote,
      `Imported ${importedCount} scenario(s).`
    );
  } catch (error) {
    setInlineNote(
      targetNote,
      error?.message || "Could not import scenario file.",
      true
    );
  } finally {
    if (importScenarioInput) {
      importScenarioInput.value = "";
    }
  }
}

async function handleImportScenarioCsvFile(event) {
  const targetNote = savedCompareNote || scenarioNote;
  const file = event?.target?.files?.[0];
  if (!file) {
    return;
  }
  try {
    const text = await file.text();
    const items = parseScenarioCsv(text);
    if (!items.length) {
      setInlineNote(targetNote, "No valid scenarios found in CSV.", true);
      return;
    }
    let importedCount = 0;
    const now = new Date().toISOString();
    items.forEach((item) => {
      const trimmed = item.name.trim();
      if (!trimmed) {
        return;
      }
      const name = getScenarioByName(trimmed)
        ? buildCloneName(trimmed)
        : trimmed;
      const scenarioId = `scn-${Date.now().toString(36)}-${Math.random()
        .toString(36)
        .slice(2, 7)}`;
      scenarioStore.push({
        id: scenarioId,
        name,
        input: item.input,
        createdAt: now,
        updatedAt: now,
      });
      importedCount += 1;
    });
    if (!importedCount) {
      setInlineNote(targetNote, "No valid scenarios found in CSV.", true);
      return;
    }
    persistScenarioStore(scenarioStore);
    renderScenarioList();
    setInlineNote(
      targetNote,
      `Imported ${importedCount} scenario(s) from CSV.`
    );
  } catch (error) {
    setInlineNote(
      targetNote,
      error?.message || "Could not import scenario CSV.",
      true
    );
  } finally {
    if (importScenarioCsvInput) {
      importScenarioCsvInput.value = "";
    }
  }
}

function buildPrivateProvidersCsv(providers) {
  const headers = PRIVATE_PROVIDER_CSV_FIELDS.map((field) => field.label);
  const lines = [headers.join(",")];
  providers.forEach((provider) => {
    const row = PRIVATE_PROVIDER_CSV_FIELDS.map((field) => {
      if (field.key === "name") {
        return escapeCsv(provider.name || "");
      }
      const value = provider.config?.[field.key];
      if (field.type === "boolean") {
        return escapeCsv(value ? "true" : "false");
      }
      return escapeCsv(value ?? "");
    });
    lines.push(row.join(","));
  });
  return lines.join("\n");
}

function parsePrivateProvidersCsv(text) {
  const rows = parseCsvRows(text);
  if (rows.length < 2) {
    return [];
  }
  const headers = rows[0];
  const index = buildCsvFieldIndex(headers, PRIVATE_PROVIDER_CSV_FIELDS);
  return rows.slice(1).map((row) => {
    const config = {};
    let name = "";
    PRIVATE_PROVIDER_CSV_FIELDS.forEach((field) => {
      const idx = index[field.key];
      if (idx === undefined) {
        return;
      }
      const raw = row[idx] ?? "";
      if (field.key === "name") {
        name = raw.trim();
        return;
      }
      if (raw === "") {
        return;
      }
      if (field.type === "number") {
        const parsed = Number.parseFloat(raw);
        if (Number.isFinite(parsed)) {
          config[field.key] = parsed;
        }
        return;
      }
      if (field.type === "boolean") {
        const parsed = parseCsvBoolean(raw);
        if (parsed !== undefined) {
          config[field.key] = parsed;
        }
        return;
      }
      config[field.key] = raw;
    });
    return { name, config: normalizePrivateConfig(config) };
  }).filter((item) => item.name);
}

function handleExportPrivateProvidersCsv() {
  if (!privateSaveNote) {
    return;
  }
  if (!privateProviderStore.providers.length) {
    setPrivateNote("No private providers to export.", true);
    return;
  }
  const csv = buildPrivateProvidersCsv(privateProviderStore.providers);
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  const dateStamp = new Date().toISOString().slice(0, 10);
  link.download = `private-providers-${dateStamp}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
  setPrivateNote("Private providers CSV exported.");
}

async function handleImportPrivateProvidersCsvFile(event) {
  if (!privateSaveNote) {
    return;
  }
  const file = event?.target?.files?.[0];
  if (!file) {
    return;
  }
  try {
    const text = await file.text();
    const providers = parsePrivateProvidersCsv(text);
    if (!providers.length) {
      setPrivateNote("No valid private providers found in CSV.", true);
      return;
    }
    let importedCount = 0;
    const now = new Date().toISOString();
    providers.forEach((item) => {
      const trimmed = item.name.trim();
      if (!trimmed) {
        return;
      }
      const existing = privateProviderStore.providers.find(
        (provider) =>
          provider.name.toLowerCase() === trimmed.toLowerCase()
      );
      if (existing) {
        existing.config = item.config;
        existing.updatedAt = now;
      } else {
        const id = `prv-${Date.now().toString(36)}-${Math.random()
          .toString(36)
          .slice(2, 7)}`;
        privateProviderStore.providers.push({
          id,
          name: trimmed,
          config: item.config,
          createdAt: now,
          updatedAt: now,
        });
      }
      importedCount += 1;
    });
    if (!importedCount) {
      setPrivateNote("No valid private providers found in CSV.", true);
      return;
    }
    persistPrivateProviders(privateProviderStore);
    renderPrivateProviderCards();
    setPrivateNote(`Imported ${importedCount} private provider(s).`);
  } catch (error) {
    setPrivateNote(
      error?.message || "Could not import private providers CSV.",
      true
    );
  } finally {
    if (importPrivateProvidersInput) {
      importPrivateProvidersInput.value = "";
    }
  }
}

function handleSaveScenario() {
  if (!scenarioNameInput) {
    return;
  }
  const name = scenarioNameInput.value.trim();
  if (!name) {
    setScenarioNote("Enter a scenario name to save.", true);
    return;
  }
  const payload = serializeForm(form);
  const existing = getScenarioByName(name);
  const timestamp = new Date().toISOString();
  let scenarioId = "";
  if (existing) {
    existing.input = payload;
    existing.updatedAt = timestamp;
    scenarioId = existing.id;
    setScenarioNote("Scenario updated.");
  } else {
    scenarioId = `scn-${Date.now().toString(36)}-${Math.random()
      .toString(36)
      .slice(2, 7)}`;
    scenarioStore.push({
      id: scenarioId,
      name,
      input: payload,
      createdAt: timestamp,
    });
    setScenarioNote("Scenario saved.");
  }
  persistScenarioStore(scenarioStore);
  renderScenarioList(scenarioId);
  if (currentResultsTab === "saved") {
    refreshSavedCompare();
  }
}

function handleLoadScenario() {
  if (!scenarioList) {
    return;
  }
  const scenario = getScenarioById(scenarioList.value);
  if (!scenario) {
    setScenarioNote("Select a scenario to load.", true);
    return;
  }
  applyScenarioInput(scenario.input);
  if (scenarioNameInput) {
    scenarioNameInput.value = scenario.name;
  }
  setScenarioNote(`Loaded "${scenario.name}".`);
  handleCompare();
}

function handleCloneScenario() {
  if (!scenarioList) {
    return;
  }
  const scenario = getScenarioById(scenarioList.value);
  if (!scenario) {
    setScenarioNote("Select a scenario to clone.", true);
    return;
  }
  const cloneName = buildCloneName(scenario.name);
  const timestamp = new Date().toISOString();
  const cloneId = `scn-${Date.now().toString(36)}-${Math.random()
    .toString(36)
    .slice(2, 7)}`;
  scenarioStore.push({
    id: cloneId,
    name: cloneName,
    input: { ...scenario.input },
    createdAt: timestamp,
  });
  persistScenarioStore(scenarioStore);
  renderScenarioList(cloneId);
  if (scenarioNameInput) {
    scenarioNameInput.value = cloneName;
  }
  applyScenarioInput(scenario.input);
  setScenarioNote(`Cloned "${scenario.name}" to "${cloneName}".`);
  handleCompare();
}

function handleDeleteScenario() {
  if (!scenarioList) {
    return;
  }
  const scenario = getScenarioById(scenarioList.value);
  if (!scenario) {
    setScenarioNote("Select a scenario to delete.", true);
    return;
  }
  deleteScenarioById(scenario.id);
  if (currentResultsTab === "saved") {
    refreshSavedCompare();
  }
  if (scenarioNameInput) {
    scenarioNameInput.value = "";
  }
  setScenarioNote(`Deleted "${scenario.name}".`);
}

async function handleCompareScenario() {
  if (!scenarioList || !scenarioDelta) {
    return;
  }
  const scenario = getScenarioById(scenarioList.value);
  if (!scenario) {
    setScenarioNote("Select a scenario to compare.", true);
    return;
  }
  scenarioDelta.classList.remove("is-hidden");
  scenarioDelta.textContent = "Comparing scenario...";
  try {
    const currentData = lastPricing || (await fetchAndRender());
    const scenarioData = await comparePricing(scenario.input);
    const comparison = buildScenarioComparison(
      currentData,
      scenarioData,
      scenario.name
    );
    scenarioDelta.textContent = comparison.text;
    scenarioDelta.classList.toggle("negative", comparison.diffTotal > 0);
  } catch (error) {
    scenarioDelta.textContent =
      error?.message || "Scenario comparison failed.";
    scenarioDelta.classList.add("negative");
  }
}

form.addEventListener("submit", handleCompare);
exportButton.addEventListener("click", handleExportCsv);
if (authOpenLoginButton && authForm) {
  authOpenLoginButton.addEventListener("click", () => {
    authForm.classList.toggle("is-hidden");
    if (!authForm.classList.contains("is-hidden")) {
      authUsernameInput?.focus();
    }
  });
}
if (authForm) {
  authForm.addEventListener("submit", handleAuthLoginSubmit);
}
if (authSaveDbButton) {
  authSaveDbButton.addEventListener("click", handleAuthSaveToDb);
}
if (authLogoutButton) {
  authLogoutButton.addEventListener("click", handleAuthLogout);
}
if (adminRefreshUsersButton) {
  adminRefreshUsersButton.addEventListener("click", () => {
    loadAdminUsers();
  });
}
if (dataTransferExportButton) {
  dataTransferExportButton.addEventListener("click", handleExportUserData);
}
if (dataTransferImportButton && dataTransferImportInput) {
  dataTransferImportButton.addEventListener("click", () => {
    dataTransferImportInput.click();
  });
  dataTransferImportInput.addEventListener("change", handleImportUserDataFile);
}
if (dataVersionRefreshButton) {
  dataVersionRefreshButton.addEventListener("click", () => {
    refreshDataVersionHistory();
  });
}
if (dataVersionRollbackButton) {
  dataVersionRollbackButton.addEventListener("click", handleDataVersionRollback);
}
if (adminAddUserForm) {
  adminAddUserForm.addEventListener("submit", handleAdminAddUserSubmit);
}
if (adminUpdatePasswordForm) {
  adminUpdatePasswordForm.addEventListener(
    "submit",
    handleAdminUpdatePasswordSubmit
  );
}
if (adminUsersTable) {
  adminUsersTable.addEventListener("click", (event) => {
    const roleButton = event.target.closest("[data-admin-role]");
    if (roleButton) {
      const encodedUsername = String(roleButton.dataset.adminRole || "");
      const role = String(roleButton.dataset.adminRoleValue || "");
      if (!encodedUsername || !role) {
        return;
      }
      handleAdminUpdateRole(decodeURIComponent(encodedUsername), role);
      return;
    }
    const removeButton = event.target.closest("[data-admin-delete]");
    if (!removeButton) {
      return;
    }
    const encoded = String(removeButton.dataset.adminDelete || "");
    if (!encoded) {
      return;
    }
    handleAdminDeleteUser(decodeURIComponent(encoded));
  });
}
if (authImportGuestButton) {
  authImportGuestButton.addEventListener("click", handleImportGuestState);
}
resultsTabButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const nextTab = button.dataset.results;
    if (currentMode === "network" || currentMode === "storage") {
      if (nextTab === "insight") {
        if (currentMode === "network") {
          setNetworkResultTab("insight");
        } else {
          setStorageResultTab("insight");
        }
        return;
      }
      if (nextTab === "pricing") {
        if (currentMode === "network") {
          setNetworkResultTab(
            currentNetworkResult === "insight" ? "vpc" : currentNetworkResult
          );
        } else {
          setStorageResultTab(
            currentStorageResult === "insight"
              ? "object"
              : currentStorageResult
          );
        }
      }
      return;
    }
    setResultsTab(nextTab);
  });
});
vendorSubtabButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const nextView = button.dataset.vendorView;
    setVendorSubtab(nextView);
  });
});
networkResultTabs.forEach((button) => {
  button.addEventListener("click", () => {
    setNetworkResultTab(button.dataset.networkResult);
  });
});
storageResultTabs.forEach((button) => {
  button.addEventListener("click", () => {
    setStorageResultTab(button.dataset.storageResult);
  });
});
networkAddonTabButtons.forEach((button) => {
  button.addEventListener("click", () => {
    setNetworkAddonFocus(button.dataset.networkFocus);
  });
});
if (runRegionCompareButton) {
  runRegionCompareButton.addEventListener("click", runRegionCompare);
}
if (savedCompareRefresh) {
  savedCompareRefresh.addEventListener("click", () => {
    const scenarioIds = resolveScenarioSelections();
    refreshSavedCompare({
      scenarioIds: scenarioIds.length ? scenarioIds : null,
    });
  });
}
if (savedComparePrivateRun) {
  savedComparePrivateRun.addEventListener("click", runSavedPrivateCompare);
}
Object.entries(commitDiscountInputs).forEach(([, input]) => {
  if (input) {
    input.addEventListener("input", () => renderCommit(lastPricing));
  }
});
if (runRecommendationsButton) {
  runRecommendationsButton.addEventListener("click", () => {
    renderRecommendations(lastPricing);
  });
}
if (recommendProviderFilter) {
  recommendProviderFilter.addEventListener("change", () => {
    renderRecommendations(lastPricing);
  });
}
if (recommendLimitInput) {
  recommendLimitInput.addEventListener("input", () => {
    renderRecommendations(lastPricing);
  });
}
if (saveScenarioButton) {
  saveScenarioButton.addEventListener("click", handleSaveScenario);
}
if (loadScenarioButton) {
  loadScenarioButton.addEventListener("click", handleLoadScenario);
}
if (cloneScenarioButton) {
  cloneScenarioButton.addEventListener("click", handleCloneScenario);
}
if (compareScenarioButton) {
  compareScenarioButton.addEventListener("click", handleCompareScenario);
}
if (deleteScenarioButton) {
  deleteScenarioButton.addEventListener("click", handleDeleteScenario);
}
if (importScenarioButton && importScenarioInput) {
  importScenarioButton.addEventListener("click", () => {
    importScenarioInput.click();
  });
  importScenarioInput.addEventListener("change", handleImportScenarioFile);
}
if (importScenarioCsvButton && importScenarioCsvInput) {
  importScenarioCsvButton.addEventListener("click", () => {
    importScenarioCsvInput.click();
  });
  importScenarioCsvInput.addEventListener("change", handleImportScenarioCsvFile);
}
if (exportPrivateProvidersButton) {
  exportPrivateProvidersButton.addEventListener(
    "click",
    handleExportPrivateProvidersCsv
  );
}
if (importPrivateProvidersButton && importPrivateProvidersInput) {
  importPrivateProvidersButton.addEventListener("click", () => {
    importPrivateProvidersInput.click();
  });
  importPrivateProvidersInput.addEventListener(
    "change",
    handleImportPrivateProvidersCsvFile
  );
}
if (addPrivateProviderButton) {
  addPrivateProviderButton.addEventListener("click", () => {
    addPrivateProviderCard();
  });
}
viewTabButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const nextView = button.dataset.view;
    setView(nextView);
    if (nextView && nextView !== "compare") {
      handleCompare();
    }
  });
});
if (scenarioList) {
  scenarioList.addEventListener("change", () => {
    const scenario = getScenarioById(scenarioList.value);
    if (scenarioNameInput) {
      scenarioNameInput.value = scenario?.name || "";
    }
  });
}
[awsInstanceFilter, azureInstanceFilter, gcpInstanceFilter].forEach((input) => {
  if (input) {
    input.addEventListener("input", refreshInstanceSelects);
  }
});
modeTabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    const nextPanel = tab.dataset.mode;
    setPanel(nextPanel);
    if (
      nextPanel !== "private" &&
      nextPanel !== "scenarios" &&
      nextPanel !== "saved" &&
      nextPanel !== "vsax-compare" &&
      nextPanel !== "billing" &&
      nextPanel !== "data-transfer" &&
      nextPanel !== "admin"
    ) {
      handleCompare();
    }
  });
});
if (vsaxCompareGroupSelect) {
  vsaxCompareGroupSelect.addEventListener("change", () => {
    vsaxCompareState.selectedGroupName = String(
      vsaxCompareGroupSelect.value || ""
    ).trim();
    persistVsaxComparePreferences();
    refreshVsaxCompareData({ force: true });
  });
}
if (vsaxCompareRegionSelect) {
  vsaxCompareRegionSelect.addEventListener("change", () => {
    vsaxCompareState.regionKey = String(vsaxCompareRegionSelect.value || "us-east")
      .trim()
      || "us-east";
    persistVsaxComparePreferences();
    refreshVsaxCompareData({ force: true });
  });
}
if (vsaxComparePricingProviderSelect) {
  vsaxComparePricingProviderSelect.addEventListener("change", () => {
    vsaxCompareState.pricingProvider =
      vsaxComparePricingProviderSelect.value === "api" ? "api" : "retail";
    persistVsaxComparePreferences();
    refreshVsaxCompareData({ force: true });
  });
}
if (vsaxCompareRefreshButton) {
  vsaxCompareRefreshButton.addEventListener("click", () => {
    refreshVsaxCompareData({ force: true });
  });
}
if (vsaxCompareExportCsvButton) {
  vsaxCompareExportCsvButton.addEventListener("click", handleVsaxCompareExportCsv);
}
if (vsaxCompareExportExcelButton) {
  vsaxCompareExportExcelButton.addEventListener(
    "click",
    handleVsaxCompareExportExcel
  );
}
if (vsaxCompareExportPdfButton) {
  vsaxCompareExportPdfButton.addEventListener("click", handleVsaxCompareExportPdf);
}
workloadSelect.addEventListener("change", () => {
  updateCpuOptions();
  updateInstanceOptions();
});
cpuSelect.addEventListener("change", () => {
  updateInstanceOptions();
});
if (networkProviderCards) {
  networkProviderCards.addEventListener("change", () => {
    if (currentMode === "network") {
      handleCompare();
    }
  });
}
if (storageProviderCards) {
  storageProviderCards.addEventListener("change", () => {
    if (currentMode === "storage") {
      handleCompare();
    }
  });
}
billingProviderTabs.forEach((button) => {
  button.addEventListener("click", () => {
    setBillingProvider(button.dataset.billingProvider);
  });
});
if (billingImportButton && billingImportInput) {
  billingImportButton.addEventListener("click", () => {
    billingImportInput.click();
  });
  billingImportInput.addEventListener("change", handleBillingImportFile);
}
if (billingExportButton) {
  billingExportButton.addEventListener("click", handleBillingExportCsv);
}
if (billingClearButton) {
  billingClearButton.addEventListener("click", handleBillingClearProvider);
}
if (billingClearAllButton) {
  billingClearAllButton.addEventListener("click", handleBillingClearAll);
}
if (billingProductFilter) {
  billingProductFilter.addEventListener("change", handleBillingProductFilterChange);
}
if (billingMonthFilter) {
  billingMonthFilter.addEventListener("change", handleBillingMonthFilterChange);
}
if (billingChartGroup) {
  billingChartGroup.addEventListener("change", handleBillingChartGroupChange);
}
if (billingTagService) {
  billingTagService.addEventListener("change", updateBillingTagEditorFields);
}
if (billingApplyTagsButton) {
  billingApplyTagsButton.addEventListener("click", handleBillingApplyTags);
}
if (billingClearTagsButton) {
  billingClearTagsButton.addEventListener("click", handleBillingClearTags);
}
if (billingBulkSelectVisible) {
  billingBulkSelectVisible.addEventListener("click", handleBillingBulkSelectVisible);
}
if (billingBulkClearSelection) {
  billingBulkClearSelection.addEventListener(
    "click",
    handleBillingBulkClearSelection
  );
}
if (billingBulkApply) {
  billingBulkApply.addEventListener("click", handleBillingBulkApply);
}
if (billingBulkClearTags) {
  billingBulkClearTags.addEventListener("click", handleBillingBulkClearTags);
}
if (billingTable) {
  billingTable.addEventListener("change", (event) => {
    const checkbox = event.target.closest("[data-billing-detail-select]");
    if (!checkbox) {
      return;
    }
    const key = String(checkbox.dataset.billingDetailSelect || "").trim();
    if (!key) {
      return;
    }
    const selected = getBillingSelectedDetailSet(currentBillingProvider);
    if (checkbox.checked) {
      selected.add(key);
    } else {
      selected.delete(key);
    }
    updateBillingBulkSelectionSummary(currentBillingProvider);
    syncBillingControls(getBillingPanelData(currentBillingProvider));
  });
  billingTable.addEventListener("click", (event) => {
    const inlineApplyButton = event.target.closest(
      "[data-billing-inline-bulk-apply]"
    );
    if (inlineApplyButton) {
      const appInput = billingTable.querySelector(
        "[data-billing-inline-bulk-product-app]"
      );
      const tagInput = billingTable.querySelector(
        "[data-billing-inline-bulk-tags]"
      );
      const productApp = String(appInput?.value || "").trim();
      const tags = parseBillingCustomTags(tagInput?.value || "");
      applyBillingTagsToSelected(productApp, tags, false);
      return;
    }

    const inlineClearTagsButton = event.target.closest(
      "[data-billing-inline-bulk-clear-tags]"
    );
    if (inlineClearTagsButton) {
      applyBillingTagsToSelected("", [], true);
      return;
    }

    const inlineClearSelectionButton = event.target.closest(
      "[data-billing-inline-bulk-clear-selection]"
    );
    if (inlineClearSelectionButton) {
      const selected = getBillingSelectedDetailSet(currentBillingProvider);
      selected.clear();
      renderBillingImportPanel();
      setInlineNote(billingNote, "Cleared line-item selection.");
      return;
    }

    const saveButton = event.target.closest("[data-billing-detail-save]");
    if (saveButton) {
      const key = saveButton.dataset.billingDetailSave || "";
      const { serviceName, detailName } = decodeBillingDetailKey(key);
      if (!serviceName || !detailName) {
        return;
      }
      const productInput = Array.from(
        billingTable.querySelectorAll("[data-billing-detail-product-app]")
      ).find((input) => input.dataset.billingDetailProductApp === key);
      const tagsInput = Array.from(
        billingTable.querySelectorAll("[data-billing-detail-tags]")
      ).find((input) => input.dataset.billingDetailTags === key);
      const productApp = String(productInput?.value || "").trim();
      const tags = parseBillingCustomTags(tagsInput?.value || "");
      setBillingDetailTag(currentBillingProvider, serviceName, detailName, {
        productApp,
        tags,
      });
      persistBillingDetailTagsStore(billingDetailTagStore);
      renderBillingImportPanel();
      setInlineNote(
        billingNote,
        `Saved line-item tags for "${detailName}" in "${serviceName}".`
      );
      return;
    }

    const clearButton = event.target.closest("[data-billing-detail-clear]");
    if (clearButton) {
      const key = clearButton.dataset.billingDetailClear || "";
      const { serviceName, detailName } = decodeBillingDetailKey(key);
      if (!serviceName || !detailName) {
        return;
      }
      setBillingDetailTag(currentBillingProvider, serviceName, detailName, null);
      persistBillingDetailTagsStore(billingDetailTagStore);
      renderBillingImportPanel();
      setInlineNote(
        billingNote,
        `Cleared line-item tags for "${detailName}" in "${serviceName}".`
      );
      return;
    }

    const toggleButton = event.target.closest("[data-billing-toggle]");
    if (!toggleButton) {
      return;
    }
    const encodedName = toggleButton.dataset.billingToggle || "";
    const serviceName = decodeURIComponent(encodedName);
    if (!serviceName) {
      return;
    }
    const expanded = getBillingExpandedServiceSet(currentBillingProvider);
    if (expanded.has(serviceName)) {
      expanded.delete(serviceName);
    } else {
      expanded.add(serviceName);
    }
    renderBillingImportPanel();
  });
}
sqlRateInput.addEventListener("input", () => {
  const rateValue = Number.parseFloat(sqlRateInput.value);
  sqlRateTouched = !isDefaultSqlRate(rateValue, sqlEditionSelect.value);
});
sqlEditionSelect.addEventListener("change", () => {
  if (!sqlRateTouched) {
    const nextRate = SQL_DEFAULTS[sqlEditionSelect.value];
    sqlRateInput.value = nextRate.toString();
  }
  const nextValue = Number.parseFloat(sqlRateInput.value);
  sqlRateTouched = !isDefaultSqlRate(nextValue, sqlEditionSelect.value);
});
let pricingWorkspaceInitialized = false;

async function initializePricingWorkspace() {
  if (pricingWorkspaceInitialized) {
    return;
  }
  pricingWorkspaceInitialized = true;
  await initializeAuthSession();
  await loadVsaxCapacityCatalog({ silent: true });
  reloadPersistentStateFromLocalStorage();
  setPanel(readSavedPanel() || modeInput.value);
  buildRegionChecklist();
  try {
    await loadSizeOptions();
  } catch (error) {
    formNote.textContent =
      error?.message ||
      "Could not load size options. Defaults will be used.";
    setSelectOptions(cpuSelect, [8], 8);
    setInstanceOptions(awsInstanceSelect, [], "");
    setInstanceOptions(azureInstanceSelect, [], "");
    setInstanceOptions(gcpInstanceSelect, [], "");
    const fallbackOptions = [{ key: "none", label: "None" }];
    setSelectOptionsWithLabels(awsVpcSelect, fallbackOptions, "none");
    setSelectOptionsWithLabels(awsFirewallSelect, fallbackOptions, "none");
    setSelectOptionsWithLabels(awsLbSelect, fallbackOptions, "none");
    setSelectOptionsWithLabels(azureVpcSelect, fallbackOptions, "none");
    setSelectOptionsWithLabels(azureFirewallSelect, fallbackOptions, "none");
    setSelectOptionsWithLabels(azureLbSelect, fallbackOptions, "none");
    setSelectOptionsWithLabels(gcpVpcSelect, fallbackOptions, "none");
    setSelectOptionsWithLabels(gcpFirewallSelect, fallbackOptions, "none");
    setSelectOptionsWithLabels(gcpLbSelect, fallbackOptions, "none");
  }
  if (
    activePanel !== "private" &&
    activePanel !== "scenarios" &&
    activePanel !== "saved" &&
    activePanel !== "vsax-compare" &&
    activePanel !== "billing" &&
    activePanel !== "data-transfer" &&
    activePanel !== "admin"
  ) {
    handleCompare();
  }
}

if (document.readyState === "complete") {
  void initializePricingWorkspace();
} else {
  window.addEventListener(
    "load",
    () => {
      void initializePricingWorkspace();
    },
    { once: true }
  );
}
