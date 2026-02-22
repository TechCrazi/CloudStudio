const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const express = require("express");
const Database = require("better-sqlite3");
const { PricingClient, GetProductsCommand } = require("@aws-sdk/client-pricing");
const { getAwsAccountConfigs } = require("../../aws-account-config");

function isContainerRuntime() {
  if (fs.existsSync("/.dockerenv")) {
    return true;
  }
  try {
    const cgroup = fs.readFileSync("/proc/1/cgroup", "utf8");
    return /(docker|containerd|kubepods|podman)/i.test(cgroup);
  } catch (_error) {
    return false;
  }
}

function loadLocalEnvFile() {
  if (String(process.env.CLOUDSTUDIO_SINGLE_ENV || "").trim() === "true") {
    return;
  }
  if (isContainerRuntime()) {
    return;
  }
  const envPath = path.resolve(__dirname, ".env");
  if (!fs.existsSync(envPath)) {
    return;
  }
  try {
    if (typeof process.loadEnvFile === "function") {
      process.loadEnvFile(envPath);
    } else {
      const raw = fs.readFileSync(envPath, "utf8");
      raw.split(/\r?\n/).forEach((line) => {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith("#")) {
          return;
        }
        const content = trimmed.startsWith("export ")
          ? trimmed.slice(7).trim()
          : trimmed;
        const separator = content.indexOf("=");
        if (separator <= 0) {
          return;
        }
        const key = content.slice(0, separator).trim();
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
          return;
        }
        let value = content.slice(separator + 1).trim();
        if (
          (value.startsWith('"') && value.endsWith('"')) ||
          (value.startsWith("'") && value.endsWith("'"))
        ) {
          value = value.slice(1, -1);
        }
        if (process.env[key] === undefined) {
          process.env[key] = value;
        }
      });
    }
    console.log(`[env] Loaded ${envPath} for local development.`);
  } catch (error) {
    console.warn(
      `[env] Failed to load ${envPath}:`,
      error?.message || String(error)
    );
  }
}

loadLocalEnvFile();

const fetcher =
  global.fetch ||
  ((...args) =>
    import("node-fetch").then(({ default: fetch }) => fetch(...args)));

const app = express();
app.set('etag', false);
const PORT = process.env.PORT || 3000;
const HOURS_IN_MONTH = 730;
const MIN_CPU = 8;
const MIN_MEMORY = 8;
const MIN_NETWORK_GBPS = 10;
const PRICING_WARMUP_ENABLED = process.env.PRICING_CACHE_WARMUP !== "false";
const PRICING_WARMUP_CONCURRENCY = Math.max(
  1,
  Number.parseInt(process.env.PRICING_CACHE_CONCURRENCY || "4", 10) || 4
);
const PRICING_CACHE_REFRESH_INTERVAL_MS = Math.max(
  60 * 1000,
  Number.parseInt(
    process.env.PRICING_CACHE_REFRESH_INTERVAL_MS || `${30 * 60 * 1000}`,
    10
  ) || 30 * 60 * 1000
);
const BACKUP_RETENTION_DAYS = 15;
const BACKUP_DAILY_DELTA_PERCENT = 10;
const K8S_OS_DISK_MIN_GB = 32;
const K8S_MIN_NODE_COUNT = 3;
const K8S_CONTROL_PLANE_HOURLY = {
  aws: 0.5,
  azure: 0.6,
  gcp: 0.5,
};
const AUTH_COOKIE_NAME = "cloud_price_session";
const MODULE_DATA_DIR = path.resolve(__dirname, "..", "..", "..", "data");
const DEFAULT_AUTH_DATA_DIR = path.join(MODULE_DATA_DIR, "auth");
const DEFAULT_AUTH_DB_FILE = path.join(MODULE_DATA_DIR, "cloudstudio.db");
const AUTH_DB_CONFIG_PROVIDED = Boolean(
  String(process.env.AUTH_DATA_DIR || "").trim() ||
    String(process.env.AUTH_DB_FILE || "").trim()
);
const AUTH_DATA_DIR = path.resolve(
  process.env.AUTH_DATA_DIR || DEFAULT_AUTH_DATA_DIR
);
const AUTH_DB_FILE = path.resolve(
  process.env.AUTH_DB_FILE || DEFAULT_AUTH_DB_FILE
);
const AUTH_USERS_FILE = path.resolve(
  process.env.AUTH_USERS_FILE || path.join(AUTH_DATA_DIR, "users.json")
);
const AUTH_STATE_DIR = path.resolve(
  process.env.AUTH_STATE_DIR || path.join(AUTH_DATA_DIR, "user-state")
);
const AUTH_SESSION_TTL_MS = Math.max(
  5 * 60 * 1000,
  Number.parseInt(
    process.env.AUTH_SESSION_TTL_MS || `${7 * 24 * 60 * 60 * 1000}`,
    10
  ) || 7 * 24 * 60 * 60 * 1000
);
const AUTH_STATE_MAX_BYTES = Math.max(
  32 * 1024,
  Number.parseInt(process.env.AUTH_STATE_MAX_BYTES || `${2 * 1024 * 1024}`, 10) ||
    2 * 1024 * 1024
);

const REGION_MAP = {
  "us-east": {
    label: "US East",
    aws: { region: "us-east-1", location: "US East (N. Virginia)" },
    azure: { region: "eastus", location: "East US" },
    gcp: { region: "us-east1", location: "South Carolina" },
  },
  "us-central": {
    label: "US Central",
    aws: { region: "us-east-2", location: "US East (Ohio)" },
    azure: { region: "centralus", location: "Central US" },
    gcp: { region: "us-central1", location: "Iowa" },
  },
  "us-west": {
    label: "US West",
    aws: { region: "us-west-2", location: "US West (Oregon)" },
    azure: { region: "westus2", location: "West US 2" },
    gcp: { region: "us-west1", location: "Oregon" },
  },
  "us-west-1": {
    label: "US West (N. California)",
    aws: { region: "us-west-1", location: "US West (N. California)" },
    azure: { region: "westus", location: "West US" },
    gcp: { region: "us-west2", location: "Los Angeles" },
  },
  "ca-central": {
    label: "Canada Central",
    aws: { region: "ca-central-1", location: "Canada (Central)" },
    azure: { region: "canadacentral", location: "Canada Central" },
    gcp: { region: "northamerica-northeast1", location: "Montreal" },
  },
  "eu-west": {
    label: "EU West",
    aws: { region: "eu-west-1", location: "EU (Ireland)" },
    azure: { region: "westeurope", location: "West Europe" },
    gcp: { region: "europe-west1", location: "Belgium" },
  },
  "eu-central": {
    label: "EU Central",
    aws: { region: "eu-central-1", location: "EU (Frankfurt)" },
    azure: { region: "germanywestcentral", location: "Germany West Central" },
    gcp: { region: "europe-west3", location: "Frankfurt" },
  },
  "eu-uk": {
    label: "UK South",
    aws: { region: "eu-west-2", location: "EU (London)" },
    azure: { region: "uksouth", location: "UK South" },
    gcp: { region: "europe-west2", location: "London" },
  },
  "eu-north": {
    label: "EU North",
    aws: { region: "eu-north-1", location: "EU (Stockholm)" },
    azure: { region: "northeurope", location: "North Europe" },
    gcp: { region: "europe-north1", location: "Finland" },
  },
  "ap-sg": {
    label: "Asia Pacific (Singapore)",
    aws: { region: "ap-southeast-1", location: "Asia Pacific (Singapore)" },
    azure: { region: "southeastasia", location: "Southeast Asia" },
    gcp: { region: "asia-southeast1", location: "Singapore" },
  },
  "ap-jp": {
    label: "Japan East",
    aws: { region: "ap-northeast-1", location: "Asia Pacific (Tokyo)" },
    azure: { region: "japaneast", location: "Japan East" },
    gcp: { region: "asia-northeast1", location: "Tokyo" },
  },
  "ap-in": {
    label: "India Central",
    aws: { region: "ap-south-1", location: "Asia Pacific (Mumbai)" },
    azure: { region: "centralindia", location: "Central India" },
    gcp: { region: "asia-south1", location: "Mumbai" },
  },
  "ap-au": {
    label: "Australia East",
    aws: { region: "ap-southeast-2", location: "Asia Pacific (Sydney)" },
    azure: { region: "australiaeast", location: "Australia East" },
    gcp: { region: "australia-southeast1", location: "Sydney" },
  },
  "sa-east": {
    label: "South America (Sao Paulo)",
    aws: { region: "sa-east-1", location: "South America (Sao Paulo)" },
    azure: { region: "brazilsouth", location: "Brazil South" },
    gcp: { region: "southamerica-east1", location: "Sao Paulo" },
  },
  "af-south": {
    label: "Africa South",
    aws: { region: "af-south-1", location: "Africa (Cape Town)" },
    azure: { region: "southafricanorth", location: "South Africa North" },
    gcp: { region: "africa-south1", location: "Johannesburg" },
  },
};

const AWS_FAMILIES = {
  general: {
    label: "General Purpose (M6i)",
    sizes: [
      { type: "m6i.large", vcpu: 2, memory: 8, networkGbps: 12.5, localDisk: false },
      { type: "m6i.xlarge", vcpu: 4, memory: 16, networkGbps: 12.5, localDisk: false },
      { type: "m6i.2xlarge", vcpu: 8, memory: 32, networkGbps: 12.5, localDisk: false },
      { type: "m6i.4xlarge", vcpu: 16, memory: 64, networkGbps: 25, localDisk: false },
      { type: "m6i.8xlarge", vcpu: 32, memory: 128, networkGbps: 25, localDisk: false },
      { type: "m6i.12xlarge", vcpu: 48, memory: 192, networkGbps: 50, localDisk: false },
      { type: "m6i.16xlarge", vcpu: 64, memory: 256, networkGbps: 50, localDisk: false },
      { type: "m6i.24xlarge", vcpu: 96, memory: 384, networkGbps: 50, localDisk: false },
      { type: "m6i.32xlarge", vcpu: 128, memory: 512, networkGbps: 50, localDisk: false },
    ],
  },
  compute: {
    label: "Compute Optimized (C6i)",
    sizes: [
      { type: "c6i.large", vcpu: 2, memory: 4, networkGbps: 12.5, localDisk: false },
      { type: "c6i.xlarge", vcpu: 4, memory: 8, networkGbps: 12.5, localDisk: false },
      { type: "c6i.2xlarge", vcpu: 8, memory: 16, networkGbps: 12.5, localDisk: false },
      { type: "c6i.4xlarge", vcpu: 16, memory: 32, networkGbps: 25, localDisk: false },
      { type: "c6i.8xlarge", vcpu: 32, memory: 64, networkGbps: 25, localDisk: false },
      { type: "c6i.12xlarge", vcpu: 48, memory: 96, networkGbps: 50, localDisk: false },
      { type: "c6i.16xlarge", vcpu: 64, memory: 128, networkGbps: 50, localDisk: false },
      { type: "c6i.24xlarge", vcpu: 96, memory: 192, networkGbps: 50, localDisk: false },
      { type: "c6i.32xlarge", vcpu: 128, memory: 256, networkGbps: 50, localDisk: false },
    ],
  },
  memory: {
    label: "Memory Optimized (R6i + X2idn)",
    sizes: [
      { type: "r6i.large", vcpu: 2, memory: 16, networkGbps: 12.5, localDisk: false },
      { type: "r6i.xlarge", vcpu: 4, memory: 32, networkGbps: 12.5, localDisk: false },
      { type: "r6i.2xlarge", vcpu: 8, memory: 64, networkGbps: 12.5, localDisk: false },
      { type: "r6i.4xlarge", vcpu: 16, memory: 128, networkGbps: 25, localDisk: false },
      { type: "r6i.8xlarge", vcpu: 32, memory: 256, networkGbps: 25, localDisk: false },
      { type: "r6i.12xlarge", vcpu: 48, memory: 384, networkGbps: 50, localDisk: false },
      { type: "r6i.16xlarge", vcpu: 64, memory: 512, networkGbps: 50, localDisk: false },
      { type: "r6i.24xlarge", vcpu: 96, memory: 768, networkGbps: 50, localDisk: false },
      { type: "r6i.32xlarge", vcpu: 128, memory: 1024, networkGbps: 50, localDisk: false },
      { type: "x2idn.16xlarge", vcpu: 64, memory: 1024, networkGbps: 50, localDisk: false },
      { type: "x2idn.32xlarge", vcpu: 128, memory: 2048, networkGbps: 50, localDisk: false },
    ],
  },
};

const AZURE_FAMILIES = {
  general: {
    label: "General Purpose (Dsv5)",
    sizes: [
      { type: "Standard_D2s_v5", vcpu: 2, memory: 8, networkGbps: 10, localDisk: false },
      { type: "Standard_D4s_v5", vcpu: 4, memory: 16, networkGbps: 10, localDisk: false },
      { type: "Standard_D8s_v5", vcpu: 8, memory: 32, networkGbps: 10, localDisk: false },
      { type: "Standard_D16s_v5", vcpu: 16, memory: 64, networkGbps: 10, localDisk: false },
      { type: "Standard_D32s_v5", vcpu: 32, memory: 128, networkGbps: 10, localDisk: false },
      { type: "Standard_D64s_v5", vcpu: 64, memory: 256, networkGbps: 10, localDisk: false },
      { type: "Standard_D96s_v5", vcpu: 96, memory: 384, networkGbps: 10, localDisk: false },
    ],
  },
  compute: {
    label: "Compute Optimized (Fsv2)",
    sizes: [
      { type: "Standard_F2s_v2", vcpu: 2, memory: 4, networkGbps: 10, localDisk: false },
      { type: "Standard_F4s_v2", vcpu: 4, memory: 8, networkGbps: 10, localDisk: false },
      { type: "Standard_F8s_v2", vcpu: 8, memory: 16, networkGbps: 10, localDisk: false },
      { type: "Standard_F16s_v2", vcpu: 16, memory: 32, networkGbps: 10, localDisk: false },
      { type: "Standard_F32s_v2", vcpu: 32, memory: 64, networkGbps: 10, localDisk: false },
      { type: "Standard_F64s_v2", vcpu: 64, memory: 128, networkGbps: 10, localDisk: false },
      { type: "Standard_F72s_v2", vcpu: 72, memory: 144, networkGbps: 10, localDisk: false },
    ],
  },
  memory: {
    label: "Memory Optimized (Esv5 + M series)",
    sizes: [
      { type: "Standard_E2s_v5", vcpu: 2, memory: 16, networkGbps: 10, localDisk: false },
      { type: "Standard_E4s_v5", vcpu: 4, memory: 32, networkGbps: 10, localDisk: false },
      { type: "Standard_E8s_v5", vcpu: 8, memory: 64, networkGbps: 10, localDisk: false },
      { type: "Standard_E16s_v5", vcpu: 16, memory: 128, networkGbps: 10, localDisk: false },
      { type: "Standard_E32s_v5", vcpu: 32, memory: 256, networkGbps: 10, localDisk: false },
      { type: "Standard_E64s_v5", vcpu: 64, memory: 512, networkGbps: 10, localDisk: false },
      { type: "Standard_E96s_v5", vcpu: 96, memory: 672, networkGbps: 10, localDisk: false },
      { type: "Standard_M64s", vcpu: 64, memory: 1024, networkGbps: 10, localDisk: false },
      { type: "Standard_M128s", vcpu: 128, memory: 2048, networkGbps: 10, localDisk: false },
      { type: "Standard_M128ms", vcpu: 128, memory: 3892, networkGbps: 10, localDisk: false },
    ],
  },
};

const VM_WORKLOADS = {
  general: {
    label: "General purpose",
    flavors: {
      aws: ["general", "compute", "memory"],
      azure: ["general", "compute", "memory"],
      gcp: ["general", "compute", "memory"],
    },
    defaults: {
      aws: "general",
      azure: "general",
      gcp: "general",
    },
  },
  sql: {
    label: "SQL Server",
    flavors: {
      aws: ["memory", "general"],
      azure: ["memory", "general"],
      gcp: ["memory", "general"],
    },
    defaults: {
      aws: "memory",
      azure: "memory",
      gcp: "memory",
    },
  },
  web: {
    label: "Web Server",
    flavors: {
      aws: ["compute", "general"],
      azure: ["compute", "general"],
      gcp: ["compute", "general"],
    },
    defaults: {
      aws: "compute",
      azure: "compute",
      gcp: "compute",
    },
  },
};

const K8S_FLAVORS = {
  aws: ["general", "compute"],
  azure: ["general", "compute"],
  gcp: ["general", "compute"],
};
const K8S_DEFAULT_FLAVORS = {
  aws: "general",
  azure: "general",
  gcp: "general",
};

const DISK_TIERS = {
  premium: {
    label: "Premium SSD",
    storageRates: {
      aws: 0.125,
      azure: 0.12,
      gcp: 0.17,
    },
    snapshotRates: {
      aws: 0.125,
      azure: 0.12,
      gcp: 0.17,
    },
  },
  max: {
    label: "Max performance (Ultra/Extreme/io2 BE)",
    storageRates: {
      aws: 0.25,
      azure: 0.24,
      gcp: 0.34,
    },
    snapshotRates: {
      aws: 0.25,
      azure: 0.24,
      gcp: 0.34,
    },
  },
};
const DEFAULT_DISK_TIER = "max";

const K8S_SHARED_STORAGE_DEFAULT_RATES = {
  aws: 0.3,
  azure: 0.16,
  gcp: 0.3,
};
const K8S_SHARED_STORAGE_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const AWS_EFS_REGION_INDEX_URL =
  "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEFS/current/region_index.json";
const GCP_FILESTORE_PRICING_URL = "https://cloud.google.com/filestore/pricing";

const EGRESS_RATES = {
  aws: 0.09,
  azure: 0.087,
  gcp: 0.12,
};
const NETWORK_TRAFFIC_RATES = {
  aws: { interVlan: 0.01, intraVlan: 0, interRegion: 0.02 },
  azure: { interVlan: 0.01, intraVlan: 0, interRegion: 0.02 },
  gcp: { interVlan: 0.01, intraVlan: 0, interRegion: 0.02 },
};
const NETWORK_ADDON_DATA_RATES = {
  aws: { vpc: 0, gateway: 0.02, firewall: 0.01, loadBalancer: 0.008 },
  azure: { vpc: 0, gateway: 0.018, firewall: 0.016, loadBalancer: 0.008 },
  gcp: { vpc: 0, gateway: 0.018, firewall: 0.01, loadBalancer: 0.01 },
};
const STORAGE_CATEGORY_DEFAULT_RATES = {
  aws: { object: 0.023, file: 0.3, table: 0.25, queue: 0 },
  azure: { object: 0.018, file: 0.16, table: 0.06, queue: 0.06 },
  gcp: { object: 0.02, file: 0.3, table: 0.17, queue: 0.08 },
};
const STORAGE_REPLICATION_DEFAULT_RATES = {
  aws: 0.02,
  azure: 0.02,
  gcp: 0.02,
};
const STORAGE_PERFORMANCE_RATES = {
  aws: {
    iopsMonthly: 0.005,
    throughputMonthly: 0.04,
    requestMonthly: 0.04,
    operationMonthly: 0.05,
  },
  azure: {
    iopsMonthly: 0,
    throughputMonthly: 0,
    requestMonthly: 0.03,
    operationMonthly: 0.03,
  },
  gcp: {
    iopsMonthly: 0,
    throughputMonthly: 0,
    requestMonthly: 0.05,
    operationMonthly: 0.04,
  },
};
const NETWORK_ADDON_OPTIONS = {
  aws: {
    vpc: [
      { key: "none", label: "None", pricing: { type: "static", hourly: 0 } },
      { key: "vpc-base", label: "VPC (base)", pricing: { type: "static", hourly: 0 } },
      {
        key: "transit-gateway",
        label: "Transit Gateway (per hour)",
        pricing: {
          type: "aws-price-list",
          serviceCode: "AmazonVPC",
          usagetypeIncludes: "TransitGateway-Hours",
          operationIncludes: "TransitGatewayVPC",
        },
      },
    ],
    gateway: [
      { key: "none", label: "None", pricing: { type: "static", hourly: 0 } },
      {
        key: "transit-gateway",
        label: "Transit Gateway",
        pricing: {
          type: "aws-price-list",
          serviceCode: "AmazonVPC",
          usagetypeIncludes: "TransitGateway-Hours",
          operationIncludes: "TransitGatewayVPC",
        },
      },
      {
        key: "vpn-gateway",
        label: "VPN Gateway",
        pricing: {
          type: "aws-price-list",
          serviceCode: "AmazonVPC",
          usagetypeIncludes: "VPNConnection-Hours",
        },
      },
    ],
    firewall: [
      { key: "none", label: "None", pricing: { type: "static", hourly: 0 } },
      {
        key: "standard",
        label: "Network Firewall (Standard)",
        pricing: {
          type: "aws-price-list",
          serviceCode: "AWSNetworkFirewall",
          usagetypeIncludes: /\\bEndpoint-Hour\\b/i,
          subcategoryIncludes: /^Endpoint$/i,
        },
      },
      {
        key: "advanced",
        label: "Network Firewall (Advanced inspection)",
        pricing: {
          type: "aws-price-list",
          serviceCode: "AWSNetworkFirewall",
          usagetypeIncludes: /Advanced-Inspection-Endpoint-Hour/i,
          subcategoryIncludes: /Endpoint-Advanced/i,
        },
      },
    ],
    loadBalancer: [
      { key: "none", label: "None", pricing: { type: "static", hourly: 0 } },
      {
        key: "classic",
        label: "Classic ELB",
        pricing: {
          type: "aws-price-list",
          serviceCode: "AWSELB",
          usagetypeIncludes: "LoadBalancerUsage",
        },
      },
      {
        key: "application",
        label: "Application LB (ALB)",
        pricing: {
          type: "aws-price-list",
          serviceCode: "AWSELB",
          usagetypeIncludes: "LoadBalancerUsage",
        },
      },
      {
        key: "network",
        label: "Network LB (NLB)",
        pricing: {
          type: "aws-price-list",
          serviceCode: "AWSELB",
          usagetypeIncludes: "LoadBalancerUsage",
        },
      },
      {
        key: "gateway",
        label: "Gateway LB (GWLB)",
        pricing: {
          type: "aws-price-list",
          serviceCode: "AWSELB",
          usagetypeIncludes: "LoadBalancerUsage",
        },
      },
    ],
  },
  azure: {
    vpc: [
      { key: "none", label: "None", pricing: { type: "static", hourly: 0 } },
      { key: "vnet-base", label: "VNet (base)", pricing: { type: "static", hourly: 0 } },
      {
        key: "vpn-basic",
        label: "VPN Gateway Basic",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "Basic",
          meterNameIncludes: "Basic",
        },
      },
      {
        key: "vpn-gw1",
        label: "VPN Gateway VpnGw1",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw1",
          meterNameIncludes: "VpnGw1",
        },
      },
      {
        key: "vpn-gw1az",
        label: "VPN Gateway VpnGw1AZ",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw1AZ",
          meterNameIncludes: "VpnGw1AZ",
        },
      },
      {
        key: "vpn-gw2",
        label: "VPN Gateway VpnGw2",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw2",
          meterNameIncludes: "VpnGw2",
        },
      },
      {
        key: "vpn-gw2az",
        label: "VPN Gateway VpnGw2AZ",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw2AZ",
          meterNameIncludes: "VpnGw2AZ",
        },
      },
      {
        key: "vpn-gw3",
        label: "VPN Gateway VpnGw3",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw3",
          meterNameIncludes: "VpnGw3",
        },
      },
      {
        key: "vpn-gw3az",
        label: "VPN Gateway VpnGw3AZ",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw3AZ",
          meterNameIncludes: "VpnGw3AZ",
        },
      },
      {
        key: "vpn-gw4",
        label: "VPN Gateway VpnGw4",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw4",
          meterNameIncludes: "VpnGw4",
        },
      },
      {
        key: "vpn-gw4az",
        label: "VPN Gateway VpnGw4AZ",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw4AZ",
          meterNameIncludes: "VpnGw4AZ",
        },
      },
      {
        key: "vpn-gw5",
        label: "VPN Gateway VpnGw5",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw5",
          meterNameIncludes: "VpnGw5",
        },
      },
      {
        key: "vpn-gw5az",
        label: "VPN Gateway VpnGw5AZ",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw5AZ",
          meterNameIncludes: "VpnGw5AZ",
        },
      },
    ],
    gateway: [
      { key: "none", label: "None", pricing: { type: "static", hourly: 0 } },
      {
        key: "vpn-gw1",
        label: "VPN Gateway VpnGw1",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw1",
          meterNameIncludes: "VpnGw1",
        },
      },
      {
        key: "vpn-gw2",
        label: "VPN Gateway VpnGw2",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw2",
          meterNameIncludes: "VpnGw2",
        },
      },
      {
        key: "vpn-gw3",
        label: "VPN Gateway VpnGw3",
        pricing: {
          type: "azure-retail",
          serviceName: "VPN Gateway",
          skuName: "VpnGw3",
          meterNameIncludes: "VpnGw3",
        },
      },
    ],
    firewall: [
      { key: "none", label: "None", pricing: { type: "static", hourly: 0 } },
      {
        key: "basic",
        label: "Azure Firewall Basic",
        pricing: {
          type: "azure-retail",
          serviceName: "Azure Firewall",
          skuName: "Basic",
          meterNameIncludes: "Deployment",
        },
      },
      {
        key: "standard",
        label: "Azure Firewall Standard",
        pricing: {
          type: "azure-retail",
          serviceName: "Azure Firewall",
          skuName: "Standard",
          meterNameIncludes: "Deployment",
        },
      },
      {
        key: "premium",
        label: "Azure Firewall Premium",
        pricing: {
          type: "azure-retail",
          serviceName: "Azure Firewall",
          skuName: "Premium",
          meterNameIncludes: "Deployment",
        },
      },
    ],
    loadBalancer: [
      { key: "none", label: "None", pricing: { type: "static", hourly: 0 } },
      {
        key: "standard",
        label: "Standard Load Balancer (L4)",
        pricing: {
          type: "azure-retail",
          serviceName: "Load Balancer",
          skuName: "Standard",
          unitIncludes: "Hour",
        },
      },
      {
        key: "appgw-basic-small",
        label: "App Gateway Basic (Small)",
        pricing: {
          type: "azure-retail",
          serviceName: "Application Gateway",
          productNameIncludes: "Basic Application Gateway",
          meterNameIncludes: "Small Gateway",
        },
      },
      {
        key: "appgw-basic-medium",
        label: "App Gateway Basic (Medium)",
        pricing: {
          type: "azure-retail",
          serviceName: "Application Gateway",
          productNameIncludes: "Basic Application Gateway",
          meterNameIncludes: "Medium Gateway",
        },
      },
      {
        key: "appgw-basic-large",
        label: "App Gateway Basic (Large)",
        pricing: {
          type: "azure-retail",
          serviceName: "Application Gateway",
          productNameIncludes: "Basic Application Gateway",
          meterNameIncludes: "Large Gateway",
        },
      },
      {
        key: "appgw-waf-medium",
        label: "App Gateway WAF (Medium)",
        pricing: {
          type: "azure-retail",
          serviceName: "Application Gateway",
          productNameIncludes: "WAF Application Gateway",
          meterNameIncludes: "Medium Gateway",
        },
      },
      {
        key: "appgw-waf-large",
        label: "App Gateway WAF (Large)",
        pricing: {
          type: "azure-retail",
          serviceName: "Application Gateway",
          productNameIncludes: "WAF Application Gateway",
          meterNameIncludes: "Large Gateway",
        },
      },
    ],
  },
  gcp: {
    vpc: [
      { key: "none", label: "None", pricing: { type: "static", hourly: 0 } },
      { key: "vpc-base", label: "VPC (base)", pricing: { type: "static", hourly: 0 } },
      {
        key: "cloud-vpn",
        label: "Cloud VPN (HA)",
        pricing: {
          type: "gcp-billing",
          serviceName: "Cloud VPN",
          descriptionPatterns: ["VPN", "tunnel"],
        },
      },
    ],
    gateway: [
      { key: "none", label: "None", pricing: { type: "static", hourly: 0 } },
      {
        key: "cloud-vpn",
        label: "Cloud VPN (HA)",
        pricing: {
          type: "gcp-billing",
          serviceName: "Cloud VPN",
          descriptionPatterns: ["VPN", "tunnel"],
        },
      },
    ],
    firewall: [
      { key: "none", label: "None", pricing: { type: "static", hourly: 0 } },
      { key: "vpc-firewall", label: "VPC Firewall (rules)", pricing: { type: "static", hourly: 0 } },
      {
        key: "cloud-armor",
        label: "Cloud Armor (WAF)",
        pricing: {
          type: "gcp-billing",
          serviceName: "Cloud Armor",
          descriptionPatterns: ["policy"],
        },
      },
    ],
    loadBalancer: [
      { key: "none", label: "None", pricing: { type: "static", hourly: 0 } },
      {
        key: "external-http",
        label: "External HTTP(S) LB",
        pricing: {
          type: "gcp-billing",
          serviceName: "Cloud Load Balancing",
          descriptionPatterns: ["Forwarding Rule", "HTTP"],
        },
      },
      {
        key: "external-tcp",
        label: "External TCP/UDP LB",
        pricing: {
          type: "gcp-billing",
          serviceName: "Cloud Load Balancing",
          descriptionPatterns: ["Forwarding Rule"],
        },
      },
      {
        key: "internal",
        label: "Internal LB",
        pricing: {
          type: "gcp-billing",
          serviceName: "Cloud Load Balancing",
          descriptionPatterns: ["Forwarding Rule", "Internal"],
        },
      },
    ],
  },
};
const NETWORK_ADDON_DEFAULTS = {
  aws: {
    vpc: "none",
    gateway: "none",
    firewall: "none",
    loadBalancer: "none",
  },
  azure: {
    vpc: "none",
    gateway: "none",
    firewall: "none",
    loadBalancer: "none",
  },
  gcp: {
    vpc: "none",
    gateway: "none",
    firewall: "none",
    loadBalancer: "none",
  },
};

const SQL_LICENSE_RATES = {
  standard: 0.35,
  enterprise: 0.5,
};

const AWS_PUBLIC_PRICING_URL = "https://instances.vantage.sh/instances.json";
const AWS_PUBLIC_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const AWS_PRICE_LIST_BASE_URL = "https://pricing.us-east-1.amazonaws.com";
const AWS_PRICE_LIST_REGION_INDEX_URL =
  "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/region_index.json";
const AWS_PRICE_LIST_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const AZURE_PUBLIC_PRICING_URL = "https://instances.vantage.sh/azure/instances.json";
const AZURE_PUBLIC_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const GCP_PUBLIC_PRICING_URL = "https://instances.vantage.sh/gcp/instances.json";
const GCP_PUBLIC_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const GCP_BILLING_SERVICE_ID = "6F81-5844-456A";
const GCP_BILLING_CACHE_TTL_MS = 6 * 60 * 60 * 1000;
const AZURE_PUBLIC_RESERVED_KEYS = {
  1: "yrTerm1Standard.allUpfront",
  3: "yrTerm3Standard.allUpfront",
};
const AWS_RESERVED_KEYS = {
  standard: {
    1: "yrTerm1Standard.noUpfront",
    3: "yrTerm3Standard.noUpfront",
  },
  convertible: {
    1: "yrTerm1Convertible.noUpfront",
    3: "yrTerm3Convertible.noUpfront",
  },
};
const AZURE_RESERVATION_TERM_HOURS = {
  1: 8760,
  3: 26280,
};
const AZURE_VANTAGE_REGION_MAP = {
  eastus: "us-east",
  centralus: "us-central",
  westus: "us-west",
  westus2: "us-west-2",
  canadacentral: "canada-central",
  westeurope: "europe-west",
  germanywestcentral: "germany-west-central",
  uksouth: "united-kingdom-south",
  northeurope: "europe-north",
  southeastasia: "asia-pacific-southeast",
  japaneast: "japan-east",
  centralindia: "central-india",
  australiaeast: "australia-east",
  brazilsouth: "brazil-south",
  southafricanorth: "south-africa-north",
};
const GCP_FLAVOR_MAP = {
  general: ["General purpose"],
  compute: ["Compute optimized"],
  memory: ["Memory optimized"],
};
const GCP_FLAVOR_LABELS = {
  general: "General purpose",
  compute: "Compute optimized",
  memory: "Memory optimized",
};
const GCP_FAMILY_TO_FLAVOR = Object.entries(GCP_FLAVOR_MAP).reduce(
  (acc, [flavorKey, families]) => {
    families.forEach((family) => {
      acc[family] = flavorKey;
    });
    return acc;
  },
  {}
);

let awsPricingClient = null;
let awsPricingClientKey = "";
const awsCache = new Map();
const awsPublicCache = { loadedAt: 0, data: null };
const awsPriceListIndexCache = { loadedAt: 0, data: null };
const awsPriceListRegionCache = new Map();
const awsServiceIndexCache = new Map();
const awsServiceRegionCache = new Map();
const azureCache = new Map();
const azureReservedCache = new Map();
const azurePublicCache = { loadedAt: 0, data: null };
const azureNetworkCache = new Map();
const gcpPublicCache = { loadedAt: 0, data: null };
const gcpBillingCache = { loadedAt: 0, data: null };
const gcpApiCache = new Map();
const gcpServiceCache = { loadedAt: 0, data: null };
const gcpServiceSkuCache = new Map();
const awsEfsRegionIndexCache = { loadedAt: 0, data: null };
const k8sSharedStorageCache = new Map();
const authSessions = new Map();
let authDb = null;
let authStatements = null;
let authInitPromise = null;
let cacheRefreshTimer = null;
let cacheRefreshRunning = false;
let lastCacheRefreshAt = 0;
let lastCacheRefreshStatus = "never";
let lastCacheRefreshSummary = null;
let lastCacheRefreshError = null;

function normalizeAuthUsername(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

const AUTH_DEFAULT_ADMIN_USERS = new Set(
  String(process.env.APP_ADMIN_USERS || "smit")
    .split(",")
    .map((value) => normalizeAuthUsername(value))
    .filter(Boolean)
);

function normalizeAuthAdminFlag(value) {
  return Number(value) === 1 || value === true;
}

function parseAuthAdminInput(value) {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (value === 1) {
      return true;
    }
    if (value === 0) {
      return false;
    }
    return null;
  }
  const text = String(value || "")
    .trim()
    .toLowerCase();
  if (!text) {
    return null;
  }
  if (["1", "true", "admin"].includes(text)) {
    return true;
  }
  if (["0", "false", "user", "regular"].includes(text)) {
    return false;
  }
  return null;
}

function normalizeAuthUserRecord(record) {
  if (!record || typeof record !== "object") {
    return null;
  }
  return {
    ...record,
    isAdmin: normalizeAuthAdminFlag(record.isAdmin),
  };
}

function isAuthAdmin(usernameRaw) {
  const username = normalizeAuthUsername(usernameRaw);
  if (!username) {
    return false;
  }
  const user = getAuthUser(username);
  if (user) {
    return normalizeAuthAdminFlag(user.isAdmin);
  }
  return AUTH_DEFAULT_ADMIN_USERS.has(username);
}

function hashAuthPassword(password, saltHex) {
  const salt = saltHex || crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(password, salt, 64).toString("hex");
  return { salt, hash };
}

function verifyAuthPassword(password, userRecord) {
  if (!userRecord || !userRecord.passwordSalt || !userRecord.passwordHash) {
    return false;
  }
  const { hash } = hashAuthPassword(password, userRecord.passwordSalt);
  const expected = Buffer.from(userRecord.passwordHash, "hex");
  const actual = Buffer.from(hash, "hex");
  if (expected.length !== actual.length) {
    return false;
  }
  return crypto.timingSafeEqual(expected, actual);
}

function ensureAuthStorage() {
  fs.mkdirSync(AUTH_DATA_DIR, { recursive: true });
  fs.mkdirSync(AUTH_STATE_DIR, { recursive: true });
}

function readJsonFile(filePath, fallback) {
  try {
    const raw = fs.readFileSync(filePath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    return fallback;
  }
}

function buildLegacyStatePath(usernameRaw) {
  const username = normalizeAuthUsername(usernameRaw);
  const hash = crypto.createHash("sha256").update(username).digest("hex");
  return path.join(AUTH_STATE_DIR, `${hash}.json`);
}

function persistAuthDatabase() {
  // better-sqlite3 persists on each statement; no explicit export needed.
  return;
}

function sqliteGetRow(sql, params = []) {
  if (!authDb) {
    return null;
  }
  const statement = authDb.prepare(sql);
  const row = statement.get(...params);
  return row || null;
}

function sqliteGetRows(sql, params = []) {
  if (!authDb) {
    return [];
  }
  const statement = authDb.prepare(sql);
  const rows = statement.all(...params);
  return Array.isArray(rows) ? rows : [];
}

function sqliteRun(sql, params = []) {
  if (!authDb) {
    throw new Error("Auth database is not initialized.");
  }
  const statement = authDb.prepare(sql);
  statement.run(...params);
}

async function initializeAuthDatabase() {
  ensureAuthStorage();
  authDb = new Database(AUTH_DB_FILE);
  authDb.pragma("journal_mode = WAL");
  authDb.pragma("foreign_keys = ON");
  authDb.exec(`
    CREATE TABLE IF NOT EXISTS users (
      username TEXT PRIMARY KEY,
      password_salt TEXT NOT NULL,
      password_hash TEXT NOT NULL,
      is_admin INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS user_state (
      username TEXT PRIMARY KEY,
      state_json TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY (username) REFERENCES users(username) ON DELETE CASCADE
    );
  `);
  const userColumns = sqliteGetRows("PRAGMA table_info(users)");
  const hasIsAdminColumn = userColumns.some(
    (column) => String(column?.name || "").toLowerCase() === "is_admin"
  );
  if (!hasIsAdminColumn) {
    sqliteRun("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0");
    const now = new Date().toISOString();
    AUTH_DEFAULT_ADMIN_USERS.forEach((username) => {
      sqliteRun(
        "UPDATE users SET is_admin = 1, updated_at = ? WHERE username = ?",
        [now, username]
      );
    });
  }
  authStatements = {
    countUsers: {
      get: () => sqliteGetRow("SELECT COUNT(*) AS count FROM users"),
    },
    countAdminUsers: {
      get: () => sqliteGetRow("SELECT COUNT(*) AS count FROM users WHERE is_admin = 1"),
    },
    getUserByUsername: {
      get: (username) =>
        sqliteGetRow(
          `SELECT
             username,
             password_salt AS passwordSalt,
             password_hash AS passwordHash,
             is_admin AS isAdmin,
             created_at AS createdAt,
             updated_at AS updatedAt
           FROM users
           WHERE username = ?`,
          [username]
        ),
    },
    upsertUser: {
      run: ({
        username,
        passwordSalt,
        passwordHash,
        isAdmin,
        createdAt,
        updatedAt,
      }) =>
        sqliteRun(
          `INSERT INTO users (
             username,
             password_salt,
             password_hash,
             is_admin,
             created_at,
             updated_at
           )
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(username) DO UPDATE SET
             password_salt = excluded.password_salt,
             password_hash = excluded.password_hash,
             is_admin = excluded.is_admin,
             updated_at = excluded.updated_at`,
          [username, passwordSalt, passwordHash, isAdmin, createdAt, updatedAt]
        ),
    },
    listUsers: {
      all: () =>
        sqliteGetRows(
          `SELECT
             username,
             is_admin AS isAdmin,
             created_at AS createdAt,
             updated_at AS updatedAt
           FROM users
           ORDER BY username COLLATE NOCASE`
        ),
    },
    setUserAdminByUsername: {
      run: ({ username, isAdmin, updatedAt }) =>
        sqliteRun(
          `UPDATE users
           SET is_admin = ?, updated_at = ?
           WHERE username = ?`,
          [isAdmin, updatedAt, username]
        ),
    },
    deleteUserByUsername: {
      run: (username) => {
        sqliteRun("DELETE FROM user_state WHERE username = ?", [username]);
        sqliteRun("DELETE FROM users WHERE username = ?", [username]);
      },
    },
    getStateByUsername: {
      get: (username) =>
        sqliteGetRow(
          `SELECT
             state_json AS stateJson,
             updated_at AS updatedAt
           FROM user_state
           WHERE username = ?`,
          [username]
        ),
    },
    upsertState: {
      run: ({ username, stateJson, updatedAt }) =>
        sqliteRun(
          `INSERT INTO user_state (username, state_json, updated_at)
           VALUES (?, ?, ?)
           ON CONFLICT(username) DO UPDATE SET
             state_json = excluded.state_json,
             updated_at = excluded.updated_at`,
          [username, stateJson, updatedAt]
        ),
    },
  };
  persistAuthDatabase();
}

function getAuthUserCount() {
  if (!authStatements) {
    return 0;
  }
  const row = authStatements.countUsers.get();
  const count = Number(row?.count);
  return Number.isFinite(count) ? count : 0;
}

function getAuthAdminCount() {
  if (!authStatements) {
    return 0;
  }
  const row = authStatements.countAdminUsers.get();
  const count = Number(row?.count);
  return Number.isFinite(count) ? count : 0;
}

function getAuthUser(usernameRaw) {
  if (!authStatements) {
    return null;
  }
  const username = normalizeAuthUsername(usernameRaw);
  if (!username) {
    return null;
  }
  return normalizeAuthUserRecord(authStatements.getUserByUsername.get(username));
}

function listAuthUsers() {
  if (!authStatements) {
    return [];
  }
  return authStatements.listUsers
    .all()
    .map((record) => normalizeAuthUserRecord(record))
    .filter(Boolean);
}

function upsertAuthUser(usernameRaw, password, options = {}) {
  const username = normalizeAuthUsername(usernameRaw);
  const passwordText = String(password || "");
  if (!username || !passwordText) {
    return false;
  }
  const now = new Date().toISOString();
  const existing = getAuthUser(username);
  const hasExplicitRole = Object.prototype.hasOwnProperty.call(options, "isAdmin");
  const nextIsAdmin = hasExplicitRole
    ? normalizeAuthAdminFlag(options.isAdmin)
    : existing
      ? normalizeAuthAdminFlag(existing.isAdmin)
      : AUTH_DEFAULT_ADMIN_USERS.has(username);
  const { salt, hash } = hashAuthPassword(passwordText);
  authStatements.upsertUser.run({
    username,
    passwordSalt: salt,
    passwordHash: hash,
    isAdmin: nextIsAdmin ? 1 : 0,
    createdAt: existing?.createdAt || now,
    updatedAt: now,
  });
  persistAuthDatabase();
  return true;
}

function setAuthUserRole(usernameRaw, isAdmin) {
  const username = normalizeAuthUsername(usernameRaw);
  if (!username || !authStatements) {
    return false;
  }
  const existing = getAuthUser(username);
  if (!existing) {
    return false;
  }
  authStatements.setUserAdminByUsername.run({
    username,
    isAdmin: normalizeAuthAdminFlag(isAdmin) ? 1 : 0,
    updatedAt: new Date().toISOString(),
  });
  persistAuthDatabase();
  return true;
}

function deleteAuthUser(usernameRaw) {
  const username = normalizeAuthUsername(usernameRaw);
  if (!username || !authStatements) {
    return false;
  }
  const existing = getAuthUser(username);
  if (!existing) {
    return false;
  }
  authStatements.deleteUserByUsername.run(username);
  persistAuthDatabase();
  return true;
}

function parseAuthSeedUsers(value) {
  const text = String(value || "").trim();
  if (!text) {
    return [];
  }
  return text
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => {
      const separator = item.indexOf(":");
      if (separator <= 0) {
        return null;
      }
      return {
        username: item.slice(0, separator).trim(),
        password: item.slice(separator + 1).trim(),
      };
    })
    .filter((entry) => entry && entry.username && entry.password);
}

function seedAuthUsersFromEnv() {
  const seedUsers = [...parseAuthSeedUsers(process.env.APP_AUTH_USERS)];
  if (process.env.APP_LOGIN_USER && process.env.APP_LOGIN_PASSWORD) {
    seedUsers.push({
      username: process.env.APP_LOGIN_USER,
      password: process.env.APP_LOGIN_PASSWORD,
    });
  }
  if (!seedUsers.length) {
    return;
  }
  let changed = false;
  seedUsers.forEach((entry) => {
    if (upsertAuthUser(entry.username, entry.password)) {
      changed = true;
    }
  });
  if (changed) {
    console.log(
      `[auth] Seeded ${seedUsers.length} login credential${
        seedUsers.length === 1 ? "" : "s"
      } into SQLite.`
    );
  }
}

function migrateLegacyUsersJsonToSqlite() {
  if (getAuthUserCount() > 0) {
    return;
  }
  if (!fs.existsSync(AUTH_USERS_FILE)) {
    return;
  }
  const parsed = readJsonFile(AUTH_USERS_FILE, { users: [] });
  const users = Array.isArray(parsed?.users) ? parsed.users : [];
  if (!users.length) {
    return;
  }
  let migrated = 0;
  users.forEach((entry) => {
    const username = normalizeAuthUsername(entry?.username);
    if (!username || !entry?.passwordSalt || !entry?.passwordHash) {
      return;
    }
    const createdAt = entry?.createdAt || new Date().toISOString();
    const updatedAt = entry?.updatedAt || createdAt;
    authStatements.upsertUser.run({
      username,
      passwordSalt: entry.passwordSalt,
      passwordHash: entry.passwordHash,
      isAdmin: AUTH_DEFAULT_ADMIN_USERS.has(username) ? 1 : 0,
      createdAt,
      updatedAt,
    });
    migrated += 1;
  });
  if (migrated > 0) {
    persistAuthDatabase();
  }
  if (migrated > 0) {
    console.log(
      `[auth] Migrated ${migrated} user credential${
        migrated === 1 ? "" : "s"
      } from legacy users.json to SQLite.`
    );
  }
}

function upsertAuthUserState(usernameRaw, state, updatedAtRaw) {
  const username = normalizeAuthUsername(usernameRaw);
  if (!username || !state || typeof state !== "object" || Array.isArray(state)) {
    return false;
  }
  const stateJson = JSON.stringify(state);
  const payloadBytes = Buffer.byteLength(stateJson);
  if (payloadBytes > AUTH_STATE_MAX_BYTES) {
    throw new Error(`State payload too large (${payloadBytes} bytes).`);
  }
  const updatedAt = updatedAtRaw || new Date().toISOString();
  authStatements.upsertState.run({
    username,
    stateJson,
    updatedAt,
  });
  persistAuthDatabase();
  return true;
}

function readAuthUserState(usernameRaw) {
  const username = normalizeAuthUsername(usernameRaw);
  if (!username) {
    return { state: null, updatedAt: null };
  }
  const row = authStatements.getStateByUsername.get(username);
  if (row?.stateJson) {
    try {
      const parsed = JSON.parse(row.stateJson);
      return {
        state:
          parsed && typeof parsed === "object" && !Array.isArray(parsed)
            ? parsed
            : null,
        updatedAt: row.updatedAt || null,
      };
    } catch (error) {
      return { state: null, updatedAt: row.updatedAt || null };
    }
  }
  const legacyPath = buildLegacyStatePath(username);
  if (!fs.existsSync(legacyPath)) {
    return { state: null, updatedAt: null };
  }
  const legacyPayload = readJsonFile(legacyPath, null);
  const legacyState =
    legacyPayload && typeof legacyPayload.state === "object"
      ? legacyPayload.state
      : null;
  if (!legacyState) {
    return { state: null, updatedAt: null };
  }
  const updatedAt = legacyPayload?.updatedAt || new Date().toISOString();
  try {
    upsertAuthUserState(username, legacyState, updatedAt);
    console.log(`[auth] Migrated legacy user-state JSON for ${username}.`);
  } catch (error) {
    // Keep serving the legacy value even if write-back fails.
  }
  return { state: legacyState, updatedAt };
}

function parseCookieHeader(cookieHeader) {
  const output = {};
  if (!cookieHeader) {
    return output;
  }
  String(cookieHeader)
    .split(";")
    .forEach((segment) => {
      const [rawKey, ...rest] = segment.trim().split("=");
      if (!rawKey) {
        return;
      }
      const value = rest.join("=") || "";
      try {
        output[rawKey] = decodeURIComponent(value);
      } catch (error) {
        output[rawKey] = value;
      }
    });
  return output;
}

function cleanupExpiredAuthSessions() {
  const now = Date.now();
  for (const [sessionId, session] of authSessions.entries()) {
    if (!session || !Number.isFinite(session.expiresAt) || session.expiresAt <= now) {
      authSessions.delete(sessionId);
    }
  }
}

function revokeAuthSessionsForUser(usernameRaw, options = {}) {
  const username = normalizeAuthUsername(usernameRaw);
  if (!username) {
    return;
  }
  const excludeSessionId = String(options.excludeSessionId || "");
  for (const [sessionId, session] of authSessions.entries()) {
    if (session?.username !== username) {
      continue;
    }
    if (excludeSessionId && sessionId === excludeSessionId) {
      continue;
    }
    authSessions.delete(sessionId);
  }
}

function getRequestSession(req) {
  cleanupExpiredAuthSessions();
  const cookies = parseCookieHeader(req.headers?.cookie || "");
  const sessionId = cookies[AUTH_COOKIE_NAME];
  if (!sessionId) {
    return null;
  }
  const session = authSessions.get(sessionId);
  if (!session) {
    return null;
  }
  if (session.expiresAt <= Date.now()) {
    authSessions.delete(sessionId);
    return null;
  }
  return {
    id: sessionId,
    username: session.username,
  };
}

function createAuthSession(username) {
  const sessionId = crypto.randomBytes(32).toString("hex");
  authSessions.set(sessionId, {
    username,
    expiresAt: Date.now() + AUTH_SESSION_TTL_MS,
  });
  return sessionId;
}

function authCookieOptions() {
  return {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    path: "/",
    maxAge: AUTH_SESSION_TTL_MS,
  };
}

function authCookieClearOptions() {
  return {
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    path: "/",
  };
}

function authSessionMiddleware(req, res, next) {
  const inheritedAuthUser =
    req.authUser && typeof req.authUser === "object"
      ? {
          username: normalizeAuthUsername(req.authUser.username),
          isAdmin: normalizeAuthAdminFlag(req.authUser.isAdmin),
        }
      : null;
  const inheritedSessionId = String(req.authSessionId || "");
  const session = getRequestSession(req);
  if (session) {
    req.authUser = {
      username: session.username,
      isAdmin: isAuthAdmin(session.username),
    };
    req.authSessionId = session.id;
    return next();
  }

  if (inheritedAuthUser?.username) {
    req.authUser = {
      username: inheritedAuthUser.username,
      isAdmin: isAuthAdmin(inheritedAuthUser.username),
    };
    req.authSessionId = inheritedSessionId || null;
    return next();
  }

  req.authUser = null;
  req.authSessionId = null;
  next();
}

function requireAuth(req, res, next) {
  if (!req.authUser) {
    res.status(401).json({ error: "Authentication required." });
    return;
  }
  const user = getAuthUser(req.authUser.username);
  if (!user) {
    if (req.authSessionId) {
      authSessions.delete(req.authSessionId);
    }
    res.status(401).json({ error: "Session is no longer valid." });
    return;
  }
  next();
}

function requireAdmin(req, res, next) {
  if (!req.authUser) {
    res.status(401).json({ error: "Authentication required." });
    return;
  }
  if (!isAuthAdmin(req.authUser.username)) {
    res.status(403).json({ error: "Admin access required." });
    return;
  }
  next();
}

async function initializeAuth() {
  if (!AUTH_DB_CONFIG_PROVIDED) {
    console.log(
      "[auth] DB config missing (set AUTH_DATA_DIR or AUTH_DB_FILE). Running guest-only mode."
    );
    return;
  }
  await initializeAuthDatabase();
  migrateLegacyUsersJsonToSqlite();
  seedAuthUsersFromEnv();
}

authInitPromise = initializeAuth();
setInterval(cleanupExpiredAuthSessions, 5 * 60 * 1000).unref();

function awaitAuthInitialization(req, res, next) {
  authInitPromise
    .then(() => {
      next();
    })
    .catch((error) => {
      res.status(500).json({
        error: error?.message || "Auth initialization failed.",
      });
    });
}

app.use(express.json({ limit: "5mb" }));
app.use(awaitAuthInitialization);
app.use(authSessionMiddleware);
app.use(express.static(path.join(__dirname, "public")));

app.get("/api/auth/me", (req, res) => {
  if (!AUTH_DB_CONFIG_PROVIDED) {
    res.json({
      authenticated: false,
      user: null,
      loginEnabled: false,
    });
    return;
  }
  const sessionUser = req.authUser ? getAuthUser(req.authUser.username) : null;
  if (!req.authUser) {
    res.json({
      authenticated: false,
      user: null,
      loginEnabled: getAuthUserCount() > 0,
    });
    return;
  }
  if (!sessionUser) {
    if (req.authSessionId) {
      authSessions.delete(req.authSessionId);
    }
    res.json({
      authenticated: false,
      user: null,
      loginEnabled: getAuthUserCount() > 0,
    });
    return;
  }
  res.json({
    authenticated: true,
    user: {
      username: sessionUser.username,
      isAdmin: normalizeAuthAdminFlag(sessionUser.isAdmin),
    },
    loginEnabled: true,
  });
});

app.post("/api/auth/login", (req, res) => {
  if (!AUTH_DB_CONFIG_PROVIDED) {
    res.status(503).json({
      error: "DB config missing. Set AUTH_DATA_DIR or AUTH_DB_FILE.",
    });
    return;
  }
  if (getAuthUserCount() <= 0) {
    res.status(503).json({
      error:
        "Login is not configured. Set APP_AUTH_USERS or APP_LOGIN_USER/APP_LOGIN_PASSWORD.",
    });
    return;
  }
  const username = normalizeAuthUsername(req.body?.username);
  const password = String(req.body?.password || "");
  if (!username || !password) {
    res.status(400).json({ error: "Username and password are required." });
    return;
  }
  const record = getAuthUser(username);
  if (!record || !verifyAuthPassword(password, record)) {
    res.status(401).json({ error: "Invalid username or password." });
    return;
  }
  const sessionId = createAuthSession(username);
  res.cookie(AUTH_COOKIE_NAME, sessionId, authCookieOptions());
  res.json({
    authenticated: true,
    user: {
      username: record.username,
      isAdmin: normalizeAuthAdminFlag(record.isAdmin),
    },
  });
});

app.post("/api/auth/logout", (req, res) => {
  if (req.authSessionId) {
    authSessions.delete(req.authSessionId);
  }
  res.clearCookie(AUTH_COOKIE_NAME, authCookieClearOptions());
  res.json({
    authenticated: false,
    user: null,
  });
});

app.get("/api/user/state", requireAuth, (req, res) => {
  const payload = readAuthUserState(req.authUser.username);
  res.json({
    state: payload.state,
    updatedAt: payload.updatedAt || null,
  });
});

app.put("/api/user/state", requireAuth, (req, res) => {
  const nextState = req.body?.state;
  if (
    !nextState ||
    typeof nextState !== "object" ||
    Array.isArray(nextState)
  ) {
    res.status(400).json({ error: "State payload must be an object." });
    return;
  }
  try {
    const updatedAt = new Date().toISOString();
    upsertAuthUserState(req.authUser.username, nextState, updatedAt);
    const payloadBytes = Buffer.byteLength(JSON.stringify(nextState));
    res.json({
      ok: true,
      updatedAt,
      bytes: payloadBytes,
    });
  } catch (error) {
    const message = error?.message || "Could not persist user state.";
    if (message.includes("too large")) {
      res.status(413).json({ error: message });
      return;
    }
    res.status(500).json({ error: message });
  }
});

app.get("/api/admin/users", requireAuth, requireAdmin, (req, res) => {
  const users = listAuthUsers().map((user) => ({
    username: user.username,
    isAdmin: normalizeAuthAdminFlag(user.isAdmin),
    createdAt: user.createdAt || null,
    updatedAt: user.updatedAt || null,
  }));
  res.json({
    users,
    requestedBy: req.authUser.username,
  });
});

app.post("/api/admin/users", requireAuth, requireAdmin, (req, res) => {
  const username = normalizeAuthUsername(req.body?.username);
  const password = String(req.body?.password || "");
  const hasRoleField =
    Object.prototype.hasOwnProperty.call(req.body || {}, "isAdmin") ||
    Object.prototype.hasOwnProperty.call(req.body || {}, "role");
  const parsedRole = hasRoleField
    ? parseAuthAdminInput(
        Object.prototype.hasOwnProperty.call(req.body || {}, "isAdmin")
          ? req.body?.isAdmin
          : req.body?.role
      )
    : false;
  if (!username || !password) {
    res.status(400).json({ error: "Username and password are required." });
    return;
  }
  if (hasRoleField && parsedRole === null) {
    res.status(400).json({ error: "Role must be admin or regular." });
    return;
  }
  if (getAuthUser(username)) {
    res.status(409).json({ error: "User already exists." });
    return;
  }
  const created = upsertAuthUser(username, password, {
    isAdmin: normalizeAuthAdminFlag(parsedRole),
  });
  if (!created) {
    res.status(400).json({ error: "Could not create user." });
    return;
  }
  const user = getAuthUser(username);
  res.status(201).json({
    ok: true,
    user: {
      username,
      isAdmin: normalizeAuthAdminFlag(user?.isAdmin),
      createdAt: user?.createdAt || null,
      updatedAt: user?.updatedAt || null,
    },
  });
});

app.patch(
  "/api/admin/users/:username/password",
  requireAuth,
  requireAdmin,
  (req, res) => {
    const username = normalizeAuthUsername(req.params?.username);
    const password = String(req.body?.password || "");
    if (!username || !password) {
      res.status(400).json({ error: "Username and password are required." });
      return;
    }
    if (!getAuthUser(username)) {
      res.status(404).json({ error: "User not found." });
      return;
    }
    const updated = upsertAuthUser(username, password);
    if (!updated) {
      res.status(400).json({ error: "Could not update password." });
      return;
    }
    revokeAuthSessionsForUser(username, { excludeSessionId: req.authSessionId });
    const user = getAuthUser(username);
    res.json({
      ok: true,
      user: {
        username,
        isAdmin: normalizeAuthAdminFlag(user?.isAdmin),
        createdAt: user?.createdAt || null,
        updatedAt: user?.updatedAt || null,
      },
    });
  }
);

app.patch(
  "/api/admin/users/:username/role",
  requireAuth,
  requireAdmin,
  (req, res) => {
    const username = normalizeAuthUsername(req.params?.username);
    const hasRoleField =
      Object.prototype.hasOwnProperty.call(req.body || {}, "isAdmin") ||
      Object.prototype.hasOwnProperty.call(req.body || {}, "role");
    if (!username || !hasRoleField) {
      res.status(400).json({ error: "Username and role are required." });
      return;
    }
    const nextIsAdmin = parseAuthAdminInput(
      Object.prototype.hasOwnProperty.call(req.body || {}, "isAdmin")
        ? req.body?.isAdmin
        : req.body?.role
    );
    if (nextIsAdmin === null) {
      res.status(400).json({ error: "Role must be admin or regular." });
      return;
    }
    const existing = getAuthUser(username);
    if (!existing) {
      res.status(404).json({ error: "User not found." });
      return;
    }
    if (
      normalizeAuthAdminFlag(existing.isAdmin) &&
      !nextIsAdmin &&
      getAuthAdminCount() <= 1
    ) {
      res.status(400).json({ error: "At least one admin user is required." });
      return;
    }
    if (normalizeAuthAdminFlag(existing.isAdmin) === nextIsAdmin) {
      res.json({
        ok: true,
        user: {
          username: existing.username,
          isAdmin: normalizeAuthAdminFlag(existing.isAdmin),
          createdAt: existing.createdAt || null,
          updatedAt: existing.updatedAt || null,
        },
      });
      return;
    }
    const updated = setAuthUserRole(username, nextIsAdmin);
    if (!updated) {
      res.status(400).json({ error: "Could not update role." });
      return;
    }
    const user = getAuthUser(username);
    res.json({
      ok: true,
      user: {
        username,
        isAdmin: normalizeAuthAdminFlag(user?.isAdmin),
        createdAt: user?.createdAt || null,
        updatedAt: user?.updatedAt || null,
      },
    });
  }
);

app.delete("/api/admin/users/:username", requireAuth, requireAdmin, (req, res) => {
  const username = normalizeAuthUsername(req.params?.username);
  if (!username) {
    res.status(400).json({ error: "Username is required." });
    return;
  }
  const existing = getAuthUser(username);
  if (!existing) {
    res.status(404).json({ error: "User not found." });
    return;
  }
  if (normalizeAuthAdminFlag(existing.isAdmin)) {
    res.status(400).json({ error: "Admin user cannot be removed." });
    return;
  }
  const removed = deleteAuthUser(username);
  if (!removed) {
    res.status(400).json({ error: "Could not remove user." });
    return;
  }
  revokeAuthSessionsForUser(username);
  res.json({
    ok: true,
    username,
  });
});

function parseQueryStringValues(value) {
  if (Array.isArray(value)) {
    return value
      .flatMap((entry) => parseQueryStringValues(entry))
      .filter(Boolean);
  }
  const text = String(value || "").trim();
  if (!text) {
    return [];
  }
  return text
    .split(",")
    .map((entry) => String(entry || "").trim())
    .filter(Boolean);
}

function roundTo(value, digits = 2) {
  if (!Number.isFinite(value)) {
    return null;
  }
  const factor = 10 ** Math.max(0, digits);
  return Math.round(value * factor) / factor;
}

function createVsaxCapacitySummary(groupName, rows = []) {
  const devices = Array.isArray(rows) ? rows : [];
  let totalVcpu = 0;
  let totalMemoryBytes = 0;
  let totalStorageBytes = 0;
  let vcpuSamples = 0;
  let memorySamples = 0;
  let storageSamples = 0;

  for (const device of devices) {
    const cpuTotal = Number(device?.cpu_total);
    const memoryTotal = Number(device?.memory_total);
    const storageTotal = Number(device?.disk_total_bytes);

    if (Number.isFinite(cpuTotal) && cpuTotal > 0) {
      totalVcpu += cpuTotal;
      vcpuSamples += 1;
    }
    if (Number.isFinite(memoryTotal) && memoryTotal > 0) {
      totalMemoryBytes += memoryTotal;
      memorySamples += 1;
    }
    if (Number.isFinite(storageTotal) && storageTotal > 0) {
      totalStorageBytes += storageTotal;
      storageSamples += 1;
    }
  }

  const deviceCount = devices.length;
  const avgVcpuPerDevice =
    vcpuSamples > 0 ? roundTo(totalVcpu / vcpuSamples, 2) : null;
  const avgMemoryBytesPerDevice =
    memorySamples > 0 ? totalMemoryBytes / memorySamples : null;
  const avgStorageBytesPerDevice =
    storageSamples > 0 ? totalStorageBytes / storageSamples : null;

  return {
    groupName: String(groupName || "").trim() || null,
    deviceCount,
    totalVcpu: roundTo(totalVcpu, 2),
    totalMemoryBytes: Math.round(totalMemoryBytes),
    totalStorageBytes: Math.round(totalStorageBytes),
    totalMemoryGb: roundTo(totalMemoryBytes / 1024 ** 3, 2),
    totalStorageTb: roundTo(totalStorageBytes / 1024 ** 4, 2),
    avgVcpuPerDevice,
    avgMemoryBytesPerDevice: Number.isFinite(avgMemoryBytesPerDevice)
      ? Math.round(avgMemoryBytesPerDevice)
      : null,
    avgStorageBytesPerDevice: Number.isFinite(avgStorageBytesPerDevice)
      ? Math.round(avgStorageBytesPerDevice)
      : null,
    avgMemoryGbPerDevice: Number.isFinite(avgMemoryBytesPerDevice)
      ? roundTo(avgMemoryBytesPerDevice / 1024 ** 3, 2)
      : null,
    avgStorageTbPerDevice: Number.isFinite(avgStorageBytesPerDevice)
      ? roundTo(avgStorageBytesPerDevice / 1024 ** 4, 2)
      : null,
    vcpuCoverageCount: vcpuSamples,
    memoryCoverageCount: memorySamples,
    storageCoverageCount: storageSamples,
  };
}

function tableExists(tableName) {
  if (!authDb) {
    return false;
  }
  const row = sqliteGetRow(
    "SELECT name FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
    [String(tableName || "").trim()]
  );
  return Boolean(row?.name);
}

function tableHasColumn(tableName, columnName) {
  if (!authDb) {
    return false;
  }
  const table = String(tableName || "").trim();
  const column = String(columnName || "").trim().toLowerCase();
  if (!table || !column) {
    return false;
  }
  const rows = sqliteGetRows(`PRAGMA table_info(${table})`);
  return rows.some(
    (row) => String(row?.name || "").trim().toLowerCase() === column
  );
}

app.get("/api/private/vsax/capacity", (req, res) => {
  if (!authDb) {
    res.status(503).json({ error: "Database is not initialized." });
    return;
  }
  if (!tableExists("vsax_devices")) {
    res.status(404).json({ error: "VSAx device inventory table is not available." });
    return;
  }

  const requestedGroupNames = Array.from(
    new Set(
      parseQueryStringValues(req.query?.groupName || req.query?.groupNames).map(
        (value) => value.trim()
      )
    )
  ).filter(Boolean);

  const discoveredGroups = tableExists("vsax_groups")
    ? sqliteGetRows(
        `
          SELECT DISTINCT group_name
          FROM vsax_groups
          WHERE is_active = 1
          ORDER BY group_name COLLATE NOCASE
        `
      )
        .map((row) => String(row?.group_name || "").trim())
        .filter(Boolean)
    : sqliteGetRows(
        `
          SELECT DISTINCT group_name
          FROM vsax_devices
          WHERE is_active = 1
          ORDER BY group_name COLLATE NOCASE
        `
      )
        .map((row) => String(row?.group_name || "").trim())
        .filter(Boolean);

  const selectedGroupNames = requestedGroupNames.length
    ? requestedGroupNames
    : discoveredGroups;

  if (!selectedGroupNames.length) {
    res.json({
      generatedAt: new Date().toISOString(),
      selectedGroupNames: [],
      selectedGroupName: null,
      groups: [],
      summary: null,
    });
    return;
  }

  const placeholders = selectedGroupNames.map(() => "?").join(",");
  const hasCpuTotalColumn = tableHasColumn("vsax_devices", "cpu_total");
  const cpuTotalSelect = hasCpuTotalColumn ? "cpu_total" : "NULL AS cpu_total";
  const deviceRows = sqliteGetRows(
    `
      SELECT group_name, device_id, ${cpuTotalSelect}, memory_total, disk_total_bytes
      FROM vsax_devices
      WHERE is_active = 1
        AND group_name IN (${placeholders})
    `,
    selectedGroupNames
  );

  const rowsByGroup = new Map(
    selectedGroupNames.map((groupName) => [groupName, []])
  );
  for (const row of deviceRows) {
    const groupName = String(row?.group_name || "").trim();
    if (!groupName) {
      continue;
    }
    if (!rowsByGroup.has(groupName)) {
      rowsByGroup.set(groupName, []);
    }
    rowsByGroup.get(groupName).push(row);
  }

  const groupSummaries = selectedGroupNames.map((groupName) =>
    createVsaxCapacitySummary(groupName, rowsByGroup.get(groupName) || [])
  );
  const aggregateSummary = createVsaxCapacitySummary(
    selectedGroupNames.length === 1 ? selectedGroupNames[0] : "all",
    deviceRows
  );

  res.json({
    generatedAt: new Date().toISOString(),
    selectedGroupNames,
    selectedGroupName:
      selectedGroupNames.length === 1 ? selectedGroupNames[0] : null,
    groups: groupSummaries,
    summary: aggregateSummary,
    availableGroups: discoveredGroups,
  });
});

function normalizeVsaxCompareRegionKey(value) {
  const regionKey = String(value || "").trim();
  if (regionKey && Object.prototype.hasOwnProperty.call(REGION_MAP, regionKey)) {
    return regionKey;
  }
  return "us-east";
}

function normalizeVsaxComparePricingProvider(value) {
  return value === "api" ? "api" : "retail";
}

function normalizeVsaxCompareDiskTier(value) {
  const diskTierKey = String(value || "").trim();
  if (diskTierKey && Object.prototype.hasOwnProperty.call(DISK_TIERS, diskTierKey)) {
    return diskTierKey;
  }
  return DEFAULT_DISK_TIER;
}

function selectSizeByCpuAndMemory(sizes, requestedCpu, requestedMemoryGb) {
  const sorted = sortSizes(Array.isArray(sizes) ? sizes : []);
  if (!sorted.length) {
    return { size: null, fit: "none" };
  }
  const cpuTarget = Math.max(1, Math.round(toNumber(requestedCpu, 1)));
  const memoryTarget = Math.max(0, toNumber(requestedMemoryGb, 0));

  const exactFit = sorted.find(
    (size) => size.vcpu >= cpuTarget && size.memory >= memoryTarget
  );
  if (exactFit) {
    return { size: exactFit, fit: "fit" };
  }

  const cpuFit = sorted.find((size) => size.vcpu >= cpuTarget);
  if (cpuFit) {
    return { size: cpuFit, fit: "memory-under" };
  }

  return {
    size: sorted[sorted.length - 1] || null,
    fit: "max-cap",
  };
}

function deriveVsaxDiskLayout(totalDiskBytes) {
  const rawDiskGb = Number(totalDiskBytes) / 1024 ** 3;
  const totalDiskGb =
    Number.isFinite(rawDiskGb) && rawDiskGb > 0 ? rawDiskGb : 256;
  let osDiskGb = Math.min(256, totalDiskGb);
  if (osDiskGb < 32) {
    osDiskGb = totalDiskGb;
  }
  const dataDiskGb = Math.max(0, totalDiskGb - osDiskGb);
  return {
    totalDiskGb,
    osDiskGb,
    dataDiskGb,
  };
}

async function resolveAwsVsaxOnDemandRate({
  instanceType,
  region,
  pricingProvider,
  logContext = {},
}) {
  if (!instanceType) {
    return {
      hourlyRate: null,
      source: "missing",
      status: "error",
      message: "AWS instance type is unavailable.",
    };
  }

  const os = "windows";
  const sqlEdition = "none";
  const useApi = pricingProvider === "api";

  if (useApi && hasAwsApiCredentials()) {
    try {
      const hourlyRate = await getAwsOnDemandPrice({
        instanceType,
        location: region.aws.location,
        os,
        sqlEdition,
        logContext: {
          ...logContext,
          silent: true,
        },
      });
      return {
        hourlyRate,
        source: "aws-pricing-api",
        status: "ok",
        message: null,
      };
    } catch (_apiError) {
      try {
        const hourlyRate = await getAwsPriceListOnDemandRate({
          instanceType,
          region: region.aws.region,
          location: region.aws.location,
          os,
          sqlEdition,
          logContext: {
            ...logContext,
            silent: true,
          },
        });
        return {
          hourlyRate,
          source: "aws-price-list",
          status: "fallback",
          message:
            "AWS Pricing API lookup failed for this shape; used AWS price list fallback.",
        };
      } catch (_priceListError) {
        // Fall back to public snapshot below.
      }
    }
  }

  try {
    const hourlyRate = await getAwsPublicPrice({
      instanceType,
      region: region.aws.region,
      os,
      sqlEdition,
    });
    return {
      hourlyRate,
      source: "public-snapshot",
      status: useApi ? "fallback" : "ok",
      message:
        useApi && !hasAwsApiCredentials()
          ? "AWS API credentials not found; using public snapshot."
          : null,
    };
  } catch (error) {
    return {
      hourlyRate: null,
      source: "missing",
      status: "error",
      message: error?.message || "AWS pricing lookup failed.",
    };
  }
}

async function resolveAzureVsaxOnDemandRate({
  instanceType,
  region,
  pricingProvider,
}) {
  if (!instanceType) {
    return {
      hourlyRate: null,
      source: "missing",
      status: "error",
      message: "Azure instance type is unavailable.",
    };
  }

  const os = "windows";
  const useApi = pricingProvider === "api";

  if (useApi) {
    try {
      const hourlyRate = await getAzureOnDemandPrice({
        skuName: instanceType,
        region: region.azure.region,
        os,
      });
      return {
        hourlyRate,
        source: "azure-retail-api",
        status: "ok",
        message: null,
      };
    } catch (_apiError) {
      // Fall back to public snapshot below.
    }
  }

  try {
    const hourlyRate = await getAzurePublicPrice({
      skuName: instanceType,
      region: region.azure.region,
      os,
    });
    return {
      hourlyRate,
      source: "public-snapshot",
      status: useApi ? "fallback" : "ok",
      message: useApi
        ? "Azure pricing API lookup failed for this shape; using public snapshot."
        : null,
    };
  } catch (error) {
    return {
      hourlyRate: null,
      source: "missing",
      status: "error",
      message: error?.message || "Azure pricing lookup failed.",
    };
  }
}

async function resolveVsaxRateWithCache(cache, cacheKey, resolver) {
  if (!(cache instanceof Map)) {
    return resolver();
  }
  if (cache.has(cacheKey)) {
    return cache.get(cacheKey);
  }
  const resultPromise = Promise.resolve().then(resolver);
  cache.set(cacheKey, resultPromise);
  try {
    const result = await resultPromise;
    cache.set(cacheKey, result);
    return result;
  } catch (error) {
    cache.delete(cacheKey);
    throw error;
  }
}

app.get("/api/vsax/price-compare", async (req, res) => {
  if (!authDb) {
    res.status(503).json({ error: "Database is not initialized." });
    return;
  }
  if (!tableExists("vsax_devices")) {
    res.status(404).json({ error: "VSAx device inventory table is not available." });
    return;
  }

  const requestedGroupName = String(req.query?.groupName || "").trim();
  const pricingProvider = normalizeVsaxComparePricingProvider(
    req.query?.pricingProvider
  );
  const regionKey = normalizeVsaxCompareRegionKey(req.query?.regionKey);
  const region = REGION_MAP[regionKey];
  const diskTierKey = normalizeVsaxCompareDiskTier(req.query?.diskTier);
  const diskTier = DISK_TIERS[diskTierKey] || DISK_TIERS[DEFAULT_DISK_TIER];

  const availableGroups = tableExists("vsax_groups")
    ? sqliteGetRows(
        `
          SELECT DISTINCT group_name
          FROM vsax_groups
          WHERE is_active = 1
          ORDER BY group_name COLLATE NOCASE
        `
      )
        .map((row) => String(row?.group_name || "").trim())
        .filter(Boolean)
    : sqliteGetRows(
        `
          SELECT DISTINCT group_name
          FROM vsax_devices
          WHERE is_active = 1
          ORDER BY group_name COLLATE NOCASE
        `
      )
        .map((row) => String(row?.group_name || "").trim())
        .filter(Boolean);

  if (!availableGroups.length) {
    res.json({
      generatedAt: new Date().toISOString(),
      groupName: null,
      availableGroups: [],
      regionKey,
      regionLabel: region.label,
      pricingProvider,
      diskTier: {
        key: diskTierKey,
        label: diskTier?.label || null,
      },
      summary: {
        systemCount: 0,
        awsPricedCount: 0,
        azurePricedCount: 0,
        awsMonthlyTotal: 0,
        azureMonthlyTotal: 0,
        awsHourlyTotal: 0,
        azureHourlyTotal: 0,
      },
      capacitySummary: null,
      systems: [],
    });
    return;
  }

  const selectedGroupName = availableGroups.includes(requestedGroupName)
    ? requestedGroupName
    : availableGroups[0];

  const hasCpuTotalColumn = tableHasColumn("vsax_devices", "cpu_total");
  const cpuTotalSelect = hasCpuTotalColumn ? "cpu_total" : "NULL AS cpu_total";

  const rows = sqliteGetRows(
    `
      SELECT
        group_name,
        device_id,
        device_name,
        ${cpuTotalSelect},
        memory_total,
        disk_total_bytes,
        last_sync_at
      FROM vsax_devices
      WHERE is_active = 1
        AND group_name = ?
      ORDER BY COALESCE(device_name, device_id) COLLATE NOCASE
    `,
    [selectedGroupName]
  );

  const awsSizes = collectProviderSizes(
    AWS_FAMILIES,
    VM_WORKLOADS.general.flavors.aws,
    {
      minCpu: 1,
      minMemory: 1,
      minNetworkGbps: MIN_NETWORK_GBPS,
      requireNetwork: true,
    }
  );
  const azureSizes = collectProviderSizes(
    AZURE_FAMILIES,
    VM_WORKLOADS.general.flavors.azure,
    {
      minCpu: 1,
      minMemory: 1,
      minNetworkGbps: MIN_NETWORK_GBPS,
      requireNetwork: true,
    }
  );

  if (!awsSizes.length || !azureSizes.length) {
    res.status(500).json({
      error: "Pricing size catalogs are unavailable for AWS or Azure.",
    });
    return;
  }

  const awsRateCache = new Map();
  const azureRateCache = new Map();
  const systems = [];
  let awsMonthlyTotal = 0;
  let azureMonthlyTotal = 0;
  let awsHourlyTotal = 0;
  let azureHourlyTotal = 0;
  let awsPricedCount = 0;
  let azurePricedCount = 0;

  for (const row of rows) {
    const systemName =
      String(row?.device_name || "").trim() ||
      String(row?.device_id || "").trim() ||
      "Unnamed system";
    const memoryBytes = Number(row?.memory_total);
    const memoryGb =
      Number.isFinite(memoryBytes) && memoryBytes > 0
        ? memoryBytes / 1024 ** 3
        : null;
    const diskLayout = deriveVsaxDiskLayout(row?.disk_total_bytes);
    const rawCpu = Number(row?.cpu_total);
    const notes = [];

    let requestedVcpu = null;
    if (Number.isFinite(rawCpu) && rawCpu > 0) {
      requestedVcpu = Math.max(1, Math.round(rawCpu));
    } else if (Number.isFinite(memoryGb) && memoryGb > 0) {
      requestedVcpu = Math.max(1, Math.ceil(memoryGb / 4));
      notes.push("vCPU missing in VSAx; estimated from RAM at ~4 GB/vCPU.");
    } else {
      requestedVcpu = MIN_CPU;
      notes.push("vCPU and RAM missing in VSAx; defaulted to 8 vCPU.");
    }

    const awsSelection = selectSizeByCpuAndMemory(
      awsSizes,
      requestedVcpu,
      memoryGb
    );
    const azureSelection = selectSizeByCpuAndMemory(
      azureSizes,
      requestedVcpu,
      memoryGb
    );

    if (awsSelection.fit === "memory-under") {
      notes.push("AWS sizing met vCPU but could not fully satisfy RAM.");
    }
    if (azureSelection.fit === "memory-under") {
      notes.push("Azure sizing met vCPU but could not fully satisfy RAM.");
    }
    if (awsSelection.fit === "max-cap") {
      notes.push("AWS sizing used the largest available shape.");
    }
    if (azureSelection.fit === "max-cap") {
      notes.push("Azure sizing used the largest available shape.");
    }

    const awsRate = awsSelection.size
      ? await resolveVsaxRateWithCache(
          awsRateCache,
          `${pricingProvider}|${region.aws.region}|${awsSelection.size.type}`,
          () =>
            resolveAwsVsaxOnDemandRate({
              instanceType: awsSelection.size.type,
              region,
              pricingProvider,
              logContext: {
                regionKey,
                groupName: selectedGroupName,
                systemName,
                silent: true,
              },
            })
        )
      : {
          hourlyRate: null,
          source: "missing",
          status: "error",
          message: "No AWS size could be selected.",
        };

    const azureRate = azureSelection.size
      ? await resolveVsaxRateWithCache(
          azureRateCache,
          `${pricingProvider}|${region.azure.region}|${azureSelection.size.type}`,
          () =>
            resolveAzureVsaxOnDemandRate({
              instanceType: azureSelection.size.type,
              region,
              pricingProvider,
            })
        )
      : {
          hourlyRate: null,
          source: "missing",
          status: "error",
          message: "No Azure size could be selected.",
        };

    const awsTotals =
      awsSelection.size && Number.isFinite(awsRate.hourlyRate)
        ? computeTotals({
            hourlyRate: awsRate.hourlyRate,
            osDiskGb: diskLayout.osDiskGb,
            dataDiskGb: diskLayout.dataDiskGb,
            snapshotGb: 0,
            egressGb: 0,
            hours: HOURS_IN_MONTH,
            storageRate: diskTier.storageRates.aws,
            dataStorageRate: diskTier.storageRates.aws,
            snapshotRate: diskTier.snapshotRates.aws,
            egressRate: EGRESS_RATES.aws,
            networkMonthly: 0,
            interVlanMonthly: 0,
            intraVlanMonthly: 0,
            interRegionMonthly: 0,
            iopsMonthly: 0,
            throughputMonthly: 0,
            sqlLicenseRate: 0,
            windowsLicenseMonthly: 0,
            vcpu: awsSelection.size.vcpu,
            drPercent: 0,
            vmCount: 1,
            controlPlaneMonthly: 0,
            egressScale: 1,
            osScale: 1,
            dataScale: 1,
          })
        : null;

    const azureTotals =
      azureSelection.size && Number.isFinite(azureRate.hourlyRate)
        ? computeTotals({
            hourlyRate: azureRate.hourlyRate,
            osDiskGb: diskLayout.osDiskGb,
            dataDiskGb: diskLayout.dataDiskGb,
            snapshotGb: 0,
            egressGb: 0,
            hours: HOURS_IN_MONTH,
            storageRate: diskTier.storageRates.azure,
            dataStorageRate: diskTier.storageRates.azure,
            snapshotRate: diskTier.snapshotRates.azure,
            egressRate: EGRESS_RATES.azure,
            networkMonthly: 0,
            interVlanMonthly: 0,
            intraVlanMonthly: 0,
            interRegionMonthly: 0,
            iopsMonthly: 0,
            throughputMonthly: 0,
            sqlLicenseRate: 0,
            windowsLicenseMonthly: 0,
            vcpu: azureSelection.size.vcpu,
            drPercent: 0,
            vmCount: 1,
            controlPlaneMonthly: 0,
            egressScale: 1,
            osScale: 1,
            dataScale: 1,
          })
        : null;

    const awsMonthly = Number(awsTotals?.total);
    const azureMonthly = Number(azureTotals?.total);
    const awsHourly = Number(awsRate?.hourlyRate);
    const azureHourly = Number(azureRate?.hourlyRate);

    if (Number.isFinite(awsMonthly)) {
      awsMonthlyTotal += awsMonthly;
      awsPricedCount += 1;
    }
    if (Number.isFinite(azureMonthly)) {
      azureMonthlyTotal += azureMonthly;
      azurePricedCount += 1;
    }
    if (Number.isFinite(awsHourly)) {
      awsHourlyTotal += awsHourly;
    }
    if (Number.isFinite(azureHourly)) {
      azureHourlyTotal += azureHourly;
    }

    systems.push({
      groupName: String(row?.group_name || "").trim() || selectedGroupName,
      deviceId: String(row?.device_id || "").trim() || null,
      systemName,
      lastSyncAt: row?.last_sync_at || null,
      metrics: {
        vcpu: requestedVcpu,
        memoryGb: Number.isFinite(memoryGb) ? roundTo(memoryGb, 2) : null,
        diskGb: roundTo(diskLayout.totalDiskGb, 2),
        osDiskGb: roundTo(diskLayout.osDiskGb, 2),
        dataDiskGb: roundTo(diskLayout.dataDiskGb, 2),
      },
      aws: {
        instanceType: awsSelection.size?.type || null,
        vcpu: Number.isFinite(awsSelection.size?.vcpu)
          ? awsSelection.size.vcpu
          : null,
        memoryGb: Number.isFinite(awsSelection.size?.memory)
          ? awsSelection.size.memory
          : null,
        hourlyRate: Number.isFinite(awsHourly) ? roundTo(awsHourly, 6) : null,
        monthlyTotal: Number.isFinite(awsMonthly) ? roundTo(awsMonthly, 2) : null,
        computeMonthly: Number.isFinite(awsTotals?.computeMonthly)
          ? roundTo(awsTotals.computeMonthly, 2)
          : null,
        storageMonthly: Number.isFinite(awsTotals?.storageMonthly)
          ? roundTo(awsTotals.storageMonthly, 2)
          : null,
        source: awsRate?.source || null,
        status: awsRate?.status || "error",
        message: awsRate?.message || null,
        fit: awsSelection.fit,
      },
      azure: {
        instanceType: azureSelection.size?.type || null,
        vcpu: Number.isFinite(azureSelection.size?.vcpu)
          ? azureSelection.size.vcpu
          : null,
        memoryGb: Number.isFinite(azureSelection.size?.memory)
          ? azureSelection.size.memory
          : null,
        hourlyRate: Number.isFinite(azureHourly) ? roundTo(azureHourly, 6) : null,
        monthlyTotal: Number.isFinite(azureMonthly)
          ? roundTo(azureMonthly, 2)
          : null,
        computeMonthly: Number.isFinite(azureTotals?.computeMonthly)
          ? roundTo(azureTotals.computeMonthly, 2)
          : null,
        storageMonthly: Number.isFinite(azureTotals?.storageMonthly)
          ? roundTo(azureTotals.storageMonthly, 2)
          : null,
        source: azureRate?.source || null,
        status: azureRate?.status || "error",
        message: azureRate?.message || null,
        fit: azureSelection.fit,
      },
      notes,
    });
  }

  const summary = {
    systemCount: systems.length,
    awsPricedCount,
    azurePricedCount,
    awsMonthlyTotal: roundTo(awsMonthlyTotal, 2),
    azureMonthlyTotal: roundTo(azureMonthlyTotal, 2),
    awsHourlyTotal: roundTo(awsHourlyTotal, 6),
    azureHourlyTotal: roundTo(azureHourlyTotal, 6),
    monthlyDelta: roundTo(azureMonthlyTotal - awsMonthlyTotal, 2),
  };

  res.json({
    generatedAt: new Date().toISOString(),
    groupName: selectedGroupName,
    availableGroups,
    regionKey,
    regionLabel: region.label,
    pricingProvider,
    diskTier: {
      key: diskTierKey,
      label: diskTier?.label || null,
    },
    capacitySummary: createVsaxCapacitySummary(selectedGroupName, rows),
    summary,
    systems,
  });
});

app.get("/api/options", async (req, res) => {
  try {
    const options = await buildSizeOptions();
    res.json(options);
  } catch (error) {
    res.status(500).json({
      error: error?.message || "Failed to build size options.",
    });
  }
});

function getMapLatestLoadedAt(map) {
  if (!(map instanceof Map) || !map.size) {
    return 0;
  }
  let latest = 0;
  for (const value of map.values()) {
    if (value && typeof value === "object" && Number.isFinite(value.loadedAt)) {
      latest = Math.max(latest, value.loadedAt);
    }
  }
  return latest;
}

function getSharedStorageCacheLatestLoadedAt() {
  if (!(k8sSharedStorageCache instanceof Map) || !k8sSharedStorageCache.size) {
    return 0;
  }
  let latest = 0;
  for (const value of k8sSharedStorageCache.values()) {
    if (value && Number.isFinite(value.expiresAt)) {
      const inferredLoadedAt = value.expiresAt - K8S_SHARED_STORAGE_CACHE_TTL_MS;
      latest = Math.max(latest, inferredLoadedAt);
    }
  }
  return latest;
}

function getSharedStorageCacheStaleEntries() {
  if (!(k8sSharedStorageCache instanceof Map) || !k8sSharedStorageCache.size) {
    return 0;
  }
  let stale = 0;
  const now = Date.now();
  for (const value of k8sSharedStorageCache.values()) {
    if (value && Number.isFinite(value.expiresAt) && value.expiresAt < now) {
      stale += 1;
    }
  }
  return stale;
}

function buildCacheStatus() {
  const now = Date.now();
  const definitions = [
    {
      key: "aws-public-snapshot",
      label: "AWS public snapshot",
      ttlMs: AWS_PUBLIC_CACHE_TTL_MS,
      size: awsPublicCache.data instanceof Map ? awsPublicCache.data.size : 0,
      loadedAt: Number.isFinite(awsPublicCache.loadedAt)
        ? awsPublicCache.loadedAt
        : 0,
    },
    {
      key: "azure-public-snapshot",
      label: "Azure public snapshot",
      ttlMs: AZURE_PUBLIC_CACHE_TTL_MS,
      size: azurePublicCache.data instanceof Map ? azurePublicCache.data.size : 0,
      loadedAt: Number.isFinite(azurePublicCache.loadedAt)
        ? azurePublicCache.loadedAt
        : 0,
    },
    {
      key: "gcp-public-snapshot",
      label: "GCP public snapshot",
      ttlMs: GCP_PUBLIC_CACHE_TTL_MS,
      size: Array.isArray(gcpPublicCache.data) ? gcpPublicCache.data.length : 0,
      loadedAt: Number.isFinite(gcpPublicCache.loadedAt)
        ? gcpPublicCache.loadedAt
        : 0,
    },
    {
      key: "gcp-billing-api",
      label: "GCP billing SKUs",
      ttlMs: GCP_BILLING_CACHE_TTL_MS,
      size: Array.isArray(gcpBillingCache.data) ? gcpBillingCache.data.length : 0,
      loadedAt: Number.isFinite(gcpBillingCache.loadedAt)
        ? gcpBillingCache.loadedAt
        : 0,
    },
    {
      key: "aws-price-list-index",
      label: "AWS price list index",
      ttlMs: AWS_PRICE_LIST_CACHE_TTL_MS,
      size:
        awsPriceListIndexCache.data &&
        typeof awsPriceListIndexCache.data === "object"
          ? Object.keys(awsPriceListIndexCache.data).length
          : 0,
      loadedAt: Number.isFinite(awsPriceListIndexCache.loadedAt)
        ? awsPriceListIndexCache.loadedAt
        : 0,
    },
    {
      key: "aws-price-list-regions",
      label: "AWS regional price lists",
      ttlMs: AWS_PRICE_LIST_CACHE_TTL_MS,
      size: awsPriceListRegionCache.size,
      loadedAt: getMapLatestLoadedAt(awsPriceListRegionCache),
    },
    {
      key: "aws-service-index",
      label: "AWS service indexes",
      ttlMs: AWS_PRICE_LIST_CACHE_TTL_MS,
      size: awsServiceIndexCache.size,
      loadedAt: getMapLatestLoadedAt(awsServiceIndexCache),
    },
    {
      key: "aws-service-regions",
      label: "AWS service region catalogs",
      ttlMs: AWS_PRICE_LIST_CACHE_TTL_MS,
      size: awsServiceRegionCache.size,
      loadedAt: getMapLatestLoadedAt(awsServiceRegionCache),
    },
    {
      key: "aws-efs-index",
      label: "AWS EFS index",
      ttlMs: K8S_SHARED_STORAGE_CACHE_TTL_MS,
      size:
        awsEfsRegionIndexCache.data &&
        typeof awsEfsRegionIndexCache.data === "object"
          ? Object.keys(awsEfsRegionIndexCache.data?.regions || {}).length
          : 0,
      loadedAt: Number.isFinite(awsEfsRegionIndexCache.loadedAt)
        ? awsEfsRegionIndexCache.loadedAt
        : 0,
    },
    {
      key: "shared-storage",
      label: "Shared storage rates",
      ttlMs: K8S_SHARED_STORAGE_CACHE_TTL_MS,
      size: k8sSharedStorageCache.size,
      loadedAt: getSharedStorageCacheLatestLoadedAt(),
      staleEntries: getSharedStorageCacheStaleEntries(),
    },
    {
      key: "aws-ec2-rates",
      label: "AWS EC2 rates",
      ttlMs: null,
      size: awsCache.size,
      loadedAt: 0,
    },
    {
      key: "azure-vm-rates",
      label: "Azure VM rates",
      ttlMs: null,
      size: azureCache.size,
      loadedAt: 0,
    },
    {
      key: "azure-reserved-rates",
      label: "Azure reserved rates",
      ttlMs: null,
      size: azureReservedCache.size,
      loadedAt: 0,
    },
    {
      key: "network-addon-rates",
      label: "Network add-on rates",
      ttlMs: null,
      size: azureNetworkCache.size,
      loadedAt: 0,
    },
    {
      key: "gcp-api-instance-rates",
      label: "GCP API instance rates",
      ttlMs: null,
      size: gcpApiCache.size,
      loadedAt: 0,
    },
  ];

  const caches = definitions.map((item) => {
    const loadedAt =
      Number.isFinite(item.loadedAt) && item.loadedAt > 0 ? item.loadedAt : null;
    const ageMs = loadedAt ? now - loadedAt : null;
    const stale =
      Number.isFinite(item.ttlMs) && item.ttlMs > 0 && loadedAt
        ? ageMs > item.ttlMs
        : false;
    return {
      key: item.key,
      label: item.label,
      ttlMs: item.ttlMs,
      loadedAt,
      ageMs,
      stale,
      size: item.size,
      staleEntries: item.staleEntries || 0,
    };
  });

  const staleCaches = caches
    .filter((cache) => cache.stale || cache.staleEntries > 0)
    .map((cache) => cache.key);
  return {
    generatedAt: new Date(now).toISOString(),
    refresh: {
      status: lastCacheRefreshStatus,
      running: cacheRefreshRunning,
      lastRefreshAt: lastCacheRefreshAt
        ? new Date(lastCacheRefreshAt).toISOString()
        : null,
      intervalMs: PRICING_CACHE_REFRESH_INTERVAL_MS,
      summary: lastCacheRefreshSummary,
      error: lastCacheRefreshError,
    },
    summary: {
      staleCount: staleCaches.length,
      staleCaches,
      loadedCount: caches.filter((cache) => cache.loadedAt).length,
      cacheGroups: caches.length,
    },
    caches,
  };
}

function buildCacheHealthWarning() {
  const status = buildCacheStatus();
  if (status.summary.staleCount <= 0) {
    return null;
  }
  return `Cache warning: stale cache groups detected (${status.summary.staleCaches.join(
    ", "
  )}). Results can fall back to defaults until refresh completes.`;
}

async function runCacheRefreshCycle(trigger = "scheduled") {
  if (cacheRefreshRunning) {
    return {
      accepted: false,
      trigger,
      message: "Cache refresh already running.",
    };
  }
  cacheRefreshRunning = true;
  lastCacheRefreshStatus = "running";
  lastCacheRefreshError = null;
  try {
    const summary = await warmPricingCaches();
    lastCacheRefreshAt = Date.now();
    lastCacheRefreshStatus = "ok";
    lastCacheRefreshSummary = summary;
    return {
      accepted: true,
      trigger,
      summary,
    };
  } catch (error) {
    lastCacheRefreshStatus = "error";
    lastCacheRefreshError = error?.message || String(error);
    throw error;
  } finally {
    cacheRefreshRunning = false;
  }
}

function startCacheRefreshLoop() {
  if (cacheRefreshTimer) {
    return;
  }
  cacheRefreshTimer = setInterval(() => {
    runCacheRefreshCycle("scheduled").catch((error) => {
      console.error("[pricing] Background cache refresh failed.", error);
    });
  }, PRICING_CACHE_REFRESH_INTERVAL_MS);
  if (typeof cacheRefreshTimer.unref === "function") {
    cacheRefreshTimer.unref();
  }
}

app.get("/api/cache/status", (req, res) => {
  res.json(buildCacheStatus());
});

app.post("/api/cache/refresh", async (req, res) => {
  try {
    const result = await runCacheRefreshCycle("manual");
    res.status(result.accepted ? 200 : 202).json(result);
  } catch (error) {
    res.status(500).json({
      accepted: false,
      trigger: "manual",
      error: error?.message || "Cache refresh failed.",
    });
  }
});

function isSizeEligible(size, options) {
  const { minCpu, minMemory, minNetworkGbps, requireNetwork } = options;
  if (!Number.isFinite(size.vcpu) || size.vcpu < minCpu) {
    return false;
  }
  if (!Number.isFinite(size.memory) || size.memory < minMemory) {
    return false;
  }
  if (size.localDisk === true) {
    return false;
  }
  const networkGbps = Number.isFinite(size.networkGbps)
    ? size.networkGbps
    : null;
  if (requireNetwork) {
    return Number.isFinite(networkGbps) && networkGbps >= minNetworkGbps;
  }
  if (Number.isFinite(networkGbps) && networkGbps < minNetworkGbps) {
    return false;
  }
  return true;
}

function filterSizes(sizes, options) {
  return sizes.filter((size) => isSizeEligible(size, options));
}

function buildProviderFlavorSizes(families, options) {
  const output = {};
  for (const [key, family] of Object.entries(families)) {
    output[key] = {
      label: family.label,
      sizes: filterSizes(family.sizes, options),
    };
  }
  return output;
}

function buildEmptyGcpFlavors() {
  const output = {};
  for (const [key, label] of Object.entries(GCP_FLAVOR_LABELS)) {
    output[key] = { label, sizes: [] };
  }
  return output;
}

function buildNetworkAddonOptions() {
  const providers = {};
  const defaults = {};
  for (const [providerKey, addonMap] of Object.entries(
    NETWORK_ADDON_OPTIONS
  )) {
    providers[providerKey] = {};
    defaults[providerKey] = NETWORK_ADDON_DEFAULTS[providerKey] || {};
    for (const [addonKey, options] of Object.entries(addonMap)) {
      providers[providerKey][addonKey] = options.map((option) => ({
        key: option.key,
        label: option.label,
      }));
    }
  }
  return { providers, defaults };
}

function buildGcpFlavorSizesFromList(list, options) {
  const output = buildEmptyGcpFlavors();
  const seen = new Set();
  list.forEach((item) => {
    const flavorKey = GCP_FAMILY_TO_FLAVOR[item.family];
    if (!flavorKey || !output[flavorKey]) {
      return;
    }
    if (item.local_ssd || item.shared_cpu) {
      return;
    }
    const size = {
      type: item.instance_type,
      vcpu: Number(item.vCPU),
      memory: Number(item.memory),
      networkGbps: null,
      networkLabel: "Variable",
      localDisk: false,
    };
    if (!isSizeEligible(size, options)) {
      return;
    }
    if (seen.has(size.type)) {
      return;
    }
    output[flavorKey].sizes.push(size);
    seen.add(size.type);
  });
  return output;
}

async function buildSizeOptions() {
  const workloads = {};
  for (const [key, config] of Object.entries(VM_WORKLOADS)) {
    workloads[key] = {
      label: config.label,
      flavors: config.flavors,
      defaults: config.defaults,
    };
  }

  const coreConstraints = {
    minCpu: MIN_CPU,
    minMemory: MIN_MEMORY,
    minNetworkGbps: MIN_NETWORK_GBPS,
    requireNetwork: true,
  };
  const gcpConstraints = {
    minCpu: MIN_CPU,
    minMemory: MIN_MEMORY,
    minNetworkGbps: MIN_NETWORK_GBPS,
    requireNetwork: false,
  };

  const awsFlavors = buildProviderFlavorSizes(AWS_FAMILIES, coreConstraints);
  const azureFlavors = buildProviderFlavorSizes(AZURE_FAMILIES, coreConstraints);
  let gcpFlavors = buildEmptyGcpFlavors();
  try {
    const list = await loadGcpPublicPricing();
    gcpFlavors = buildGcpFlavorSizesFromList(list, gcpConstraints);
  } catch (error) {
    logPricingWarning(
      "gcp",
      { source: "public-pricing" },
      "Failed to load GCP public pricing list."
    );
    gcpFlavors = buildEmptyGcpFlavors();
  }

  return {
    minCpu: MIN_CPU,
    minMemory: MIN_MEMORY,
    workloads,
    k8s: {
      flavors: K8S_FLAVORS,
      defaults: K8S_DEFAULT_FLAVORS,
    },
    providers: {
      aws: { flavors: awsFlavors },
      azure: { flavors: azureFlavors },
      gcp: { flavors: gcpFlavors },
    },
    networkAddons: buildNetworkAddonOptions(),
  };
}

function toNumber(value, fallback) {
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toBoolean(value) {
  return value === true || value === "true" || value === "on";
}

function normalizeNetworkAddonFocus(value) {
  if (value === "firewall") {
    return "gateway";
  }
  if (value === "loadBalancer" || value === "gateway") {
    return value;
  }
  if (value === "vpc") {
    return value;
  }
  return "all";
}


function logPricingWarning(provider, context, message) {
  if (context?.silent) {
    return;
  }
  console.warn(`[pricing:${provider}] ${message}`, context);
}

function logPricingError(provider, context, error) {
  if (context?.silent) {
    return;
  }
  const details = error?.stack || error?.message || String(error);
  console.error(`[pricing:${provider}] ${details}`, context);
}

function normalizeAwsPricingAccountKey(value, fallback = "") {
  return String(value || fallback)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-");
}

function loadAwsPricingAccounts() {
  return getAwsAccountConfigs({
    includeEnvFallback: true,
    includeProfileOnly: false
  }).map((account) => ({
    accountId: normalizeAwsPricingAccountKey(account.accountId),
    displayName: String(account.displayName || account.vendorName || account.accountId || "AWS").trim() || "AWS",
    vendorName: String(account.vendorName || "").trim() || null,
    accessKeyId: String(account.accessKeyId || "").trim(),
    secretAccessKey: String(account.secretAccessKey || "").trim(),
    sessionToken: String(account.sessionToken || "").trim() || null,
  }));
}

function resolveAwsPricingAccount(accounts) {
  if (!accounts.length) {
    return null;
  }

  const requested = String(process.env.AWS_PRICING_ACCOUNT_ID || "").trim();
  if (!requested) {
    return accounts[0];
  }

  const key = normalizeAwsPricingAccountKey(requested);
  const selected =
    accounts.find((account) => account.accountId === key) ||
    accounts.find(
      (account) => normalizeAwsPricingAccountKey(account.displayName) === key
    ) ||
    accounts.find(
      (account) => normalizeAwsPricingAccountKey(account.vendorName || "") === key
    );

  if (selected) {
    return selected;
  }

  console.warn(
    `[pricing:aws] AWS_PRICING_ACCOUNT_ID "${requested}" not found in configured AWS vendors. Using "${accounts[0].accountId}".`
  );
  return accounts[0];
}

function getAwsPricingCredentialAccount() {
  return resolveAwsPricingAccount(loadAwsPricingAccounts());
}

function getAwsPricingClient() {
  const awsPricingCredentialAccount = getAwsPricingCredentialAccount();
  const credentialKey = awsPricingCredentialAccount
    ? [
        awsPricingCredentialAccount.accountId,
        awsPricingCredentialAccount.accessKeyId,
        awsPricingCredentialAccount.sessionToken || "",
      ].join("|")
    : `ambient|${String(process.env.AWS_PROFILE || "").trim()}|${String(process.env.AWS_ACCESS_KEY_ID || "").trim()}`;

  if (awsPricingClient && awsPricingClientKey === credentialKey) {
    return awsPricingClient;
  }
  if (awsPricingClient && awsPricingClientKey !== credentialKey) {
    try {
      awsPricingClient.destroy();
    } catch (_error) {
      // Ignore client disposal errors.
    }
    awsPricingClient = null;
  }

  const config = {
    region: "us-east-1",
  };

  if (awsPricingCredentialAccount) {
    config.credentials = {
      accessKeyId: awsPricingCredentialAccount.accessKeyId,
      secretAccessKey: awsPricingCredentialAccount.secretAccessKey,
    };
    if (awsPricingCredentialAccount.sessionToken) {
      config.credentials.sessionToken = awsPricingCredentialAccount.sessionToken;
    }
  }

  awsPricingClient = new PricingClient(config);
  awsPricingClientKey = credentialKey;
  return awsPricingClient;
}

function hasAwsApiCredentials() {
  const awsPricingCredentialAccount = getAwsPricingCredentialAccount();
  return Boolean(
    awsPricingCredentialAccount ||
    process.env.AWS_PROFILE ||
      (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY)
  );
}

function getGcpApiKey() {
  return (
    process.env.GCP_PRICING_API_KEY ||
    process.env.GCP_API_KEY ||
    process.env.GOOGLE_API_KEY
  );
}

function hasGcpApiCredentials() {
  return Boolean(getGcpApiKey());
}

function resolvePricingProvider(body) {
  if (body.pricingProvider === "api" || body.pricingProvider === "retail") {
    return body.pricingProvider;
  }
  if (body.azurePricingSource === "retail") {
    return "api";
  }
  if (body.azurePricingSource === "vantage") {
    return "retail";
  }
  return "retail";
}

function normalizeSqlEdition(value) {
  if (value === "standard" || value === "enterprise") {
    return value;
  }
  return "none";
}

function sortSizes(sizes) {
  return sizes.slice().sort((a, b) => {
    if (a.vcpu === b.vcpu) {
      return a.memory - b.memory;
    }
    return a.vcpu - b.vcpu;
  });
}

function flattenFlavorSizes(flavors) {
  const sizes = [];
  const seen = new Set();
  Object.values(flavors || {}).forEach((flavor) => {
    (flavor.sizes || []).forEach((size) => {
      if (seen.has(size.type)) {
        return;
      }
      sizes.push(size);
      seen.add(size.type);
    });
  });
  return sizes;
}

async function runWithConcurrency(tasks, limit) {
  const executing = new Set();
  const results = [];
  for (const task of tasks) {
    const promise = Promise.resolve().then(task);
    results.push(promise);
    executing.add(promise);
    const cleanup = () => executing.delete(promise);
    promise.then(cleanup, cleanup);
    if (executing.size >= limit) {
      await Promise.race(executing);
    }
  }
  return Promise.allSettled(results);
}

async function warmPricingCaches() {
  const startedAt = Date.now();
  const summary = {
    aws: { ok: 0, fail: 0 },
    azure: { ok: 0, fail: 0, reservedOk: 0, reservedFail: 0 },
    gcp: { ok: 0, fail: 0 },
    public: { aws: false, azure: false, gcp: false },
    sharedStorage: { ok: 0, fail: 0 },
  };
  const warmContext = { warmup: true, silent: true };
  console.log("[pricing] Cache warm-up starting...");

  const publicTasks = [
    () =>
      loadAwsPublicPricing()
        .then(() => {
          summary.public.aws = true;
        })
        .catch(() => {
          summary.public.aws = false;
        }),
    () =>
      loadAzurePublicPricing()
        .then(() => {
          summary.public.azure = true;
        })
        .catch(() => {
          summary.public.azure = false;
        }),
    () =>
      loadGcpPublicPricing()
        .then(() => {
          summary.public.gcp = true;
        })
        .catch(() => {
          summary.public.gcp = false;
        }),
  ];
  await Promise.allSettled(publicTasks.map((task) => task()));

  if (hasGcpApiCredentials()) {
    try {
      await loadGcpBillingSkus(getGcpApiKey());
    } catch (error) {
      logPricingError("gcp", { warmup: true }, error);
    }
  }

  let options;
  try {
    options = await buildSizeOptions();
  } catch (error) {
    logPricingError("pricing", { warmup: true }, error);
    return;
  }

  const awsSizes = flattenFlavorSizes(options.providers.aws.flavors);
  const azureSizes = flattenFlavorSizes(options.providers.azure.flavors);
  const gcpSizes = flattenFlavorSizes(options.providers.gcp.flavors);
  const regions = Object.values(REGION_MAP);

  const tasks = [];

  if (hasAwsApiCredentials()) {
    regions.forEach((region) => {
      awsSizes.forEach((size) => {
        ["windows", "linux"].forEach((os) => {
          tasks.push(async () => {
            try {
              await getAwsOnDemandPrice({
                instanceType: size.type,
                location: region.aws.location,
                os,
                sqlEdition: "none",
                logContext: warmContext,
              });
              summary.aws.ok += 1;
            } catch (error) {
              summary.aws.fail += 1;
            }
          });
        });
      });
    });
  } else {
    console.log(
      "[pricing] Cache warm-up skipped for AWS API (missing credentials)."
    );
  }

  regions.forEach((region) => {
    azureSizes.forEach((size) => {
      ["windows", "linux"].forEach((os) => {
        tasks.push(async () => {
          try {
            await getAzureOnDemandPrice({
              skuName: size.type,
              region: region.azure.region,
              os,
            });
            summary.azure.ok += 1;
          } catch (error) {
            summary.azure.fail += 1;
          }
        });
        [1, 3].forEach((termYears) => {
          tasks.push(async () => {
            try {
              await getAzureReservedPrice({
                skuName: size.type,
                region: region.azure.region,
                os,
                termYears,
              });
              summary.azure.reservedOk += 1;
            } catch (error) {
              summary.azure.reservedFail += 1;
            }
          });
        });
      });
    });
  });

  if (hasGcpApiCredentials()) {
    regions.forEach((region) => {
      gcpSizes.forEach((size) => {
        ["windows", "linux"].forEach((os) => {
          tasks.push(async () => {
            try {
              await getGcpApiOnDemandPrice({
                instanceType: size.type,
                vcpu: size.vcpu,
                memory: size.memory,
                region: region.gcp.region,
                os,
                apiKey: getGcpApiKey(),
              });
              summary.gcp.ok += 1;
            } catch (error) {
              summary.gcp.fail += 1;
            }
          });
        });
      });
    });
  } else {
    console.log(
      "[pricing] Cache warm-up skipped for GCP API (missing credentials)."
    );
  }

  regions.forEach((region) => {
    tasks.push(async () => {
      try {
        await resolveK8sSharedStorageRates(region);
        summary.sharedStorage.ok += 1;
      } catch (error) {
        summary.sharedStorage.fail += 1;
      }
    });
  });

  await runWithConcurrency(tasks, PRICING_WARMUP_CONCURRENCY);

  const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log(
    `[pricing] Cache warm-up complete in ${elapsed}s.`,
    summary
  );
  return {
    ...summary,
    elapsedSeconds: Number.parseFloat(elapsed),
  };
}

function pickSizeByCpu(sizes, cpu) {
  if (!sizes.length) {
    return null;
  }
  const sorted = sortSizes(sizes);
  const exact = sorted.find((size) => size.vcpu === cpu);
  if (exact) {
    return exact;
  }
  const higher = sorted.find((size) => size.vcpu > cpu);
  if (higher) {
    return higher;
  }
  return sorted[sorted.length - 1] || null;
}

function selectSizeByTypeOrCpu(sizes, instanceType, cpu) {
  if (instanceType) {
    const match = sizes.find((size) => size.type === instanceType);
    if (match) {
      return { size: match, reason: "type" };
    }
  }
  const fallback = pickSizeByCpu(sizes, cpu);
  if (!fallback) {
    return { size: null, reason: "none" };
  }
  return {
    size: fallback,
    reason: instanceType ? "fallback" : "cpu",
  };
}

function collectProviderSizes(families, flavorKeys, options) {
  const sizes = [];
  const seen = new Set();
  (flavorKeys || []).forEach((flavorKey) => {
    const family = families[flavorKey];
    if (!family?.sizes) {
      return;
    }
    const filtered = filterSizes(family.sizes, options);
    filtered.forEach((size) => {
      if (seen.has(size.type)) {
        return;
      }
      sizes.push({ ...size, flavorKey });
      seen.add(size.type);
    });
  });
  return sizes;
}

function collectGcpSizesFromList(list, flavorKeys, options) {
  const familySet = new Set();
  (flavorKeys || []).forEach((flavorKey) => {
    const families = GCP_FLAVOR_MAP[flavorKey] || [];
    families.forEach((family) => familySet.add(family));
  });
  const sizes = [];
  const seen = new Set();
  list.forEach((item) => {
    if (!familySet.has(item.family)) {
      return;
    }
    if (item.local_ssd || item.shared_cpu) {
      return;
    }
    const size = {
      type: item.instance_type,
      vcpu: Number(item.vCPU),
      memory: Number(item.memory),
      networkGbps: null,
      networkLabel: "Variable",
      localDisk: false,
      flavorKey: GCP_FAMILY_TO_FLAVOR[item.family],
    };
    if (!isSizeEligible(size, options)) {
      return;
    }
    if (seen.has(size.type)) {
      return;
    }
    sizes.push(size);
    seen.add(size.type);
  });
  return sizes;
}

function isMissingAwsCredentials(error) {
  const message = (error?.message || "").toLowerCase();
  const name = (error?.name || "").toLowerCase();
  return (
    name.includes("credential") ||
    message.includes("credential") ||
    message.includes("could not load") ||
    message.includes("missing credentials")
  );
}

async function loadAwsPublicPricing() {
  if (
    awsPublicCache.data &&
    Date.now() - awsPublicCache.loadedAt < AWS_PUBLIC_CACHE_TTL_MS
  ) {
    return awsPublicCache.data;
  }

  const response = await fetcher(AWS_PUBLIC_PRICING_URL, {
    headers: {
      "User-Agent": "cloud-price/0.1",
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(
      `Public AWS pricing fetch failed: ${response.status}`
    );
  }
  const list = await response.json();
  const pricingMap = new Map();
  for (const item of list) {
    if (item?.instance_type && item?.pricing) {
      pricingMap.set(item.instance_type, item.pricing);
    }
  }
  awsPublicCache.data = pricingMap;
  awsPublicCache.loadedAt = Date.now();
  return pricingMap;
}

function resolveAwsPublicKey(os, sqlEdition) {
  if (sqlEdition === "standard") {
    return os === "windows" ? "mswinSQL" : "linuxSQL";
  }
  if (sqlEdition === "enterprise") {
    return os === "windows" ? "mswinSQLEnterprise" : "linuxSQLEnterprise";
  }
  return os === "windows" ? "mswin" : "linux";
}

async function getAwsPublicPrice({
  instanceType,
  region,
  os,
  sqlEdition,
}) {
  const pricingMap = await loadAwsPublicPricing();
  const instancePricing = pricingMap.get(instanceType);
  if (!instancePricing) {
    throw new Error("Public AWS pricing missing for instance type.");
  }
  const regionPricing = instancePricing[region];
  if (!regionPricing) {
    throw new Error("Public AWS pricing missing for region.");
  }
  const pricingKey = resolveAwsPublicKey(os, sqlEdition);
  const rateValue = regionPricing?.[pricingKey]?.ondemand;
  const rate = Number.parseFloat(rateValue || "0");
  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("Public AWS pricing missing for OS/SQL combo.");
  }
  return rate;
}

async function getAwsPublicReservedPrice({
  instanceType,
  region,
  os,
  sqlEdition,
  termYears,
  reservedType,
}) {
  const pricingMap = await loadAwsPublicPricing();
  const instancePricing = pricingMap.get(instanceType);
  if (!instancePricing) {
    throw new Error("Public AWS pricing missing for instance type.");
  }
  const regionPricing = instancePricing[region];
  if (!regionPricing) {
    throw new Error("Public AWS pricing missing for region.");
  }
  const pricingKey = resolveAwsPublicKey(os, sqlEdition);
  const reservedKey = AWS_RESERVED_KEYS[reservedType]?.[termYears];
  if (!reservedKey) {
    throw new Error("Unsupported reservation term.");
  }
  const rateValue = regionPricing?.[pricingKey]?.reserved?.[reservedKey];
  const rate = Number.parseFloat(rateValue || "0");
  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("Public AWS reserved pricing missing.");
  }
  return rate;
}

function mapAzureSkuToVantageType(skuName) {
  return skuName
    .toLowerCase()
    .replace(/^standard_/, "")
    .replace(/_/g, "");
}

async function loadAzurePublicPricing() {
  if (
    azurePublicCache.data &&
    Date.now() - azurePublicCache.loadedAt < AZURE_PUBLIC_CACHE_TTL_MS
  ) {
    return azurePublicCache.data;
  }

  const response = await fetcher(AZURE_PUBLIC_PRICING_URL, {
    headers: {
      "User-Agent": "cloud-price/0.1",
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(
      `Public Azure pricing fetch failed: ${response.status}`
    );
  }
  const list = await response.json();
  const pricingMap = new Map();
  for (const item of list) {
    if (item?.instance_type && item?.pricing) {
      pricingMap.set(item.instance_type, item.pricing);
    }
  }
  azurePublicCache.data = pricingMap;
  azurePublicCache.loadedAt = Date.now();
  return pricingMap;
}

async function getAzurePublicPrice({ skuName, region, os }) {
  const pricingMap = await loadAzurePublicPricing();
  const instanceKey = mapAzureSkuToVantageType(skuName);
  const instancePricing = pricingMap.get(instanceKey);
  if (!instancePricing) {
    throw new Error("Public Azure pricing missing for instance type.");
  }
  const vantageRegion = AZURE_VANTAGE_REGION_MAP[region];
  if (!vantageRegion) {
    throw new Error("Public Azure pricing missing for region.");
  }
  const regionPricing = instancePricing[vantageRegion];
  const osKey = os === "windows" ? "windows" : "linux";
  const rateValue = regionPricing?.[osKey]?.ondemand;
  const rate = Number.parseFloat(rateValue || "0");
  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("Public Azure pricing missing for OS.");
  }
  return rate;
}

async function getAzurePublicReservedPrice({ skuName, region, os, termYears }) {
  const pricingMap = await loadAzurePublicPricing();
  const instanceKey = mapAzureSkuToVantageType(skuName);
  const instancePricing = pricingMap.get(instanceKey);
  if (!instancePricing) {
    throw new Error("Public Azure pricing missing for instance type.");
  }
  const vantageRegion = AZURE_VANTAGE_REGION_MAP[region];
  if (!vantageRegion) {
    throw new Error("Public Azure pricing missing for region.");
  }
  const regionPricing = instancePricing[vantageRegion];
  const osKey = os === "windows" ? "windows" : "linux";
  const reservedKey = AZURE_PUBLIC_RESERVED_KEYS[termYears];
  if (!reservedKey) {
    throw new Error("Unsupported reservation term.");
  }
  const rateValue = regionPricing?.[osKey]?.reserved?.[reservedKey];
  const rate = Number.parseFloat(rateValue || "0");
  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("Public Azure reserved pricing missing.");
  }
  return rate;
}

async function loadGcpPublicPricing() {
  if (
    gcpPublicCache.data &&
    Date.now() - gcpPublicCache.loadedAt < GCP_PUBLIC_CACHE_TTL_MS
  ) {
    return gcpPublicCache.data;
  }

  const response = await fetcher(GCP_PUBLIC_PRICING_URL, {
    headers: {
      "User-Agent": "cloud-price/0.1",
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`Public GCP pricing fetch failed: ${response.status}`);
  }
  const list = await response.json();
  gcpPublicCache.data = list;
  gcpPublicCache.loadedAt = Date.now();
  return list;
}

async function getGcpOnDemandPrice({
  flavorKeys,
  instanceType,
  cpu,
  region,
  os,
  requirePricing = true,
}) {
  const list = await loadGcpPublicPricing();
  const candidates = list.filter((item) => {
    if (!flavorKeys?.length) {
      return false;
    }
    const flavorKey = GCP_FAMILY_TO_FLAVOR[item.family];
    if (!flavorKey || !flavorKeys.includes(flavorKey)) {
      return false;
    }
    if (item.local_ssd || item.shared_cpu) {
      return false;
    }
    const regionPricing = item.pricing?.[region];
    if (!regionPricing) {
      return false;
    }
    if (!requirePricing) {
      return true;
    }
    const osPricing = regionPricing?.[os];
    return Boolean(osPricing?.ondemand);
  });

  const gcpConstraints = {
    minCpu: MIN_CPU,
    minMemory: MIN_MEMORY,
    minNetworkGbps: MIN_NETWORK_GBPS,
    requireNetwork: false,
  };
  const sizeList = candidates
    .map((item) => ({
      type: item.instance_type,
      vcpu: Number(item.vCPU),
      memory: Number(item.memory),
      networkGbps: null,
      networkLabel: "Variable",
      localDisk: false,
      flavorKey: GCP_FAMILY_TO_FLAVOR[item.family],
    }))
    .filter((size) => isSizeEligible(size, gcpConstraints));

  const selection = selectSizeByTypeOrCpu(sizeList, instanceType, cpu);
  if (!selection.size) {
    throw new Error("No GCP instance meets the requirements.");
  }

  const matched = candidates.find(
    (item) => item.instance_type === selection.size.type
  );
  if (!requirePricing) {
    return { size: selection.size, rate: null, selection };
  }

  const rateValue = matched?.pricing?.[region]?.[os]?.ondemand;
  const rate = Number.parseFloat(rateValue || "0");
  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("Invalid GCP hourly rate.");
  }

  return { size: selection.size, rate, selection };
}

function unitPriceToNumber(price) {
  if (!price) {
    return 0;
  }
  const units = Number.parseFloat(price.units || "0");
  const nanos = Number.parseFloat(price.nanos || "0");
  if (!Number.isFinite(units) && !Number.isFinite(nanos)) {
    return 0;
  }
  return (Number.isFinite(units) ? units : 0) +
    (Number.isFinite(nanos) ? nanos : 0) / 1e9;
}

async function loadGcpBillingSkus(apiKey) {
  if (
    gcpBillingCache.data &&
    Date.now() - gcpBillingCache.loadedAt < GCP_BILLING_CACHE_TTL_MS
  ) {
    return gcpBillingCache.data;
  }

  let url =
    `https://cloudbilling.googleapis.com/v1/services/${GCP_BILLING_SERVICE_ID}/skus?key=` +
    encodeURIComponent(apiKey);
  const skus = [];

  while (url) {
    const response = await fetcher(url);
    if (!response.ok) {
      throw new Error(`GCP Billing API error: ${response.status}`);
    }
    const data = await response.json();
    if (Array.isArray(data.skus)) {
      skus.push(...data.skus);
    }
    if (data.nextPageToken) {
      url =
        `https://cloudbilling.googleapis.com/v1/services/${GCP_BILLING_SERVICE_ID}/skus?key=` +
        encodeURIComponent(apiKey) +
        `&pageToken=${encodeURIComponent(data.nextPageToken)}`;
    } else {
      url = null;
    }
  }

  gcpBillingCache.data = skus;
  gcpBillingCache.loadedAt = Date.now();
  return skus;
}

function findGcpSkuRate({
  skus,
  familyToken,
  region,
  os,
  kind,
}) {
  const token = familyToken.toUpperCase();
  const isWindows = os === "windows";
  const pattern =
    kind === "cpu"
      ? new RegExp(`${token}.*instance core`, "i")
      : new RegExp(`${token}.*instance ram`, "i");
  const candidate = skus.find((sku) => {
    if (sku.category?.resourceFamily !== "Compute") {
      return false;
    }
    if (sku.category?.usageType !== "OnDemand") {
      return false;
    }
    const regions = sku.serviceRegions || [];
    if (!regions.includes(region) && !regions.includes("global")) {
      return false;
    }
    const description = sku.description || "";
    if (!pattern.test(description)) {
      return false;
    }
    const hasWindows = /windows/i.test(description);
    if (isWindows && !hasWindows) {
      return false;
    }
    if (!isWindows && hasWindows) {
      return false;
    }
    return true;
  });

  const price =
    candidate?.pricingInfo?.[0]?.pricingExpression?.tieredRates?.[0]
      ?.unitPrice;
  const rate = unitPriceToNumber(price);
  return Number.isFinite(rate) && rate > 0 ? rate : null;
}

async function getGcpApiOnDemandPrice({
  instanceType,
  vcpu,
  memory,
  region,
  os,
  apiKey,
}) {
  const cacheKey = [instanceType, region, os].join("|");
  if (gcpApiCache.has(cacheKey)) {
    return gcpApiCache.get(cacheKey);
  }
  const familyToken = instanceType.split("-")[0] || "";
  const skus = await loadGcpBillingSkus(apiKey);
  const cpuRate = findGcpSkuRate({
    skus,
    familyToken,
    region,
    os,
    kind: "cpu",
  });
  const ramRate = findGcpSkuRate({
    skus,
    familyToken,
    region,
    os,
    kind: "ram",
  });
  if (!Number.isFinite(cpuRate) || !Number.isFinite(ramRate)) {
    throw new Error("GCP API pricing missing for instance family.");
  }
  const rate = cpuRate * vcpu + ramRate * memory;
  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("Invalid GCP API hourly rate.");
  }
  const result = { rate, source: "gcp-cloud-billing" };
  gcpApiCache.set(cacheKey, result);
  return result;
}

function readSharedStorageCache(key) {
  const cached = k8sSharedStorageCache.get(key);
  if (!cached) {
    return null;
  }
  if (Date.now() > cached.expiresAt) {
    k8sSharedStorageCache.delete(key);
    return null;
  }
  return cached;
}

function writeSharedStorageCache(key, rate, source) {
  k8sSharedStorageCache.set(key, {
    rate,
    source,
    expiresAt: Date.now() + K8S_SHARED_STORAGE_CACHE_TTL_MS,
  });
}

async function loadAwsEfsRegionIndex() {
  if (
    awsEfsRegionIndexCache.data &&
    Date.now() - awsEfsRegionIndexCache.loadedAt <
      K8S_SHARED_STORAGE_CACHE_TTL_MS
  ) {
    return awsEfsRegionIndexCache.data;
  }

  const response = await fetcher(AWS_EFS_REGION_INDEX_URL, {
    headers: {
      "User-Agent": "cloud-price/0.1",
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(
      `AWS EFS price index fetch failed: ${response.status}`
    );
  }
  const data = await response.json();
  awsEfsRegionIndexCache.data = data;
  awsEfsRegionIndexCache.loadedAt = Date.now();
  return data;
}

async function getAwsEfsStandardRate(regionCode) {
  const cacheKey = `aws:${regionCode}`;
  const cached = readSharedStorageCache(cacheKey);
  if (cached) {
    return cached;
  }

  const index = await loadAwsEfsRegionIndex();
  const regionEntry = index?.regions?.[regionCode];
  if (!regionEntry?.currentVersionUrl) {
    throw new Error("AWS EFS pricing missing for region.");
  }

  const url = `https://pricing.us-east-1.amazonaws.com${regionEntry.currentVersionUrl}`;
  const response = await fetcher(url, {
    headers: {
      "User-Agent": "cloud-price/0.1",
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`AWS EFS pricing fetch failed: ${response.status}`);
  }
  const data = await response.json();
  let rate = null;

  for (const [sku, product] of Object.entries(data.products || {})) {
    const attrs = product.attributes || {};
    if (product.productFamily !== "Storage") {
      continue;
    }
    if (attrs.storageClass !== "General Purpose") {
      continue;
    }
    if (attrs.regionCode && attrs.regionCode !== regionCode) {
      continue;
    }
    const terms = data.terms?.OnDemand?.[sku];
    if (!terms) {
      continue;
    }
    for (const term of Object.values(terms)) {
      for (const dimension of Object.values(term.priceDimensions || {})) {
        if (dimension.unit !== "GB-Mo") {
          continue;
        }
        if (
          dimension.description &&
          !/standard storage/i.test(dimension.description)
        ) {
          continue;
        }
        const candidateRate = Number.parseFloat(
          dimension.pricePerUnit?.USD || "0"
        );
        if (Number.isFinite(candidateRate) && candidateRate > 0) {
          rate = candidateRate;
          break;
        }
      }
      if (rate) {
        break;
      }
    }
    if (rate) {
      break;
    }
  }

  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("Invalid AWS EFS storage rate.");
  }

  const result = { rate, source: "aws-efs-price-list" };
  writeSharedStorageCache(cacheKey, result.rate, result.source);
  return result;
}

async function getAzurePremiumFilesRate(region) {
  const cacheKey = `azure:${region}`;
  const cached = readSharedStorageCache(cacheKey);
  if (cached) {
    return cached;
  }

  const query = [
    "serviceName eq 'Storage'",
    `armRegionName eq '${region}'`,
    "productName eq 'Premium Files'",
    "contains(meterName, 'Provisioned')",
  ].join(" and ");
  const url =
    "https://prices.azure.com/api/retail/prices?$filter=" +
    encodeURIComponent(query);
  const response = await fetcher(url);
  if (!response.ok) {
    throw new Error(`Azure pricing API error: ${response.status}`);
  }
  const data = await response.json();
  const items = data.Items || [];
  const preferred = items.find(
    (item) =>
      /premium lrs/i.test(item.skuName || "") &&
      /provisioned/i.test(item.meterName || "")
  );
  const candidate =
    preferred ||
    items.find((item) => /provisioned/i.test(item.meterName || ""));
  const rate = Number.parseFloat(candidate?.retailPrice || "0");
  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("Invalid Azure premium files rate.");
  }

  const result = { rate, source: "azure-retail-premium-files" };
  writeSharedStorageCache(cacheKey, result.rate, result.source);
  return result;
}

async function getGcpFilestoreEnterpriseRate() {
  const cacheKey = "gcp:filestore";
  const cached = readSharedStorageCache(cacheKey);
  if (cached) {
    return cached;
  }

  const response = await fetcher(GCP_FILESTORE_PRICING_URL, {
    headers: {
      "User-Agent": "cloud-price/0.1",
      Accept: "text/html",
    },
  });
  if (!response.ok) {
    throw new Error(
      `GCP Filestore pricing fetch failed: ${response.status}`
    );
  }
  const text = await response.text();
  const enterpriseMatch = text.match(
    />Enterprise<\/p><\/td>[\s\S]*?\$([0-9.]+)\s*\/\s*1\s*gibibyte hour/i
  );
  const highScaleMatch = text.match(
    />High-Scale<\/p><\/td>[\s\S]*?\$([0-9.]+)\s*\/\s*1\s*gibibyte hour/i
  );
  const ratePerGiBHour = Number.parseFloat(
    enterpriseMatch?.[1] || highScaleMatch?.[1] || "0"
  );
  if (!Number.isFinite(ratePerGiBHour) || ratePerGiBHour <= 0) {
    throw new Error("Invalid GCP Filestore rate.");
  }
  const rate = ratePerGiBHour * HOURS_IN_MONTH;
  const result = { rate, source: "gcp-filestore-pricing-page" };
  writeSharedStorageCache(cacheKey, result.rate, result.source);
  return result;
}

async function resolveK8sSharedStorageRates(region) {
  const rates = { ...K8S_SHARED_STORAGE_DEFAULT_RATES };
  const sources = {
    aws: "fallback-default",
    azure: "fallback-default",
    gcp: "fallback-default",
  };

  const tasks = [
    ["aws", () => getAwsEfsStandardRate(region.aws.region)],
    ["azure", () => getAzurePremiumFilesRate(region.azure.region)],
    ["gcp", () => getGcpFilestoreEnterpriseRate()],
  ];

  const results = await Promise.allSettled(tasks.map(([, task]) => task()));
  results.forEach((result, index) => {
    const key = tasks[index][0];
    if (result.status === "fulfilled") {
      const value = result.value;
      if (Number.isFinite(value?.rate)) {
        rates[key] = value.rate;
        sources[key] = value.source;
      }
    }
  });

  return { rates, sources };
}

async function resolveStorageFocusRates(region) {
  const rates = {
    aws: {
      ...STORAGE_CATEGORY_DEFAULT_RATES.aws,
      replication: STORAGE_REPLICATION_DEFAULT_RATES.aws,
    },
    azure: {
      ...STORAGE_CATEGORY_DEFAULT_RATES.azure,
      replication: STORAGE_REPLICATION_DEFAULT_RATES.azure,
    },
    gcp: {
      ...STORAGE_CATEGORY_DEFAULT_RATES.gcp,
      replication: STORAGE_REPLICATION_DEFAULT_RATES.gcp,
    },
  };
  const sources = {
    aws: {
      object: "fallback-default",
      file: "fallback-default",
      table: "fallback-default",
      queue: "fallback-default",
      replication: "fallback-default",
    },
    azure: {
      object: "fallback-default",
      file: "fallback-default",
      table: "fallback-default",
      queue: "fallback-default",
      replication: "fallback-default",
    },
    gcp: {
      object: "fallback-default",
      file: "fallback-default",
      table: "fallback-default",
      queue: "fallback-default",
      replication: "fallback-default",
    },
  };

  const tasks = [
    {
      provider: "aws",
      key: "object",
      run: () =>
        getAwsServiceGbMonthRate({
          serviceCode: "AmazonS3",
          regionCode: region.aws.region,
          location: region.aws.location,
          matchers: {
            usagetypeIncludes: /TimedStorage-ByteHrs/i,
            operationIncludes: /StandardStorage/i,
          },
        }),
      source: "aws-price-list",
    },
    {
      provider: "aws",
      key: "file",
      run: () => getAwsEfsStandardRate(region.aws.region).then((v) => v.rate),
      source: "aws-efs-price-list",
    },
    {
      provider: "aws",
      key: "table",
      run: () =>
        getAwsServiceGbMonthRate({
          serviceCode: "AmazonDynamoDB",
          regionCode: region.aws.region,
          location: region.aws.location,
          matchers: {
            usagetypeIncludes: /TimedStorage-ByteHrs/i,
          },
        }),
      source: "aws-price-list",
    },
    {
      provider: "azure",
      key: "object",
      run: () =>
        getAzureRetailGbMonthRate({
          serviceName: "Storage",
          region: region.azure.region,
          productNameIncludes: /blob/i,
          meterNameIncludes: /data stored/i,
        }),
      source: "azure-retail-api",
    },
    {
      provider: "azure",
      key: "file",
      run: () => getAzurePremiumFilesRate(region.azure.region).then((v) => v.rate),
      source: "azure-retail-premium-files",
    },
    {
      provider: "azure",
      key: "table",
      run: () =>
        getAzureRetailGbMonthRate({
          serviceName: "Storage",
          region: region.azure.region,
          productNameIncludes: /table/i,
          meterNameIncludes: /data stored/i,
        }),
      source: "azure-retail-api",
    },
    {
      provider: "azure",
      key: "queue",
      run: () =>
        getAzureRetailGbMonthRate({
          serviceName: "Storage",
          region: region.azure.region,
          productNameIncludes: /queue/i,
          meterNameIncludes: /data stored/i,
        }),
      source: "azure-retail-api",
    },
    {
      provider: "gcp",
      key: "object",
      run: () =>
        getGcpServiceGbMonthRate({
          serviceName: "Cloud Storage",
          region: region.gcp.region,
          descriptionPatterns: [/standard/i, /storage/i],
        }),
      source: "gcp-cloud-billing",
    },
    {
      provider: "gcp",
      key: "file",
      run: () => getGcpFilestoreEnterpriseRate().then((v) => v.rate),
      source: "gcp-filestore-pricing-page",
    },
    {
      provider: "gcp",
      key: "table",
      run: () =>
        getGcpServiceGbMonthRate({
          serviceName: "Cloud Bigtable",
          region: region.gcp.region,
          descriptionPatterns: [/storage/i],
        }),
      source: "gcp-cloud-billing",
    },
    {
      provider: "gcp",
      key: "queue",
      run: () =>
        getGcpServiceGbMonthRate({
          serviceName: "Cloud Pub/Sub",
          region: region.gcp.region,
          descriptionPatterns: [/storage/i],
        }),
      source: "gcp-cloud-billing",
    },
  ];
  const results = await Promise.allSettled(tasks.map((task) => task.run()));
  results.forEach((result, index) => {
    const task = tasks[index];
    if (result.status !== "fulfilled") {
      return;
    }
    const value = Number.parseFloat(result.value);
    if (!Number.isFinite(value) || value <= 0) {
      return;
    }
    rates[task.provider][task.key] = value;
    sources[task.provider][task.key] = task.source;
  });

  return { rates, sources };
}

function computeStorageFocusTotals(profile, providerRates) {
  const accountCount = Math.max(
    1,
    Math.round(toNumber(profile?.accountCount, 1))
  );
  const objectGb = Math.max(0, toNumber(profile?.objectTb, 0)) * 1024;
  const fileGb = Math.max(0, toNumber(profile?.fileTb, 0)) * 1024;
  const tableGb = Math.max(0, toNumber(profile?.tableTb, 0)) * 1024;
  const queueGb = Math.max(0, toNumber(profile?.queueTb, 0)) * 1024;
  const objectMonthly = objectGb * providerRates.object * accountCount;
  const fileMonthly = fileGb * providerRates.file * accountCount;
  const tableMonthly = tableGb * providerRates.table * accountCount;
  const queueMonthly = queueGb * providerRates.queue * accountCount;
  const storageMonthly =
    objectMonthly + fileMonthly + tableMonthly + queueMonthly;
  const replicationMonthly =
    profile?.drEnabled
      ? Math.max(0, toNumber(profile?.drDeltaTb, 0)) *
        1024 *
        providerRates.replication *
        accountCount
      : 0;
  const total = storageMonthly + replicationMonthly;
  return {
    totals: {
      computeMonthly: 0,
      controlPlaneMonthly: 0,
      storageMonthly,
      backupMonthly: 0,
      egressMonthly: replicationMonthly,
      sqlMonthly: 0,
      windowsLicenseMonthly: 0,
      networkMonthly: 0,
      interVlanMonthly: 0,
      intraVlanMonthly: 0,
      interRegionMonthly: 0,
      iopsMonthly: 0,
      throughputMonthly: 0,
      drMonthly: 0,
      total,
    },
    breakdown: {
      accountCount,
      drEnabled: Boolean(profile?.drEnabled),
      drDeltaTb: Math.max(0, toNumber(profile?.drDeltaTb, 0)),
      objectTb: Math.max(0, toNumber(profile?.objectTb, 0)),
      fileTb: Math.max(0, toNumber(profile?.fileTb, 0)),
      tableTb: Math.max(0, toNumber(profile?.tableTb, 0)),
      queueTb: Math.max(0, toNumber(profile?.queueTb, 0)),
      objectMonthly,
      fileMonthly,
      tableMonthly,
      queueMonthly,
      replicationMonthly,
    },
  };
}

async function getAzureReservedPrice({ skuName, region, os, termYears }) {
  const cacheKey = [skuName, region, os, termYears].join("|");
  if (azureReservedCache.has(cacheKey)) {
    return azureReservedCache.get(cacheKey);
  }

  const reservationTerm = termYears === 3 ? "3 Years" : "1 Year";
  const query = [
    `armRegionName eq '${region}'`,
    "serviceName eq 'Virtual Machines'",
    `armSkuName eq '${skuName}'`,
    "type eq 'Reservation'",
    `reservationTerm eq '${reservationTerm}'`,
  ].join(" and ");

  const url =
    "https://prices.azure.com/api/retail/prices?$filter=" +
    encodeURIComponent(query);
  const response = await fetcher(url);
  if (!response.ok) {
    throw new Error(`Azure pricing API error: ${response.status}`);
  }
  const data = await response.json();
  const items = data.Items || [];
  if (!items.length) {
    throw new Error("Azure reservation pricing not found.");
  }

  const isWindows = os === "windows";
  const windowsMatch = isWindows
    ? items.find((item) => /windows/i.test(item.productName || ""))
    : null;
  const candidate = windowsMatch || items[0];

  const rawRate = Number.parseFloat(candidate?.retailPrice || "0");
  if (!Number.isFinite(rawRate) || rawRate <= 0) {
    throw new Error("Invalid Azure reservation rate.");
  }

  const termHours = AZURE_RESERVATION_TERM_HOURS[termYears] || 8760;
  const hourlyRate = rawRate / termHours;
  let note =
    "Reservation price converted from term total to a monthly equivalent.";
  if (isWindows && !windowsMatch) {
    note = `${note} Azure reservation prices are not OS-specific in the retail API.`;
  }

  const result = { hourlyRate, note };
  azureReservedCache.set(cacheKey, result);
  return result;
}

function normalizeAwsAttribute(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ");
}

function matchesAwsOperatingSystem(value, os) {
  const normalized = normalizeAwsAttribute(value);
  if (!normalized) {
    return false;
  }
  if (os === "windows") {
    return normalized === "windows";
  }
  return normalized.startsWith("linux");
}

function matchesAwsLicenseModel(value, os, sqlEdition) {
  const normalized = normalizeAwsAttribute(value);
  if (os === "windows" && sqlEdition === "none") {
    if (!normalized) {
      return true;
    }
    return normalized === "no license required";
  }
  return true;
}

function matchesAwsSqlEdition(preInstalledSw, sqlEdition) {
  const normalized = normalizeAwsAttribute(preInstalledSw);
  if (sqlEdition === "standard") {
    return normalized.includes("sql") && normalized.includes("std");
  }
  if (sqlEdition === "enterprise") {
    return normalized.includes("sql") && normalized.includes("ent");
  }
  return normalized === "" || !normalized.includes("sql");
}

function filterAwsPriceList(priceList, { os, sqlEdition }) {
  return priceList.filter((item) => {
    const attrs = item.product?.attributes || {};
    const osValue = normalizeAwsAttribute(attrs.operatingSystem);
    const tenancyValue = normalizeAwsAttribute(attrs.tenancy);
    const capacityValue = normalizeAwsAttribute(attrs.capacitystatus);
    if (os === "windows" && osValue && osValue !== "windows") {
      return false;
    }
    if (os !== "windows" && osValue && osValue !== "linux") {
      return false;
    }
    if (tenancyValue && tenancyValue !== "shared") {
      return false;
    }
    if (capacityValue && capacityValue !== "used") {
      return false;
    }
    if (!matchesAwsLicenseModel(attrs.licenseModel, os, sqlEdition)) {
      return false;
    }
    return matchesAwsSqlEdition(attrs.preInstalledSw, sqlEdition);
  });
}

function extractAwsOnDemand(priceList) {
  return priceList
    .flatMap((item) => Object.values(item.terms?.OnDemand || {}))
    .flatMap((term) => Object.values(term.priceDimensions || {}));
}

async function loadAwsPriceListRegionIndex() {
  if (
    awsPriceListIndexCache.data &&
    Date.now() - awsPriceListIndexCache.loadedAt < AWS_PRICE_LIST_CACHE_TTL_MS
  ) {
    return awsPriceListIndexCache.data;
  }
  const response = await fetcher(AWS_PRICE_LIST_REGION_INDEX_URL, {
    headers: {
      "User-Agent": "cloud-price/0.1",
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(
      `AWS price list region index fetch failed: ${response.status}`
    );
  }
  const data = await response.json();
  const regions = data?.regions || {};
  awsPriceListIndexCache.data = regions;
  awsPriceListIndexCache.loadedAt = Date.now();
  return regions;
}

async function loadAwsPriceListRegion(regionCode) {
  const cached = awsPriceListRegionCache.get(regionCode);
  if (cached && Date.now() - cached.loadedAt < AWS_PRICE_LIST_CACHE_TTL_MS) {
    return cached.data;
  }
  const regions = await loadAwsPriceListRegionIndex();
  const entry = regions[regionCode];
  if (!entry?.currentVersionUrl) {
    throw new Error("AWS price list missing region.");
  }
  const url = `${AWS_PRICE_LIST_BASE_URL}${entry.currentVersionUrl}`;
  const response = await fetcher(url, {
    headers: {
      "User-Agent": "cloud-price/0.1",
      Accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`AWS price list fetch failed: ${response.status}`);
  }
  const data = await response.json();
  awsPriceListRegionCache.set(regionCode, {
    loadedAt: Date.now(),
    data,
  });
  return data;
}

async function getAwsPriceListOnDemandRate({
  instanceType,
  region,
  location,
  os,
  sqlEdition,
  logContext,
}) {
  const data = await loadAwsPriceListRegion(region);
  const products = data?.products || {};
  const onDemandTerms = data?.terms?.OnDemand || {};
  const candidates = [];

  for (const [sku, product] of Object.entries(products)) {
    const attrs = product?.attributes || {};
    if (attrs.instanceType !== instanceType) {
      continue;
    }
    if (attrs.location !== location) {
      continue;
    }
    if (!matchesAwsOperatingSystem(attrs.operatingSystem, os)) {
      continue;
    }
    if (attrs.tenancy && attrs.tenancy !== "Shared") {
      continue;
    }
    if (!matchesAwsLicenseModel(attrs.licenseModel, os, sqlEdition)) {
      continue;
    }
    if (!matchesAwsSqlEdition(attrs.preInstalledSw, sqlEdition)) {
      continue;
    }
    candidates.push({ sku, attrs });
  }

  if (!candidates.length) {
    throw new Error("AWS price list missing matching SKU.");
  }

  const usedCandidate = candidates.find(
    (candidate) =>
      normalizeAwsAttribute(candidate.attrs.capacitystatus) === "used"
  );
  const selected = usedCandidate || candidates[0];
  const termMap = onDemandTerms[selected.sku] || {};
  const priceDimensions = Object.values(termMap).flatMap((term) =>
    Object.values(term.priceDimensions || {})
  );

  if (!priceDimensions.length) {
    logPricingWarning(
      "aws",
      { ...logContext, source: "price-list", sku: selected.sku },
      "AWS price list missing on-demand dimensions."
    );
    throw new Error("AWS price list missing on-demand price dimensions.");
  }

  const unitMatches = new Set(["Hrs", "Hour", "Hours", "hrs", "hour", "hours"]);
  let hourly = priceDimensions.find((dimension) =>
    unitMatches.has(dimension.unit)
  );
  if (!hourly) {
    hourly =
      priceDimensions.find((dimension) =>
        /per\s+hour/i.test(dimension.description || "")
      ) || priceDimensions[0];
  }

  const rate = Number.parseFloat(hourly?.pricePerUnit?.USD || "0");
  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("Invalid AWS price list hourly rate.");
  }

  return rate;
}

async function reconcileAwsApiRate({
  awsResponse,
  awsSize,
  region,
  os,
  sqlEdition,
  logContext,
}) {
  if (!Number.isFinite(awsResponse.hourlyRate)) {
    return;
  }
  if (awsResponse.source === "aws-price-list") {
    return;
  }
  try {
    const priceListRate = await getAwsPriceListOnDemandRate({
      instanceType: awsSize.type,
      region: region.aws.region,
      location: region.aws.location,
      os,
      sqlEdition,
      logContext,
    });
    if (
      Number.isFinite(priceListRate) &&
      awsResponse.hourlyRate < priceListRate
    ) {
      logPricingWarning(
        "aws",
        {
          ...logContext,
          apiRate: awsResponse.hourlyRate,
          priceListRate,
        },
        "AWS API rate below price list; using AWS price list."
      );
      awsResponse.hourlyRate = priceListRate;
      awsResponse.source = "aws-price-list";
      awsResponse.message = "Using AWS price list for consistency.";
    }
  } catch (error) {
    logPricingError(
      "aws",
      {
        ...logContext,
        instanceType: awsSize.type,
        region: region.aws.region,
        os,
        sqlEdition,
        source: "price-list",
      },
      error
    );
  }
}

async function getAwsOnDemandPrice({
  instanceType,
  location,
  os,
  sqlEdition,
  logContext,
}) {
  const preInstalledSw =
    sqlEdition === "standard"
      ? "SQL Std"
      : sqlEdition === "enterprise"
      ? "SQL Ent"
      : "NA";
  const cacheKey = [instanceType, location, os, preInstalledSw].join("|");
  if (awsCache.has(cacheKey)) {
    return awsCache.get(cacheKey);
  }

  const baseFilters = [
    { Type: "TERM_MATCH", Field: "instanceType", Value: instanceType },
    { Type: "TERM_MATCH", Field: "location", Value: location },
  ];
  const osFilter = {
    Type: "TERM_MATCH",
    Field: "operatingSystem",
    Value: os === "windows" ? "Windows" : "Linux",
  };
  const strictFilters = [
    ...baseFilters,
    osFilter,
    { Type: "TERM_MATCH", Field: "tenancy", Value: "Shared" },
    { Type: "TERM_MATCH", Field: "preInstalledSw", Value: preInstalledSw },
    { Type: "TERM_MATCH", Field: "capacitystatus", Value: "Used" },
  ];
  const noPreinstallFilters = [
    ...baseFilters,
    osFilter,
    { Type: "TERM_MATCH", Field: "tenancy", Value: "Shared" },
    { Type: "TERM_MATCH", Field: "capacitystatus", Value: "Used" },
  ];
  const relaxedFilters = [
    ...baseFilters,
    osFilter,
    { Type: "TERM_MATCH", Field: "tenancy", Value: "Shared" },
  ];
  const fallbackFilters = [...baseFilters, osFilter];
  const filterSets = [
    { name: "strict", filters: strictFilters, localFilter: null },
    {
      name: "no-preinstall",
      filters: noPreinstallFilters,
      localFilter: (list) => filterAwsPriceList(list, { os, sqlEdition }),
    },
    {
      name: "no-capacity",
      filters: relaxedFilters,
      localFilter: (list) => filterAwsPriceList(list, { os, sqlEdition }),
    },
    {
      name: "fallback",
      filters: fallbackFilters,
      localFilter: (list) => filterAwsPriceList(list, { os, sqlEdition }),
    },
  ];

  let onDemand = [];
  for (const set of filterSets) {
    const command = new GetProductsCommand({
      ServiceCode: "AmazonEC2",
      Filters: set.filters,
      FormatVersion: "aws_v1",
      MaxResults: 100,
    });
    const response = await getAwsPricingClient().send(command);
    const priceList = (response.PriceList || []).map((item) =>
      typeof item === "string" ? JSON.parse(item) : item
    );
    if (!priceList.length) {
      logPricingWarning(
        "aws",
        { ...logContext, filterSet: set.name },
        "AWS pricing API returned no products."
      );
      continue;
    }
    const filteredList = set.localFilter ? set.localFilter(priceList) : priceList;
    onDemand = extractAwsOnDemand(filteredList);
    if (onDemand.length) {
      break;
    }
    const termKeys = Array.from(
      new Set(
        priceList.flatMap((item) => Object.keys(item.terms || {}))
      )
    );
    logPricingWarning(
      "aws",
      {
        ...logContext,
        filterSet: set.name,
        productCount: priceList.length,
        filteredCount: filteredList.length,
        termKeys,
      },
      "No AWS on-demand price dimensions found for filter set."
    );
  }
  if (!onDemand.length) {
    throw new Error("No AWS on-demand price dimensions found.");
  }

  const unitMatches = new Set(["Hrs", "Hour", "Hours", "hrs", "hour", "hours"]);
  let hourly = onDemand.find((dimension) => unitMatches.has(dimension.unit));
  if (!hourly) {
    const units = Array.from(
      new Set(onDemand.map((dimension) => dimension.unit).filter(Boolean))
    );
    logPricingWarning(
      "aws",
      { ...logContext, units },
      "No hourly unit found in AWS price dimensions; using fallback."
    );
    hourly =
      onDemand.find((dimension) =>
        /per\s+hour/i.test(dimension.description || "")
      ) || onDemand[0];
  }

  const rate = Number.parseFloat(hourly.pricePerUnit?.USD || "0");
  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("Invalid AWS hourly rate.");
  }

  awsCache.set(cacheKey, rate);
  return rate;
}

async function getAzureOnDemandPrice({ skuName, region, os }) {
  const cacheKey = [skuName, region, os].join("|");
  if (azureCache.has(cacheKey)) {
    return azureCache.get(cacheKey);
  }

  const query = [
    `armRegionName eq '${region}'`,
    "serviceName eq 'Virtual Machines'",
    `armSkuName eq '${skuName}'`,
    "priceType eq 'Consumption'",
  ].join(" and ");

  const url =
    "https://prices.azure.com/api/retail/prices?$filter=" +
    encodeURIComponent(query);
  const response = await fetcher(url);
  if (!response.ok) {
    throw new Error(`Azure pricing API error: ${response.status}`);
  }
  const data = await response.json();
  const items = data.Items || [];
  const isWindows = os === "windows";
  const filtered = items.filter((item) => {
    const label = `${item.productName || ""} ${item.skuName || ""} ${
      item.meterName || ""
    }`;
    if (/spot|low priority/i.test(label)) {
      return false;
    }
    if (isWindows) {
      return /windows/i.test(item.productName || "");
    }
    return !/windows/i.test(item.productName || "");
  });
  const candidate = filtered[0] || items[0];
  const rate = Number.parseFloat(candidate?.retailPrice || "0");
  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("Invalid Azure hourly rate.");
  }
  azureCache.set(cacheKey, rate);
  return rate;
}

function toText(value) {
  return typeof value === "string" ? value : value == null ? "" : String(value);
}

function includesMatch(value, matcher) {
  if (!matcher) {
    return true;
  }
  const text = toText(value);
  if (!text) {
    return false;
  }
  if (matcher instanceof RegExp) {
    return matcher.test(text);
  }
  return text.toLowerCase().includes(toText(matcher).toLowerCase());
}

function listIncludes(list, matcher) {
  if (!matcher) {
    return true;
  }
  if (!Array.isArray(list)) {
    return false;
  }
  const needle = toText(matcher).toLowerCase();
  return list.some((item) => toText(item).toLowerCase().includes(needle));
}

function isHourlyUnit(unit) {
  return /hour/i.test(toText(unit));
}

function isGbMonthUnit(unit) {
  const text = toText(unit).toLowerCase();
  return (
    text.includes("gb-mo") ||
    text.includes("gb/month") ||
    text.includes("gb month") ||
    text.includes("giby.mo") ||
    text.includes("gib/month") ||
    text.includes("gib month")
  );
}

async function loadAwsServiceRegionIndex(serviceCode) {
  const cached = awsServiceIndexCache.get(serviceCode);
  if (cached && Date.now() - cached.loadedAt < AWS_PRICE_LIST_CACHE_TTL_MS) {
    return cached.data;
  }
  const url = `${AWS_PRICE_LIST_BASE_URL}/offers/v1.0/aws/${serviceCode}/current/region_index.json`;
  const response = await fetcher(url, {
    headers: { "User-Agent": "cloud-price/0.1", Accept: "application/json" },
  });
  if (!response.ok) {
    throw new Error(`AWS ${serviceCode} price index fetch failed: ${response.status}`);
  }
  const data = await response.json();
  awsServiceIndexCache.set(serviceCode, {
    loadedAt: Date.now(),
    data,
  });
  return data;
}

async function loadAwsServicePriceList(serviceCode, regionCode) {
  const cacheKey = `${serviceCode}|${regionCode}`;
  const cached = awsServiceRegionCache.get(cacheKey);
  if (cached && Date.now() - cached.loadedAt < AWS_PRICE_LIST_CACHE_TTL_MS) {
    return cached.data;
  }
  const index = await loadAwsServiceRegionIndex(serviceCode);
  const regionEntry = index?.regions?.[regionCode];
  if (!regionEntry?.currentVersionUrl) {
    throw new Error(`AWS ${serviceCode} pricing missing for region.`);
  }
  const url = `${AWS_PRICE_LIST_BASE_URL}${regionEntry.currentVersionUrl}`;
  const response = await fetcher(url, {
    headers: { "User-Agent": "cloud-price/0.1", Accept: "application/json" },
  });
  if (!response.ok) {
    throw new Error(`AWS ${serviceCode} pricing fetch failed: ${response.status}`);
  }
  const data = await response.json();
  awsServiceRegionCache.set(cacheKey, {
    loadedAt: Date.now(),
    data,
  });
  return data;
}

async function getAwsServiceHourlyRate({
  serviceCode,
  regionCode,
  location,
  matchers,
}) {
  const data = await loadAwsServicePriceList(serviceCode, regionCode);
  const products = data.products || {};
  const candidates = [];
  for (const [sku, product] of Object.entries(products)) {
    const attrs = product.attributes || {};
    if (location && attrs.location && attrs.location !== location) {
      continue;
    }
    if (
      matchers?.productFamily &&
      product.productFamily &&
      product.productFamily !== matchers.productFamily
    ) {
      continue;
    }
    if (
      matchers?.usagetypeIncludes &&
      !includesMatch(attrs.usagetype, matchers.usagetypeIncludes)
    ) {
      continue;
    }
    if (
      matchers?.operationIncludes &&
      !includesMatch(attrs.operation, matchers.operationIncludes)
    ) {
      continue;
    }
    if (
      matchers?.groupDescriptionIncludes &&
      !includesMatch(attrs.groupDescription, matchers.groupDescriptionIncludes)
    ) {
      continue;
    }
    if (
      matchers?.subcategoryIncludes &&
      !includesMatch(attrs.subcategory, matchers.subcategoryIncludes)
    ) {
      continue;
    }
    candidates.push({ sku, attrs });
  }
  for (const candidate of candidates) {
    const terms = data.terms?.OnDemand?.[candidate.sku] || {};
    for (const term of Object.values(terms)) {
      for (const dimension of Object.values(term.priceDimensions || {})) {
        if (!isHourlyUnit(dimension.unit)) {
          continue;
        }
        const rate = Number.parseFloat(dimension.pricePerUnit?.USD || "0");
        if (Number.isFinite(rate) && rate > 0) {
          return rate;
        }
      }
    }
  }
  throw new Error(`AWS ${serviceCode} hourly rate not found.`);
}

async function getAwsServiceGbMonthRate({
  serviceCode,
  regionCode,
  location,
  matchers,
}) {
  const data = await loadAwsServicePriceList(serviceCode, regionCode);
  const products = data.products || {};
  const candidates = [];
  for (const [sku, product] of Object.entries(products)) {
    const attrs = product.attributes || {};
    if (location && attrs.location && attrs.location !== location) {
      continue;
    }
    if (
      matchers?.productFamily &&
      product.productFamily &&
      product.productFamily !== matchers.productFamily
    ) {
      continue;
    }
    if (
      matchers?.usagetypeIncludes &&
      !includesMatch(attrs.usagetype, matchers.usagetypeIncludes)
    ) {
      continue;
    }
    if (
      matchers?.operationIncludes &&
      !includesMatch(attrs.operation, matchers.operationIncludes)
    ) {
      continue;
    }
    if (
      matchers?.groupDescriptionIncludes &&
      !includesMatch(attrs.groupDescription, matchers.groupDescriptionIncludes)
    ) {
      continue;
    }
    if (
      matchers?.subcategoryIncludes &&
      !includesMatch(attrs.subcategory, matchers.subcategoryIncludes)
    ) {
      continue;
    }
    if (
      matchers?.storageClassIncludes &&
      !includesMatch(attrs.storageClass, matchers.storageClassIncludes)
    ) {
      continue;
    }
    candidates.push({ sku, attrs });
  }
  for (const candidate of candidates) {
    const terms = data.terms?.OnDemand?.[candidate.sku] || {};
    for (const term of Object.values(terms)) {
      for (const dimension of Object.values(term.priceDimensions || {})) {
        if (!isGbMonthUnit(dimension.unit)) {
          continue;
        }
        const rate = Number.parseFloat(dimension.pricePerUnit?.USD || "0");
        if (Number.isFinite(rate) && rate > 0) {
          return rate;
        }
      }
    }
  }
  throw new Error(`AWS ${serviceCode} GB-month rate not found.`);
}

async function getAzureRetailHourlyRate({
  serviceName,
  region,
  skuName,
  productNameIncludes,
  meterNameIncludes,
  unitIncludes,
  allowRegionFallback = true,
}) {
  const cacheKey = [
    serviceName,
    region,
    skuName || "",
    productNameIncludes || "",
    meterNameIncludes || "",
    unitIncludes || "",
  ].join("|");
  if (azureNetworkCache.has(cacheKey)) {
    return azureNetworkCache.get(cacheKey);
  }
  const baseQuery = [
    `serviceName eq '${serviceName}'`,
    region ? `armRegionName eq '${region}'` : null,
  ]
    .filter(Boolean)
    .join(" and ");
  let url =
    "https://prices.azure.com/api/retail/prices?$filter=" +
    encodeURIComponent(baseQuery);
  while (url) {
    const response = await fetcher(url);
    if (!response.ok) {
      throw new Error(`Azure pricing API error: ${response.status}`);
    }
    const data = await response.json();
    const items = data.Items || [];
    const filtered = items.filter((item) => {
      if (!isHourlyUnit(item.unitOfMeasure)) {
        return false;
      }
      if (unitIncludes && !includesMatch(item.unitOfMeasure, unitIncludes)) {
        return false;
      }
      if (skuName && item.skuName !== skuName) {
        return false;
      }
      if (productNameIncludes && !includesMatch(item.productName, productNameIncludes)) {
        return false;
      }
      if (meterNameIncludes && !includesMatch(item.meterName, meterNameIncludes)) {
        return false;
      }
      return true;
    });
    const candidate = filtered[0] || null;
    if (candidate) {
      const rate = Number.parseFloat(candidate.retailPrice || "0");
      if (Number.isFinite(rate)) {
        azureNetworkCache.set(cacheKey, rate);
        return rate;
      }
    }
    url = data.NextPageLink || null;
  }
  if (region && allowRegionFallback) {
    return getAzureRetailHourlyRate({
      serviceName,
      region: null,
      skuName,
      productNameIncludes,
      meterNameIncludes,
      unitIncludes,
      allowRegionFallback: false,
    });
  }
  throw new Error("Azure hourly rate not found.");
}

async function getAzureRetailGbMonthRate({
  serviceName,
  region,
  skuName,
  productNameIncludes,
  meterNameIncludes,
  allowRegionFallback = true,
}) {
  const cacheKey = [
    "gb-month",
    serviceName,
    region,
    skuName || "",
    productNameIncludes || "",
    meterNameIncludes || "",
  ].join("|");
  if (azureNetworkCache.has(cacheKey)) {
    return azureNetworkCache.get(cacheKey);
  }
  const baseQuery = [
    `serviceName eq '${serviceName}'`,
    region ? `armRegionName eq '${region}'` : null,
  ]
    .filter(Boolean)
    .join(" and ");
  let url =
    "https://prices.azure.com/api/retail/prices?$filter=" +
    encodeURIComponent(baseQuery);
  while (url) {
    const response = await fetcher(url);
    if (!response.ok) {
      throw new Error(`Azure pricing API error: ${response.status}`);
    }
    const data = await response.json();
    const items = data.Items || [];
    const filtered = items.filter((item) => {
      if (!isGbMonthUnit(item.unitOfMeasure)) {
        return false;
      }
      if (skuName && item.skuName !== skuName) {
        return false;
      }
      if (productNameIncludes && !includesMatch(item.productName, productNameIncludes)) {
        return false;
      }
      if (meterNameIncludes && !includesMatch(item.meterName, meterNameIncludes)) {
        return false;
      }
      return true;
    });
    const candidate = filtered[0] || null;
    if (candidate) {
      const rate = Number.parseFloat(candidate.retailPrice || "0");
      if (Number.isFinite(rate) && rate > 0) {
        azureNetworkCache.set(cacheKey, rate);
        return rate;
      }
    }
    url = data.NextPageLink || null;
  }
  if (region && allowRegionFallback) {
    return getAzureRetailGbMonthRate({
      serviceName,
      region: null,
      skuName,
      productNameIncludes,
      meterNameIncludes,
      allowRegionFallback: false,
    });
  }
  throw new Error("Azure GB-month rate not found.");
}

async function loadGcpServices(apiKey) {
  if (
    gcpServiceCache.data &&
    Date.now() - gcpServiceCache.loadedAt < GCP_BILLING_CACHE_TTL_MS
  ) {
    return gcpServiceCache.data;
  }
  let url =
    "https://cloudbilling.googleapis.com/v1/services?key=" +
    encodeURIComponent(apiKey);
  const services = [];
  while (url) {
    const response = await fetcher(url);
    if (!response.ok) {
      throw new Error(`GCP Billing API error: ${response.status}`);
    }
    const data = await response.json();
    if (Array.isArray(data.services)) {
      services.push(...data.services);
    }
    if (data.nextPageToken) {
      url =
        "https://cloudbilling.googleapis.com/v1/services?key=" +
        encodeURIComponent(apiKey) +
        `&pageToken=${encodeURIComponent(data.nextPageToken)}`;
    } else {
      url = null;
    }
  }
  gcpServiceCache.data = services;
  gcpServiceCache.loadedAt = Date.now();
  return services;
}

async function getGcpServiceIdByName(apiKey, displayName) {
  const services = await loadGcpServices(apiKey);
  const normalized = toText(displayName).toLowerCase();
  const exact = services.find(
    (service) => toText(service.displayName).toLowerCase() === normalized
  );
  if (exact) {
    return exact.name?.split("/").pop();
  }
  const partial = services.find((service) =>
    toText(service.displayName).toLowerCase().includes(normalized)
  );
  if (!partial) {
    throw new Error(`GCP service not found: ${displayName}`);
  }
  return partial.name?.split("/").pop();
}

async function loadGcpServiceSkus(apiKey, serviceId) {
  const cached = gcpServiceSkuCache.get(serviceId);
  if (cached && Date.now() - cached.loadedAt < GCP_BILLING_CACHE_TTL_MS) {
    return cached.data;
  }
  let url =
    `https://cloudbilling.googleapis.com/v1/services/${serviceId}/skus?key=` +
    encodeURIComponent(apiKey);
  const skus = [];
  while (url) {
    const response = await fetcher(url);
    if (!response.ok) {
      throw new Error(`GCP Billing API error: ${response.status}`);
    }
    const data = await response.json();
    if (Array.isArray(data.skus)) {
      skus.push(...data.skus);
    }
    if (data.nextPageToken) {
      url =
        `https://cloudbilling.googleapis.com/v1/services/${serviceId}/skus?key=` +
        encodeURIComponent(apiKey) +
        `&pageToken=${encodeURIComponent(data.nextPageToken)}`;
    } else {
      url = null;
    }
  }
  gcpServiceSkuCache.set(serviceId, { data: skus, loadedAt: Date.now() });
  return skus;
}

function findGcpHourlySkuRate({ skus, region, descriptionPatterns }) {
  const patterns = (descriptionPatterns || []).map((pattern) =>
    pattern instanceof RegExp ? pattern : new RegExp(pattern, "i")
  );
  const candidate = skus.find((sku) => {
    const description = toText(sku.description);
    if (!patterns.every((pattern) => pattern.test(description))) {
      return false;
    }
    const regions = sku.serviceRegions || [];
    if (!listIncludes(regions, region) && !regions.includes("global")) {
      return false;
    }
    const pricingInfo = sku.pricingInfo || [];
    const usageUnit = pricingInfo[0]?.pricingExpression?.usageUnit;
    const usageDesc = pricingInfo[0]?.pricingExpression?.usageUnitDescription;
    if (!isHourlyUnit(usageUnit) && !isHourlyUnit(usageDesc)) {
      return false;
    }
    return true;
  });
  const price =
    candidate?.pricingInfo?.[0]?.pricingExpression?.tieredRates?.[0]
      ?.unitPrice;
  const rate = unitPriceToNumber(price);
  if (!Number.isFinite(rate) || rate <= 0) {
    return null;
  }
  return rate;
}

function findGcpGbMonthSkuRate({ skus, region, descriptionPatterns }) {
  const patterns = (descriptionPatterns || []).map((pattern) =>
    pattern instanceof RegExp ? pattern : new RegExp(pattern, "i")
  );
  const candidate = skus.find((sku) => {
    const description = toText(sku.description);
    if (!patterns.every((pattern) => pattern.test(description))) {
      return false;
    }
    const regions = sku.serviceRegions || [];
    if (!listIncludes(regions, region) && !regions.includes("global")) {
      return false;
    }
    const pricingInfo = sku.pricingInfo || [];
    const usageUnit = toText(pricingInfo[0]?.pricingExpression?.usageUnit).toLowerCase();
    const usageDesc = toText(
      pricingInfo[0]?.pricingExpression?.usageUnitDescription
    ).toLowerCase();
    const byUnit = usageUnit.includes("giby.mo") || usageUnit.includes("gibibyte month");
    const byDesc =
      usageDesc.includes("gibibyte month") ||
      usageDesc.includes("gibibyte-month") ||
      usageDesc.includes("giby.mo");
    if (!byUnit && !byDesc) {
      return false;
    }
    return true;
  });
  const price =
    candidate?.pricingInfo?.[0]?.pricingExpression?.tieredRates?.[0]
      ?.unitPrice;
  const rate = unitPriceToNumber(price);
  if (!Number.isFinite(rate) || rate <= 0) {
    return null;
  }
  return rate;
}

async function getGcpServiceGbMonthRate({
  serviceName,
  region,
  descriptionPatterns,
}) {
  if (!hasGcpApiCredentials()) {
    throw new Error("GCP API key missing.");
  }
  const serviceId = await getGcpServiceIdByName(getGcpApiKey(), serviceName);
  const skus = await loadGcpServiceSkus(getGcpApiKey(), serviceId);
  const rate = findGcpGbMonthSkuRate({
    skus,
    region,
    descriptionPatterns,
  });
  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error(`GCP GB-month rate not found for ${serviceName}.`);
  }
  return rate;
}

function resolveNetworkFlavor(providerKey, addonKey, flavorKey) {
  const options = NETWORK_ADDON_OPTIONS[providerKey]?.[addonKey] || [];
  const defaults = NETWORK_ADDON_DEFAULTS[providerKey] || {};
  const fallbackKey = defaults[addonKey] || (options[0] ? options[0].key : "");
  const resolvedKey = flavorKey || fallbackKey;
  return (
    options.find((option) => option.key === resolvedKey) ||
    options.find((option) => option.key === fallbackKey) ||
    options[0] ||
    null
  );
}

function normalizeAddonSelection(selection) {
  if (typeof selection === "string") {
    return {
      flavor: selection,
      count: 1,
      dataTb: 0,
    };
  }
  const flavor =
    typeof selection?.flavor === "string" ? selection.flavor : "";
  const count = Math.max(0, Math.round(toNumber(selection?.count, 1)));
  const dataTb = Math.max(0, toNumber(selection?.dataTb, 0));
  return {
    flavor,
    count,
    dataTb,
  };
}

async function resolveNetworkAddonRate({
  providerKey,
  addonKey,
  flavor,
  region,
}) {
  const pricing = flavor?.pricing || {};
  if (pricing.type === "static") {
    return { hourlyRate: pricing.hourly || 0, source: "static" };
  }
  if (pricing.type === "aws-price-list") {
    const rate = await getAwsServiceHourlyRate({
      serviceCode: pricing.serviceCode,
      regionCode: region.aws.region,
      location: region.aws.location,
      matchers: pricing,
    });
    return { hourlyRate: rate, source: "aws-price-list" };
  }
  if (pricing.type === "azure-retail") {
    const rate = await getAzureRetailHourlyRate({
      serviceName: pricing.serviceName,
      region: region.azure.region,
      skuName: pricing.skuName,
      productNameIncludes: pricing.productNameIncludes,
      meterNameIncludes: pricing.meterNameIncludes,
      unitIncludes: pricing.unitIncludes,
    });
    return { hourlyRate: rate, source: "azure-retail-api" };
  }
  if (pricing.type === "gcp-billing") {
    if (!hasGcpApiCredentials()) {
      throw new Error("GCP API key missing.");
    }
    const serviceId = await getGcpServiceIdByName(
      getGcpApiKey(),
      pricing.serviceName
    );
    const skus = await loadGcpServiceSkus(getGcpApiKey(), serviceId);
    const rate = findGcpHourlySkuRate({
      skus,
      region: region.gcp.region,
      descriptionPatterns: pricing.descriptionPatterns,
    });
    if (!Number.isFinite(rate) || rate <= 0) {
      throw new Error("GCP hourly rate not found.");
    }
    return { hourlyRate: rate, source: "gcp-cloud-billing" };
  }
  return { hourlyRate: 0, source: "unknown" };
}

function resolveNetworkAddonDataRate(providerKey, addonKey) {
  const providerRates = NETWORK_ADDON_DATA_RATES[providerKey] || {};
  const rate = providerRates[addonKey];
  return Number.isFinite(rate) ? rate : 0;
}

async function resolveNetworkAddonsForProvider({
  providerKey,
  region,
  selections,
  hours,
}) {
  const items = [];
  const errors = [];
  let hourlyTotal = 0;
  let monthlyTotal = 0;
  const addonKeys = Object.keys(selections || {});
  for (const addonKey of addonKeys) {
    const normalized = normalizeAddonSelection(selections?.[addonKey]);
    if (!normalized.count || normalized.count <= 0) {
      continue;
    }
    const flavor = resolveNetworkFlavor(
      providerKey,
      addonKey,
      normalized.flavor
    );
    if (!flavor || flavor.key === "none") {
      continue;
    }
    try {
      const { hourlyRate, source } = await resolveNetworkAddonRate({
        providerKey,
        addonKey,
        flavor,
        region,
      });
      const dataRatePerGb = resolveNetworkAddonDataRate(providerKey, addonKey);
      const dataGb = normalized.dataTb * 1024;
      const itemHourly = hourlyRate * normalized.count;
      const itemMonthly =
        itemHourly * (Number.isFinite(hours) ? hours : HOURS_IN_MONTH) +
        dataGb * dataRatePerGb * normalized.count;
      hourlyTotal += itemHourly;
      monthlyTotal += itemMonthly;
      items.push({
        addonKey,
        key: flavor.key,
        label: flavor.label,
        hourlyRate: itemHourly,
        unitHourlyRate: hourlyRate,
        count: normalized.count,
        dataTb: normalized.dataTb,
        dataRatePerGb,
        monthlyTotal: itemMonthly,
        source,
        status: "ok",
      });
    } catch (error) {
      errors.push(`${addonKey}:${flavor.label}`);
      items.push({
        addonKey,
        key: flavor.key,
        label: flavor.label,
        count: normalized.count,
        dataTb: normalized.dataTb,
        hourlyRate: 0,
        unitHourlyRate: 0,
        dataRatePerGb: 0,
        monthlyTotal: 0,
        source: "missing",
        status: "error",
      });
    }
  }
  const note = errors.length
    ? `Network add-on pricing missing for ${errors.join(", ")}.`
    : null;
  return {
    items,
    hourlyTotal,
    monthlyTotal,
    note,
  };
}

function filterNetworkAddonsByFocus(addons, focus, hours) {
  if (!addons || focus === "all") {
    return addons;
  }
  const items = Array.isArray(addons.items)
    ? addons.items.filter((item) => item.addonKey === focus)
    : [];
  const hourlyTotal = items.reduce((sum, item) => {
    const hourlyRate = Number.isFinite(item.hourlyRate) ? item.hourlyRate : 0;
    return sum + hourlyRate;
  }, 0);
  const monthlyTotal = items.reduce((sum, item) => {
    const monthly = Number.isFinite(item.monthlyTotal)
      ? item.monthlyTotal
      : Number.isFinite(hours)
      ? (Number.isFinite(item.hourlyRate) ? item.hourlyRate : 0) * hours
      : 0;
    return sum + monthly;
  }, 0);
  const errors = items
    .filter((item) => item.status === "error")
    .map((item) => `${focus}:${item.label}`);
  return {
    ...addons,
    items,
    hourlyTotal,
    monthlyTotal,
    note: errors.length
      ? `Network add-on pricing missing for ${errors.join(", ")}.`
      : null,
  };
}

function computeTotals({
  hourlyRate,
  osDiskGb,
  dataDiskGb,
  snapshotGb,
  egressGb,
  hours,
  storageRate,
  dataStorageRate,
  snapshotRate,
  egressRate,
  networkMonthly,
  interVlanMonthly,
  intraVlanMonthly,
  interRegionMonthly,
  iopsMonthly,
  throughputMonthly,
  sqlLicenseRate,
  windowsLicenseMonthly,
  vcpu,
  drPercent,
  vmCount,
  controlPlaneMonthly,
  egressScale,
  osScale,
  dataScale,
}) {
  const computeBase = hourlyRate ? hourlyRate * hours : 0;
  const osGb = Number.isFinite(osDiskGb) ? osDiskGb : 0;
  const dataGb = Number.isFinite(dataDiskGb) ? dataDiskGb : 0;
  const dataRate = Number.isFinite(dataStorageRate)
    ? dataStorageRate
    : storageRate;
  const osBase = osGb * storageRate;
  const dataBase = dataGb * dataRate;
  const backupBase = snapshotGb * snapshotRate;
  const egressBase = egressGb * egressRate;
  const sqlBase = sqlLicenseRate * vcpu * hours;
  const scale = Number.isFinite(vmCount) && vmCount > 0 ? vmCount : 1;
  const controlPlane = Number.isFinite(controlPlaneMonthly)
    ? controlPlaneMonthly
    : 0;
  const osMultiplier =
    Number.isFinite(osScale) && osScale > 0 ? osScale : scale;
  const dataMultiplier =
    Number.isFinite(dataScale) && dataScale > 0 ? dataScale : scale;
  const egressMultiplier =
    Number.isFinite(egressScale) && egressScale > 0
      ? egressScale
      : scale;
  const computeMonthly = computeBase * scale;
  const backupMonthly = backupBase * scale;
  const egressMonthly = egressBase * egressMultiplier;
  const sqlMonthly = sqlBase * scale;
  const windowsLicenseMonthlyTotal = Number.isFinite(windowsLicenseMonthly)
    ? windowsLicenseMonthly * scale
    : 0;
  const networkBase = Number.isFinite(networkMonthly) ? networkMonthly : 0;
  const interVlanBase = Number.isFinite(interVlanMonthly)
    ? interVlanMonthly
    : 0;
  const intraVlanBase = Number.isFinite(intraVlanMonthly)
    ? intraVlanMonthly
    : 0;
  const interRegionBase = Number.isFinite(interRegionMonthly)
    ? interRegionMonthly
    : 0;
  const iopsBase = Number.isFinite(iopsMonthly) ? iopsMonthly : 0;
  const throughputBase = Number.isFinite(throughputMonthly)
    ? throughputMonthly
    : 0;
  const baseStorageMonthly =
    osBase * osMultiplier + dataBase * dataMultiplier;
  const storageMonthly = baseStorageMonthly + iopsBase + throughputBase;
  const drRate = Number.isFinite(drPercent) ? drPercent / 100 : 0;
  const drMonthly =
    drRate > 0
      ? (computeMonthly + storageMonthly + backupMonthly + sqlMonthly) *
        drRate
      : 0;
  const total =
    computeMonthly +
    storageMonthly +
    backupMonthly +
    egressMonthly +
    sqlMonthly +
    windowsLicenseMonthlyTotal +
    drMonthly +
    networkBase +
    interVlanBase +
    intraVlanBase +
    interRegionBase +
    controlPlane;
  return {
    computeMonthly,
    controlPlaneMonthly: controlPlane,
    storageMonthly,
    backupMonthly,
    egressMonthly,
    sqlMonthly,
    windowsLicenseMonthly: windowsLicenseMonthlyTotal,
    networkMonthly: networkBase,
    interVlanMonthly: interVlanBase,
    intraVlanMonthly: intraVlanBase,
    interRegionMonthly: interRegionBase,
    iopsMonthly: iopsBase,
    throughputMonthly: throughputBase,
    drMonthly,
    total,
  };
}

function applyPricingFocusToTotals(totals, focus) {
  if (!totals || focus === "all") {
    return totals;
  }
  const computeMonthly = 0;
  const controlPlaneMonthly = 0;
  const sqlMonthly = 0;
  const windowsLicenseMonthly = 0;
  const drMonthly = 0;
  if (focus === "network") {
    const networkMonthly = totals.networkMonthly || 0;
    const interVlanMonthly = totals.interVlanMonthly || 0;
    const intraVlanMonthly = totals.intraVlanMonthly || 0;
    const interRegionMonthly = totals.interRegionMonthly || 0;
    const egressMonthly = totals.egressMonthly || 0;
    const total =
      networkMonthly +
      interVlanMonthly +
      intraVlanMonthly +
      interRegionMonthly +
      egressMonthly;
    return {
      ...totals,
      computeMonthly,
      controlPlaneMonthly,
      storageMonthly: 0,
      backupMonthly: 0,
      egressMonthly,
      networkMonthly,
      interVlanMonthly,
      intraVlanMonthly,
      interRegionMonthly,
      iopsMonthly: 0,
      throughputMonthly: 0,
      sqlMonthly,
      windowsLicenseMonthly,
      drMonthly,
      total,
    };
  }
  if (focus === "storage") {
    const storageMonthly = totals.storageMonthly || 0;
    const iopsMonthly = totals.iopsMonthly || 0;
    const throughputMonthly = totals.throughputMonthly || 0;
    const total = storageMonthly;
    return {
      ...totals,
      computeMonthly,
      controlPlaneMonthly,
      storageMonthly,
      backupMonthly: 0,
      egressMonthly: 0,
      networkMonthly: 0,
      interVlanMonthly: 0,
      intraVlanMonthly: 0,
      interRegionMonthly: 0,
      iopsMonthly,
      throughputMonthly,
      sqlMonthly,
      windowsLicenseMonthly,
      drMonthly,
      total,
    };
  }
  return totals;
}

function computeTotalsOrNull(params) {
  if (!Number.isFinite(params.hourlyRate)) {
    return null;
  }
  return computeTotals(params);
}

app.post("/api/compare", async (req, res) => {
  const body = req.body || {};
  const requestId = `req-${Date.now().toString(36)}-${Math.random()
    .toString(36)
    .slice(2, 7)}`;
  const cpu = Math.max(MIN_CPU, Math.round(toNumber(body.cpu, MIN_CPU)));
  const awsInstanceType =
    typeof body.awsInstanceType === "string"
      ? body.awsInstanceType.trim()
      : "";
  const azureInstanceType =
    typeof body.azureInstanceType === "string"
      ? body.azureInstanceType.trim()
      : "";
  const gcpInstanceType =
    typeof body.gcpInstanceType === "string"
      ? body.gcpInstanceType.trim()
      : "";
  const mode = body.mode === "k8s" ? "k8s" : "vm";
  const pricingFocus =
    body.pricingFocus === "network"
      ? "network"
      : body.pricingFocus === "storage"
      ? "storage"
      : "all";
  const networkAddonFocus =
    pricingFocus === "network"
      ? normalizeNetworkAddonFocus(body.networkAddonFocus)
      : "all";
  const osDiskMin = mode === "k8s" ? K8S_OS_DISK_MIN_GB : 0;
  const osDiskDefault = mode === "k8s" ? K8S_OS_DISK_MIN_GB : 256;
  const osDiskGb = Math.max(
    osDiskMin,
    toNumber(body.osDiskGb, osDiskDefault)
  );
  const dataDiskTb = Math.max(0, toNumber(body.dataDiskTb, 1));
  const dataDiskGb = dataDiskTb * 1024;
  const storageGb = osDiskGb + dataDiskGb;
  const backupEnabled =
    pricingFocus === "all" ? toBoolean(body.backupEnabled) : false;
  const awsVpcFlavor =
    typeof body.awsVpcFlavor === "string" ? body.awsVpcFlavor.trim() : "";
  const awsFirewallFlavor =
    typeof body.awsFirewallFlavor === "string"
      ? body.awsFirewallFlavor.trim()
      : "";
  const awsLoadBalancerFlavor =
    typeof body.awsLoadBalancerFlavor === "string"
      ? body.awsLoadBalancerFlavor.trim()
      : "";
  const azureVpcFlavor =
    typeof body.azureVpcFlavor === "string" ? body.azureVpcFlavor.trim() : "";
  const azureFirewallFlavor =
    typeof body.azureFirewallFlavor === "string"
      ? body.azureFirewallFlavor.trim()
      : "";
  const azureLoadBalancerFlavor =
    typeof body.azureLoadBalancerFlavor === "string"
      ? body.azureLoadBalancerFlavor.trim()
      : "";
  const gcpVpcFlavor =
    typeof body.gcpVpcFlavor === "string" ? body.gcpVpcFlavor.trim() : "";
  const gcpFirewallFlavor =
    typeof body.gcpFirewallFlavor === "string"
      ? body.gcpFirewallFlavor.trim()
      : "";
  const gcpLoadBalancerFlavor =
    typeof body.gcpLoadBalancerFlavor === "string"
      ? body.gcpLoadBalancerFlavor.trim()
      : "";
  const networkAddonInput = (providerKey, addonKey) => {
    const flavorKey = `${providerKey}Network${addonKey}Flavor`;
    const countKey = `${providerKey}Network${addonKey}Count`;
    const dataKey = `${providerKey}Network${addonKey}DataTb`;
    return {
      flavor:
        typeof body[flavorKey] === "string" ? body[flavorKey].trim() : "",
      count: Math.max(0, Math.round(toNumber(body[countKey], 1))),
      dataTb: Math.max(0, toNumber(body[dataKey], 0)),
    };
  };
  const awsNetworkVpc = networkAddonInput("aws", "Vpc");
  const awsNetworkGateway = networkAddonInput("aws", "Gateway");
  const awsNetworkLoadBalancer = networkAddonInput("aws", "LoadBalancer");
  const azureNetworkVpc = networkAddonInput("azure", "Vpc");
  const azureNetworkGateway = networkAddonInput("azure", "Gateway");
  const azureNetworkLoadBalancer = networkAddonInput(
    "azure",
    "LoadBalancer"
  );
  const gcpNetworkVpc = networkAddonInput("gcp", "Vpc");
  const gcpNetworkGateway = networkAddonInput("gcp", "Gateway");
  const gcpNetworkLoadBalancer = networkAddonInput("gcp", "LoadBalancer");

  const storageProfileInput = (providerKey) => ({
    accountCount: Math.max(
      1,
      Math.round(toNumber(body[`${providerKey}StorageAccountCount`], 1))
    ),
    drEnabled: toBoolean(body[`${providerKey}StorageDrEnabled`]),
    drDeltaTb: Math.max(
      0,
      toNumber(body[`${providerKey}StorageDrDeltaTb`], 0)
    ),
    objectTb: Math.max(
      0,
      toNumber(body[`${providerKey}StorageObjectTb`], 0)
    ),
    fileTb: Math.max(0, toNumber(body[`${providerKey}StorageFileTb`], 0)),
    tableTb: Math.max(
      0,
      toNumber(body[`${providerKey}StorageTableTb`], 0)
    ),
    queueTb: Math.max(
      0,
      toNumber(body[`${providerKey}StorageQueueTb`], 0)
    ),
  });
  const awsStorageProfile = storageProfileInput("aws");
  const azureStorageProfile = storageProfileInput("azure");
  const gcpStorageProfile = storageProfileInput("gcp");
  const awsObjectStorageRate = Math.max(
    0,
    toNumber(body.awsObjectStorageRate, 0)
  );
  const azureObjectStorageRate = Math.max(
    0,
    toNumber(body.azureObjectStorageRate, 0)
  );
  const gcpObjectStorageRate = Math.max(
    0,
    toNumber(body.gcpObjectStorageRate, 0)
  );
  const snapshotMultiplier =
    1 +
    Math.max(0, BACKUP_RETENTION_DAYS - 1) *
      (BACKUP_DAILY_DELTA_PERCENT / 100);
  const snapshotBaseGb = mode === "k8s" ? osDiskGb : storageGb;
  const snapshotGb = backupEnabled ? snapshotBaseGb * snapshotMultiplier : 0;
  const egressTb =
    pricingFocus === "storage" ? 0 : Math.max(0, toNumber(body.egressTb, 0));
  const interVlanTb =
    pricingFocus === "network" ? Math.max(0, toNumber(body.interVlanTb, 0)) : 0;
  const intraVlanTb =
    pricingFocus === "network" ? Math.max(0, toNumber(body.intraVlanTb, 0)) : 0;
  const interRegionTb =
    pricingFocus === "network" ? Math.max(0, toNumber(body.interRegionTb, 0)) : 0;
  const storageIops =
    pricingFocus === "storage" ? Math.max(0, toNumber(body.storageIops, 0)) : 0;
  const storageThroughputMbps =
    pricingFocus === "storage"
      ? Math.max(0, toNumber(body.storageThroughputMbps, 0))
      : 0;
  const egressGb = egressTb * 1024;
  const interVlanGb = interVlanTb * 1024;
  const intraVlanGb = intraVlanTb * 1024;
  const interRegionGb = interRegionTb * 1024;
  const hours = Math.max(1, toNumber(body.hours, HOURS_IN_MONTH));
  const drPercent =
    pricingFocus === "all" ? Math.max(0, toNumber(body.drPercent, 0)) : 0;
  const vmCountMin = mode === "k8s" ? K8S_MIN_NODE_COUNT : 1;
  const vmCount = Math.max(
    vmCountMin,
    Math.round(toNumber(body.vmCount, vmCountMin))
  );
  const egressScale =
    pricingFocus === "network" ? 1 : mode === "vm" ? vmCount : 1;
  const osScale = vmCount;
  const dataScale =
    pricingFocus === "storage" ? 1 : mode === "k8s" ? 1 : vmCount;
  const workloadKey =
    mode === "vm" && body.workload in VM_WORKLOADS
      ? body.workload
      : "general";
  const workloadConfig = VM_WORKLOADS[workloadKey];
  const allowedFlavors =
    mode === "k8s" ? K8S_FLAVORS : workloadConfig.flavors;
  let pricingProvider = resolvePricingProvider(body);
  if (pricingFocus === "network" || pricingFocus === "storage") {
    pricingProvider = "api";
  }
  const diskTierKey =
    typeof body.diskTier === "string" &&
    Object.prototype.hasOwnProperty.call(DISK_TIERS, body.diskTier)
      ? body.diskTier
      : DEFAULT_DISK_TIER;
  const diskTier = DISK_TIERS[diskTierKey] || DISK_TIERS[DEFAULT_DISK_TIER];
  const regionKey = body.regionKey in REGION_MAP ? body.regionKey : "us-east";
  const sqlEdition =
    mode === "k8s" || pricingFocus !== "all"
      ? "none"
      : normalizeSqlEdition(body.sqlEdition);
  const awsSqlPricingEdition = "none";
  const os = mode === "k8s" ? "linux" : "windows";
  const awsReservedType = "convertible";
  const sqlLicenseRate =
    mode === "k8s"
      ? 0
      : sqlEdition === "none"
      ? 0
      : toNumber(body.sqlLicenseRate, SQL_LICENSE_RATES[sqlEdition]);
  const privateEnabled =
    pricingFocus === "all" ? toBoolean(body.privateEnabled) : false;
  const privateVmwareMonthly = Math.max(
    0,
    toNumber(body.privateVmwareMonthly, 0)
  );
  const privateWindowsLicenseMonthly = Math.max(
    0,
    toNumber(body.privateWindowsLicenseMonthly, 0)
  );
  const privateNodeCount = Math.max(
    2,
    Math.round(toNumber(body.privateNodeCount, 2))
  );
  const privateNodeCpu = Math.max(0, toNumber(body.privateNodeCpu, 0));
  const privateNodeRam = Math.max(0, toNumber(body.privateNodeRam, 0));
  const privateNodeStorageTb = Math.max(
    0,
    toNumber(body.privateNodeStorageTb, 0)
  );
  const privateNodeVcpuCapacity = privateNodeCpu > 0 ? privateNodeCpu : 0;
  const privateVmOsDiskGb = Math.max(
    1,
    toNumber(body.privateVmOsDiskGb, osDiskGb)
  );
  const privateVmMemoryOverride = toNumber(body.privateVmMemory, null);
  const privateSanUsableTb = Math.max(
    0,
    toNumber(body.privateSanUsableTb, 0)
  );
  const privateSanTotalMonthly = Math.max(
    0,
    toNumber(body.privateSanTotalMonthly, 0)
  );
  let privateStoragePerTb = Math.max(
    0,
    toNumber(body.privateStoragePerTb, 0)
  );
  if (
    privateStoragePerTb === 0 &&
    privateSanUsableTb > 0 &&
    privateSanTotalMonthly > 0
  ) {
    privateStoragePerTb = privateSanTotalMonthly / privateSanUsableTb;
  }
  const privateNetworkMonthly = Math.max(
    0,
    toNumber(body.privateNetworkMonthly, 0)
  );
  const privateFirewallMonthly = Math.max(
    0,
    toNumber(body.privateFirewallMonthly, 0)
  );
  const privateLoadBalancerMonthly = Math.max(
    0,
    toNumber(body.privateLoadBalancerMonthly, 0)
  );
  const privateNetworkMonthlyTotal =
    privateNetworkMonthly + privateFirewallMonthly + privateLoadBalancerMonthly;
  const privateStorageRate =
    privateStoragePerTb > 0 ? privateStoragePerTb / 1024 : 0;
  let privateHourlyRate = null;
  let privateVmPerNode = 1;
  let privateCapacityNote = null;

  const sizeConstraints = {
    minCpu: MIN_CPU,
    minMemory: MIN_MEMORY,
    minNetworkGbps: MIN_NETWORK_GBPS,
    requireNetwork: true,
  };
  const region = REGION_MAP[regionKey];
  const privateRegion = { location: "Private DC" };
  const regionPayload = { ...region, private: privateRegion };
  const logContext = {
    requestId,
    mode,
    pricingProvider,
    regionKey,
    cpu,
    awsInstanceType,
    azureInstanceType,
    gcpInstanceType,
  };
  let sharedStorageRates = K8S_SHARED_STORAGE_DEFAULT_RATES;
  let sharedStorageSources = null;
  if (mode === "k8s") {
    const sharedStorage = await resolveK8sSharedStorageRates(region);
    sharedStorageRates = sharedStorage.rates;
    sharedStorageSources = sharedStorage.sources;
  }
  let storageFocusRates = null;
  let storageFocusSources = null;
  if (pricingFocus === "storage") {
    const resolved = await resolveStorageFocusRates(region);
    storageFocusRates = resolved.rates;
    storageFocusSources = resolved.sources;
  }
  const awsNetworkSelections =
    pricingFocus === "network"
      ? {
          vpc: awsNetworkVpc,
          gateway: awsNetworkGateway,
          loadBalancer: awsNetworkLoadBalancer,
        }
      : {
          vpc: { flavor: awsVpcFlavor, count: 1, dataTb: 0 },
          firewall: { flavor: awsFirewallFlavor, count: 1, dataTb: 0 },
          loadBalancer: {
            flavor: awsLoadBalancerFlavor,
            count: 1,
            dataTb: 0,
          },
        };
  const azureNetworkSelections =
    pricingFocus === "network"
      ? {
          vpc: azureNetworkVpc,
          gateway: azureNetworkGateway,
          loadBalancer: azureNetworkLoadBalancer,
        }
      : {
          vpc: { flavor: azureVpcFlavor, count: 1, dataTb: 0 },
          firewall: { flavor: azureFirewallFlavor, count: 1, dataTb: 0 },
          loadBalancer: {
            flavor: azureLoadBalancerFlavor,
            count: 1,
            dataTb: 0,
          },
        };
  const gcpNetworkSelections =
    pricingFocus === "network"
      ? {
          vpc: gcpNetworkVpc,
          gateway: gcpNetworkGateway,
          loadBalancer: gcpNetworkLoadBalancer,
        }
      : {
          vpc: { flavor: gcpVpcFlavor, count: 1, dataTb: 0 },
          firewall: { flavor: gcpFirewallFlavor, count: 1, dataTb: 0 },
          loadBalancer: {
            flavor: gcpLoadBalancerFlavor,
            count: 1,
            dataTb: 0,
          },
        };
  const [awsNetworkAddonsRaw, azureNetworkAddonsRaw, gcpNetworkAddonsRaw] =
    await Promise.all([
    resolveNetworkAddonsForProvider({
      providerKey: "aws",
      region,
      selections: awsNetworkSelections,
      hours,
    }),
    resolveNetworkAddonsForProvider({
      providerKey: "azure",
      region,
      selections: azureNetworkSelections,
      hours,
    }),
    resolveNetworkAddonsForProvider({
      providerKey: "gcp",
      region,
      selections: gcpNetworkSelections,
      hours,
    }),
  ]);
  const awsNetworkAddons = awsNetworkAddonsRaw;
  const azureNetworkAddons = azureNetworkAddonsRaw;
  const gcpNetworkAddons = gcpNetworkAddonsRaw;
  const awsInterVlanMonthly =
    interVlanGb * NETWORK_TRAFFIC_RATES.aws.interVlan;
  const azureInterVlanMonthly =
    interVlanGb * NETWORK_TRAFFIC_RATES.azure.interVlan;
  const gcpInterVlanMonthly =
    interVlanGb * NETWORK_TRAFFIC_RATES.gcp.interVlan;
  const awsIntraVlanMonthly =
    intraVlanGb * NETWORK_TRAFFIC_RATES.aws.intraVlan;
  const azureIntraVlanMonthly =
    intraVlanGb * NETWORK_TRAFFIC_RATES.azure.intraVlan;
  const gcpIntraVlanMonthly =
    intraVlanGb * NETWORK_TRAFFIC_RATES.gcp.intraVlan;
  const awsInterRegionMonthly =
    interRegionGb * NETWORK_TRAFFIC_RATES.aws.interRegion;
  const azureInterRegionMonthly =
    interRegionGb * NETWORK_TRAFFIC_RATES.azure.interRegion;
  const gcpInterRegionMonthly =
    interRegionGb * NETWORK_TRAFFIC_RATES.gcp.interRegion;
  const awsIopsMonthly =
    storageIops * STORAGE_PERFORMANCE_RATES.aws.iopsMonthly;
  const azureIopsMonthly =
    storageIops * STORAGE_PERFORMANCE_RATES.azure.iopsMonthly;
  const gcpIopsMonthly =
    storageIops * STORAGE_PERFORMANCE_RATES.gcp.iopsMonthly;
  const awsThroughputMonthly =
    storageThroughputMbps *
    STORAGE_PERFORMANCE_RATES.aws.throughputMonthly;
  const azureThroughputMonthly =
    storageThroughputMbps *
    STORAGE_PERFORMANCE_RATES.azure.throughputMonthly;
  const gcpThroughputMonthly =
    storageThroughputMbps *
    STORAGE_PERFORMANCE_RATES.gcp.throughputMonthly;

  const awsSizes = collectProviderSizes(
    AWS_FAMILIES,
    allowedFlavors.aws,
    sizeConstraints
  );
  const azureSizes = collectProviderSizes(
    AZURE_FAMILIES,
    allowedFlavors.azure,
    sizeConstraints
  );
  const awsSelection = selectSizeByTypeOrCpu(
    awsSizes,
    awsInstanceType,
    cpu
  );
  const azureSelection = selectSizeByTypeOrCpu(
    azureSizes,
    azureInstanceType,
    cpu
  );
  const awsSize = awsSelection.size;
  const azureSize = azureSelection.size;

  if (!awsSize || !azureSize) {
    res.status(400).json({
      error:
        `No instance sizes meet the ${
          mode === "k8s" ? "Linux" : "Windows"
        }, premium disk, and network constraints.`,
    });
    return;
  }

  const awsFamily = AWS_FAMILIES[awsSize.flavorKey];
  const azureFamily = AZURE_FAMILIES[azureSize.flavorKey];

  const sizeNotes = [];
  const addSelectionNote = (providerLabel, requestedType, selection) => {
    if (!selection?.size) {
      return;
    }
    if (requestedType && selection.reason === "fallback") {
      sizeNotes.push(
        `${providerLabel} instance ${requestedType} not available for ${cpu} vCPU; using ${selection.size.type}.`
      );
      return;
    }
    if (selection.size.vcpu !== cpu) {
      sizeNotes.push(
        `${providerLabel} capped at ${selection.size.vcpu} vCPU (request was ${cpu} vCPU).`
      );
    }
  };
  addSelectionNote("AWS", awsInstanceType, awsSelection);
  addSelectionNote("Azure", azureInstanceType, azureSelection);

  const isPublicOnlyFocus =
    pricingFocus === "network" || pricingFocus === "storage";
  const useApiPricing = isPublicOnlyFocus ? true : pricingProvider === "api";

  const awsResponse = {
    status: useApiPricing ? "api" : "retail",
    message: null,
    instance: awsSize,
    hourlyRate: null,
    source: useApiPricing ? "aws-pricing-api" : "vantage",
  };

  if (isPublicOnlyFocus) {
    awsResponse.hourlyRate = 0;
    awsResponse.source = "focus-mode";
    awsResponse.message =
      "Compute excluded in networking/storage focus; pricing includes selected focus components only.";
  } else if (useApiPricing) {
    if (!hasAwsApiCredentials()) {
      logPricingWarning(
        "aws",
        { ...logContext, instanceType: awsSize.type },
        "Missing AWS credentials."
      );
      awsResponse.status = "error";
      awsResponse.message =
        "AWS API key missing. Add AWS vendor credentials in Admin -> Vendor onboarding (optionally set AWS_PRICING_ACCOUNT_ID), or use AWS_DEFAULT_ACCESS_KEY_ID/AWS_DEFAULT_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or AWS_PROFILE.";
    } else {
      try {
        const rate = await getAwsOnDemandPrice({
          instanceType: awsSize.type,
          location: region.aws.location,
          os,
          sqlEdition: awsSqlPricingEdition,
          logContext,
        });
        awsResponse.hourlyRate = rate;
      } catch (error) {
        logPricingError(
          "aws",
          {
            ...logContext,
            instanceType: awsSize.type,
            location: region.aws.location,
            os,
            sqlEdition: awsSqlPricingEdition,
          },
          error
        );
        try {
          const fallbackRate = await getAwsPriceListOnDemandRate({
            instanceType: awsSize.type,
            region: region.aws.region,
            location: region.aws.location,
            os,
            sqlEdition: awsSqlPricingEdition,
            logContext,
          });
          awsResponse.hourlyRate = fallbackRate;
          awsResponse.source = "aws-price-list";
          awsResponse.message =
            "AWS pricing API missing terms; using AWS price list.";
        } catch (fallbackError) {
          logPricingError(
            "aws",
            {
              ...logContext,
              instanceType: awsSize.type,
              region: region.aws.region,
              os,
              sqlEdition: awsSqlPricingEdition,
              source: "price-list",
            },
            fallbackError
          );
          awsResponse.status = "error";
          awsResponse.message =
            fallbackError?.message || "AWS pricing lookup failed.";
        }
      }
      await reconcileAwsApiRate({
        awsResponse,
        awsSize,
        region,
        os,
        sqlEdition: awsSqlPricingEdition,
        logContext,
      });
    }
  } else {
    try {
      const rate = await getAwsPublicPrice({
        instanceType: awsSize.type,
        region: region.aws.region,
        os,
        sqlEdition: awsSqlPricingEdition,
      });
      awsResponse.hourlyRate = rate;
      awsResponse.message =
        "Using public AWS pricing snapshot (instances.vantage.sh).";
    } catch (error) {
      logPricingError(
        "aws",
        {
          ...logContext,
          instanceType: awsSize.type,
          region: region.aws.region,
          os,
          sqlEdition: awsSqlPricingEdition,
          source: "public-snapshot",
        },
        error
      );
      awsResponse.status = "error";
      awsResponse.message =
        error?.message || "AWS public pricing lookup failed.";
    }
  }

  const azureResponse = {
    status: useApiPricing ? "api" : "retail",
    message: null,
    instance: azureSize,
    hourlyRate: null,
    source: useApiPricing ? "azure-retail-api" : "vantage",
  };

  if (isPublicOnlyFocus) {
    azureResponse.hourlyRate = 0;
    azureResponse.source = "focus-mode";
    azureResponse.message =
      "Compute excluded in networking/storage focus; pricing includes selected focus components only.";
  } else if (useApiPricing) {
    try {
      const rate = await getAzureOnDemandPrice({
        skuName: azureSize.type,
        region: region.azure.region,
        os,
      });
      azureResponse.hourlyRate = rate;
    } catch (error) {
      logPricingError(
        "azure",
        {
          ...logContext,
          instanceType: azureSize.type,
          region: region.azure.region,
          os,
        },
        error
      );
      azureResponse.status = "error";
      azureResponse.message =
        error?.message || "Azure pricing lookup failed.";
    }
  } else {
    try {
      const rate = await getAzurePublicPrice({
        skuName: azureSize.type,
        region: region.azure.region,
        os,
      });
      azureResponse.hourlyRate = rate;
      azureResponse.message =
        "Using Azure public pricing snapshot (instances.vantage.sh/azure).";
    } catch (error) {
      logPricingError(
        "azure",
        {
          ...logContext,
          instanceType: azureSize.type,
          region: region.azure.region,
          os,
          source: "public-snapshot",
        },
        error
      );
      azureResponse.status = "error";
      azureResponse.message =
        error?.message || "Azure public pricing lookup failed.";
    }
  }

  let awsReserved1Rate = null;
  let awsReserved3Rate = null;
  let awsReservationNote = null;
  let azureReserved1Rate = null;
  let azureReserved3Rate = null;
  let azureReservationNote = null;

  if (!isPublicOnlyFocus) {
    try {
      awsReserved1Rate = await getAwsPublicReservedPrice({
        instanceType: awsSize.type,
        region: region.aws.region,
        os,
        sqlEdition: awsSqlPricingEdition,
        termYears: 1,
        reservedType: awsReservedType,
      });
      awsReservationNote =
        `AWS reserved pricing uses ${awsReservedType} no-upfront rates from the public snapshot.`;
    } catch (error) {
      awsReserved1Rate = null;
    }

    try {
      awsReserved3Rate = await getAwsPublicReservedPrice({
        instanceType: awsSize.type,
        region: region.aws.region,
        os,
        sqlEdition: awsSqlPricingEdition,
        termYears: 3,
        reservedType: awsReservedType,
      });
      awsReservationNote =
        `AWS reserved pricing uses ${awsReservedType} no-upfront rates from the public snapshot.`;
    } catch (error) {
      awsReserved3Rate = null;
    }

    if (useApiPricing) {
      const reservedRates = [awsReserved1Rate, awsReserved3Rate].filter(
        (rate) => Number.isFinite(rate)
      );
      const minReserved = reservedRates.length
        ? Math.min(...reservedRates)
        : null;
      if (
        Number.isFinite(minReserved) &&
        Number.isFinite(awsResponse.hourlyRate) &&
        awsResponse.hourlyRate < minReserved
      ) {
        try {
          const priceListRate = await getAwsPriceListOnDemandRate({
            instanceType: awsSize.type,
            region: region.aws.region,
            location: region.aws.location,
            os,
            sqlEdition: awsSqlPricingEdition,
            logContext,
          });
          if (
            Number.isFinite(priceListRate) &&
            priceListRate >= minReserved
          ) {
            logPricingWarning(
              "aws",
              {
                ...logContext,
                apiRate: awsResponse.hourlyRate,
                priceListRate,
                minReserved,
              },
              "AWS API rate below reserved; using price list for consistency."
            );
            awsResponse.hourlyRate = priceListRate;
            awsResponse.source = "aws-price-list";
            awsResponse.message =
              "AWS API rate below reserved; using AWS price list for consistency.";
          }
        } catch (error) {
          logPricingError(
            "aws",
            {
              ...logContext,
              instanceType: awsSize.type,
              region: region.aws.region,
              os,
              sqlEdition: awsSqlPricingEdition,
              source: "price-list",
            },
            error
          );
        }
      }
    }

    try {
      if (useApiPricing) {
        const result = await getAzureReservedPrice({
          skuName: azureSize.type,
          region: region.azure.region,
          os,
          termYears: 1,
        });
        azureReserved1Rate = result.hourlyRate;
        if (result.note) {
          azureReservationNote = result.note;
        }
      } else {
        azureReserved1Rate = await getAzurePublicReservedPrice({
          skuName: azureSize.type,
          region: region.azure.region,
          os,
          termYears: 1,
        });
        azureReservationNote =
          "Azure reserved pricing uses monthly (no-upfront) effective rates from the public snapshot.";
      }
    } catch (error) {
      azureReservationNote = null;
    }

    try {
      if (useApiPricing) {
        const result = await getAzureReservedPrice({
          skuName: azureSize.type,
          region: region.azure.region,
          os,
          termYears: 3,
        });
        azureReserved3Rate = result.hourlyRate;
        if (result.note && !azureReservationNote) {
          azureReservationNote = result.note;
        }
      } else {
        azureReserved3Rate = await getAzurePublicReservedPrice({
          skuName: azureSize.type,
          region: region.azure.region,
          os,
          termYears: 3,
        });
        azureReservationNote =
          azureReservationNote ||
          "Azure reserved pricing uses monthly (no-upfront) effective rates from the public snapshot.";
      }
    } catch (error) {
      azureReservationNote = azureReservationNote || null;
    }
  }

  const gcpResponse = {
    status: useApiPricing ? "api" : "retail",
    message: null,
    instance: null,
    hourlyRate: null,
    source: useApiPricing ? "cloud-api" : "vantage",
  };

  let gcpSize = null;
  let gcpSelection = null;
  if (isPublicOnlyFocus) {
    try {
      const sizeResult = await getGcpOnDemandPrice({
        flavorKeys: allowedFlavors.gcp,
        instanceType: gcpInstanceType,
        cpu,
        region: region.gcp.region,
        os,
        requirePricing: false,
      });
      gcpSize = sizeResult.size;
      gcpSelection = sizeResult.selection;
      gcpResponse.instance = sizeResult.size;
      gcpResponse.hourlyRate = 0;
      gcpResponse.source = "focus-mode";
      gcpResponse.message =
        "Compute excluded in networking/storage focus; pricing includes selected focus components only.";
    } catch (error) {
      logPricingError(
        "gcp",
        {
          ...logContext,
          instanceType: gcpInstanceType,
          region: region.gcp.region,
          os,
        },
        error
      );
      gcpResponse.status = "error";
      gcpResponse.message =
        error?.message || "GCP instance selection failed.";
    }
  } else if (useApiPricing) {
    if (!hasGcpApiCredentials()) {
      logPricingWarning(
        "gcp",
        { ...logContext, instanceType: gcpInstanceType || null },
        "Missing GCP API key."
      );
      gcpResponse.status = "error";
      gcpResponse.message =
        "GCP API key missing. Set GCP_PRICING_API_KEY.";
    } else {
      try {
        const sizeResult = await getGcpOnDemandPrice({
          flavorKeys: allowedFlavors.gcp,
          instanceType: gcpInstanceType,
          cpu,
          region: region.gcp.region,
          os,
          requirePricing: false,
        });
        gcpSize = sizeResult.size;
        gcpSelection = sizeResult.selection;
        gcpResponse.instance = sizeResult.size;
        const apiResult = await getGcpApiOnDemandPrice({
          instanceType: gcpSize.type,
          vcpu: gcpSize.vcpu,
          memory: gcpSize.memory,
          region: region.gcp.region,
          os,
          apiKey: getGcpApiKey(),
        });
        gcpResponse.hourlyRate = apiResult.rate;
        gcpResponse.source = apiResult.source;
      } catch (error) {
        logPricingError(
          "gcp",
          {
            ...logContext,
            instanceType: gcpInstanceType,
            region: region.gcp.region,
            os,
          },
          error
        );
        gcpResponse.status = "error";
        gcpResponse.message =
          error?.message || "GCP pricing lookup failed.";
      }
    }
  } else {
    try {
      const result = await getGcpOnDemandPrice({
        flavorKeys: allowedFlavors.gcp,
        instanceType: gcpInstanceType,
        cpu,
        region: region.gcp.region,
        os,
      });
      gcpSize = result.size;
      gcpSelection = result.selection;
      gcpResponse.instance = result.size;
      gcpResponse.hourlyRate = result.rate;
      gcpResponse.message =
        "Using GCP public pricing snapshot (instances.vantage.sh/gcp).";
    } catch (error) {
      logPricingError(
        "gcp",
        {
          ...logContext,
          instanceType: gcpInstanceType,
          region: region.gcp.region,
          os,
          source: "public-snapshot",
        },
        error
      );
      gcpResponse.status = "error";
      gcpResponse.message =
        error?.message || "GCP pricing lookup failed.";
    }
  }
  addSelectionNote("GCP", gcpInstanceType, gcpSelection);

  const privateInstance = {
    type: "Private custom",
    vcpu: cpu,
    memory:
      Number.isFinite(privateVmMemoryOverride)
        ? privateVmMemoryOverride
        : awsSize?.memory ?? azureSize?.memory ?? gcpSize?.memory ?? null,
    networkGbps: null,
    networkLabel: "Custom",
    localDisk: false,
  };
  const privateVmMemory = privateInstance.memory;
  const privateCapacityInputs =
    privateNodeCpu > 0 || privateNodeRam > 0 || privateNodeStorageTb > 0;
  if (privateEnabled) {
    const capacityLimits = [];
    if (privateNodeVcpuCapacity > 0 && cpu > 0) {
      capacityLimits.push(
        Math.floor(privateNodeVcpuCapacity / cpu)
      );
    }
    if (
      privateNodeRam > 0 &&
      Number.isFinite(privateVmMemory) &&
      privateVmMemory > 0
    ) {
      capacityLimits.push(Math.floor(privateNodeRam / privateVmMemory));
    }
    const nodeStorageGb =
      privateNodeStorageTb > 0 ? privateNodeStorageTb * 1024 : 0;
    if (nodeStorageGb > 0 && privateVmOsDiskGb > 0) {
      capacityLimits.push(
        Math.floor(nodeStorageGb / privateVmOsDiskGb)
      );
    }
    if (capacityLimits.length) {
      const minCapacity = Math.min(...capacityLimits);
      if (Number.isFinite(minCapacity) && minCapacity > 0) {
        privateVmPerNode = minCapacity;
      }
    }
    const privateClusterVmwareMonthly =
      privateVmwareMonthly * privateNodeCount;
    const privateUsableNodes = Math.max(privateNodeCount - 1, 1);
    const privateClusterVmCapacity = privateVmPerNode * privateUsableNodes;
    privateHourlyRate =
      privateClusterVmCapacity > 0
        ? privateClusterVmwareMonthly / hours / privateClusterVmCapacity
        : null;
    if (privateCapacityInputs) {
      privateCapacityNote = `Compute assumes ${privateVmPerNode} VM${
        privateVmPerNode === 1 ? "" : "s"
      } per node, ${privateNodeCount} nodes with N+1 spare (${privateUsableNodes} usable).`;
    }
  }
  const privateNetworkItems = [];
  if (privateNetworkMonthly > 0) {
    privateNetworkItems.push({ key: "network", label: "Network" });
  }
  if (privateFirewallMonthly > 0) {
    privateNetworkItems.push({ key: "firewall", label: "Firewall" });
  }
  if (privateLoadBalancerMonthly > 0) {
    privateNetworkItems.push({ key: "loadBalancer", label: "Load balancer" });
  }
  const privateNetworkAddons = {
    items: privateNetworkItems,
    monthlyTotal: privateNetworkMonthlyTotal,
    note:
      privateEnabled && privateNetworkItems.length
        ? "Manual network inputs."
        : null,
  };

  let dataStorageRates =
    mode === "k8s" ? sharedStorageRates : diskTier.storageRates;
  if (pricingFocus === "storage") {
    dataStorageRates = {
      aws: awsObjectStorageRate / 1024,
      azure: azureObjectStorageRate / 1024,
      gcp: gcpObjectStorageRate / 1024,
    };
  }
  const storageRates =
    pricingFocus === "storage" ? dataStorageRates : diskTier.storageRates;
  const awsControlPlaneMonthly =
    mode === "k8s" ? K8S_CONTROL_PLANE_HOURLY.aws * hours : 0;
  const azureControlPlaneMonthly =
    mode === "k8s" ? K8S_CONTROL_PLANE_HOURLY.azure * hours : 0;
  const gcpControlPlaneMonthly =
    mode === "k8s" ? K8S_CONTROL_PLANE_HOURLY.gcp * hours : 0;

  let awsTotals = applyPricingFocusToTotals(
    computeTotalsOrNull({
      hourlyRate: awsResponse.hourlyRate,
      osDiskGb,
      dataDiskGb,
      snapshotGb,
      egressGb,
      hours,
      storageRate: storageRates.aws,
      dataStorageRate: dataStorageRates.aws,
      snapshotRate: diskTier.snapshotRates.aws,
      egressRate: EGRESS_RATES.aws,
      networkMonthly: awsNetworkAddons.monthlyTotal,
      interVlanMonthly: awsInterVlanMonthly,
      intraVlanMonthly: awsIntraVlanMonthly,
      interRegionMonthly: awsInterRegionMonthly,
      iopsMonthly: awsIopsMonthly,
      throughputMonthly: awsThroughputMonthly,
      sqlLicenseRate,
      vcpu: awsSize.vcpu,
      drPercent,
      vmCount,
      controlPlaneMonthly: awsControlPlaneMonthly,
      egressScale,
      osScale,
      dataScale,
    }),
    pricingFocus
  );

  let azureTotals = applyPricingFocusToTotals(
    computeTotalsOrNull({
      hourlyRate: azureResponse.hourlyRate,
      osDiskGb,
      dataDiskGb,
      snapshotGb,
      egressGb,
      hours,
      storageRate: storageRates.azure,
      dataStorageRate: dataStorageRates.azure,
      snapshotRate: diskTier.snapshotRates.azure,
      egressRate: EGRESS_RATES.azure,
      networkMonthly: azureNetworkAddons.monthlyTotal,
      interVlanMonthly: azureInterVlanMonthly,
      intraVlanMonthly: azureIntraVlanMonthly,
      interRegionMonthly: azureInterRegionMonthly,
      iopsMonthly: azureIopsMonthly,
      throughputMonthly: azureThroughputMonthly,
      sqlLicenseRate,
      vcpu: azureSize.vcpu,
      drPercent,
      vmCount,
      controlPlaneMonthly: azureControlPlaneMonthly,
      egressScale,
      osScale,
      dataScale,
    }),
    pricingFocus
  );

  let awsReserved1Totals = applyPricingFocusToTotals(
    computeTotalsOrNull({
      hourlyRate: awsReserved1Rate,
      osDiskGb,
      dataDiskGb,
      snapshotGb,
      egressGb,
      hours,
      storageRate: storageRates.aws,
      dataStorageRate: dataStorageRates.aws,
      snapshotRate: diskTier.snapshotRates.aws,
      egressRate: EGRESS_RATES.aws,
      networkMonthly: awsNetworkAddons.monthlyTotal,
      interVlanMonthly: awsInterVlanMonthly,
      intraVlanMonthly: awsIntraVlanMonthly,
      interRegionMonthly: awsInterRegionMonthly,
      iopsMonthly: awsIopsMonthly,
      throughputMonthly: awsThroughputMonthly,
      sqlLicenseRate,
      vcpu: awsSize.vcpu,
      drPercent,
      vmCount,
      controlPlaneMonthly: awsControlPlaneMonthly,
      egressScale,
      osScale,
      dataScale,
    }),
    pricingFocus
  );

  let awsReserved3Totals = applyPricingFocusToTotals(
    computeTotalsOrNull({
      hourlyRate: awsReserved3Rate,
      osDiskGb,
      dataDiskGb,
      snapshotGb,
      egressGb,
      hours,
      storageRate: storageRates.aws,
      dataStorageRate: dataStorageRates.aws,
      snapshotRate: diskTier.snapshotRates.aws,
      egressRate: EGRESS_RATES.aws,
      networkMonthly: awsNetworkAddons.monthlyTotal,
      interVlanMonthly: awsInterVlanMonthly,
      intraVlanMonthly: awsIntraVlanMonthly,
      interRegionMonthly: awsInterRegionMonthly,
      iopsMonthly: awsIopsMonthly,
      throughputMonthly: awsThroughputMonthly,
      sqlLicenseRate,
      vcpu: awsSize.vcpu,
      drPercent,
      vmCount,
      controlPlaneMonthly: awsControlPlaneMonthly,
      egressScale,
      osScale,
      dataScale,
    }),
    pricingFocus
  );

  let azureReserved1Totals = applyPricingFocusToTotals(
    computeTotalsOrNull({
      hourlyRate: azureReserved1Rate,
      osDiskGb,
      dataDiskGb,
      snapshotGb,
      egressGb,
      hours,
      storageRate: storageRates.azure,
      dataStorageRate: dataStorageRates.azure,
      snapshotRate: diskTier.snapshotRates.azure,
      egressRate: EGRESS_RATES.azure,
      networkMonthly: azureNetworkAddons.monthlyTotal,
      interVlanMonthly: azureInterVlanMonthly,
      intraVlanMonthly: azureIntraVlanMonthly,
      interRegionMonthly: azureInterRegionMonthly,
      iopsMonthly: azureIopsMonthly,
      throughputMonthly: azureThroughputMonthly,
      sqlLicenseRate,
      vcpu: azureSize.vcpu,
      drPercent,
      vmCount,
      controlPlaneMonthly: azureControlPlaneMonthly,
      egressScale,
      osScale,
      dataScale,
    }),
    pricingFocus
  );

  let azureReserved3Totals = applyPricingFocusToTotals(
    computeTotalsOrNull({
      hourlyRate: azureReserved3Rate,
      osDiskGb,
      dataDiskGb,
      snapshotGb,
      egressGb,
      hours,
      storageRate: storageRates.azure,
      dataStorageRate: dataStorageRates.azure,
      snapshotRate: diskTier.snapshotRates.azure,
      egressRate: EGRESS_RATES.azure,
      networkMonthly: azureNetworkAddons.monthlyTotal,
      interVlanMonthly: azureInterVlanMonthly,
      intraVlanMonthly: azureIntraVlanMonthly,
      interRegionMonthly: azureInterRegionMonthly,
      iopsMonthly: azureIopsMonthly,
      throughputMonthly: azureThroughputMonthly,
      sqlLicenseRate,
      vcpu: azureSize.vcpu,
      drPercent,
      vmCount,
      controlPlaneMonthly: azureControlPlaneMonthly,
      egressScale,
      osScale,
      dataScale,
    }),
    pricingFocus
  );

  let gcpTotals = applyPricingFocusToTotals(
    computeTotalsOrNull({
      hourlyRate: gcpResponse.hourlyRate,
      osDiskGb,
      dataDiskGb,
      snapshotGb,
      egressGb,
      hours,
      storageRate: storageRates.gcp,
      dataStorageRate: dataStorageRates.gcp,
      snapshotRate: diskTier.snapshotRates.gcp,
      egressRate: EGRESS_RATES.gcp,
      networkMonthly: gcpNetworkAddons.monthlyTotal,
      interVlanMonthly: gcpInterVlanMonthly,
      intraVlanMonthly: gcpIntraVlanMonthly,
      interRegionMonthly: gcpInterRegionMonthly,
      iopsMonthly: gcpIopsMonthly,
      throughputMonthly: gcpThroughputMonthly,
      sqlLicenseRate,
      vcpu: gcpSize?.vcpu || cpu,
      drPercent,
      vmCount,
      controlPlaneMonthly: gcpControlPlaneMonthly,
      egressScale,
      osScale,
      dataScale,
    }),
    pricingFocus
  );

  const privateTotals = privateEnabled
    ? applyPricingFocusToTotals(
        computeTotalsOrNull({
          hourlyRate: privateHourlyRate,
          osDiskGb,
          dataDiskGb,
          snapshotGb,
          egressGb,
          hours,
          storageRate: privateStorageRate,
          dataStorageRate: privateStorageRate,
          snapshotRate: privateStorageRate,
          egressRate: 0,
          networkMonthly: privateNetworkMonthlyTotal,
          interVlanMonthly: 0,
          intraVlanMonthly: 0,
          interRegionMonthly: 0,
          iopsMonthly: 0,
          throughputMonthly: 0,
          sqlLicenseRate,
          windowsLicenseMonthly: privateWindowsLicenseMonthly,
          vcpu: cpu,
          drPercent,
          vmCount,
          controlPlaneMonthly: 0,
          egressScale,
          osScale,
          dataScale,
        }),
        pricingFocus
      )
    : null;

  let awsStorageServices = null;
  let azureStorageServices = null;
  let gcpStorageServices = null;
  if (pricingFocus === "storage" && storageFocusRates) {
    const awsStorage = computeStorageFocusTotals(
      awsStorageProfile,
      storageFocusRates.aws
    );
    const azureStorage = computeStorageFocusTotals(
      azureStorageProfile,
      storageFocusRates.azure
    );
    const gcpStorage = computeStorageFocusTotals(
      gcpStorageProfile,
      storageFocusRates.gcp
    );
    awsTotals = awsStorage.totals;
    azureTotals = azureStorage.totals;
    gcpTotals = gcpStorage.totals;
    awsReserved1Totals = null;
    awsReserved3Totals = null;
    azureReserved1Totals = null;
    azureReserved3Totals = null;
    awsStorageServices = {
      ...awsStorage.breakdown,
      rates: storageFocusRates.aws,
      sources: storageFocusSources?.aws || null,
    };
    azureStorageServices = {
      ...azureStorage.breakdown,
      rates: storageFocusRates.azure,
      sources: storageFocusSources?.azure || null,
    };
    gcpStorageServices = {
      ...gcpStorage.breakdown,
      rates: storageFocusRates.gcp,
      sources: storageFocusSources?.gcp || null,
    };
  }

  const gcpReserved1Totals = null;
  const gcpReserved3Totals = null;
  const privateReserved1Totals = null;
  const privateReserved3Totals = null;
  const azureOnDemandSource =
    useApiPricing ? "azure-retail-consumption" : "public-snapshot";
  const azureReservedSource =
    useApiPricing ? "azure-retail-reservation" : "public-snapshot";
  const constraintsNote =
    pricingFocus === "network"
      ? "Networking focus: public-cloud VPC/VNet, VPC gateway, and load balancer pricing only (with per-component counts and data transfer). Inter-VLAN, intra-VLAN, inter-region transfer, and egress are modeled. Compute, storage, SQL, DR, and private cloud are excluded."
      : pricingFocus === "storage"
      ? "Storage focus: public storage service pricing only (object, file, table, queue) with optional DR replication delta. Compute, networking, SQL, DR uplift, and private cloud are excluded."
      : mode === "k8s"
      ? "Kubernetes mode: node sizing uses VM families. Control plane fees use premium tiers. Linux-only. Minimum node count 3. OS disk minimum 32 GB. Shared data storage uses EFS/Azure Files/Filestore public pricing (cached; falls back to defaults) and is cluster-level. SQL pricing disabled. Disk tier selectable (Premium or Max performance). Optional network add-ons: VPC/VNet, firewall, load balancer. No local or temp disks. Network >= 10 Gbps (GCP network listed as variable). Minimum 8 vCPU and 8 GB RAM."
      : "Windows-only. Disk tier selectable (Premium or Max performance). Optional network add-ons: VPC/VNet, firewall, load balancer. No local or temp disks. Network >= 10 Gbps (GCP network listed as variable). Minimum 8 vCPU and 8 GB RAM.";
  const cacheStatus = buildCacheStatus();
  const cacheWarning =
    cacheStatus.summary.staleCount > 0
      ? `Cache warning: stale cache groups detected (${cacheStatus.summary.staleCaches.join(
          ", "
        )}). Results can fall back to defaults until refresh completes.`
      : null;
  const cacheStatusSummary = cacheStatus.summary;
  const cacheMeta = {
    generatedAt: cacheStatus.generatedAt,
    lastRefreshAt: cacheStatus.refresh.lastRefreshAt,
    refreshStatus: cacheStatus.refresh.status,
    refreshRunning: cacheStatus.refresh.running,
    staleCount: cacheStatus.summary.staleCount,
    staleCaches: cacheStatus.summary.staleCaches,
  };

  res.json({
    input: {
      cpu,
      awsInstanceType: awsSize?.type || null,
      azureInstanceType: azureSize?.type || null,
      gcpInstanceType: gcpSize?.type || null,
      osDiskGb,
      dataDiskTb,
      dataDiskGb,
      snapshotGb,
      storageGb,
      egressTb,
      egressGb,
      interVlanTb,
      intraVlanTb,
      interRegionTb,
      storageIops,
      storageThroughputMbps,
      hours,
      backupEnabled,
      backupRetentionDays: BACKUP_RETENTION_DAYS,
      backupDailyDeltaPercent: BACKUP_DAILY_DELTA_PERCENT,
      drPercent,
      vmCount,
      mode,
      pricingFocus,
      networkAddonFocus,
      workload: workloadKey,
      regionKey,
      sqlEdition,
      os,
      awsReservedType,
      pricingProvider,
      sqlLicenseRate,
      diskTier: diskTierKey,
      diskTierLabel: diskTier.label,
      awsVpcFlavor,
      awsFirewallFlavor,
      awsLoadBalancerFlavor,
      awsNetworkVpcFlavor: awsNetworkVpc.flavor,
      awsNetworkVpcCount: awsNetworkVpc.count,
      awsNetworkVpcDataTb: awsNetworkVpc.dataTb,
      awsNetworkGatewayFlavor: awsNetworkGateway.flavor,
      awsNetworkGatewayCount: awsNetworkGateway.count,
      awsNetworkGatewayDataTb: awsNetworkGateway.dataTb,
      awsNetworkLoadBalancerFlavor: awsNetworkLoadBalancer.flavor,
      awsNetworkLoadBalancerCount: awsNetworkLoadBalancer.count,
      awsNetworkLoadBalancerDataTb: awsNetworkLoadBalancer.dataTb,
      azureVpcFlavor,
      azureFirewallFlavor,
      azureLoadBalancerFlavor,
      azureNetworkVpcFlavor: azureNetworkVpc.flavor,
      azureNetworkVpcCount: azureNetworkVpc.count,
      azureNetworkVpcDataTb: azureNetworkVpc.dataTb,
      azureNetworkGatewayFlavor: azureNetworkGateway.flavor,
      azureNetworkGatewayCount: azureNetworkGateway.count,
      azureNetworkGatewayDataTb: azureNetworkGateway.dataTb,
      azureNetworkLoadBalancerFlavor: azureNetworkLoadBalancer.flavor,
      azureNetworkLoadBalancerCount: azureNetworkLoadBalancer.count,
      azureNetworkLoadBalancerDataTb: azureNetworkLoadBalancer.dataTb,
      gcpVpcFlavor,
      gcpFirewallFlavor,
      gcpLoadBalancerFlavor,
      gcpNetworkVpcFlavor: gcpNetworkVpc.flavor,
      gcpNetworkVpcCount: gcpNetworkVpc.count,
      gcpNetworkVpcDataTb: gcpNetworkVpc.dataTb,
      gcpNetworkGatewayFlavor: gcpNetworkGateway.flavor,
      gcpNetworkGatewayCount: gcpNetworkGateway.count,
      gcpNetworkGatewayDataTb: gcpNetworkGateway.dataTb,
      gcpNetworkLoadBalancerFlavor: gcpNetworkLoadBalancer.flavor,
      gcpNetworkLoadBalancerCount: gcpNetworkLoadBalancer.count,
      gcpNetworkLoadBalancerDataTb: gcpNetworkLoadBalancer.dataTb,
      awsObjectStorageRate,
      azureObjectStorageRate,
      gcpObjectStorageRate,
      awsStorageAccountCount: awsStorageProfile.accountCount,
      awsStorageDrEnabled: awsStorageProfile.drEnabled,
      awsStorageDrDeltaTb: awsStorageProfile.drDeltaTb,
      awsStorageObjectTb: awsStorageProfile.objectTb,
      awsStorageFileTb: awsStorageProfile.fileTb,
      awsStorageTableTb: awsStorageProfile.tableTb,
      awsStorageQueueTb: awsStorageProfile.queueTb,
      azureStorageAccountCount: azureStorageProfile.accountCount,
      azureStorageDrEnabled: azureStorageProfile.drEnabled,
      azureStorageDrDeltaTb: azureStorageProfile.drDeltaTb,
      azureStorageObjectTb: azureStorageProfile.objectTb,
      azureStorageFileTb: azureStorageProfile.fileTb,
      azureStorageTableTb: azureStorageProfile.tableTb,
      azureStorageQueueTb: azureStorageProfile.queueTb,
      gcpStorageAccountCount: gcpStorageProfile.accountCount,
      gcpStorageDrEnabled: gcpStorageProfile.drEnabled,
      gcpStorageDrDeltaTb: gcpStorageProfile.drDeltaTb,
      gcpStorageObjectTb: gcpStorageProfile.objectTb,
      gcpStorageFileTb: gcpStorageProfile.fileTb,
      gcpStorageTableTb: gcpStorageProfile.tableTb,
      gcpStorageQueueTb: gcpStorageProfile.queueTb,
      privateEnabled,
      privateVmwareMonthly,
      privateWindowsLicenseMonthly,
      privateNodeCount,
      privateStoragePerTb,
      privateNetworkMonthly,
      privateFirewallMonthly,
      privateLoadBalancerMonthly,
      privateNetworkMonthlyTotal,
      privateNodeCpu,
      privateNodeRam,
      privateNodeStorageTb,
      privateVmOsDiskGb,
      privateVmMemory: Number.isFinite(privateVmMemoryOverride)
        ? privateVmMemoryOverride
        : null,
      privateSanUsableTb,
      privateSanTotalMonthly,
      privateVmPerNode,
    },
    region: regionPayload,
    aws: {
      ...awsResponse,
      family: awsFamily?.label || "AWS",
      reservationNote: awsReservationNote,
      storage: {
        osDiskGb,
        dataDiskTb,
        dataDiskGb,
        totalGb: storageGb,
      },
      backup: {
        enabled: backupEnabled,
        snapshotGb,
        retentionDays: BACKUP_RETENTION_DAYS,
        dailyDeltaPercent: BACKUP_DAILY_DELTA_PERCENT,
      },
      networkAddons: awsNetworkAddons,
      storageServices: awsStorageServices,
      dr: {
        percent: drPercent,
      },
      totals: awsTotals,
      pricingTiers: {
        onDemand: {
          hourlyRate: awsResponse.hourlyRate,
          totals: awsTotals,
          source: awsResponse.source,
        },
        reserved1yr: {
          hourlyRate: awsReserved1Rate,
          totals: awsReserved1Totals,
          source: "public-snapshot",
          reservedType: awsReservedType,
        },
        reserved3yr: {
          hourlyRate: awsReserved3Rate,
          totals: awsReserved3Totals,
          source: "public-snapshot",
          reservedType: awsReservedType,
        },
      },
      storageRate: diskTier.storageRates.aws,
      snapshotRate: diskTier.snapshotRates.aws,
      egressRate: EGRESS_RATES.aws,
      egress: {
        egressTb,
        egressGb,
      },
      sqlNote:
        mode === "k8s"
          ? null
          : sqlEdition === "none"
          ? "No SQL Server license."
          : "SQL license add-on estimated per vCPU-hour (BYOL).",
    },
    azure: {
      ...azureResponse,
      family: azureFamily?.label || "Azure",
      reservationNote: azureReservationNote,
      storage: {
        osDiskGb,
        dataDiskTb,
        dataDiskGb,
        totalGb: storageGb,
      },
      backup: {
        enabled: backupEnabled,
        snapshotGb,
        retentionDays: BACKUP_RETENTION_DAYS,
        dailyDeltaPercent: BACKUP_DAILY_DELTA_PERCENT,
      },
      networkAddons: azureNetworkAddons,
      storageServices: azureStorageServices,
      dr: {
        percent: drPercent,
      },
      totals: azureTotals,
      pricingTiers: {
        onDemand: {
          hourlyRate: azureResponse.hourlyRate,
          totals: azureTotals,
          source: azureOnDemandSource,
        },
        reserved1yr: {
          hourlyRate: azureReserved1Rate,
          totals: azureReserved1Totals,
          source: azureReservedSource,
        },
        reserved3yr: {
          hourlyRate: azureReserved3Rate,
          totals: azureReserved3Totals,
          source: azureReservedSource,
        },
      },
      storageRate: diskTier.storageRates.azure,
      snapshotRate: diskTier.snapshotRates.azure,
      egressRate: EGRESS_RATES.azure,
      egress: {
        egressTb,
        egressGb,
      },
      sqlNote:
        mode === "k8s"
          ? null
          : sqlEdition === "none"
          ? "No SQL Server license."
          : "SQL license add-on estimated per vCPU-hour.",
    },
    gcp: {
      ...gcpResponse,
      family: GCP_FLAVOR_LABELS[gcpSize?.flavorKey] || "GCP",
      storage: {
        osDiskGb,
        dataDiskTb,
        dataDiskGb,
        totalGb: storageGb,
      },
      backup: {
        enabled: backupEnabled,
        snapshotGb,
        retentionDays: BACKUP_RETENTION_DAYS,
        dailyDeltaPercent: BACKUP_DAILY_DELTA_PERCENT,
      },
      networkAddons: gcpNetworkAddons,
      storageServices: gcpStorageServices,
      dr: {
        percent: drPercent,
      },
      totals: gcpTotals,
      pricingTiers: {
        onDemand: {
          hourlyRate: gcpResponse.hourlyRate,
          totals: gcpTotals,
          source: gcpResponse.source,
        },
        reserved1yr: {
          hourlyRate: null,
          totals: gcpReserved1Totals,
          source: "n/a",
        },
        reserved3yr: {
          hourlyRate: null,
          totals: gcpReserved3Totals,
          source: "n/a",
        },
      },
      storageRate: diskTier.storageRates.gcp,
      snapshotRate: diskTier.snapshotRates.gcp,
      egressRate: EGRESS_RATES.gcp,
      egress: {
        egressTb,
        egressGb,
      },
      sqlNote:
        mode === "k8s"
          ? null
          : sqlEdition === "none"
          ? "No SQL Server license."
          : "SQL license add-on estimated per vCPU-hour.",
    },
    private: {
      status: privateEnabled ? "manual" : "off",
      message: privateEnabled
        ? ["Manual private cloud inputs.", privateCapacityNote]
            .filter(Boolean)
            .join(" ")
        : "Enable private cloud to include manual pricing.",
      enabled: privateEnabled,
      instance: privateInstance,
      hourlyRate: privateHourlyRate,
      vmPerNode: privateVmPerNode,
      windowsLicenseMonthly: privateWindowsLicenseMonthly,
      nodeCount: privateNodeCount,
      source: "manual",
      family: "Private cloud",
      storage: {
        osDiskGb,
        dataDiskTb,
        dataDiskGb,
        totalGb: storageGb,
      },
      backup: {
        enabled: backupEnabled,
        snapshotGb,
        retentionDays: BACKUP_RETENTION_DAYS,
        dailyDeltaPercent: BACKUP_DAILY_DELTA_PERCENT,
      },
      networkAddons: privateNetworkAddons,
      dr: {
        percent: drPercent,
      },
      totals: privateTotals,
      pricingTiers: {
        onDemand: {
          hourlyRate: privateHourlyRate,
          totals: privateTotals,
          source: "manual",
        },
        reserved1yr: {
          hourlyRate: null,
          totals: privateReserved1Totals,
          source: "n/a",
        },
        reserved3yr: {
          hourlyRate: null,
          totals: privateReserved3Totals,
          source: "n/a",
        },
      },
      storageRate: privateStorageRate,
      snapshotRate: privateStorageRate,
      egressRate: 0,
      egress: {
        egressTb,
        egressGb,
      },
      sqlNote:
        mode === "k8s"
          ? null
          : sqlEdition === "none"
          ? "No SQL Server license."
          : "SQL license add-on estimated per vCPU-hour.",
    },
    notes: {
      constraints: constraintsNote,
      sizeCap: sizeNotes.length ? sizeNotes.join(" ") : null,
      sharedStorageSources: mode === "k8s" ? sharedStorageSources : null,
      storageSources: pricingFocus === "storage" ? storageFocusSources : null,
      cacheWarning,
      cacheStatus: cacheStatusSummary,
      cacheMeta,
      apiCredentials: {
        aws: hasAwsApiCredentials(),
        azure: true,
        gcp: hasGcpApiCredentials(),
      },
    },
  });
});

let backgroundWorkersStarted = false;

function startBackgroundWorkers() {
  if (backgroundWorkersStarted) {
    return;
  }
  backgroundWorkersStarted = true;

  if (PRICING_WARMUP_ENABLED) {
    runCacheRefreshCycle("startup").catch((error) => {
      console.error("[pricing] Cache warm-up failed.", error);
    });
    startCacheRefreshLoop();
    console.log(
      `[pricing] Background cache refresh enabled every ${Math.round(
        PRICING_CACHE_REFRESH_INTERVAL_MS / 1000
      )}s.`
    );
  } else {
    console.log("[pricing] Cache warm-up disabled.");
  }
}

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`Cloud price app running at http://localhost:${PORT}`);
    startBackgroundWorkers();
  });
}

module.exports = app;
module.exports.startBackgroundWorkers = startBackgroundWorkers;
