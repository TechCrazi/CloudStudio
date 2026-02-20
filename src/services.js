const path = require('path');

function resolveServiceApp(moduleExport) {
  if (moduleExport && typeof moduleExport === 'object' && typeof moduleExport.app === 'function') {
    return moduleExport.app;
  }
  if (typeof moduleExport === 'function') {
    return moduleExport;
  }
  return null;
}

function createManagedServices(options = {}) {
  const projectRoot = path.resolve(options.rootDir || path.resolve(__dirname, '..'));

  const storageDir = path.resolve(
    options.storageDir ||
      process.env.CLOUDSTUDIO_STORAGE_DIR ||
      path.join(projectRoot, 'src', 'modules', 'storage')
  );

  const priceDir = path.resolve(
    options.priceDir ||
      process.env.CLOUDSTUDIO_PRICE_DIR ||
      path.join(projectRoot, 'src', 'modules', 'pricing')
  );

  const ipAddressDir = path.resolve(
    options.ipAddressDir ||
      process.env.CLOUDSTUDIO_IP_ADDRESS_DIR ||
      path.join(projectRoot, 'src', 'modules', 'ip-address')
  );

  const billingDir = path.resolve(
    options.billingDir ||
      process.env.CLOUDSTUDIO_BILLING_DIR ||
      path.join(projectRoot, 'src', 'modules', 'billing')
  );

  const tagDir = path.resolve(
    options.tagDir ||
      process.env.CLOUDSTUDIO_TAG_DIR ||
      path.join(projectRoot, 'src', 'modules', 'tag')
  );

  const storageModule = require(path.join(storageDir, 'src', 'server.js'));
  const priceModule = require(path.join(priceDir, 'server.js'));
  const ipAddressModule = require(path.join(ipAddressDir, 'server.js'));
  const billingModule = require(path.join(billingDir, 'server.js'));
  const tagModule = require(path.join(tagDir, 'server.js'));

  const storageApp = resolveServiceApp(storageModule);
  const priceApp = resolveServiceApp(priceModule);
  const ipAddressApp = resolveServiceApp(ipAddressModule);
  const billingApp = resolveServiceApp(billingModule);
  const tagApp = resolveServiceApp(tagModule);

  if (!storageApp) {
    throw new Error(`CloudStorageStudio app export not found at ${path.join(storageDir, 'src', 'server.js')}`);
  }
  if (!priceApp) {
    throw new Error(`CloudPriceStudio app export not found at ${path.join(priceDir, 'server.js')}`);
  }
  if (!ipAddressApp) {
    throw new Error(`IP Address app export not found at ${path.join(ipAddressDir, 'server.js')}`);
  }
  if (!billingApp) {
    throw new Error(`Billing app export not found at ${path.join(billingDir, 'server.js')}`);
  }
  if (!tagApp) {
    throw new Error(`Tag app export not found at ${path.join(tagDir, 'server.js')}`);
  }

  const services = {
    storage: {
      name: 'CloudStorageStudio',
      mountPath: '/apps/storage',
      healthPath: '/api/health',
      app: storageApp,
      running: false,
      external: false,
      startedAt: null,
      lastError: null,
      startBackgroundWorkers:
        typeof storageModule.startBackgroundWorkers === 'function'
          ? storageModule.startBackgroundWorkers
          : null
    },
    price: {
      name: 'CloudPriceStudio',
      mountPath: '/apps/pricing',
      healthPath: '/api/options',
      app: priceApp,
      running: false,
      external: false,
      startedAt: null,
      lastError: null,
      startBackgroundWorkers:
        typeof priceModule.startBackgroundWorkers === 'function'
          ? priceModule.startBackgroundWorkers
          : null
    },
    ipAddress: {
      name: 'IP Address',
      mountPath: '/apps/ip-address',
      healthPath: '/api/health',
      app: ipAddressApp,
      running: false,
      external: false,
      startedAt: null,
      lastError: null,
      startBackgroundWorkers:
        typeof ipAddressModule.startBackgroundWorkers === 'function'
          ? ipAddressModule.startBackgroundWorkers
          : null
    },
    billing: {
      name: 'Billing',
      mountPath: '/apps/billing',
      healthPath: '/api/health',
      app: billingApp,
      running: false,
      external: false,
      startedAt: null,
      lastError: null,
      startBackgroundWorkers:
        typeof billingModule.startBackgroundWorkers === 'function'
          ? billingModule.startBackgroundWorkers
          : null
    },
    tag: {
      name: 'Tag',
      mountPath: '/apps/tag',
      healthPath: '/api/health',
      app: tagApp,
      running: false,
      external: false,
      startedAt: null,
      lastError: null,
      startBackgroundWorkers:
        typeof tagModule.startBackgroundWorkers === 'function'
          ? tagModule.startBackgroundWorkers
          : null
    }
  };

  let mounted = false;
  let started = false;

  function mountAll(rootApp) {
    if (mounted) {
      return;
    }

    rootApp.use(services.storage.mountPath, services.storage.app);
    rootApp.use(services.price.mountPath, services.price.app);
    rootApp.use(services.ipAddress.mountPath, services.ipAddress.app);
    rootApp.use(services.billing.mountPath, services.billing.app);
    rootApp.use(services.tag.mountPath, services.tag.app);
    mounted = true;
  }

  async function startAll() {
    if (started) {
      return getStatus();
    }

    const now = new Date().toISOString();

    for (const service of Object.values(services)) {
      try {
        if (service.startBackgroundWorkers) {
          await Promise.resolve(service.startBackgroundWorkers());
        }
        service.running = true;
        service.external = false;
        service.lastError = null;
        service.startedAt = now;
      } catch (error) {
        service.running = false;
        service.external = false;
        service.lastError = error?.message || 'Failed to start service workers.';
        throw error;
      }
    }

    started = true;
    return getStatus();
  }

  function stopAll() {
    // No-op for in-process mounted apps.
  }

  function mapStatus(service) {
    return {
      name: service.name,
      baseUrl: service.mountPath,
      healthUrl: `${service.mountPath}${service.healthPath}`,
      running: service.running,
      external: service.external,
      startedAt: service.startedAt,
      lastError: service.lastError
    };
  }

  function getStatus() {
    return {
      storage: mapStatus(services.storage),
      price: mapStatus(services.price),
      ipAddress: mapStatus(services.ipAddress),
      billing: mapStatus(services.billing),
      tag: mapStatus(services.tag)
    };
  }

  function getServiceBaseUrl(name) {
    if (!services[name]) {
      return null;
    }
    return services[name].mountPath;
  }

  return {
    mountAll,
    startAll,
    stopAll,
    getStatus,
    getServiceBaseUrl
  };
}

module.exports = {
  createManagedServices
};
