const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const {
  S3Client,
  PutObjectCommand,
  GetBucketLifecycleConfigurationCommand,
  PutBucketLifecycleConfigurationCommand
} = require('@aws-sdk/client-s3');
const { db } = require('./db');

function parseBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }
  const text = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(text)) {
    return true;
  }
  if (['0', 'false', 'no', 'off'].includes(text)) {
    return false;
  }
  return fallback;
}

function parseInteger(value, fallback, min = null, max = null) {
  const parsed = Number.parseInt(String(value ?? '').trim(), 10);
  let output = Number.isFinite(parsed) ? parsed : fallback;
  if (Number.isFinite(min)) {
    output = Math.max(min, output);
  }
  if (Number.isFinite(max)) {
    output = Math.min(max, output);
  }
  return output;
}

function normalizePrefix(value) {
  return String(value || '')
    .trim()
    .replace(/^\/+/, '')
    .replace(/\/+$/, '');
}

function isoCompactStamp(date = new Date()) {
  return date
    .toISOString()
    .replace(/[-:]/g, '')
    .replace(/\.\d{3}Z$/, 'Z');
}

function isNoLifecycleConfigError(error) {
  const name = String(error?.name || error?.Code || '').trim();
  const statusCode = Number(error?.$metadata?.httpStatusCode || 0);
  return name === 'NoSuchLifecycleConfiguration' || statusCode === 404;
}

function hasExplicitEnv(name) {
  if (!name) {
    return false;
  }
  const raw = process.env[name];
  return raw !== undefined && String(raw).trim() !== '';
}

function pickEnvValue(names = []) {
  for (const name of names) {
    if (!name) {
      continue;
    }
    if (hasExplicitEnv(name)) {
      return String(process.env[name]).trim();
    }
  }
  return null;
}

function pickString(options = {}) {
  const envValue = pickEnvValue(options.envNames || []);
  if (envValue !== null) {
    return envValue;
  }
  const uiValue = String(options.uiValue || '').trim();
  if (uiValue) {
    return uiValue;
  }
  return String(options.defaultValue || '').trim();
}

function pickBoolean(options = {}) {
  const fallback = Boolean(options.defaultValue);
  const envValue = pickEnvValue(options.envNames || []);
  if (envValue !== null) {
    return parseBoolean(envValue, fallback);
  }
  if (options.uiValue !== undefined && options.uiValue !== null && String(options.uiValue).trim() !== '') {
    return parseBoolean(options.uiValue, fallback);
  }
  return fallback;
}

function pickInteger(options = {}) {
  const fallback = Number(options.defaultValue || 0);
  const min = Number.isFinite(Number(options.min)) ? Number(options.min) : null;
  const max = Number.isFinite(Number(options.max)) ? Number(options.max) : null;
  const envValue = pickEnvValue(options.envNames || []);
  if (envValue !== null) {
    return parseInteger(envValue, fallback, min, max);
  }
  if (options.uiValue !== undefined && options.uiValue !== null && String(options.uiValue).trim() !== '') {
    return parseInteger(options.uiValue, fallback, min, max);
  }
  return parseInteger(fallback, fallback, min, max);
}

function createS3Client(config = {}) {
  return new S3Client({
    region: config.region,
    endpoint: config.endpoint,
    forcePathStyle: Boolean(config.forcePathStyle),
    credentials: {
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
      ...(config.sessionToken ? { sessionToken: config.sessionToken } : {})
    }
  });
}

function createDbBackupScheduler(options = {}) {
  const logger = options.logger || console;
  const getUiConfig = typeof options.getUiConfig === 'function' ? options.getUiConfig : () => ({});

  const state = {
    enabled: false,
    running: false,
    intervalMs: 60 * 60 * 1000,
    retentionDays: 15,
    bucket: null,
    prefix: null,
    region: null,
    endpoint: null,
    forcePathStyle: false,
    tempDir: null,
    runOnStartup: true,
    applyLifecycle: true,
    lifecycleRuleId: 'cloudstudio-db-backup-retention',
    hasAccessKeyId: false,
    hasSecretAccessKey: false,
    hasSessionToken: false,
    nextRunAt: null,
    lastStartedAt: null,
    lastCompletedAt: null,
    lastSuccessAt: null,
    lastDurationMs: null,
    lastReason: null,
    lastStatus: 'idle',
    lastKey: null,
    lastSizeBytes: null,
    lifecycleEnsuredAt: null,
    lastError: null,
    lastLifecycleError: null,
    disabledReason: null
  };

  let timer = null;
  let s3 = null;
  let lifecycleEnsured = false;
  let schedulerStarted = false;
  let currentConfig = null;

  function resolveEffectiveConfig() {
    const ui = getUiConfig() || {};
    const fallbackTempDir = path.resolve(__dirname, '..', 'data', 'tmp-backups');

    const enabled = pickBoolean({
      envNames: ['DB_BACKUP_ENABLED'],
      uiValue: ui.enabled,
      defaultValue: false
    });
    const bucket = pickString({
      envNames: ['DB_BACKUP_BUCKET', 'WASABI_BACKUP_BUCKET'],
      uiValue: ui.bucket,
      defaultValue: ''
    });
    const prefix = normalizePrefix(
      pickString({
        envNames: ['DB_BACKUP_PREFIX'],
        uiValue: ui.prefix,
        defaultValue: 'cloudstudio-db-backups'
      })
    );
    const region =
      pickString({
        envNames: ['DB_BACKUP_REGION', 'WASABI_REGION'],
        uiValue: ui.region,
        defaultValue: 'us-east-1'
      }) || 'us-east-1';
    const endpoint =
      pickString({
        envNames: ['DB_BACKUP_ENDPOINT', 'WASABI_S3_ENDPOINT'],
        uiValue: ui.endpoint,
        defaultValue: 'https://s3.wasabisys.com'
      }) || 'https://s3.wasabisys.com';
    const accessKeyId = pickString({
      envNames: ['DB_BACKUP_ACCESS_KEY_ID', 'WASABI_ACCESS_KEY'],
      uiValue: ui.accessKeyId,
      defaultValue: ''
    });
    const secretAccessKey = pickString({
      envNames: ['DB_BACKUP_SECRET_ACCESS_KEY', 'WASABI_SECRET_KEY'],
      uiValue: ui.secretAccessKey,
      defaultValue: ''
    });
    const sessionToken = pickString({
      envNames: ['DB_BACKUP_SESSION_TOKEN'],
      uiValue: ui.sessionToken,
      defaultValue: ''
    });
    const intervalMs = pickInteger({
      envNames: ['DB_BACKUP_INTERVAL_MS'],
      uiValue: ui.intervalMs,
      defaultValue: 60 * 60 * 1000,
      min: 60 * 1000,
      max: 7 * 24 * 60 * 60 * 1000
    });
    const runOnStartup = pickBoolean({
      envNames: ['DB_BACKUP_RUN_ON_STARTUP'],
      uiValue: ui.runOnStartup,
      defaultValue: true
    });
    const retentionDays = pickInteger({
      envNames: ['DB_BACKUP_RETENTION_DAYS'],
      uiValue: ui.retentionDays,
      defaultValue: 15,
      min: 1,
      max: 3650
    });
    const applyLifecycle = pickBoolean({
      envNames: ['DB_BACKUP_APPLY_LIFECYCLE'],
      uiValue: ui.applyLifecycle,
      defaultValue: true
    });
    const lifecycleRuleId =
      pickString({
        envNames: ['DB_BACKUP_LIFECYCLE_RULE_ID'],
        uiValue: ui.lifecycleRuleId,
        defaultValue: 'cloudstudio-db-backup-retention'
      })
        .slice(0, 255)
        .trim() || 'cloudstudio-db-backup-retention';
    const forcePathStyle = pickBoolean({
      envNames: ['DB_BACKUP_FORCE_PATH_STYLE'],
      uiValue: ui.forcePathStyle,
      defaultValue: false
    });
    const tempDir = path.resolve(
      pickString({
        envNames: ['DB_BACKUP_TMP_DIR'],
        uiValue: ui.tempDir,
        defaultValue: fallbackTempDir
      }) || fallbackTempDir
    );

    const hasCreds = Boolean(accessKeyId && secretAccessKey);
    const isOperational = Boolean(enabled && bucket && hasCreds);

    let disabledReason = null;
    if (!enabled) {
      disabledReason = 'DB_BACKUP_ENABLED is false.';
    } else if (!bucket) {
      disabledReason = 'DB backup bucket is not configured.';
    } else if (!hasCreds) {
      disabledReason = 'DB backup credentials are missing.';
    }

    return {
      enabled,
      isOperational,
      disabledReason,
      bucket,
      prefix,
      region,
      endpoint,
      accessKeyId,
      secretAccessKey,
      sessionToken,
      hasAccessKeyId: Boolean(accessKeyId),
      hasSecretAccessKey: Boolean(secretAccessKey),
      hasSessionToken: Boolean(sessionToken),
      intervalMs,
      runOnStartup,
      retentionDays,
      applyLifecycle,
      lifecycleRuleId,
      forcePathStyle,
      tempDir
    };
  }

  function getObjectKey(now = new Date()) {
    const filename = `cloudstudio-db-${isoCompactStamp(now)}-${crypto.randomBytes(3).toString('hex')}.sqlite`;
    return currentConfig?.prefix ? `${currentConfig.prefix}/${filename}` : filename;
  }

  function setNextRunAtFromNow() {
    state.nextRunAt = new Date(Date.now() + state.intervalMs).toISOString();
  }

  function stopTimerOnly() {
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
    state.nextRunAt = null;
  }

  function startTimerOnly() {
    if (timer || !state.enabled) {
      return;
    }
    setNextRunAtFromNow();
    timer = setInterval(() => {
      setNextRunAtFromNow();
      void runBackup('interval');
    }, state.intervalMs);
  }

  function applyEffectiveConfig(reason = 'refresh') {
    const previous = currentConfig;
    const next = resolveEffectiveConfig();
    const intervalChanged = !previous || previous.intervalMs !== next.intervalMs;
    const lifecycleScopeChanged =
      !previous ||
      previous.bucket !== next.bucket ||
      previous.prefix !== next.prefix ||
      previous.retentionDays !== next.retentionDays ||
      previous.lifecycleRuleId !== next.lifecycleRuleId ||
      previous.applyLifecycle !== next.applyLifecycle;

    currentConfig = next;
    state.enabled = next.isOperational;
    state.intervalMs = next.intervalMs;
    state.retentionDays = next.retentionDays;
    state.bucket = next.bucket || null;
    state.prefix = next.prefix || null;
    state.region = next.region || null;
    state.endpoint = next.endpoint || null;
    state.forcePathStyle = next.forcePathStyle;
    state.tempDir = next.tempDir || null;
    state.runOnStartup = next.runOnStartup;
    state.applyLifecycle = next.applyLifecycle;
    state.lifecycleRuleId = next.lifecycleRuleId;
    state.hasAccessKeyId = next.hasAccessKeyId;
    state.hasSecretAccessKey = next.hasSecretAccessKey;
    state.hasSessionToken = next.hasSessionToken;
    state.disabledReason = next.disabledReason;

    if (lifecycleScopeChanged) {
      lifecycleEnsured = false;
      state.lifecycleEnsuredAt = null;
    }

    if (next.isOperational) {
      s3 = createS3Client(next);
    } else {
      s3 = null;
    }

    if (schedulerStarted) {
      if (!next.isOperational) {
        stopTimerOnly();
      } else {
        if (timer && intervalChanged) {
          stopTimerOnly();
        }
        startTimerOnly();
      }
    }

    if (reason !== 'run') {
      logger.info(
        `[cloudstudio:db-backup] Config refreshed (${reason}): enabled=${next.enabled}, operational=${next.isOperational}, intervalMs=${next.intervalMs}`
      );
    }

    return next;
  }

  async function ensureLifecycleRule() {
    const cfg = currentConfig || applyEffectiveConfig('lifecycle');
    if (!state.enabled || !cfg?.applyLifecycle || !s3) {
      return;
    }
    if (lifecycleEnsured) {
      return;
    }

    const desiredRule = {
      ID: cfg.lifecycleRuleId,
      Status: 'Enabled',
      Filter: cfg.prefix ? { Prefix: `${cfg.prefix}/` } : {},
      Expiration: { Days: cfg.retentionDays },
      AbortIncompleteMultipartUpload: {
        DaysAfterInitiation: 1
      }
    };
    if (!cfg.prefix) {
      delete desiredRule.Filter;
    }

    let existingRules = [];
    try {
      const current = await s3.send(
        new GetBucketLifecycleConfigurationCommand({
          Bucket: cfg.bucket
        })
      );
      existingRules = Array.isArray(current?.Rules) ? current.Rules : [];
    } catch (error) {
      if (!isNoLifecycleConfigError(error)) {
        throw error;
      }
      existingRules = [];
    }

    const nextRules = existingRules.filter((rule) => String(rule?.ID || '') !== cfg.lifecycleRuleId);
    nextRules.push(desiredRule);

    await s3.send(
      new PutBucketLifecycleConfigurationCommand({
        Bucket: cfg.bucket,
        LifecycleConfiguration: {
          Rules: nextRules
        }
      })
    );
    lifecycleEnsured = true;
    state.lifecycleEnsuredAt = new Date().toISOString();
    state.lastLifecycleError = null;
  }

  async function runBackup(reason = 'interval') {
    const cfg = currentConfig || applyEffectiveConfig('run');
    if (!state.enabled || !s3 || !cfg?.isOperational) {
      return {
        ok: false,
        skipped: true,
        reason: state.disabledReason || 'Backup scheduler is disabled.'
      };
    }
    if (state.running) {
      return {
        ok: false,
        skipped: true,
        reason: 'Backup already in progress.'
      };
    }

    state.running = true;
    state.lastReason = reason;
    state.lastStartedAt = new Date().toISOString();
    state.lastStatus = 'running';
    state.lastError = null;
    const startedAtMs = Date.now();

    fs.mkdirSync(cfg.tempDir, { recursive: true });
    const tempFile = path.join(cfg.tempDir, `cloudstudio-db-${Date.now()}.sqlite`);

    try {
      await db.backup(tempFile);
      const stats = fs.statSync(tempFile);
      const objectKey = getObjectKey(new Date());

      await s3.send(
        new PutObjectCommand({
          Bucket: cfg.bucket,
          Key: objectKey,
          Body: fs.createReadStream(tempFile),
          ContentType: 'application/x-sqlite3',
          Metadata: {
            source: 'cloudstudio',
            db: 'cloudstudio.db'
          }
        })
      );

      if (cfg.applyLifecycle) {
        try {
          await ensureLifecycleRule();
        } catch (error) {
          state.lastLifecycleError = error?.message || String(error);
          logger.warn(`[cloudstudio:db-backup] Lifecycle update skipped: ${state.lastLifecycleError}`);
        }
      }

      state.lastCompletedAt = new Date().toISOString();
      state.lastSuccessAt = state.lastCompletedAt;
      state.lastDurationMs = Date.now() - startedAtMs;
      state.lastStatus = 'ok';
      state.lastKey = objectKey;
      state.lastSizeBytes = Number(stats.size || 0);
      state.lastError = null;

      logger.info(
        `[cloudstudio:db-backup] Upload complete (${reason}) -> s3://${cfg.bucket}/${objectKey} (${state.lastSizeBytes} bytes)`
      );

      return {
        ok: true,
        key: objectKey,
        bytes: state.lastSizeBytes
      };
    } catch (error) {
      state.lastCompletedAt = new Date().toISOString();
      state.lastDurationMs = Date.now() - startedAtMs;
      state.lastStatus = 'error';
      state.lastError = error?.message || String(error);
      logger.error(`[cloudstudio:db-backup] Backup failed (${reason}): ${state.lastError}`);
      return {
        ok: false,
        error: state.lastError
      };
    } finally {
      state.running = false;
      try {
        if (fs.existsSync(tempFile)) {
          fs.unlinkSync(tempFile);
        }
      } catch (_error) {
        // Ignore cleanup failures.
      }
    }
  }

  function start() {
    schedulerStarted = true;
    const cfg = applyEffectiveConfig('startup');
    if (!cfg.isOperational) {
      logger.info(`[cloudstudio:db-backup] Scheduler disabled: ${state.disabledReason}`);
      return;
    }

    logger.info(
      `[cloudstudio:db-backup] Scheduler enabled: every ${Math.round(state.intervalMs / 1000)}s, bucket=${state.bucket}, retention=${state.retentionDays}d`
    );

    startTimerOnly();

    if (cfg.runOnStartup) {
      void runBackup('startup');
    } else if (cfg.applyLifecycle) {
      void ensureLifecycleRule().catch((error) => {
        state.lastLifecycleError = error?.message || String(error);
        logger.warn(`[cloudstudio:db-backup] Lifecycle update on startup failed: ${state.lastLifecycleError}`);
      });
    }
  }

  function stop() {
    schedulerStarted = false;
    stopTimerOnly();
  }

  function reloadConfig(reason = 'manual-reload') {
    applyEffectiveConfig(reason);
    return getStatus();
  }

  function getStatus() {
    return {
      ...state
    };
  }

  return {
    start,
    stop,
    runBackupNow: () => runBackup('manual'),
    reloadConfig,
    getStatus
  };
}

module.exports = {
  createDbBackupScheduler
};
