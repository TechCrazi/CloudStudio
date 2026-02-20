const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');
const { pipeline } = require('stream/promises');
const {
  S3Client,
  PutObjectCommand,
  GetBucketLifecycleConfigurationCommand,
  PutBucketLifecycleConfigurationCommand
} = require('@aws-sdk/client-s3');
const { db } = require('./db');
const Database = require('better-sqlite3');

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

function buildRandomSuffix(bytes = 3) {
  return crypto.randomBytes(bytes).toString('hex');
}

function safeUnlink(filePath) {
  if (!filePath) {
    return;
  }
  try {
    fs.unlinkSync(filePath);
  } catch (_error) {
    // Ignore cleanup failures.
  }
}

function fileExists(filePath) {
  if (!filePath) {
    return false;
  }
  try {
    return fs.existsSync(filePath);
  } catch (_error) {
    return false;
  }
}

function safeReadJson(filePath, fallback = null) {
  if (!fileExists(filePath)) {
    return fallback;
  }
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (_error) {
    return fallback;
  }
}

function safeWriteJson(filePath, payload) {
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
}

function moveFileSync(sourcePath, targetPath) {
  try {
    fs.renameSync(sourcePath, targetPath);
    return;
  } catch (_error) {
    fs.copyFileSync(sourcePath, targetPath);
    safeUnlink(sourcePath);
  }
}

async function hashFileSha256(filePath) {
  return await new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath);
    stream.on('error', reject);
    stream.on('data', (chunk) => {
      hash.update(chunk);
    });
    stream.on('end', () => {
      resolve(hash.digest('hex'));
    });
  });
}

async function gzipFile(inputPath, outputPath, level) {
  await pipeline(
    fs.createReadStream(inputPath),
    zlib.createGzip({ level }),
    fs.createWriteStream(outputPath)
  );
}

function verifySqliteBackupFile(filePath) {
  const backupDb = new Database(filePath, { readonly: true });
  try {
    const row = backupDb.prepare('PRAGMA quick_check').get();
    const status = String(row?.quick_check || row?.integrity_check || '').trim().toLowerCase();
    if (status && status !== 'ok') {
      throw new Error(`SQLite quick_check failed: ${status}`);
    }
  } finally {
    backupDb.close();
  }
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
    queueDir: null,
    compressionEnabled: true,
    compressionLevel: 6,
    verifyQuickCheck: true,
    retryBatchSize: 3,
    maxQueuedFiles: 336,
    queuedFiles: 0,
    queuedBytes: 0,
    queuePrunedFiles: 0,
    queueRecoveredFiles: 0,
    queueLastError: null,
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
    const queueDir = path.resolve(
      pickString({
        envNames: ['DB_BACKUP_QUEUE_DIR'],
        uiValue: ui.queueDir,
        defaultValue: path.join(tempDir, 'queue')
      }) || path.join(tempDir, 'queue')
    );
    const compressionEnabled = pickBoolean({
      envNames: ['DB_BACKUP_COMPRESS'],
      uiValue: ui.compressionEnabled,
      defaultValue: true
    });
    const compressionLevel = pickInteger({
      envNames: ['DB_BACKUP_COMPRESSION_LEVEL'],
      uiValue: ui.compressionLevel,
      defaultValue: 6,
      min: 1,
      max: 9
    });
    const verifyQuickCheck = pickBoolean({
      envNames: ['DB_BACKUP_VERIFY_QUICK_CHECK'],
      uiValue: ui.verifyQuickCheck,
      defaultValue: true
    });
    const retryBatchSize = pickInteger({
      envNames: ['DB_BACKUP_RETRY_BATCH_SIZE'],
      uiValue: ui.retryBatchSize,
      defaultValue: 3,
      min: 1,
      max: 100
    });
    const maxQueuedFiles = pickInteger({
      envNames: ['DB_BACKUP_MAX_QUEUED_FILES'],
      uiValue: ui.maxQueuedFiles,
      defaultValue: 336,
      min: 1,
      max: 100000
    });
    const uploadManifest = pickBoolean({
      envNames: ['DB_BACKUP_UPLOAD_MANIFEST'],
      uiValue: ui.uploadManifest,
      defaultValue: true
    });

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
      tempDir,
      queueDir,
      compressionEnabled,
      compressionLevel,
      verifyQuickCheck,
      retryBatchSize,
      maxQueuedFiles,
      uploadManifest
    };
  }

  function getObjectKey(now = new Date(), extension = 'sqlite') {
    const ext = String(extension || 'sqlite')
      .trim()
      .replace(/^\.+/, '') || 'sqlite';
    const filename = `cloudstudio-db-${isoCompactStamp(now)}-${buildRandomSuffix(3)}.${ext}`;
    return currentConfig?.prefix ? `${currentConfig.prefix}/${filename}` : filename;
  }

  function getManifestObjectKey(objectKey) {
    return `${String(objectKey || '').trim()}.manifest.json`;
  }

  function listQueueMetaFiles(cfg = currentConfig) {
    if (!cfg?.queueDir || !fileExists(cfg.queueDir)) {
      return [];
    }
    const names = fs
      .readdirSync(cfg.queueDir, { withFileTypes: true })
      .filter((entry) => entry.isFile() && entry.name.endsWith('.queue.json'))
      .map((entry) => path.join(cfg.queueDir, entry.name))
      .sort((left, right) => {
        const leftStat = fs.statSync(left);
        const rightStat = fs.statSync(right);
        return leftStat.mtimeMs - rightStat.mtimeMs;
      });
    return names;
  }

  function updateQueueStats(cfg = currentConfig) {
    if (!cfg?.queueDir || !fileExists(cfg.queueDir)) {
      state.queuedFiles = 0;
      state.queuedBytes = 0;
      return;
    }
    const metaFiles = listQueueMetaFiles(cfg);
    let queuedFiles = 0;
    let queuedBytes = 0;
    for (const metaPath of metaFiles) {
      const meta = safeReadJson(metaPath, null);
      if (!meta || typeof meta !== 'object') {
        continue;
      }
      const dataFilename = String(meta.dataFilename || '').trim();
      const dataPath = dataFilename ? path.join(cfg.queueDir, dataFilename) : '';
      if (!fileExists(dataPath)) {
        safeUnlink(metaPath);
        continue;
      }
      queuedFiles += 1;
      try {
        queuedBytes += Number(fs.statSync(dataPath).size || 0);
      } catch (_error) {
        // ignore size failures
      }
    }
    state.queuedFiles = queuedFiles;
    state.queuedBytes = queuedBytes;
  }

  function pruneQueue(cfg = currentConfig) {
    if (!cfg?.queueDir) {
      return;
    }
    const metas = listQueueMetaFiles(cfg);
    if (metas.length <= cfg.maxQueuedFiles) {
      return;
    }
    const removeCount = metas.length - cfg.maxQueuedFiles;
    for (let index = 0; index < removeCount; index += 1) {
      const metaPath = metas[index];
      const meta = safeReadJson(metaPath, null);
      const dataFilename = String(meta?.dataFilename || '').trim();
      const dataPath = dataFilename ? path.join(cfg.queueDir, dataFilename) : '';
      safeUnlink(dataPath);
      safeUnlink(metaPath);
      state.queuePrunedFiles += 1;
    }
  }

  function queueFailedUpload(cfg, descriptor = {}, errorMessage = 'Upload failed') {
    if (!cfg?.queueDir || !descriptor?.uploadPath || !fileExists(descriptor.uploadPath)) {
      return null;
    }
    fs.mkdirSync(cfg.queueDir, { recursive: true });
    const queueId = `${Date.now()}-${buildRandomSuffix(3)}`;
    const ext = path.extname(descriptor.uploadPath) || '.bin';
    const dataFilename = `${queueId}${ext}`;
    const metaFilename = `${queueId}.queue.json`;
    const dataPath = path.join(cfg.queueDir, dataFilename);
    const metaPath = path.join(cfg.queueDir, metaFilename);

    moveFileSync(descriptor.uploadPath, dataPath);
    const meta = {
      queueId,
      queuedAt: new Date().toISOString(),
      attempts: 0,
      lastError: String(errorMessage || '').trim() || 'Upload failed',
      dataFilename,
      objectKey: descriptor.objectKey,
      manifestObjectKey: descriptor.manifestObjectKey,
      contentType: descriptor.contentType,
      contentEncoding: descriptor.contentEncoding || null,
      metadata: descriptor.metadata || {},
      manifest: descriptor.manifest || null
    };
    safeWriteJson(metaPath, meta);
    updateQueueStats(cfg);
    pruneQueue(cfg);
    updateQueueStats(cfg);
    return {
      dataPath,
      metaPath
    };
  }

  async function uploadBackupDescriptor(cfg, descriptor = {}) {
    await s3.send(
      new PutObjectCommand({
        Bucket: cfg.bucket,
        Key: descriptor.objectKey,
        Body: fs.createReadStream(descriptor.uploadPath),
        ContentType: descriptor.contentType,
        ...(descriptor.contentEncoding ? { ContentEncoding: descriptor.contentEncoding } : {}),
        Metadata: descriptor.metadata || {}
      })
    );

    if (cfg.uploadManifest && descriptor.manifestObjectKey && descriptor.manifest && typeof descriptor.manifest === 'object') {
      await s3.send(
        new PutObjectCommand({
          Bucket: cfg.bucket,
          Key: descriptor.manifestObjectKey,
          Body: JSON.stringify(descriptor.manifest, null, 2),
          ContentType: 'application/json'
        })
      );
    }
  }

  async function retryQueuedBackups(cfg) {
    if (!cfg?.isOperational || !s3 || !cfg.queueDir) {
      updateQueueStats(cfg);
      return;
    }
    if (!fileExists(cfg.queueDir)) {
      updateQueueStats(cfg);
      return;
    }

    const metas = listQueueMetaFiles(cfg);
    if (!metas.length) {
      updateQueueStats(cfg);
      return;
    }

    let recovered = 0;
    const maxAttempts = Math.max(1, Number(cfg.retryBatchSize || 1));
    for (let index = 0; index < metas.length && recovered < maxAttempts; index += 1) {
      const metaPath = metas[index];
      const meta = safeReadJson(metaPath, null);
      if (!meta || typeof meta !== 'object') {
        safeUnlink(metaPath);
        continue;
      }
      const dataFilename = String(meta.dataFilename || '').trim();
      const dataPath = dataFilename ? path.join(cfg.queueDir, dataFilename) : '';
      if (!fileExists(dataPath)) {
        safeUnlink(metaPath);
        continue;
      }

      const descriptor = {
        objectKey: meta.objectKey,
        manifestObjectKey: meta.manifestObjectKey,
        uploadPath: dataPath,
        contentType: meta.contentType || 'application/octet-stream',
        contentEncoding: meta.contentEncoding || undefined,
        metadata: meta.metadata && typeof meta.metadata === 'object' ? meta.metadata : {},
        manifest: meta.manifest && typeof meta.manifest === 'object' ? meta.manifest : null
      };

      try {
        await uploadBackupDescriptor(cfg, descriptor);
        safeUnlink(dataPath);
        safeUnlink(metaPath);
        recovered += 1;
        state.queueRecoveredFiles += 1;
        state.queueLastError = null;
      } catch (error) {
        meta.attempts = Number(meta.attempts || 0) + 1;
        meta.lastError = error?.message || String(error);
        meta.lastAttemptAt = new Date().toISOString();
        safeWriteJson(metaPath, meta);
        state.queueLastError = meta.lastError;
        break;
      }
    }

    updateQueueStats(cfg);
    if (recovered > 0) {
      logger.info(`[cloudstudio:db-backup] Recovered ${recovered} queued backup upload(s).`);
    }
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
    state.queueDir = next.queueDir || null;
    state.compressionEnabled = next.compressionEnabled;
    state.compressionLevel = next.compressionLevel;
    state.verifyQuickCheck = next.verifyQuickCheck;
    state.retryBatchSize = next.retryBatchSize;
    state.maxQueuedFiles = next.maxQueuedFiles;
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

    try {
      if (next.tempDir) {
        fs.mkdirSync(next.tempDir, { recursive: true });
      }
      if (next.queueDir) {
        fs.mkdirSync(next.queueDir, { recursive: true });
      }
    } catch (_error) {
      // Ignore directory creation errors here; backup run will capture actionable failure details.
    }
    updateQueueStats(next);
    pruneQueue(next);
    updateQueueStats(next);

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
    const tempSQLiteFile = path.join(cfg.tempDir, `cloudstudio-db-${Date.now()}-${buildRandomSuffix(2)}.sqlite`);
    let uploadPath = tempSQLiteFile;
    let descriptor = null;

    try {
      await retryQueuedBackups(cfg);
      await db.backup(tempSQLiteFile);
      if (cfg.verifyQuickCheck) {
        verifySqliteBackupFile(tempSQLiteFile);
      }

      const sqliteStats = fs.statSync(tempSQLiteFile);
      let contentType = 'application/x-sqlite3';
      let contentEncoding;
      let extension = 'sqlite';
      let compressed = false;

      if (cfg.compressionEnabled) {
        const compressedPath = `${tempSQLiteFile}.gz`;
        await gzipFile(tempSQLiteFile, compressedPath, cfg.compressionLevel);
        uploadPath = compressedPath;
        contentType = 'application/gzip';
        contentEncoding = 'gzip';
        extension = 'sqlite.gz';
        compressed = true;
      }

      const uploadStats = fs.statSync(uploadPath);
      const sha256 = await hashFileSha256(uploadPath);
      const objectKey = getObjectKey(new Date(), extension);
      const manifestObjectKey = getManifestObjectKey(objectKey);
      const manifest = {
        version: 1,
        generatedAt: new Date().toISOString(),
        key: objectKey,
        manifestKey: manifestObjectKey,
        sourceDb: 'cloudstudio.db',
        sqliteBytes: Number(sqliteStats.size || 0),
        uploadedBytes: Number(uploadStats.size || 0),
        checksum: {
          algorithm: 'sha256',
          value: sha256
        },
        compression: compressed
          ? {
              type: 'gzip',
              level: cfg.compressionLevel
            }
          : {
              type: 'none'
            }
      };
      descriptor = {
        objectKey,
        manifestObjectKey,
        uploadPath,
        contentType,
        contentEncoding,
        metadata: {
          source: 'cloudstudio',
          db: 'cloudstudio.db',
          checksum: 'sha256',
          sha256,
          compression: compressed ? 'gzip' : 'none'
        },
        manifest
      };

      await uploadBackupDescriptor(cfg, descriptor);

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
      state.lastKey = descriptor.objectKey;
      state.lastSizeBytes = Number(manifest.uploadedBytes || 0);
      state.lastError = null;
      state.queueLastError = null;
      updateQueueStats(cfg);
      pruneQueue(cfg);
      updateQueueStats(cfg);

      logger.info(
        `[cloudstudio:db-backup] Upload complete (${reason}) -> s3://${cfg.bucket}/${descriptor.objectKey} (${state.lastSizeBytes} bytes, compression=${compressed ? 'gzip' : 'none'})`
      );

      return {
        ok: true,
        key: descriptor.objectKey,
        bytes: state.lastSizeBytes
      };
    } catch (error) {
      let queued = false;
      if (descriptor && descriptor.uploadPath && fileExists(descriptor.uploadPath)) {
        const queuedEntry = queueFailedUpload(cfg, descriptor, error?.message || String(error));
        if (queuedEntry) {
          queued = true;
          logger.warn(
            `[cloudstudio:db-backup] Upload failed (${reason}); queued backup for retry: ${path.basename(queuedEntry.dataPath)}`
          );
        }
      }
      state.lastCompletedAt = new Date().toISOString();
      state.lastDurationMs = Date.now() - startedAtMs;
      if (queued) {
        state.lastStatus = 'degraded';
        state.lastError = null;
      } else {
        state.lastStatus = 'error';
        state.lastError = error?.message || String(error);
      }
      if (queued) {
        logger.warn(`[cloudstudio:db-backup] Backup queued (${reason}) due to upload error: ${error?.message || String(error)}`);
      } else {
        logger.error(`[cloudstudio:db-backup] Backup failed (${reason}): ${state.lastError}`);
      }
      updateQueueStats(cfg);
      pruneQueue(cfg);
      updateQueueStats(cfg);
      return {
        ok: queued,
        queued,
        error: queued ? null : state.lastError,
        queuedFiles: state.queuedFiles
      };
    } finally {
      state.running = false;
      if (uploadPath !== tempSQLiteFile) {
        safeUnlink(uploadPath);
      }
      safeUnlink(tempSQLiteFile);
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
    } else {
      void retryQueuedBackups(cfg).catch((error) => {
        state.queueLastError = error?.message || String(error);
        logger.warn(`[cloudstudio:db-backup] Queue retry on startup failed: ${state.queueLastError}`);
      });
      if (cfg.applyLifecycle) {
        void ensureLifecycleRule().catch((error) => {
          state.lastLifecycleError = error?.message || String(error);
          logger.warn(`[cloudstudio:db-backup] Lifecycle update on startup failed: ${state.lastLifecycleError}`);
        });
      }
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
