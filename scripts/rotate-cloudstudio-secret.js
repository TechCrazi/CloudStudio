#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { createHash, createCipheriv, createDecipheriv, randomBytes } = require('crypto');
const Database = require('better-sqlite3');
const dotenv = require('dotenv');

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const part = String(argv[i] || '').trim();
    if (!part.startsWith('--')) {
      continue;
    }
    const key = part.slice(2);
    const next = argv[i + 1];
    if (!next || String(next).startsWith('--')) {
      args[key] = true;
      continue;
    }
    args[key] = String(next);
    i += 1;
  }
  return args;
}

function deriveSecretKey(seed) {
  return createHash('sha256').update(String(seed || '')).digest();
}

function encryptJsonWithSeed(seed, payload) {
  if (!payload || typeof payload !== 'object') {
    throw new Error('Encryption payload must be an object.');
  }
  const key = deriveSecretKey(seed);
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', key, iv);
  const plaintext = Buffer.from(JSON.stringify(payload), 'utf8');
  const encrypted = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${iv.toString('base64')}.${tag.toString('base64')}.${encrypted.toString('base64')}`;
}

function decryptJsonWithSeed(seed, ciphertext) {
  const text = String(ciphertext || '').trim();
  if (!text) {
    return null;
  }
  const parts = text.split('.');
  if (parts.length !== 3) {
    throw new Error('Ciphertext is malformed.');
  }
  const key = deriveSecretKey(seed);
  const iv = Buffer.from(parts[0], 'base64');
  const tag = Buffer.from(parts[1], 'base64');
  const encrypted = Buffer.from(parts[2], 'base64');
  const decipher = createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(tag);
  const decrypted = Buffer.concat([decipher.update(encrypted), decipher.final()]);
  return JSON.parse(decrypted.toString('utf8'));
}

function main() {
  const rootDir = path.resolve(__dirname, '..');
  const envPath = path.resolve(rootDir, '.env');
  const env = dotenv.parse(fs.readFileSync(envPath));
  const args = parseArgs(process.argv.slice(2));

  const oldKey = String(args['old-key'] || env.CLOUDSTUDIO_SECRET_KEY || '').trim();
  const newKey = String(args['new-key'] || '').trim();
  const dbPath = path.resolve(rootDir, args.db || env.CLOUDSTUDIO_DB_FILE || env.SQLITE_PATH || './data/cloudstudio.db');

  if (!oldKey) {
    throw new Error('Missing --old-key and CLOUDSTUDIO_SECRET_KEY is empty in .env');
  }
  if (!newKey) {
    throw new Error('Missing --new-key.');
  }
  if (oldKey === newKey) {
    throw new Error('New key must be different from old key.');
  }

  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  const now = new Date().toISOString();

  const vendorRows = db
    .prepare(
      `
      SELECT id, credentials_encrypted
      FROM vendors
      WHERE credentials_encrypted IS NOT NULL
        AND length(trim(credentials_encrypted)) > 0
      `
    )
    .all();

  const settingRows = db.prepare('SELECT key, value_json FROM app_settings').all();

  let vendorUpdated = 0;
  let appSettingsUpdated = 0;

  const rotateTxn = db.transaction(() => {
    const updateVendor = db.prepare('UPDATE vendors SET credentials_encrypted = ?, updated_at = ? WHERE id = ?');
    const updateSetting = db.prepare('UPDATE app_settings SET value_json = ?, updated_at = ? WHERE key = ?');

    for (const row of vendorRows) {
      const payload = decryptJsonWithSeed(oldKey, row.credentials_encrypted);
      const rotated = encryptJsonWithSeed(newKey, payload);
      updateVendor.run(rotated, now, row.id);
      vendorUpdated += 1;
    }

    for (const row of settingRows) {
      let value = null;
      try {
        value = row.value_json ? JSON.parse(row.value_json) : null;
      } catch (_error) {
        value = null;
      }
      if (!value || typeof value !== 'object') {
        continue;
      }
      const ciphertext = String(value.ciphertext || '').trim();
      if (!ciphertext) {
        continue;
      }
      const payload = decryptJsonWithSeed(oldKey, ciphertext);
      value.ciphertext = encryptJsonWithSeed(newKey, payload);
      updateSetting.run(JSON.stringify(value), now, row.key);
      appSettingsUpdated += 1;
    }
  });

  rotateTxn();

  // Verify decryptability under new key.
  for (const row of vendorRows) {
    const refreshed = db.prepare('SELECT credentials_encrypted FROM vendors WHERE id = ?').get(row.id);
    decryptJsonWithSeed(newKey, refreshed.credentials_encrypted);
  }
  for (const row of settingRows) {
    const refreshed = db.prepare('SELECT value_json FROM app_settings WHERE key = ?').get(row.key);
    let value = null;
    try {
      value = refreshed?.value_json ? JSON.parse(refreshed.value_json) : null;
    } catch (_error) {
      value = null;
    }
    const ciphertext = String(value?.ciphertext || '').trim();
    if (ciphertext) {
      decryptJsonWithSeed(newKey, ciphertext);
    }
  }

  console.log(
    JSON.stringify(
      {
        ok: true,
        dbPath,
        vendorRowsUpdated: vendorUpdated,
        appSettingRowsUpdated: appSettingsUpdated
      },
      null,
      2
    )
  );
}

try {
  main();
} catch (error) {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: error?.message || String(error)
      },
      null,
      2
    )
  );
  process.exit(1);
}
