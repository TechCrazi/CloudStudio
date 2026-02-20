const { createHash, createCipheriv, createDecipheriv, randomBytes } = require('crypto');

const SECRET_SEED = String(process.env.CLOUDSTUDIO_SECRET_KEY || 'cloudstudio-dev-secret-change-me');
const SECRET_KEY = createHash('sha256').update(SECRET_SEED).digest();

function hashApiKey(apiKey) {
  return createHash('sha256').update(String(apiKey || '')).digest('hex');
}

function generateApiKey() {
  const raw = randomBytes(24).toString('base64url');
  return `cstudio_${raw}`;
}

function maskApiKey(apiKey) {
  const value = String(apiKey || '');
  if (value.length <= 10) {
    return `${value.slice(0, 2)}***`;
  }
  return `${value.slice(0, 7)}...${value.slice(-4)}`;
}

function encryptJson(payload) {
  if (!payload || typeof payload !== 'object') {
    return null;
  }

  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', SECRET_KEY, iv);
  const plaintext = Buffer.from(JSON.stringify(payload), 'utf8');
  const encrypted = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${iv.toString('base64')}.${tag.toString('base64')}.${encrypted.toString('base64')}`;
}

function decryptJson(ciphertext) {
  if (!ciphertext) {
    return null;
  }

  const parts = String(ciphertext).split('.');
  if (parts.length !== 3) {
    return null;
  }

  try {
    const iv = Buffer.from(parts[0], 'base64');
    const tag = Buffer.from(parts[1], 'base64');
    const encrypted = Buffer.from(parts[2], 'base64');

    const decipher = createDecipheriv('aes-256-gcm', SECRET_KEY, iv);
    decipher.setAuthTag(tag);
    const decrypted = Buffer.concat([decipher.update(encrypted), decipher.final()]);
    return JSON.parse(decrypted.toString('utf8'));
  } catch (_error) {
    return null;
  }
}

module.exports = {
  hashApiKey,
  generateApiKey,
  maskApiKey,
  encryptJson,
  decryptJson
};
