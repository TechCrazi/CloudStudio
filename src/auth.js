const crypto = require('crypto');
const { db } = require('./db');

const AUTH_COOKIE_NAME = String(process.env.CLOUDSTUDIO_SESSION_COOKIE_NAME || 'cloudstudio_session').trim() || 'cloudstudio_session';
const AUTH_SESSION_TTL_MS = Math.max(
  5 * 60 * 1000,
  Number.parseInt(process.env.CLOUDSTUDIO_SESSION_TTL_MS || `${7 * 24 * 60 * 60 * 1000}`, 10) || 7 * 24 * 60 * 60 * 1000
);

const authSessions = new Map();
const VIEW_KEYS = Object.freeze([
  'dashboard',
  'storage',
  'storage-unified',
  'storage-azure',
  'storage-aws',
  'storage-gcp',
  'storage-wasabi',
  'storage-vsax',
  'storage-other',
  'ip-address',
  'pricing',
  'billing',
  'tags',
  'cloud-metrics',
  'cloud-database',
  'grafana-cloud',
  'live',
  'firewall',
  'vpn',
  'security',
  'vendors',
  'admin-settings',
  'admin-users',
  'admin-api-keys',
  'admin-backup',
  'apidocs'
]);
const ADMIN_ONLY_VIEW_KEY_SET = new Set(['vendors', 'admin-settings', 'admin-users', 'admin-api-keys', 'admin-backup']);
const NON_ADMIN_VIEW_KEYS = Object.freeze(VIEW_KEYS.filter((view) => !ADMIN_ONLY_VIEW_KEY_SET.has(view)));
const VIEW_KEY_SET = new Set(VIEW_KEYS);

function normalizeUsername(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function normalizeAdminFlag(value) {
  return Number(value) === 1 || value === true;
}

function parseAdminUsers() {
  return new Set(
    String(process.env.APP_ADMIN_USERS || 'admin')
      .split(',')
      .map((item) => normalizeUsername(item))
      .filter(Boolean)
  );
}

function normalizeViewKey(value) {
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

function normalizeAllowedViewsInput(raw) {
  let values = [];
  if (Array.isArray(raw)) {
    values = raw;
  } else if (typeof raw === 'string') {
    const text = raw.trim();
    if (text) {
      if (text.startsWith('[')) {
        try {
          const parsed = JSON.parse(text);
          if (Array.isArray(parsed)) {
            values = parsed;
          }
        } catch (_error) {
          values = text.split(',');
        }
      } else {
        values = text.split(',');
      }
    }
  }

  const output = [];
  const seen = new Set();
  values.forEach((entry) => {
    const key = normalizeViewKey(entry);
    if (!key || !VIEW_KEY_SET.has(key) || seen.has(key)) {
      return;
    }
    seen.add(key);
    output.push(key);
  });
  return output;
}

function resolveAllowedViews(options = {}) {
  const isAdmin = normalizeAdminFlag(options.isAdmin);
  if (isAdmin) {
    return [...VIEW_KEYS];
  }

  const fromInput = normalizeAllowedViewsInput(options.rawAllowedViews);
  const fromFallback = normalizeAllowedViewsInput(options.fallbackAllowedViews);
  const source = fromInput.length ? fromInput : fromFallback.length ? fromFallback : [...NON_ADMIN_VIEW_KEYS];
  const filtered = source.filter((view) => !ADMIN_ONLY_VIEW_KEY_SET.has(view));
  return filtered.length ? filtered : [...NON_ADMIN_VIEW_KEYS];
}

function hashPassword(password, saltHex) {
  const salt = saltHex || crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(String(password || ''), salt, 64).toString('hex');
  return { salt, hash };
}

function verifyPassword(password, userRecord) {
  if (!userRecord?.passwordSalt || !userRecord?.passwordHash) {
    return false;
  }
  const { hash } = hashPassword(password, userRecord.passwordSalt);
  const expected = Buffer.from(userRecord.passwordHash, 'hex');
  const actual = Buffer.from(hash, 'hex');
  if (expected.length !== actual.length) {
    return false;
  }
  return crypto.timingSafeEqual(expected, actual);
}

function ensureAuthSchema() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      username TEXT PRIMARY KEY,
      password_salt TEXT NOT NULL,
      password_hash TEXT NOT NULL,
      is_admin INTEGER NOT NULL DEFAULT 0,
      allowed_views TEXT NOT NULL DEFAULT '[]',
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

  const userColumns = db.prepare('PRAGMA table_info(users)').all();
  const hasIsAdmin = userColumns.some((column) => String(column?.name || '').toLowerCase() === 'is_admin');
  if (!hasIsAdmin) {
    db.exec('ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0');
  }
  const hasAllowedViews = userColumns.some((column) => String(column?.name || '').toLowerCase() === 'allowed_views');
  if (!hasAllowedViews) {
    db.exec(`ALTER TABLE users ADD COLUMN allowed_views TEXT NOT NULL DEFAULT '[]'`);
  }
}

function getUserCount() {
  const row = db.prepare('SELECT COUNT(*) AS count FROM users').get();
  const count = Number(row?.count);
  return Number.isFinite(count) ? count : 0;
}

function getUserByUsername(usernameRaw) {
  const username = normalizeUsername(usernameRaw);
  if (!username) {
    return null;
  }
  const row = db
    .prepare(
      `SELECT
         username,
         password_salt AS passwordSalt,
         password_hash AS passwordHash,
         is_admin AS isAdmin,
         allowed_views AS allowedViews,
         created_at AS createdAt,
         updated_at AS updatedAt
       FROM users
       WHERE username = ?
       LIMIT 1`
    )
    .get(username);

  if (!row) {
    return null;
  }

  return {
    username: row.username,
    passwordSalt: row.passwordSalt,
    passwordHash: row.passwordHash,
    isAdmin: normalizeAdminFlag(row.isAdmin),
    allowedViews: resolveAllowedViews({
      isAdmin: row.isAdmin,
      rawAllowedViews: row.allowedViews
    }),
    createdAt: row.createdAt,
    updatedAt: row.updatedAt
  };
}

function listUsers() {
  const rows = db
    .prepare(
      `SELECT
         username,
         is_admin AS isAdmin,
         allowed_views AS allowedViews,
         created_at AS createdAt,
         updated_at AS updatedAt
       FROM users
       ORDER BY username`
    )
    .all();

  return rows.map((row) => ({
    username: row.username,
    isAdmin: normalizeAdminFlag(row.isAdmin),
    allowedViews: resolveAllowedViews({
      isAdmin: row.isAdmin,
      rawAllowedViews: row.allowedViews
    }),
    createdAt: row.createdAt,
    updatedAt: row.updatedAt
  }));
}

function upsertUser(usernameRaw, password, options = {}) {
  const username = normalizeUsername(usernameRaw);
  const passwordText = String(password || '');
  if (!username || !passwordText) {
    return false;
  }

  const now = new Date().toISOString();
  const existing = getUserByUsername(username);
  const adminUsers = parseAdminUsers();
  const isAdmin = options.isAdmin !== undefined ? normalizeAdminFlag(options.isAdmin) : existing ? existing.isAdmin : adminUsers.has(username);
  const allowedViews = resolveAllowedViews({
    isAdmin,
    rawAllowedViews: options.allowedViews,
    fallbackAllowedViews: existing?.allowedViews
  });

  const { salt, hash } = hashPassword(passwordText);
  db.prepare(
    `INSERT INTO users (
       username,
       password_salt,
       password_hash,
       is_admin,
       allowed_views,
       created_at,
       updated_at
     ) VALUES (?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(username) DO UPDATE SET
       password_salt = excluded.password_salt,
       password_hash = excluded.password_hash,
       is_admin = excluded.is_admin,
       allowed_views = excluded.allowed_views,
       updated_at = excluded.updated_at`
  ).run(
    username,
    salt,
    hash,
    isAdmin ? 1 : 0,
    JSON.stringify(allowedViews),
    existing?.createdAt || now,
    now
  );

  return true;
}

function createUser(usernameRaw, passwordRaw, options = {}) {
  const username = normalizeUsername(usernameRaw);
  const password = String(passwordRaw || '');
  if (!username || !password) {
    return { ok: false, reason: 'Username and password are required.' };
  }
  if (getUserByUsername(username)) {
    return { ok: false, reason: 'User already exists.' };
  }
  const saved = upsertUser(username, password, options);
  if (!saved) {
    return { ok: false, reason: 'Could not create user.' };
  }
  return { ok: true, user: getUserByUsername(username) };
}

function updateUser(usernameRaw, updates = {}) {
  const username = normalizeUsername(usernameRaw);
  if (!username) {
    return { ok: false, reason: 'Username is required.' };
  }
  const existing = getUserByUsername(username);
  if (!existing) {
    return { ok: false, reason: 'User not found.' };
  }

  const password = updates.password !== undefined ? String(updates.password || '') : null;
  if (password !== null && !password) {
    return { ok: false, reason: 'Password cannot be empty.' };
  }

  const nextIsAdmin = updates.isAdmin !== undefined ? normalizeAdminFlag(updates.isAdmin) : existing.isAdmin;
  const nextAllowedViews = resolveAllowedViews({
    isAdmin: nextIsAdmin,
    rawAllowedViews: updates.allowedViews,
    fallbackAllowedViews: existing.allowedViews
  });

  const now = new Date().toISOString();
  let nextPasswordSalt = existing.passwordSalt;
  let nextPasswordHash = existing.passwordHash;
  if (password !== null) {
    const hashed = hashPassword(password);
    nextPasswordSalt = hashed.salt;
    nextPasswordHash = hashed.hash;
  }

  db.prepare(
    `UPDATE users
     SET password_salt = ?,
         password_hash = ?,
         is_admin = ?,
         allowed_views = ?,
         updated_at = ?
     WHERE username = ?`
  ).run(
    nextPasswordSalt,
    nextPasswordHash,
    nextIsAdmin ? 1 : 0,
    JSON.stringify(nextAllowedViews),
    now,
    username
  );

  return { ok: true, user: getUserByUsername(username) };
}

function revokeSessionsForUser(usernameRaw, options = {}) {
  const username = normalizeUsername(usernameRaw);
  if (!username) {
    return;
  }
  const excludeSessionId = String(options.excludeSessionId || '').trim();
  for (const [sessionId, session] of authSessions.entries()) {
    if (!session || normalizeUsername(session.username) !== username) {
      continue;
    }
    if (excludeSessionId && sessionId === excludeSessionId) {
      continue;
    }
    authSessions.delete(sessionId);
  }
}

function deleteUser(usernameRaw) {
  const username = normalizeUsername(usernameRaw);
  if (!username) {
    return false;
  }
  const info = db.prepare('DELETE FROM users WHERE username = ?').run(username);
  revokeSessionsForUser(username);
  return info.changes > 0;
}

function countAdminUsers() {
  const row = db.prepare('SELECT COUNT(*) AS count FROM users WHERE is_admin = 1').get();
  const count = Number(row?.count);
  return Number.isFinite(count) ? count : 0;
}

function parseSeedUsers(value) {
  const text = String(value || '').trim();
  if (!text) {
    return [];
  }

  return text
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => {
      const separator = item.indexOf(':');
      if (separator <= 0) {
        return null;
      }
      const username = item.slice(0, separator).trim();
      const password = item.slice(separator + 1).trim();
      if (!username || !password) {
        return null;
      }
      return { username, password };
    })
    .filter(Boolean);
}

function seedUsersFromEnv() {
  const seedUsers = [...parseSeedUsers(process.env.APP_AUTH_USERS)];
  if (process.env.APP_LOGIN_USER && process.env.APP_LOGIN_PASSWORD) {
    seedUsers.push({
      username: process.env.APP_LOGIN_USER,
      password: process.env.APP_LOGIN_PASSWORD
    });
  }

  let added = 0;
  for (const entry of seedUsers) {
    const existing = getUserByUsername(entry.username);
    const adminUsers = parseAdminUsers();
    const desiredAdmin = adminUsers.has(normalizeUsername(entry.username));
    if (existing && verifyPassword(entry.password, existing) && normalizeAdminFlag(existing.isAdmin) === desiredAdmin) {
      continue;
    }
    if (upsertUser(entry.username, entry.password, { isAdmin: desiredAdmin })) {
      added += 1;
    }
  }

  if (added > 0) {
    console.log(`[auth] Seeded ${added} auth user${added === 1 ? '' : 's'} from environment.`);
  }
}

function ensureFallbackUser() {
  if (getUserCount() > 0) {
    return;
  }

  const fallbackUser = process.env.APP_LOGIN_USER || 'admin';
  const fallbackPassword = process.env.APP_LOGIN_PASSWORD || 'ChangeMe123!';
  upsertUser(fallbackUser, fallbackPassword, { isAdmin: true });
  console.warn(
    `[auth] No users found. Created fallback admin '${normalizeUsername(
      fallbackUser
    )}'. Set APP_AUTH_USERS or APP_LOGIN_USER/APP_LOGIN_PASSWORD to override.`
  );
}

function parseCookieHeader(cookieHeader) {
  const output = {};
  if (!cookieHeader) {
    return output;
  }
  String(cookieHeader)
    .split(';')
    .forEach((segment) => {
      const [rawKey, ...rest] = segment.trim().split('=');
      if (!rawKey) {
        return;
      }
      const value = rest.join('=') || '';
      try {
        output[rawKey] = decodeURIComponent(value);
      } catch (_error) {
        output[rawKey] = value;
      }
    });
  return output;
}

function cleanupExpiredSessions() {
  const now = Date.now();
  for (const [sessionId, session] of authSessions.entries()) {
    if (!session || !Number.isFinite(session.expiresAt) || session.expiresAt <= now) {
      authSessions.delete(sessionId);
    }
  }
}

function createSession(username) {
  const sessionId = crypto.randomBytes(32).toString('hex');
  authSessions.set(sessionId, {
    username,
    expiresAt: Date.now() + AUTH_SESSION_TTL_MS
  });
  return sessionId;
}

function getSessionFromRequest(req) {
  cleanupExpiredSessions();
  const cookies = parseCookieHeader(req.headers?.cookie || '');
  const sessionId = cookies[AUTH_COOKIE_NAME];
  if (!sessionId) {
    return null;
  }
  const session = authSessions.get(sessionId);
  if (!session) {
    return null;
  }
  if (!session.username || session.expiresAt <= Date.now()) {
    authSessions.delete(sessionId);
    return null;
  }

  const user = getUserByUsername(session.username);
  if (!user) {
    authSessions.delete(sessionId);
    return null;
  }

  return {
    id: sessionId,
    user
  };
}

function authCookieOptions() {
  return {
    httpOnly: true,
    sameSite: 'lax',
    secure: process.env.NODE_ENV === 'production',
    path: '/',
    maxAge: AUTH_SESSION_TTL_MS
  };
}

function authCookieClearOptions() {
  return {
    httpOnly: true,
    sameSite: 'lax',
    secure: process.env.NODE_ENV === 'production',
    path: '/'
  };
}

function authSessionMiddleware(req, _res, next) {
  const session = getSessionFromRequest(req);
  req.authSessionId = session?.id || null;
  req.authUser = session?.user || null;
  next();
}

function requireAuth(req, res, next) {
  if (!req.authUser) {
    return res.status(401).json({ error: 'Authentication required.' });
  }
  return next();
}

function authenticateCredentials(usernameRaw, passwordRaw) {
  const username = normalizeUsername(usernameRaw);
  const password = String(passwordRaw || '');
  if (!username || !password) {
    return { ok: false, reason: 'Username and password are required.' };
  }

  const user = getUserByUsername(username);
  if (!user || !verifyPassword(password, user)) {
    return { ok: false, reason: 'Invalid username or password.' };
  }

  return { ok: true, user };
}

function toPublicUser(user) {
  if (!user) {
    return null;
  }
  const isAdmin = normalizeAdminFlag(user.isAdmin);
  const allowedViews = resolveAllowedViews({
    isAdmin,
    rawAllowedViews: user.allowedViews
  });
  return {
    username: user.username,
    isAdmin,
    allowedViews,
    createdAt: user.createdAt,
    updatedAt: user.updatedAt
  };
}

function hasViewAccess(user, viewNameRaw) {
  if (!user) {
    return false;
  }
  const isAdmin = normalizeAdminFlag(user.isAdmin);
  const viewName = normalizeViewKey(viewNameRaw);
  if (!viewName || !VIEW_KEY_SET.has(viewName)) {
    return false;
  }
  if (isAdmin) {
    return true;
  }
  if (ADMIN_ONLY_VIEW_KEY_SET.has(viewName)) {
    return false;
  }
  const allowedViews = resolveAllowedViews({
    isAdmin,
    rawAllowedViews: user.allowedViews
  });
  return allowedViews.includes(viewName);
}

function loginUser(res, user) {
  const sessionId = createSession(user.username);
  res.cookie(AUTH_COOKIE_NAME, sessionId, authCookieOptions());
}

function logoutRequest(req, res) {
  if (req.authSessionId) {
    authSessions.delete(req.authSessionId);
  }
  res.clearCookie(AUTH_COOKIE_NAME, authCookieClearOptions());
}

function initializeAuth() {
  ensureAuthSchema();
  seedUsersFromEnv();
  ensureFallbackUser();
  setInterval(cleanupExpiredSessions, 5 * 60 * 1000).unref();
}

module.exports = {
  initializeAuth,
  VIEW_KEYS,
  NON_ADMIN_VIEW_KEYS,
  normalizeAllowedViewsInput,
  authSessionMiddleware,
  requireAuth,
  hasViewAccess,
  authenticateCredentials,
  toPublicUser,
  listUsers,
  createUser,
  updateUser,
  deleteUser,
  countAdminUsers,
  loginUser,
  logoutRequest
};
