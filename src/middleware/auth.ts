import { Context, Next } from 'hono';
import { getCookie, setCookie, deleteCookie } from 'hono/cookie';
import type { HonoEnv } from '../types';

const ENCODER = new TextEncoder();

async function sha256(data: string): Promise<string> {
  const hash = await crypto.subtle.digest('SHA-256', ENCODER.encode(data));
  return Array.from(new Uint8Array(hash))
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

async function hmacSign(payload: string, secret: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    'raw',
    ENCODER.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const sig = await crypto.subtle.sign('HMAC', key, ENCODER.encode(payload));
  return btoa(String.fromCharCode(...new Uint8Array(sig)))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '');
}

async function hmacVerify(payload: string, signature: string, secret: string): Promise<boolean> {
  const expected = await hmacSign(payload, secret);
  return expected === signature;
}

function base64UrlEncode(str: string): string {
  return btoa(str).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function base64UrlDecode(str: string): string {
  const padded = str.replace(/-/g, '+').replace(/_/g, '/');
  return atob(padded);
}

export async function createJWT(
  payload: Record<string, unknown>,
  secret: string,
  expiresInSeconds = 86400
): Promise<string> {
  const header = { alg: 'HS256', typ: 'JWT' };
  const now = Math.floor(Date.now() / 1000);
  const fullPayload = { ...payload, iat: now, exp: now + expiresInSeconds };

  const headerB64 = base64UrlEncode(JSON.stringify(header));
  const payloadB64 = base64UrlEncode(JSON.stringify(fullPayload));
  const sigInput = `${headerB64}.${payloadB64}`;
  const signature = await hmacSign(sigInput, secret);
  return `${sigInput}.${signature}`;
}

export async function verifyJWT(
  token: string,
  secret: string
): Promise<Record<string, unknown> | null> {
  const parts = token.split('.');
  if (parts.length !== 3) return null;

  const [headerB64, payloadB64, signature] = parts;
  const sigInput = `${headerB64}.${payloadB64}`;
  const valid = await hmacVerify(sigInput, signature, secret);
  if (!valid) return null;

  try {
    const payload = JSON.parse(base64UrlDecode(payloadB64));
    const now = Math.floor(Date.now() / 1000);
    if (payload.exp && payload.exp < now) return null;
    return payload;
  } catch {
    return null;
  }
}

export async function hashPassword(password: string): Promise<string> {
  // Use SHA-256 with a salt prefix for CF Workers (no bcrypt available)
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const saltHex = Array.from(salt)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
  const hash = await sha256(`${saltHex}:${password}`);
  return `sha256:${saltHex}:${hash}`;
}

export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  if (stored.startsWith('sha256:')) {
    const parts = stored.split(':');
    if (parts.length !== 3) return false;
    const [, salt, expectedHash] = parts;
    const hash = await sha256(`${salt}:${password}`);
    return hash === expectedHash;
  }
  // Fallback: plain text comparison (for initial setup only)
  return password === stored;
}

function getJWTSecret(c: Context<HonoEnv>): string {
  const secret = c.env.JWT_SECRET?.trim();
  if (!secret) {
    throw new Error('JWT_SECRET 未配置');
  }
  return secret;
}

export async function ensureAdminExists(
  db: D1Database,
  env?: HonoEnv['Bindings']
): Promise<void> {
  const row = await db.prepare('SELECT COUNT(*) as cnt FROM admin_users').first<{ cnt: number }>();
  if (row && row.cnt > 0) return;

  const username = env?.ADMIN_USERNAME?.trim() || 'admin';
  const passwordHash = env?.ADMIN_PASSWORD_HASH?.trim();
  const password = env?.ADMIN_PASSWORD?.trim();

  let hash = passwordHash || '';
  if (!hash && password) {
    hash = await hashPassword(password);
  }
  if (!hash) {
    return;
  }

  await db
    .prepare('INSERT OR IGNORE INTO admin_users (username, password_hash) VALUES (?, ?)')
    .bind(username, hash)
    .run();
}

export function authMiddleware() {
  return async (c: Context<HonoEnv>, next: Next) => {
    const path = new URL(c.req.url).pathname;

    // Public routes that don't need auth
    if (
      path === '/login' ||
      path === '/api/auth/login' ||
      path.startsWith('/assets/')
    ) {
      return next();
    }

    const secret = getJWTSecret(c);

    // Check cookie first
    const token = getCookie(c, 'cpa_session');
    if (token) {
      const payload = await verifyJWT(token, secret);
      if (payload) {
        c.set('user', payload);
        return next();
      }
    }

    // Check Authorization header
    const authHeader = c.req.header('Authorization');
    if (authHeader?.startsWith('Bearer ')) {
      const bearerToken = authHeader.slice(7);
      const payload = await verifyJWT(bearerToken, secret);
      if (payload) {
        c.set('user', payload);
        return next();
      }
    }

    // API requests get 401
    if (path.startsWith('/api/')) {
      return c.json({ error: 'Unauthorized' }, 401);
    }

    // Page requests redirect to login
    return c.redirect('/login');
  };
}

export async function handleLogin(c: Context<HonoEnv>): Promise<Response> {
  const body = await c.req.json<{ username: string; password: string }>();
  const { username, password } = body;

  if (!username || !password) {
    return c.json({ error: '请输入用户名和密码' }, 400);
  }

  const row = await c.env.DB.prepare('SELECT * FROM admin_users WHERE username = ?')
    .bind(username)
    .first<{ id: number; username: string; password_hash: string }>();

  if (!row) {
    return c.json({ error: '用户名或密码错误' }, 401);
  }

  const valid = await verifyPassword(password, row.password_hash);
  if (!valid) {
    return c.json({ error: '用户名或密码错误' }, 401);
  }

  // Update last login
  await c.env.DB.prepare('UPDATE admin_users SET last_login_at = datetime(\'now\') WHERE id = ?')
    .bind(row.id)
    .run();

  const secret = getJWTSecret(c);
  const token = await createJWT({ sub: row.id, username: row.username }, secret, 86400 * 7);

  const isSecure = new URL(c.req.url).protocol === 'https:';
  setCookie(c, 'cpa_session', token, {
    httpOnly: false, // allow JS to read for fallback auth
    secure: isSecure,
    sameSite: 'Lax',
    maxAge: 86400 * 7,
    path: '/',
  });

  // Log activity
  await c.env.DB.prepare(
    'INSERT INTO activity_log (action, detail, username) VALUES (?, ?, ?)'
  )
    .bind('login', `用户 ${username} 登录成功`, username)
    .run();

  return c.json({ ok: true, token, username: row.username });
}

export async function handleLogout(c: Context<HonoEnv>): Promise<Response> {
  deleteCookie(c, 'cpa_session', { path: '/' });
  return c.json({ ok: true });
}

export async function handleChangePassword(c: Context<HonoEnv>): Promise<Response> {
  const user = c.get('user') as Record<string, unknown>;
  const body = await c.req.json<{ old_password: string; new_password: string }>();

  if (!body.old_password || !body.new_password) {
    return c.json({ error: '请输入旧密码和新密码' }, 400);
  }
  if (body.new_password.length < 6) {
    return c.json({ error: '新密码至少6位' }, 400);
  }

  const row = await c.env.DB.prepare('SELECT * FROM admin_users WHERE id = ?')
    .bind(user.sub)
    .first<{ id: number; password_hash: string }>();

  if (!row) {
    return c.json({ error: '用户不存在' }, 404);
  }

  const valid = await verifyPassword(body.old_password, row.password_hash);
  if (!valid) {
    return c.json({ error: '旧密码错误' }, 401);
  }

  const newHash = await hashPassword(body.new_password);
  await c.env.DB.prepare('UPDATE admin_users SET password_hash = ?, updated_at = datetime(\'now\') WHERE id = ?')
    .bind(newHash, row.id)
    .run();

  return c.json({ ok: true });
}
