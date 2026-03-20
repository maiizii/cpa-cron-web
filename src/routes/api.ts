import { Hono } from 'hono';
import type { HonoEnv } from '../types';
import {
  handleLogin,
  handleLogout,
  handleChangePassword,
} from '../middleware/auth';
import { loadConfig, saveConfig, validateConfig, loadCacheMeta, loadCronMeta } from '../core/config';
import {
  getDashboardStats,
  getAccounts,
  deleteAccountFromDB,
  updateAccountDisabledState,
  getScanRuns,
  getActivityLog,
  logActivity,
  getRecentTasks,
  getTaskById,
  createTask,
  updateTask,
} from '../core/db';
import { runScan, runMaintain, runUpload } from '../core/engine';
import type { UploadFileItem } from '../core/engine';
import { deleteAccount, setAccountDisabled } from '../core/cpa-client';

const api = new Hono<HonoEnv>();

// ── Auth ─────────────────────────────────────────────────────────────

api.post('/auth/login', handleLogin);
api.post('/auth/logout', handleLogout);
api.post('/auth/change-password', handleChangePassword);

api.get('/auth/me', async (c) => {
  const user = c.get('user') as Record<string, unknown>;
  return c.json({ ok: true, user: { username: user?.username, sub: user?.sub } });
});

// ── Dashboard ────────────────────────────────────────────────────────

api.get('/dashboard', async (c) => {
  const stats = await getDashboardStats(c.env.DB);
  const cron = await loadCronMeta(c.env.DB);
  return c.json({ ...stats, cron });
});

// ── Config ───────────────────────────────────────────────────────────

api.get('/config', async (c) => {
  const config = await loadConfig(c.env.DB, c.env);
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(config)) {
    if (k === 'token') {
      out[k] = v ? '***' + String(v).slice(-6) : '';
    } else {
      out[k] = String(v);
    }
  }
  return c.json(out);
});

api.put('/config', async (c) => {
  const body = await c.req.json();
  if (body.token && typeof body.token === 'string' && body.token.startsWith('***')) {
    delete body.token;
  }
  await saveConfig(c.env.DB, body);
  const user = c.get('user') as Record<string, unknown>;
  await logActivity(c.env.DB, 'config_update', '配置已更新', String(user?.username ?? ''));
  return c.json({ ok: true });
});

api.get('/config/validate', async (c) => {
  const config = await loadConfig(c.env.DB, c.env);
  const errors = validateConfig(config);
  return c.json({ valid: errors.length === 0, errors });
});

api.post('/config/test', async (c) => {
  const config = await loadConfig(c.env.DB, c.env);
  let body: Record<string, string> = {};
  try { body = await c.req.json(); } catch { /* empty body is fine */ }
  const baseUrl = (body.base_url?.trim() || config.base_url || '').replace(/\/+$/, '');
  const token = body.token?.trim() || config.token || '';

  if (!baseUrl) return c.json({ ok: false, error: '请先配置 base_url' });
  if (!token) return c.json({ ok: false, error: '请先配置 token' });

  const targetUrl = `${baseUrl}/v0/management/auth-files`;
  try { new URL(targetUrl); } catch {
    return c.json({ ok: false, error: `base_url 格式无效: ${baseUrl}` });
  }

  const timeoutMs = Math.max(5, config.timeout || 15) * 1000;
  let resp: Response;
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      resp = await fetch(targetUrl, {
        headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
        signal: controller.signal,
      });
    } finally { clearTimeout(timer); }
  } catch (e: unknown) {
    const errStr = String(e);
    if (errStr.includes('AbortError') || errStr.includes('abort'))
      return c.json({ ok: false, error: `连接超时 (${timeoutMs / 1000}s)，请检查 base_url 是否正确` });
    if (errStr.includes('internal error'))
      return c.json({ ok: false, error: `无法连接到目标服务器，请检查 base_url 是否正确、服务是否在线: ${baseUrl}` });
    return c.json({ ok: false, error: `连接失败: ${errStr.slice(0, 200)}` });
  }

  if (resp.status === 401 || resp.status === 403) {
    let detail = '';
    try { detail = await resp.text(); } catch { /* ignore */ }
    const lowered = detail.toLowerCase();

    if (lowered.includes('ip banned') || lowered.includes('too many failed attempts')) {
      const compact = detail.replace(/\s+/g, ' ').trim().slice(0, 240);
      return c.json({ ok: false, error: `当前出口 IP 已被风控封禁: ${compact || '请稍后再试'}` });
    }

    if (resp.status === 401) {
      return c.json({ ok: false, error: `认证失败 (HTTP 401)，请检查 token 是否正确${detail ? '，远端返回: ' + detail.slice(0, 160) : ''}` });
    }

    return c.json({ ok: false, error: `访问被拒绝 (HTTP 403)${detail ? '，远端返回: ' + detail.slice(0, 180) : '，请检查 token 权限或服务风控状态'}` });
  }
  if (!resp.ok) {
    let detail = '';
    try { detail = (await resp.text()).slice(0, 200); } catch { /* */ }
    return c.json({ ok: false, error: `HTTP ${resp.status}${detail ? ': ' + detail : ''}` });
  }
  try {
    const data = await resp.json() as Record<string, unknown>;
    const files = Array.isArray(data.files) ? data.files : [];
    return c.json({ ok: true, message: `连接成功! 共 ${files.length} 个认证文件` });
  } catch {
    return c.json({ ok: false, error: '返回内容不是有效 JSON' });
  }
});

// ── Accounts ─────────────────────────────────────────────────────────

api.get('/accounts', async (c) => {
  const limit = parseInt(c.req.query('limit') || '50', 10);
  const offset = parseInt(c.req.query('offset') || '0', 10);
  const filter = c.req.query('filter') || '';
  const sort = c.req.query('sort') || 'updated_at';
  const order = (c.req.query('order') || 'desc') as 'asc' | 'desc';
  const type = c.req.query('type') || '';
  const provider = c.req.query('provider') || '';
  const status_filter = c.req.query('status') || '';
  const result = await getAccounts(c.env.DB, { limit, offset, filter, sort, order, type, provider, status_filter });
  return c.json(result);
});

api.get('/accounts/meta', async (c) => {
  const config = await loadConfig(c.env.DB, c.env);
  const cache = await loadCacheMeta(c.env.DB);
  return c.json({
    current_base_url: config.base_url,
    cache_base_url: cache.cache_base_url,
    cache_last_success_at: cache.cache_last_success_at,
    cache_last_status: cache.cache_last_status,
    cache_last_error: cache.cache_last_error,
    cache_matches_current: !!config.base_url && !!cache.cache_base_url && config.base_url === cache.cache_base_url,
  });
});

api.delete('/accounts/:name', async (c) => {
  const name = decodeURIComponent(c.req.param('name'));
  const config = await loadConfig(c.env.DB, c.env);
  if (!config.base_url || !config.token)
    return c.json({ error: 'CPA 配置不完整' }, 400);

  const result = await deleteAccount(config.base_url, config.token, name, config.timeout, config.delete_retries);
  if (result.ok) {
    await deleteAccountFromDB(c.env.DB, name);
    const user = c.get('user') as Record<string, unknown>;
    await logActivity(c.env.DB, 'delete_account', `删除账号: ${name}`, String(user?.username ?? ''));
  }
  return c.json(result);
});

api.post('/accounts/:name/toggle', async (c) => {
  const name = decodeURIComponent(c.req.param('name'));
  const body = await c.req.json<{ disabled: boolean }>();
  const config = await loadConfig(c.env.DB, c.env);
  if (!config.base_url || !config.token)
    return c.json({ error: 'CPA 配置不完整' }, 400);

  const result = await setAccountDisabled(config.base_url, config.token, name, body.disabled, config.timeout);
  if (result.ok) {
    await updateAccountDisabledState(c.env.DB, name, body.disabled);
    const user = c.get('user') as Record<string, unknown>;
    await logActivity(c.env.DB, 'toggle_account', `${body.disabled ? '禁用' : '启用'}账号: ${name}`, String(user?.username ?? ''));
  }
  return c.json(result);
});

// ── Operations (async via waitUntil) ─────────────────────────────────

/** Helper: get ExecutionContext from c.executionCtx */
function getCtx(c: { executionCtx: ExecutionContext }): ExecutionContext {
  return c.executionCtx;
}

api.post('/operations/scan', async (c) => {
  const config = await loadConfig(c.env.DB, c.env);
  const errors = validateConfig(config);
  if (errors.length > 0)
    return c.json({ error: '配置验证失败', details: errors }, 400);

  const user = c.get('user') as Record<string, unknown>;
  const username = String(user?.username ?? '');
  const taskId = await createTask(c.env.DB, 'scan', { username });

  // Run in background — response returns immediately
  getCtx(c).waitUntil(runScan(c.env.DB, config, taskId, username));

  return c.json({ ok: true, task_id: taskId });
});

api.post('/operations/maintain', async (c) => {
  const config = await loadConfig(c.env.DB, c.env);
  const errors = validateConfig(config);
  if (errors.length > 0)
    return c.json({ error: '配置验证失败', details: errors }, 400);

  const user = c.get('user') as Record<string, unknown>;
  const username = String(user?.username ?? '');
  const taskId = await createTask(c.env.DB, 'maintain', { username });

  getCtx(c).waitUntil(runMaintain(c.env.DB, config, taskId, username));

  return c.json({ ok: true, task_id: taskId });
});

api.post('/operations/upload', async (c) => {
  const config = await loadConfig(c.env.DB, c.env);
  const errors = validateConfig(config);
  if (errors.length > 0)
    return c.json({ error: '配置验证失败', details: errors }, 400);

  const contentType = c.req.header('Content-Type') || '';
  let files: UploadFileItem[] = [];

  if (contentType.includes('multipart/form-data')) {
    const formData = await c.req.formData();
    const entries = formData.getAll('files');
    for (const entry of entries) {
      if (typeof entry === 'object' && entry !== null && 'text' in entry) {
        const fileEntry = entry as unknown as { name: string; text(): Promise<string> };
        const text = await fileEntry.text();
        try { JSON.parse(text); } catch { continue; }
        files.push({ file_name: fileEntry.name, content: text });
      }
    }
  } else {
    const body = await c.req.json<{ files: UploadFileItem[] }>();
    files = body.files || [];
  }

  if (files.length === 0) return c.json({ error: '未提供上传文件' }, 400);

  const user = c.get('user') as Record<string, unknown>;
  const username = String(user?.username ?? '');
  const taskId = await createTask(c.env.DB, 'upload', { username, file_count: files.length });

  getCtx(c).waitUntil(runUpload(c.env.DB, config, files, taskId, username));

  return c.json({ ok: true, task_id: taskId });
});

// ── Export ────────────────────────────────────────────────────────────

api.get('/export/invalid', async (c) => {
  const result = await getAccounts(c.env.DB, { status_filter: 'invalid_401', limit: 500 });
  return c.json(result.rows);
});

api.get('/export/quota', async (c) => {
  const result = await getAccounts(c.env.DB, { status_filter: 'quota_limited', limit: 500 });
  return c.json(result.rows);
});

// ── Scan History ─────────────────────────────────────────────────────

api.get('/scan-runs', async (c) => {
  const limit = parseInt(c.req.query('limit') || '20', 10);
  const offset = parseInt(c.req.query('offset') || '0', 10);
  const result = await getScanRuns(c.env.DB, limit, offset);
  return c.json(result);
});

// ── Activity Log ─────────────────────────────────────────────────────

api.get('/activity', async (c) => {
  const limit = parseInt(c.req.query('limit') || '50', 10);
  const offset = parseInt(c.req.query('offset') || '0', 10);
  const result = await getActivityLog(c.env.DB, limit, offset);
  return c.json(result);
});

// ── Tasks ────────────────────────────────────────────────────────────

api.get('/tasks', async (c) => {
  const tasks = await getRecentTasks(c.env.DB);
  return c.json(tasks);
});

api.get('/tasks/:id', async (c) => {
  const id = parseInt(c.req.param('id'), 10);
  const task = await getTaskById(c.env.DB, id);
  if (!task) return c.json({ error: '任务不存在' }, 404);
  return c.json(task);
});

export default api;
