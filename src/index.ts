import { Hono } from 'hono';
import { cors } from 'hono/cors';
import type { HonoEnv } from './types';
import { authMiddleware, ensureAdminExists } from './middleware/auth';
import apiRoutes from './routes/api';
import pageRoutes from './routes/pages';
import { loadConfig, validateConfig, saveCronMeta } from './core/config';
import { createTask, logActivity, updateTask } from './core/db';
import { runMaintain } from './core/engine';

const app = new Hono<HonoEnv>();

// Force UTF-8 for all responses to avoid garbled Chinese text
app.use('*', async (c, next) => {
  await next();
  const contentType = c.res.headers.get('content-type') || '';
  if (contentType.includes('text/html') && !contentType.toLowerCase().includes('charset=')) {
    c.res.headers.set('content-type', 'text/html; charset=utf-8');
  }
  if (contentType.includes('application/json') && !contentType.toLowerCase().includes('charset=')) {
    c.res.headers.set('content-type', 'application/json; charset=utf-8');
  }
});

// CORS for API
app.use('/api/*', cors({
  origin: '*',
  allowMethods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
  allowHeaders: ['Content-Type', 'Authorization'],
  credentials: true,
}));

// Init middleware: ensure admin user exists and DB tables are ready
app.use('*', async (c, next) => {
  try {
    await ensureAdminExists(c.env.DB, c.env);
  } catch {
    // Tables might not exist yet on first deploy — that's ok,
    // they'll be created by the migration script.
  }
  return next();
});

// Auth middleware (skips /login and /api/auth/login)
app.use('*', authMiddleware());

// API routes
app.route('/api', apiRoutes);

// Page routes (HTML)
app.route('/', pageRoutes);

// 404 fallback
app.notFound((c) => {
  if (c.req.url.includes('/api/')) {
    return c.json({ error: 'Not Found' }, 404);
  }
  return c.redirect('/');
});

// Global error handler
app.onError((err, c) => {
  console.error('Unhandled error:', err);
  if (c.req.url.includes('/api/')) {
    return c.json({ error: err.message || 'Internal Server Error' }, 500);
  }
  return c.text('Internal Server Error', 500);
});

const CRON_LOCK_KEY = 'cron:maintain:lock';
const CRON_LOCK_TTL_SECONDS = 300; // 5 minutes — safety margin for long runs

async function acquireCronLock(kv: KVNamespace): Promise<boolean> {
  const existing = await kv.get(CRON_LOCK_KEY);
  if (existing) return false;
  // KV put is not atomic, but combined with TTL it provides sufficient
  // protection against overlapping cron runs in practice.
  await kv.put(CRON_LOCK_KEY, new Date().toISOString(), { expirationTtl: CRON_LOCK_TTL_SECONDS });
  return true;
}

async function releaseCronLock(kv: KVNamespace): Promise<void> {
  try { await kv.delete(CRON_LOCK_KEY); } catch { /* best-effort */ }
}

async function runScheduledMaintain(env: HonoEnv['Bindings'], cronExpression: string): Promise<void> {
  const now = new Date().toISOString();
  const cronExpr = cronExpression || '*/30 * * * *';
  let taskId: number | null = null;

  // ── distributed lock: skip if another cron run is still in progress ──
  const acquired = await acquireCronLock(env.KV);
  if (!acquired) {
    await logActivity(env.DB, 'cron_maintain_skipped', `定时任务跳过: 上一次 cron 仍在执行中 (lock=${CRON_LOCK_KEY})`, 'system');
    return;
  }

  try {
    await saveCronMeta(env.DB, {
      cron_last_run_at: now,
      cron_last_result: 'running',
      cron_last_error: '',
      cron_expression: cronExpr,
    });

    taskId = await createTask(env.DB, 'cron-maintain', { source: 'cron', cron: cronExpr });

    try {
      await ensureAdminExists(env.DB, env);
    } catch {
      // migration may not be ready yet
    }

    const config = await loadConfig(env.DB, env);
    const errors = validateConfig(config);
    if (errors.length > 0) {
      await saveCronMeta(env.DB, {
        cron_last_run_at: now,
        cron_last_result: 'skipped',
        cron_last_error: errors.join('; '),
        cron_expression: cronExpr,
      });
      if (taskId != null) {
        await updateTask(env.DB, taskId, {
          status: 'completed',
          started_at: now,
          finished_at: new Date().toISOString(),
          error: null,
          result: JSON.stringify({
            success: true,
            skipped: true,
            reason: errors.join('; '),
            cron_expression: cronExpr,
          }),
        });
      }
      await logActivity(env.DB, 'cron_maintain_started', `定时维护触发: cron=${cronExpr}`, 'system');
      await logActivity(env.DB, 'cron_maintain_skipped', `定时任务跳过，配置无效: ${errors.join('; ')}`, 'system');
      return;
    }

    await logActivity(env.DB, 'cron_maintain_started', `定时维护开始执行: cron=${cronExpr}`, 'system');

    try {
      await runMaintain(env.DB, config, taskId, 'system');
      await saveCronMeta(env.DB, {
        cron_last_run_at: now,
        cron_last_result: 'success',
        cron_last_error: '',
        cron_expression: cronExpr,
      });
      await logActivity(env.DB, 'cron_maintain_completed', `定时维护执行完成: cron=${cronExpr}`, 'system');
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      await saveCronMeta(env.DB, {
        cron_last_run_at: now,
        cron_last_result: 'failed',
        cron_last_error: msg,
        cron_expression: cronExpr,
      });
      if (taskId != null) {
        await updateTask(env.DB, taskId, {
          status: 'failed',
          finished_at: new Date().toISOString(),
          error: msg,
        });
      }
      await logActivity(env.DB, 'cron_maintain_failed', `定时维护执行失败: cron=${cronExpr} | ${msg}`, 'system');
      throw err;
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    try {
      await saveCronMeta(env.DB, {
        cron_last_run_at: now,
        cron_last_result: 'failed',
        cron_last_error: msg,
        cron_expression: cronExpr,
      });
      if (taskId != null) {
        await updateTask(env.DB, taskId, {
          status: 'failed',
          finished_at: new Date().toISOString(),
          error: msg,
        });
      }
      await logActivity(env.DB, 'cron_maintain_fatal', `定时维护顶层失败: cron=${cronExpr} | ${msg}`, 'system');
    } catch {
      // ignore secondary failure while recording fatal cron state
    }
    throw err;
  } finally {
    await releaseCronLock(env.KV);
  }
}

export default {
  fetch: app.fetch,
  scheduled: async (event: ScheduledEvent, env: HonoEnv['Bindings'], ctx: ExecutionContext) => {
    ctx.waitUntil(runScheduledMaintain(env, event.cron || '*/30 * * * *'));
  },
};
