/**
 * D1 database access layer — mirrors the Python SQLite operations.
 */

const AUTH_ACCOUNT_COLUMNS = [
  'name', 'disabled', 'id_token_json', 'email', 'provider', 'source',
  'unavailable', 'auth_index', 'account', 'type', 'runtime_only',
  'status', 'status_message', 'chatgpt_account_id', 'id_token_plan_type',
  'auth_updated_at', 'auth_modtime', 'auth_last_refresh',
  'api_http_status', 'api_status_code', 'usage_allowed', 'usage_limit_reached',
  'usage_plan_type', 'usage_email', 'usage_reset_at', 'usage_reset_after_seconds',
  'usage_spark_allowed', 'usage_spark_limit_reached',
  'usage_spark_reset_at', 'usage_spark_reset_after_seconds',
  'quota_signal_source', 'is_invalid_401', 'is_quota_limited', 'is_recovered',
  'probe_error_kind', 'probe_error_text', 'managed_reason',
  'last_action', 'last_action_status', 'last_action_error',
  'last_seen_at', 'last_probed_at', 'updated_at',
];

export async function upsertAuthAccounts(
  db: D1Database,
  rows: Record<string, unknown>[]
): Promise<void> {
  if (rows.length === 0) return;

  // D1 batch limit: process in chunks of 50
  const CHUNK = 50;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const stmts = chunk.map((row) => {
      const cols = AUTH_ACCOUNT_COLUMNS;
      const placeholders = cols.map(() => '?').join(', ');
      const updates = cols
        .filter((c) => c !== 'name')
        .map((c) => `${c} = excluded.${c}`)
        .join(', ');
      const values = cols.map((c) => row[c] ?? null);
      return db
        .prepare(
          `INSERT INTO auth_accounts (${cols.join(', ')}) VALUES (${placeholders}) ON CONFLICT(name) DO UPDATE SET ${updates}`
        )
        .bind(...values);
    });
    await db.batch(stmts);
  }
}

export async function loadExistingState(
  db: D1Database
): Promise<Map<string, Record<string, unknown>>> {
  const result = await db.prepare('SELECT * FROM auth_accounts').all();
  const map = new Map<string, Record<string, unknown>>();
  for (const row of result.results) {
    const r = row as Record<string, unknown>;
    map.set(String(r.name), r);
  }
  return map;
}

export async function startScanRun(
  db: D1Database,
  mode: string,
  settings: Record<string, unknown>
): Promise<number> {
  const now = new Date().toISOString();
  const result = await db
    .prepare(
      `INSERT INTO scan_runs (mode, started_at, status, total_files, filtered_files, probed_files, invalid_401_count, quota_limited_count, recovered_count, delete_401, quota_action, probe_workers, action_workers, timeout_seconds, retries) VALUES (?, ?, 'running', 0, 0, 0, 0, 0, 0, ?, ?, ?, ?, ?, ?)`
    )
    .bind(
      mode,
      now,
      settings.delete_401 ? 1 : 0,
      String(settings.quota_action ?? 'disable'),
      Number(settings.probe_workers ?? 100),
      Number(settings.action_workers ?? 100),
      Number(settings.timeout ?? 15),
      Number(settings.retries ?? 3)
    )
    .run();
  return result.meta.last_row_id as number;
}

export async function finishScanRun(
  db: D1Database,
  runId: number,
  data: {
    status: string;
    total_files: number;
    filtered_files: number;
    probed_files: number;
    invalid_401_count: number;
    quota_limited_count: number;
    recovered_count: number;
  }
): Promise<void> {
  await db
    .prepare(
      `UPDATE scan_runs SET finished_at = ?, status = ?, total_files = ?, filtered_files = ?, probed_files = ?, invalid_401_count = ?, quota_limited_count = ?, recovered_count = ? WHERE run_id = ?`
    )
    .bind(
      new Date().toISOString(),
      data.status,
      data.total_files,
      data.filtered_files,
      data.probed_files,
      data.invalid_401_count,
      data.quota_limited_count,
      data.recovered_count,
      runId
    )
    .run();
}

export async function getLastScanRun(db: D1Database): Promise<Record<string, unknown> | null> {
  const row = await db
    .prepare('SELECT * FROM scan_runs ORDER BY run_id DESC LIMIT 1')
    .first();
  return row as Record<string, unknown> | null;
}

export async function getScanRuns(
  db: D1Database,
  limit = 20,
  offset = 0
): Promise<{ rows: Record<string, unknown>[]; total: number }> {
  const countRow = await db.prepare('SELECT COUNT(*) as cnt FROM scan_runs').first<{ cnt: number }>();
  const total = countRow?.cnt ?? 0;
  const result = await db
    .prepare('SELECT * FROM scan_runs ORDER BY run_id DESC LIMIT ? OFFSET ?')
    .bind(limit, offset)
    .all();
  return { rows: result.results as Record<string, unknown>[], total };
}

export async function getAccounts(
  db: D1Database,
  opts: {
    limit?: number;
    offset?: number;
    filter?: string;
    sort?: string;
    order?: 'asc' | 'desc';
    type?: string;
    provider?: string;
    status_filter?: string;
  } = {}
): Promise<{ rows: Record<string, unknown>[]; total: number }> {
  const conditions: string[] = [];
  const params: unknown[] = [];

  if (opts.type) {
    conditions.push('type = ?');
    params.push(opts.type);
  }
  if (opts.provider) {
    conditions.push('provider = ?');
    params.push(opts.provider);
  }
  if (opts.filter) {
    conditions.push('(name LIKE ? OR email LIKE ? OR account LIKE ?)');
    const like = `%${opts.filter}%`;
    params.push(like, like, like);
  }
  if (opts.status_filter) {
    switch (opts.status_filter) {
      case 'active':
        conditions.push("disabled = 0 AND is_invalid_401 = 0 AND is_quota_limited = 0 AND (probe_error_kind IS NULL OR probe_error_kind = '')");
        break;
      case 'disabled':
        conditions.push('disabled = 1');
        break;
      case 'invalid_401':
        conditions.push('is_invalid_401 = 1');
        break;
      case 'quota_limited':
        conditions.push('is_quota_limited = 1');
        break;
      case 'recovered':
        conditions.push('is_recovered = 1');
        break;
      case 'probe_error':
        conditions.push("probe_error_kind IS NOT NULL AND probe_error_kind != ''");
        break;
    }
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  const sortCol = opts.sort || 'updated_at';
  const sortOrder = opts.order || 'desc';
  const allowedCols = new Set(AUTH_ACCOUNT_COLUMNS);
  const safeSort = allowedCols.has(sortCol) ? sortCol : 'updated_at';
  const safeOrder = sortOrder === 'asc' ? 'ASC' : 'DESC';

  const countResult = await db
    .prepare(`SELECT COUNT(*) as cnt FROM auth_accounts ${where}`)
    .bind(...params)
    .first<{ cnt: number }>();
  const total = countResult?.cnt ?? 0;

  const limit = Math.min(opts.limit || 50, 500);
  const offset = opts.offset || 0;

  const result = await db
    .prepare(
      `SELECT * FROM auth_accounts ${where} ORDER BY ${safeSort} ${safeOrder} LIMIT ? OFFSET ?`
    )
    .bind(...params, limit, offset)
    .all();

  return { rows: result.results as Record<string, unknown>[], total };
}

export async function getAccountByName(
  db: D1Database,
  name: string
): Promise<Record<string, unknown> | null> {
  const row = await db.prepare('SELECT * FROM auth_accounts WHERE name = ?').bind(name).first();
  return row as Record<string, unknown> | null;
}

export async function deleteAccountFromDB(db: D1Database, name: string): Promise<void> {
  await db.prepare('DELETE FROM auth_accounts WHERE name = ?').bind(name).run();
}

export async function deleteAccountsFromDB(
  db: D1Database,
  names: string[]
): Promise<number> {
  if (names.length === 0) return 0;

  const CHUNK = 200;
  let deleted = 0;
  for (let i = 0; i < names.length; i += CHUNK) {
    const chunk = names.slice(i, i + CHUNK);
    const placeholders = chunk.map(() => '?').join(', ');
    const result = await db
      .prepare(`DELETE FROM auth_accounts WHERE name IN (${placeholders})`)
      .bind(...chunk)
      .run();
    deleted += Number(result.meta.changes || 0);
  }

  return deleted;
}

export async function deleteAccountsNotInSet(
  db: D1Database,
  keepNames: string[]
): Promise<number> {
  if (keepNames.length === 0) {
    const result = await db.prepare('DELETE FROM auth_accounts').run();
    return Number(result.meta.changes || 0);
  }

  const CHUNK = 200;
  let deleted = 0;
  for (let i = 0; i < keepNames.length; i += CHUNK) {
    const chunk = keepNames.slice(i, i + CHUNK);
    const placeholders = chunk.map(() => '?').join(', ');
    if (i === 0) {
      // delete all not in first chunk, then insert remaining chunks into temp keep set is overkill for local D1.
      // Simpler: rebuild by deleting stale names via separate query below.
    }
  }

  const existing = await db.prepare('SELECT name FROM auth_accounts').all<{ name: string }>();
  const keepSet = new Set(keepNames);
  const staleNames = existing.results.map((r) => r.name).filter((n) => !keepSet.has(n));
  if (staleNames.length === 0) return 0;

  for (let i = 0; i < staleNames.length; i += CHUNK) {
    const chunk = staleNames.slice(i, i + CHUNK);
    const placeholders = chunk.map(() => '?').join(', ');
    const result = await db.prepare(`DELETE FROM auth_accounts WHERE name IN (${placeholders})`).bind(...chunk).run();
    deleted += Number(result.meta.changes || 0);
  }
  return deleted;
}

export async function updateAccountDisabledState(
  db: D1Database,
  name: string,
  disabled: boolean
): Promise<void> {
  await db
    .prepare("UPDATE auth_accounts SET disabled = ?, updated_at = ?, last_action_status = 'success' WHERE name = ?")
    .bind(disabled ? 1 : 0, new Date().toISOString(), name)
    .run();
}

export async function getDashboardStats(db: D1Database): Promise<Record<string, unknown>> {
  const stats = await db.batch([
    db.prepare('SELECT COUNT(*) as cnt FROM auth_accounts'),
    db.prepare('SELECT COUNT(*) as cnt FROM auth_accounts WHERE disabled = 0 AND is_invalid_401 = 0 AND is_quota_limited = 0 AND (probe_error_kind IS NULL OR probe_error_kind = \'\')'),
    db.prepare('SELECT COUNT(*) as cnt FROM auth_accounts WHERE disabled = 1'),
    db.prepare('SELECT COUNT(*) as cnt FROM auth_accounts WHERE is_invalid_401 = 1'),
    db.prepare('SELECT COUNT(*) as cnt FROM auth_accounts WHERE is_quota_limited = 1'),
    db.prepare('SELECT COUNT(*) as cnt FROM auth_accounts WHERE is_recovered = 1'),
    db.prepare('SELECT COUNT(*) as cnt FROM auth_accounts WHERE probe_error_kind IS NOT NULL AND probe_error_kind != \'\''),
    db.prepare('SELECT * FROM scan_runs ORDER BY run_id DESC LIMIT 1'),
    db.prepare('SELECT * FROM activity_log ORDER BY id DESC LIMIT 10'),
    db.prepare("SELECT created_at FROM activity_log WHERE action = 'cron_maintain_started' ORDER BY id DESC LIMIT 1"),
    db.prepare("SELECT created_at FROM activity_log WHERE action = 'cron_maintain_completed' ORDER BY id DESC LIMIT 1"),
    db.prepare("SELECT created_at FROM activity_log WHERE action = 'cron_maintain_failed' ORDER BY id DESC LIMIT 1"),
  ]);

  const cnt = (r: D1Result, idx = 0) => {
    const rows = r.results as Record<string, unknown>[];
    return rows.length > 0 ? Number((rows[idx] as Record<string, unknown>).cnt ?? 0) : 0;
  };

  const cronStarted = ((stats[9].results as Record<string, unknown>[])[0]?.created_at as string | undefined) ?? null;
  const cronCompleted = ((stats[10].results as Record<string, unknown>[])[0]?.created_at as string | undefined) ?? null;
  const cronFailed = ((stats[11].results as Record<string, unknown>[])[0]?.created_at as string | undefined) ?? null;

  let cronDurationSeconds: number | null = null;
  if (cronStarted && cronCompleted) {
    const startMs = Date.parse(cronStarted.replace(' ', 'T') + 'Z');
    const endMs = Date.parse(cronCompleted.replace(' ', 'T') + 'Z');
    if (Number.isFinite(startMs) && Number.isFinite(endMs) && endMs >= startMs) {
      cronDurationSeconds = Math.round((endMs - startMs) / 1000);
    }
  }

  let cronStatus: 'success' | 'failed' | 'running' | 'never' = 'never';
  if (cronCompleted) {
    cronStatus = 'success';
  } else if (cronFailed) {
    cronStatus = 'failed';
  } else if (cronStarted) {
    cronStatus = 'running';
  }

  return {
    total_accounts: cnt(stats[0]),
    active_accounts: cnt(stats[1]),
    disabled_accounts: cnt(stats[2]),
    invalid_401: cnt(stats[3]),
    quota_limited: cnt(stats[4]),
    recovered: cnt(stats[5]),
    probe_errors: cnt(stats[6]),
    last_scan: (stats[7].results as Record<string, unknown>[])[0] ?? null,
    recent_activity: stats[8].results as Record<string, unknown>[],
    cron_summary: {
      last_started_at: cronStarted,
      last_completed_at: cronCompleted,
      last_duration_seconds: cronDurationSeconds,
      last_status: cronStatus,
    },
  };
}

export async function logActivity(
  db: D1Database,
  action: string,
  detail: string,
  username?: string
): Promise<void> {
  await db
    .prepare('INSERT INTO activity_log (action, detail, username) VALUES (?, ?, ?)')
    .bind(action, detail, username ?? null)
    .run();
}

export async function getActivityLog(
  db: D1Database,
  limit = 50,
  offset = 0
): Promise<{ rows: Record<string, unknown>[]; total: number }> {
  const countRow = await db.prepare('SELECT COUNT(*) as cnt FROM activity_log').first<{ cnt: number }>();
  const total = countRow?.cnt ?? 0;
  const result = await db
    .prepare('SELECT * FROM activity_log ORDER BY id DESC LIMIT ? OFFSET ?')
    .bind(limit, offset)
    .all();
  return { rows: result.results as Record<string, unknown>[], total };
}

export async function getTaskById(db: D1Database, id: number): Promise<Record<string, unknown> | null> {
  return (await db.prepare('SELECT * FROM task_queue WHERE id = ?').bind(id).first()) as Record<string, unknown> | null;
}

export async function createTask(
  db: D1Database,
  type: string,
  params: Record<string, unknown> = {}
): Promise<number> {
  const result = await db
    .prepare("INSERT INTO task_queue (type, status, params) VALUES (?, 'pending', ?)")
    .bind(type, JSON.stringify(params))
    .run();
  return result.meta.last_row_id as number;
}

export async function updateTask(
  db: D1Database,
  id: number,
  data: Record<string, unknown>
): Promise<void> {
  const sets: string[] = [];
  const vals: unknown[] = [];
  for (const [k, v] of Object.entries(data)) {
    sets.push(`${k} = ?`);
    vals.push(v);
  }
  if (sets.length === 0) return;
  vals.push(id);
  await db.prepare(`UPDATE task_queue SET ${sets.join(', ')} WHERE id = ?`).bind(...vals).run();
}

export async function getRecentTasks(
  db: D1Database,
  limit = 20
): Promise<Record<string, unknown>[]> {
  const result = await db
    .prepare('SELECT * FROM task_queue ORDER BY id DESC LIMIT ?')
    .bind(limit)
    .all();
  return result.results as Record<string, unknown>[];
}
