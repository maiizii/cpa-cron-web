import type { AppConfig, Env } from '../types';

export interface CacheMeta {
  cache_base_url: string;
  cache_last_success_at: string;
  cache_last_status: string;
  cache_last_error: string;
}

export interface CronMeta {
  cron_expression: string;
  cron_last_run_at: string;
  cron_last_result: string;
  cron_last_error: string;
}

const CONFIG_KEYS: (keyof AppConfig)[] = [
  'base_url', 'token', 'target_type', 'provider',
  'probe_workers', 'action_workers', 'timeout', 'retries', 'delete_retries',
  'quota_action', 'quota_disable_threshold', 'delete_401', 'auto_reenable',
  'reenable_scope', 'upload_workers', 'upload_retries', 'upload_method',
  'upload_force', 'min_valid_accounts', 'refill_strategy', 'user_agent',
];

const BOOL_KEYS: (keyof AppConfig)[] = ['delete_401', 'auto_reenable', 'upload_force'];
const INT_KEYS: (keyof AppConfig)[] = [
  'probe_workers', 'action_workers', 'timeout', 'retries', 'delete_retries',
  'upload_workers', 'upload_retries', 'min_valid_accounts',
];
const FLOAT_KEYS: (keyof AppConfig)[] = ['quota_disable_threshold'];

function parseBool(val: string): boolean {
  return ['true', '1', 'yes', 'on'].includes(val.toLowerCase().trim());
}

export async function loadConfig(db: D1Database, env?: Env): Promise<AppConfig> {
  const rows = await db.prepare('SELECT key, value FROM app_config').all<{ key: string; value: string }>();
  const map = new Map<string, string>();
  for (const row of rows.results) {
    map.set(row.key, row.value);
  }

  const envBaseUrl = (env?.CPA_BASE_URL || '').trim();
  const envToken = (env?.CPA_TOKEN || '').trim();

  return {
    base_url: (map.get('base_url') || envBaseUrl || '').trim(),
    token: (map.get('token') || envToken || '').trim(),
    target_type: (map.get('target_type') || 'codex').trim(),
    provider: (map.get('provider') || '').trim(),
    probe_workers: parseInt(map.get('probe_workers') || '100', 10),
    action_workers: parseInt(map.get('action_workers') || '100', 10),
    timeout: parseInt(map.get('timeout') || '15', 10),
    retries: parseInt(map.get('retries') || '3', 10),
    delete_retries: parseInt(map.get('delete_retries') || '2', 10),
    quota_action: (map.get('quota_action') || 'disable') as 'disable' | 'delete',
    quota_disable_threshold: parseFloat(map.get('quota_disable_threshold') || '0'),
    delete_401: parseBool(map.get('delete_401') || 'true'),
    auto_reenable: parseBool(map.get('auto_reenable') || 'true'),
    reenable_scope: (map.get('reenable_scope') || 'signal') as 'signal' | 'managed',
    upload_workers: parseInt(map.get('upload_workers') || '20', 10),
    upload_retries: parseInt(map.get('upload_retries') || '2', 10),
    upload_method: (map.get('upload_method') || 'json') as 'json' | 'multipart',
    upload_force: parseBool(map.get('upload_force') || 'false'),
    min_valid_accounts: parseInt(map.get('min_valid_accounts') || '100', 10),
    refill_strategy: (map.get('refill_strategy') || 'to-threshold') as 'to-threshold' | 'fixed',
    user_agent: (map.get('user_agent') || 'codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal').trim(),
  };
}

export async function saveConfig(db: D1Database, config: Partial<AppConfig>): Promise<void> {
  const batch: D1PreparedStatement[] = [];
  for (const [key, value] of Object.entries(config)) {
    if (!CONFIG_KEYS.includes(key as keyof AppConfig)) continue;
    const strValue = typeof value === 'boolean' ? String(value) : String(value);
    batch.push(
      db.prepare("INSERT INTO app_config (key, value, updated_at) VALUES (?, ?, datetime('now')) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at")
        .bind(key, strValue)
    );
  }
  if (batch.length > 0) {
    await db.batch(batch);
  }
}

export function validateConfig(config: AppConfig): string[] {
  const errors: string[] = [];
  if (!config.base_url) errors.push('base_url 不能为空');
  if (!config.token) errors.push('token 不能为空');
  if (!config.target_type) errors.push('target_type 不能为空');
  if (config.probe_workers < 1) errors.push('probe_workers 必须 >= 1');
  if (config.action_workers < 1) errors.push('action_workers 必须 >= 1');
  if (config.timeout < 1) errors.push('timeout 必须 >= 1');
  if (config.retries < 0) errors.push('retries 不能小于 0');
  if (config.delete_retries < 0) errors.push('delete_retries 不能小于 0');
  if (config.quota_disable_threshold < 0 || config.quota_disable_threshold > 1) {
    errors.push('quota_disable_threshold 必须在 0~1 之间');
  }
  if (!['disable', 'delete'].includes(config.quota_action)) {
    errors.push('quota_action 只能是 disable 或 delete');
  }
  if (!['signal', 'managed'].includes(config.reenable_scope)) {
    errors.push('reenable_scope 只能是 signal 或 managed');
  }
  if (config.upload_workers < 1) errors.push('upload_workers 必须 >= 1');
  if (config.upload_retries < 0) errors.push('upload_retries 不能小于 0');
  return errors;
}

export async function loadCacheMeta(db: D1Database): Promise<CacheMeta> {
  const rows = await db.prepare("SELECT key, value FROM app_config WHERE key IN ('cache_base_url','cache_last_success_at','cache_last_status','cache_last_error')").all<{ key: string; value: string }>();
  const map = new Map<string, string>();
  for (const row of rows.results) {
    map.set(row.key, row.value);
  }
  return {
    cache_base_url: (map.get('cache_base_url') || '').trim(),
    cache_last_success_at: (map.get('cache_last_success_at') || '').trim(),
    cache_last_status: (map.get('cache_last_status') || '').trim(),
    cache_last_error: (map.get('cache_last_error') || '').trim(),
  };
}

export async function saveCacheMeta(db: D1Database, meta: Partial<CacheMeta>): Promise<void> {
  const batch: D1PreparedStatement[] = [];
  for (const [key, value] of Object.entries(meta)) {
    if (value == null) continue;
    batch.push(
      db.prepare("INSERT INTO app_config (key, value, updated_at) VALUES (?, ?, datetime('now')) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at")
        .bind(key, String(value))
    );
  }
  if (batch.length > 0) {
    await db.batch(batch);
  }
}

export async function loadCronMeta(db: D1Database): Promise<CronMeta> {
  const rows = await db.prepare("SELECT key, value FROM app_config WHERE key IN ('cron_expression','cron_last_run_at','cron_last_result','cron_last_error')").all<{ key: string; value: string }>();
  const map = new Map<string, string>();
  for (const row of rows.results) {
    map.set(row.key, row.value);
  }
  return {
    cron_expression: (map.get('cron_expression') || '*/30 * * * *').trim(),
    cron_last_run_at: (map.get('cron_last_run_at') || '').trim(),
    cron_last_result: (map.get('cron_last_result') || '').trim(),
    cron_last_error: (map.get('cron_last_error') || '').trim(),
  };
}

export async function saveCronMeta(db: D1Database, meta: Partial<CronMeta>): Promise<void> {
  const batch: D1PreparedStatement[] = [];
  for (const [key, value] of Object.entries(meta)) {
    if (value == null) continue;
    batch.push(
      db.prepare("INSERT INTO app_config (key, value, updated_at) VALUES (?, ?, datetime('now')) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at")
        .bind(key, String(value))
    );
  }
  if (batch.length > 0) {
    await db.batch(batch);
  }
}
