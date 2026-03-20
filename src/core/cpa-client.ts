/**
 * CPA Management API client — mirrors the Python original's HTTP interactions.
 * All functions use the Workers-native `fetch` API.
 */

import type { AppConfig, AuthAccount, ActionResult, UploadResult } from '../types';

const WHAM_USAGE_URL = 'https://chatgpt.com/backend-api/wham/usage';
const SPARK_METERED_FEATURE = 'codex_bengalfox';

// ── helpers ──────────────────────────────────────────────────────────

function mgmtHeaders(token: string, json = false): Record<string, string> {
  const h: Record<string, string> = {
    Authorization: `Bearer ${token}`,
    Accept: 'application/json, text/plain, */*',
  };
  if (json) h['Content-Type'] = 'application/json';
  return h;
}

function compactText(text: unknown, limit = 240): string | null {
  if (text == null) return null;
  const s = String(text).replace(/\r/g, ' ').replace(/\n/g, ' ').trim();
  if (!s) return null;
  return s.length <= limit ? s : s.slice(0, Math.max(0, limit - 3)) + '...';
}

function safeJson(body: string): Record<string, unknown> {
  try {
    const d = JSON.parse(body);
    return typeof d === 'object' && d !== null && !Array.isArray(d)
      ? (d as Record<string, unknown>)
      : {};
  } catch {
    return {};
  }
}

function maybeJsonLoads(value: unknown): unknown {
  if (typeof value === 'object' && value !== null) return value;
  if (typeof value !== 'string') return null;
  const s = (value as string).trim();
  if (!s) return null;
  try { return JSON.parse(s); } catch { return null; }
}

function normalizeOptionalFlag(value: unknown): number | null {
  if (typeof value === 'boolean') return value ? 1 : 0;
  if (value == null) return null;
  const n = Number(value);
  if (isNaN(n)) return null;
  return n === 0 || n === 1 ? n : null;
}

function normalizeOptionalNumber(value: unknown): number | null {
  if (value == null || typeof value === 'boolean') return null;
  const n = Number(value);
  if (!isFinite(n)) return null;
  return n;
}

function normalizeOptionalRatio(value: unknown): number | null {
  const n = normalizeOptionalNumber(value);
  if (n == null) return null;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

function retryBackoff(attempt: number): number {
  return Math.min(3, 0.5 * 2 ** Math.max(0, attempt));
}

function pickFirstNumber(record: Record<string, unknown>, keys: string[]): number | null {
  for (const k of keys) {
    if (!(k in record)) continue;
    const v = normalizeOptionalNumber(record[k]);
    if (v != null) return v;
  }
  return null;
}

// ── rate limit analysis ──────────────────────────────────────────────

function extractRemainingRatio(rateLimit: Record<string, unknown> | null): number | null {
  if (!rateLimit || typeof rateLimit !== 'object') return null;
  const totalKeys = ['total', 'limit', 'max', 'maximum', 'quota', 'request_limit', 'requests_limit', 'total_requests'];
  const remainingKeys = ['remaining', 'remaining_requests', 'requests_remaining', 'available', 'available_requests', 'left'];
  const usedKeys = ['used', 'consumed', 'used_requests', 'requests_used', 'spent'];

  const windows: Record<string, unknown>[] = [rateLimit];
  for (const wk of ['primary_window', 'window', 'current_window']) {
    const w = rateLimit[wk];
    if (w && typeof w === 'object') windows.push(w as Record<string, unknown>);
  }

  for (const w of windows) {
    const total = pickFirstNumber(w, totalKeys);
    if (total == null || total <= 0) continue;
    const remaining = pickFirstNumber(w, remainingKeys);
    const used = pickFirstNumber(w, usedKeys);
    if (remaining == null && used == null) continue;
    const ratio = remaining != null ? remaining / total : (total - (used ?? 0)) / total;
    return normalizeOptionalRatio(ratio);
  }
  return null;
}

function findSparkRateLimit(body: Record<string, unknown>): Record<string, unknown> | null {
  const additional = body.additional_rate_limits;
  if (!Array.isArray(additional)) return null;

  const candidates: Array<{ item: Record<string, unknown>; rl: Record<string, unknown> }> = [];
  for (const item of additional) {
    if (!item || typeof item !== 'object') continue;
    const rl = (item as Record<string, unknown>).rate_limit;
    if (!rl || typeof rl !== 'object') continue;
    candidates.push({ item: item as Record<string, unknown>, rl: rl as Record<string, unknown> });
  }

  for (const { item, rl } of candidates) {
    const mf = String(item.metered_feature ?? '').trim().toLowerCase();
    if (mf === SPARK_METERED_FEATURE) return rl;
  }
  for (const { item, rl } of candidates) {
    const ln = String(item.limit_name ?? '').trim().toLowerCase();
    if (ln.includes('spark')) return rl;
  }
  return null;
}

// ── quota signal resolution ──────────────────────────────────────────

export function resolveQuotaSignal(record: Record<string, unknown>): {
  limitReached: number | null;
  allowed: number | null;
  source: string;
} {
  const planType = String(record.usage_plan_type ?? record.id_token_plan_type ?? '').trim().toLowerCase();
  const sparkLR = normalizeOptionalFlag(record.usage_spark_limit_reached);
  const sparkAllowed = normalizeOptionalFlag(record.usage_spark_allowed);
  const primaryLR = normalizeOptionalFlag(record.usage_limit_reached);
  const primaryAllowed = normalizeOptionalFlag(record.usage_allowed);

  if (planType === 'pro' && sparkLR != null) {
    return {
      limitReached: sparkLR,
      allowed: sparkAllowed ?? primaryAllowed,
      source: 'spark',
    };
  }
  return { limitReached: primaryLR, allowed: primaryAllowed, source: 'primary' };
}

export function resolveQuotaRemainingRatio(record: Record<string, unknown>): {
  ratio: number | null;
  source: string;
} {
  const { source: sigSource } = resolveQuotaSignal(record);
  const primaryRatio = normalizeOptionalRatio(record.usage_remaining_ratio);
  const sparkRatio = normalizeOptionalRatio(record.usage_spark_remaining_ratio);

  if (sigSource === 'spark') {
    if (sparkRatio != null) return { ratio: sparkRatio, source: 'spark' };
    if (primaryRatio != null) return { ratio: primaryRatio, source: 'primary_fallback' };
    return { ratio: null, source: 'spark' };
  }
  if (primaryRatio != null) return { ratio: primaryRatio, source: 'primary' };
  if (sparkRatio != null) return { ratio: sparkRatio, source: 'spark_fallback' };
  return { ratio: null, source: 'primary' };
}

// ── account classification ───────────────────────────────────────────

export function classifyAccountState(
  record: Record<string, unknown>,
  quotaDisableThreshold: number
): Record<string, unknown> {
  const invalid401 =
    !!record.unavailable || record.api_status_code === 401;

  const { limitReached, allowed, source } = resolveQuotaSignal(record);
  const { ratio: effectiveRatio, source: ratioSource } = resolveQuotaRemainingRatio(record);

  const thresholdTriggered =
    quotaDisableThreshold > 0 &&
    effectiveRatio != null &&
    effectiveRatio <= quotaDisableThreshold;

  const quotaLimited =
    !invalid401 &&
    !record.unavailable &&
    record.api_status_code === 200 &&
    (limitReached === 1 || thresholdTriggered);

  const recovered =
    !invalid401 &&
    !quotaLimited &&
    !!record.disabled &&
    record.api_status_code === 200 &&
    allowed === 1 &&
    limitReached === 0;

  record.quota_signal_source = source;
  record.quota_remaining_ratio = effectiveRatio;
  record.quota_remaining_ratio_source = ratioSource;
  record.quota_threshold_triggered = thresholdTriggered ? 1 : 0;
  record.is_invalid_401 = invalid401 ? 1 : 0;
  record.is_quota_limited = quotaLimited ? 1 : 0;
  record.is_recovered = recovered ? 1 : 0;
  record.updated_at = new Date().toISOString();
  return record;
}

// ── item field extraction ────────────────────────────────────────────

function getItemName(item: Record<string, unknown>): string {
  return String(item.name ?? item.id ?? '').trim();
}
function getItemType(item: Record<string, unknown>): string {
  return String(item.type ?? item.typo ?? '').trim();
}
function getItemAccount(item: Record<string, unknown>): string {
  return String(item.account ?? item.email ?? '').trim();
}
function getIdTokenObject(item: Record<string, unknown>): Record<string, unknown> {
  const parsed = maybeJsonLoads(item.id_token);
  return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
    ? (parsed as Record<string, unknown>)
    : {};
}
function extractChatgptAccountId(item: Record<string, unknown>): string {
  const idToken = getIdTokenObject(item);
  for (const source of [idToken, item]) {
    for (const key of ['chatgpt_account_id', 'chatgptAccountId', 'account_id', 'accountId']) {
      const v = source[key];
      if (typeof v === 'string' && v.trim()) return v.trim();
    }
  }
  return '';
}
function extractIdTokenPlanType(item: Record<string, unknown>): string {
  const idToken = getIdTokenObject(item);
  const v = idToken.plan_type;
  return typeof v === 'string' ? v.trim() : '';
}

// ── build auth record from raw CPA item ──────────────────────────────

export function buildAuthRecord(
  item: Record<string, unknown>,
  existing: Record<string, unknown> | null,
  nowIso: string
): Record<string, unknown> {
  const idTokenObj = getIdTokenObject(item);
  const idTokenJson = Object.keys(idTokenObj).length > 0 ? JSON.stringify(idTokenObj) : null;
  const ex = existing || {};
  return {
    name: getItemName(item),
    disabled: item.disabled ? 1 : 0,
    id_token_json: idTokenJson,
    email: (String(item.email ?? '').trim()) || null,
    provider: (String(item.provider ?? '').trim()) || null,
    source: (String(item.source ?? '').trim()) || null,
    unavailable: item.unavailable ? 1 : 0,
    auth_index: (String(item.auth_index ?? '').trim()) || null,
    account: getItemAccount(item) || null,
    type: getItemType(item) || null,
    runtime_only: item.runtime_only ? 1 : 0,
    status: (String(item.status ?? '').trim()) || null,
    status_message: compactText(item.status_message, 1200),
    chatgpt_account_id: extractChatgptAccountId(item) || null,
    id_token_plan_type: extractIdTokenPlanType(item) || null,
    auth_updated_at: (String(item.updated_at ?? '').trim()) || null,
    auth_modtime: (String(item.modtime ?? '').trim()) || null,
    auth_last_refresh: (String(item.last_refresh ?? '').trim()) || null,
    api_http_status: null,
    api_status_code: null,
    usage_allowed: null,
    usage_limit_reached: null,
    usage_plan_type: null,
    usage_email: null,
    usage_reset_at: null,
    usage_reset_after_seconds: null,
    usage_spark_allowed: null,
    usage_spark_limit_reached: null,
    usage_spark_reset_at: null,
    usage_spark_reset_after_seconds: null,
    quota_signal_source: null,
    is_invalid_401: 0,
    is_quota_limited: 0,
    is_recovered: 0,
    probe_error_kind: null,
    probe_error_text: null,
    managed_reason: ex.managed_reason ?? null,
    last_action: ex.last_action ?? null,
    last_action_status: ex.last_action_status ?? null,
    last_action_error: ex.last_action_error ?? null,
    last_seen_at: nowIso,
    last_probed_at: null,
    updated_at: nowIso,
  };
}

// ── filter ───────────────────────────────────────────────────────────

export function matchesFilters(
  record: Record<string, unknown>,
  targetType: string,
  provider: string
): boolean {
  if (String(record.type ?? '').toLowerCase() !== targetType.toLowerCase()) return false;
  if (provider && String(record.provider ?? '').toLowerCase() !== provider.toLowerCase()) return false;
  return true;
}

// ── CPA management API calls ─────────────────────────────────────────

export async function fetchAuthFiles(
  baseUrl: string,
  token: string,
  timeout: number
): Promise<Record<string, unknown>[]> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout * 1000);
  try {
    const resp = await fetch(`${baseUrl.replace(/\/+$/, '')}/v0/management/auth-files`, {
      headers: mgmtHeaders(token),
      signal: controller.signal,
    });
    if (!resp.ok) throw new Error(`auth-files HTTP ${resp.status}`);
    const data = await resp.json() as Record<string, unknown>;
    const files = data.files;
    return Array.isArray(files) ? files as Record<string, unknown>[] : [];
  } finally {
    clearTimeout(timer);
  }
}

export async function probeWhamUsage(
  baseUrl: string,
  token: string,
  record: Record<string, unknown>,
  timeout: number,
  retries: number,
  userAgent: string,
  quotaDisableThreshold: number
): Promise<Record<string, unknown>> {
  const result = { ...record };
  result.last_probed_at = new Date().toISOString();

  const authIndex = String(result.auth_index ?? '').trim();
  const accountId = String(result.chatgpt_account_id ?? '').trim();

  if (!authIndex) {
    result.probe_error_kind = 'missing_auth_index';
    result.probe_error_text = 'missing auth_index';
    return result;
  }
  if (!accountId) {
    result.probe_error_kind = 'missing_chatgpt_account_id';
    result.probe_error_text = 'missing Chatgpt-Account-Id';
    return result;
  }

  const payload = {
    authIndex,
    method: 'GET',
    url: WHAM_USAGE_URL,
    header: {
      Authorization: 'Bearer $TOKEN$',
      'Content-Type': 'application/json',
      'User-Agent': userAgent,
      'Chatgpt-Account-Id': accountId,
    },
  };

  const url = `${baseUrl.replace(/\/+$/, '')}/v0/management/api-call`;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeout * 1000);
      let resp: Response;
      try {
        resp = await fetch(url, {
          method: 'POST',
          headers: mgmtHeaders(token, true),
          body: JSON.stringify(payload),
          signal: controller.signal,
        });
      } finally {
        clearTimeout(timer);
      }

      result.api_http_status = resp.status;

      if (resp.status === 429) {
        result.probe_error_kind = 'management_api_http_429';
        result.probe_error_text = 'management api-call http 429';
        if (attempt < retries) {
          await sleep(retryBackoff(attempt) * 1000);
          continue;
        }
        return result;
      }
      if (resp.status >= 500) {
        result.probe_error_kind = 'management_api_http_5xx';
        result.probe_error_text = `management api-call http ${resp.status}`;
        if (attempt < retries) {
          await sleep(retryBackoff(attempt) * 1000);
          continue;
        }
        return result;
      }
      if (resp.status >= 400) {
        result.probe_error_kind = 'management_api_http_4xx';
        result.probe_error_text = `management api-call http ${resp.status}`;
        return result;
      }

      const text = await resp.text();
      let outer: Record<string, unknown>;
      try {
        const parsed = JSON.parse(text);
        if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
          result.probe_error_kind = 'api_call_not_object';
          result.probe_error_text = 'api-call response is not JSON object';
          return result;
        }
        outer = parsed as Record<string, unknown>;
      } catch {
        result.probe_error_kind = 'api_call_invalid_json';
        result.probe_error_text = 'api-call response is not valid JSON';
        return result;
      }

      const statusCode = outer.status_code as number | undefined;
      result.api_status_code = statusCode ?? null;
      if (statusCode == null) {
        result.probe_error_kind = 'missing_status_code';
        result.probe_error_text = 'missing status_code in api-call response';
        return result;
      }
      if (statusCode === 401) {
        result.probe_error_kind = null;
        result.probe_error_text = null;
        return classifyAccountState(result, quotaDisableThreshold);
      }

      // Parse body
      let parsedBody: Record<string, unknown> = {};
      const body = outer.body;
      if (typeof body === 'object' && body !== null && !Array.isArray(body)) {
        parsedBody = body as Record<string, unknown>;
      } else if (typeof body === 'string') {
        try {
          const p = JSON.parse(body);
          if (typeof p === 'object' && p !== null && !Array.isArray(p)) parsedBody = p as Record<string, unknown>;
          else {
            result.probe_error_kind = 'body_not_object';
            result.probe_error_text = 'api-call body is not JSON object';
            return result;
          }
        } catch {
          result.probe_error_kind = 'body_invalid_json';
          result.probe_error_text = 'api-call body is not valid JSON';
          return result;
        }
      }

      // Extract usage data
      const rateLimit = parsedBody.rate_limit as Record<string, unknown> | null;
      const primaryWindow = rateLimit && typeof rateLimit === 'object'
        ? rateLimit.primary_window as Record<string, unknown> | null
        : null;

      result.usage_allowed = (rateLimit && typeof rateLimit.allowed === 'boolean')
        ? (rateLimit.allowed ? 1 : 0) : null;
      result.usage_limit_reached = (rateLimit && typeof rateLimit.limit_reached === 'boolean')
        ? (rateLimit.limit_reached ? 1 : 0) : null;
      result.usage_remaining_ratio = extractRemainingRatio(rateLimit as Record<string, unknown> | null);
      result.usage_plan_type = (String(parsedBody.plan_type ?? '').trim()) || null;
      result.usage_email = (String(parsedBody.email ?? '').trim()) || null;
      result.usage_reset_at = (primaryWindow && primaryWindow.reset_at != null)
        ? Number(primaryWindow.reset_at) : null;
      result.usage_reset_after_seconds = (primaryWindow && primaryWindow.reset_after_seconds != null)
        ? Number(primaryWindow.reset_after_seconds) : null;

      // Spark
      const sparkRL = findSparkRateLimit(parsedBody);
      const sparkPW = sparkRL && typeof sparkRL === 'object'
        ? sparkRL.primary_window as Record<string, unknown> | null : null;
      result.usage_spark_allowed = (sparkRL && typeof sparkRL.allowed === 'boolean')
        ? (sparkRL.allowed ? 1 : 0) : null;
      result.usage_spark_limit_reached = (sparkRL && typeof sparkRL.limit_reached === 'boolean')
        ? (sparkRL.limit_reached ? 1 : 0) : null;
      result.usage_spark_remaining_ratio = extractRemainingRatio(sparkRL);
      result.usage_spark_reset_at = (sparkPW && sparkPW.reset_at != null)
        ? Number(sparkPW.reset_at) : null;
      result.usage_spark_reset_after_seconds = (sparkPW && sparkPW.reset_after_seconds != null)
        ? Number(sparkPW.reset_after_seconds) : null;

      if (statusCode === 200) {
        result.probe_error_kind = null;
        result.probe_error_text = null;
        return classifyAccountState(result, quotaDisableThreshold);
      }

      result.probe_error_kind = 'other';
      result.probe_error_text = `unexpected upstream status_code=${statusCode}`;
      return result;
    } catch (e) {
      if (e instanceof DOMException && e.name === 'AbortError') {
        result.probe_error_kind = 'timeout';
        result.probe_error_text = 'timeout';
      } else {
        result.probe_error_kind = 'other';
        result.probe_error_text = String(e);
      }
      if (attempt >= retries) return result;
      await sleep(retryBackoff(attempt) * 1000);
    }
  }
  return result;
}

// ── account actions ──────────────────────────────────────────────────

export async function deleteAccount(
  baseUrl: string,
  token: string,
  name: string,
  timeout: number,
  deleteRetries: number
): Promise<ActionResult> {
  const encoded = encodeURIComponent(name);
  const url = `${baseUrl.replace(/\/+$/, '')}/v0/management/auth-files?name=${encoded}`;
  const maxAttempts = Math.max(1, deleteRetries + 1);

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeout * 1000);
      let resp: Response;
      try {
        resp = await fetch(url, { method: 'DELETE', headers: mgmtHeaders(token), signal: controller.signal });
      } finally {
        clearTimeout(timer);
      }
      const text = await resp.text();
      const data = safeJson(text);
      const ok = resp.status === 200 && data.status === 'ok';
      if (ok) return { name, ok: true, status_code: resp.status, error: null, attempts: attempt + 1 };

      const shouldRetry = [408, 425, 429].includes(resp.status) || resp.status >= 500;
      if (shouldRetry && attempt < maxAttempts - 1) {
        await sleep(Math.min(5000, 500 * 2 ** attempt));
        continue;
      }
      return { name, ok: false, status_code: resp.status, error: compactText(text, 200), attempts: attempt + 1 };
    } catch (e) {
      if (attempt < maxAttempts - 1) {
        await sleep(Math.min(5000, 500 * 2 ** attempt));
        continue;
      }
      return { name, ok: false, status_code: null, error: String(e), attempts: attempt + 1 };
    }
  }
  return { name, ok: false, status_code: null, error: 'unreachable', attempts: 0 };
}

export async function setAccountDisabled(
  baseUrl: string,
  token: string,
  name: string,
  disabled: boolean,
  timeout: number
): Promise<ActionResult> {
  const url = `${baseUrl.replace(/\/+$/, '')}/v0/management/auth-files/status`;
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout * 1000);
    let resp: Response;
    try {
      resp = await fetch(url, {
        method: 'PATCH',
        headers: mgmtHeaders(token, true),
        body: JSON.stringify({ name, disabled }),
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }
    const text = await resp.text();
    const data = safeJson(text);
    const ok = resp.status === 200 && data.status === 'ok';
    return { name, ok, status_code: resp.status, error: ok ? null : compactText(text, 200), disabled };
  } catch (e) {
    return { name, ok: false, status_code: null, error: String(e), disabled };
  }
}

export async function uploadAuthFile(
  baseUrl: string,
  token: string,
  fileName: string,
  content: string,
  method: 'json' | 'multipart',
  timeout: number,
  retries: number
): Promise<UploadResult> {
  const encoded = encodeURIComponent(fileName);

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeout * 1000);
      let resp: Response;
      try {
        if (method === 'multipart') {
          const form = new FormData();
          form.append('file', new Blob([content], { type: 'application/json' }), fileName);
          resp = await fetch(`${baseUrl.replace(/\/+$/, '')}/v0/management/auth-files`, {
            method: 'POST',
            headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
            body: form,
            signal: controller.signal,
          });
        } else {
          resp = await fetch(`${baseUrl.replace(/\/+$/, '')}/v0/management/auth-files?name=${encoded}`, {
            method: 'POST',
            headers: mgmtHeaders(token, true),
            body: content,
            signal: controller.signal,
          });
        }
      } finally {
        clearTimeout(timer);
      }
      const text = await resp.text();
      const data = safeJson(text);
      const ok = resp.status === 200 && data.status === 'ok';
      if (ok) {
        return { file_name: fileName, status_code: resp.status, ok: true, outcome: 'uploaded_success', error: null, error_kind: null };
      }

      const shouldRetry = resp.status === 429 || resp.status >= 500;
      if (shouldRetry && attempt < retries) {
        await sleep(500 * 2 ** attempt);
        continue;
      }
      return {
        file_name: fileName, status_code: resp.status, ok: false,
        outcome: 'upload_failed', error: compactText(text, 240) || `http ${resp.status}`, error_kind: null,
      };
    } catch (e) {
      if (attempt < retries) {
        await sleep(500 * 2 ** attempt);
        continue;
      }
      const isTimeout = e instanceof DOMException && e.name === 'AbortError';
      return {
        file_name: fileName, status_code: null, ok: false,
        outcome: 'upload_failed', error: isTimeout ? 'timeout' : String(e), error_kind: isTimeout ? 'timeout' : 'other',
      };
    }
  }
  return { file_name: fileName, status_code: null, ok: false, outcome: 'upload_failed', error: 'retries exhausted', error_kind: null };
}

// ── concurrency helpers ──────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** Run tasks with concurrency limit */
export async function runWithConcurrency<T>(
  tasks: (() => Promise<T>)[],
  concurrency: number
): Promise<T[]> {
  const results: T[] = [];
  let idx = 0;
  const execute = async (): Promise<void> => {
    while (idx < tasks.length) {
      const i = idx++;
      results[i] = await tasks[i]();
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, tasks.length) }, () => execute()));
  return results;
}

/** Count valid accounts (same logic as Python) */
export function countValidAccounts(records: Record<string, unknown>[]): number {
  let valid = 0;
  for (const row of records) {
    if (row.disabled) continue;
    if (Number(row.is_invalid_401 ?? 0) === 1) continue;
    if (Number(row.is_quota_limited ?? 0) === 1) continue;
    if (row.probe_error_kind) continue;
    valid++;
  }
  return valid;
}
