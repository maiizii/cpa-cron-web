/**
 * Async engine: scan / maintain / upload / refill.
 * Designed for CF Workers — all heavy work runs in batches,
 * each batch writes to D1 + updates task progress so the
 * frontend can poll in real-time.
 *
 * Batch size & concurrency are kept low to stay within
 * CF Workers CPU-time limits even for 500+ accounts.
 */

import type { AppConfig } from '../types';
import {
  fetchAuthFiles,
  probeWhamUsage,
  buildAuthRecord,
  matchesFilters,
  deleteAccount,
  setAccountDisabled,
  uploadAuthFile,
  runWithConcurrency,
  countValidAccounts,
} from './cpa-client';
import {
  upsertAuthAccounts,
  loadExistingState,
  startScanRun,
  finishScanRun,
  logActivity,
  updateTask,
  deleteAccountsFromDB,
  deleteAccountsNotInSet,
  getAccountsByNames,
  getTaskById,
  isTaskStopRequested,
} from './db';
import { saveCacheMeta } from './config';

// ── constants ────────────────────────────────────────────────────────

const PROBE_BATCH_SIZE = 15;   // accounts per batch
const DEFAULT_PROBE_CONCURRENCY = 5;   // parallel fetches inside a batch
const ACTION_BATCH_SIZE = 20;
const DEFAULT_ACTION_CONCURRENCY = 5;
const UPLOAD_BATCH_SIZE = 10;
const DEFAULT_UPLOAD_CONCURRENCY = 5;
const MAX_PROBE_CONCURRENCY = 12;
const MAX_ACTION_CONCURRENCY = 10;
const MAX_UPLOAD_CONCURRENCY = 8;
const SCAN_STEP_SIZE = 16;
const MAINTAIN_ACTION_STEP_SIZE = 6;

// ── helpers ──────────────────────────────────────────────────────────

function chunks<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function upsertAuthAccountsInChunks(
  db: D1Database,
  rows: Record<string, unknown>[],
  chunkSize = 5
): Promise<void> {
  for (const chunk of chunks(rows, Math.max(1, chunkSize))) {
    await upsertAuthAccounts(db, chunk);
  }
}

async function deleteAccountsFromDBInChunks(
  db: D1Database,
  names: string[],
  chunkSize = 20
): Promise<number> {
  let deleted = 0;
  for (const chunk of chunks(names, Math.max(1, chunkSize))) {
    deleted += await deleteAccountsFromDB(db, chunk);
  }
  return deleted;
}

function boundedConcurrency(value: number, fallback: number, max: number): number {
  if (!Number.isFinite(value) || value < 1) return fallback;
  return Math.min(Math.max(1, Math.floor(value)), max);
}

function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timeout after ${ms}ms`)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      }
    );
  });
}

function summarizeActionResults(results: Array<{ name: string; ok: boolean; error: string | null }>): {
  total: number;
  success: number;
  failed: number;
  successNames: string[];
  failedItems: Array<{ name: string; error: string | null }>;
} {
  const successItems = results.filter((result) => result.ok);
  const failedItems = results
    .filter((result) => !result.ok)
    .map((result) => ({ name: result.name, error: result.error }));
  return {
    total: results.length,
    success: successItems.length,
    failed: failedItems.length,
    successNames: successItems.map((result) => result.name),
    failedItems,
  };
}

function formatActionSummaryDetail(
  label: string,
  summary: ReturnType<typeof summarizeActionResults>,
  extraParts: string[] = []
): string {
  const parts = [
    `${label}: 总计=${summary.total}`,
    `成功=${summary.success}`,
    `失败=${summary.failed}`,
    ...extraParts.filter(Boolean),
  ];

  if (summary.successNames.length > 0) {
    parts.push(`成功账号=${summary.successNames.slice(0, 20).join(', ')}`);
  }
  if (summary.failedItems.length > 0) {
    parts.push(
      `失败账号=${summary.failedItems
        .slice(0, 20)
        .map((item) => `${item.name}${item.error ? `(${item.error})` : ''}`)
        .join(', ')}`
    );
  }

  return parts.join(' | ');
}

async function logActionResults(
  db: D1Database,
  action: string,
  results: Array<{ name: string; ok: boolean; status_code?: number | null; error: string | null; attempts?: number; disabled?: boolean }>,
  username?: string,
  options?: { logSuccesses?: boolean }
): Promise<void> {
  const logSuccesses = options?.logSuccesses !== false;
  for (const result of results) {
    if (result.ok && !logSuccesses) continue;
    const detail = [
      result.ok ? '成功' : '失败',
      `账号=${result.name}`,
      result.status_code != null ? `HTTP=${result.status_code}` : '',
      result.attempts != null ? `尝试=${result.attempts}` : '',
      typeof result.disabled === 'boolean' ? `disabled=${result.disabled ? 1 : 0}` : '',
      result.error ? `错误=${result.error}` : '',
    ].filter(Boolean).join(' | ');
    await logActivity(db, action, detail, username);
  }
}

async function runTasksWithProgress<T>(
  tasks: Array<() => Promise<T>>,
  concurrency: number,
  onStart?: (index: number) => Promise<void> | void,
  onFinish?: (index: number, result: T) => Promise<void> | void,
  onError?: (index: number, error: unknown) => Promise<T> | T
): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let idx = 0;
  const execute = async (): Promise<void> => {
    while (true) {
      const i = idx++;
      if (i >= tasks.length) return;
      if (onStart) await onStart(i);
      try {
        const result = await tasks[i]();
        results[i] = result;
        if (onFinish) await onFinish(i, result);
      } catch (error) {
        if (!onError) throw error;
        const recovered = await onError(i, error);
        results[i] = recovered;
        if (onFinish) await onFinish(i, recovered);
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, tasks.length) }, () => execute()));
  return results;
}

async function safeTaskUpdate(
  db: D1Database,
  taskId: number,
  patch: Record<string, unknown>,
  timeoutMs = 8000
): Promise<void> {
  try {
    await withTimeout(updateTask(db, taskId, patch), timeoutMs, `updateTask ${taskId}`);
  } catch {
    // ignore task progress update failures; never block the scan loop
  }
}

async function safeAccountUpsert(
  db: D1Database,
  row: Record<string, unknown>,
  timeoutMs = 8000
): Promise<{ ok: boolean; error?: string }> {
  try {
    await withTimeout(upsertAuthAccounts(db, [row]), timeoutMs, `upsertAuthAccounts ${String(row.name ?? '')}`);
    return { ok: true };
  } catch (error) {
    return { ok: false, error: String(error) };
  }
}

async function safeBatchAccountUpsert(
  db: D1Database,
  rows: Record<string, unknown>[],
  timeoutMs = 12000
): Promise<{ ok: boolean; error?: string }> {
  for (const row of rows) {
    const result = await safeAccountUpsert(db, row, timeoutMs);
    if (!result.ok) {
      return { ok: false, error: result.error };
    }
  }
  return { ok: true };
}

async function shouldStopTask(db: D1Database, taskId: number): Promise<boolean> {
  try {
    return await isTaskStopRequested(db, taskId);
  } catch {
    return false;
  }
}

async function markTaskStopped(
  db: D1Database,
  taskId: number,
  partial: Record<string, unknown>
): Promise<void> {
  await safeTaskUpdate(db, taskId, {
    status: 'stopped',
    finished_at: new Date().toISOString(),
    result: JSON.stringify({ stopped: true, ...partial }),
  });
}

function parseTaskJson(value: unknown): Record<string, unknown> {
  if (!value) return {};
  if (typeof value === 'object' && value !== null) return value as Record<string, unknown>;
  if (typeof value !== 'string') return {};
  try {
    const parsed = JSON.parse(value);
    return typeof parsed === 'object' && parsed !== null ? parsed as Record<string, unknown> : {};
  } catch {
    return {};
  }
}

function toFiniteNumber(value: unknown, fallback: number): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((item) => String(item)).filter(Boolean);
}

async function runDeleteBatch(
  config: AppConfig,
  names: string[],
  concurrency: number
): Promise<Array<{ name: string; ok: boolean; status_code?: number | null; error: string | null; attempts?: number }>> {
  const tasks = names.map((name) => () =>
    deleteAccount(config.base_url, config.token, name, config.timeout, config.delete_retries)
  );
  return runWithConcurrency(tasks, concurrency);
}

async function runDisableBatch(
  config: AppConfig,
  names: string[],
  concurrency: number
): Promise<Array<{ name: string; ok: boolean; status_code?: number | null; error: string | null; attempts?: number; disabled?: boolean }>> {
  const tasks = names.map((name) => () =>
    setAccountDisabled(config.base_url, config.token, name, true, config.timeout)
  );
  return runWithConcurrency(tasks, concurrency);
}

async function runReenableBatch(
  config: AppConfig,
  names: string[],
  concurrency: number
): Promise<Array<{ name: string; ok: boolean; status_code?: number | null; error: string | null; attempts?: number; disabled?: boolean }>> {
  const tasks = names.map((name) => () =>
    setAccountDisabled(config.base_url, config.token, name, false, config.timeout)
  );
  return runWithConcurrency(tasks, concurrency);
}

async function finalizeScanSnapshot(
  db: D1Database,
  inventoryRecords: Record<string, unknown>[]
): Promise<{ deletedStaleCount: number; snapshotSyncError: string | null }> {
  let snapshotSyncError: string | null = null;
  let deletedStaleCount = 0;
  try {
    await upsertAuthAccounts(db, inventoryRecords);
    const remoteNames = inventoryRecords.map((r) => String(r.name)).filter(Boolean);
    deletedStaleCount = await deleteAccountsNotInSet(db, remoteNames);
  } catch (error) {
    snapshotSyncError = String(error);
  }
  return { deletedStaleCount, snapshotSyncError };
}

export interface EngineResult {
  success: boolean;
  total_files: number;
  filtered_count: number;
  probed_count: number;
  invalid_401_count: number;
  quota_limited_count: number;
  recovered_count: number;
  failure_count: number;
  actions?: {
    deleted_401: number;
    disabled_quota: number;
    deleted_quota: number;
    reenabled: number;
  };
  upload?: {
    uploaded: number;
    skipped: number;
    failed: number;
  };
  error?: string;
}

export async function advanceScanTask(
  db: D1Database,
  config: AppConfig,
  taskId: number,
  username?: string,
  stepSize = SCAN_STEP_SIZE
): Promise<EngineResult> {
  const task = await getTaskById(db, taskId);
  if (!task) {
    return {
      success: false,
      total_files: 0,
      filtered_count: 0,
      probed_count: 0,
      invalid_401_count: 0,
      quota_limited_count: 0,
      recovered_count: 0,
      failure_count: 0,
      error: 'task_not_found',
    };
  }

  const params = parseTaskJson(task.params);
  const resultState = parseTaskJson(task.result);
  const nowIso = new Date().toISOString();

  if (String(task.status || '') === 'stopping') {
    await markTaskStopped(db, taskId, {
      phase: String(resultState.phase || 'probing'),
      probed: toFiniteNumber(task.progress, 0),
      total_files: toFiniteNumber(resultState.total_files, 0),
      filtered: toFiniteNumber(task.total, 0),
    });
    return {
      success: false,
      total_files: toFiniteNumber(resultState.total_files, 0),
      filtered_count: toFiniteNumber(task.total, 0),
      probed_count: toFiniteNumber(task.progress, 0),
      invalid_401_count: 0,
      quota_limited_count: 0,
      recovered_count: 0,
      failure_count: 0,
      error: 'stopped',
    };
  }

  let inventoryRecords = Array.isArray(resultState.inventory_records)
    ? resultState.inventory_records as Record<string, unknown>[]
    : [];
  let currentCandidates = Array.isArray(resultState.candidate_records)
    ? resultState.candidate_records as Record<string, unknown>[]
    : [];
  let runId = toFiniteNumber(params.scan_run_id, 0);
  let initialized = Number(params.scan_initialized || 0) === 1;
  let probeIndex = toFiniteNumber(resultState.probe_index, toFiniteNumber(task.progress, 0));
  let totalFiles = toFiniteNumber(resultState.total_files, 0);
  let filteredTotal = toFiniteNumber(resultState.filtered, toFiniteNumber(task.total, 0));

  if (initialized && (inventoryRecords.length === 0 || currentCandidates.length === 0) && filteredTotal >= 0) {
    const files = await fetchAuthFiles(config.base_url, config.token, config.timeout);
    const existingState = await loadExistingState(db);
    inventoryRecords = [];
    for (const item of files) {
      const r = item as Record<string, unknown>;
      const name = String(r.name ?? r.id ?? '').trim();
      if (!name) continue;
      inventoryRecords.push(buildAuthRecord(r, existingState.get(name) ?? null, nowIso));
    }
    currentCandidates = inventoryRecords.filter((r) =>
      matchesFilters(r, config.target_type, config.provider)
    );
    totalFiles = files.length;
    filteredTotal = currentCandidates.length;
  }

  if (!initialized) {
    runId = await startScanRun(db, 'scan', config as unknown as Record<string, unknown>);
    await updateTask(db, taskId, {
      status: 'running',
      started_at: String(task.started_at || nowIso),
      result: JSON.stringify({ phase: 'fetching_files' }),
      params: JSON.stringify({ ...params, scan_initialized: 1, scan_run_id: runId }),
    });

    const files = await fetchAuthFiles(config.base_url, config.token, config.timeout);
    await safeTaskUpdate(db, taskId, {
      result: JSON.stringify({ phase: 'loading_local_state', total_files: files.length }),
    });

    const existingState = await loadExistingState(db);
    inventoryRecords = [];
    for (const item of files) {
      const r = item as Record<string, unknown>;
      const name = String(r.name ?? r.id ?? '').trim();
      if (!name) continue;
      inventoryRecords.push(buildAuthRecord(r, existingState.get(name) ?? null, nowIso));
    }
    currentCandidates = inventoryRecords.filter((r) =>
      matchesFilters(r, config.target_type, config.provider)
    );
    totalFiles = files.length;
    filteredTotal = currentCandidates.length;
    probeIndex = 0;

    await updateTask(db, taskId, {
      total: filteredTotal,
      progress: 0,
      result: JSON.stringify({
        phase: 'probing',
        total_files: totalFiles,
        filtered: filteredTotal,
        probe_index: 0,
      }),
    });
  }

  if (probeIndex >= currentCandidates.length) {
    await safeTaskUpdate(db, taskId, {
      result: JSON.stringify({
        phase: 'finalizing_snapshot',
        total_files: totalFiles,
        filtered: filteredTotal,
        probed: probeIndex,
        probe_index: probeIndex,
      }),
    });

    const invalidRecords = currentCandidates.filter((r) => r.is_invalid_401 === 1);
    const quotaRecords = currentCandidates.filter((r) => r.is_quota_limited === 1);
    const recoveredRecords = currentCandidates.filter((r) => r.is_recovered === 1);
    const failureRecords = currentCandidates.filter((r) => !!r.probe_error_kind);
    const { deletedStaleCount, snapshotSyncError } = await finalizeScanSnapshot(db, inventoryRecords);

    const engineResult: EngineResult = {
      success: true,
      total_files: totalFiles,
      filtered_count: filteredTotal,
      probed_count: probeIndex,
      invalid_401_count: invalidRecords.length,
      quota_limited_count: quotaRecords.length,
      recovered_count: recoveredRecords.length,
      failure_count: failureRecords.length,
    };
    if (snapshotSyncError) engineResult.error = snapshotSyncError;

    await finishScanRun(db, runId, {
      status: 'success',
      total_files: totalFiles,
      filtered_files: filteredTotal,
      probed_files: probeIndex,
      invalid_401_count: invalidRecords.length,
      quota_limited_count: quotaRecords.length,
      recovered_count: recoveredRecords.length,
    });
    await saveCacheMeta(db, {
      cache_base_url: config.base_url,
      cache_last_success_at: new Date().toISOString(),
      cache_last_status: snapshotSyncError ? 'partial' : 'success',
      cache_last_error: snapshotSyncError ?? '',
    });
    await updateTask(db, taskId, {
      status: 'completed',
      progress: filteredTotal,
      finished_at: new Date().toISOString(),
      result: JSON.stringify({
        ...engineResult,
        snapshot_sync_error: snapshotSyncError,
        deleted_stale_count: deletedStaleCount,
      }),
      params: JSON.stringify({ ...params, scan_initialized: 1, scan_run_id: runId, scan_finished: 1 }),
    });
    await logActivity(
      db,
      'scan',
      `扫描完成: 总计=${totalFiles} 过滤=${filteredTotal} 401=${invalidRecords.length} 限额=${quotaRecords.length} 恢复=${recoveredRecords.length} 清理旧缓存=${deletedStaleCount}${snapshotSyncError ? ` 快照同步异常=${snapshotSyncError}` : ''}`,
      username
    );
    return engineResult;
  }

  const endIndex = Math.min(probeIndex + Math.max(1, stepSize), currentCandidates.length);
  const batch = currentCandidates.slice(probeIndex, endIndex);
  const itemTimeoutMs = 12000;
  let batchLastError: string | null = null;
  const inventoryByName = new Map<string, Record<string, unknown>>();
  for (const row of inventoryRecords) inventoryByName.set(String(row.name), row);

  for (let index = 0; index < batch.length; index++) {
    if (await shouldStopTask(db, taskId)) {
      await finishScanRun(db, runId, {
        status: 'stopped',
        total_files: totalFiles,
        filtered_files: filteredTotal,
        probed_files: probeIndex + index,
        invalid_401_count: 0,
        quota_limited_count: 0,
        recovered_count: 0,
      });
      await markTaskStopped(db, taskId, {
        phase: 'probing',
        probed: probeIndex + index,
        total_files: totalFiles,
        filtered: filteredTotal,
      });
      return {
        success: false,
        total_files: totalFiles,
        filtered_count: filteredTotal,
        probed_count: probeIndex + index,
        invalid_401_count: 0,
        quota_limited_count: 0,
        recovered_count: 0,
        failure_count: 0,
        error: 'stopped',
      };
    }

    const absoluteIndex = probeIndex + index;
    const record = batch[index];
    const fallbackName = String(record?.name ?? '');

    await safeTaskUpdate(db, taskId, {
      progress: absoluteIndex,
      result: JSON.stringify({
        phase: 'probing',
        probed: absoluteIndex,
        total_files: totalFiles,
        filtered: filteredTotal,
        probe_index: absoluteIndex,
        current_batch: `${probeIndex + 1}-${endIndex}`,
        current_index: absoluteIndex + 1,
        current_item: fallbackName,
        current_step: 'probe_item_start',
        last_error: batchLastError,
      }),
    });

    let merged: Record<string, unknown>;
    try {
      const result = await withTimeout(
        probeWhamUsage(
          config.base_url,
          config.token,
          record,
          config.timeout,
          config.retries,
          config.user_agent,
          config.quota_disable_threshold,
          itemTimeoutMs
        ),
        itemTimeoutMs + 2000,
        `probeWhamUsage ${fallbackName}`
      );
      merged = { ...result, updated_at: new Date().toISOString() } as Record<string, unknown>;
    } catch (error) {
      const errText = String(error);
      merged = {
        ...record,
        last_probed_at: new Date().toISOString(),
        probe_error_kind: errText.includes('timeout after') ? 'probe_outer_timeout' : 'probe_wrapper_error',
        probe_error_text: errText,
        updated_at: new Date().toISOString(),
      } as Record<string, unknown>;
    }

    const upsertSingle = await safeAccountUpsert(db, merged, itemTimeoutMs);
    if (!upsertSingle.ok) {
      merged.probe_error_kind = 'db_write_timeout';
      merged.probe_error_text = upsertSingle.error ?? 'auth_accounts single write failed';
      batchLastError = upsertSingle.error ?? 'auth_accounts single write failed';
    }

    const name = String(merged.name ?? fallbackName);
    const inventoryRow = inventoryByName.get(name);
    if (inventoryRow) Object.assign(inventoryRow, merged);
    currentCandidates[absoluteIndex] = merged;
    batchLastError = (merged.probe_error_text as string | null | undefined) ?? batchLastError;

    await safeTaskUpdate(db, taskId, {
      progress: absoluteIndex + 1,
      result: JSON.stringify({
        phase: 'probing',
        probed: absoluteIndex + 1,
        total_files: totalFiles,
        filtered: filteredTotal,
        probe_index: absoluteIndex + 1,
        current_batch: `${probeIndex + 1}-${endIndex}`,
        current_index: absoluteIndex + 1,
        current_item: name,
        current_step: 'probe_item_done',
        last_error: batchLastError,
      }),
    });
  }

  return {
    success: true,
    total_files: totalFiles,
    filtered_count: filteredTotal,
    probed_count: endIndex,
    invalid_401_count: 0,
    quota_limited_count: 0,
    recovered_count: 0,
    failure_count: 0,
  };
}

export async function advanceMaintainTask(
  db: D1Database,
  config: AppConfig,
  taskId: number,
  username?: string,
  actionStepSize = MAINTAIN_ACTION_STEP_SIZE
): Promise<EngineResult> {
  const task = await getTaskById(db, taskId);
  if (!task) {
    return {
      success: false,
      total_files: 0,
      filtered_count: 0,
      probed_count: 0,
      invalid_401_count: 0,
      quota_limited_count: 0,
      recovered_count: 0,
      failure_count: 0,
      error: 'task_not_found',
    };
  }

  const params = parseTaskJson(task.params);
  const state = parseTaskJson(task.result);

  if (String(task.status || '') === 'stopping') {
    await markTaskStopped(db, taskId, {
      phase: String(state.phase || 'maintaining'),
      progress: toFiniteNumber(task.progress, 0),
      total: toFiniteNumber(task.total, 0),
    });
    return {
      success: false,
      total_files: toFiniteNumber(state.total_files, 0),
      filtered_count: toFiniteNumber(state.filtered_count, 0),
      probed_count: toFiniteNumber(state.probed_count, 0),
      invalid_401_count: toFiniteNumber(state.invalid_401_count, 0),
      quota_limited_count: toFiniteNumber(state.quota_limited_count, 0),
      recovered_count: toFiniteNumber(state.recovered_count, 0),
      failure_count: toFiniteNumber(state.failure_count, 0),
      error: 'stopped',
    };
  }

  const maintainInitialized = Number(params.maintain_initialized || 0) === 1;
  if (!maintainInitialized) {
    const existingState = await loadExistingState(db);
    const candidateRecords = Array.from(existingState.values()).filter((r) =>
      matchesFilters(r, config.target_type, config.provider)
    );
    const invalidRecords = candidateRecords.filter((r) => Number(r.is_invalid_401) === 1);
    const quotaRecords = candidateRecords.filter(
      (r) => Number(r.is_quota_limited) === 1 && Number(r.is_invalid_401) !== 1
    );
    const recoveredRecords = candidateRecords.filter((r) => Number(r.is_recovered) === 1);
    const isCronRun = username === 'system';

    await logActivity(
      db,
      'maintain_started',
      `维护开始: 候选=${candidateRecords.length} 401=${invalidRecords.length} 限额=${quotaRecords.length} 恢复候选=${recoveredRecords.length} quota_action=${config.quota_action} delete_401=${config.delete_401 ? 1 : 0} auto_reenable=${config.auto_reenable ? 1 : 0}`,
      username
    );

    const steps: Array<{ kind: 'delete401' | 'disableQuota' | 'deleteQuota' | 'reenable'; names: string[] }> = [];
    if (config.delete_401 && invalidRecords.length > 0) {
      steps.push({ kind: 'delete401', names: invalidRecords.map((r) => String(r.name)).filter(Boolean) });
    }
    if (config.quota_action === 'disable') {
      const toDisable = quotaRecords.filter((r) => Number(r.disabled) !== 1).map((r) => String(r.name)).filter(Boolean);
      if (toDisable.length > 0) steps.push({ kind: 'disableQuota', names: toDisable });
    } else {
      const toDelete = quotaRecords.map((r) => String(r.name)).filter(Boolean);
      if (toDelete.length > 0) steps.push({ kind: 'deleteQuota', names: toDelete });
    }
    if (config.auto_reenable) {
      const recoverable = config.reenable_scope === 'signal'
        ? recoveredRecords
        : recoveredRecords.filter((r) => String(r.managed_reason ?? '') === 'quota_disabled');
      const toReenable = recoverable.map((r) => String(r.name)).filter(Boolean);
      if (toReenable.length > 0) steps.push({ kind: 'reenable', names: toReenable });
    }

    const flatOps = steps.flatMap((step) => step.names.map((name) => ({ kind: step.kind, name })));
    await updateTask(db, taskId, {
      status: 'running',
      started_at: String(task.started_at || new Date().toISOString()),
      total: flatOps.length,
      progress: 0,
      result: JSON.stringify({
        phase: 'maintaining',
        scan_done: true,
        total_files: candidateRecords.length,
        filtered_count: candidateRecords.length,
        probed_count: candidateRecords.filter((r) => r.last_probed_at).length,
        action_index: 0,
        action_total: flatOps.length,
        pending_operations: flatOps,
        deleted_names: [],
        stats: { deleted_401: 0, disabled_quota: 0, deleted_quota: 0, reenabled: 0, deleted_local: 0 },
        is_cron_run: isCronRun ? 1 : 0,
      }),
      params: JSON.stringify({ ...params, maintain_initialized: 1 }),
    });
  }

  const latestTask = maintainInitialized ? task : await getTaskById(db, taskId);
  const latestState = parseTaskJson(latestTask?.result);

  const pendingOperations = Array.isArray(latestState.pending_operations)
    ? latestState.pending_operations as Array<{ kind: 'delete401' | 'disableQuota' | 'deleteQuota' | 'reenable'; name: string }>
    : [];
  const actionIndex = toFiniteNumber(latestState.action_index, toFiniteNumber(latestTask?.progress, 0));
  const endIndex = Math.min(actionIndex + Math.max(1, actionStepSize), pendingOperations.length);
  const slice = pendingOperations.slice(actionIndex, endIndex);
  const actionConcurrency = boundedConcurrency(config.action_workers, DEFAULT_ACTION_CONCURRENCY, MAX_ACTION_CONCURRENCY);
  const batchNames = Array.from(new Set(slice.map((op) => op.name).filter(Boolean)));
  const existingState = await getAccountsByNames(db, batchNames);
  const deletedNames = new Set<string>(toStringArray(state.deleted_names));
  const stats = {
    deleted_401: toFiniteNumber(state.stats && (state.stats as Record<string, unknown>).deleted_401, 0),
    disabled_quota: toFiniteNumber(state.stats && (state.stats as Record<string, unknown>).disabled_quota, 0),
    deleted_quota: toFiniteNumber(state.stats && (state.stats as Record<string, unknown>).deleted_quota, 0),
    reenabled: toFiniteNumber(state.stats && (state.stats as Record<string, unknown>).reenabled, 0),
    deleted_local: toFiniteNumber(state.stats && (state.stats as Record<string, unknown>).deleted_local, 0),
  };
  const nowIso = new Date().toISOString();
  const isCronRun = Number(state.is_cron_run || 0) === 1;

  const grouped = new Map<string, string[]>();
  for (const op of slice) {
    const arr = grouped.get(op.kind) || [];
    arr.push(op.name);
    grouped.set(op.kind, arr);
  }

  for (const [kind, names] of grouped.entries()) {
    if (await shouldStopTask(db, taskId)) {
      await markTaskStopped(db, taskId, {
        phase: 'maintaining',
        progress: actionIndex,
        total: pendingOperations.length,
      });
      return {
        success: false,
        total_files: toFiniteNumber(state.total_files, 0),
        filtered_count: toFiniteNumber(state.filtered_count, 0),
        probed_count: toFiniteNumber(state.probed_count, 0),
        invalid_401_count: 0,
        quota_limited_count: 0,
        recovered_count: 0,
        failure_count: 0,
        error: 'stopped',
      };
    }

    if (kind === 'delete401' || kind === 'deleteQuota') {
      const results = await runDeleteBatch(config, names, actionConcurrency);
      await logActionResults(
        db,
        kind === 'delete401' ? 'maintain_delete_401_account' : 'maintain_delete_quota_account',
        results,
        username,
        { logSuccesses: !isCronRun }
      );
      const updates: Record<string, unknown>[] = [];
      for (const result of results) {
        if (result.ok) {
          deletedNames.add(result.name);
          if (kind === 'delete401') stats.deleted_401++; else stats.deleted_quota++;
        }
        const record = existingState.get(result.name);
        if (record) {
          record.last_action = kind === 'delete401' ? 'delete_401' : 'delete_quota';
          record.last_action_status = result.ok ? 'success' : 'failed';
          record.last_action_error = result.error;
          if (result.ok) record.managed_reason = kind === 'delete401' ? 'deleted_401' : 'quota_deleted';
          record.updated_at = nowIso;
          updates.push(record);
        }
      }
      if (updates.length > 0) await upsertAuthAccountsInChunks(db, updates, 5);

      const deletedBatchNames = results.filter((result) => result.ok).map((result) => result.name);
      if (deletedBatchNames.length > 0) {
        stats.deleted_local += await deleteAccountsFromDBInChunks(db, deletedBatchNames, 10);
        for (const name of deletedBatchNames) existingState.delete(name);
      }

      const summary = summarizeActionResults(results);
      await logActivity(
        db,
        kind === 'delete401' ? 'maintain_delete_401_batch' : 'maintain_delete_quota_batch',
        formatActionSummaryDetail(
          kind === 'delete401' ? '删除401批次' : '删除限额批次',
          summary,
          [`本地删除=${deletedBatchNames.length}`]
        ),
        username
      );
    } else if (kind === 'disableQuota') {
      const results = await runDisableBatch(config, names, actionConcurrency);
      await logActionResults(db, 'maintain_disable_quota_account', results, username, { logSuccesses: !isCronRun });
      const updates: Record<string, unknown>[] = [];
      for (const result of results) {
        if (result.ok) stats.disabled_quota++;
        const record = existingState.get(result.name);
        if (record) {
          record.last_action = 'disable_quota';
          record.last_action_status = result.ok ? 'success' : 'failed';
          record.last_action_error = result.error;
          if (result.ok) {
            record.managed_reason = 'quota_disabled';
            record.disabled = 1;
            record.is_recovered = 0;
          }
          record.updated_at = nowIso;
          updates.push(record);
        }
      }
      if (updates.length > 0) await upsertAuthAccountsInChunks(db, updates, 5);
      const summary = summarizeActionResults(results);
      await logActivity(db, 'maintain_disable_quota_batch', formatActionSummaryDetail('禁用限额批次', summary), username);
    } else if (kind === 'reenable') {
      const results = await runReenableBatch(config, names, actionConcurrency);
      await logActionResults(db, 'maintain_reenable_account', results, username, { logSuccesses: !isCronRun });
      const updates: Record<string, unknown>[] = [];
      for (const result of results) {
        if (result.ok) stats.reenabled++;
        const record = existingState.get(result.name);
        if (record) {
          record.last_action = 'reenable_quota';
          record.last_action_status = result.ok ? 'success' : 'failed';
          record.last_action_error = result.error;
          if (result.ok) {
            record.managed_reason = null;
            record.disabled = 0;
            record.is_recovered = 0;
            record.is_quota_limited = 0;
            record.probe_error_kind = null;
            record.probe_error_text = null;
          }
          record.updated_at = nowIso;
          updates.push(record);
        }
      }
      if (updates.length > 0) await upsertAuthAccountsInChunks(db, updates, 5);
      const summary = summarizeActionResults(results);
      await logActivity(db, 'maintain_reenable_batch', formatActionSummaryDetail('恢复启用批次', summary), username);
    }
  }

  await updateTask(db, taskId, {
    status: 'running',
    progress: endIndex,
    total: pendingOperations.length,
    result: JSON.stringify({
      ...state,
      phase: 'maintaining',
      action_index: endIndex,
      action_total: pendingOperations.length,
      deleted_names: Array.from(deletedNames),
      stats,
      current_batch: `${actionIndex + 1}-${endIndex}`,
    }),
  });

  if (endIndex < pendingOperations.length) {
    return {
      success: true,
      total_files: toFiniteNumber(state.total_files, 0),
      filtered_count: toFiniteNumber(state.filtered_count, 0),
      probed_count: toFiniteNumber(state.probed_count, 0),
      invalid_401_count: 0,
      quota_limited_count: 0,
      recovered_count: 0,
      failure_count: 0,
    };
  }

  const finalState = await loadExistingState(db);
  const finalCandidates = Array.from(finalState.values()).filter((r) =>
    matchesFilters(r, config.target_type, config.provider)
  );
  const finalInvalidRecords = finalCandidates.filter((r) => Number(r.is_invalid_401) === 1);
  const finalQuotaRecords = finalCandidates.filter((r) => Number(r.is_quota_limited) === 1);
  const finalRecoveredRecords = finalCandidates.filter((r) => Number(r.is_recovered) === 1);
  const finalFailureRecords = finalCandidates.filter((r) => r.probe_error_kind);
  const finalProbedFiles = finalCandidates.filter((r) => r.last_probed_at).length;

  const maintainRunId = toFiniteNumber(state.maintain_run_id || params.maintain_run_id, 0);
  if (maintainRunId > 0) {
    await finishScanRun(db, maintainRunId, {
      status: 'success',
      total_files: toFiniteNumber(state.total_files, 0),
      filtered_files: finalCandidates.length,
      probed_files: finalProbedFiles,
      invalid_401_count: finalInvalidRecords.length,
      quota_limited_count: finalQuotaRecords.length,
      recovered_count: finalRecoveredRecords.length,
    });
  }

  const engineResult: EngineResult = {
    success: true,
    total_files: toFiniteNumber(state.total_files, 0),
    filtered_count: finalCandidates.length,
    probed_count: finalProbedFiles,
    invalid_401_count: finalInvalidRecords.length,
    quota_limited_count: finalQuotaRecords.length,
    recovered_count: finalRecoveredRecords.length,
    failure_count: finalFailureRecords.length,
    actions: {
      deleted_401: stats.deleted_401,
      disabled_quota: stats.disabled_quota,
      deleted_quota: stats.deleted_quota,
      reenabled: stats.reenabled,
    },
  };

  await updateTask(db, taskId, {
    status: 'completed',
    progress: pendingOperations.length,
    total: pendingOperations.length,
    finished_at: new Date().toISOString(),
    result: JSON.stringify(engineResult),
  });

  await logActivity(
    db,
    'maintain',
    `维护完成: 删除401=${stats.deleted_401} 删除本地=${stats.deleted_local} 禁用限额=${stats.disabled_quota} 删除限额=${stats.deleted_quota} 恢复=${stats.reenabled} 剩余401=${finalInvalidRecords.length} 剩余限额=${finalQuotaRecords.length}`,
    username
  );

  return engineResult;
}

// ── scan ─────────────────────────────────────────────────────────────

export async function runScan(
  db: D1Database,
  config: AppConfig,
  taskId: number,
  username?: string,
  options?: { finalizeTask?: boolean }
): Promise<EngineResult> {
  const finalizeTask = options?.finalizeTask !== false;
  const runId = await startScanRun(db, 'scan', config as unknown as Record<string, unknown>);

  try {
    // Phase 1 — fetch file list
    await updateTask(db, taskId, {
      status: 'running',
      started_at: new Date().toISOString(),
      result: JSON.stringify({ phase: 'fetching_files' }),
    });

    const nowIso = new Date().toISOString();
    const files = await fetchAuthFiles(config.base_url, config.token, config.timeout);

    await safeTaskUpdate(db, taskId, {
      result: JSON.stringify({ phase: 'loading_local_state', total_files: files.length }),
    });

    const existingState = await loadExistingState(db);
    const probeConcurrency = 1;

    const inventoryRecords: Record<string, unknown>[] = [];
    for (const item of files) {
      const r = item as Record<string, unknown>;
      const name = String(r.name ?? r.id ?? '').trim();
      if (!name) continue;
      inventoryRecords.push(buildAuthRecord(r, existingState.get(name) ?? null, nowIso));
    }
    const inventoryByName = new Map<string, Record<string, unknown>>();
    for (const record of inventoryRecords) {
      inventoryByName.set(String(record.name), record);
    }

    // Filter candidates first; do not let full local snapshot sync block scan progress.
    const candidateRecords = inventoryRecords.filter((r) =>
      matchesFilters(r, config.target_type, config.provider)
    );

    const total = candidateRecords.length;
    await updateTask(db, taskId, {
      total,
      progress: 0,
      result: JSON.stringify({ phase: 'probing', total_files: files.length, filtered: total }),
    });

    // Phase 2 — probe in batches
    let probed = 0;
    const batches = chunks(candidateRecords, PROBE_BATCH_SIZE);

    for (const batch of batches) {
      if (await shouldStopTask(db, taskId)) {
        await finishScanRun(db, runId, {
          status: 'stopped',
          total_files: files.length,
          filtered_files: candidateRecords.length,
          probed_files: probed,
          invalid_401_count: 0,
          quota_limited_count: 0,
          recovered_count: 0,
        });
        if (finalizeTask) {
          await markTaskStopped(db, taskId, {
            phase: 'probing',
            probed,
            total_files: files.length,
            filtered: total,
          });
        }
        return {
          success: false,
          total_files: files.length,
          filtered_count: candidateRecords.length,
          probed_count: probed,
          invalid_401_count: 0,
          quota_limited_count: 0,
          recovered_count: 0,
          failure_count: 0,
          error: 'stopped',
        };
      }

      const batchLabel = `${probed + 1}-${Math.min(probed + batch.length, total)}`;
      await safeTaskUpdate(db, taskId, {
        progress: probed,
        result: JSON.stringify({
          phase: 'probing',
          probed,
          total_files: files.length,
          filtered: total,
          current_batch: batchLabel,
          current_item: String(batch[0]?.name ?? ''),
          current_step: 'probe_batch_start',
        }),
      });

      const itemTimeoutMs = 12000;
      const batchResults: Record<string, unknown>[] = [];
      let batchLastError: string | null = null;

      for (let index = 0; index < batch.length; index++) {
        if (await shouldStopTask(db, taskId)) {
          await finishScanRun(db, runId, {
            status: 'stopped',
            total_files: files.length,
            filtered_files: candidateRecords.length,
            probed_files: probed + index,
            invalid_401_count: 0,
            quota_limited_count: 0,
            recovered_count: 0,
          });
          if (finalizeTask) {
            await markTaskStopped(db, taskId, {
              phase: 'probing',
              probed: probed + index,
              total_files: files.length,
              filtered: total,
              current_batch: batchLabel,
              current_index: probed + index + 1,
            });
          }
          return {
            success: false,
            total_files: files.length,
            filtered_count: candidateRecords.length,
            probed_count: probed + index,
            invalid_401_count: 0,
            quota_limited_count: 0,
            recovered_count: 0,
            failure_count: 0,
            error: 'stopped',
          };
        }

        const record = batch[index];
        const fallbackName = String(record?.name ?? '');

        await safeTaskUpdate(db, taskId, {
          progress: probed + index,
          result: JSON.stringify({
            phase: 'probing',
            probed: probed + index,
            total_files: files.length,
            filtered: total,
            current_batch: batchLabel,
            current_index: probed + index + 1,
            current_item: fallbackName,
            current_step: 'probe_item_start',
            last_error: batchLastError,
          }),
        });

        let merged: Record<string, unknown>;
        try {
          const result = await withTimeout(
            probeWhamUsage(
              config.base_url,
              config.token,
              record,
              config.timeout,
              config.retries,
              config.user_agent,
              config.quota_disable_threshold,
              itemTimeoutMs
            ),
            itemTimeoutMs + 2000,
            `probeWhamUsage ${fallbackName}`
          );
          merged = { ...result, updated_at: new Date().toISOString() } as Record<string, unknown>;
        } catch (error) {
          const errText = String(error);
          merged = {
            ...record,
            last_probed_at: new Date().toISOString(),
            probe_error_kind: errText.includes('timeout after') ? 'probe_outer_timeout' : 'probe_wrapper_error',
            probe_error_text: errText,
            updated_at: new Date().toISOString(),
          } as Record<string, unknown>;
        }

        const upsertSingle = await safeAccountUpsert(db, merged, itemTimeoutMs);
        if (!upsertSingle.ok) {
          merged.probe_error_kind = 'db_write_timeout';
          merged.probe_error_text = upsertSingle.error ?? 'auth_accounts single write failed';
          batchLastError = upsertSingle.error ?? 'auth_accounts single write failed';
        }

        const name = String(merged.name ?? fallbackName);
        const original = inventoryByName.get(name);
        if (original) Object.assign(original, merged);
        batchResults.push(merged);
        batchLastError = (merged.probe_error_text as string | null | undefined) ?? batchLastError;

        await safeTaskUpdate(db, taskId, {
          progress: probed + index + 1,
          result: JSON.stringify({
            phase: 'probing',
            probed: probed + index + 1,
            total_files: files.length,
            filtered: total,
            current_batch: batchLabel,
            current_index: probed + index + 1,
            current_item: name,
            current_step: 'probe_item_done',
            last_error: batchLastError,
          }),
        });
      }

      probed += batch.length;
      const lastItem = String(batchResults[batchResults.length - 1]?.name ?? batch[batch.length - 1]?.name ?? '');
      await safeTaskUpdate(db, taskId, {
        progress: probed,
        result: JSON.stringify({
          phase: 'probing',
          probed,
          total_files: files.length,
          filtered: total,
          current_batch: batchLabel,
          current_item: lastItem,
          current_step: 'probe_batch_done',
          last_error: batchLastError,
        }),
      });
    }

    // Phase 3 — classify
    const currentCandidates = inventoryRecords.filter((r) =>
      matchesFilters(r, config.target_type, config.provider)
    );
    const invalidRecords = currentCandidates.filter((r) => r.is_invalid_401 === 1);
    const quotaRecords = currentCandidates.filter((r) => r.is_quota_limited === 1);
    const recoveredRecords = currentCandidates.filter((r) => r.is_recovered === 1);
    const failureRecords = currentCandidates.filter((r) => r.probe_error_kind);
    const probedFiles = currentCandidates.filter((r) => r.last_probed_at).length;

    await finishScanRun(db, runId, {
      status: 'success',
      total_files: files.length,
      filtered_files: currentCandidates.length,
      probed_files: probedFiles,
      invalid_401_count: invalidRecords.length,
      quota_limited_count: quotaRecords.length,
      recovered_count: recoveredRecords.length,
    });

    await saveCacheMeta(db, {
      cache_base_url: config.base_url,
      cache_last_success_at: new Date().toISOString(),
      cache_last_status: 'success',
      cache_last_error: '',
    });

    const engineResult: EngineResult = {
      success: true,
      total_files: files.length,
      filtered_count: currentCandidates.length,
      probed_count: probedFiles,
      invalid_401_count: invalidRecords.length,
      quota_limited_count: quotaRecords.length,
      recovered_count: recoveredRecords.length,
      failure_count: failureRecords.length,
    };

    await safeTaskUpdate(db, taskId, {
      result: JSON.stringify({
        phase: 'finalizing_snapshot',
        total_files: files.length,
        filtered: currentCandidates.length,
        probed: probedFiles,
      }),
    });

    let snapshotSyncError: string | null = null;
    let deletedStaleCount = 0;
    try {
      await upsertAuthAccounts(db, inventoryRecords);
      const remoteNames = inventoryRecords.map((r) => String(r.name)).filter(Boolean);
      deletedStaleCount = await deleteAccountsNotInSet(db, remoteNames);
    } catch (error) {
      snapshotSyncError = String(error);
    }

    if (snapshotSyncError) {
      engineResult.error = snapshotSyncError;
    }

    if (finalizeTask) {
      await updateTask(db, taskId, {
        status: 'completed',
        progress: total,
        finished_at: new Date().toISOString(),
        result: JSON.stringify({
          ...engineResult,
          snapshot_sync_error: snapshotSyncError,
          deleted_stale_count: deletedStaleCount,
        }),
      });
    }

    await logActivity(
      db,
      'scan',
      `扫描完成: 总计=${files.length} 过滤=${currentCandidates.length} 401=${invalidRecords.length} 限额=${quotaRecords.length} 恢复=${recoveredRecords.length} 清理旧缓存=${deletedStaleCount}${snapshotSyncError ? ` 快照同步异常=${snapshotSyncError}` : ''}`,
      username
    );

    return engineResult;
  } catch (e) {
    const errMsg = String(e);
    await finishScanRun(db, runId, {
      status: 'failed', total_files: 0, filtered_files: 0, probed_files: 0,
      invalid_401_count: 0, quota_limited_count: 0, recovered_count: 0,
    });
    await saveCacheMeta(db, {
      cache_last_status: 'failed',
      cache_last_error: errMsg,
    });
    if (finalizeTask) {
      await updateTask(db, taskId, {
        status: 'failed', finished_at: new Date().toISOString(), error: errMsg,
      });
    }
    return { success: false, total_files: 0, filtered_count: 0, probed_count: 0,
      invalid_401_count: 0, quota_limited_count: 0, recovered_count: 0, failure_count: 0, error: errMsg };
  }
}

// ── maintain ─────────────────────────────────────────────────────────

export async function runMaintain(
  db: D1Database,
  config: AppConfig,
  taskId: number,
  username?: string
): Promise<EngineResult> {
  // Phase 1: scan
  await updateTask(db, taskId, {
    status: 'running', started_at: new Date().toISOString(),
    result: JSON.stringify({ phase: 'scanning' }),
  });

  // Create a sub-task for scan progress tracking
  const scanResult = await runScan(db, config, taskId, username, { finalizeTask: false });
  if (!scanResult.success) {
    await updateTask(db, taskId, {
      status: 'failed', finished_at: new Date().toISOString(), error: scanResult.error || 'scan failed',
    });
    return scanResult;
  }

  // Phase 2: actions
  await updateTask(db, taskId, {
    result: JSON.stringify({ phase: 'maintaining', scan: scanResult }),
  });

  const existingState = await loadExistingState(db);
  const candidateRecords = Array.from(existingState.values()).filter((r) =>
    matchesFilters(r, config.target_type, config.provider)
  );
  const invalidRecords = candidateRecords.filter((r) => Number(r.is_invalid_401) === 1);
  const quotaRecords = candidateRecords.filter(
    (r) => Number(r.is_quota_limited) === 1 && Number(r.is_invalid_401) !== 1
  );
  const recoveredRecords = candidateRecords.filter((r) => Number(r.is_recovered) === 1);

  await logActivity(
    db,
    'maintain_started',
    `维护开始: 候选=${candidateRecords.length} 401=${invalidRecords.length} 限额=${quotaRecords.length} 恢复候选=${recoveredRecords.length} quota_action=${config.quota_action} delete_401=${config.delete_401 ? 1 : 0} auto_reenable=${config.auto_reenable ? 1 : 0}`,
    username
  );

  const deletedNames = new Set<string>();
  const nowIso = new Date().toISOString();
  const actionConcurrency = boundedConcurrency(config.action_workers, DEFAULT_ACTION_CONCURRENCY, MAX_ACTION_CONCURRENCY);
  const isCronRun = username === 'system';
  let deleted401 = 0, disabledQuota = 0, deletedQuota = 0, reenabled = 0;
  let deletedLocal = 0;

  // Delete 401 — in batches
  if (config.delete_401 && invalidRecords.length > 0) {
    const names = invalidRecords.map((r) => String(r.name)).filter(Boolean);
    for (const batch of chunks(names, ACTION_BATCH_SIZE)) {
      const tasks = batch.map((name) => () =>
        deleteAccount(config.base_url, config.token, name, config.timeout, config.delete_retries)
      );
      const results = await runWithConcurrency(tasks, actionConcurrency);
      await logActionResults(db, 'maintain_delete_401_account', results, username, { logSuccesses: !isCronRun });
      const updates: Record<string, unknown>[] = [];
      for (const result of results) {
        if (result.ok) { deletedNames.add(result.name); deleted401++; }
        const record = existingState.get(result.name);
        if (record) {
          record.last_action = 'delete_401';
          record.last_action_status = result.ok ? 'success' : 'failed';
          record.last_action_error = result.error;
          record.managed_reason = result.ok ? 'deleted_401' : (record.managed_reason ?? null);
          record.updated_at = nowIso;
          updates.push(record);
        }
      }
      await upsertAuthAccounts(db, updates);

      const deletedBatchNames = results.filter((result) => result.ok).map((result) => result.name);
      if (deletedBatchNames.length > 0) {
        deletedLocal += await deleteAccountsFromDB(db, deletedBatchNames);
        for (const name of deletedBatchNames) existingState.delete(name);
      }

      const summary = summarizeActionResults(results);
      await logActivity(
        db,
        'maintain_delete_401_batch',
        formatActionSummaryDetail('删除401批次', summary, [`本地删除=${deletedBatchNames.length}`]),
        username
      );
    }
  }

  // Quota action — in batches
  if (config.quota_action === 'disable') {
    const toDisable = quotaRecords.filter(
      (r) => !deletedNames.has(String(r.name)) && Number(r.disabled) !== 1
    );
    for (const batch of chunks(toDisable, ACTION_BATCH_SIZE)) {
      const tasks = batch.map((r) => () =>
        setAccountDisabled(config.base_url, config.token, String(r.name), true, config.timeout)
      );
      const results = await runWithConcurrency(tasks, actionConcurrency);
      await logActionResults(db, 'maintain_disable_quota_account', results, username, { logSuccesses: !isCronRun });
      const updates: Record<string, unknown>[] = [];
      for (const result of results) {
        if (result.ok) disabledQuota++;
        const record = existingState.get(result.name);
        if (record) {
          record.last_action = 'disable_quota';
          record.last_action_status = result.ok ? 'success' : 'failed';
          record.last_action_error = result.error;
          if (result.ok) {
            record.managed_reason = 'quota_disabled';
            record.disabled = 1;
            record.is_recovered = 0;
          }
          record.updated_at = nowIso;
          updates.push(record);
        }
      }
      await upsertAuthAccounts(db, updates);

      const summary = summarizeActionResults(results);
      await logActivity(
        db,
        'maintain_disable_quota_batch',
        formatActionSummaryDetail('禁用限额批次', summary),
        username
      );
    }
    // Mark already-disabled
    const alreadyDisabled = quotaRecords.filter(
      (r) => !deletedNames.has(String(r.name)) && Number(r.disabled) === 1
    );
    if (alreadyDisabled.length > 0) {
      const updates = alreadyDisabled.map((r) => ({
        ...r, managed_reason: 'quota_disabled', last_action: 'mark_quota_disabled',
        last_action_status: 'success', last_action_error: null, updated_at: nowIso,
      }));
      await upsertAuthAccounts(db, updates);
      await logActivity(
        db,
        'maintain_mark_quota_disabled',
        `标记已禁用限额账号: ${alreadyDisabled.map((row) => String(row.name)).slice(0, 20).join(', ')} | 数量=${alreadyDisabled.length}`,
        username
      );
    }
  } else {
    const toDelete = quotaRecords
      .filter((r) => !deletedNames.has(String(r.name)))
      .map((r) => String(r.name)).filter(Boolean);
    for (const batch of chunks(toDelete, ACTION_BATCH_SIZE)) {
      const tasks = batch.map((name) => () =>
        deleteAccount(config.base_url, config.token, name, config.timeout, config.delete_retries)
      );
      const results = await runWithConcurrency(tasks, actionConcurrency);
      await logActionResults(db, 'maintain_delete_quota_account', results, username, { logSuccesses: !isCronRun });
      const updates: Record<string, unknown>[] = [];
      for (const result of results) {
        if (result.ok) { deletedNames.add(result.name); deletedQuota++; }
        const record = existingState.get(result.name);
        if (record) {
          record.last_action = 'delete_quota';
          record.last_action_status = result.ok ? 'success' : 'failed';
          record.last_action_error = result.error;
          if (result.ok) record.managed_reason = 'quota_deleted';
          record.updated_at = nowIso;
          updates.push(record);
        }
      }
      await upsertAuthAccounts(db, updates);

      const deletedBatchNames = results.filter((result) => result.ok).map((result) => result.name);
      if (deletedBatchNames.length > 0) {
        deletedLocal += await deleteAccountsFromDB(db, deletedBatchNames);
        for (const name of deletedBatchNames) existingState.delete(name);
      }

      const summary = summarizeActionResults(results);
      await logActivity(
        db,
        'maintain_delete_quota_batch',
        formatActionSummaryDetail('删除限额批次', summary, [`本地删除=${deletedBatchNames.length}`]),
        username
      );
    }
  }

  // Re-enable recovered — in batches
  if (config.auto_reenable) {
    const scope = config.reenable_scope;
    const recoverable = scope === 'signal'
      ? recoveredRecords
      : recoveredRecords.filter((r) => String(r.managed_reason ?? '') === 'quota_disabled');
    const toReenable = recoverable
      .filter((r) => !deletedNames.has(String(r.name)))
      .map((r) => String(r.name)).filter(Boolean);
    for (const batch of chunks(toReenable, ACTION_BATCH_SIZE)) {
      const tasks = batch.map((name) => () =>
        setAccountDisabled(config.base_url, config.token, name, false, config.timeout)
      );
      const results = await runWithConcurrency(tasks, actionConcurrency);
      await logActionResults(db, 'maintain_reenable_account', results, username, { logSuccesses: !isCronRun });
      const updates: Record<string, unknown>[] = [];
      for (const result of results) {
        if (result.ok) reenabled++;
        const record = existingState.get(result.name);
        if (record) {
          record.last_action = 'reenable_quota';
          record.last_action_status = result.ok ? 'success' : 'failed';
          record.last_action_error = result.error;
          if (result.ok) {
            record.managed_reason = null;
            record.disabled = 0;
            record.is_recovered = 0;
            record.is_quota_limited = 0;
            record.probe_error_kind = null;
            record.probe_error_text = null;
          }
          record.updated_at = nowIso;
          updates.push(record);
        }
      }
      await upsertAuthAccounts(db, updates);

      const summary = summarizeActionResults(results);
      await logActivity(
        db,
        'maintain_reenable_batch',
        formatActionSummaryDetail('恢复启用批次', summary),
        username
      );
    }
  }

  const finalState = await loadExistingState(db);
  const finalCandidates = Array.from(finalState.values()).filter((r) =>
    matchesFilters(r, config.target_type, config.provider)
  );
  const finalInvalidRecords = finalCandidates.filter((r) => Number(r.is_invalid_401) === 1);
  const finalQuotaRecords = finalCandidates.filter((r) => Number(r.is_quota_limited) === 1);
  const finalRecoveredRecords = finalCandidates.filter((r) => Number(r.is_recovered) === 1);
  const finalFailureRecords = finalCandidates.filter((r) => r.probe_error_kind);
  const finalProbedFiles = finalCandidates.filter((r) => r.last_probed_at).length;

  const maintainRunId = await startScanRun(db, 'maintain', config as unknown as Record<string, unknown>);
  await finishScanRun(db, maintainRunId, {
    status: 'success',
    total_files: scanResult.total_files,
    filtered_files: finalCandidates.length,
    probed_files: finalProbedFiles,
    invalid_401_count: finalInvalidRecords.length,
    quota_limited_count: finalQuotaRecords.length,
    recovered_count: finalRecoveredRecords.length,
  });

  const engineResult: EngineResult = {
    success: true,
    total_files: scanResult.total_files,
    filtered_count: finalCandidates.length,
    probed_count: finalProbedFiles,
    invalid_401_count: finalInvalidRecords.length,
    quota_limited_count: finalQuotaRecords.length,
    recovered_count: finalRecoveredRecords.length,
    failure_count: finalFailureRecords.length,
    actions: { deleted_401: deleted401, disabled_quota: disabledQuota, deleted_quota: deletedQuota, reenabled },
  };

  await updateTask(db, taskId, {
    status: 'completed', finished_at: new Date().toISOString(),
    result: JSON.stringify(engineResult),
  });

  await logActivity(db, 'maintain',
    `维护完成: 删除401=${deleted401} 删除本地=${deletedLocal} 禁用限额=${disabledQuota} 删除限额=${deletedQuota} 恢复=${reenabled} 剩余401=${finalInvalidRecords.length} 剩余限额=${finalQuotaRecords.length}`,
    username
  );

  return engineResult;
}

// ── upload ────────────────────────────────────────────────────────────

export interface UploadFileItem {
  file_name: string;
  content: string;
}

export async function runUpload(
  db: D1Database,
  config: AppConfig,
  files: UploadFileItem[],
  taskId: number,
  username?: string
): Promise<EngineResult> {
  let uploaded = 0, skipped = 0, failed = 0;
  const uploadConcurrency = boundedConcurrency(config.upload_workers, DEFAULT_UPLOAD_CONCURRENCY, MAX_UPLOAD_CONCURRENCY);

  // Check remote duplicates
  let remoteNames = new Set<string>();
  if (!config.upload_force) {
    try {
      const remoteFiles = await fetchAuthFiles(config.base_url, config.token, config.timeout);
      for (const f of remoteFiles) {
        const name = String((f as Record<string, unknown>).name ?? '').trim();
        if (name) remoteNames.add(name);
      }
    } catch { /* proceed anyway */ }
  }

  const candidates = files.filter((f) => {
    if (!config.upload_force && remoteNames.has(f.file_name)) { skipped++; return false; }
    return true;
  });

  await updateTask(db, taskId, {
    total: candidates.length, progress: 0, status: 'running',
    started_at: new Date().toISOString(),
    result: JSON.stringify({ phase: 'uploading', total: candidates.length, skipped }),
  });

  let processed = 0;
  for (const batch of chunks(candidates, UPLOAD_BATCH_SIZE)) {
    const tasks = batch.map((file) => async () => {
      const result = await uploadAuthFile(
        config.base_url, config.token, file.file_name, file.content,
        config.upload_method, config.timeout, config.upload_retries
      );
      if (result.ok) uploaded++; else failed++;
      return result;
    });
    await runWithConcurrency(tasks, uploadConcurrency);
    processed += batch.length;
    await updateTask(db, taskId, {
      progress: processed,
      result: JSON.stringify({ phase: 'uploading', uploaded, skipped, failed, processed }),
    });
  }

  const engineResult: EngineResult = {
    success: failed === 0,
    total_files: files.length, filtered_count: candidates.length,
    probed_count: 0, invalid_401_count: 0, quota_limited_count: 0,
    recovered_count: 0, failure_count: failed,
    upload: { uploaded, skipped, failed },
  };

  await updateTask(db, taskId, {
    status: failed === 0 ? 'completed' : 'failed',
    finished_at: new Date().toISOString(),
    result: JSON.stringify(engineResult),
  });

  await logActivity(db, 'upload', `上传完成: 成功=${uploaded} 跳过=${skipped} 失败=${failed}`, username);
  return engineResult;
}

// ── maintain-refill ──────────────────────────────────────────────────

export async function runMaintainRefill(
  db: D1Database,
  config: AppConfig,
  uploadFiles: UploadFileItem[],
  taskId: number,
  username?: string
): Promise<EngineResult> {
  const maintainResult = await runMaintain(db, config, taskId, username);
  if (!maintainResult.success) return maintainResult;

  const state = await loadExistingState(db);
  const candidates = Array.from(state.values()).filter((r) =>
    matchesFilters(r, config.target_type, config.provider)
  );
  const validCount = countValidAccounts(candidates);
  const minValid = config.min_valid_accounts;

  if (validCount >= minValid) {
    await logActivity(db, 'maintain-refill', `有效账号充足: valid=${validCount} >= min=${minValid}`, username);
    return maintainResult;
  }

  const gap = minValid - validCount;
  const uploadCount = config.refill_strategy === 'fixed' ? minValid : gap;
  const filesToUpload = uploadFiles.slice(0, uploadCount);

  if (filesToUpload.length === 0) {
    const res: EngineResult = {
      ...maintainResult,
      error: `有效账号不足: valid=${validCount} < min=${minValid}, 但无可上传文件`,
    };
    await updateTask(db, taskId, {
      status: 'completed', finished_at: new Date().toISOString(),
      result: JSON.stringify(res),
    });
    return res;
  }

  const uploadResult = await runUpload(db, config, filesToUpload, taskId, username);

  await logActivity(db, 'maintain-refill',
    `补充完成: valid_before=${validCount} uploaded=${uploadResult.upload?.uploaded ?? 0}`, username);

  return { ...maintainResult, upload: uploadResult.upload };
}
