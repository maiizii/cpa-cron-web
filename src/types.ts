export interface Env {
  DB: D1Database;
  KV: KVNamespace;
  CPA_BASE_URL?: string;
  CPA_TOKEN?: string;
  ADMIN_USERNAME?: string;
  ADMIN_PASSWORD?: string;
  ADMIN_PASSWORD_HASH?: string;
  JWT_SECRET?: string;
}

export type AppVariables = {
  user: Record<string, unknown>;
};

export type HonoEnv = {
  Bindings: Env;
  Variables: AppVariables;
};

export interface AppConfig {
  base_url: string;
  token: string;
  target_type: string;
  provider: string;
  probe_workers: number;
  action_workers: number;
  timeout: number;
  retries: number;
  delete_retries: number;
  quota_action: 'disable' | 'delete';
  quota_disable_threshold: number;
  delete_401: boolean;
  auto_reenable: boolean;
  reenable_scope: 'signal' | 'managed';
  upload_workers: number;
  upload_retries: number;
  upload_method: 'json' | 'multipart';
  upload_force: boolean;
  min_valid_accounts: number;
  refill_strategy: 'to-threshold' | 'fixed';
  user_agent: string;
}

export interface AuthAccount {
  name: string;
  disabled: number;
  id_token_json: string | null;
  email: string | null;
  provider: string | null;
  source: string | null;
  unavailable: number;
  auth_index: string | null;
  account: string | null;
  type: string | null;
  runtime_only: number;
  status: string | null;
  status_message: string | null;
  chatgpt_account_id: string | null;
  id_token_plan_type: string | null;
  auth_updated_at: string | null;
  auth_modtime: string | null;
  auth_last_refresh: string | null;
  api_http_status: number | null;
  api_status_code: number | null;
  usage_allowed: number | null;
  usage_limit_reached: number | null;
  usage_plan_type: string | null;
  usage_email: string | null;
  usage_reset_at: number | null;
  usage_reset_after_seconds: number | null;
  usage_spark_allowed: number | null;
  usage_spark_limit_reached: number | null;
  usage_spark_reset_at: number | null;
  usage_spark_reset_after_seconds: number | null;
  quota_signal_source: string | null;
  is_invalid_401: number;
  is_quota_limited: number;
  is_recovered: number;
  probe_error_kind: string | null;
  probe_error_text: string | null;
  managed_reason: string | null;
  last_action: string | null;
  last_action_status: string | null;
  last_action_error: string | null;
  last_seen_at: string;
  last_probed_at: string | null;
  updated_at: string;
  // runtime-only fields (not stored in DB columns)
  usage_remaining_ratio?: number | null;
  usage_spark_remaining_ratio?: number | null;
  quota_remaining_ratio?: number | null;
  quota_remaining_ratio_source?: string | null;
  quota_threshold_triggered?: number;
}

export interface ScanRun {
  run_id: number;
  mode: string;
  started_at: string;
  finished_at: string | null;
  status: string;
  total_files: number;
  filtered_files: number;
  probed_files: number;
  invalid_401_count: number;
  quota_limited_count: number;
  recovered_count: number;
}

export interface TaskRecord {
  id: number;
  type: string;
  status: string;
  params: string;
  result: string | null;
  progress: number;
  total: number;
  error: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export interface ActivityRecord {
  id: number;
  action: string;
  detail: string | null;
  username: string | null;
  created_at: string;
}

export interface ActionResult {
  name: string;
  ok: boolean;
  status_code: number | null;
  error: string | null;
  attempts?: number;
  disabled?: boolean;
}

export interface UploadResult {
  file_name: string;
  status_code: number | null;
  ok: boolean;
  outcome: string;
  error: string | null;
  error_kind: string | null;
}

export interface ScanResult {
  run_id: number;
  total_files: number;
  filtered_count: number;
  probed_count: number;
  invalid_401_count: number;
  quota_limited_count: number;
  recovered_count: number;
  failure_count: number;
}

export interface DashboardStats {
  total_accounts: number;
  active_accounts: number;
  disabled_accounts: number;
  invalid_401: number;
  quota_limited: number;
  recovered: number;
  probe_errors: number;
  last_scan: ScanRun | null;
  recent_activity: ActivityRecord[];
  cron_summary?: {
    last_started_at: string | null;
    last_completed_at: string | null;
    last_duration_seconds: number | null;
    last_status: 'success' | 'failed' | 'running' | 'never';
  };
}
