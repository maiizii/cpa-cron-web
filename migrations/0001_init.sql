-- cpa-cron-web - D1 Database Schema

-- Admin users table
CREATE TABLE IF NOT EXISTS admin_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_login_at TEXT
);

-- Auth accounts (mirrors Python SQLite schema)
CREATE TABLE IF NOT EXISTS auth_accounts (
    name TEXT PRIMARY KEY,
    disabled INTEGER NOT NULL DEFAULT 0,
    id_token_json TEXT,
    email TEXT,
    provider TEXT,
    source TEXT,
    unavailable INTEGER NOT NULL DEFAULT 0,
    auth_index TEXT,
    account TEXT,
    type TEXT,
    runtime_only INTEGER NOT NULL DEFAULT 0,
    status TEXT,
    status_message TEXT,
    chatgpt_account_id TEXT,
    id_token_plan_type TEXT,
    auth_updated_at TEXT,
    auth_modtime TEXT,
    auth_last_refresh TEXT,
    api_http_status INTEGER,
    api_status_code INTEGER,
    usage_allowed INTEGER,
    usage_limit_reached INTEGER,
    usage_plan_type TEXT,
    usage_email TEXT,
    usage_reset_at INTEGER,
    usage_reset_after_seconds INTEGER,
    usage_spark_allowed INTEGER,
    usage_spark_limit_reached INTEGER,
    usage_spark_reset_at INTEGER,
    usage_spark_reset_after_seconds INTEGER,
    quota_signal_source TEXT,
    is_invalid_401 INTEGER NOT NULL DEFAULT 0,
    is_quota_limited INTEGER NOT NULL DEFAULT 0,
    is_recovered INTEGER NOT NULL DEFAULT 0,
    probe_error_kind TEXT,
    probe_error_text TEXT,
    managed_reason TEXT,
    last_action TEXT,
    last_action_status TEXT,
    last_action_error TEXT,
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_probed_at TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Scan runs history
CREATE TABLE IF NOT EXISTS scan_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mode TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    total_files INTEGER NOT NULL DEFAULT 0,
    filtered_files INTEGER NOT NULL DEFAULT 0,
    probed_files INTEGER NOT NULL DEFAULT 0,
    invalid_401_count INTEGER NOT NULL DEFAULT 0,
    quota_limited_count INTEGER NOT NULL DEFAULT 0,
    recovered_count INTEGER NOT NULL DEFAULT 0,
    delete_401 INTEGER NOT NULL DEFAULT 0,
    quota_action TEXT NOT NULL DEFAULT 'disable',
    probe_workers INTEGER NOT NULL DEFAULT 100,
    action_workers INTEGER NOT NULL DEFAULT 100,
    timeout_seconds INTEGER NOT NULL DEFAULT 15,
    retries INTEGER NOT NULL DEFAULT 3
);

-- Upload tracking
CREATE TABLE IF NOT EXISTS auth_file_uploads (
    base_url TEXT NOT NULL,
    file_name TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_attempt_at TEXT,
    uploaded_at TEXT,
    last_http_status INTEGER,
    last_error TEXT,
    last_response TEXT,
    PRIMARY KEY (base_url, file_name, content_sha256)
);

CREATE INDEX IF NOT EXISTS idx_auth_file_uploads_status ON auth_file_uploads(status);
CREATE INDEX IF NOT EXISTS idx_auth_file_uploads_file_name ON auth_file_uploads(file_name);

-- CPA config (stored in DB, editable from admin panel)
CREATE TABLE IF NOT EXISTS app_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Task queue for async operations
CREATE TABLE IF NOT EXISTS task_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    params TEXT NOT NULL DEFAULT '{}',
    result TEXT,
    progress INTEGER NOT NULL DEFAULT 0,
    total INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    finished_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_task_queue_status ON task_queue(status);
CREATE INDEX IF NOT EXISTS idx_task_queue_type ON task_queue(type);

-- Activity log
CREATE TABLE IF NOT EXISTS activity_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    detail TEXT,
    username TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_activity_log_created ON activity_log(created_at);

-- Insert default config values
INSERT OR IGNORE INTO app_config (key, value) VALUES ('base_url', '');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('token', '');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('target_type', 'codex');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('provider', '');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('probe_workers', '100');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('action_workers', '100');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('timeout', '15');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('retries', '3');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('delete_retries', '2');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('quota_action', 'disable');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('quota_disable_threshold', '0');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('delete_401', 'true');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('auto_reenable', 'true');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('reenable_scope', 'signal');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('upload_workers', '20');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('upload_retries', '2');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('upload_method', 'json');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('upload_force', 'false');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('min_valid_accounts', '100');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('refill_strategy', 'to-threshold');
INSERT OR IGNORE INTO app_config (key, value) VALUES ('user_agent', 'codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal');
