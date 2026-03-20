export function htmlLayout(title: string, content: string, activeNav = ''): string {
  const navItems = [
    { href: '/', label: '仪表盘', icon: 'dashboard', id: 'dashboard' },
    { href: '/accounts', label: '账号管理', icon: 'people', id: 'accounts' },
    { href: '/operations', label: '运维操作', icon: 'build', id: 'operations' },
    { href: '/history', label: '扫描历史', icon: 'history', id: 'history' },
    { href: '/activity', label: '操作日志', icon: 'receipt_long', id: 'activity' },
    { href: '/settings', label: '系统配置', icon: 'settings', id: 'settings' },
  ];

  const navHtml = navItems
    .map(
      (item) =>
        `<a href="${item.href}" class="nav-item ${activeNav === item.id ? 'active' : ''}"><span class="material-icons">${item.icon}</span><span>${item.label}</span></a>`
    )
    .join('\n');

  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${title} - cpa-cron-web</title>
<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
<style>
:root {
  --bg: #0f1117;
  --bg-card: #1a1d27;
  --bg-card-hover: #222636;
  --bg-sidebar: #141620;
  --border: #2a2e3d;
  --text: #e4e6eb;
  --text-dim: #8b8fa3;
  --primary: #6c5ce7;
  --primary-hover: #7c6df7;
  --success: #00b894;
  --danger: #e74c3c;
  --warning: #fdcb6e;
  --info: #74b9ff;
  --radius: 8px;
  --sidebar-width: 240px;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; display: flex; }
a { color: var(--primary); text-decoration: none; }
/* Sidebar */
.sidebar { width: var(--sidebar-width); background: var(--bg-sidebar); border-right: 1px solid var(--border); height: 100vh; position: fixed; left: 0; top: 0; display: flex; flex-direction: column; z-index: 100; }
.sidebar-header { padding: 20px; border-bottom: 1px solid var(--border); }
.sidebar-header h1 { font-size: 18px; font-weight: 700; color: var(--primary); }
.sidebar-header p { font-size: 12px; color: var(--text-dim); margin-top: 4px; }
.sidebar-nav { flex: 1; padding: 12px 0; }
.nav-item { display: flex; align-items: center; gap: 12px; padding: 10px 20px; color: var(--text-dim); font-size: 14px; transition: all .15s; }
.nav-item:hover { color: var(--text); background: rgba(108,92,231,.1); }
.nav-item.active { color: var(--primary); background: rgba(108,92,231,.15); border-right: 3px solid var(--primary); }
.nav-item .material-icons { font-size: 20px; }
.sidebar-footer { padding: 16px 20px; border-top: 1px solid var(--border); }
.user-info { display: flex; align-items: center; gap: 10px; }
.user-avatar { width: 32px; height: 32px; border-radius: 50%; background: var(--primary); display: flex; align-items: center; justify-content: center; font-size: 14px; font-weight: 600; }
.user-name { font-size: 13px; }
.btn-logout { background: none; border: none; color: var(--text-dim); cursor: pointer; font-size: 12px; margin-top: 8px; }
.btn-logout:hover { color: var(--danger); }
/* Main */
.main { margin-left: var(--sidebar-width); flex: 1; min-height: 100vh; }
.topbar { padding: 16px 32px; border-bottom: 1px solid var(--border); display: flex; align-items: center; justify-content: space-between; }
.topbar h2 { font-size: 20px; font-weight: 600; }
.content { padding: 24px 32px; }
/* Cards */
.stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
.stat-card { background: var(--bg-card); border: 1px solid var(--border); border-radius: var(--radius); padding: 20px; transition: all .15s; }
.stat-card:hover { background: var(--bg-card-hover); border-color: var(--primary); }
.stat-card .label { font-size: 12px; color: var(--text-dim); text-transform: uppercase; letter-spacing: .5px; }
.stat-card .value { font-size: 28px; font-weight: 700; margin-top: 8px; }
.stat-card .value.success { color: var(--success); }
.stat-card .value.danger { color: var(--danger); }
.stat-card .value.warning { color: var(--warning); }
.stat-card .value.info { color: var(--info); }
/* Table */
.table-wrapper { background: var(--bg-card); border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
.table-toolbar { padding: 16px; display: flex; gap: 12px; align-items: center; flex-wrap: wrap; border-bottom: 1px solid var(--border); }
table { width: 100%; border-collapse: collapse; }
th, td { padding: 10px 16px; text-align: left; border-bottom: 1px solid var(--border); font-size: 13px; }
th { background: rgba(108,92,231,.08); color: var(--text-dim); font-weight: 600; text-transform: uppercase; font-size: 11px; letter-spacing: .5px; white-space: nowrap; }
tr:hover td { background: rgba(108,92,231,.04); }
/* Badge */
.badge { padding: 3px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
.badge-success { background: rgba(0,184,148,.15); color: var(--success); }
.badge-danger { background: rgba(231,76,60,.15); color: var(--danger); }
.badge-warning { background: rgba(253,203,110,.15); color: var(--warning); }
.badge-info { background: rgba(116,185,255,.15); color: var(--info); }
.badge-dim { background: rgba(139,143,163,.15); color: var(--text-dim); }
/* Buttons */
.btn { padding: 8px 16px; border-radius: var(--radius); font-size: 13px; font-weight: 500; border: 1px solid transparent; cursor: pointer; transition: all .15s; display: inline-flex; align-items: center; gap: 6px; }
.btn-primary { background: var(--primary); color: #fff; }
.btn-primary:hover { background: var(--primary-hover); }
.btn-danger { background: var(--danger); color: #fff; }
.btn-danger:hover { background: #c0392b; }
.btn-outline { background: transparent; border-color: var(--border); color: var(--text); }
.btn-outline:hover { border-color: var(--primary); color: var(--primary); }
.btn-sm { padding: 4px 10px; font-size: 12px; }
.btn:disabled { opacity: .5; cursor: not-allowed; }
/* Forms */
input, select, textarea { background: var(--bg); border: 1px solid var(--border); border-radius: var(--radius); padding: 8px 12px; color: var(--text); font-size: 13px; outline: none; transition: border-color .15s; }
input:focus, select:focus, textarea:focus { border-color: var(--primary); }
.form-group { margin-bottom: 16px; }
.form-group label { display: block; font-size: 12px; color: var(--text-dim); margin-bottom: 6px; font-weight: 500; }
.form-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
/* Pagination */
.pagination { display: flex; align-items: center; gap: 8px; padding: 16px; justify-content: center; }
.pagination button { background: var(--bg-card); border: 1px solid var(--border); color: var(--text); padding: 6px 12px; border-radius: var(--radius); cursor: pointer; font-size: 13px; }
.pagination button:hover { border-color: var(--primary); }
.pagination button:disabled { opacity: .4; cursor: not-allowed; }
.pagination span { font-size: 13px; color: var(--text-dim); }
/* Alert */
.alert { padding: 12px 16px; border-radius: var(--radius); margin-bottom: 16px; font-size: 13px; }
.alert-success { background: rgba(0,184,148,.1); border: 1px solid rgba(0,184,148,.3); color: var(--success); }
.alert-danger { background: rgba(231,76,60,.1); border: 1px solid rgba(231,76,60,.3); color: var(--danger); }
.alert-info { background: rgba(116,185,255,.1); border: 1px solid rgba(116,185,255,.3); color: var(--info); }
/* Modal */
.modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,.6); z-index: 200; display: none; align-items: center; justify-content: center; }
.modal-overlay.show { display: flex; }
.modal { background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; width: 90%; max-width: 500px; padding: 24px; }
.modal h3 { font-size: 16px; margin-bottom: 16px; }
.modal-actions { display: flex; gap: 8px; justify-content: flex-end; margin-top: 20px; }
/* Loading */
.spinner { display: inline-block; width: 20px; height: 20px; border: 2px solid var(--border); border-top-color: var(--primary); border-radius: 50%; animation: spin .6s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
/* Toast notification */
.toast-container { position: fixed; top: 20px; right: 20px; z-index: 9999; display: flex; flex-direction: column; gap: 10px; pointer-events: none; }
.toast { pointer-events: auto; padding: 14px 20px; border-radius: 10px; font-size: 13px; font-weight: 500; color: #fff; box-shadow: 0 8px 32px rgba(0,0,0,.4); transform: translateX(120%); animation: toastIn .35s ease forwards; display: flex; align-items: center; gap: 10px; max-width: 420px; backdrop-filter: blur(8px); }
.toast.toast-out { animation: toastOut .3s ease forwards; }
.toast-success { background: linear-gradient(135deg, rgba(0,184,148,.92), rgba(0,150,120,.92)); }
.toast-danger { background: linear-gradient(135deg, rgba(231,76,60,.92), rgba(192,57,43,.92)); }
.toast-info { background: linear-gradient(135deg, rgba(116,185,255,.92), rgba(108,92,231,.92)); }
.toast-warning { background: linear-gradient(135deg, rgba(253,203,110,.92), rgba(225,177,44,.92)); color: #1a1d27; }
.toast .material-icons { font-size: 20px; flex-shrink: 0; }
@keyframes toastIn { from { transform: translateX(120%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
@keyframes toastOut { from { transform: translateX(0); opacity: 1; } to { transform: translateX(120%); opacity: 0; } }
/* Pulse animation for active states */
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: .6; } }
.pulse { animation: pulse 1.5s ease-in-out infinite; }
/* Progress bar shine */
@keyframes shine { from { left: -50%; } to { left: 150%; } }
.progress-bar-animated { position: relative; overflow: hidden; }
.progress-bar-animated::after { content: ''; position: absolute; top: 0; left: -50%; width: 50%; height: 100%; background: linear-gradient(90deg, transparent, rgba(255,255,255,.2), transparent); animation: shine 1.5s ease infinite; }
/* Responsive */
@media (max-width: 768px) {
  .sidebar { display: none; }
  .main { margin-left: 0; }
  .stats-grid { grid-template-columns: repeat(2, 1fr); }
  .content { padding: 16px; }
}
/* Empty state */
.empty { text-align: center; padding: 48px 16px; color: var(--text-dim); }
.empty .material-icons { font-size: 48px; margin-bottom: 12px; opacity: .3; }
</style>
</head>
<body>
<aside class="sidebar">
  <div class="sidebar-header">
    <h1>cpa-cron-web</h1>
    <p>v1.0 | Web Management</p>
  </div>
  <nav class="sidebar-nav">
    ${navHtml}
  </nav>
  <div class="sidebar-footer">
    <div class="user-info">
      <div class="user-avatar" id="userAvatar">A</div>
      <div>
        <div class="user-name" id="userName">Admin</div>
        <button class="btn-logout" onclick="logout()">退出登录</button>
      </div>
    </div>
  </div>
</aside>
<div class="main">
  <div class="topbar">
    <h2>${title}</h2>
    <div id="topbarActions"></div>
  </div>
  <script>
  // Toast notification system
  (function() {
    const container = document.createElement('div');
    container.className = 'toast-container';
    document.body.appendChild(container);
    window.showToast = function(message, type, duration) {
      type = type || 'info';
      duration = duration || 4000;
      const iconMap = { success: 'check_circle', danger: 'error', warning: 'warning', info: 'info' };
      const toast = document.createElement('div');
      toast.className = 'toast toast-' + type;
      toast.innerHTML = '<span class="material-icons">' + (iconMap[type] || 'info') + '</span><span>' + message + '</span>';
      container.appendChild(toast);
      setTimeout(function() {
        toast.classList.add('toast-out');
        setTimeout(function() { toast.remove(); }, 300);
      }, duration);
    };
  })();

  window.formatChinaTime = function(value) {
    if (!value) return '-';
    const raw = String(value).trim();
    if (!raw) return '-';
    let date = null;
    if (/^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$/.test(raw)) {
      date = new Date(raw.replace(' ', 'T') + 'Z');
    } else {
      date = new Date(raw);
    }
    if (Number.isNaN(date.getTime())) return raw;
    return new Intl.DateTimeFormat('zh-CN', {
      timeZone: 'Asia/Shanghai',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).format(date).replace(/\\//g, '-');
  };

  async function api(path, opts = {}) {
    const token = localStorage.getItem('cpa_token') || '';
    const headers = { 'Content-Type': 'application/json', ...(token ? { Authorization: 'Bearer ' + token } : {}), ...opts.headers };
    const fetchOpts = { ...opts, headers };
    const resp = await fetch('/api' + path, fetchOpts);
    if (resp.status === 401) { localStorage.removeItem('cpa_token'); window.location.href = '/login'; return null; }
    return resp.json();
  }
  async function logout() {
    await api('/auth/logout', { method: 'POST' });
    localStorage.removeItem('cpa_token');
    window.location.href = '/login';
  }
  window.addEventListener('DOMContentLoaded', async () => {
    const me = await api('/auth/me');
    if (me?.user) {
      const userNameEl = document.getElementById('userName');
      const userAvatarEl = document.getElementById('userAvatar');
      if (userNameEl) userNameEl.textContent = me.user.username;
      if (userAvatarEl) userAvatarEl.textContent = (me.user.username||'A')[0].toUpperCase();
    }
  });
  </script>
  <div class="content">
    ${content}
  </div>
</div>
</body>
</html>`;
}

export function loginPage(): string {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>登录 - cpa-cron-web</title>
<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
<style>
:root { --bg: #0f1117; --bg-card: #1a1d27; --border: #2a2e3d; --text: #e4e6eb; --text-dim: #8b8fa3; --primary: #6c5ce7; --primary-hover: #7c6df7; --danger: #e74c3c; --radius: 8px; }
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; display: flex; align-items: center; justify-content: center; }
.login-card { background: var(--bg-card); border: 1px solid var(--border); border-radius: 16px; padding: 48px 40px; width: 100%; max-width: 420px; }
.login-card h1 { font-size: 24px; text-align: center; margin-bottom: 8px; color: var(--primary); }
.login-card p { text-align: center; color: var(--text-dim); font-size: 14px; margin-bottom: 32px; }
.form-group { margin-bottom: 20px; }
.form-group label { display: block; font-size: 13px; color: var(--text-dim); margin-bottom: 6px; }
.form-group input { width: 100%; background: var(--bg); border: 1px solid var(--border); border-radius: var(--radius); padding: 12px 14px; color: var(--text); font-size: 14px; outline: none; transition: border-color .15s; }
.form-group input:focus { border-color: var(--primary); }
.btn-login { width: 100%; padding: 12px; background: var(--primary); color: #fff; border: none; border-radius: var(--radius); font-size: 15px; font-weight: 600; cursor: pointer; transition: background .15s; }
.btn-login:hover { background: var(--primary-hover); }
.btn-login:disabled { opacity: .5; cursor: not-allowed; }
.error-msg { background: rgba(231,76,60,.1); border: 1px solid rgba(231,76,60,.3); color: var(--danger); padding: 10px 14px; border-radius: var(--radius); font-size: 13px; margin-bottom: 16px; display: none; }
.footer { text-align: center; margin-top: 24px; font-size: 12px; color: var(--text-dim); }
</style>
</head>
<body>
<div class="login-card">
  <h1>cpa-cron-web</h1>
  <p>CPA 账号管理系统</p>
  <div class="error-msg" id="errorMsg"></div>
  <form id="loginForm">
    <div class="form-group">
      <label>用户名</label>
      <input type="text" id="username" placeholder="请输入用户名" autocomplete="username" required>
    </div>
    <div class="form-group">
      <label>密码</label>
      <input type="password" id="password" placeholder="请输入密码" autocomplete="current-password" required>
    </div>
    <button type="submit" class="btn-login" id="loginBtn">登 录</button>
  </form>
  <div class="footer">首次部署请通过环境变量设置管理员账号后再登录</div>
</div>
<script>
document.getElementById('loginForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const btn = document.getElementById('loginBtn');
  const errEl = document.getElementById('errorMsg');
  btn.disabled = true;
  btn.textContent = '登录中...';
  errEl.style.display = 'none';
  try {
    const resp = await fetch('/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        username: document.getElementById('username').value,
        password: document.getElementById('password').value,
      }),
    });
    const data = await resp.json();
    if (data.ok) {
      localStorage.setItem('cpa_token', data.token);
      window.location.href = '/';
    } else {
      errEl.textContent = data.error || '登录失败';
      errEl.style.display = 'block';
    }
  } catch (err) {
    errEl.textContent = '网络错误';
    errEl.style.display = 'block';
  }
  btn.disabled = false;
  btn.textContent = '登 录';
});
</script>
</body>
</html>`;
}
