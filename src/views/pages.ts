import { htmlLayout } from './layout';

type AccountsInitialData = {
  rows: Array<Record<string, unknown>>;
  total: number;
};

function formatChinaTimeText(value: unknown): string {
  if (!value) return '-';
  const raw = String(value).trim();
  if (!raw) return '-';
  let date: Date;
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(raw)) {
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
  }).format(date).replace(/\//g, '-');
}

export function dashboardPage(): string {
  return htmlLayout('仪表盘', `
<div id="statsContainer"><div class="spinner" style="margin:40px auto;display:block"></div></div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:24px;margin-top:24px">
  <div class="table-wrapper">
    <div class="table-toolbar"><strong>最近扫描</strong></div>
    <div id="lastScan" style="padding:16px"><span class="text-dim">加载中...</span></div>
  </div>
  <div class="table-wrapper">
    <div class="table-toolbar"><strong>Cron 状态</strong></div>
    <div id="cronStatus" style="padding:16px"><span class="text-dim">加载中...</span></div>
  </div>
</div>
<div style="display:grid;grid-template-columns:1fr;gap:24px;margin-top:24px">
  <div class="table-wrapper">
    <div class="table-toolbar"><strong>最近操作</strong></div>
    <div id="recentActivity" style="padding:0">
      <table><thead><tr><th>操作</th><th>详情</th><th>用户</th><th>时间</th></tr></thead><tbody id="activityBody"></tbody></table>
    </div>
  </div>
</div>
<script>
(async () => {
  const data = await api('/dashboard');
  if (!data) return;
  document.getElementById('statsContainer').innerHTML = \`
    <div class="stats-grid">
      <div class="stat-card"><div class="label">总账号数</div><div class="value">\${data.total_accounts}</div></div>
      <div class="stat-card"><div class="label">有效账号</div><div class="value success">\${data.active_accounts}</div></div>
      <div class="stat-card"><div class="label">已禁用</div><div class="value warning">\${data.disabled_accounts}</div></div>
      <div class="stat-card"><div class="label">401 失效</div><div class="value danger">\${data.invalid_401}</div></div>
      <div class="stat-card"><div class="label">配额耗尽</div><div class="value danger">\${data.quota_limited}</div></div>
      <div class="stat-card"><div class="label">已恢复</div><div class="value info">\${data.recovered}</div></div>
      <div class="stat-card"><div class="label">探测异常</div><div class="value warning">\${data.probe_errors}</div></div>
    </div>
  \`;
  if (data.last_scan) {
    const s = data.last_scan;
    document.getElementById('lastScan').innerHTML = \`
      <p style="font-size:13px;color:var(--text-dim)">
        运行ID: \${s.run_id} | 模式: \${s.mode} | 状态: <span class="badge \${s.status==='success'?'badge-success':'badge-danger'}">\${s.status}</span><br>
        总文件: \${s.total_files} | 过滤: \${s.filtered_files} | 探测: \${s.probed_files}<br>
        401: \${s.invalid_401_count} | 限额: \${s.quota_limited_count} | 恢复: \${s.recovered_count}<br>
        开始: \${window.formatChinaTime(s.started_at)} | 结束: \${window.formatChinaTime(s.finished_at)}
      </p>
    \`;
  } else {
    document.getElementById('lastScan').innerHTML = '<div class="empty"><span class="material-icons">info</span><p>尚无扫描记录</p></div>';
  }

  function cronToHuman(expr) {
    if (!expr) return '未配置';
    const p = expr.trim().split(/\\s+/);
    if (p.length < 5) return expr;
    const [min, hr] = p;
    if (min === '*' && hr === '*') return '每分钟';
    if (min.startsWith('*/')) {
      const n = parseInt(min.slice(2), 10);
      if (n > 0 && hr === '*') return '每 ' + n + ' 分钟';
    }
    if (min.match(/^\\d+$/) && hr === '*') return '每小时第 ' + min + ' 分钟';
    if (min.match(/^\\d+$/) && hr.match(/^\\d+$/)) return '每天 ' + hr.padStart(2,'0') + ':' + min.padStart(2,'0');
    if (min.match(/^\\d+$/) && hr.startsWith('*/')) {
      const n = parseInt(hr.slice(2), 10);
      return '每 ' + n + ' 小时 (第 ' + min + ' 分)';
    }
    return expr;
  }

  function fmtDashboardTime(value) {
    return window.formatChinaTime(value);
  }

  const cron = data.cron || {};
  const cronSummary = data.cron_summary || {};
  const cronExpr = cron.cron_expression || '*/30 * * * *';
  const cronState = cron.cron_last_result || '未运行';
  const cronBadge = cronState === 'success'
    ? 'badge-success'
    : cronState === 'failed'
      ? 'badge-danger'
      : cronState === 'running'
        ? 'badge-info'
        : 'badge-warning';
  document.getElementById('cronStatus').innerHTML = \`
    <div style="display:flex;flex-direction:column;gap:10px;font-size:13px;color:var(--text-dim)">
      <div>
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:6px">
          <span style="font-weight:600;color:var(--text)">执行频率</span>
          <span class="badge badge-info">\${cronToHuman(cronExpr)}</span>
          <code style="opacity:0.5">\${cronExpr}</code>
        </div>
        <div>上次触发: \${fmtDashboardTime(cron.cron_last_run_at)}</div>
        <div>当前结果: <span class="badge \${cronBadge}">\${cronState}</span></div>
      </div>
      <div style="padding:10px 12px;border:1px solid var(--border);border-radius:8px;background:rgba(116,185,255,.05)">
        <div style="font-weight:600;color:var(--text);margin-bottom:6px">最近一次定时执行</div>
        <div>开始于: \${fmtDashboardTime(cronSummary.last_started_at)}</div>
        <div>成功于: \${fmtDashboardTime(cronSummary.last_completed_at)}</div>
        <div>耗时: \${cronSummary.last_duration_seconds != null ? cronSummary.last_duration_seconds + ' 秒' : '-'}</div>
      </div>
      <div>最近失败原因: \${cron.cron_last_error || '-'}</div>
    </div>
  \`;

  const tbody = document.getElementById('activityBody');
  if (data.recent_activity?.length) {
    tbody.innerHTML = data.recent_activity.map(a => \`
      <tr><td><span class="badge badge-info">\${a.action}</span></td><td style="max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">\${a.detail||'-'}</td><td>\${a.username||'-'}</td><td style="white-space:nowrap">\${window.formatChinaTime(a.created_at)}</td></tr>
    \`).join('');
  } else {
    tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--text-dim)">暂无记录</td></tr>';
  }
})();
</script>
`, 'dashboard');
}

export function accountsPage(initialData?: AccountsInitialData): string {
  const initialRows = initialData?.rows || [];
  const initialTotal = initialData?.total || 0;
  const initialTbody = initialRows.length
    ? initialRows.map((r) => `
    <tr>
      <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis">${String(r.email || r.account || '-')}</td>
      <td>${String(r.provider || '-')}</td>
      <td>${Number(r.is_invalid_401 || 0) === 1 ? '<span class="badge badge-danger">401</span>' : Number(r.disabled || 0) === 1 && Number(r.is_quota_limited || 0) === 1 ? '<span class="badge badge-warning">限额已禁用</span>' : Number(r.is_quota_limited || 0) === 1 ? '<span class="badge badge-warning">限额</span>' : Number(r.disabled || 0) === 1 && Number(r.is_recovered || 0) === 1 ? '<span class="badge badge-info">可恢复</span>' : Number(r.is_recovered || 0) === 1 ? '<span class="badge badge-info">恢复</span>' : Number(r.disabled || 0) === 1 ? '<span class="badge badge-dim">禁用</span>' : r.probe_error_kind ? '<span class="badge badge-warning">异常</span>' : '<span class="badge badge-success">有效</span>'}</td>
      <td>${r.api_status_code != null ? String(r.api_status_code) : '-'}</td>
      <td style="white-space:nowrap;font-size:12px">${formatChinaTimeText(r.updated_at)}</td>
      <td>
        <button class="btn btn-sm ${Number(r.disabled || 0) === 1 ? 'btn-primary' : 'btn-outline'}" onclick="toggleAccount('${encodeURIComponent(String(r.name || ''))}',${Number(r.disabled || 0) !== 1})">${Number(r.disabled || 0) === 1 ? '启用' : '禁用'}</button>
        <button class="btn btn-danger btn-sm" onclick="deleteAcc('${encodeURIComponent(String(r.name || ''))}')">删除</button>
      </td>
    </tr>`).join('')
    : '<tr><td colspan="6" style="text-align:center;color:var(--text-dim)">暂无数据</td></tr>';
  return htmlLayout('账号管理', `
<div id="accountsAlert" style="display:none" class="alert alert-info" style="margin-bottom:16px"></div>
<div class="table-wrapper" style="margin-bottom:16px">
  <div style="padding:14px 16px;display:flex;gap:18px;flex-wrap:wrap;align-items:center;font-size:13px;color:var(--text-dim)">
    <span id="accountsFreshness">数据来源时间: 加载中...</span>
    <span id="accountsProbeTime">最近探测: -</span>
    <span id="accountsTaskState">当前任务: 空闲</span>
    <button class="btn btn-primary btn-sm" onclick="quickScan()" id="quickScanBtn"><span class="material-icons" style="font-size:16px">sync</span> 立即扫描</button>
  </div>
</div>
<div class="table-wrapper">
  <div class="table-toolbar">
    <input type="text" id="searchInput" placeholder="搜索账号名/邮箱..." style="width:240px" oninput="debounceSearch()">
    <select id="statusFilter" onchange="loadAccounts()" style="width:150px">
      <option value="">全部状态</option>
      <option value="active">有效</option>
      <option value="disabled">已禁用</option>
      <option value="invalid_401">401失效</option>
      <option value="quota_limited">配额耗尽</option>
      <option value="recovered">已恢复</option>
      <option value="probe_error">探测异常</option>
    </select>
    <select id="sortSelect" onchange="loadAccounts()" style="width:140px">
      <option value="updated_at">更新时间</option>
      <option value="name">名称</option>
      <option value="email">邮箱</option>
      <option value="last_probed_at">探测时间</option>
    </select>
    <button class="btn btn-outline btn-sm" onclick="toggleOrder()">
      <span class="material-icons" style="font-size:16px" id="orderIcon">arrow_downward</span>
    </button>
    <span id="totalCount" style="font-size:13px;color:var(--text-dim);margin-left:auto">共 ${initialTotal} 条</span>
  </div>
  <div id="accountsTable">
    <table>
      <thead><tr>
        <th>邮箱</th><th>Provider</th><th>状态</th><th>API</th><th>更新时间</th><th>操作</th>
      </tr></thead>
      <tbody id="accountsBody">${initialTbody}</tbody>
    </table>
  </div>
  <div class="pagination">
    <button id="prevBtn" onclick="changePage(-1)" disabled>&lt; 上一页</button>
    <span id="pageInfo">1 / ${Math.max(1, Math.ceil(initialTotal / 50))}</span>
    <button id="nextBtn" onclick="changePage(1)" ${initialTotal <= 50 ? 'disabled' : ''}>下一页 &gt;</button>
  </div>
</div>

<script>
let currentPage = 0, pageSize = 50, sortOrder = 'desc', totalRows = ${initialTotal};
let searchTimer;
let accountMetaTimer = null;

function syncPager() {
  document.getElementById('totalCount').textContent = '共 ' + totalRows + ' 条';
  document.getElementById('pageInfo').textContent = (currentPage + 1) + ' / ' + Math.max(1, Math.ceil(totalRows / pageSize));
  document.getElementById('prevBtn').disabled = currentPage === 0;
  document.getElementById('nextBtn').disabled = (currentPage + 1) * pageSize >= totalRows;
}

function fmtTime(value) {
  return window.formatChinaTime(value);
}

async function refreshAccountMeta() {
  const [dash, tasks, meta] = await Promise.all([api('/dashboard'), api('/tasks'), api('/accounts/meta')]);
  if (dash) {
    const lastScan = dash.last_scan;
    const freshness = lastScan ? (lastScan.finished_at || lastScan.started_at || '-') : '-';
    document.getElementById('accountsFreshness').textContent = '数据来源时间: ' + fmtTime(freshness);
  }

  const listReq = await api('/accounts?limit=1&sort=last_probed_at&order=desc');
  if (listReq && listReq.rows && listReq.rows.length > 0) {
    const row = listReq.rows[0];
    document.getElementById('accountsProbeTime').textContent = '最近探测: ' + fmtTime(row.last_probed_at || row.updated_at || row.last_seen_at);
  } else {
    document.getElementById('accountsProbeTime').textContent = '最近探测: -';
  }

  if (tasks && Array.isArray(tasks) && tasks.length > 0) {
    const active = tasks.find(t => t.status === 'running' || t.status === 'pending');
    if (active) {
      const total = Number(active.total || 0);
      const progress = Number(active.progress || 0);
      const percent = total > 0 ? Math.min(100, Math.round(progress / total * 100)) : 0;
      document.getElementById('accountsTaskState').textContent = '当前任务: ' + active.type + ' / ' + active.status + (total ? (' / ' + progress + '/' + total + ' (' + percent + '%)') : '');
    } else {
      const latest = tasks[0];
      document.getElementById('accountsTaskState').textContent = latest ? ('当前任务: 最近 ' + latest.type + ' / ' + latest.status) : '当前任务: 空闲';
    }
  } else {
    document.getElementById('accountsTaskState').textContent = '当前任务: 空闲';
  }

  if (meta) {
    if (meta.cache_matches_current) {
      hideAlert();
      if (meta.cache_last_success_at) {
        document.getElementById('accountsFreshness').textContent = '数据来源时间: ' + fmtTime(meta.cache_last_success_at);
      }
    } else if (meta.current_base_url && meta.cache_base_url) {
      showAlert(
        '当前配置站点已切换，但账号列表仍是旧快照。当前站点: ' + meta.current_base_url + '；缓存站点: ' + meta.cache_base_url + '。请点击“立即扫描”刷新当前站点数据。',
        'warning'
      );
    } else if (meta.current_base_url && !meta.cache_base_url) {
      showAlert('当前站点还没有任何本地快照，请点击“立即扫描”首次同步账号。', 'info');
    }

    if (meta.cache_last_status === 'failed' && meta.cache_last_error) {
      showAlert('最近一次扫描失败，但旧快照仍可查看。失败原因: ' + meta.cache_last_error, 'warning');
    }
  }

  if (accountMetaTimer) clearTimeout(accountMetaTimer);
  accountMetaTimer = setTimeout(refreshAccountMeta, 5000);
}

function debounceSearch() { clearTimeout(searchTimer); searchTimer = setTimeout(()=>{ currentPage=0; loadAccounts(); }, 300); }
function toggleOrder() {
  sortOrder = sortOrder === 'desc' ? 'asc' : 'desc';
  document.getElementById('orderIcon').textContent = sortOrder === 'desc' ? 'arrow_downward' : 'arrow_upward';
  loadAccounts();
}
function changePage(dir) { currentPage = Math.max(0, currentPage + dir); loadAccounts(); }
function statusBadge(row) {
  if (row.is_invalid_401) return '<span class="badge badge-danger">401</span>';
  if (row.disabled && row.is_quota_limited) return '<span class="badge badge-warning">限额已禁用</span>';
  if (row.is_quota_limited) return '<span class="badge badge-warning">限额</span>';
  if (row.disabled && row.is_recovered) return '<span class="badge badge-info">可恢复</span>';
  if (row.is_recovered) return '<span class="badge badge-info">恢复</span>';
  if (row.disabled) return '<span class="badge badge-dim">禁用</span>';
  if (row.probe_error_kind) return '<span class="badge badge-warning">异常</span>';
  return '<span class="badge badge-success">有效</span>';
}
async function loadAccounts() {
  const filter = document.getElementById('searchInput').value;
  const status = document.getElementById('statusFilter').value;
  const sort = document.getElementById('sortSelect').value;
  const data = await api(\`/accounts?limit=\${pageSize}&offset=\${currentPage*pageSize}&filter=\${encodeURIComponent(filter)}&status=\${status}&sort=\${sort}&order=\${sortOrder}\`);
  if (!data) return;
  totalRows = data.total;
  syncPager();
  const tbody = document.getElementById('accountsBody');
  if (!data.rows.length) {
    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-dim)">暂无数据</td></tr>';
    return;
  }
  tbody.innerHTML = data.rows.map(r => \`
    <tr>
      <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis">\${r.email||r.account||'-'}</td>
      <td>\${r.provider||'-'}</td>
      <td>\${statusBadge(r)}</td>
      <td>\${r.api_status_code!=null?r.api_status_code:'-'}</td>
      <td style="white-space:nowrap;font-size:12px">\${window.formatChinaTime(r.updated_at)}</td>
      <td>
        <button class="btn btn-sm \${r.disabled?'btn-primary':'btn-outline'}" onclick="toggleAccount('\${encodeURIComponent(r.name)}',\${!r.disabled})">\${r.disabled?'启用':'禁用'}</button>
        <button class="btn btn-danger btn-sm" onclick="deleteAcc('\${encodeURIComponent(r.name)}')">删除</button>
      </td>
    </tr>
  \`).join('');
}
async function toggleAccount(name, disabled) {
  if (!confirm(disabled ? '确认禁用此账号？' : '确认启用此账号？')) return;
  await api('/accounts/' + name + '/toggle', { method:'POST', body: JSON.stringify({disabled}) });
  loadAccounts();
}
async function deleteAcc(name) {
  if (!confirm('确认删除此账号？此操作不可撤销！')) return;
  await api('/accounts/' + name, { method:'DELETE' });
  loadAccounts();
}

let scanPollTimer = null;
function showAlert(msg, type) {
  const el = document.getElementById('accountsAlert');
  el.className = 'alert alert-' + (type || 'info');
  el.style.display = 'block';
  el.style.marginBottom = '16px';
  el.innerHTML = msg;
}
function hideAlert() {
  document.getElementById('accountsAlert').style.display = 'none';
}

async function pollScanTask(taskId) {
  if (scanPollTimer) clearTimeout(scanPollTimer);
  const task = await api('/tasks/' + taskId);
  if (!task) return;

  const progress = Number(task.progress || 0);
  const total = Number(task.total || 0);
  const percent = total > 0 ? Math.min(100, Math.round(progress / total * 100)) : 0;

  if (task.status === 'completed') {
    let resultData = null;
    try { resultData = task.result ? JSON.parse(task.result) : null; } catch {}
    const summary = resultData ? (
      '401: ' + (resultData.invalid_401_count || 0) +
      ' | 限额: ' + (resultData.quota_limited_count || 0) +
      ' | 恢复: ' + (resultData.recovered_count || 0) +
      (resultData.actions ? ' | 已删除: ' + (resultData.actions.deleted_401 || 0) : '')
    ) : '';

    showAlert(
      '<div style="display:flex;align-items:center;gap:12px">' +
        '<span class="material-icons" style="font-size:28px;color:var(--success)">task_alt</span>' +
        '<div style="flex:1">' +
          '<div style="font-size:14px;font-weight:600;color:var(--success)">扫描完成</div>' +
          (summary ? '<div style="margin-top:4px;font-size:12px;color:var(--text-dim)">' + summary + '</div>' : '') +
        '</div>' +
      '</div>',
      'success'
    );
    if (window.showToast) window.showToast('扫描完成 — 列表已刷新', 'success');
    document.getElementById('quickScanBtn').disabled = false;
    document.getElementById('quickScanBtn').innerHTML = '<span class="material-icons" style="font-size:16px">sync</span> 立即扫描';
    currentPage = 0;
    syncPager();
    await loadAccounts();
    await refreshAccountMeta();
    setTimeout(hideAlert, 8000);
    return;
  }
  if (task.status === 'failed') {
    showAlert(
      '<div style="display:flex;align-items:center;gap:12px">' +
        '<span class="material-icons" style="font-size:28px;color:var(--danger)">cancel</span>' +
        '<div style="flex:1">' +
          '<div style="font-size:14px;font-weight:600;color:var(--danger)">扫描失败</div>' +
          '<div style="margin-top:4px;font-size:12px">' + (task.error || '未知错误') + '</div>' +
        '</div>' +
      '</div>',
      'danger'
    );
    if (window.showToast) window.showToast('扫描失败: ' + (task.error || '未知错误'), 'danger', 6000);
    document.getElementById('quickScanBtn').disabled = false;
    document.getElementById('quickScanBtn').innerHTML = '<span class="material-icons" style="font-size:16px">sync</span> 立即扫描';
    return;
  }

  // Still running
  let payload = null;
  try { payload = task.result ? JSON.parse(task.result) : null; } catch {}
  const phase = payload && payload.phase ? payload.phase : '';
  const phaseMap = { fetching_files: '正在拉取文件列表...', probing: '正在探测账号状态...', scanning: '正在扫描中...', maintaining: '正在执行维护...' };
  const phaseText = phaseMap[phase] || phase || '处理中...';

  // Update scan button to show live status
  document.getElementById('quickScanBtn').innerHTML = '<div class="spinner" style="width:14px;height:14px;border-width:2px"></div> ' + (total ? percent + '%' : phaseText);

  showAlert(
    '<div style="display:flex;align-items:center;gap:12px">' +
    '<div class="spinner" style="width:18px;height:18px;border-width:2px;flex-shrink:0"></div>' +
    '<div style="flex:1">' +
      '<div style="display:flex;justify-content:space-between;align-items:center">' +
        '<span style="font-weight:600">' + phaseText + '</span>' +
        (total ? '<span style="font-size:12px;color:var(--text-dim)">' + progress + ' / ' + total + '</span>' : '') +
      '</div>' +
      '<div style="background:var(--bg);border:1px solid var(--border);border-radius:4px;overflow:hidden;height:8px;margin-top:8px">' +
        '<div class="progress-bar-animated" style="width:' + percent + '%;height:100%;background:linear-gradient(90deg,var(--primary),var(--info));transition:width .3s;border-radius:4px"></div>' +
      '</div>' +
      (total ? '<div style="font-size:11px;color:var(--text-dim);margin-top:4px">已完成 ' + percent + '%</div>' : '') +
    '</div></div>',
    'info'
  );

  // Refresh account list while scanning (live data as batches write)
  loadAccounts();

  scanPollTimer = setTimeout(function() { pollScanTask(taskId); }, 2000);
}

async function quickScan() {
  if (!confirm('确认执行扫描? 将从当前 CPA 站点拉取最新数据并探测。')) return;
  const btn = document.getElementById('quickScanBtn');
  btn.disabled = true;
  btn.innerHTML = '<div class="spinner" style="width:14px;height:14px;border-width:2px"></div> 创建任务中...';
  showAlert(
    '<div style="display:flex;align-items:center;gap:10px">' +
      '<div class="spinner" style="width:16px;height:16px;border-width:2px;flex-shrink:0"></div>' +
      '<span>正在创建扫描任务...</span>' +
    '</div>',
    'info'
  );
  try {
    const data = await api('/operations/scan', { method: 'POST' });
    if (!data || !data.ok || !data.task_id) {
      showAlert(
        '<div style="display:flex;align-items:center;gap:10px">' +
          '<span class="material-icons" style="color:var(--danger)">error</span>' +
          '<span>创建失败: ' + (data ? data.error || '未知错误' : '网络错误') + '</span>' +
        '</div>',
        'danger'
      );
      if (window.showToast) window.showToast('扫描任务创建失败', 'danger');
      btn.disabled = false;
      btn.innerHTML = '<span class="material-icons" style="font-size:16px">sync</span> 立即扫描';
      return;
    }
    if (window.showToast) window.showToast('扫描任务已创建，开始执行...', 'info', 3000);
    await pollScanTask(data.task_id);
  } catch (e) {
    showAlert(
      '<div style="display:flex;align-items:center;gap:10px">' +
        '<span class="material-icons" style="color:var(--danger)">error</span>' +
        '<span>请求失败: ' + e.message + '</span>' +
      '</div>',
      'danger'
    );
    if (window.showToast) window.showToast('网络请求失败', 'danger');
    btn.disabled = false;
    btn.innerHTML = '<span class="material-icons" style="font-size:16px">sync</span> 立即扫描';
  }
}

syncPager();
loadAccounts();
refreshAccountMeta();
</script>
`, 'accounts');
}

export function operationsPage(): string {
  return htmlLayout('运维操作', `
<div class="stats-grid" style="grid-template-columns:repeat(3,1fr)">
  <div class="stat-card" id="card-scan" style="cursor:pointer" onclick="runOperation('scan')">
    <div style="display:flex;align-items:center;gap:12px">
      <span class="material-icons" id="icon-scan" style="font-size:36px;color:var(--info)">search</span>
      <div style="flex:1">
        <div class="label" id="label-scan">同步最新账号</div>
        <div style="font-size:13px;margin-top:4px;color:var(--text-dim)" id="desc-scan">从当前站点拉取最新账号并更新状态</div>
        <div id="progress-scan" style="display:none;margin-top:8px">
          <div style="background:var(--bg);border:1px solid var(--border);border-radius:4px;overflow:hidden;height:6px">
            <div id="bar-scan" class="progress-bar-animated" style="width:0%;height:100%;background:linear-gradient(90deg,var(--info),var(--primary));transition:width .3s;border-radius:4px"></div>
          </div>
          <div id="progress-text-scan" style="font-size:11px;color:var(--text-dim);margin-top:4px"></div>
        </div>
      </div>
    </div>
  </div>
  <div class="stat-card" id="card-maintain" style="cursor:pointer" onclick="runOperation('maintain')">
    <div style="display:flex;align-items:center;gap:12px">
      <span class="material-icons" id="icon-maintain" style="font-size:36px;color:var(--warning)">build</span>
      <div style="flex:1">
        <div class="label" id="label-maintain">清理失效账号</div>
        <div style="font-size:13px;margin-top:4px;color:var(--text-dim)" id="desc-maintain">删除 401、处理限额、恢复正常账号</div>
        <div id="progress-maintain" style="display:none;margin-top:8px">
          <div style="background:var(--bg);border:1px solid var(--border);border-radius:4px;overflow:hidden;height:6px">
            <div id="bar-maintain" class="progress-bar-animated" style="width:0%;height:100%;background:linear-gradient(90deg,var(--warning),var(--danger));transition:width .3s;border-radius:4px"></div>
          </div>
          <div id="progress-text-maintain" style="font-size:11px;color:var(--text-dim);margin-top:4px"></div>
        </div>
      </div>
    </div>
  </div>
  <div class="stat-card" style="cursor:pointer" onclick="document.getElementById('uploadSection').style.display='block'">
    <div style="display:flex;align-items:center;gap:12px">
      <span class="material-icons" style="font-size:36px;color:var(--success)">cloud_upload</span>
      <div>
        <div class="label">上传账号文件</div>
        <div style="font-size:13px;margin-top:4px;color:var(--text-dim)">把你手里的 json 账号直接上传到当前站点</div>
      </div>
    </div>
  </div>
</div>

<div id="uploadSection" style="display:none;margin-bottom:24px" class="table-wrapper">
  <div class="table-toolbar"><strong>上传账号文件</strong></div>
  <div style="padding:20px">
    <div style="font-size:13px;color:var(--text-dim);margin-bottom:12px">这里上传的是你已经准备好的账号 json 文件，上传成功后会直接进入当前 CPA 站点账号池。</div>
    <input type="file" id="uploadFiles" multiple accept=".json" style="margin-bottom:12px">
    <div style="display:flex;gap:8px">
      <button class="btn btn-primary" onclick="doUpload()"><span class="material-icons" style="font-size:16px">cloud_upload</span> 开始上传</button>
      <button class="btn btn-outline" onclick="document.getElementById('uploadSection').style.display='none'">取消</button>
    </div>
  </div>
</div>

<div id="resultArea" style="display:none" class="table-wrapper">
  <div class="table-toolbar"><strong>执行结果</strong> <button class="btn btn-outline btn-sm" onclick="document.getElementById('resultArea').style.display='none'" style="margin-left:auto">关闭</button></div>
  <div id="resultContent" style="padding:20px"></div>
</div>

<script>
let taskPollTimer = null;
let activeMode = null;

const modeLabels = {
  scan: { label: '同步', icon: 'search', color: '--info', defaultLabel: '同步最新账号', defaultDesc: '从当前站点拉取最新账号并更新状态' },
  maintain: { label: '清理', icon: 'build', color: '--warning', defaultLabel: '清理失效账号', defaultDesc: '删除 401、处理限额、恢复正常账号' },
};

function phaseLabel(phase) {
  const map = {
    fetching_files: '正在拉取认证文件列表...',
    probing: '正在分批探测账号...',
    scanning: '正在扫描...',
    maintaining: '正在执行维护动作...',
    uploading: '正在分批上传文件...',
  };
  return map[phase] || phase || '处理中...';
}

function setCardState(mode, state, detail) {
  const card = document.getElementById('card-' + mode);
  const icon = document.getElementById('icon-' + mode);
  const label = document.getElementById('label-' + mode);
  const desc = document.getElementById('desc-' + mode);
  const progressWrap = document.getElementById('progress-' + mode);
  const bar = document.getElementById('bar-' + mode);
  const progressText = document.getElementById('progress-text-' + mode);
  const ml = modeLabels[mode];
  if (!card || !ml) return;

  if (state === 'idle') {
    card.style.pointerEvents = '';
    card.style.opacity = '';
    card.style.borderColor = '';
    icon.textContent = ml.icon;
    icon.className = 'material-icons';
    label.textContent = ml.defaultLabel;
    desc.textContent = ml.defaultDesc;
    if (progressWrap) progressWrap.style.display = 'none';
  } else if (state === 'running') {
    card.style.pointerEvents = 'none';
    card.style.opacity = '1';
    card.style.borderColor = 'var(' + ml.color + ')';
    icon.innerHTML = '<div class="spinner" style="width:32px;height:32px;border-width:3px;border-top-color:var(' + ml.color + ')"></div>';
    label.textContent = ml.label + '中...';
    desc.textContent = detail.phaseText || '处理中...';
    if (progressWrap && detail.total > 0) {
      progressWrap.style.display = 'block';
      bar.style.width = detail.percent + '%';
      progressText.textContent = detail.progress + ' / ' + detail.total + ' (' + detail.percent + '%)';
    } else if (progressWrap) {
      progressWrap.style.display = 'block';
      bar.style.width = '100%';
      bar.classList.add('pulse');
      progressText.textContent = detail.phaseText || '处理中...';
    }
  } else if (state === 'success') {
    card.style.pointerEvents = '';
    card.style.opacity = '';
    card.style.borderColor = 'var(--success)';
    icon.className = 'material-icons';
    icon.innerHTML = '';
    icon.textContent = 'check_circle';
    icon.style.color = 'var(--success)';
    label.textContent = ml.label + '完成';
    desc.textContent = detail || '';
    if (progressWrap) { progressWrap.style.display = 'none'; bar.classList.remove('pulse'); }
    setTimeout(function() {
      setCardState(mode, 'idle');
      icon.style.color = 'var(' + ml.color + ')';
    }, 8000);
  } else if (state === 'failed') {
    card.style.pointerEvents = '';
    card.style.opacity = '';
    card.style.borderColor = 'var(--danger)';
    icon.className = 'material-icons';
    icon.innerHTML = '';
    icon.textContent = 'error';
    icon.style.color = 'var(--danger)';
    label.textContent = ml.label + '失败';
    desc.textContent = detail || '未知错误';
    if (progressWrap) { progressWrap.style.display = 'none'; bar.classList.remove('pulse'); }
    setTimeout(function() {
      setCardState(mode, 'idle');
      icon.style.color = 'var(' + ml.color + ')';
    }, 10000);
  }
}

function renderEngineResult(data) {
  let html = '<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">' +
    '<span class="material-icons" style="font-size:32px;color:var(--success)">task_alt</span>' +
    '<div><div style="font-size:16px;font-weight:600;color:var(--success)">任务完成</div></div></div>';
  html += '<div class="stats-grid" style="grid-template-columns:repeat(4,1fr)">';
  html += statCard('总文件', data.total_files);
  html += statCard('过滤后', data.filtered_count);
  html += statCard('401', data.invalid_401_count, 'danger');
  html += statCard('限额', data.quota_limited_count, 'warning');
  html += statCard('恢复', data.recovered_count, 'info');
  html += statCard('失败', data.failure_count, 'danger');
  if (data.actions) {
    html += statCard('删除401账号', data.actions.deleted_401, 'danger');
    html += statCard('禁用限额账号', data.actions.disabled_quota, 'warning');
    html += statCard('删除限额账号', data.actions.deleted_quota, 'danger');
    html += statCard('恢复正常账号', data.actions.reenabled, 'success');
  }
  if (data.upload) {
    html += statCard('上传成功', data.upload.uploaded, 'success');
    html += statCard('跳过', data.upload.skipped, 'dim');
    html += statCard('上传失败', data.upload.failed, 'danger');
  }
  html += '</div>';
  return html;
}

function buildResultSummaryText(data) {
  const parts = [];
  if (data.invalid_401_count) parts.push('401: ' + data.invalid_401_count);
  if (data.quota_limited_count) parts.push('限额: ' + data.quota_limited_count);
  if (data.recovered_count) parts.push('恢复: ' + data.recovered_count);
  if (data.actions) {
    if (data.actions.deleted_401) parts.push('删除401: ' + data.actions.deleted_401);
    if (data.actions.disabled_quota) parts.push('禁用限额: ' + data.actions.disabled_quota);
    if (data.actions.reenabled) parts.push('恢复: ' + data.actions.reenabled);
  }
  return parts.join(' | ') || '执行完成';
}

async function pollTask(taskId) {
  if (taskPollTimer) clearTimeout(taskPollTimer);
  const task = await api('/tasks/' + taskId);
  if (!task) return;

  let payload = null;
  try { payload = task.result ? JSON.parse(task.result) : null; } catch { payload = null; }
  const total = Number(task.total || 0);
  const progress = Number(task.progress || 0);
  const percent = total > 0 ? Math.min(100, Math.round((progress / total) * 100)) : 0;

  if (task.status === 'completed') {
    const summaryText = payload ? buildResultSummaryText(payload) : '执行完成';
    setCardState(activeMode, 'success', summaryText);
    showResult(payload ? renderEngineResult(payload) : '<div class="alert alert-success">任务完成</div>');
    if (window.showToast) window.showToast((modeLabels[activeMode]?.label || '任务') + '完成 — ' + summaryText, 'success', 5000);
    activeMode = null;
    return;
  }
  if (task.status === 'failed') {
    const errMsg = task.error || '未知错误';
    setCardState(activeMode, 'failed', errMsg);
    let html = '<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">' +
      '<span class="material-icons" style="font-size:32px;color:var(--danger)">cancel</span>' +
      '<div><div style="font-size:16px;font-weight:600;color:var(--danger)">任务失败</div>' +
      '<div style="font-size:13px;margin-top:4px;color:var(--text-dim)">' + errMsg + '</div></div></div>';
    if (payload && payload.success === false && payload.error) {
      html += '<div style="margin-top:8px;color:var(--text-dim)">' + payload.error + '</div>';
    }
    showResult(html);
    if (window.showToast) window.showToast((modeLabels[activeMode]?.label || '任务') + '失败: ' + errMsg, 'danger', 6000);
    activeMode = null;
    return;
  }

  const phase = payload && payload.phase ? payload.phase : '处理中';
  const phaseText = phaseLabel(phase);

  setCardState(activeMode, 'running', { phaseText: phaseText, total: total, progress: progress, percent: percent });

  const html =
    '<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">' +
      '<div class="spinner" style="width:24px;height:24px;border-width:3px;flex-shrink:0"></div>' +
      '<div><div style="font-size:15px;font-weight:600">' + phaseText + '</div>' +
      '<div style="font-size:12px;color:var(--text-dim);margin-top:2px">任务ID: ' + task.id + '</div></div>' +
    '</div>' +
    '<div style="background:var(--bg);border:1px solid var(--border);border-radius:8px;overflow:hidden;height:20px;margin-bottom:10px">' +
      '<div class="progress-bar-animated" style="width:' + percent + '%;height:100%;background:linear-gradient(90deg,var(--primary),var(--info));transition:width .3s"></div>' +
    '</div>' +
    '<div style="display:flex;justify-content:space-between;font-size:13px;color:var(--text-dim)">' +
      '<span>进度: ' + progress + ' / ' + (total || '-') + '</span>' +
      (total ? '<span style="font-weight:600;color:var(--text)">' + percent + '%</span>' : '') +
    '</div>';
  showResult(html);
  taskPollTimer = setTimeout(() => pollTask(taskId), 2000);
}

async function runOperation(mode) {
  const modeName = modeLabels[mode]?.label || mode;
  if (!confirm('确认执行「' + modeName + '」操作？')) return;
  activeMode = mode;
  setCardState(mode, 'running', { phaseText: '正在创建任务...', total: 0, progress: 0, percent: 0 });
  showResult(
    '<div style="display:flex;align-items:center;gap:12px">' +
      '<div class="spinner" style="width:20px;height:20px;border-width:2px"></div>' +
      '<span>正在创建' + modeName + '任务...</span>' +
    '</div>'
  );
  try {
    const data = await api('/operations/' + mode, { method: 'POST' });
    if (!data) { setCardState(mode, 'failed', '请求无响应'); activeMode = null; return; }
    if (!data.ok || !data.task_id) {
      setCardState(mode, 'failed', data.error || '创建失败');
      showResult('<div class="alert alert-danger">' + (data.error || '任务创建失败') + (data.details ? '<br>' + data.details.join('<br>') : '') + '</div>');
      if (window.showToast) window.showToast(modeName + '任务创建失败', 'danger');
      activeMode = null;
      return;
    }
    if (window.showToast) window.showToast(modeName + '任务已创建，正在执行...', 'info', 3000);
    await pollTask(data.task_id);
  } catch (e) {
    setCardState(mode, 'failed', e.message);
    showResult('<div class="alert alert-danger">请求失败: ' + e.message + '</div>');
    if (window.showToast) window.showToast('网络请求失败', 'danger');
    activeMode = null;
  }
}

function statCard(label, value, color) {
  return '<div class="stat-card"><div class="label">' + label + '</div><div class="value ' + (color||'') + '">' + (value??0) + '</div></div>';
}

async function doUpload() {
  const input = document.getElementById('uploadFiles');
  if (!input.files.length) { alert('请选择文件'); return; }
  showResult('<div class="spinner"></div> 正在创建上传任务...');
  const formData = new FormData();
  for (const file of input.files) formData.append('files', file);
  try {
    const token = localStorage.getItem('cpa_token') || '';
    const resp = await fetch('/api/operations/upload', { method:'POST', body: formData, headers: token ? { Authorization: 'Bearer ' + token } : {} });
    if (resp.status === 401) { localStorage.removeItem('cpa_token'); window.location.href = '/login'; return; }
    const data = await resp.json();
    if (!data.ok || !data.task_id) {
      showResult('<div class="alert alert-danger">' + (data.error || '上传任务创建失败') + '</div>');
      return;
    }
    await pollTask(data.task_id);
    document.getElementById('uploadSection').style.display = 'none';
  } catch (e) {
    showResult('<div class="alert alert-danger">上传失败: ' + e.message + '</div>');
  }
}

function showResult(html) {
  const area = document.getElementById('resultArea');
  area.style.display = 'block';
  document.getElementById('resultContent').innerHTML = html;
}
</script>
`, 'operations');
}

export function historyPage(): string {
  return htmlLayout('扫描历史', `
<div class="table-wrapper">
  <div class="table-toolbar"><strong>扫描运行记录</strong></div>
  <table>
    <thead><tr><th>ID</th><th>模式</th><th>状态</th><th>总文件</th><th>过滤</th><th>探测</th><th>401</th><th>限额</th><th>恢复</th><th>开始时间</th><th>结束时间</th></tr></thead>
    <tbody id="historyBody"><tr><td colspan="11" style="text-align:center"><div class="spinner"></div></td></tr></tbody>
  </table>
  <div class="pagination">
    <button id="prevBtn" onclick="changePage(-1)" disabled>&lt; 上一页</button>
    <span id="pageInfo">1</span>
    <button id="nextBtn" onclick="changePage(1)">下一页 &gt;</button>
  </div>
</div>
<script>
let currentPage = 0, pageSize = 20, total = 0;
function changePage(dir) { currentPage = Math.max(0, currentPage + dir); load(); }
async function load() {
  const data = await api(\`/scan-runs?limit=\${pageSize}&offset=\${currentPage*pageSize}\`);
  if (!data) return;
  total = data.total;
  document.getElementById('pageInfo').textContent = \`\${currentPage+1} / \${Math.max(1,Math.ceil(total/pageSize))}\`;
  document.getElementById('prevBtn').disabled = currentPage === 0;
  document.getElementById('nextBtn').disabled = (currentPage+1)*pageSize >= total;
  const tbody = document.getElementById('historyBody');
  if (!data.rows.length) { tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:var(--text-dim)">暂无记录</td></tr>'; return; }
  tbody.innerHTML = data.rows.map(r => \`
    <tr>
      <td>\${r.run_id}</td><td>\${r.mode}</td>
      <td><span class="badge \${r.status==='success'?'badge-success':'badge-danger'}">\${r.status}</span></td>
      <td>\${r.total_files}</td><td>\${r.filtered_files}</td><td>\${r.probed_files}</td>
      <td>\${r.invalid_401_count}</td><td>\${r.quota_limited_count}</td><td>\${r.recovered_count}</td>
      <td style="white-space:nowrap;font-size:12px">\${window.formatChinaTime(r.started_at)}</td>
      <td style="white-space:nowrap;font-size:12px">\${window.formatChinaTime(r.finished_at)}</td>
    </tr>
  \`).join('');
}
load();
</script>
`, 'history');
}

export function activityPage(): string {
  return htmlLayout('操作日志', `
<div class="table-wrapper">
  <div class="table-toolbar"><strong>操作日志</strong></div>
  <table>
    <thead><tr><th>ID</th><th>操作</th><th>详情</th><th>用户</th><th>时间</th></tr></thead>
    <tbody id="logBody"><tr><td colspan="5" style="text-align:center"><div class="spinner"></div></td></tr></tbody>
  </table>
  <div class="pagination">
    <button id="prevBtn" onclick="changePage(-1)" disabled>&lt; 上一页</button>
    <span id="pageInfo">1</span>
    <button id="nextBtn" onclick="changePage(1)">下一页 &gt;</button>
  </div>
</div>
<script>
let currentPage = 0, pageSize = 50, total = 0;
function changePage(dir) { currentPage = Math.max(0, currentPage + dir); load(); }
async function load() {
  const data = await api(\`/activity?limit=\${pageSize}&offset=\${currentPage*pageSize}\`);
  if (!data) return;
  total = data.total;
  document.getElementById('pageInfo').textContent = \`\${currentPage+1} / \${Math.max(1,Math.ceil(total/pageSize))}\`;
  document.getElementById('prevBtn').disabled = currentPage === 0;
  document.getElementById('nextBtn').disabled = (currentPage+1)*pageSize >= total;
  const tbody = document.getElementById('logBody');
  if (!data.rows.length) { tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--text-dim)">暂无记录</td></tr>'; return; }
  tbody.innerHTML = data.rows.map(r => \`
    <tr>
      <td>\${r.id}</td>
      <td><span class="badge badge-info">\${r.action}</span></td>
      <td style="max-width:400px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">\${r.detail||'-'}</td>
      <td>\${r.username||'-'}</td>
      <td style="white-space:nowrap;font-size:12px">\${window.formatChinaTime(r.created_at)}</td>
    </tr>
  \`).join('');
}
load();
</script>
`, 'activity');
}

export function settingsPage(): string {
  return htmlLayout('系统配置', `
<div class="table-wrapper" style="margin-bottom:16px">
  <div class="table-toolbar"><strong>已保存配置</strong></div>
  <div style="padding:16px 20px;display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;font-size:13px">
    <div><span style="color:var(--text-dim)">当前 Base URL:</span><div id="saved_base_url" style="margin-top:4px;word-break:break-all">加载中...</div></div>
    <div><span style="color:var(--text-dim)">当前 Token:</span><div id="saved_token" style="margin-top:4px;word-break:break-all">加载中...</div></div>
    <div><span style="color:var(--text-dim)">当前 Target Type:</span><div id="saved_target_type" style="margin-top:4px">加载中...</div></div>
    <div><span style="color:var(--text-dim)">当前 Provider:</span><div id="saved_provider" style="margin-top:4px">加载中...</div></div>
  </div>
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:24px">
  <div class="table-wrapper">
    <div class="table-toolbar"><strong>CPA 连接配置</strong></div>
    <div style="padding:20px">
      <div class="form-group">
        <label>Base URL</label>
        <input type="url" id="cfg_base_url" style="width:100%" placeholder="https://your-cpa.example.com">
        <div id="base_url_display" style="font-size:12px;color:var(--text-dim);margin-top:4px"></div>
      </div>
      <div class="form-group">
        <label>Token</label>
        <div style="display:flex;gap:8px">
          <input type="password" id="cfg_token" style="flex:1" placeholder="输入新Token（留空则不修改）">
          <button class="btn btn-outline btn-sm" onclick="toggleTokenVisibility()" type="button"><span class="material-icons" style="font-size:16px">visibility</span></button>
        </div>
        <div id="token_display" style="font-size:12px;color:var(--text-dim);margin-top:4px"></div>
      </div>
      <div class="form-group"><label>Target Type</label><input type="text" id="cfg_target_type" style="width:100%" value="codex"></div>
      <div class="form-group"><label>Provider (可选)</label><input type="text" id="cfg_provider" style="width:100%"></div>
      <div class="form-group"><label>User Agent</label><input type="text" id="cfg_user_agent" style="width:100%"></div>
      <div style="margin-top:12px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
        <button class="btn btn-outline" onclick="testConnection()" id="testBtn"><span class="material-icons" style="font-size:16px">cable</span> 测试连接</button>
        <button class="btn btn-primary" onclick="saveSettings()" id="saveBtn"><span class="material-icons" style="font-size:16px">save</span> 保存配置</button>
        <span id="testResult" style="font-size:13px"></span>
        <span id="saveResult" style="font-size:13px"></span>
      </div>
    </div>
  </div>
  <div class="table-wrapper">
    <div class="table-toolbar"><strong>探测 & 维护参数</strong></div>
    <div style="padding:20px">
      <div style="margin-bottom:14px;font-size:12px;color:var(--text-dim)">并发参数会参与实际执行，但 Worker 端会应用安全上限以避免把 CPA 接口或 Cloudflare 运行时打满。当前上限：探测 12，维护操作 10。</div>
      <div class="form-row">
        <div class="form-group"><label>探测并发</label><input type="number" id="cfg_probe_workers" style="width:100%" min="1"></div>
        <div class="form-group"><label>操作并发</label><input type="number" id="cfg_action_workers" style="width:100%" min="1"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label>超时(秒)</label><input type="number" id="cfg_timeout" style="width:100%" min="1"></div>
        <div class="form-group"><label>重试次数</label><input type="number" id="cfg_retries" style="width:100%" min="0"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label>删除重试</label><input type="number" id="cfg_delete_retries" style="width:100%" min="0"></div>
        <div class="form-group">
          <label>限额动作</label>
          <select id="cfg_quota_action" style="width:100%"><option value="disable">禁用</option><option value="delete">删除</option></select>
        </div>
      </div>
      <div class="form-row">
        <div class="form-group"><label>限额阈值 (0~1)</label><input type="number" id="cfg_quota_disable_threshold" style="width:100%" step="0.01" min="0" max="1"></div>
        <div class="form-group">
          <label>恢复范围</label>
          <select id="cfg_reenable_scope" style="width:100%"><option value="signal">signal</option><option value="managed">managed</option></select>
        </div>
      </div>
      <div class="form-row">
        <div class="form-group"><label>删除 401</label><select id="cfg_delete_401" style="width:100%"><option value="true">是</option><option value="false">否</option></select></div>
        <div class="form-group"><label>自动恢复</label><select id="cfg_auto_reenable" style="width:100%"><option value="true">是</option><option value="false">否</option></select></div>
      </div>
    </div>
  </div>
  <div class="table-wrapper">
    <div class="table-toolbar"><strong>上传参数</strong></div>
    <div style="padding:20px">
      <div style="margin-bottom:14px;font-size:12px;color:var(--text-dim)">上传并发同样参与实际执行，但会受到 Worker 端安全上限约束。当前上限：上传 8。</div>
      <div class="form-row">
        <div class="form-group"><label>上传并发</label><input type="number" id="cfg_upload_workers" style="width:100%" min="1"></div>
        <div class="form-group"><label>上传重试</label><input type="number" id="cfg_upload_retries" style="width:100%" min="0"></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label>上传方式</label><select id="cfg_upload_method" style="width:100%"><option value="json">json</option><option value="multipart">multipart</option></select></div>
        <div class="form-group"><label>强制上传</label><select id="cfg_upload_force" style="width:100%"><option value="false">否</option><option value="true">是</option></select></div>
      </div>
      <div class="form-row">
        <div class="form-group"><label>最小有效账号</label><input type="number" id="cfg_min_valid_accounts" style="width:100%" min="0"></div>
        <div class="form-group"><label>补充策略</label><select id="cfg_refill_strategy" style="width:100%"><option value="to-threshold">to-threshold</option><option value="fixed">fixed</option></select></div>
      </div>
    </div>
  </div>
  <div class="table-wrapper">
    <div class="table-toolbar"><strong>修改密码</strong></div>
    <div style="padding:20px">
      <div class="form-group"><label>旧密码</label><input type="password" id="old_password" style="width:100%"></div>
      <div class="form-group"><label>新密码</label><input type="password" id="new_password" style="width:100%"></div>
      <button class="btn btn-outline" onclick="changePassword()">修改密码</button>
      <div id="pwResult" style="margin-top:8px"></div>
    </div>
  </div>
</div>
<script>
const configFields = ['base_url','token','target_type','provider','user_agent','probe_workers','action_workers','timeout','retries','delete_retries','quota_action','quota_disable_threshold','reenable_scope','delete_401','auto_reenable','upload_workers','upload_retries','upload_method','upload_force','min_valid_accounts','refill_strategy'];

function toggleTokenVisibility() {
  const el = document.getElementById('cfg_token');
  el.type = el.type === 'password' ? 'text' : 'password';
}

async function loadSettings() {
  const data = await api('/config');
  if (!data) return;
  for (const key of configFields) {
    const el = document.getElementById('cfg_' + key);
    if (!el) continue;
    if (key === 'token') {
      // Never fill masked token into input; show it as hint below
      el.value = '';
      const display = document.getElementById('token_display');
      if (display) display.textContent = data[key] ? '当前: ' + data[key] : '未设置';
      continue;
    }
    // For <select> and <input>, always set string value
    const strVal = String(data[key] ?? '');
    el.value = strVal;
  }
  // Show current base_url below input
  const urlDisplay = document.getElementById('base_url_display');
  if (urlDisplay) {
    urlDisplay.textContent = data.base_url ? '当前: ' + data.base_url : '未设置';
  }

  // Clear saved-config summary so user can immediately see what's persisted
  const savedBaseUrl = document.getElementById('saved_base_url');
  const savedToken = document.getElementById('saved_token');
  const savedTargetType = document.getElementById('saved_target_type');
  const savedProvider = document.getElementById('saved_provider');
  if (savedBaseUrl) savedBaseUrl.textContent = data.base_url || '未设置';
  if (savedToken) savedToken.textContent = data.token || '未设置';
  if (savedTargetType) savedTargetType.textContent = data.target_type || '未设置';
  if (savedProvider) savedProvider.textContent = data.provider || '未设置';
}

async function saveSettings() {
  const btn = document.getElementById('saveBtn');
  btn.disabled = true;
  const payload = {};
  for (const key of configFields) {
    const el = document.getElementById('cfg_' + key);
    if (!el) continue;
    const val = el.value.trim();
    // Skip empty token — means user didn't want to change it
    if (key === 'token' && !val) continue;
    payload[key] = val;
  }
  const data = await api('/config', { method: 'PUT', body: JSON.stringify(payload) });
  if (data?.ok) {
    document.getElementById('saveResult').innerHTML = '<span style="color:var(--success)">保存成功</span>';
    // Reload to show updated values
    await loadSettings();
  } else {
    document.getElementById('saveResult').innerHTML = '<span style="color:var(--danger)">保存失败</span>';
  }
  btn.disabled = false;
  setTimeout(() => document.getElementById('saveResult').innerHTML = '', 4000);
}

async function testConnection() {
  const btn = document.getElementById('testBtn');
  const result = document.getElementById('testResult');
  btn.disabled = true;
  result.innerHTML = '<span class="spinner" style="width:14px;height:14px;border-width:2px"></span> 测试中...';
  try {
    // Send current input values so user doesn't need to save first
    const payload = {};
    const urlVal = document.getElementById('cfg_base_url').value.trim();
    const tokenVal = document.getElementById('cfg_token').value.trim();
    if (urlVal) payload.base_url = urlVal;
    if (tokenVal) payload.token = tokenVal;
    const data = await api('/config/test', { method: 'POST', body: JSON.stringify(payload) });
    if (data?.ok) {
      result.innerHTML = '<span style="color:var(--success)">' + (data.message || '连接成功') + '</span>';
    } else {
      result.innerHTML = '<span style="color:var(--danger)">' + (data?.error || '连接失败') + '</span>';
    }
  } catch (e) {
    result.innerHTML = '<span style="color:var(--danger)">请求失败: ' + e.message + '</span>';
  }
  btn.disabled = false;
}

async function changePassword() {
  const old_password = document.getElementById('old_password').value;
  const new_password = document.getElementById('new_password').value;
  if (!old_password || !new_password) { alert('请填写完整'); return; }
  const data = await api('/auth/change-password', { method:'POST', body: JSON.stringify({ old_password, new_password }) });
  document.getElementById('pwResult').innerHTML = data?.ok
    ? '<span style="color:var(--success)">密码已修改</span>'
    : '<span style="color:var(--danger)">' + (data?.error || '修改失败') + '</span>';
}

loadSettings();
</script>
`, 'settings');
}
