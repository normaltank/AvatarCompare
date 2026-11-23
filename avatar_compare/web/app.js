'use strict';
const $ = (id) => document.getElementById(id);
const visible = (id, show) => { $(id).hidden = !show; };
let selectedTarget = null, selectedFolder = null, current = {}, resultSignature = '', busy = false;
let initialLoaded = false, pollTimer, historyLoaded = false, quitting = false;
let picking = false;
let lastSyncedJob = '';

async function api(path, data) {
  const options = data === undefined ? {} : {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data)};
  const response = await fetch('/api/' + path, options);
  const value = await response.json();
  if (!response.ok) throw new Error(value.error || '操作失败，请重试');
  return value;
}
function node(tag, text, className) {
  const value = document.createElement(tag);
  if (text !== undefined) value.textContent = text;
  if (className) value.className = className;
  return value;
}
function message(text, error = false) {
  $('notice').textContent = text;
  $('notice').classList.toggle('error', error);
  visible('notice', !!text);
}
function formError(text) { $('form-error').textContent = text; visible('form-error', !!text); }
function count(value) {
  if (value === null || value === undefined) return '—';
  try { return BigInt(value).toLocaleString('zh-CN'); } catch { return String(value); }
}
function duration(seconds) {
  if (seconds == null || !Number.isFinite(seconds)) return '—';
  if (seconds < 60) return Math.ceil(seconds) + ' 秒';
  if (seconds < 3600) return Math.ceil(seconds / 60) + ' 分钟';
  if (seconds < 86400) return (seconds / 3600).toFixed(1) + ' 小时';
  return (seconds / 86400).toFixed(1) + ' 天';
}
function updateRange() {
  try {
    const start = $('start').value.trim(), end = $('end').value.trim();
    if (!/^\d+$/.test(start) || !/^\d+$/.test(end) || BigInt(end) < BigInt(start)) throw new Error();
    const amount = BigInt(end) - BigInt(start) + 1n;
    $('range-count').textContent = `共 ${amount.toLocaleString('zh-CN')} 个编号，包含两端`;
  } catch { $('range-count').textContent = '包含起止编号两端'; }
}
function updateMode() {
  const range = $('mode').value === 'range';
  visible('range-fields', range); visible('range-settings', range); visible('folder-fields', !range);
  $('start').required = $('end').required = range;
  $('start').disabled = $('end').disabled = !range;
  $('range-settings').querySelectorAll('input,select').forEach((input) => { input.disabled = !range; });
  updateSettings();
}
function updateSettings() {
  visible('custom-speed', $('speed').value === 'custom');
  $('custom-speed').querySelectorAll('input,select').forEach((input) => { input.disabled = $('speed').value !== 'custom' || $('mode').value !== 'range'; });
  $('speed-help').textContent = $('speed').value === 'maximum' ? '使用全部逻辑 CPU 和独立计算进程；是否更快取决于下载与图片复杂度。' : '并行下载、计算及写入；重复头像自动复用计算。';
  const compact = $('compact').checked;
  visible('capture-source-option', compact);
  $('retention-help').textContent = compact
    ? $('capture-source').checked
      ? '当前目标保留候选和前 100 项复核记录。图源清单另存已成功读取的原图。'
      : '保留已处理区间、错误和前 100 项复核记录。同批次可复用已保留的图片。'
    : '完整保留逐项记录与原图，方便后续复用。大范围任务会占用更多磁盘。';
}
function applyParams(params) {
  selectedTarget = {id: params.target_id, name: params.target_name};
  $('target-preview').src = '/api/target?id=' + encodeURIComponent(selectedTarget.id);
  visible('target-preview', true); visible('target-empty', false);
  $('target-name').textContent = selectedTarget.name;
  $('mode').value = params.mode;
  const map = {start: 'start', end: 'end', speed: 'speed', workers: 'workers', downloads: 'downloads', backend: 'backend', rate: 'rate', timeout: 'timeout', retries: 'retries', endpoint: 'endpoint', revision: 'revision'};
  for (const [key, id] of Object.entries(map)) if (params[key] != null) $(id).value = params[key];
  $('token-env').value = params.token_env ?? '';
  $('compact').checked = params.retention === 'compact';
  $('capture-source').checked = Boolean(params.capture_source);
  $('full-speed').checked = Number(params.rate || 0) === 0;
  $('refresh').checked = $('retry-errors').checked = false;
  if (params.folder_id) {
    selectedFolder = {id: params.folder_id, name: params.folder};
    $('folder-name').textContent = params.folder;
  }
  updateMode(); updateSettings(); updateRange(); formError('');
}
async function upload(file) {
  if (!file || busy || picking) return;
  if (file.size > 32 * 1024 * 1024) { formError('图片超过 32 MiB，请缩小后重试'); return; }
  picking = true; $('input-fields').disabled = true; $('start-button').disabled = true;
  $('target-name').textContent = '正在读取图片…'; formError('');
  try {
    const response = await fetch('/api/upload?name=' + encodeURIComponent(file.name), {method: 'POST', headers: {'Content-Type': 'application/octet-stream'}, body: file});
    const value = await response.json();
    if (!response.ok) throw new Error(value.error || '无法读取图片');
    selectedTarget = value;
    $('target-preview').src = value.url;
    visible('target-preview', true); visible('target-empty', false);
    $('target-name').textContent = value.name;
  } catch (error) { formError(error.message); $('target-name').textContent = selectedTarget?.name || '请选择有效图片'; }
  finally { picking = false; $('input-fields').disabled = busy; $('start-button').disabled = busy; }
}
function formValues() {
  const values = {target_id: selectedTarget?.id, folder_id: selectedFolder?.id, mode: $('mode').value,
    start: $('start').value.trim(), end: $('end').value.trim(), retention: $('compact').checked ? 'compact' : 'all',
    capture_source: $('compact').checked && $('capture-source').checked,
    speed: $('speed').value, workers: $('workers').value, downloads: $('downloads').value, backend: $('backend').value,
    rate: $('rate').value, timeout: $('timeout').value, retries: $('retries').value, endpoint: $('endpoint').value,
    revision: $('revision').value, token_env: $('token-env').value, refresh: $('refresh').checked, retry_errors: $('retry-errors').checked};
  return values;
}
async function begin() {
  formError(''); message('');
  if (!selectedTarget) { formError('请先选择目标图片'); $('target-file').focus(); return; }
  try {
    $('start-button').disabled = true;
    await api('start', formValues());
    resultSignature = '';
    await poll();
  } catch (error) { formError(error.message); $('start-button').disabled = busy; }
}
function renderResults(state) {
  const rows = state.results || [];
  const previewKey = state.preview_key || state.job_key || '';
  const scanning = ['running', 'demo', 'stopping'].includes(state.status);
  $('result-count').textContent = rows.length
    ? `${scanning ? '扫描中 · ' : ''}展示前 ${rows.length} 项`
    : state.busy ? '正在比较候选' : '比较完成后显示';
  const signature = previewKey + ':' + rows.map((row) => (row.identifier || row.path) + ':' + row.digest).join(',');
  if (signature === resultSignature) return;
  resultSignature = signature;
  $('results-list').replaceChildren();
  visible('results-empty', rows.length === 0); visible('results-note', rows.length > 0);
  rows.forEach((row) => {
    const item = node('li', undefined, 'candidate');
    if (row.digest && row.metrics) {
      const image = node('img'); image.alt = '候选头像 ' + (row.identifier || ''); image.loading = 'lazy';
      image.src = '/api/preview?job=' + encodeURIComponent(previewKey) + '&digest=' + row.digest;
      item.append(image);
    } else item.append(node('div', '未获取图片', 'image-fallback'));
    const body = node('div', undefined, 'candidate-main');
    const heading = node('div', undefined, 'candidate-heading');
    const title = row.identifier ? '编号 ' + row.identifier : row.path.split(/[\\/]/).pop();
    heading.append(node('h3', title), node('span', row.label, row.label?.startsWith('证据不足') ? 'weak' : ''));
    body.append(heading);
    if (row.metrics) {
      const metric = row.metrics, geometry = metric.geometry;
      const metrics = node('div', undefined, 'candidate-metrics');
      [['结构相似', Number(metric.ssim).toFixed(3)], ['哈希一致', (metric.phash_agreement * 100).toFixed(1) + '%'], [metric.exact_file || metric.exact_pixels ? '内容核对' : '一致点', metric.exact_file || metric.exact_pixels ? '精确一致' : count(geometry.inliers)]].forEach(([label, value]) => {
        const entry = node('span', label); entry.append(node('b', value)); metrics.append(entry);
      });
      body.append(metrics);
      const details = node('details'); details.append(node('summary', '查看比较依据'));
      const explanation = metric.exact_file ? '两张图片的文件摘要完全相同，无需局部几何复核。' : metric.exact_pixels ? '经过方向与透明背景处理后，两张图片的像素完全相同，无需局部几何复核。' : `${geometry.ratio_matches} 个候选对应点中有 ${geometry.inliers} 个一致点；目标覆盖 ${(geometry.query_coverage * 100).toFixed(1)}%，候选覆盖 ${(geometry.candidate_coverage * 100).toFixed(1)}%。`;
      details.append(node('p', explanation + ' 完整并排图片和对应连线见对照报告。')); body.append(details);
    } else body.append(node('p', row.error || '未能读取此图片', 'candidate-error'));
    item.append(body); $('results-list').append(item);
  });
}
function render(state) {
  current = state; busy = state.busy;
  $('hardware').textContent = `${state.hardware.cpus} 个逻辑 CPU 可用`;
  $('workers').max = Math.max(4, state.hardware.cpus);
  $('version').textContent = 'AvatarCompare ' + state.version;
  $('input-fields').disabled = busy || picking; $('start-button').disabled = busy || picking;
  $('start-button').querySelector('span').textContent = busy ? '任务进行中' : '开始比对';
  $('demo-button').disabled = busy;
  const labels = {idle: '就绪', running: '正在比对', demo: '模拟验证', stopping: '正在停止', saving: '保存报告', complete: '已完成', stopped: '已停止', error: '操作失败', exporting: '正在导出', exported: '已导出'};
  $('task-status').textContent = labels[state.status] || state.status;
  $('task-status').className = 'badge' + (state.status === 'idle' ? ' neutral' : state.status === 'error' ? ' error' : '');
  $('task-heading').textContent = state.status === 'idle' ? '等待开始' : state.status === 'demo' || state.demo ? '本机模拟任务' : state.params?.target_name || '当前任务';
  const params = state.params || {};
  if (!busy && !state.demo && state.job_key && state.job_key !== lastSyncedJob && selectedTarget?.id === params.target_id && $('endpoint').value === params.endpoint) {
    $('revision').value = params.revision || ''; $('refresh').checked = false; lastSyncedJob = state.job_key;
  }
  if (params.start) $('task-description').textContent = `${params.start} — ${params.end} · ${params.retention === 'compact' ? '省空间' : '完整保留'} · ${Number(params.rate) === 0 ? '全速' : '每秒最多 ' + params.rate + ' 次请求'}`;
  else if (params.folder) $('task-description').textContent = '本地文件夹 · ' + params.folder;
  else if (state.status === 'demo') $('task-description').textContent = '正在使用本机模拟接口，验证获取、比较和保存流程。';
  const stats = state.stats || {};
  visible('progress-content', state.status !== 'idle');
  const percent = Number(stats.total) ? Math.min(100, Number(stats.processed || 0) / Number(stats.total) * 100) : 0;
  $('progress-count').textContent = count(stats.processed || 0) + ' / ' + count(stats.total || 0);
  $('progress-percent').textContent = percent.toFixed(1) + '%'; $('progress-bar').value = percent;
  const seconds = stats.elapsed_seconds, speed = stats.items_per_second ?? (seconds ? (Number(stats.processed) - Number(stats.resumed)) / seconds : null);
  $('stat-speed').textContent = !busy && Number(stats.processed) > 0 && stats.processed === stats.resumed ? '全部复用' : speed != null ? speed.toFixed(1) + ' 项/秒' : '—';
  $('stat-eta').textContent = !busy && state.status === 'complete' ? '已完成' : duration(stats.eta_seconds);
  $('stat-resumed').textContent = count(stats.resumed || 0); $('stat-errors').textContent = count(stats.errors || 0);
  visible('stop-button', busy && ['running', 'demo', 'stopping'].includes(state.status));
  $('stop-button').disabled = state.status === 'stopping';
  $('stop-button').textContent = state.status === 'stopping' ? '正在停止并保存…' : '停止并保存';
  visible('continue-button', !busy && state.status === 'stopped' && !state.demo);
  const hasReport = !!state.job_key && !busy;
  visible('report-link', hasReport); visible('output-button', hasReport);
  $('report-link').href = '/api/report?job=' + encodeURIComponent(state.job_key || '');
  $('task-message').textContent = state.message || '';
  $('task-message').classList.toggle('error', state.status === 'error'); visible('task-message', !!state.message);
  visible('open-export', !!state.export_key && !busy);
  renderResults(state);
  if (!initialLoaded) {
    initialLoaded = true;
    if (params.target_id && !state.demo && state.status !== 'demo') applyParams(params);
  }
  if (state.quitting) { quitting = true; message('正在保存并退出工具…'); }
}
async function poll() {
  clearTimeout(pollTimer);
  try {
    const wasBusy = busy;
    render(await api('state'));
    visible('connection', false);
    if (wasBusy && !busy && !$('history-page').hidden) await loadHistory();
  } catch (error) {
    $('connection').textContent = quitting ? '工具已退出。再次双击“启动.cmd”即可打开。' : '与本机工具的连接中断，正在重连。若已退出，请双击“启动.cmd”。';
    visible('connection', true);
    if (quitting) { $('input-fields').disabled = true; $('start-button').disabled = true; return; }
  }
  pollTimer = setTimeout(poll, busy ? 650 : 1600);
}
async function loadJob(key, retry = false) {
  try {
    const params = await api('load', {key}); applyParams(params);
    lastSyncedJob = current.job_key || '';
    $('retry-errors').checked = retry; if (retry) $('advanced').open = true;
    showPage('work');
    message(retry ? '已载入原任务，开始后会重新尝试失败项。' : '已载入原目标、范围和批次。点击“开始比对”继续，已处理项会自动跳过。');
    $('start-button').focus();
  } catch (error) { message(error.message, true); }
}
function rangesText(ranges) {
  if (ranges === null || ranges === undefined) return '逐项记录，未计算连续区间摘要';
  return ranges.length ? ranges.map(([start, end]) => start === end ? start : start + '–' + end).join('，') : '暂无';
}
async function loadHistory() {
  try {
    const {jobs} = await api('history');
    $('history-list').replaceChildren(); historyLoaded = true;
    if (!jobs.length) { $('history-list').append(node('p', '还没有扫描记录。完成或停止一个任务后，这里会保留进度。', 'loading')); return; }
    jobs.forEach((job) => {
      const item = node('article', undefined, 'history-item');
      const top = node('div', undefined, 'history-top'), title = node('div');
      title.append(node('h2', job.target_name));
      const range = job.requested_range ? job.requested_range.join(' — ') : '本地/旧版任务';
      const metadata = [range, job.source_name, new Date(job.updated * 1000).toLocaleString('zh-CN')];
      title.append(node('p', metadata.filter(Boolean).join(' · '), 'history-meta'));
      top.append(title, node('span', job.retention === 'compact' ? (job.capture_source ? '图源清单' : '省空间') : job.retention === 'local' ? '本地文件夹' : '完整保留', 'badge neutral'));
      item.append(top);
      const stats = node('div', undefined, 'history-stats');
      stats.append(node('span', '已处理 ' + count(job.processed)), node('span', '成功 ' + count(job.successful)), node('span', '失败 ' + count(job.errors)));
      if (job.retained_records != null) stats.append(node('span', '保留 ' + count(job.retained_records)));
      item.append(stats);
      const details = node('details'); details.append(node('summary', '查看区间与失败记录'));
      details.append(node('p', '已处理（含失败）：' + rangesText(job.processed_ranges)));
      if (job.retention === 'compact') {
        details.append(node('p', '成功比对：' + rangesText(job.successful_ranges)));
        details.append(node('p', '失败编号：' + (job.failed_identifiers?.join('，') || '暂无')));
        details.append(node('p', '已丢弃未命中记录：' + count(job.discarded_unmatched)));
      }
      if (job.ranges_truncated || job.errors_truncated) details.append(node('p', '以上为截断预览，完整区间和错误请导出查看。'));
      details.append(node('p', '任务 ID：' + job.run_id)); item.append(details);
      const actions = node('div', undefined, 'history-actions');
      if (job.can_resume) {
        const resume = node('button', '载入并继续', 'secondary'); resume.disabled = busy;
        resume.onclick = () => loadJob(job.key); actions.append(resume);
        if (job.errors) { const retry = node('button', '重试失败项', 'secondary'); retry.disabled = busy; retry.onclick = () => loadJob(job.key, true); actions.append(retry); }
      }
      if (job.has_report) {
        const report = node('a', '查看报告', 'secondary button-link'); report.href = '/api/report?job=' + encodeURIComponent(job.key); report.target = '_blank'; report.rel = 'noopener'; actions.append(report);
      }
      if (job.retention !== 'local') {
        const checkbox = node('input'); checkbox.type = 'checkbox'; checkbox.disabled = busy;
        const label = node('label', undefined, 'check-row'); label.append(checkbox, node('span', '含保留原图'));
        const exportButton = node('button', '导出记录', 'secondary'); exportButton.disabled = busy;
        exportButton.onclick = async () => { try { await api('export', {key: job.key, images: checkbox.checked}); showPage('work'); await poll(); } catch (error) { message(error.message, true); } };
        actions.append(exportButton, label);
      }
      if (!job.can_resume) item.append(node('p', '旧任务缺少原始配置或目标文件；请手动选择输入。现有记录仍可查看和导出。', 'history-no-resume'));
      item.append(actions); $('history-list').append(item);
    });
  } catch (error) { $('history-list').replaceChildren(node('p', error.message, 'inline-error')); }
}
function showPage(page) {
  const work = page === 'work';
  visible('work-page', work); visible('history-page', !work);
  for (const name of ['work', 'history']) { $('tab-' + name).classList.toggle('active', name === page); $('tab-' + name).setAttribute('aria-pressed', String(name === page)); }
  if (!work) loadHistory();
}
$('scan-form').addEventListener('submit', (event) => { event.preventDefault(); begin(); });
$('target-file').addEventListener('change', (event) => upload(event.target.files[0]));
for (const name of ['dragenter', 'dragover']) $('drop-zone').addEventListener(name, (event) => { event.preventDefault(); if (!busy) $('drop-zone').classList.add('dragging'); });
for (const name of ['dragleave', 'drop']) $('drop-zone').addEventListener(name, (event) => { event.preventDefault(); $('drop-zone').classList.remove('dragging'); });
$('drop-zone').addEventListener('drop', (event) => upload(event.dataTransfer.files[0]));
$('mode').addEventListener('change', updateMode);
for (const id of ['start', 'end']) $(id).addEventListener('input', updateRange);
for (const id of ['speed', 'compact', 'capture-source']) $(id).addEventListener('change', updateSettings);
for (const button of document.querySelectorAll('[data-qlogo-endpoint]')) {
  button.addEventListener('click', () => {
    if ($('endpoint').value === button.dataset.qlogoEndpoint) return;
    $('endpoint').value = button.dataset.qlogoEndpoint;
    $('revision').value = $('token-env').value = '';
    $('refresh').checked = $('retry-errors').checked = false;
    lastSyncedJob = current.job_key || '';
  });
}
$('full-speed').addEventListener('change', () => { $('rate').value = $('full-speed').checked ? '0' : '2'; });
$('rate').addEventListener('input', () => { $('full-speed').checked = Number($('rate').value) === 0; });
$('choose-folder').onclick = async () => {
  picking = true; $('input-fields').disabled = true; $('start-button').disabled = true;
  try { const value = await api('pick-folder', {}); if (value.folder) { selectedFolder = value.folder; $('folder-name').textContent = value.folder.name; } }
  catch (error) { formError(error.message); }
  finally { picking = false; $('input-fields').disabled = busy; $('start-button').disabled = busy; }
};
$('choose-original').onclick = async () => {
  picking = true; $('input-fields').disabled = true; $('start-button').disabled = true;
  try { const value = await api('pick-target', {}); if (value.target) {
    selectedTarget = value.target; $('target-preview').src = value.target.url;
    visible('target-preview', true); visible('target-empty', false); $('target-name').textContent = value.target.name;
  } } catch (error) { formError(error.message); }
  finally { picking = false; $('input-fields').disabled = busy; $('start-button').disabled = busy; }
};
$('stop-button').onclick = async () => { try { await api('stop', {}); await poll(); } catch (error) { message(error.message, true); } };
$('continue-button').onclick = async () => { await loadJob(current.job_key); };
$('output-button').onclick = async () => { try { await api('open', {key: current.job_key, kind: 'folder'}); } catch (error) { message(error.message, true); } };
$('open-export').onclick = async () => { try { await api('open', {key: current.export_key, kind: 'export'}); } catch (error) { message(error.message, true); } };
$('demo-button').onclick = async () => { try { message(''); await api('demo', {}); await poll(); } catch (error) { message(error.message, true); } };
$('tab-work').onclick = () => showPage('work'); $('tab-history').onclick = () => showPage('history'); $('refresh-history').onclick = loadHistory;
$('quit').onclick = () => { visible('quit-confirm', !$('quit-confirm').hidden ? false : true); if (!$('quit-confirm').hidden) $('quit-cancel').focus(); };
$('quit-cancel').onclick = () => { visible('quit-confirm', false); $('quit').focus(); };
document.addEventListener('keydown', (event) => { if (event.key === 'Escape') visible('quit-confirm', false); });
$('quit-confirm-button').onclick = async () => { try { quitting = true; await api('quit', {}); visible('quit-confirm', false); message('正在停止任务并保存，稍后可重新启动。'); await poll(); } catch (error) { message(error.message, true); } };
updateMode(); updateSettings(); poll();
