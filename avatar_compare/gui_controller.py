"""Local UI orchestration. Matching and persistence remain owned by the engines."""
from __future__ import annotations

from contextlib import closing
from copy import deepcopy
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

from PIL import Image, ImageOps

from .cache_lock import CacheLease
from .source import QLOGO_ENDPOINT, SourceConfig, cache_batch

LOG = logging.getLogger(__name__)
MAX_UPLOAD = 32 * 1024 * 1024


def save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False), encoding='utf-8')
    temporary.replace(path)


def read_json(path):
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


class Controller:
    def __init__(self, root, *, opener=None):
        self.root = Path(root).resolve()
        self.ui_dir = self.root / '.ui'
        self.ui_dir.mkdir(parents=True, exist_ok=True)
        self.guard = threading.RLock()
        self.inputs = read_json(self.ui_dir / 'inputs.json')
        self.jobs = read_json(self.ui_dir / 'jobs.json')
        self.sources = read_json(self.ui_dir / 'sources.json')
        self.current = {'status': 'idle', 'stats': {}, 'results': [], 'message': ''}
        self.cancel = threading.Event()
        self.live_preview_state = None
        self.worker = None
        self.opener = opener or self._open_local
        self.folders = {}

    @staticmethod
    def _open_local(path):
        if os.name == 'nt':
            os.startfile(str(path))
        else:
            import webbrowser
            webbrowser.open(path.as_uri())

    def busy(self):
        return bool(self.worker and self.worker.is_alive())

    def snapshot(self):
        with self.guard:
            return {**deepcopy(self.current), 'busy': self.busy(),
                    'hardware': {'cpus': os.cpu_count() or 1},
                    'source': QLOGO_ENDPOINT, 'version': __import__('avatar_compare').__version__}

    def upload(self, data, name):
        if not data or len(data) > MAX_UPLOAD:
            raise ValueError('请选择不超过 32 MiB 的图片')
        with Image.open(io.BytesIO(data)) as image:
            if image.width * image.height > 20_000_000:
                raise ValueError('图片超过 2000 万像素，请缩小后重试')
            image.verify()
        identity = hashlib.sha256(data).hexdigest()
        safe_name = Path(name.replace('\\', '/')).name[:180] or '目标图片'
        with self.guard:
            path = self.ui_dir / 'inputs' / (identity + '.img')
            path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists():
                temporary = path.with_suffix('.tmp')
                temporary.write_bytes(data)
                temporary.replace(path)
            self.inputs[identity] = {**self.inputs.get(identity, {}), 'path': str(path), 'name': safe_name}
            save_json(self.ui_dir / 'inputs.json', self.inputs)
        return {'id': identity, 'name': safe_name, 'url': '/api/target?id=' + identity}

    def target_preview(self, identity):
        with self.guard:
            entry = self.inputs.get(identity)
        if not entry:
            raise ValueError('请重新选择目标图片')
        with Image.open(entry['path']) as image:
            image = ImageOps.exif_transpose(image).convert('RGBA')
            image.thumbnail((480, 480))
            background = Image.new('RGBA', image.size, 'white')
            background.alpha_composite(image)
            stream = io.BytesIO()
            background.convert('RGB').save(stream, 'JPEG', quality=90)
            return stream.getvalue()

    def _pick(self, kind):
        command = [sys.executable, '-m', 'avatar_compare.gui_picker', '--kind', kind]
        # A pythonw parent has no stdout, but the child communicates through pipes.
        if os.name == 'nt':
            command[0] = str(Path(sys.executable).with_name('python.exe'))
        completed = subprocess.run(command, cwd=Path(__file__).resolve().parents[1], capture_output=True,
                                   creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0,
                                   timeout=600)
        if completed.returncode:
            raise ValueError('无法打开系统文件夹选择器，请重试')
        value = json.loads(completed.stdout.decode('utf-8'))
        if not value:
            return None
        return Path(value).resolve(strict=True)

    def pick_target(self):
        path = self._pick('target')
        if path is None:
            return None
        from .features import read_bytes
        result = self.upload(read_bytes(path), path.name)
        with self.guard:
            self.inputs[result['id']]['original'] = str(path)
            save_json(self.ui_dir / 'inputs.json', self.inputs)
        return result

    def pick_folder(self):
        path = self._pick('folder')
        if path is None:
            return None
        if not path.is_dir():
            raise ValueError('请选择图片文件夹')
        identity = uuid.uuid4().hex
        self.folders[identity] = path
        return {'id': identity, 'name': str(path)}

    def validate(self, values):
        from .range_engine import validate_range
        with self.guard:
            entry = self.inputs.get(values.get('target_id'))
        if not entry or not Path(entry['path']).is_file():
            raise ValueError('请先选择目标图片')
        mode = values.get('mode', 'range')
        if mode not in {'range', 'local'}:
            raise ValueError('请选择编号范围或本地文件夹')
        params = {'target_id': values['target_id'], 'target_name': entry['name'],
                  'target': entry['path'], 'mode': mode}
        if mode == 'local':
            from .features import read_bytes
            original = entry.get('original')
            if not original or not Path(original).is_file() or hashlib.sha256(read_bytes(Path(original))).hexdigest() != values['target_id']:
                raise ValueError('本地文件夹比对请先点“选择原始目标文件”，以便跳过目标文件自身')
            folder = self.folders.get(values.get('folder_id'))
            if not folder or not folder.is_dir():
                raise ValueError('请先选择候选图片文件夹')
            params.update(target=original, folder=str(folder), retention='all', workers=2)
            return params
        try:
            # Never round 64-bit IDs through a browser floating-point Number.
            raw = [str(values.get(field, '')).strip() for field in ('start', 'end')]
            if not all(value.isascii() and value.isdecimal() for value in raw):
                raise ValueError()
            start, end = map(int, raw)
            validate_range(start, end)
        except (ValueError, TypeError):
            raise ValueError('起止编号须为非负整数，结束编号不小于起始编号，且范围须在 64 位整数内') from None
        retention = values.get('retention', 'compact')
        if retention not in {'all', 'compact'}:
            raise ValueError('请选择保留方式')
        capture_source = bool(values.get('capture_source', False))
        if capture_source and retention != 'compact':
            raise ValueError('保留图源清单需要启用省空间模式')
        speed = values.get('speed', 'efficient')
        cpus = min(os.cpu_count() or 1, 256)
        if speed == 'efficient':
            workers, downloads, backend = min(8, cpus), min(128, max(16, cpus * 4)), 'thread'
        elif speed == 'maximum':
            workers, downloads, backend = cpus, min(256, cpus * 4), 'process'
        elif speed == 'custom':
            workers, downloads = int(values.get('workers', 2)), int(values.get('downloads', 16))
            backend = values.get('backend', 'thread')
        else:
            raise ValueError('请选择计算设置')
        if not 1 <= workers <= max(4, cpus) or not 1 <= downloads <= 256 or backend not in {'thread', 'process'}:
            raise ValueError(f'计算并行数为 1–{max(4, cpus)}，下载并行数为 1–256')
        endpoint = str(values.get('endpoint') or QLOGO_ENDPOINT).strip()
        revision = str(values.get('revision') or '').strip()
        rate, timeout, retries = float(values.get('rate', 0)), float(values.get('timeout', 10)), int(values.get('retries', 2))
        token_env = str(values.get('token_env') or '').strip() or None
        name = 'QQ头像接口（用户指定）' if endpoint == QLOGO_ENDPOINT else '获授权测试接口'
        SourceConfig(endpoint, revision or 'validation', name=name, token_env=token_env,
                     rate=rate, timeout=timeout, retries=retries, keep_alive=True)
        refresh = bool(values.get('refresh', False))
        if refresh and revision:
            raise ValueError('重新获取和指定批次不能同时使用；清空批次后重试')
        params.update(start=str(start), end=str(end), retention=retention, speed=speed, capture_source=capture_source,
                      workers=workers, downloads=downloads, backend=backend, endpoint=endpoint,
                      revision=revision, rate=rate, timeout=timeout, retries=retries, token_env=token_env,
                      refresh=refresh, retry_errors=bool(values.get('retry_errors', False)))
        return params

    def _launch(self, function, *, status, params=None):
        with self.guard:
            if self.busy():
                raise ValueError('已有操作正在运行；请完成或停止后再开始')
            self.cancel = threading.Event()
            if status in {'running', 'demo'}:
                self.live_preview_state = None
                self.current = {'status': status, 'stats': {}, 'results': [], 'message': '', 'params': params or {}}
            else:
                self.current.update(status=status, message='')
            def run():
                try:
                    function()
                except Exception as error:
                    LOG.exception('Local UI operation failed')
                    with self.guard:
                        self.current.update(status='error', message=f'{type(error).__name__}: {error}。已提交的进度仍保存在任务库，可重试或从扫描记录恢复。')
            self.worker = threading.Thread(target=run, name='avatar-ui-task', daemon=False)
            self.worker.start()

    def start(self, values):
        params = self.validate(values)
        self._launch(lambda: self._scan(params), status='running', params=params)

    def stop(self):
        with self.guard:
            if self.current['status'] in {'running', 'demo', 'stopping'} and self.busy():
                self.cancel.set()
                self.current.update(status='stopping', message='正在停止下载并保存已处理结果，请等待当前操作退出。')

    def _progress(self, event):
        with self.guard:
            self.current['stats'] = dict(event)

    def _scan(self, params, *, state=None, output=None):
        from .engine import search
        from .staged import search_staged
        from .report import evidence_label, write_reports
        from .storage_profile import require_profile
        compact = params['retention'] == 'compact'
        state = state or self.root / ('.state-compact' if compact else '.state')
        output = output or self.root / 'outputs'
        group = 'compact' if compact else 'all'
        if state not in [self.root / '.state-compact', self.root / '.state']:
            group = 'demo'

        key_prefix = group + ':'

        def labeled_candidates(rows):
            return [{**row, 'label': evidence_label(row)} for row in rows]

        def publish_candidates(run_id, rows):
            with self.guard:
                self.live_preview_state = state
                self.current.update(preview_key=key_prefix + run_id, results=labeled_candidates(rows))

        with CacheLease(state):
            require_profile(state, compact=compact)
            if params['mode'] == 'local':
                result = search(Path(params['target']), Path(params['folder']), state,
                                workers=2, cancel=self.cancel, progress=self._progress)
            else:
                revision = params['revision'] or cache_batch(state, params['endpoint'], params['token_env'], refresh=params['refresh'])
                params['revision'] = revision
                name = 'QQ头像接口（用户指定）' if params['endpoint'] == QLOGO_ENDPOINT else '获授权测试接口'
                config = SourceConfig(params['endpoint'], revision, name=name, token_env=params['token_env'],
                                      rate=params['rate'], timeout=params['timeout'], retries=params['retries'], keep_alive=True)
                token = os.environ.get(params['token_env'], '') if params['token_env'] else ''
                source_key = hashlib.sha256((config.endpoint + '\0' + revision + '\0' + token).encode()).hexdigest()
                with self.guard:
                    self.sources[params['retention'] + ':' + params['target_id'] + ':' + source_key] = deepcopy(params)
                    self.sources[params['retention'] + ':' + params['target_id'] + ':' + source_key]['refresh'] = False
                    save_json(self.ui_dir / 'sources.json', self.sources)
                result = search_staged(Path(params['target']), int(params['start']), int(params['end']), state, config,
                                       workers=params['workers'], download_workers=params['downloads'],
                                       compute_backend=params['backend'], share_computations=True, compact=compact,
                                       cancel=self.cancel, progress=self._progress, retry_errors=params['retry_errors'],
                                       top_limit=20, on_top_matches=publish_candidates,
                                       capture_source=params.get('capture_source', False))
            with self.guard:
                self.current.update(status='saving', message='正在生成候选对照报告…')
            reports = write_reports(result, output, limit=20)
        key = key_prefix + result['run_id']
        with self.guard:
            self.jobs[key] = {'params': deepcopy(params), 'state_dir': str(state),
                              'reports': {kind: str(path) for kind, path in reports.items()}}
            # A new explicit run reuses its exact epoch, rather than refreshing again.
            self.jobs[key]['params']['refresh'] = False
            save_json(self.ui_dir / 'jobs.json', self.jobs)
            self.live_preview_state = None
            self.current.pop('preview_key', None)
            self.current.update(status=result['status'], stats=result['stats'], job_key=key,
                                params=deepcopy(params), results=labeled_candidates(result['results'][:20]),
                                message='已停止，进度已保存。可继续此任务。' if result['status'] == 'stopped' else f"比较完成，{result['stats']['errors']} 个编号/文件未成功，可从扫描记录重试。" if result['stats']['errors'] else '比较完成。候选及错误记录已保存。')

    def history(self):
        from .history import inspect
        result = []
        for group, folder in [('compact', '.state-compact'), ('all', '.state')]:
            state = self.root / folder
            for row in inspect(state, limit=40):
                key = group + ':' + row['run_id']
                with self.guard:
                    meta = deepcopy(self.jobs.get(key, {}))
                row.update(key=key, target_name=meta.get('params', {}).get('target_name') or Path(row['target']).name,
                           has_report=(self.report_path(key, missing_ok=True) is not None))
                if row.get('requested_range'):
                    row['requested_range'] = list(map(str, row['requested_range']))
                for field in ['processed_ranges', 'successful_ranges']:
                    if row.get(field) is not None:
                        row[field] = [[str(a), str(b)] for a, b in row[field]]
                if 'failed_identifiers' in row:
                    row['failed_identifiers'] = list(map(str, row['failed_identifiers']))
                row['can_resume'] = self._resume_params(key, row, check=False) is not None
                result.append(row)
        # Demo caches are intentionally separate, but their latest report remains visible in the work area.
        return sorted(result, key=lambda row: row['updated'], reverse=True)

    def _job(self, key):
        from .history import inspect
        group, separator, run_id = str(key).partition(':')
        if not separator or group not in {'all', 'compact', 'demo'} or not run_id.isalnum():
            raise ValueError('无效任务')
        if group == 'demo':
            with self.guard:
                meta = self.jobs.get(key)
            if not meta:
                raise ValueError('未找到任务')
            state = Path(meta['state_dir'])
        else:
            state = self.root / ('.state-compact' if group == 'compact' else '.state')
        rows = inspect(state, run_id=run_id)
        if not rows:
            raise ValueError('未找到任务记录')
        return state, rows[0]

    def _resume_params(self, key, row, *, check):
        from .engine import COMPARE_VERSION
        with self.guard:
            meta = deepcopy(self.jobs.get(key, {}))
        params = meta.get('params')
        if not params and row.get('algorithm') == COMPARE_VERSION:
            source_key = row.get('retention', '') + ':' + row.get('query_digest', '') + ':' + row.get('source_fingerprint', '')
            with self.guard:
                params = deepcopy(self.sources.get(source_key))
            if params and row.get('requested_range'):
                params.update(start=str(row['requested_range'][0]), end=str(row['requested_range'][1]))
        if not params:
            bounds = row.get('requested_range')
            if not bounds or row.get('algorithm') != COMPARE_VERSION:
                return None
            revision = row.get('source_revision')
            expected = hashlib.sha256((QLOGO_ENDPOINT + '\0' + (revision or '') + '\0').encode()).hexdigest()
            if expected != row.get('source_fingerprint'):
                return None
            params = dict(mode='range', start=str(bounds[0]), end=str(bounds[1]), retention=row['retention'],
                          speed='efficient', endpoint=QLOGO_ENDPOINT, revision=revision, rate=0, timeout=10, retries=2,
                          refresh=False, token_env=None, retry_errors=False, target=row['target'])
        if params.get('mode') == 'range':
            params['capture_source'] = bool(row.get('capture_source'))
        if not check:
            return params if Path(params['target']).is_file() else None
        path = Path(params['target'])
        from .features import read_bytes
        data = read_bytes(path)
        if row.get('query_digest') and hashlib.sha256(data).hexdigest() != row['query_digest']:
            raise ValueError('目标文件内容已改变，请重新选择图片创建任务')
        selected = self.upload(data, params.get('target_name') or path.name)
        params.update(target_id=selected['id'], target_name=selected['name'], refresh=False, retry_errors=False)
        if params['mode'] == 'local':
            with self.guard:
                self.inputs[selected['id']]['original'] = str(path)
                save_json(self.ui_dir / 'inputs.json', self.inputs)
            folder_id = uuid.uuid4().hex
            self.folders[folder_id] = Path(params['folder'])
            params['folder_id'] = folder_id
        return params

    def load_job(self, key):
        _, row = self._job(key)
        params = self._resume_params(key, row, check=True)
        if params is None:
            raise ValueError('此旧任务缺少原始范围或接口配置，请手动选择目标及范围；不会自动替换来源')
        return params

    def preview(self, key, digest):
        if len(digest) != 64 or any(value not in '0123456789abcdef' for value in digest):
            raise ValueError('无效图片摘要')
        with self.guard:
            preview_key = self.current.get('preview_key', self.current.get('job_key'))
            if key != preview_key or digest not in {row.get('digest') for row in self.current.get('results', [])}:
                raise ValueError('图片不在当前展示结果中')
            state = self.live_preview_state or Path(self.jobs[key]['state_dir'])
        # Read-only: do not acquire Store's writer lease during an active scan.
        from .engine import STORAGE_VERSION, FEATURE_VERSION, unpack
        with closing(sqlite3.connect((state / 'cache.sqlite3').as_uri() + '?mode=ro', uri=True)) as db:
            record = db.execute('SELECT payload FROM features WHERE digest=? AND version IN (?,?) ORDER BY version DESC LIMIT 1', (digest, STORAGE_VERSION, FEATURE_VERSION)).fetchone()
            if not record:
                raise ValueError('没有可用的预览')
            return unpack(record[0]).preview

    def report_path(self, key, *, missing_ok=False):
        with self.guard:
            meta = self.jobs.get(key, {})
        value = meta.get('reports', {}).get('html')
        if not value:
            group, _, run_id = str(key).partition(':')
            if group in {'compact', 'all'} and run_id.isalnum():
                value = str(self.root / 'outputs' / ('comparison-' + run_id + '.html'))
        path = Path(value) if value else None
        if path and path.is_file() and path.resolve().is_relative_to(self.root / 'outputs'):
            return path
        if missing_ok:
            return None
        raise ValueError('此任务尚未生成报告，可继续任务后生成')

    def export(self, key, *, images=False):
        state, row = self._job(key)
        if row['retention'] == 'local':
            raise ValueError('本地任务的全部记录已在报告同名 CSV 和 JSON 中，请打开报告目录')
        def work():
            from .ledger import export_records
            output = self.root / 'outputs' / ('export-' + row['run_id'] + '-' + time.strftime('%Y%m%d-%H%M%S'))
            paths = export_records(state, row['run_id'], output, images=images)
            with self.guard:
                self.jobs.setdefault(key, {}).update(export_dir=str(output))
                save_json(self.ui_dir / 'jobs.json', self.jobs)
                self.current.update(status='exported', message='导出完成：' + str(output), export_key=key,
                                    export_files={kind: str(path) for kind, path in paths.items()})
        self._launch(work, status='exporting')

    def open_output(self, key, kind):
        self._job(key)
        if kind == 'report':
            path = self.report_path(key)
        elif kind == 'folder':
            path = self.report_path(key).parent
        elif kind == 'export':
            with self.guard:
                value = self.jobs.get(key, {}).get('export_dir')
            if not value:
                raise ValueError('请先导出记录')
            path = Path(value).resolve()
            if not path.is_dir() or not path.is_relative_to(self.root / 'outputs'):
                raise ValueError('导出目录不可用')
        else:
            raise ValueError('无效操作')
        self.opener(path)

    def demo(self):
        def work():
            from .mock_service import MockServer, dataset
            output = self.root / 'outputs' / ('gui-demo-' + time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6])
            output.mkdir(parents=True)
            # Same deterministic fixture used by the existing range validation.
            target, images, _ = dataset()
            images.update({1018: b'invalid image', 1019: target, 1020: target})
            selected = self.upload(target, '模拟目标.png')
            with MockServer(images, faults={1019: [503, 200], 1020: [503]}, delay=0.04) as server:
                params = self.validate(dict(target_id=selected['id'], start='1001', end='1020',
                                            retention='compact', speed='efficient', endpoint=server.url,
                                            revision='gui-fixture-v1', rate=0))
                self._scan(params, state=output / 'state', output=output)
            with self.guard:
                self.current['demo'] = True
                self.current['message'] = '本机模拟验证完成。模拟头像、错误和报告保存在 outputs，未访问真实头像接口。'
        self._launch(work, status='demo')
