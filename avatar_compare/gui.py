"""Single-instance loopback UI. No external assets, framework or upload service."""
from __future__ import annotations

import argparse
import hmac
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import secrets
import subprocess
import sys
import threading
import time
from urllib.parse import parse_qs, urlsplit
from urllib.request import ProxyHandler, Request, build_opener
import webbrowser

for variable in ['OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'OMP_NUM_THREADS']:
    os.environ[variable] = '1'

from .cache_lock import CacheLease
from .gui_controller import Controller, MAX_UPLOAD, read_json, save_json


class LocalServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, root, *, port=0, controller=None, token=None):
        self.controller = controller or Controller(root)
        self.token = token or secrets.token_urlsafe(32)
        self.stopping = threading.Event()
        super().__init__(('127.0.0.1', port), Handler)
        self.origin = f'http://127.0.0.1:{self.server_port}'
        self.cookie_name = f'avatar_ui_{self.server_port}'

    @property
    def launch_url(self):
        return self.origin + '/launch/' + self.token

    def request_shutdown(self):
        if self.stopping.is_set():
            return
        self.stopping.set()
        self.controller.stop()
        def finish():
            worker = self.controller.worker
            if worker:
                worker.join()
            self.shutdown()
        threading.Thread(target=finish, name='avatar-ui-shutdown', daemon=True).start()


class Handler(BaseHTTPRequestHandler):
    server_version = 'AvatarCompare'

    def setup(self):
        super().setup()
        self.connection.settimeout(30)

    def log_message(self, *args):
        # Never log launch capabilities, IDs, target paths or source credentials.
        pass

    def send(self, data, *, status=200, kind='application/json; charset=utf-8', headers=None, report=False):
        if not isinstance(data, bytes):
            def safe_numbers(value):
                if isinstance(value, int) and abs(value) > 2**53 - 1:
                    return str(value)
                if isinstance(value, dict):
                    return {key: safe_numbers(item) for key, item in value.items()}
                if isinstance(value, (tuple, list)):
                    return [safe_numbers(item) for item in value]
                return value
            data = safe_numbers(data)
            data = json.dumps(data, ensure_ascii=False, allow_nan=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', kind)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('X-Frame-Options', 'DENY')
        policy = "default-src 'none'; style-src 'unsafe-inline'; img-src data:; frame-ancestors 'none'; base-uri 'none'" if report else "default-src 'self'; img-src 'self' blob:; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'"
        self.send_header('Content-Security-Policy', policy)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass

    def authorized(self):
        if self.headers.get('Host') != urlsplit(self.server.origin).netloc:
            return False
        try:
            cookies = SimpleCookie(self.headers.get('Cookie', ''))
            value = cookies.get(self.server.cookie_name)
            return bool(value and hmac.compare_digest(value.value, self.server.token))
        except Exception:
            return False

    def do_GET(self):
        path = urlsplit(self.path).path
        if self.headers.get('Host') != urlsplit(self.server.origin).netloc:
            self.send({'error': '无效的本机地址'}, status=403)
            return
        if path.startswith('/launch/'):
            if not hmac.compare_digest(path[len('/launch/'):], self.server.token):
                self.send({'error': '请双击启动.cmd 打开界面'}, status=403)
                return
            self.send(b'', status=303, headers={'Location': '/', 'Set-Cookie': f'{self.server.cookie_name}={self.server.token}; HttpOnly; SameSite=Strict; Path=/'})
            return
        if not self.authorized():
            self.send({'error': '请双击启动.cmd 打开界面'}, status=401)
            return
        query = parse_qs(urlsplit(self.path).query)
        value = lambda key: query.get(key, [''])[0]
        controller = self.server.controller
        try:
            if path == '/api/state':
                self.send({**controller.snapshot(), 'quitting': self.server.stopping.is_set()})
            elif path == '/api/history':
                self.send({'jobs': controller.history()})
            elif path == '/api/target':
                self.send(controller.target_preview(value('id')), kind='image/jpeg')
            elif path == '/api/preview':
                data = controller.preview(value('job'), value('digest'))
                self.send(data, kind='image/jpeg' if data.startswith(b'\xff\xd8') else 'image/png')
            elif path == '/api/report':
                self.send(controller.report_path(value('job')).read_bytes(), kind='text/html; charset=utf-8', report=True)
            elif path in {'/', '/app.js', '/style.css'}:
                filename, kind = {'/': ('index.html', 'text/html; charset=utf-8'), '/app.js': ('app.js', 'text/javascript; charset=utf-8'), '/style.css': ('style.css', 'text/css; charset=utf-8')}[path]
                self.send((Path(__file__).parent / 'web' / filename).read_bytes(), kind=kind)
            elif path == '/favicon.ico':
                self.send(b'', status=204, kind='image/x-icon')
            else:
                self.send({'error': '未找到页面'}, status=404)
        except (ValueError, OSError) as error:
            self.send({'error': str(error)}, status=400)
        except Exception:
            logging.exception('UI read failed')
            self.send({'error': '读取失败，请重试；详情见 .ui/gui.log'}, status=500)

    def do_POST(self):
        if not self.authorized() or self.headers.get('Origin') != self.server.origin:
            self.send({'error': '请从本机界面操作'}, status=403)
            return
        if self.server.stopping.is_set():
            self.send({'error': '工具正在退出，请稍后重新启动'}, status=409)
            return
        path = urlsplit(self.path).path
        controller = self.server.controller
        try:
            length = int(self.headers.get('Content-Length', '-1'))
            limit = MAX_UPLOAD if path == '/api/upload' else 64 * 1024
            if not 0 <= length <= limit:
                raise ValueError('请求过大或长度无效')
            data = self.rfile.read(length)
            if len(data) != length:
                raise ValueError('请求不完整，请重试')
            if path == '/api/upload':
                query = parse_qs(urlsplit(self.path).query)
                self.send(controller.upload(data, query.get('name', ['目标图片'])[0]))
                return
            if self.headers.get_content_type() != 'application/json':
                raise ValueError('无效请求格式')
            values = json.loads(data or b'{}')
            if not isinstance(values, dict):
                raise ValueError('无效请求参数')
            if path == '/api/start':
                controller.start(values)
            elif path == '/api/stop':
                controller.stop()
            elif path == '/api/load':
                self.send(controller.load_job(values.get('key')))
                return
            elif path == '/api/export':
                controller.export(values.get('key'), images=bool(values.get('images')))
            elif path == '/api/open':
                controller.open_output(values.get('key'), values.get('kind'))
            elif path == '/api/pick-folder':
                self.send({'folder': controller.pick_folder()})
                return
            elif path == '/api/pick-target':
                self.send({'target': controller.pick_target()})
                return
            elif path == '/api/demo':
                controller.demo()
            elif path == '/api/quit':
                self.send({'ok': True})
                self.server.request_shutdown()
                return
            else:
                self.send({'error': '未找到操作'}, status=404)
                return
            self.send({'ok': True})
        except (ValueError, OSError, TypeError, OverflowError) as error:
            self.send({'error': str(error)}, status=400)
        except Exception:
            logging.exception('UI action failed')
            self.send({'error': '操作失败，请重试；详情见 .ui/gui.log'}, status=500)


def existing_url(root):
    value = read_json(root / '.ui' / 'runtime.json')
    try:
        port, token = int(value['port']), str(value['token'])
        if not 1 <= port <= 65535 or len(token) < 32:
            return None
        origin = f'http://127.0.0.1:{port}'
        request = Request(origin + '/api/state', headers={'Cookie': f'avatar_ui_{port}={token}'})
        with build_opener(ProxyHandler({})).open(request, timeout=2) as response:
            if response.headers.get('Server', '').startswith('AvatarCompare') and not json.load(response).get('quitting'):
                return origin + '/launch/' + token
    except (OSError, ValueError, KeyError):
        pass
    return None


def main():
    parser = argparse.ArgumentParser(description='头像比对本机图形界面')
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--no-browser', action='store_true')
    parser.add_argument('--launch', action='store_true', help='启动隐藏的本机进程后退出')
    args = parser.parse_args()
    root = args.root.resolve()
    url = existing_url(root)
    if url:
        if not args.no_browser:
            webbrowser.open(url)
        return 0
    if args.launch:
        executable = Path(sys.executable)
        if os.name == 'nt' and executable.with_name('pythonw.exe').is_file():
            executable = executable.with_name('pythonw.exe')
        command = [str(executable), '-m', 'avatar_compare.gui', '--root', str(root)]
        if args.no_browser:
            command.append('--no-browser')
        subprocess.Popen(command, cwd=Path(__file__).resolve().parents[1], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                         creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        # Wait for startup so a failed background launch is visible to the CMD caller.
        for _ in range(100):
            time.sleep(0.1)
            if existing_url(root):
                return 0
        raise RuntimeError('界面启动失败，请查看 .ui/gui.log 或重新运行安装依赖.ps1')
    ui = root / '.ui'
    ui.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, handlers=[RotatingFileHandler(ui / 'gui.log', maxBytes=2*1024*1024, backupCount=2, encoding='utf-8')])
    try:
        with CacheLease(ui):
            server = LocalServer(root)
            save_json(ui / 'runtime.json', {'port': server.server_port, 'pid': os.getpid(), 'token': server.token})
            try:
                if not args.no_browser:
                    webbrowser.open(server.launch_url)
                server.serve_forever(poll_interval=0.2)
            finally:
                server.controller.stop()
                if server.controller.worker:
                    server.controller.worker.join()
                server.server_close()
                (ui / 'runtime.json').unlink(missing_ok=True)
    except ValueError:
        # Two simultaneous double-clicks may race. Reuse the lock owner's UI.
        for _ in range(30):
            time.sleep(0.1)
            url = existing_url(root)
            if url:
                if not args.no_browser:
                    webbrowser.open(url)
                return 0
        raise
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
