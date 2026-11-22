"""Explicit, rate limited HTTP image source; never follows redirects."""
from __future__ import annotations

from dataclasses import dataclass, field
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
from http.client import HTTPConnection, HTTPSConnection, HTTPException
import math
import os
from pathlib import Path
import sqlite3
import ssl
import string
import threading
import time
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .features import MAX_BYTES
from .cache_lock import CacheLease
from . import __version__

QLOGO_ENDPOINT = 'https://q1.qlogo.cn/g?b=qq&nk={id}&s=100'


def cache_batch(state_dir: Path, endpoint: str, token_env=None, *, refresh=False) -> str:
    """Stable cache epoch for mutable sources; explicit refresh selects a new epoch."""
    token = os.environ.get(token_env, '') if token_env else ''
    key = hashlib.sha256((endpoint+'\0'+token).encode()).hexdigest()
    state_dir.mkdir(parents=True, exist_ok=True)
    with CacheLease(state_dir), closing(sqlite3.connect(state_dir/'cache.sqlite3', timeout=30)) as db:
        with db:
            db.execute('CREATE TABLE IF NOT EXISTS source_batches(source TEXT PRIMARY KEY, batch TEXT)')
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT batch FROM source_batches WHERE source=?', (key,)).fetchone()
            if row and not refresh:
                return row[0]
            batch = 'cache-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
            db.execute('INSERT OR REPLACE INTO source_batches VALUES(?,?)', (key, batch))
            return batch


class FetchFailure(Exception):
    def __init__(self, code: str, *, status=None, retryable=False):
        super().__init__(code)
        self.code, self.status, self.retryable = code, status, retryable


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


@dataclass(frozen=True)
class SourceConfig:
    endpoint: str = field(repr=False)
    revision: str
    name: str = '获授权测试接口'
    token_env: str | None = None
    timeout: float = 10.0
    retries: int = 2
    rate: float = 2.0
    max_bytes: int = MAX_BYTES
    keep_alive: bool = False

    def __post_init__(self):
        fields = list(string.Formatter().parse(self.endpoint))
        if sum(name == 'id' for _, name, _, _ in fields) != 1 or any(name is not None and (name != 'id' or spec or conv) for _, name, spec, conv in fields):
            raise ValueError('接口模板必须且只能包含一个 {id} 占位符')
        url = urlsplit(self.endpoint.format(id=1))
        if url.scheme not in {'http', 'https'} or not url.hostname or url.username or url.password or url.fragment:
            raise ValueError('接口必须是无用户凭据和片段的 HTTP(S) URL')
        if '{id}' in self.endpoint.split('/', 3)[2]:
            raise ValueError('编号只能出现在 URL 路径或查询参数中')
        if url.scheme == 'http' and url.hostname not in {'127.0.0.1', 'localhost', '::1'}:
            raise ValueError('非本机接口需要 HTTPS')
        if not self.revision.strip() or not self.name.strip():
            raise ValueError('请提供非空的数据版本与来源名称')
        if not math.isfinite(self.timeout) or not 0 < self.timeout <= 120 or not math.isfinite(self.rate) or not 0 <= self.rate <= 100:
            raise ValueError('超时须在 (0,120] 秒，请求速率须在 [0,100] 次/秒；0为不固定等待')
        if not 0 <= self.retries <= 5 or not 1 <= self.max_bytes <= MAX_BYTES:
            raise ValueError('重试次数须为0到5，单次响应上限不得超过32 MiB')
        if self.token_env and not os.environ.get(self.token_env):
            raise ValueError('指定的令牌环境变量为空')


class HttpSource:
    def __init__(self, config: SourceConfig):
        self.config = config
        self.token = os.environ.get(config.token_env, '') if config.token_env else ''
        # Credential changes are isolated without persisting the credential itself.
        self.key = hashlib.sha256((config.endpoint+'\0'+config.revision+'\0'+self.token).encode()).hexdigest()
        self.lock = threading.Lock()
        self.next_request = 0.0
        self.requests = 0
        self.active = self.peak_active = 0
        self.connections = {}
        self.ssl_context = None
        if urlsplit(config.endpoint).scheme == 'https':
            self.ssl_context = ssl.create_default_context()
            self.ssl_context.set_alpn_protocols(['http/1.1'])
        # Direct access avoids environment proxies leaking a local mock request.
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def acquire(self, cancel: threading.Event):
        while not cancel.is_set():
            with self.lock:
                now = time.monotonic()
                delay = self.next_request-now
                if delay <= 0:
                    self.next_request = now+1/self.config.rate if self.config.rate else now
                    self.requests += 1
                    self.active += 1
                    self.peak_active = max(self.peak_active, self.active)
                    return now
            cancel.wait(min(delay, 0.1))
        raise FetchFailure('cancelled')

    @contextmanager
    def open(self, request):
        if not self.config.keep_alive:
            with self.opener.open(request, timeout=self.config.timeout) as response:
                yield response
            return
        url = urlsplit(request.full_url)
        identity = threading.get_ident()
        connection = self.connections.get(identity)
        if connection is None:
            cls = HTTPSConnection if url.scheme == 'https' else HTTPConnection
            arguments = {'context': self.ssl_context} if url.scheme == 'https' else {}
            connection = cls(url.hostname, url.port, timeout=self.config.timeout, **arguments)
            with self.lock:
                self.connections[identity] = connection
        response = None
        try:
            connection.request('GET', (url.path or '/')+('?' + url.query if url.query else ''), headers=dict(request.header_items()))
            response = connection.getresponse()
            if response.status >= 300:
                raise HTTPError('', response.status, '', response.headers, None)
            yield response
        except BaseException:
            connection.close()
            with self.lock:
                self.connections.pop(identity, None)
            raise
        finally:
            if response is not None:
                response.close()

    def close(self):
        for connection in self.connections.values():
            connection.close()
        self.connections.clear()

    def fetch(self, identifier: int, cancel: threading.Event) -> tuple[bytes | None, dict]:
        started = time.perf_counter()
        events = []
        for attempt in range(1, self.config.retries+2):
            acquired = False
            retry_after = 0.0
            try:
                self.acquire(cancel)
                acquired = True
                headers = {'Accept': 'image/*', 'Accept-Encoding': 'identity', 'User-Agent': 'AvatarCompare/'+__version__}
                if self.token:
                    headers['Authorization'] = 'Bearer '+self.token
                request = Request(self.config.endpoint.format(id=identifier), headers=headers)
                with self.open(request) as response:
                    status = response.status
                    if status != 200:
                        raise FetchFailure('unexpected_status', status=status)
                    length = response.headers.get('Content-Length')
                    if length and (not length.isdecimal() or int(length) > self.config.max_bytes):
                        raise FetchFailure('response_too_large_or_invalid_length', status=status)
                    if response.headers.get('Content-Encoding', 'identity').lower() != 'identity':
                        raise FetchFailure('unsupported_content_encoding', status=status)
                    chunks, size = [], 0
                    deadline = time.monotonic()+self.config.timeout
                    while True:
                        if cancel.is_set():
                            raise FetchFailure('cancelled')
                        if time.monotonic() >= deadline:
                            raise FetchFailure('body_timeout', retryable=True)
                        chunk = response.read1(min(65536, self.config.max_bytes+1-size))
                        if not chunk:
                            break
                        chunks.append(chunk)
                        size += len(chunk)
                        if size > self.config.max_bytes:
                            raise FetchFailure('response_too_large', status=status)
                    if length and size != int(length):
                        raise FetchFailure('incomplete_response', status=status, retryable=True)
                events.append({'attempt': attempt, 'status': status, 'code': 'ok', 'time': time.time()})
                return b''.join(chunks), self.metadata(events, started, status, None)
            except HTTPError as error:
                status = error.code
                try:
                    value = error.headers.get('Retry-After', '0')
                    try:
                        retry_after = max(0.0, float(value))
                    except ValueError:
                        retry_after = max(0.0, parsedate_to_datetime(value).timestamp()-time.time())
                    if not math.isfinite(retry_after):
                        retry_after = 0.0
                except (ValueError, TypeError, OverflowError):
                    retry_after = 0.0
                error.close()
                failure = FetchFailure('http_error', status=status, retryable=status in {408, 429, 500, 502, 503, 504})
                if status in {429, 503}:
                    # Enforce cooldown for other workers even after this item's
                    # final attempt. A long server wait stops the run for resume.
                    with self.lock:
                        self.next_request = max(self.next_request, time.monotonic()+max(0.25, retry_after))
                    if retry_after > 120:
                        cancel.set()
            except (URLError, TimeoutError, ConnectionError, OSError, HTTPException):
                # Do not persist exception text: it may contain source URLs or credentials.
                failure = FetchFailure('transport_error', retryable=True)
            except FetchFailure as error:
                failure = error
            finally:
                if acquired:
                    with self.lock:
                        self.active -= 1
            events.append({'attempt': attempt, 'status': failure.status, 'code': failure.code, 'time': time.time()})
            if not failure.retryable or attempt > self.config.retries or cancel.is_set():
                return None, self.metadata(events, started, failure.status, failure.code)
            delay = max(retry_after, min(0.25*2**(attempt-1), 4.0))
            with self.lock:
                self.next_request = max(self.next_request, time.monotonic()+delay)
            cancel.wait(delay)
        raise AssertionError('unreachable')

    @staticmethod
    def metadata(events, started, status, error):
        return {'attempts': len(events), 'http_status': status, 'error_code': error, 'events': events,
                'fetch_ms': (time.perf_counter()-started)*1000, 'fetched_at': time.time()}
