from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import fields
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
from typing import Callable
import uuid
import zipfile

import cv2
import numpy as np

from .features import FEATURE_VERSION, Features, compare, extract, read_bytes
from .cache_lock import CacheLease
from .codec import compact_pack

COMPARE_VERSION = FEATURE_VERSION + ':geometry-gate-v2-distinct-locations'
STORAGE_VERSION = FEATURE_VERSION + ':npz-u8-deflate1-v1'
EXTENSIONS = {'.jpg', '.jpeg', '.png', '.webp', '.bmp', '.tif', '.tiff', '.gif'}


def signature(path: Path) -> str:
    stat = path.stat()
    return f'{stat.st_size}:{stat.st_mtime_ns}:{stat.st_ctime_ns}:{stat.st_ino}'


def pack(features: Features) -> bytes:
    metadata = {field.name: getattr(features, field.name) for field in fields(features) if field.name not in {'gray', 'points', 'descriptors', 'preview'}}
    stream = io.BytesIO()
    np.savez_compressed(stream, metadata=np.frombuffer(json.dumps(metadata).encode(), np.uint8), gray=features.gray, points=features.points, descriptors=features.descriptors, preview=np.frombuffer(features.preview, np.uint8))
    return stream.getvalue()


def unpack(blob: bytes) -> Features:
    with np.load(io.BytesIO(blob), allow_pickle=False) as arrays:
        metadata = json.loads(arrays['metadata'].tobytes())
        return Features(**metadata, gray=arrays['gray'].copy(), points=arrays['points'].copy(), descriptors=arrays['descriptors'].astype(np.float32,copy=True), preview=arrays['preview'].tobytes())


class Store:
    """Content-keyed features and comparisons, with one connection per worker."""
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lease = CacheLease(self.path.parent)
        self.lease.__enter__()
        self.local = threading.local()
        self.writer = None
        self.extractor = extract
        self.connections = []
        self.connection_lock = threading.Lock()
        try:
            db = self.db()
            db.executescript('''
            CREATE TABLE IF NOT EXISTS features(digest TEXT, version TEXT, phash TEXT, payload BLOB, PRIMARY KEY(digest,version));
            CREATE TABLE IF NOT EXISTS files(path TEXT PRIMARY KEY, signature TEXT, digest TEXT);
            CREATE TABLE IF NOT EXISTS pairs(query_digest TEXT, candidate_digest TEXT, version TEXT, metrics TEXT, PRIMARY KEY(query_digest,candidate_digest,version));
            CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY, target TEXT, folder TEXT, status TEXT, total INTEGER, updated REAL);
            CREATE TABLE IF NOT EXISTS results(run_id TEXT, path TEXT, digest TEXT, metrics TEXT, error TEXT, PRIMARY KEY(run_id,path));
            ''')
            db.commit()
        except BaseException:
            for connection in self.connections:
                connection.close()
            self.lease.close()
            raise

    def db(self) -> sqlite3.Connection:
        if not hasattr(self.local, 'db'):
            db = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('PRAGMA synchronous=FULL')
            db.row_factory = sqlite3.Row
            self.local.db = db
            with self.connection_lock:
                self.connections.append(db)
        if self.writer is not None:
            from .writer import BufferedConnection
            if not hasattr(self.local, 'proxy'):
                self.local.proxy = BufferedConnection(self.local.db, self.writer)
            return self.local.proxy
        return self.local.db

    def enable_shared_writer(self, capacity=256):
        from .writer import SharedWriter
        self.writer = SharedWriter(self.path, capacity=capacity)

    def feature_row(self, digest):
        return self.db().execute('''SELECT payload FROM features WHERE digest=? AND version IN (?,?)
            ORDER BY version=? DESC LIMIT 1''',(digest,STORAGE_VERSION,FEATURE_VERSION,STORAGE_VERSION)).fetchone()

    def close(self):
        try:
            if self.writer is not None:
                self.writer.close()
            for db in self.connections:
                db.close()
        finally:
            self.lease.close()

    def prepare_bytes(self, query: Features, data: bytes, *, retain_replay=False):
        """Compute from received bytes and return cache writes without committing."""
        started = time.perf_counter()
        digest = hashlib.sha256(data).hexdigest()
        statements = []
        row = self.feature_row(digest)
        features = None
        if row:
            try:
                features = unpack(row['payload'])
            except (ValueError, KeyError, OSError, zipfile.BadZipFile):
                pass
        hit = 'content' if features is not None else 'miss'
        if features is None:
            features = self.extractor(data)
        if hit == 'miss' or retain_replay:
            statements.append(('INSERT OR REPLACE INTO features VALUES(?,?,?,?)',
                (digest, STORAGE_VERSION, f'{features.phash:016x}', compact_pack(features))))
        cached = self.db().execute('SELECT metrics FROM pairs WHERE query_digest=? AND candidate_digest=? AND version=?',
            (query.digest, digest, COMPARE_VERSION)).fetchone()
        if cached:
            metrics = json.loads(cached['metrics'])
        else:
            exact = query.digest == features.digest or query.pixels == features.pixels
            metrics = compare(query, features, geometry=not exact)
            if exact:
                metrics['geometry'] = {'consistent': False, 'inliers': 0, 'ratio_matches': 0, 'inlier_ratio': 0.0,
                    'query_coverage': 0.0, 'candidate_coverage': 0.0, 'reason': 'exact_content_needs_no_geometry'}
        if not cached or retain_replay:
            statements.append(('INSERT OR REPLACE INTO pairs VALUES(?,?,?,?)',
                (query.digest, digest, COMPARE_VERSION, json.dumps(metrics))))
        return {'path': '', 'digest': digest, 'metrics': metrics, 'error': None, 'feature_cache': hit,
                'pair_cache': bool(cached), 'total_ms': (time.perf_counter()-started)*1000,
                'extraction_ms': features.elapsed_ms if hit == 'miss' else 0,
                'comparison_ms': 0 if cached else metrics.get('elapsed_ms', 0)}, statements

    def raw_bytes(self, acquisition):
        if acquisition.get('storage') == 'sqlite':
            row = self.db().execute('SELECT payload FROM raw_images WHERE digest=?', (acquisition['digest'],)).fetchone()
            data = bytes(row['payload']) if row else None
        else:
            try:
                data = read_bytes(Path(acquisition['path']))
            except (OSError, ValueError):
                return None
        return data if data is not None and hashlib.sha256(data).hexdigest() == acquisition['digest'] else None

    def get(self, path: Path) -> tuple[Features, str]:
        before = signature(path)
        db = self.db()
        row = db.execute('''SELECT f.payload FROM files p JOIN features f ON f.digest=p.digest
            AND f.version IN (?,?) WHERE p.path=? AND p.signature=? ORDER BY f.version=? DESC LIMIT 1''',
            (STORAGE_VERSION, FEATURE_VERSION, str(path), before, STORAGE_VERSION)).fetchone()
        if row:
            try:
                return unpack(row['payload']), 'stat'
            except (ValueError, KeyError, OSError, zipfile.BadZipFile):
                pass
        data = read_bytes(path)
        if signature(path) != before:
            raise ValueError('图片读取过程中发生变化，请重新运行')
        digest = hashlib.sha256(data).hexdigest()
        row = self.feature_row(digest)
        if row:
            try:
                features = unpack(row['payload'])
                hit = 'content'
            except (ValueError, KeyError, OSError, zipfile.BadZipFile):
                features, hit = self.extractor(data), 'miss'
        else:
            features, hit = self.extractor(data), 'miss'
        with db:
            if hit == 'miss':
                db.execute('INSERT OR REPLACE INTO features VALUES(?,?,?,?)', (digest, STORAGE_VERSION, f'{features.phash:016x}', compact_pack(features)))
            db.execute('INSERT OR REPLACE INTO files VALUES(?,?,?)', (str(path), before, digest))
        return features, hit

    def preview(self, digest: str) -> bytes:
        row = self.feature_row(digest)
        return unpack(row['payload']).preview if row else b''

    def evaluate(self, query: Features, path: Path, use_pair_cache=True) -> dict:
        started = time.perf_counter()
        try:
            features, hit = self.get(path)
            cached = self.db().execute('SELECT metrics FROM pairs WHERE query_digest=? AND candidate_digest=? AND version=?', (query.digest, features.digest, COMPARE_VERSION)).fetchone() if use_pair_cache else None
            if cached:
                metrics = json.loads(cached['metrics'])
                pair_hit = True
            else:
                exact = query.digest == features.digest or query.pixels == features.pixels
                metrics = compare(query, features, geometry=not exact)
                if exact:
                    metrics['geometry'] = {'consistent': False, 'inliers': 0, 'ratio_matches': 0, 'inlier_ratio': 0.0, 'query_coverage': 0.0, 'candidate_coverage': 0.0, 'reason': 'exact_content_needs_no_geometry'}
                with self.db():
                    self.db().execute('INSERT OR REPLACE INTO pairs VALUES(?,?,?,?)', (query.digest, features.digest, COMPARE_VERSION, json.dumps(metrics)))
                pair_hit = False
            return {'path': str(path), 'digest': features.digest, 'metrics': metrics, 'error': None, 'feature_cache': hit, 'pair_cache': pair_hit, 'total_ms': (time.perf_counter()-started)*1000,
                    'extraction_ms': features.elapsed_ms if hit == 'miss' else 0,
                    'comparison_ms': metrics.get('elapsed_ms', 0) if not pair_hit else 0}
        except Exception as error:
            return {'path': str(path), 'digest': '', 'metrics': None, 'error': f'{type(error).__name__}: {error}', 'feature_cache': 'error', 'pair_cache': False, 'total_ms': (time.perf_counter()-started)*1000}


def discover(folder: Path, *, exclude: list[Path]) -> tuple[list[Path], list[str]]:
    files, issues = [], []
    excluded = [path.resolve() for path in exclude]
    for base, dirs, names in os.walk(folder, followlinks=False, onerror=lambda error: issues.append(str(error))):
        base_path = Path(base)
        dirs[:] = sorted(name for name in dirs if not (base_path/name).is_symlink() and not getattr(base_path/name, 'is_junction', lambda: False)() and (base_path/name).resolve() not in excluded)
        for name in sorted(names):
            path = base_path/name
            if path.suffix.lower() not in EXTENSIONS or path.is_symlink():
                continue
            try:
                resolved = path.resolve(strict=True)
                if resolved.is_relative_to(folder) and not any(resolved.is_relative_to(item) for item in excluded):
                    files.append(resolved)
            except OSError as error:
                issues.append(f'{path}: {error}')
    return files, issues


def rank_key(row: dict):
    metric = row['metrics']
    if not metric:
        return (4, 0, 0, row['path'])
    geometry = metric['geometry']
    group = 0 if metric['exact_file'] else 1 if metric['exact_pixels'] else 2 if geometry['consistent'] else 3
    return (group, -geometry['inliers'], -metric['phash_agreement'], -metric['ssim'], row['path'])


def search(target: Path, folder: Path, state_dir: Path, *, workers=2, cancel: threading.Event | None = None, progress: Callable[[dict], None] | None = None, resume=True, pair_cache=True) -> dict:
    started = time.perf_counter()
    target, folder, state_dir = target.resolve(strict=True), folder.resolve(strict=True), state_dir.resolve()
    from .storage_profile import require_profile
    require_profile(state_dir,compact=False)
    if not folder.is_dir():
        raise ValueError('候选路径必须是图片文件夹')
    if not target.is_file():
        raise ValueError('目标路径必须是图片文件')
    if state_dir == folder or folder.is_relative_to(state_dir):
        raise ValueError('缓存目录不能包含整个候选文件夹')
    workers = max(1, min(int(workers), 4))
    cancel = cancel or threading.Event()
    progress = progress or (lambda event: None)
    # Avoid OpenCV internal threads multiplying the explicitly bounded worker pool.
    cv2.setNumThreads(1)
    store = Store(state_dir/'cache.sqlite3')
    pool = None
    try:
        store.enable_shared_writer()
        query, query_cache = store.get(target)
        paths, issues = discover(folder, exclude=[state_dir])
        paths = [path for path in paths if path != target]
        manifest = []
        for path in paths:
            try:
                manifest.append((str(path), signature(path)))
            except OSError as error:
                issues.append(str(error))
        paths = [Path(path) for path, _ in manifest]
        identity = json.dumps([COMPARE_VERSION, query.digest, str(folder), manifest], ensure_ascii=False).encode()
        run_id = hashlib.sha256(identity).hexdigest()[:24]
        if not resume:
            run_id += '-' + uuid.uuid4().hex[:8]
        db = store.db()
        with db:
            db.execute('INSERT OR REPLACE INTO runs VALUES(?,?,?,?,?,?)', (run_id, str(target), str(folder), 'running', len(paths), time.time()))
        saved = {row['path']: row for row in db.execute('SELECT * FROM results WHERE run_id=?', (run_id,))}
        rows = [{'path': row['path'], 'digest': row['digest'], 'metrics': json.loads(row['metrics']) if row['metrics'] else None, 'error': row['error'], 'feature_cache': 'resumed', 'pair_cache': True, 'total_ms': 0} for row in saved.values()]
        stats = {'total': len(paths), 'processed': len(rows), 'resumed': len(rows), 'feature_hits': 0, 'feature_misses': 0, 'pair_hits': 0, 'errors': sum(bool(row['error']) for row in rows), 'workers': workers, 'query_keypoints': len(query.points)}
        # Hash affects scheduling of already indexed files, never rejection.
        def priority(path):
            try:
                stamp = signature(path)
            except OSError:
                return (1, 64, str(path))
            row = db.execute('SELECT f.phash FROM files p JOIN features f ON f.digest=p.digest AND f.version=? WHERE p.path=? AND p.signature=?', (FEATURE_VERSION, str(path), stamp)).fetchone()
            return (0, (query.phash ^ int(row['phash'], 16)).bit_count(), str(path)) if row else (1, 64, str(path))
        todo = sorted((path for path in paths if str(path) not in saved), key=priority)
        progress({'stage': 'matching', **stats})
        pool = ThreadPoolExecutor(max_workers=workers)
        iterator = iter(todo)
        pending = {}
        for _ in range(workers * 2):
            path = next(iterator, None)
            if path is not None:
                pending[pool.submit(store.evaluate, query, path, pair_cache)] = path
        interrupted = False
        try:
            while pending and not cancel.is_set():
                completed, _ = wait(pending, timeout=0.1, return_when=FIRST_COMPLETED)
                for future in completed:
                    pending.pop(future)
                    row = future.result()
                    with db:
                        db.execute('INSERT OR REPLACE INTO results VALUES(?,?,?,?,?)', (run_id, row['path'], row['digest'], json.dumps(row['metrics']) if row['metrics'] else None, row['error']))
                    rows.append(row)
                    stats['processed'] += 1
                    stats['feature_hits'] += row['feature_cache'] in {'stat', 'content'}
                    stats['feature_misses'] += row['feature_cache'] == 'miss'
                    stats['pair_hits'] += row['pair_cache']
                    stats['errors'] += bool(row['error'])
                    progress({'stage': 'matching', 'path': row['path'], **stats})
                    if not cancel.is_set():
                        path = next(iterator, None)
                        if path is not None:
                            pending[pool.submit(store.evaluate, query, path, pair_cache)] = path
        except KeyboardInterrupt:
            interrupted = True
            cancel.set()
        finally:
            for future in pending:
                future.cancel()
            pool.shutdown(wait=True, cancel_futures=True)
            pool = None
        status = 'stopped' if cancel.is_set() or interrupted else 'complete'
        with db:
            db.execute('UPDATE runs SET status=?,updated=? WHERE id=?', (status, time.time(), run_id))
        rows.sort(key=rank_key)
        stats.update(elapsed_seconds=time.perf_counter()-started, query_cache=query_cache, consistent=sum(bool(row['metrics'] and (row['metrics']['exact_file'] or row['metrics']['exact_pixels'] or row['metrics']['geometry']['consistent'])) for row in rows))
        return {'run_id': run_id, 'target': str(target), 'target_digest': query.digest, 'folder': str(folder), 'state_dir': str(state_dir), 'status': status, 'stats': stats, 'issues': issues, 'results': rows, 'algorithm': COMPARE_VERSION}
    finally:
        if pool is not None:
            pool.shutdown(wait=True, cancel_futures=True)
        store.close()
