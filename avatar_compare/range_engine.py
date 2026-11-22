"""Number range acquisition and matching, sharing the existing content/pair cache."""
from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, ProcessPoolExecutor, wait
import hashlib
import heapq
import json
import os
import multiprocessing
from pathlib import Path
import threading
import time
import uuid

import cv2

from .engine import COMPARE_VERSION, Store, rank_key
from .features import read_bytes
from .source import HttpSource, SourceConfig
from .ledger import RangeLedger, consistent
from .storage_profile import require_profile,range_specs_schema,save_range_spec


def validate_range(start: int, end: int):
    if isinstance(start, bool) or isinstance(end, bool) or not isinstance(start, int) or not isinstance(end, int):
        raise ValueError('起止编号须为整数')
    if not 0 <= start <= end <= 2**63-1:
        raise ValueError('范围须满足 0 ≤ 起始编号 ≤ 结束编号 ≤ 9223372036854775807（包含两端）')
    if end-start+1 > 2**63-1:
        raise ValueError('范围总项数超过有符号64位整数上限')


def search_range(target: Path, start: int, end: int, state_dir: Path, config: SourceConfig, *,
                 workers=2, cancel=None, progress=None, retry_errors=False,
                 download_workers=None, stream_results=False, top_limit=100, shared_writer=False,
                 compute_backend='thread', worker_observer=None) -> dict:
    validate_range(start, end)
    max_compute = max(4, min(os.cpu_count() or 1, 256))
    if workers not in range(1, max_compute+1):
        raise ValueError(f'计算并行数须为1到{max_compute}（按本机逻辑CPU数）')
    download_workers = workers if download_workers is None else download_workers
    if download_workers not in range(1, 257) or not 1 <= top_limit <= 100:
        raise ValueError('下载并行数须为1到256，候选保留数须为1到100')
    if compute_backend not in {'thread', 'process'}:
        raise ValueError('计算模式须为thread或process')
    started = time.perf_counter()
    cancel = cancel if cancel is not None else threading.Event()
    progress = progress or (lambda event: None)
    target, state_dir = target.resolve(strict=True), state_dir.resolve()
    require_profile(state_dir,compact=False)
    source = HttpSource(config)
    cv2.setNumThreads(1)
    store = Store(state_dir/'cache.sqlite3')
    db = store.db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS acquisitions(source TEXT, identifier TEXT, payload TEXT,
            PRIMARY KEY(source,identifier));
        CREATE TABLE IF NOT EXISTS range_results(run_id TEXT, identifier TEXT, payload TEXT,
            PRIMARY KEY(run_id,identifier));
    ''')
    db.commit()
    pool = None
    process_pool = None
    process_ids = set()
    asset_lock = threading.Lock()
    compute_limit = threading.Semaphore(workers)
    compute_lock = threading.Lock()
    compute_active = compute_peak = 0
    try:
        # Every write uses one owner, including ordinary/legacy range mode.
        store.enable_shared_writer(capacity=download_workers*2+4)
        db = store.db()
        range_specs_schema(db)
        query, query_cache = store.get(target)
        run_id = hashlib.sha256(json.dumps(['range-v2-stream' if stream_results else 'range-v1', COMPARE_VERSION, query.digest, source.key, start, end]).encode()).hexdigest()[:24]
        ledger = RangeLedger(db, run_id) if stream_results else None
        failed = [row['identifier'] for row in db.execute('SELECT identifier,payload FROM range_results WHERE run_id=?', (run_id,))
                  if json.loads(row['payload'])['error']] if retry_errors and not ledger else []
        with db:
            db.execute('INSERT OR REPLACE INTO runs VALUES(?,?,?,?,?,?)', (run_id, str(target), source.key, 'running', end-start+1, time.time()))
            save_range_spec(db,run_id,query.digest,source.key,COMPARE_VERSION,start,end,'all',config)
            if retry_errors and not ledger:
                for identifier in failed:
                    db.execute('DELETE FROM range_results WHERE run_id=? AND identifier=?', (run_id, identifier))
                    db.execute('DELETE FROM acquisitions WHERE source=? AND identifier=?', (source.key, identifier))
        if ledger and retry_errors:
            ledger.retry_errors(source.key)
        rows = ledger.top(top_limit) if ledger else []
        saved = set()
        # A local asset may have been removed/corrupted; validate it before reusing a completed row.
        for record in (() if ledger else db.execute('SELECT identifier,payload FROM range_results WHERE run_id=?', (run_id,))):
            row = json.loads(record['payload'])
            if not row['error'] and not valid_asset(row):
                continue
            row.update(feature_cache='resumed', pair_cache=bool(row['metrics']), total_ms=0)
            rows.append(row)
            saved.add(int(record['identifier']))
        totals = ledger.totals() if ledger else {'processed': len(rows), 'errors': sum(bool(row['error']) for row in rows), 'consistent': sum(consistent(row) for row in rows)}
        stats = {'total': end-start+1, 'processed': totals['processed'], 'resumed': totals['processed'],
                 'feature_hits': 0, 'feature_misses': 0, 'pair_hits': 0,
                 'errors': totals['errors'], 'workers': workers, 'download_workers': download_workers,
                 'query_keypoints': len(query.points), 'download_cache_hits': 0,
                 'requests': 0, 'peak_in_flight': 0}
        stats.update(sum_fetch_ms=0, sum_acquisition_ms=0, sum_compute_wait_ms=0, sum_compute_ms=0,
                     sum_extraction_ms=0, sum_comparison_ms=0, sum_result_commit_ms=0)
        if totals.get('blocked') or any(row['fetch']['http_status'] in {401, 403} for row in rows):
            cancel.set()
        if compute_backend == 'process' and stats['processed'] < stats['total'] and not cancel.is_set():
            from .parallel import extract_in_process, initialize_cpu
            process_pool = ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context('spawn'), initializer=initialize_cpu)
            def process_extract(data):
                pid, features = process_pool.submit(extract_in_process, data).result()
                with compute_lock:
                    process_ids.add(pid)
                if worker_observer:
                    worker_observer(pid)
                return features
            store.extractor = process_extract

        def evaluate(identifier):
            nonlocal compute_active, compute_peak
            before = time.perf_counter()
            record = store.db().execute('SELECT payload FROM acquisitions WHERE source=? AND identifier=?', (source.key, str(identifier))).fetchone()
            acquisition = json.loads(record['payload']) if record else None
            if acquisition and not acquisition['error'] and acquisition.get('storage') == 'sqlite':
                data = store.raw_bytes(acquisition)
                if data is not None:
                    digest = acquisition['digest']
                    path = state_dir/'downloads'/source.key/digest[:2]/digest[2:4]/(digest+'.img')
                    path.parent.mkdir(parents=True,exist_ok=True)
                    with asset_lock:
                        if not valid_asset({'path':str(path),'digest':digest}):
                            temporary = path.with_suffix('.'+uuid.uuid4().hex+'.tmp')
                            try:
                                temporary.write_bytes(data)
                                temporary.replace(path)
                            finally:
                                temporary.unlink(missing_ok=True)
                    acquisition = {**acquisition,'path':str(path)}
                del data
            cached = bool(acquisition and (acquisition['error'] or valid_asset(acquisition)))
            if not cached:
                data, fetch = source.fetch(identifier, cancel)
                acquisition = {'identifier': str(identifier), 'path': '', 'digest': '', 'error': fetch['error_code'], 'fetch': fetch}
                if data is not None:
                    digest = hashlib.sha256(data).hexdigest()
                    # Shard new large-mode blobs instead of putting billions of files in one directory.
                    path = state_dir/'downloads'/source.key/digest[:2]/digest[2:4]/(digest+'.img') if stream_results else state_dir/'downloads'/source.key/(digest+'.img')
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with asset_lock:
                        if not valid_asset({'path': str(path), 'digest': digest}):
                            temporary = path.with_suffix('.'+uuid.uuid4().hex+'.tmp')
                            try:
                                temporary.write_bytes(data)
                                temporary.replace(path)
                            finally:
                                temporary.unlink(missing_ok=True)
                    acquisition.update(path=str(path), digest=digest)
                del data
                if fetch['error_code'] != 'cancelled':
                    with store.db():
                        store.db().execute('INSERT OR REPLACE INTO acquisitions VALUES(?,?,?)', (source.key, str(identifier), json.dumps(acquisition)))
            compute_started = time.perf_counter()
            if acquisition['error']:
                row = {'path': acquisition['path'], 'digest': '', 'metrics': None, 'error': acquisition['error'], 'feature_cache': 'error', 'pair_cache': False}
                compute_wait_ms = compute_ms = 0
            else:
                with compute_limit:
                    compute_wait_ms = (time.perf_counter()-compute_started)*1000
                    matching_started = time.perf_counter()
                    with compute_lock:
                        compute_active += 1
                        compute_peak = max(compute_peak, compute_active)
                    try:
                        row = store.evaluate(query, Path(acquisition['path']))
                    finally:
                        with compute_lock:
                            compute_active -= 1
                    compute_ms = (time.perf_counter()-matching_started)*1000
            fetch_ms = 0 if cached else acquisition['fetch']['fetch_ms']
            row.update(identifier=str(identifier), fetch=acquisition['fetch'], download_cache=cached,
                       network_ms=fetch_ms, acquisition_ms=max(0, (compute_started-before)*1000-fetch_ms),
                       compute_wait_ms=compute_wait_ms, compute_ms=compute_ms,
                       total_ms=(time.perf_counter()-before)*1000)
            return row

        def emit(stage):
            elapsed = time.perf_counter()-started
            done = stats['processed']-stats['resumed']
            speed = done/elapsed if elapsed else 0
            progress({'stage': stage, **stats, 'requests': source.requests,
                      'elapsed_seconds': elapsed, 'items_per_second': speed,
                      'eta_seconds': (stats['total']-stats['processed'])/speed if speed else None})

        def persist(row, *, already_saved=False):
            nonlocal rows
            if row['error'] == 'cancelled':
                return
            commit_started = time.perf_counter()
            if ledger:
                if not already_saved and not ledger.save(row):
                    return
                rows = heapq.nsmallest(top_limit, rows+[row], key=range_rank_key)
            else:
                with db:
                    db.execute('INSERT OR REPLACE INTO range_results VALUES(?,?,?)', (run_id, row['identifier'], json.dumps(row)))
                rows.append(row)
            stats['sum_result_commit_ms'] += (time.perf_counter()-commit_started)*1000
            for metric, field in [('fetch', 'network_ms'), ('acquisition', 'acquisition_ms'), ('compute_wait', 'compute_wait_ms'),
                                  ('compute', 'compute_ms'), ('extraction', 'extraction_ms'), ('comparison', 'comparison_ms')]:
                stats['sum_'+metric+'_ms'] += row.get(field, 0)
            stats['retained_results'] = len(rows)
            stats['processed'] += 1
            stats['errors'] += bool(row['error'])
            stats['feature_hits'] += row['feature_cache'] in {'stat', 'content'}
            stats['feature_misses'] += row['feature_cache'] == 'miss'
            stats['pair_hits'] += row['pair_cache']
            stats['download_cache_hits'] += row['download_cache']

        emit('matching')
        iterator = ledger.pending(start, end) if ledger else (number for number in range(start, end+1) if number not in saved)
        pool = ThreadPoolExecutor(max_workers=download_workers)
        pending = {}

        def fill():
            while not cancel.is_set() and len(pending) < download_workers*2:
                identifier = next(iterator, None)
                if identifier is None:
                    break
                pending[pool.submit(evaluate, identifier)] = identifier
            stats['peak_in_flight'] = max(stats['peak_in_flight'], len(pending))

        interrupted = False
        last_emit = time.monotonic()
        try:
            fill()
            while pending and not cancel.is_set():
                completed, _ = wait(pending, timeout=0.1, return_when=FIRST_COMPLETED)
                for future in completed:
                    pending.pop(future)
                completed_rows = [future.result() for future in completed]
                if ledger and shared_writer:
                    durable_rows = [row for row in completed_rows if row['error'] != 'cancelled']
                    commit_started = time.perf_counter()
                    changed = ledger.save_many(durable_rows) if durable_rows else []
                    stats['sum_result_commit_ms'] += (time.perf_counter()-commit_started)*1000
                    completed_rows = [row for row, saved_row in zip(durable_rows, changed) if saved_row]
                for row in completed_rows:
                    persist(row, already_saved=bool(ledger and shared_writer))
                    # Authorization failures stop this run rather than consuming the range.
                    if row['fetch']['http_status'] in {401, 403} or (ledger and row['fetch']['http_status'] == 429):
                        cancel.set()
                    emit('matching')
                if time.monotonic()-last_emit >= 0.5:
                    emit('matching')
                    last_emit = time.monotonic()
                fill()
        except KeyboardInterrupt:
            interrupted = True
            cancel.set()
        finally:
            for future in pending:
                future.cancel()
            # Network socket timeout and bounded retries bound shutdown latency.
            pool.shutdown(wait=True, cancel_futures=True)
            pool = None
            for future in pending:
                if not future.cancelled():
                    persist(future.result())
        status = 'stopped' if cancel.is_set() or interrupted else 'complete'
        with db:
            db.execute('UPDATE runs SET status=?,updated=? WHERE id=?', (status, time.time(), run_id))
        if ledger:
            # SIGINT can arrive just after a durable commit but before Python
            # updates its counters/list. The committed ledger is authoritative.
            final_totals = ledger.totals()
            stats.update(processed=final_totals['processed'], errors=final_totals['errors'])
            rows = ledger.top(top_limit)
        rows.sort(key=range_rank_key)
        stats.update(elapsed_seconds=time.perf_counter()-started, query_cache=query_cache,
                     requests=source.requests, peak_active_requests=source.peak_active,
                     consistent=ledger.totals()['consistent'] if ledger else sum(consistent(row) for row in rows),
                     peak_compute=compute_peak, retained_results=len(rows),
                     compute_backend=compute_backend, compute_processes=len(process_ids))
        if store.writer is not None:
            stats.update(writer_commits=store.writer.commits, writer_submissions=store.writer.submissions,
                         writer_peak_batch=store.writer.peak_batch)
        emit(status)
        return {'run_id': run_id, 'target': str(target), 'target_digest': query.digest,
                'folder': str(state_dir/'downloads'/source.key), 'state_dir': str(state_dir),
                'status': status, 'stats': stats, 'issues': [], 'results': rows, 'algorithm': COMPARE_VERSION,
                'source': {'name': config.name, 'revision': config.revision, 'fingerprint': source.key},
                'result_storage': 'disk' if ledger else 'inline',
                'result_database': str(state_dir/'cache.sqlite3') if ledger else None,
                'range': {'start': str(start), 'end': str(end), 'inclusive': True}}
    finally:
        if pool is not None:
            cancel.set()
            pool.shutdown(wait=True, cancel_futures=True)
        if process_pool is not None:
            process_pool.shutdown(wait=True, cancel_futures=True)
        source.close()
        store.close()


def range_rank_key(row):
    key = rank_key(row)
    return key[:-1]+(int(row['identifier']),)


def valid_asset(row):
    try:
        return bool(row['path'] and hashlib.sha256(read_bytes(Path(row['path']))).hexdigest() == row['digest'])
    except (OSError, ValueError):
        return False
