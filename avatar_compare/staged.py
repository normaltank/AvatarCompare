"""Independent transfer/compute/commit stages with byte-bounded backpressure."""
from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import dataclass
import hashlib
from heapq import nsmallest
import json
import multiprocessing
import os
from pathlib import Path
import threading
import time

import cv2
from concurrent.futures.process import BrokenProcessPool

from .engine import Store, COMPARE_VERSION
from .ledger import RangeLedger, consistent
from .pipeline_io import ByteBudget, ThreadTransfers
from .range_engine import range_rank_key, validate_range
from .source import HttpSource
from .storage_profile import require_profile,range_specs_schema,save_range_spec


@dataclass
class Prepared:
    packet: object
    row: dict
    statements: list


def search_staged(target, start, end, state_dir, config, *, workers=2, download_workers=None,
                  cancel=None, progress=None, retry_errors=False, top_limit=100,
                  compute_backend='thread', buffer_bytes=256*1024*1024,
                  worker_observer=None, share_computations=False, compact=False,
                  on_top_matches=None, **compatibility):
    """Scan a range and optionally publish the top strong matches after commit."""
    validate_range(start,end)
    downloads = workers if download_workers is None else download_workers
    if not 1 <= workers <= max(4,min(os.cpu_count() or 1,256)) or not 1 <= downloads <= 256:
        raise ValueError('计算或下载并行数超出本机允许范围')
    if not 1 <= top_limit <= 100 or buffer_bytes < config.max_bytes:
        raise ValueError('候选数量须为1到100，缓冲上限不得小于单响应上限')
    if compute_backend not in {'thread','process'}:
        raise ValueError('不支持的计算或下载后端')
    target, state_dir = Path(target).resolve(strict=True), Path(state_dir).resolve()
    require_profile(state_dir,compact=compact)
    cancel = cancel if cancel is not None else threading.Event()
    progress = progress or (lambda event:None)
    cv2.setNumThreads(1)
    started = time.perf_counter()
    budget = ByteBudget(buffer_bytes)
    store = Store(state_dir/'cache.sqlite3')
    transfers = cpu = processes = None
    net, computing, commits = {}, {}, {}
    ready, prepared = deque(), []
    process_ids, process_guard = set(), threading.Lock()
    run_id = None
    sharing = None
    try:
        db = store.db()
        db.executescript('''
            CREATE TABLE IF NOT EXISTS acquisitions(source TEXT,identifier TEXT,payload TEXT,PRIMARY KEY(source,identifier));
            CREATE TABLE IF NOT EXISTS raw_images(digest TEXT PRIMARY KEY,payload BLOB);
        ''')
        db.commit()
        store.enable_shared_writer(capacity=max(32,downloads*2))
        db = store.db()
        if compact:
            from .compact_ledger import schema,CompactLedger
            schema(db)
        range_specs_schema(db)
        query, query_cache = store.get(target)
        if share_computations:
            from .shared_work import SharedComparisons
            sharing = SharedComparisons(lambda query,data:store.prepare_bytes(query,data,retain_replay=compact))
        identity = HttpSource(config)
        source_key = identity.key
        identity.close()
        run_id = hashlib.sha256(json.dumps(['range-compact-v1' if compact else 'range-v3-staged',COMPARE_VERSION,query.digest,source_key,start,end]).encode()).hexdigest()[:24]
        ledger = CompactLedger(db,store.writer,run_id,query.digest,source_key,COMPARE_VERSION,start,end) if compact else None
        with db:
            db.execute('INSERT OR REPLACE INTO runs VALUES(?,?,?,?,?,?)',
                       (run_id,str(target),source_key,'running',end-start+1,time.time()))
            save_range_spec(db,run_id,query.digest,source_key,COMPARE_VERSION,start,end,'compact' if compact else 'all',config)
        if not compact:ledger = RangeLedger(db,run_id)
        if retry_errors:
            ledger.retry_errors(source_key)
        totals = ledger.totals()
        stats = {'total':end-start+1,'processed':totals['processed'],'resumed':totals['processed'],
                 'errors':totals['errors'],'consistent':totals['consistent'],'workers':workers,
                 'download_workers':downloads,'feature_hits':0,'feature_misses':0,'pair_hits':0,
                 'download_cache_hits':0,'query_keypoints':len(query.points),'requests':0,
                 'peak_in_flight':0,'peak_ready':0,'peak_compute':0,'peak_commit_batches':0,
                 'buffer_limit_bytes':buffer_bytes,'query_cache':query_cache,'compute_backend':compute_backend,
                 'transport':'thread','sum_fetch_ms':0,'sum_compute_ms':0,'sum_extraction_ms':0,
                 'sum_comparison_ms':0,'pipeline':'staged'}
        published_matches = []
        if on_top_matches and totals['consistent']:
            published_matches = [row for row in ledger.top(top_limit) if consistent(row)]
            on_top_matches(run_id, published_matches)
        if totals['blocked']:
            cancel.set()
        def emit(stage):
            elapsed = time.perf_counter()-started
            done = stats['processed']-stats['resumed']
            speed = done/elapsed if elapsed else 0
            progress({'stage':stage,**stats,'elapsed_seconds':elapsed,'items_per_second':speed,
                'eta_seconds':(stats['total']-stats['processed'])/speed if speed else None,
                'requests':transfers.requests if transfers else 0,'buffer_reserved_bytes':budget.used})

        if stats['processed'] < stats['total'] and not cancel.is_set():
            transfers = ThreadTransfers(config,downloads,store,budget,cancel)
            cpu = ThreadPoolExecutor(max_workers=workers,thread_name_prefix='avatar-compute')
            if compute_backend == 'process':
                from .parallel import extract_in_process, initialize_cpu
                processes = ProcessPoolExecutor(max_workers=workers,mp_context=multiprocessing.get_context('spawn'),initializer=initialize_cpu)
                def process_extract(data):
                    pid, features = processes.submit(extract_in_process,data).result()
                    with process_guard:
                        process_ids.add(pid)
                    if worker_observer:
                        worker_observer(pid)
                    return features
                store.extractor = process_extract

        def prepare(packet):
            before = time.perf_counter()
            row = {'path':'','digest':'','metrics':None,'error':packet.fetch['error_code'],
                   'feature_cache':'error','pair_cache':False}
            statements = []
            data = packet.data
            digest = hashlib.sha256(data).hexdigest() if data is not None else ''
            if data is not None:
                try:
                    row, statements = sharing.prepare(query,data) if sharing else store.prepare_bytes(query,data,retain_replay=compact)
                except (BrokenProcessPool, MemoryError, BrokenPipeError):
                    raise
                except (ValueError,OSError,cv2.error) as error:
                    row.update(digest=digest,error=f'{type(error).__name__}: {error}')
                storage = packet.acquisition.get('storage','file') if packet.cached and packet.acquisition else 'sqlite'
                path = packet.acquisition.get('path','') if storage == 'file' and packet.acquisition else ''
                if not packet.cached or compact:
                    statements.append(('''INSERT INTO raw_images VALUES(?,?) ON CONFLICT(digest)
                        DO UPDATE SET payload=excluded.payload WHERE raw_images.payload IS NOT excluded.payload''',(digest,data)))
                row.update(path=path,asset_kind=storage,raw_size=len(data),digest=digest)
            else:
                storage,path = 'sqlite',''
            acquisition = {'identifier':str(packet.identifier),'digest':digest,'path':path,
                           'storage':storage,'raw_size':len(data) if data is not None else 0,
                           'error':packet.fetch['error_code'],'fetch':packet.fetch}
            statements.append(('INSERT OR REPLACE INTO acquisitions VALUES(?,?,?)',
                               (source_key,str(packet.identifier),json.dumps(acquisition))))
            row.update(identifier=str(packet.identifier),fetch=packet.fetch,download_cache=packet.cached,
                       network_ms=0 if packet.cached else packet.fetch['fetch_ms'],
                       compute_ms=(time.perf_counter()-before)*1000,total_ms=(time.perf_counter()-before)*1000)
            return Prepared(packet,row,statements)

        def submit_commit():
            if not prepared or len(commits) >= 4:
                return
            batch = prepared[:64]
            del prepared[:len(batch)]
            if compact:
                commits[ledger.commit(batch)] = (batch,list(range(len(batch))))
                return
            statements, offsets = [], []
            seen = set()
            for item in batch:
                for sql,parameters in item.statements:
                    key = None
                    if sharing:
                        if sql.startswith('INSERT OR REPLACE INTO features'):key=(sql,*parameters[:2])
                        elif sql.startswith('INSERT OR REPLACE INTO pairs'):key=(sql,*parameters[:3])
                        elif sql.startswith('INSERT INTO raw_images'):key=(sql,parameters[0])
                    if key is not None:
                        if key in seen:continue
                        seen.add(key)
                    statements.append((sql,parameters))
                offsets.append(len(statements))
                statements.extend(ledger.statements([item.row]))
            commits[store.writer.submit_future(statements)] = (batch,offsets)

        iterator = ledger.pending(start,end)
        exhausted = False
        emit('matching')
        last_emit = time.monotonic()
        last_flush = time.monotonic()
        interrupted = False
        while True:
            try:
                if cancel.is_set():
                    while ready:
                        ready.popleft().release()
                    for future in net:
                        future.cancel()
                else:
                    while ready and len(computing) < workers and len(prepared) < 128 and len(commits) < 4:
                        packet = ready.popleft()
                        computing[cpu.submit(prepare,packet)] = packet
                    while transfers and not exhausted and len(net) < downloads and len(ready)+len(net) < downloads+workers*2:
                        identifier = next(iterator,None)
                        if identifier is None:
                            exhausted = True
                            break
                        net[transfers.submit(identifier)] = identifier
                if prepared and (len(prepared)>=64 or cancel.is_set() or budget.used>=budget.limit
                                 or not (net or computing or ready) or time.monotonic()-last_flush>=0.02):
                    if len(commits)<4:
                        submit_commit()
                        last_flush=time.monotonic()
                stats['peak_ready'] = max(stats['peak_ready'],len(ready))
                stats['peak_compute'] = max(stats['peak_compute'],len(computing))
                stats['peak_commit_batches'] = max(stats['peak_commit_batches'],len(commits))
                stats['peak_in_flight'] = max(stats['peak_in_flight'],len(net)+len(ready)+len(computing)+len(prepared)+sum(len(b) for b,_ in commits.values()))
                futures = set(net)|set(computing)|set(commits)
                if not futures and not ready and not prepared:
                    break
                timeout=min(0.05,max(0.001,0.02-(time.monotonic()-last_flush))) if prepared else 0.05
                completed,_ = wait(futures,timeout=timeout,return_when=FIRST_COMPLETED)
                for future in completed:
                    if future in net:
                        net.pop(future)
                        if future.cancelled():
                            continue
                        packet = future.result()
                        if packet.fetch['error_code'] == 'cancelled':
                            packet.release()
                            continue
                        if packet.fetch['http_status'] in {401,403,429}:
                            cancel.set()
                            # Persist the stop reason even though no further work is admitted.
                            prepared.append(prepare(packet))
                        elif cancel.is_set():
                            packet.release()
                        else:
                            ready.append(packet)
                    elif future in computing:
                        computing.pop(future)
                        prepared.append(future.result())
                    else:
                        batch,offsets = commits.pop(future)
                        counts = future.result()
                        new_matches = []
                        for item,offset in zip(batch,offsets):
                            row = item.row
                            if counts[offset]:
                                stats['processed'] += 1
                                stats['errors'] += bool(row['error'])
                                stats['feature_hits'] += row['feature_cache'] in {'stat','content'}
                                stats['feature_misses'] += row['feature_cache'] == 'miss'
                                stats['pair_hits'] += row['pair_cache']
                                stats['download_cache_hits'] += row['download_cache']
                                for name in ['extraction_ms','comparison_ms','compute_ms']:
                                    stats['sum_'+name] += row.get(name,0)
                                stats['sum_fetch_ms'] += row['network_ms']
                                if on_top_matches and consistent(row):
                                    new_matches.append(row)
                            item.packet.release()
                            if sharing and not compact and row['metrics'] and not row['error']:
                                sharing.committed(query.digest,row['digest'])
                        if new_matches:
                            matches = nsmallest(top_limit, published_matches + new_matches, key=range_rank_key)
                            if matches != published_matches:
                                published_matches = matches
                                on_top_matches(run_id, published_matches)
                        emit('matching')
                if time.monotonic()-last_emit >= 0.5:
                    emit('matching');last_emit=time.monotonic()
            except KeyboardInterrupt:
                interrupted = True
                cancel.set()
        status = 'stopped' if cancel.is_set() or interrupted else 'complete'
        with db:
            db.execute('UPDATE runs SET status=?,updated=? WHERE id=?',(status,time.time(),run_id))
        final = ledger.totals()
        stats.update(processed=final['processed'],errors=final['errors'],consistent=final['consistent'],
                     elapsed_seconds=time.perf_counter()-started,requests=transfers.requests if transfers else 0,
                     peak_active_requests=transfers.peak if transfers else 0,compute_processes=len(process_ids),
                     retained_results=min(top_limit,final['processed']),buffer_peak_reserved_bytes=budget.peak,
                     buffer_reserved_bytes_at_finish=budget.used,
                     writer_commits=store.writer.commits,writer_submissions=store.writer.submissions,
                     writer_peak_batch=store.writer.peak_batch)
        if sharing:stats.update(sharing.snapshot())
        if compact:
            stats.update(retained_evidence=final['retained'],discarded_unmatched=final['discarded'],
                         coverage_intervals=db.execute('SELECT count(*) FROM compact_done WHERE scope=?',(ledger.scope,)).fetchone()[0])
            stats['retained_results']=len(ledger.top(top_limit))
        emit(status)
        return {'run_id':run_id,'target':str(target),'target_digest':query.digest,'folder':str(state_dir/'downloads'/source_key),
            'state_dir':str(state_dir),'status':status,'stats':stats,'issues':[],'results':ledger.top(top_limit),
            'algorithm':COMPARE_VERSION,'source':{'name':config.name,'revision':config.revision,'fingerprint':source_key},
            'range':{'start':str(start),'end':str(end),'inclusive':True},'result_storage':'disk',
            'result_database':str(state_dir/'cache.sqlite3'),'pipeline':'staged',
            'retention_mode':'compact' if compact else 'all',
            **({'scope_id':ledger.scope,'records_complete':False,'review_limit':100} if compact else {})}
    except BaseException:
        cancel.set()
        if run_id:
            try:
                with store.db():
                    store.db().execute('UPDATE runs SET status=?,updated=? WHERE id=?',('failed',time.time(),run_id))
            except Exception:
                pass
        raise
    finally:
        budget.close()
        if transfers:
            transfers.close()
        if cpu:
            cpu.shutdown(wait=True,cancel_futures=True)
        if processes:
            processes.shutdown(wait=True,cancel_futures=True)
        # Submitted commit futures are not cancelled; close() drains acknowledged writes.
        store.close()
        budget.release_all()
