"""Stepped hardware saturation probe with native counters and unchanged evidence rules."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import json
import os
import multiprocessing
import signal
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Outer workers own CPU parallelism. Avoid a separate BLAS thread team (and
# reserved stacks) in every spawned process; this does not alter SIFT rules.
for variable in ['OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'OMP_NUM_THREADS']:
    os.environ[variable] = '1'

import cv2
from avatar_compare.features import extract, compare
from avatar_compare.mock_service import MockServer, encode, synthetic_picture
from avatar_compare.range_engine import search_range
from avatar_compare.resources import ResourceMonitor
from avatar_compare.source import SourceConfig

_PROCESS_QUERY = None


def initialize_process(query_data, ready, gate):
    global _PROCESS_QUERY
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    cv2.setNumThreads(1)
    _PROCESS_QUERY = extract(query_data)
    ready.put(os.getpid())
    gate.wait()


def process_ping():
    return os.getpid()


def process_batch(payloads):
    count = 0
    for payload in payloads:
        metric = compare(_PROCESS_QUERY, extract(payload))
        count += bool(metric['exact_file'] or metric['exact_pixels'] or metric['geometry']['consistent'])
    return len(payloads), count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--count', type=int, default=192)
    parser.add_argument('--repeats', type=int, default=1)
    parser.add_argument('--compute-only', action='store_true')
    parser.add_argument('--process-compute', action='store_true')
    parser.add_argument('--shared-writer', action='store_true')
    parser.add_argument('--process-extract', action='store_true')
    parser.add_argument('--staged', action='store_true')
    parser.add_argument('--configs', default='2:16,4:32,8:64,16:128,32:128')
    args = parser.parse_args()
    if args.output.exists() and any(args.output.iterdir()):
        parser.error('测速输出目录必须为空；请换新目录，避免已有缓存使冷启动测量失真')
    args.output.mkdir(parents=True, exist_ok=True)
    configs = [tuple(map(int, pair.split(':'))) for pair in args.configs.split(',')]
    images = {1001+i: encode(synthetic_picture(17 if i == 0 else 2000+i).resize((100, 100))) for i in range(args.count)}
    target = args.output/'target.png'
    target.write_bytes(images[1001])
    results = []
    cv2.setNumThreads(1)
    started = time.time()
    def save():
        (args.output/'resources.json').write_text(json.dumps({'logical_cpus': os.cpu_count(), 'count': args.count,
            'started': started, 'scope': 'Synthetic100px images; loopback server CPU is included. Process I/O counters include cached I/O, not physical disk busy%.',
            'samples': results}, ensure_ascii=False, indent=2), encoding='utf-8')
    if args.process_compute:
        payloads = list(images.values())*2
        chunks = [payloads[index:index+32] for index in range(0, len(payloads), 32)]
        for repeat in range(args.repeats):
            for workers, _ in configs[repeat%len(configs):]+configs[:repeat%len(configs)]:
                context = multiprocessing.get_context('spawn')
                ready = context.Queue()
                gate = context.Event()
                print(json.dumps({'mode': 'process-startup', 'workers': workers}), flush=True)
                startup = time.perf_counter()
                with ProcessPoolExecutor(max_workers=workers, mp_context=context, initializer=initialize_process,
                                         initargs=(images[1001], ready, gate)) as pool:
                    warmup = [pool.submit(process_ping) for _ in range(workers)]
                    try:
                        pids = [ready.get(timeout=60) for _ in range(workers)]
                    finally:
                        gate.set()
                    [future.result() for future in warmup]
                    startup_seconds = time.perf_counter()-startup
                    with ResourceMonitor(extra_pids=pids) as monitor:
                        before = time.perf_counter()
                        counts = list(pool.map(process_batch, chunks))
                        elapsed = time.perf_counter()-before
                ready.close()
                ready.join_thread()
                row = {'mode': 'process-compute', 'workers': workers, 'repeat': repeat, 'items': sum(n for n, _ in counts),
                       'startup_seconds': startup_seconds, 'seconds': elapsed,
                       'items_per_second': sum(n for n, _ in counts)/elapsed, 'resources': monitor.summary()}
                results.append(row); save()
                print(json.dumps({k:v for k,v in row.items() if k != 'resources'}), flush=True)
    elif args.compute_only:
        query = extract(images[1001])
        payloads = list(images.values())
        for repeat in range(args.repeats):
            for workers, _ in configs[repeat%len(configs):]+configs[:repeat%len(configs)]:
                with ResourceMonitor() as monitor:
                    before = time.perf_counter()
                    def evaluate(data):
                        return compare(query, extract(data))
                    with ThreadPoolExecutor(max_workers=workers) as pool:
                        # Two passes with no disk/cache intentionally measure the compute ceiling.
                        metrics = list(pool.map(evaluate, payloads*2))
                    elapsed = time.perf_counter()-before
                row = {'mode': 'compute-only', 'workers': workers, 'repeat': repeat, 'items': len(metrics),
                       'seconds': elapsed, 'items_per_second': len(metrics)/elapsed, 'resources': monitor.summary()}
                results.append(row); save()
                print(json.dumps({k:v for k,v in row.items() if k != 'resources'}), flush=True)
    else:
        with MockServer(images, delay=0.04) as server:
            config = SourceConfig(server.url, 'resource-fixture-v1', rate=0, retries=0, keep_alive=True)
            for repeat in range(args.repeats):
                for workers, downloads in configs[repeat%len(configs):]+configs[:repeat%len(configs)]:
                    with ResourceMonitor() as monitor:
                        before = time.perf_counter()
                        route = search_range
                        if args.staged:
                            from avatar_compare.staged import search_staged
                            route = search_staged
                        result = route(target, 1001, 1000+args.count, args.output/f'state-{repeat}-{workers}-{downloads}',
                            config, workers=workers, download_workers=downloads, stream_results=True, top_limit=20,
                            shared_writer=args.shared_writer, compute_backend='process' if args.process_extract else 'thread',
                            worker_observer=monitor.attach_pid if args.process_extract else None)
                        elapsed = time.perf_counter()-before
                    row = {'mode': 'end-to-end', 'workers': workers, 'downloads': downloads, 'repeat': repeat,
                           'seconds': elapsed, 'items_per_second': args.count/elapsed, 'stats': result['stats'],
                           'resources': monitor.summary()}
                    results.append(row); save()
                    print(json.dumps({k:v for k,v in row.items() if k not in {'resources','stats'}}), flush=True)
                    assert result['stats']['processed'] == args.count and result['stats']['errors'] == 0
    print(json.dumps([{'workers':row['workers'], 'downloads':row.get('downloads'), 'items_per_second':row['items_per_second'],
                      'cpu_percent':row['resources']['process_cpu_percent'], 'cpu_peak':row['resources']['process_cpu_peak_percent'],
                      'memory_mib':row['resources']['peak_working_set_bytes']/1024**2} for row in results], indent=2), flush=True)


if __name__ == '__main__':
    main()
