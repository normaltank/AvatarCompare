"""Disentangle network capacity, file publication and the legacy complete pipeline."""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import threading
import time
import uuid

APP = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP/'.experiments'/'transport-deps'))
sys.path.insert(0, str(APP))
for name in ['OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS']:
    os.environ[name] = '1'

from avatar_compare.mock_service import MockServer, encode, synthetic_picture
from avatar_compare.resources import ResourceMonitor
from avatar_compare.source import HttpSource, SourceConfig
from avatar_compare.range_engine import search_range


def thread_download(server, images, workers, output=None):
    source = HttpSource(SourceConfig(server.url, 'ablation', rate=0, retries=0, keep_alive=True))
    cancel, lock = threading.Event(), threading.Lock()
    def work(identifier):
        data, metadata = source.fetch(identifier, cancel)
        if data is None:
            raise RuntimeError(metadata['error_code'])
        digest = hashlib.sha256(data).hexdigest()
        if output:
            path = output/digest[:2]/digest[2:4]/(digest+'.img')
            path.parent.mkdir(parents=True,exist_ok=True)
            with lock:
                temporary = path.with_suffix('.'+uuid.uuid4().hex+'.tmp')
                temporary.write_bytes(data)
                temporary.replace(path)
        assert digest == hashlib.sha256(images[identifier]).hexdigest()
        return len(data)
    try:
        # map is safe for this explicitly bounded benchmark dataset, not the range engine.
        with ThreadPoolExecutor(max_workers=workers) as pool:
            sizes = list(pool.map(work, images))
        return {'requests':source.requests,'bytes':sum(sizes),'connections':'one per worker','http_versions':['HTTP/1.1']}
    finally:
        source.close()


async def async_download(server, images, workers, kind):
    iterator = iter(images)
    sizes, versions = [], set()
    if kind == 'httpx':
        import httpx
        client = httpx.AsyncClient(http2=True,trust_env=False,follow_redirects=False,
            limits=httpx.Limits(max_connections=workers,max_keepalive_connections=workers),timeout=10)
        async def worker():
            while (identifier := next(iterator, None)) is not None:
                async with client.stream('GET',server.url.format(id=identifier),headers={'Accept':'image/*','Accept-Encoding':'identity'}) as response:
                    assert response.status_code == 200
                    data = b''.join([chunk async for chunk in response.aiter_raw()])
                    versions.add(response.http_version)
                assert hashlib.sha256(data).digest() == hashlib.sha256(images[identifier]).digest()
                sizes.append(len(data))
        async with client:
            await asyncio.gather(*(worker() for _ in range(workers)))
    else:
        import aiohttp
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=workers,limit_per_host=workers),
                timeout=aiohttp.ClientTimeout(total=10),auto_decompress=False,trust_env=False) as client:
            async def worker():
                while (identifier := next(iterator, None)) is not None:
                    async with client.get(server.url.format(id=identifier),allow_redirects=False,headers={'Accept':'image/*','Accept-Encoding':'identity'}) as response:
                        assert response.status == 200
                        data = await response.read()
                        versions.add(f'HTTP/{response.version.major}.{response.version.minor}')
                    assert hashlib.sha256(data).digest() == hashlib.sha256(images[identifier]).digest()
                    sizes.append(len(data))
            await asyncio.gather(*(worker() for _ in range(workers)))
    assert len(sizes)==len(images)
    return {'requests':len(sizes),'bytes':sum(sizes),'http_versions':sorted(versions)}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--count',type=int,default=512)
    parser.add_argument('--repeats',type=int,default=2)
    parser.add_argument('--delay',type=float,default=0.04)
    parser.add_argument('--workers',type=int,default=128)
    args=parser.parse_args()
    if args.output.exists() and any(args.output.iterdir()):
        parser.error('请使用新的空输出目录')
    args.output.mkdir(parents=True,exist_ok=True)
    images={1001+i:encode(synthetic_picture(17 if i==0 else 2000+i).resize((100,100))) for i in range(args.count)}
    target=args.output/'target.png';target.write_bytes(images[1001])
    modes=['thread-network','httpx-network','aiohttp-network','thread-network-files','full-v04']
    rows=[]
    with MockServer(images,delay=args.delay) as server:
        for repeat in range(args.repeats):
            for mode in modes[repeat:]+modes[:repeat]:
                state=args.output/f'state-{repeat}-{mode}'
                with ResourceMonitor() as monitor:
                    started=time.perf_counter()
                    if mode=='full-v04':
                        result=search_range(target,1001,1000+args.count,state,
                            SourceConfig(server.url,'ablation',rate=0,retries=0,keep_alive=True),
                            workers=32,download_workers=args.workers,stream_results=True,shared_writer=True)
                        assert result['stats']['processed']==args.count and result['stats']['errors']==0
                        detail=result['stats']
                    elif mode.startswith('thread'):
                        detail=thread_download(server,images,args.workers,state if mode.endswith('files') else None)
                    else:
                        detail=asyncio.run(async_download(server,images,args.workers,mode.split('-')[0]))
                    elapsed=time.perf_counter()-started
                row={'mode':mode,'repeat':repeat,'seconds':elapsed,'items_per_second':args.count/elapsed,
                     'detail':detail,'resources':monitor.summary()}
                rows.append(row)
                (args.output/'ablation.json').write_text(json.dumps({'count':args.count,'delay':args.delay,'workers':args.workers,
                    'scope':'Synthetic100px, loopback HTTP/1.1 fixture; no public throughput claim. Network-only excludes feature and persistence work.',
                    'samples':rows},ensure_ascii=False,indent=2),encoding='utf-8')
                print(json.dumps({k:v for k,v in row.items() if k not in {'resources','detail'}}),flush=True)
    print(json.dumps({mode:statistics.median(row['items_per_second'] for row in rows if row['mode']==mode) for mode in modes},indent=2))


if __name__=='__main__':
    main()
