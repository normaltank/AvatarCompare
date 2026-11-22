"""Controlled TLS protocol comparison; no public requests. deps under .experiments."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
import os
from pathlib import Path
import ssl
import statistics
import sys
import threading
import time

APP=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(APP/'.experiments/native-http-deps'));sys.path.insert(0,str(APP))
for variable in ['OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS']:os.environ[variable]='1'
import pycurl
from avatar_compare.mock_service import synthetic_picture,encode
from avatar_compare.resources import ResourceMonitor
from avatar_compare.source import HttpSource,SourceConfig
from tls_fixture import TLSFixture


def native_download(server,images,workers,protocol,connections):
    multi=pycurl.CurlMulti();multi.setopt(pycurl.M_MAX_HOST_CONNECTIONS,connections)
    multi.setopt(pycurl.M_MAX_TOTAL_CONNECTIONS,connections)
    multi.setopt(pycurl.M_PIPELINING,pycurl.PIPE_MULTIPLEX)
    multi.setopt(pycurl.M_MAX_CONCURRENT_STREAMS,workers)
    active={};iterator=iter(images);records=[]
    def fill():
        while len(active)<workers and (identifier:=next(iterator,None)) is not None:
            handle=pycurl.Curl();buffer=io.BytesIO()
            handle.setopt(pycurl.URL,server.url.format(id=identifier));handle.setopt(pycurl.PROXY,'')
            handle.setopt(pycurl.CAINFO,str(server.certfile));handle.setopt(pycurl.FOLLOWLOCATION,0)
            handle.setopt(pycurl.HTTP_VERSION,pycurl.CURL_HTTP_VERSION_1_1 if protocol=='h1' else pycurl.CURL_HTTP_VERSION_2TLS)
            handle.setopt(pycurl.PIPEWAIT,1 if protocol=='h2' else 0);handle.setopt(pycurl.TIMEOUT_MS,10000)
            handle.setopt(pycurl.HTTPHEADER,['Accept: image/*','Accept-Encoding: identity'])
            handle.setopt(pycurl.WRITEFUNCTION,buffer.write)
            active[handle]=(identifier,buffer,time.perf_counter());multi.add_handle(handle)
    try:
        fill()
        while active:
            while multi.perform()[0]==pycurl.E_CALL_MULTI_PERFORM:pass
            while True:
                queued,okay,failed=multi.info_read()
                if failed:raise RuntimeError([(n,e) for _,n,e in failed])
                for handle in okay:
                    identifier,buffer,started=active.pop(handle)
                    assert handle.getinfo(pycurl.RESPONSE_CODE)==200
                    assert hashlib.sha256(buffer.getvalue()).digest()==hashlib.sha256(images[identifier]).digest()
                    records.append({'version':handle.getinfo(pycurl.INFO_HTTP_VERSION),'connections':handle.getinfo(pycurl.NUM_CONNECTS),
                                    'seconds':time.perf_counter()-started})
                    multi.remove_handle(handle);handle.close()
                if not queued:break
            fill()
            if active and multi.select(.02)<0:time.sleep(.001)
    finally:
        for handle in active:multi.remove_handle(handle);handle.close()
        multi.close()
    expected=pycurl.CURL_HTTP_VERSION_1_1 if protocol=='h1' else pycurl.CURL_HTTP_VERSION_2_0
    assert len(records)==len(images) and {r['version'] for r in records}=={expected},records[:1]
    ordered=sorted(r['seconds'] for r in records)
    return {'http_version':protocol,'connections_opened':sum(r['connections'] for r in records),
            'p50_seconds':statistics.median(ordered),'p95_seconds':ordered[int((len(ordered)-1)*.95)]}


def thread_download(server,images,workers):
    source=HttpSource(SourceConfig(server.url,'fixture',rate=0,retries=0,keep_alive=True))
    source.ssl_context=ssl.create_default_context(cafile=str(server.certfile));source.ssl_context.set_alpn_protocols(['http/1.1'])
    cancel=threading.Event()
    def work(identifier):
        data,metadata=source.fetch(identifier,cancel)
        assert data is not None,metadata
        assert hashlib.sha256(data).digest()==hashlib.sha256(images[identifier]).digest()
        return metadata['fetch_ms']/1000
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:seconds=list(pool.map(work,images))
        ordered=sorted(seconds)
        return {'http_version':'h1','connections_opened':len(source.connections),
                'p50_seconds':statistics.median(ordered),'p95_seconds':ordered[int((len(ordered)-1)*.95)]}
    finally:source.close()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--count',type=int,default=512);parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--workers',type=int,default=128);parser.add_argument('--delay',type=float,default=.04)
    parser.add_argument('--modes',default='thread-h1,curl-h1,curl-h2-one,curl-h2-eight')
    args=parser.parse_args()
    if args.output.exists() and any(args.output.iterdir()):parser.error('use a fresh output folder')
    args.output.mkdir(parents=True,exist_ok=True)
    images={1001+i:encode(synthetic_picture(2000+i).resize((100,100))) for i in range(args.count)}
    choices={'thread-h1':args.workers,'curl-h1':args.workers,'curl-h2-one':1,'curl-h2-eight':8}
    if not set(args.modes.split(','))<=set(choices):parser.error('invalid mode')
    modes=[(mode,choices[mode]) for mode in args.modes.split(',')]
    rows=[]
    with TLSFixture(images,args.output/'tls',delay=args.delay) as server:
        for repeat in range(args.repeats):
            for mode,connections in modes[repeat%len(modes):]+modes[:repeat%len(modes)]:
                calls=sum(server.calls.values())
                with ResourceMonitor() as monitor:
                    started=time.perf_counter()
                    detail=thread_download(server,images,args.workers) if mode=='thread-h1' else native_download(server,images,args.workers,'h1' if mode=='curl-h1' else 'h2',connections)
                    elapsed=time.perf_counter()-started
                assert sum(server.calls.values())-calls==args.count
                row={'mode':mode,'repeat':repeat,'seconds':elapsed,'items_per_second':args.count/elapsed,'detail':detail,'resources':monitor.summary()}
                rows.append(row)
                (args.output/'protocol.json').write_text(json.dumps({'pycurl':pycurl.version,'count':args.count,'workers':args.workers,'delay':args.delay,
                    'h1_pipewait':False,
                    'scope':'Controlled loopback TLS and verified protocols/bytes; server CPU included; no public throughput claim.','samples':rows},indent=2),encoding='utf-8')
                print(json.dumps({k:v for k,v in row.items() if k!='resources'}),flush=True)
    print(json.dumps({mode:statistics.median(r['items_per_second'] for r in rows if r['mode']==mode) for mode,_ in modes}),flush=True)


if __name__=='__main__':main()
