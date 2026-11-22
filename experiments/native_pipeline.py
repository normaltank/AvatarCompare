"""Candidate native adapter in the unchanged matching pipeline, isolated TLS only."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import random
from pathlib import Path
import sqlite3
import ssl
import statistics
import sys
import time
from unittest.mock import patch

APP=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(APP/'.experiments/native-http-deps'));sys.path.insert(0,str(APP))
for variable in ['OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS']:os.environ[variable]='1'
from avatar_compare.mock_service import encode,synthetic_picture
from avatar_compare.resources import ResourceMonitor
from avatar_compare.source import SourceConfig
from avatar_compare.staged import search_staged
from native_transfer import NativeTransfers
from tls_fixture import TLSFixture
from validation_helpers import semantic_hash


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--count',type=int,default=512);parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--workers',type=int,default=8);parser.add_argument('--downloads',type=int,default=128)
    parser.add_argument('--modes',default='thread-h1,curl-h1,curl-h2')
    parser.add_argument('--duplicate-ratio',type=float,default=0)
    args=parser.parse_args()
    if args.output.exists() and any(args.output.iterdir()):parser.error('use a fresh output folder')
    args.output.mkdir(parents=True,exist_ok=True)
    images={1001+i:encode(synthetic_picture(17 if i==0 else 2000+i).resize((100,100))) for i in range(args.count)}
    target=args.output/'target.png';target.write_bytes(images[1001])
    if not 0<=args.duplicate_ratio<1:parser.error('duplicate ratio must be in [0,1)')
    if args.duplicate_ratio:
        pool=list(images.values());unique=max(1,round(args.count*(1-args.duplicate_ratio)))
        indices=list(range(unique))+[i%min(8,unique) for i in range(args.count-unique)]
        random.Random(17).shuffle(indices);images={1001+i:pool[n] for i,n in enumerate(indices)}
    modes=args.modes.split(',')
    if not set(modes)<={'thread-h1','curl-h1','curl-h2','thread-h1-shared','curl-h2-shared'}:parser.error('invalid mode')
    rows=[];reference=None;original_context=ssl.create_default_context
    with TLSFixture(images,args.output/'tls',delay=.04) as server:
        def context(*a,**kw):
            if not any(kw.get(key) for key in ['cafile','capath','cadata']):kw['cafile']=str(server.certfile)
            return original_context(*a,**kw)
        config=SourceConfig(server.url,'native-pipeline-v1',rate=0,retries=0,keep_alive=True)
        for repeat in range(args.repeats):
            for mode in modes[repeat%len(modes):]+modes[:repeat%len(modes)]:
                state=args.output/f'state-{repeat}-{mode}';instances=[]
                def candidate(*a,**kw):
                    instance=NativeTransfers(*a,**kw,protocol='h1' if mode=='curl-h1' else 'h2',ca_file=server.certfile)
                    instances.append(instance);return instance
                before=sum(server.calls.values())
                with patch('avatar_compare.source.ssl.create_default_context',context),ResourceMonitor() as monitor:
                    started=time.perf_counter()
                    if mode.startswith('thread-h1'):
                        result=search_staged(target,1001,1000+args.count,state,config,workers=args.workers,download_workers=args.downloads,
                            share_computations=mode.endswith('-shared'))
                    else:
                        with patch('avatar_compare.staged.ThreadTransfers',candidate):
                            result=search_staged(target,1001,1000+args.count,state,config,workers=args.workers,download_workers=args.downloads,
                                share_computations=mode.endswith('-shared'))
                    elapsed=time.perf_counter()-started
                assert result['status']=='complete' and result['stats']['processed']==args.count and result['stats']['errors']==0,result['stats']
                assert sum(server.calls.values())-before==args.count
                semantic=semantic_hash(state,result['run_id'])
                if reference is None:reference=semantic
                assert semantic==reference,(mode,semantic,reference)
                row={'mode':mode,'repeat':repeat,'seconds':elapsed,'items_per_second':args.count/elapsed,
                    'semantic_hash':semantic,'stats':result['stats'],'resources':monitor.summary()}
                if instances:row['native']={'http_versions':dict(instances[0].versions),'connections_opened':instances[0].connections_opened}
                rows.append(row)
                (args.output/'pipeline.json').write_text(json.dumps({'count':args.count,'workers':args.workers,'downloads':args.downloads,
                    'duplicate_ratio':args.duplicate_ratio,
                    'scope':'Cold isolated TLS H1/H2, same source bytes and all per-ID evidence hashes; fixture CPU included.','samples':rows},indent=2),encoding='utf-8')
                print(json.dumps({k:v for k,v in row.items() if k not in {'resources','stats'}}),flush=True)
    print(json.dumps({mode:statistics.median(r['items_per_second'] for r in rows if r['mode']==mode) for mode in modes}),flush=True)


if __name__=='__main__':main()
