"""0/50/90% exact-byte duplicates; sharing A/B uses same current pipeline and source."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import sys
import threading
import time
from unittest.mock import patch

APP=Path(__file__).resolve().parents[1];sys.path.insert(0,str(APP))
for variable in ['OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS']:os.environ[variable]='1'
from avatar_compare.engine import Store
from avatar_compare.mock_service import MockServer,encode,synthetic_picture
from avatar_compare.resources import ResourceMonitor
from avatar_compare.source import SourceConfig
from avatar_compare.staged import search_staged
from shared_candidate import SharedComparisons
from validation_helpers import semantic_hash


class CandidateStore(Store):
    instances=[]
    def __init__(self,*a,**kw):
        super().__init__(*a,**kw);self.sharing=SharedComparisons(super().prepare_bytes)
        self.query_digest=None;type(self).instances.append(self)

    def enable_shared_writer(self,*a,**kw):
        super().enable_shared_writer(*a,**kw);original=self.writer.submit_future
        def submit(statements=(),*,script=None):
            future=original(statements,script=script)
            digests=[]
            for sql,parameters in statements:
                if sql.startswith('INSERT OR IGNORE INTO fast_items'):
                    row=json.loads(parameters[2])
                    if row['metrics'] and not row['error']:digests.append(row['digest'])
            def durable(done):
                if done.exception() is None:
                    for digest in digests:self.sharing.committed(self.query_digest,digest)
            future.add_done_callback(durable)
            return future
        self.writer.submit_future=submit

    def prepare_bytes(self,query,data,**kwargs):
        self.query_digest=query.digest
        return self.sharing.prepare(query,data)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--count',type=int,default=512);parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--ratios',default='0,0.5,0.9')
    parser.add_argument('--implementation',choices=['candidate','integrated'],default='integrated')
    args=parser.parse_args()
    if args.output.exists() and any(args.output.iterdir()):parser.error('use a fresh output folder')
    args.output.mkdir(parents=True,exist_ok=True)
    pool=[encode(synthetic_picture(2000+i).resize((100,100))) for i in range(args.count)]
    target=args.output/'target.png';target.write_bytes(encode(synthetic_picture(17).resize((100,100))))
    rows=[];original_extract=Store.prepare_bytes
    for ratio in map(float,args.ratios.split(',')):
        unique=max(1,round(args.count*(1-ratio)))
        indices=list(range(unique))+[i%min(8,unique) for i in range(args.count-unique)]
        random.Random(17).shuffle(indices)
        images={1001+i:pool[n] for i,n in enumerate(indices)}
        references=set()
        with MockServer(images,delay=.04) as server:
            config=SourceConfig(server.url,'duplicate-work-v1',rate=0,retries=0,keep_alive=True)
            for repeat in range(args.repeats):
                for mode in (['baseline','shared'] if repeat%2==0 else ['shared','baseline']):
                    CandidateStore.instances=[]
                    state=args.output/f'state-{ratio}-{repeat}-{mode}'
                    counts={'extract':0,'compare':0};guard=threading.Lock()
                    from avatar_compare import engine
                    extract,compare=engine.extract,engine.compare
                    def extracted(*a,**kw):
                        with guard:counts['extract']+=1
                        return extract(*a,**kw)
                    def compared(*a,**kw):
                        with guard:counts['compare']+=1
                        return compare(*a,**kw)
                    before=sum(server.calls.values())
                    with patch('avatar_compare.engine.extract',extracted),patch('avatar_compare.engine.compare',compared),ResourceMonitor() as monitor:
                        started=time.perf_counter()
                        if mode=='shared' and args.implementation=='candidate':
                            with patch('avatar_compare.staged.Store',CandidateStore):
                                result=search_staged(target,1001,1000+args.count,state,config,workers=8,download_workers=128)
                        else:result=search_staged(target,1001,1000+args.count,state,config,workers=8,download_workers=128,
                                                share_computations=mode=='shared')
                        elapsed=time.perf_counter()-started
                    assert result['status']=='complete' and result['stats']['processed']==args.count and result['stats']['errors']==0
                    assert sum(server.calls.values())-before==args.count
                    semantic=semantic_hash(state,result['run_id']);references.add(semantic)
                    assert len(references)==1,(ratio,mode,semantic)
                    row={'mode':mode,'repeat':repeat,'duplicate_ratio':ratio,'unique_images':unique,'count':args.count,
                        'implementation':args.implementation,
                        'seconds':elapsed,'items_per_second':args.count/elapsed,'actual_calls':counts,'semantic_hash':semantic,
                        'stats':result['stats'],'resources':monitor.summary()}
                    if CandidateStore.instances:row['work']=CandidateStore.instances[0].sharing.snapshot()
                    rows.append(row)
                    (args.output/'duplicates.json').write_text(json.dumps({'scope':'Synthetic100px; per-ID requests/results retained; paired fresh-cache baselines; server delay40ms.',
                        'samples':rows},indent=2),encoding='utf-8')
                    print(json.dumps({k:v for k,v in row.items() if k not in {'stats','resources'}}),flush=True)
    print(json.dumps({str(ratio):{mode:statistics.median(r['items_per_second'] for r in rows if r['mode']==mode and r['duplicate_ratio']==ratio)
          for mode in ['baseline','shared']} for ratio in map(float,args.ratios.split(','))}),flush=True)


if __name__=='__main__':main()
