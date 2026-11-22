"""Synthetic retention audit: same top evidence, fewer durable bytes, overlap resume."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sqlite3
import sys
import time

APP=Path(__file__).resolve().parents[1];sys.path.insert(0,str(APP))
for name in ['OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS']:os.environ[name]='1'
from avatar_compare.history import inspect
from avatar_compare.ledger import export_records
from avatar_compare.mock_service import MockServer,encode,synthetic_picture
from avatar_compare.report import write_reports
from avatar_compare.source import SourceConfig
from avatar_compare.staged import search_staged


def metrics(value):
    if isinstance(value,dict):return {k:metrics(v) for k,v in value.items() if not k.endswith('_ms')}
    if isinstance(value,list):return [metrics(v) for v in value]
    return value


def sizes(state):
    path=state/'cache.sqlite3';result={'sqlite_bytes':path.stat().st_size}
    with sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True) as db:
        for table in ['raw_images','features','pairs','acquisitions']:
            result[table+'_rows']=db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
        for table in ['raw_images','features','acquisitions']:
            result[table+'_payload_bytes']=db.execute(f'SELECT coalesce(sum(length(payload)),0) FROM {table}').fetchone()[0]
        for table in ['compact_done','compact_candidates','compact_errors','fast_items']:
            if db.execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name=?',(table,)).fetchone():
                result[table+'_rows']=db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
    return result


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--count',type=int,default=1024);args=parser.parse_args()
    if args.output.exists() and any(args.output.iterdir()):parser.error('使用新的空输出目录')
    args.output.mkdir(parents=True,exist_ok=True)
    images={1001+i:encode(synthetic_picture(17 if i==0 else 2000+i).resize((100,100))) for i in range(args.count)}
    target=args.output/'target.png';target.write_bytes(images[1001]);rows=[];reference=None
    end=1000+args.count
    with MockServer(images,delay=.04) as server:
        config=SourceConfig(server.url,'compact-audit-v1',rate=0,retries=0,keep_alive=True)
        for mode in ['all','compact']:
            state=args.output/mode
            before=time.perf_counter()
            result=search_staged(target,1001,end,state,config,workers=8,download_workers=128,
                                 compact=mode=='compact',share_computations=True,top_limit=100)
            elapsed=time.perf_counter()-before
            evidence=[(r['identifier'],r['digest'],metrics(r['metrics'])) for r in result['results']]
            if reference is None:reference=evidence
            assert evidence==reference and result['stats']['errors']==0
            row={'mode':mode,'count':args.count,'seconds':elapsed,'stats':result['stats'],'storage':sizes(state)}
            rows.append(row);print(json.dumps(row),flush=True)
        state=args.output/'compact';calls=sum(server.calls.values())
        resumed=search_staged(target,1001,end,state,config,workers=8,download_workers=128,compact=True,share_computations=True)
        assert resumed['stats']['resumed']==args.count and sum(server.calls.values())==calls
        extended=search_staged(target,1001,end+2,state,config,workers=8,download_workers=128,compact=True,share_computations=True)
        assert extended['stats']['resumed']==args.count and extended['stats']['requests']==2
        reports=write_reports(extended,args.output/'reports')
        exported=export_records(state,extended['run_id'],args.output/'export',images=True)
        summary={'scope':'1024 synthetic100px unique content by default; loopback only. Same candidate evidence, algorithm and source. No old user cache deleted.',
            'samples':rows,'same_top_evidence':True,'same_task_resume_requests':resumed['stats']['requests'],
            'extended_range':extended['range'],'extended_stats':extended['stats'],'history':inspect(state,run_id=extended['run_id'])[0],
            'reports':{k:str(v.resolve()) for k,v in reports.items()},'exports':{k:str(v.resolve()) for k,v in exported.items()}}
        (args.output/'audit.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
        reduction=1-rows[1]['storage']['sqlite_bytes']/rows[0]['storage']['sqlite_bytes']
        print(f'SQLite文件减少{reduction:.1%}；候选证据一致；跨范围复用{args.count}项，仅请求新增2项。',flush=True)


if __name__=='__main__':main()
