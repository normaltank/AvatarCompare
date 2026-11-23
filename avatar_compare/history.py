"""Read-only task coverage/history. Completed attempts and failures stay distinct."""
from __future__ import annotations
import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3

from .storage_profile import table_exists


def compact_ranges(db,scope,start,end):
    position=-1
    while True:
        rows=db.execute('SELECT lo,hi FROM compact_done WHERE scope=? AND lo>? AND lo<=? AND hi>=? ORDER BY lo LIMIT 1024',
                        (scope,position,end,start)).fetchall()
        if not rows:return
        for row in rows:yield max(start,row[0]),min(end,row[1])
        position=rows[-1][0]


def error_rows(db,scope,start,end):
    position=start-1
    while True:
        rows=db.execute('SELECT identifier,payload FROM compact_errors WHERE scope=? AND identifier>? AND identifier<=? ORDER BY identifier LIMIT 1024',
                        (scope,position,end)).fetchall()
        if not rows:return
        for row in rows:yield row[0],json.loads(row[1])
        position=rows[-1][0]


def successful_ranges(db,scope,start,end):
    failed=(identifier for identifier,_ in error_rows(db,scope,start,end))
    error=next(failed,None)
    for lo,hi in compact_ranges(db,scope,start,end):
        position=lo
        while error is not None and error<lo:error=next(failed,None)
        while error is not None and error<=hi:
            if position<error:yield position,error-1
            position=error+1;error=next(failed,None)
        if position<=hi:yield position,hi


def limited(values,limit):
    result=[]
    for value in values:
        if len(result)==limit:return result,True
        result.append(value)
    return result,False


def job_info(db,run_id,*,max_ranges=20):
    row=db.execute('SELECT * FROM runs WHERE id=?',(run_id,)).fetchone()
    if row is None:raise ValueError('未找到任务')
    info={'run_id':run_id,'target':row['target'],'last_status':row['status'],'total':row['total'],'updated':row['updated']}
    spec=db.execute('SELECT * FROM range_specs WHERE run_id=?',(run_id,)).fetchone() if table_exists(db,'range_specs') else None
    info['requested_range']=[spec['first_id'],spec['last_id']] if spec else None
    if spec:
        info.update(query_digest=spec['query_digest'],source_fingerprint=spec['source'],algorithm=spec['algorithm'],
                    source_name=spec['source_name'],source_revision=spec['source_revision'])
    compact=db.execute('SELECT scope FROM compact_jobs WHERE run_id=?',(run_id,)).fetchone() if table_exists(db,'compact_jobs') else None
    if compact:
        scope=compact[0];start,end=info['requested_range']
        info['capture_source'] = spec['retention'] == 'compact-source'
        processed=db.execute('SELECT coalesce(sum(min(hi,?)-max(lo,?)+1),0) FROM compact_done WHERE scope=? AND hi>=? AND lo<=?',
                             (end,start,scope,start,end)).fetchone()[0]
        errors=db.execute('SELECT count(*) FROM compact_errors WHERE scope=? AND identifier BETWEEN ? AND ?',(scope,start,end)).fetchone()[0]
        matches,retained=db.execute('SELECT coalesce(sum(strong),0),count(*) FROM compact_candidates WHERE scope=? AND identifier BETWEEN ? AND ?',
                                   (scope,start,end)).fetchone()
        done,truncated=limited(compact_ranges(db,scope,start,end),max_ranges)
        if processed==errors:success,success_truncated=[],False
        elif errors>max_ranges*1024:success,success_truncated=None,True
        else:success,success_truncated=limited(successful_ranges(db,scope,start,end),max_ranges)
        failures,errors_truncated=limited((identifier for identifier,_ in error_rows(db,scope,start,end)),max_ranges)
        info.update(retention='compact',scope_id=scope,processed=processed,successful=processed-errors,errors=errors,
                    consistent=matches,retained_records=retained,discarded_unmatched=processed-errors-retained,
                    processed_ranges=done,successful_ranges=success,failed_identifiers=failures,
                    ranges_truncated=truncated or success_truncated,errors_truncated=errors_truncated,complete_records=False)
        return info
    totals=db.execute('SELECT * FROM fast_totals WHERE run_id=?',(run_id,)).fetchone() if table_exists(db,'fast_totals') else None
    if totals:
        processed,errors=totals['processed'],totals['errors']
        low=db.execute('SELECT identifier FROM fast_items WHERE run_id=? ORDER BY identifier LIMIT 1',(run_id,)).fetchone()
        high=db.execute('SELECT identifier FROM fast_items WHERE run_id=? ORDER BY identifier DESC LIMIT 1',(run_id,)).fetchone()
        actual=[[low[0],high[0]]] if low and processed==high[0]-low[0]+1 else None
        info.update(retention='all',processed=processed,successful=processed-errors,errors=errors,
                    consistent=totals['consistent'],processed_ranges=actual,complete_records=True)
    elif table_exists(db,'range_results') and len(row['folder'])==64 and all(c in '0123456789abcdef' for c in row['folder']):
        processed,errors=db.execute('SELECT count(*),coalesce(sum(json_extract(payload,\'$.error\') IS NOT NULL),0) FROM range_results WHERE run_id=?',(run_id,)).fetchone()
        info.update(retention='all',processed=processed,successful=processed-errors,errors=errors,processed_ranges=None,complete_records=True)
    elif spec:info.update(retention=spec['retention'],processed=0,successful=0,errors=0,processed_ranges=[],complete_records=spec['retention']=='all')
    else:info.update(retention='local',processed=None,errors=None,complete_records=True)
    return info


def inspect(state_dir,*,run_id=None,limit=10,max_ranges=20):
    path=Path(state_dir)/'cache.sqlite3'
    if not path.exists():return []
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row
        # A short read snapshot keeps counts/ranges coherent during a live scan.
        db.execute('BEGIN')
        if not table_exists(db,'runs'):return []
        rows=db.execute('SELECT id FROM runs WHERE id=?',(run_id,)).fetchall() if run_id else db.execute('SELECT id FROM runs ORDER BY updated DESC LIMIT ?',(limit,)).fetchall()
        return [job_info(db,row[0],max_ranges=max_ranges) for row in rows]


def main():
    parser=argparse.ArgumentParser(description='只读查看任务、已处理/成功区间和失败编号；不下载头像')
    parser.add_argument('--state-dir',type=Path)
    parser.add_argument('--run-id');parser.add_argument('--limit',type=int,default=10)
    parser.add_argument('--max-ranges',type=int,default=20);parser.add_argument('--json',action='store_true')
    args=parser.parse_args()
    if args.limit<1 or args.max_ranges<1:parser.error('显示上限须为正数')
    app=Path(__file__).resolve().parents[1]
    paths=[args.state_dir] if args.state_dir else [app/'.state-compact',app/'.state']
    try:
        result=[{'state_dir':str(path.resolve()),'jobs':inspect(path,run_id=args.run_id,limit=args.limit,max_ranges=args.max_ranges)} for path in paths]
        if args.json:print(json.dumps(result,ensure_ascii=False,indent=2));return 0
        for group in result:
            print('任务库：'+group['state_dir'])
            if not group['jobs']:print('  尚无任务记录');continue
            for job in group['jobs']:
                requested=job['requested_range']
                bounds=f'{requested[0]}–{requested[1]}（含两端）' if requested else '旧版未保存请求边界；不以最大编号推断'
                fingerprint=' ['+job['query_digest'][:12]+']' if job.get('query_digest') else ''
                retention = '省空间 + 保留图源清单' if job.get('capture_source') else job['retention']
                print(f"  {job['run_id']} | {Path(job['target']).name}{fingerprint} | {retention} | 上次状态 {job['last_status']}")
                if job['retention']=='local':print('    本地文件夹任务，不使用账号范围');continue
                print(f"    请求范围：{bounds}；当前已处理 {job['processed']}，失败 {job['errors']}")
                processed=job.get('processed_ranges')
                print(f"    已处理区间（含失败）：{processed if processed is not None else '逐项记录；未计算连续区间摘要'}")
                if job['retention']=='compact':
                    print(f"    成功比对区间：{job['successful_ranges']}；失败编号：{job['failed_identifiers']}")
                    print(f"    有匹配证据 {job['consistent']} 项；候选/复核记录 {job['retained_records']} 项；已丢弃的未命中比对记录 {job['discarded_unmatched']} 项")
                    if job['ranges_truncated'] or job['errors_truncated']:print('    以上为截断预览；完整区间/错误请用ledger导出')
        return 0
    except (OSError,sqlite3.Error,ValueError) as error:
        print(f'读取记录失败：{type(error).__name__}: {error}');return 2


if __name__=='__main__':raise SystemExit(main())
