"""Stream retained evidence, errors and compressed progress without expanding misses."""
from contextlib import closing
import csv
import hashlib
import json
from pathlib import Path
import sqlite3

from .history import compact_ranges,error_rows,job_info
from .cache_lock import CacheLease


def records(db,scope,start,end):
    position=start-1
    while True:
        page=db.execute('''SELECT identifier,payload FROM compact_candidates WHERE scope=? AND identifier>? AND identifier<=?
            UNION ALL SELECT identifier,payload FROM compact_errors WHERE scope=? AND identifier>? AND identifier<=?
            ORDER BY identifier LIMIT 1024''',(scope,position,end,scope,position,end)).fetchall()
        if not page:return
        for row in page:yield json.loads(row['payload'])
        position=page[-1]['identifier']


def export_compact(state_dir,run_id,output,*,images=False):
    output=Path(output).resolve();output.mkdir(parents=True,exist_ok=True)
    stem='comparison-'+run_id
    paths={name:output/(stem+suffix) for name,suffix in [('jsonl','.retained.jsonl'),('csv','.retained.csv'),
           ('progress','.processed-ranges.jsonl'),('errors','.errors.jsonl'),('manifest','.manifest.json')]}
    if images:paths['images']=output/(stem+'-images');paths['images'].mkdir(exist_ok=True)
    # Exported review originals cannot be pruned by another owner mid-export.
    with CacheLease(state_dir),closing(sqlite3.connect((Path(state_dir)/'cache.sqlite3').resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row
        info=job_info(db,run_id);scope=info['scope_id'];start,end=info['requested_range']
        with paths['jsonl'].open('w',encoding='utf-8') as lines,paths['csv'].open('w',encoding='utf-8-sig',newline='') as table:
            writer=csv.writer(table);writer.writerow(['编号','保留类型','结构相似度','几何内点数','HTTP状态','错误'])
            for row in records(db,scope,start,end):
                metric=row['metrics'];error=str(row['error'] or '')
                if error.lstrip().startswith(('=','+','-','@')):error="'"+error
                lines.write(json.dumps(row,ensure_ascii=False)+'\n')
                writer.writerow([row['identifier'],row['retention_role'],metric['ssim'] if metric else '',
                                 metric['geometry']['inliers'] if metric else '',row['fetch']['http_status'],error])
                if images and row.get('raw_retained'):
                    data=db.execute('SELECT payload FROM raw_images WHERE digest=?',(row['digest'],)).fetchone()
                    if data is None or hashlib.sha256(data[0]).hexdigest()!=row['digest']:
                        raise ValueError('保留原图缺失或摘要不符：'+row['identifier'])
                    (paths['images']/f"{int(row['identifier'])}-{row['digest']}.img").write_bytes(data[0])
        with paths['progress'].open('w',encoding='utf-8') as stream:
            for lo,hi in compact_ranges(db,scope,start,end):
                stream.write(json.dumps({'start':str(lo),'end':str(hi),'inclusive':True,'includes_failures':True})+'\n')
        with paths['errors'].open('w',encoding='utf-8') as stream:
            for _,row in error_rows(db,scope,start,end):stream.write(json.dumps(row,ensure_ascii=False)+'\n')
        info.update(complete_per_id_records=False,originals='retained matches/review only',processed_ranges_file=paths['progress'].name,
                    errors_file=paths['errors'].name,retained_records_file=paths['jsonl'].name,
                    note='导出包含保留的候选和复核记录。图源清单中的原图留在本机缓存。'
                         '进度区间包含失败，成功比对须扣除错误记录。')
        paths['manifest'].write_text(json.dumps(info,ensure_ascii=False,indent=2),encoding='utf-8')
    return paths
