"""Disk-backed range ledger: bounded resume paging, indexed top results and export."""
from __future__ import annotations

import argparse
from contextlib import closing
import csv
import json
import hashlib
from pathlib import Path
import sqlite3


def consistent(row):
    metric = row['metrics']
    return bool(metric and (metric['exact_file'] or metric['exact_pixels'] or metric['geometry']['consistent']))


class RangeLedger:
    def __init__(self, db, run_id):
        self.db, self.run_id = db, run_id
        db.executescript('''
            CREATE TABLE IF NOT EXISTS fast_totals(run_id TEXT PRIMARY KEY, processed INTEGER,
                errors INTEGER, consistent INTEGER, blocked INTEGER);
            CREATE TABLE IF NOT EXISTS fast_items(run_id TEXT, identifier INTEGER, payload TEXT,
                rank_group INTEGER, rank_inliers INTEGER, rank_phash REAL, rank_ssim REAL,
                PRIMARY KEY(run_id,identifier));
            CREATE INDEX IF NOT EXISTS fast_rank ON fast_items(run_id,rank_group,rank_inliers,rank_phash,rank_ssim,identifier);
        ''')
        with db:
            db.execute('INSERT OR IGNORE INTO fast_totals VALUES(?,0,0,0,0)', (run_id,))

    def totals(self):
        return dict(self.db.execute('SELECT processed,errors,consistent,blocked FROM fast_totals WHERE run_id=?', (self.run_id,)).fetchone())

    def retry_errors(self, source):
        with self.db:
            self.db.execute('''DELETE FROM acquisitions WHERE source=? AND identifier IN
                (SELECT CAST(identifier AS TEXT) FROM fast_items WHERE run_id=? AND rank_group=4)''', (source, self.run_id))
            self.db.execute('DELETE FROM fast_items WHERE run_id=? AND rank_group=4', (self.run_id,))
            self.db.execute('UPDATE fast_totals SET processed=processed-errors,errors=0,blocked=0 WHERE run_id=?', (self.run_id,))

    def top(self, limit):
        return [json.loads(record['payload']) for record in self.db.execute('''SELECT payload FROM fast_items
            WHERE run_id=? ORDER BY rank_group,rank_inliers,rank_phash,rank_ssim,identifier LIMIT ?''', (self.run_id, limit))]

    def pending(self, start, end, page_size=1024):
        # Keyset pages release read cursors promptly; a long WAL snapshot would
        # prevent checkpoints for the lifetime of a very large task.
        position, candidate = start-1, start
        while candidate <= end:
            records = self.db.execute('''SELECT identifier FROM fast_items WHERE run_id=? AND identifier>?
                AND identifier<=? ORDER BY identifier LIMIT ?''', (self.run_id, position, end, page_size)).fetchall()
            if not records:
                yield from range(candidate, end+1)
                return
            for record in records:
                identifier = record['identifier']
                yield from range(candidate, identifier)
                candidate = max(candidate, identifier+1)
            position = records[-1]['identifier']

    def save(self, row):
        return self.save_many([row])[0]

    def save_many(self, rows):
        statements = self.statements(rows)
        if hasattr(self.db, 'write_batch'):
            counts = self.db.write_batch(statements)
        else:
            with self.db:
                counts = [self.db.execute(sql, parameters).rowcount for sql, parameters in statements]
        return [bool(changed) for changed in counts[::2]]

    def statements(self, rows):
        statements = []
        for row in rows:
            metric = row['metrics']
            group = (0 if metric['exact_file'] else 1 if metric['exact_pixels'] else 2 if metric['geometry']['consistent'] else 3) if metric else 4
            values = (group, -metric['geometry']['inliers'], -metric['phash_agreement'], -metric['ssim']) if metric else (4, 0, 0, 0)
            statements.extend([('INSERT OR IGNORE INTO fast_items VALUES(?,?,?,?,?,?,?)',
                                (self.run_id, int(row['identifier']), json.dumps(row), *values)),
                               ('''UPDATE fast_totals SET processed=processed+1,errors=errors+?,consistent=consistent+?,
                                   blocked=blocked+? WHERE run_id=? AND changes()=1''',
                                (bool(row['error']), consistent(row), row['fetch']['http_status'] in {401, 403, 429}, self.run_id))])
        return statements


def export_records(state_dir: Path, run_id: str, output: Path, *, images=False):
    """Export without materializing all records or requiring the matching process."""
    from .storage_profile import table_exists
    with closing(sqlite3.connect((state_dir/'cache.sqlite3').resolve().as_uri()+'?mode=ro',uri=True)) as check:
        if table_exists(check,'compact_jobs') and check.execute('SELECT 1 FROM compact_jobs WHERE run_id=?',(run_id,)).fetchone():
            from .compact_export import export_compact
            return export_compact(state_dir,run_id,output,images=images)
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    paths = {kind: output/f'comparison-{run_id}.{kind}' for kind in ['jsonl', 'csv']}
    if images:
        paths['images'] = output/f'images-{run_id}'
        paths['images'].mkdir(exist_ok=True)
    with closing(sqlite3.connect((state_dir/'cache.sqlite3').resolve().as_uri()+'?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        if not db.execute('SELECT 1 FROM fast_totals WHERE run_id=?', (run_id,)).fetchone():
            raise ValueError('未找到该全速模式任务')
        with paths['jsonl'].open('w', encoding='utf-8') as lines, paths['csv'].open('w', encoding='utf-8-sig', newline='') as table:
            writer = csv.writer(table)
            writer.writerow(['编号', '证据类型', '结构相似度', '几何内点数', 'HTTP状态', '错误'])
            position = -1
            while True:
                page = db.execute('SELECT identifier,payload FROM fast_items WHERE run_id=? AND identifier>? ORDER BY identifier LIMIT 1024', (run_id, position)).fetchall()
                if not page:
                    break
                for record in page:
                    row = json.loads(record['payload'])
                    if images and row.get('digest'):
                        if row.get('asset_kind') == 'sqlite':
                            asset = db.execute('SELECT payload FROM raw_images WHERE digest=?',(row['digest'],)).fetchone()
                            data = asset['payload'] if asset else None
                        else:
                            try:
                                data = Path(row['path']).read_bytes()
                            except OSError:
                                data = None
                        if data is None or hashlib.sha256(data).hexdigest()!=row['digest']:
                            raise ValueError(f"编号{row['identifier']}的原图缺失或摘要不符")
                        (paths['images']/f"{int(row['identifier'])}-{row['digest']}.img").write_bytes(data)
                    lines.write(json.dumps(row, ensure_ascii=False)+'\n')
                    metric = row['metrics']
                    label = '读取失败' if not metric else '文件完全相同' if metric['exact_file'] else '像素完全相同' if metric['exact_pixels'] else '局部位置关系一致（实验性）' if metric['geometry']['consistent'] else '证据不足'
                    error = str(row['error'] or '')
                    if error.lstrip().startswith(('=', '+', '-', '@')):
                        error = "'"+error
                    writer.writerow([row['identifier'], label, metric['ssim'] if metric else '',
                                     metric['geometry']['inliers'] if metric else '', row['fetch']['http_status'], error])
                position = page[-1]['identifier']
    return paths


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='流式导出全速任务完整逐项CSV和JSONL记录')
    parser.add_argument('--state-dir', type=Path, default=Path(__file__).resolve().parents[1]/'.state')
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--output', type=Path, default=Path('outputs/full-records'))
    parser.add_argument('--images', action='store_true', help='同时导出逐编号原始图片，保持原始字节（.img扩展名）')
    args = parser.parse_args()
    print(json.dumps({kind: str(path) for kind, path in export_records(args.state_dir, args.run_id, args.output, images=args.images).items()}, ensure_ascii=False))
