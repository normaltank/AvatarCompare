"""Single-target coverage intervals and retained evidence, committed atomically."""
from __future__ import annotations
import hashlib
import heapq
import json
import time

from .ledger import consistent
from .range_engine import range_rank_key

MAX_ID=2**63-1
REVIEW_LIMIT=100


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS storage_profile(id INTEGER PRIMARY KEY CHECK(id=1),profile TEXT);
      INSERT OR IGNORE INTO storage_profile VALUES(1,'compact-v1');
      CREATE TABLE IF NOT EXISTS compact_scopes(scope TEXT PRIMARY KEY,query_digest TEXT,source TEXT,algorithm TEXT,created REAL);
      CREATE TABLE IF NOT EXISTS compact_jobs(run_id TEXT PRIMARY KEY,scope TEXT);
      CREATE TABLE IF NOT EXISTS compact_done(scope TEXT,lo INTEGER,hi INTEGER,PRIMARY KEY(scope,lo),CHECK(lo<=hi));
      CREATE INDEX IF NOT EXISTS compact_done_hi ON compact_done(scope,hi);
      CREATE TABLE IF NOT EXISTS compact_errors(scope TEXT,identifier INTEGER,payload TEXT,http_status INTEGER,PRIMARY KEY(scope,identifier));
      CREATE TABLE IF NOT EXISTS compact_candidates(scope TEXT,identifier INTEGER,digest TEXT,payload TEXT,
          rank_group INTEGER,rank_inliers INTEGER,rank_phash REAL,rank_ssim REAL,strong INTEGER,PRIMARY KEY(scope,identifier));
      CREATE INDEX IF NOT EXISTS compact_rank ON compact_candidates(scope,rank_group,rank_inliers,rank_phash,rank_ssim,identifier);
      CREATE INDEX IF NOT EXISTS compact_digest ON compact_candidates(digest);
      CREATE INDEX IF NOT EXISTS compact_matches ON compact_candidates(scope,strong,identifier);
    ''')


def add_done(db,scope,lo,hi):
    overlaps=db.execute('SELECT lo,hi FROM compact_done WHERE scope=? AND hi>=? AND lo<=?',
                        (scope,max(0,lo-1),min(MAX_ID,hi+1))).fetchall()
    if overlaps:
        lo=min(lo,min(r[0] for r in overlaps));hi=max(hi,max(r[1] for r in overlaps))
        db.executemany('DELETE FROM compact_done WHERE scope=? AND lo=?',[(scope,r[0]) for r in overlaps])
    db.execute('INSERT INTO compact_done VALUES(?,?,?)',(scope,lo,hi))


def remove_point(db,scope,identifier):
    row=db.execute('SELECT lo,hi FROM compact_done WHERE scope=? AND lo<=? ORDER BY lo DESC LIMIT 1',
                   (scope,identifier)).fetchone()
    if not row or row[1]<identifier:return
    lo,hi=row
    db.execute('DELETE FROM compact_done WHERE scope=? AND lo=?',(scope,lo))
    if lo<identifier:db.execute('INSERT INTO compact_done VALUES(?,?,?)',(scope,lo,identifier-1))
    if identifier<hi:db.execute('INSERT INTO compact_done VALUES(?,?,?)',(scope,identifier+1,hi))


def blocks(numbers):
    start=last=None
    for value in sorted(set(numbers)):
        if last is not None and value==last+1:last=value
        else:
            if last is not None:yield start,last
            start=last=value
    if last is not None:yield start,last


class CompactLedger:
    def __init__(self,db,writer,run_id,query_digest,source,algorithm,start,end):
        self.db,self.writer,self.run_id=db,writer,run_id
        self.query_digest,self.source,self.algorithm=query_digest,source,algorithm
        self.start,self.end=start,end
        self.scope=hashlib.sha256(json.dumps(['compact-v1',query_digest,source,algorithm]).encode()).hexdigest()[:24]
        with db:
            db.execute('INSERT OR IGNORE INTO compact_scopes VALUES(?,?,?,?,?)',
                       (self.scope,query_digest,source,algorithm,time.time()))
            db.execute('INSERT OR REPLACE INTO compact_jobs VALUES(?,?)',(run_id,self.scope))

    def totals(self):
        parameters=(self.scope,self.start,self.end)
        processed=self.db.execute('SELECT coalesce(sum(min(hi,?)-max(lo,?)+1),0) FROM compact_done WHERE scope=? AND hi>=? AND lo<=?',
            (self.end,self.start,self.scope,self.start,self.end)).fetchone()[0]
        errors=self.db.execute('SELECT count(*) FROM compact_errors WHERE scope=? AND identifier BETWEEN ? AND ?',parameters).fetchone()[0]
        strong,retained=self.db.execute('SELECT coalesce(sum(strong),0),count(*) FROM compact_candidates WHERE scope=? AND identifier BETWEEN ? AND ?',parameters).fetchone()
        blocked=self.db.execute('SELECT 1 FROM compact_errors WHERE scope=? AND identifier BETWEEN ? AND ? AND http_status IN (401,403,429) LIMIT 1',parameters).fetchone()
        return {'processed':processed,'errors':errors,'consistent':strong,'blocked':bool(blocked),
                'retained':retained,'discarded':processed-errors-retained}

    def pending(self,start,end,page_size=1024):
        row=self.db.execute('SELECT lo,hi FROM compact_done WHERE scope=? AND lo<=? ORDER BY lo DESC LIMIT 1',(self.scope,start)).fetchone()
        candidate=start
        position=start-1
        if row and row['hi']>=start:candidate=row['hi']+1;position=row['lo']
        while candidate<=end:
            rows=self.db.execute('SELECT lo,hi FROM compact_done WHERE scope=? AND lo>? AND lo<=? ORDER BY lo LIMIT ?',
                                  (self.scope,position,end,page_size)).fetchall()
            if not rows:
                yield from range(candidate,end+1);return
            for row in rows:
                yield from range(candidate,min(row['lo'],end+1))
                candidate=max(candidate,row['hi']+1)
            position=rows[-1]['lo']

    def top(self,limit):
        rows=[json.loads(row[0]) for row in self.db.execute('''SELECT payload FROM compact_candidates
            WHERE scope=? AND identifier BETWEEN ? AND ? ORDER BY rank_group,rank_inliers,rank_phash,rank_ssim,identifier LIMIT ?''',
            (self.scope,self.start,self.end,limit)).fetchall()]
        rows.extend(json.loads(row[0]) for row in self.db.execute('SELECT payload FROM compact_errors WHERE scope=? AND identifier BETWEEN ? AND ? ORDER BY identifier LIMIT ?',
                    (self.scope,self.start,self.end,limit)).fetchall())
        return heapq.nsmallest(limit,rows,key=range_rank_key)

    def retry_errors(self,source):
        def retry(db):
            position=self.start-1
            while True:
                page=db.execute('SELECT identifier FROM compact_errors WHERE scope=? AND identifier>? AND identifier<=? ORDER BY identifier LIMIT 1024',
                                (self.scope,position,self.end)).fetchall()
                if not page:break
                for (identifier,) in page:
                    remove_point(db,self.scope,identifier)
                    db.execute('DELETE FROM compact_errors WHERE scope=? AND identifier=?',(self.scope,identifier))
                position=page[-1][0]
        self.writer.submit(transaction=retry)

    def commit(self,batch):
        return self.writer.submit_future(transaction=lambda db:self.persist(db,batch))

    def persist(self,db,batch):
        changed=[0]*len(batch);fresh=[];seen=set()
        for index,item in enumerate(batch):
            identifier=int(item.row['identifier'])
            previous=db.execute('SELECT hi FROM compact_done WHERE scope=? AND lo<=? ORDER BY lo DESC LIMIT 1',
                                (self.scope,identifier)).fetchone()
            if identifier in seen or (previous and previous[0]>=identifier):continue
            seen.add(identifier);changed[index]=1;fresh.append(item)
        old_weak=db.execute('''SELECT identifier,digest,rank_group,rank_inliers,rank_phash,rank_ssim
            FROM compact_candidates WHERE scope=? AND strong=0''',(self.scope,)).fetchall()
        choices=[((r[2],r[3],r[4],r[5],r[0]),r[0]) for r in old_weak]
        choices.extend((range_rank_key(item.row),int(item.row['identifier'])) for item in fresh
                       if item.row['metrics'] and not consistent(item.row))
        chosen={identifier for _,identifier in heapq.nsmallest(REVIEW_LIMIT,choices,key=lambda row:row[0])}
        removed=[(identifier,digest) for identifier,digest,*_ in old_weak if identifier not in chosen]
        for identifier,_ in removed:
            db.execute('DELETE FROM compact_candidates WHERE scope=? AND identifier=?',(self.scope,identifier))
        writes=set()
        for item in fresh:
            row=item.row;identifier=int(row['identifier'])
            if row['error']:
                row.update(raw_retained=False,asset_kind='discarded',retention_role='error')
                db.execute('INSERT OR REPLACE INTO compact_errors VALUES(?,?,?,?)',
                           (self.scope,identifier,json.dumps(row),row['fetch']['http_status']))
            elif consistent(row) or identifier in chosen:
                for sql,parameters in item.statements:
                    if sql.startswith('INSERT OR REPLACE INTO features'):key=(sql,*parameters[:2])
                    elif sql.startswith('INSERT OR REPLACE INTO pairs'):key=(sql,*parameters[:3])
                    elif sql.startswith('INSERT INTO raw_images'):key=(sql,parameters[0])
                    else:key=None
                    if key is not None:
                        if key in writes:continue
                        writes.add(key)
                    db.execute(sql,parameters)
                strong=consistent(row);rank=range_rank_key(row)
                row.update(raw_retained=True,retention_role='match' if strong else 'review')
                db.execute('INSERT OR REPLACE INTO compact_candidates VALUES(?,?,?,?,?,?,?,?,?)',
                    (self.scope,identifier,row['digest'],json.dumps(row),*rank[:4],strong))
        for lo,hi in blocks(int(item.row['identifier']) for item in fresh):add_done(db,self.scope,lo,hi)
        self.prune(db,removed)
        return changed

    def prune(self,db,removed):
        for identifier,_ in removed:
            if not db.execute('''SELECT 1 FROM compact_candidates c JOIN compact_scopes s ON s.scope=c.scope
                   WHERE s.source=? AND c.identifier=? LIMIT 1''',(self.source,identifier)).fetchone():
                db.execute('DELETE FROM acquisitions WHERE source=? AND identifier=?',(self.source,str(identifier)))
        for digest in {digest for _,digest in removed}:
            if not db.execute('SELECT 1 FROM compact_candidates WHERE digest=? LIMIT 1',(digest,)).fetchone():
                db.execute('DELETE FROM raw_images WHERE digest=?',(digest,))
                if not db.execute('SELECT 1 FROM compact_scopes WHERE query_digest=? LIMIT 1',(digest,)).fetchone():
                    db.execute('DELETE FROM features WHERE digest=?',(digest,))
            if not db.execute('''SELECT 1 FROM compact_candidates c JOIN compact_scopes s ON s.scope=c.scope
                  WHERE s.query_digest=? AND s.algorithm=? AND c.digest=? LIMIT 1''',(self.query_digest,self.algorithm,digest)).fetchone():
                db.execute('DELETE FROM pairs WHERE query_digest=? AND candidate_digest=? AND version=?',
                           (self.query_digest,digest,self.algorithm))
