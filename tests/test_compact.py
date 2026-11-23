from __future__ import annotations
from contextlib import closing
import hashlib
from itertools import islice
import json
from pathlib import Path
import random
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from avatar_compare.compact_ledger import CompactLedger,add_done,remove_point,schema
from avatar_compare.engine import Store,COMPARE_VERSION
from avatar_compare.history import inspect
from avatar_compare.ledger import export_records
from avatar_compare.mock_service import dataset,MockServer,encode,synthetic_picture
from avatar_compare.report import write_reports
from avatar_compare.source import SourceConfig
from avatar_compare.staged import search_staged
from test_staged import clean_metrics


class IntervalTests(unittest.TestCase):
    def test_transaction_callbacks_share_ordered_commit_and_rollback(self):
        from avatar_compare.writer import SharedWriter
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'writer.sqlite3';writer=SharedWriter(path,batch_wait=.02)
            gate=threading.Event();entered=threading.Event()
            try:
                writer.submit(script='CREATE TABLE numbers(value INTEGER);')
                def hold(db):entered.set();gate.wait(timeout=5)
                held=writer.submit_future(transaction=hold)
                self.assertTrue(entered.wait(timeout=2))
                def action(value):
                    def apply(db):
                        db.execute('INSERT INTO numbers VALUES(?)',(value,))
                        return db.execute('SELECT count(*) FROM numbers').fetchone()[0]
                    return apply
                futures=[writer.submit_future(transaction=action(n)) for n in range(8)]
                gate.set();held.result(timeout=5)
                self.assertEqual([f.result(timeout=5) for f in futures],list(range(1,9)))
                self.assertGreaterEqual(writer.peak_batch,8)
                def broken(db):
                    db.execute('INSERT INTO numbers VALUES(99)')
                    db.execute('INSERT INTO missing_table VALUES(1)')
                with self.assertRaises(sqlite3.OperationalError):writer.submit(transaction=broken)
                with closing(sqlite3.connect(path)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM numbers').fetchone()[0],8)
            finally:gate.set();writer.close()

    def test_legacy_history_never_invents_requested_bounds_or_fills_holes(self):
        from avatar_compare.ledger import RangeLedger
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);store=Store(root/'cache.sqlite3')
            try:
                db=store.db();ledger=RangeLedger(db,'legacy')
                with db:db.execute('INSERT INTO runs VALUES(?,?,?,?,?,?)',('legacy','target.png','a'*64,'stopped',100,0))
                for identifier in [1000,1002]:
                    ledger.save({'identifier':str(identifier),'metrics':None,'error':'fixture','fetch':{'http_status':404}})
                old=inspect(root,run_id='legacy')[0]
                self.assertIsNone(old['requested_range']);self.assertIsNone(old['processed_ranges'])
                self.assertEqual(old['processed'],2)
            finally:store.close()

    def test_random_merge_remove_and_pending_match_a_set(self):
        with tempfile.TemporaryDirectory() as temporary:
            store=Store(Path(temporary)/'cache.sqlite3');store.enable_shared_writer()
            try:
                db=store.db();schema(db)
                ledger=CompactLedger(db,store.writer,'fixture','query','source',COMPARE_VERSION,0,127)
                expected=set();rng=random.Random(42)
                for _ in range(100):
                    value=rng.randrange(128)
                    if rng.random()<.7:
                        end=min(127,value+rng.randrange(1,8));expected.update(range(value,end+1))
                        store.writer.submit(transaction=lambda conn:add_done(conn,ledger.scope,value,end))
                    else:
                        expected.discard(value)
                        store.writer.submit(transaction=lambda conn:remove_point(conn,ledger.scope,value))
                    self.assertEqual(list(ledger.pending(0,127,page_size=3)),sorted(set(range(128))-expected))
                    self.assertEqual(ledger.totals()['processed'],len(expected))
                    rows=db.execute('SELECT lo,hi FROM compact_done WHERE scope=? ORDER BY lo',(ledger.scope,)).fetchall()
                    self.assertTrue(all(left['hi']+1<right['lo'] for left,right in zip(rows,rows[1:])))
            finally:store.close()

    def test_holes_and_64bit_boundaries_do_not_use_maximum_as_frontier(self):
        maximum=2**63-1
        with tempfile.TemporaryDirectory() as temporary:
            store=Store(Path(temporary)/'cache.sqlite3');store.enable_shared_writer()
            try:
                db=store.db();schema(db)
                ledger=CompactLedger(db,store.writer,'fixture','q','s',COMPARE_VERSION,maximum-4,maximum)
                def save(conn):
                    add_done(conn,ledger.scope,0,1);add_done(conn,ledger.scope,maximum-2,maximum)
                store.writer.submit(transaction=save)
                self.assertEqual(list(ledger.pending(maximum-4,maximum)),[maximum-4,maximum-3])
                self.assertEqual(ledger.totals()['processed'],3)
                self.assertEqual(list(islice(ledger.pending(0,maximum),4)),[2,3,4,5])
            finally:store.close()


class CompactTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='avatar-compact-')
        self.root=Path(self.temp.name);target,self.images,_=dataset()
        self.target=self.root/'target.png';self.target.write_bytes(target)
        self.state=self.root/'compact'

    def tearDown(self):self.temp.cleanup()

    def run_scan(self,server,**kwargs):
        config=kwargs.pop('config',SourceConfig(server.url,'fixture',rate=0,retries=0,keep_alive=True))
        return search_staged(self.target,kwargs.pop('start',1001),kwargs.pop('end',1016),kwargs.pop('state',self.state),
            config,workers=kwargs.pop('workers',2),download_workers=kwargs.pop('download_workers',4),
            compact=kwargs.pop('compact',True),share_computations=kwargs.pop('share_computations',True),**kwargs)

    def test_equivalent_evidence_images_errors_export_and_history(self):
        with MockServer(self.images) as server:
            full=self.run_scan(server,end=1018,state=self.root/'full',compact=False,top_limit=20)
            result=self.run_scan(server,end=1018,top_limit=20)
            self.assertEqual({r['identifier']:clean_metrics(r['metrics']) for r in full['results']},
                             {r['identifier']:clean_metrics(r['metrics']) for r in result['results']})
            calls=sum(server.calls.values());resumed=self.run_scan(server,end=1018)
            self.assertEqual(sum(server.calls.values()),calls)
            self.assertEqual(resumed['stats']['resumed'],18)
            history=inspect(self.state)[0]
            self.assertEqual(history['requested_range'],[1001,1018])
            self.assertEqual(history['processed_ranges'],[(1001,1018)])
            self.assertEqual(history['successful_ranges'],[(1001,1016)])
            self.assertEqual(history['failed_identifiers'],[1017,1018])
            exported=export_records(self.state,result['run_id'],self.root/'export',images=True)
            rows=[json.loads(line) for line in exported['jsonl'].read_text(encoding='utf-8').splitlines()]
            self.assertEqual(len(rows),18)
            for image in exported['images'].iterdir():
                self.assertEqual(image.read_bytes(),self.images[int(image.name.split('-')[0])])
            self.assertEqual(len(exported['errors'].read_text(encoding='utf-8').splitlines()),2)
            self.assertFalse(json.loads(exported['manifest'].read_text(encoding='utf-8'))['complete_per_id_records'])
            reports=write_reports(result,self.root/'report',limit=1)
            html=reports['html'].read_text(encoding='utf-8')
            self.assertIn('省空间模式',html);self.assertNotIn('完整 18 项记录',html)
            self.assertEqual(len(reports['errors'].read_text(encoding='utf-8').splitlines()),2)

    def test_source_catalog_captures_discarded_images_and_reuses_them_for_a_new_target(self):
        from PIL import Image

        uniform = encode(Image.new("RGB", (100, 100), "blue"))
        identifiers = range(1001, 1129)
        with MockServer(dict.fromkeys(identifiers, uniform)) as server:
            ordinary = self.run_scan(server, end=1128)
            self.assertEqual(ordinary["stats"]["requests"], 128)

            captured = self.run_scan(server, end=1128, capture_source=True)
            self.assertEqual(captured["stats"]["resumed"], 0)
            self.assertEqual(captured["stats"]["requests"], 28)
            self.assertTrue(inspect(self.state, run_id=captured["run_id"])[0]["capture_source"])
            with closing(sqlite3.connect(self.state / "cache.sqlite3")) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM source_catalog").fetchone()[0], 128)
                self.assertEqual(db.execute("SELECT count(*) FROM raw_images").fetchone()[0], 1)

            self.target.write_bytes(self.images[1015])
            replayed = self.run_scan(server, end=1128)
            self.assertEqual(replayed["stats"]["resumed"], 0)
            self.assertEqual(replayed["stats"]["download_cache_hits"], 128)
            self.assertEqual(replayed["stats"]["requests"], 0)

    def test_source_catalog_repairs_bad_bytes_and_stays_within_its_batch(self):
        with MockServer(self.images) as server:
            self.run_scan(server, start=1001, end=1001, capture_source=True)
            expected = hashlib.sha256(self.images[1001]).hexdigest()
            for seed, payload in enumerate((b"corrupt", "unexpected text", None)):
                with self.subTest(payload=payload):
                    with closing(sqlite3.connect(self.state / "cache.sqlite3")) as db, db:
                        db.execute("UPDATE raw_images SET payload=? WHERE digest=?", (payload, expected))
                    self.target.write_bytes(encode(synthetic_picture(seed)))
                    repaired = self.run_scan(server, start=1001, end=1001)
                    self.assertEqual(repaired["stats"]["requests"], 1)
                    with closing(sqlite3.connect(self.state / "cache.sqlite3")) as db:
                        stored, = db.execute("SELECT payload FROM raw_images WHERE digest=?", (expected,)).fetchone()
                    self.assertEqual(stored, self.images[1001])

            fresh = SourceConfig(server.url, "fresh", rate=0, retries=0, keep_alive=True)
            refreshed = self.run_scan(server, start=1001, end=1001, config=fresh)
            self.assertEqual(refreshed["stats"]["requests"], 1)
            self.assertEqual(refreshed["stats"]["download_cache_hits"], 0)

    def test_source_catalog_keeps_original_after_candidate_eviction(self):
        from PIL import Image

        def solid(color):
            return encode(Image.new("RGB", (100, 100), color))

        self.target.write_bytes(solid("white"))
        weak = solid("black")
        digest = hashlib.sha256(weak).hexdigest()
        images = {1001: weak, **dict.fromkeys(range(1002, 1102), solid("#dddddd"))}
        with MockServer(images) as server:
            self.run_scan(server, end=1001)
            self.run_scan(server, end=1101, capture_source=True)
            self.run_scan(server, end=1101)
            with closing(sqlite3.connect(self.state / "cache.sqlite3")) as db, db:
                self.assertIsNone(db.execute("SELECT 1 FROM compact_candidates WHERE digest=?", (digest,)).fetchone())
                # An older release issues this unguarded delete during cleanup.
                db.execute("DELETE FROM raw_images WHERE digest=?", (digest,))
                self.assertEqual(db.execute("SELECT payload FROM raw_images WHERE digest=?", (digest,)).fetchone()[0], weak)
            self.target.write_bytes(solid("red"))
            replay = self.run_scan(server, end=1001)
            self.assertEqual(replay["stats"]["requests"], 0)
            self.assertEqual(replay["stats"]["download_cache_hits"], 1)

    def test_retry_failed_point_and_preserve_adjacent_success(self):
        with MockServer(self.images,faults={1003:[503,200]}) as server:
            first=self.run_scan(server)
            self.assertEqual(first['stats']['errors'],1)
            self.assertEqual(inspect(self.state)[0]['successful_ranges'],[(1001,1002),(1004,1016)])
            calls=sum(server.calls.values())
            second=self.run_scan(server,retry_errors=True)
            self.assertEqual(second['stats']['resumed'],15)
            self.assertEqual(second['stats']['errors'],0)
            self.assertEqual(sum(server.calls.values()),calls+1)
            self.assertEqual(inspect(self.state)[0]['processed_ranges'],[(1001,1016)])

    def test_429_stops_until_explicit_retry(self):
        with MockServer(self.images,faults={1001:[429,200]}) as server:
            first=self.run_scan(server)
            self.assertEqual(first['status'],'stopped')
            second=self.run_scan(server)
            self.assertEqual(second['stats']['requests'],0)
            third=self.run_scan(server,retry_errors=True)
            self.assertEqual(third['status'],'complete')
            self.assertEqual(third['stats']['errors'],0)

    def test_billion_lazy_stop_and_overlap_resume(self):
        with MockServer(self.images,delay=.01) as server:
            stop=threading.Event()
            first=self.run_scan(server,end=1_000_001_000,cancel=stop,
                progress=lambda event:stop.set() if event['processed']>=3 else None)
            self.assertEqual(first['status'],'stopped')
            self.assertLess(first['stats']['processed'],64)
            self.assertLessEqual(first['stats']['buffer_peak_reserved_bytes'],first['stats']['buffer_limit_bytes'])
            self.assertEqual(inspect(self.state)[0]['requested_range'],[1001,1_000_001_000])
            # A new smaller requested interval must not trust a maximum-ID marker.
            second=self.run_scan(server,end=1016)
            self.assertEqual(second['status'],'complete')
            self.assertEqual(second['stats']['processed'],16)
            self.assertGreater(second['stats']['resumed'],0)

    def test_transaction_fault_keeps_evidence_and_progress_atomic(self):
        with MockServer(self.images) as server:
            with patch.object(CompactLedger,'prune',side_effect=sqlite3.OperationalError('injected before commit')):
                with self.assertRaises(sqlite3.OperationalError):self.run_scan(server,start=1002,end=1002)
            with closing(sqlite3.connect(self.state/'cache.sqlite3')) as db:
                for table in ['raw_images','acquisitions','pairs','compact_done','compact_candidates','compact_errors']:
                    self.assertEqual(db.execute(f'SELECT count(*) FROM {table}').fetchone()[0],0,table)
                self.assertEqual(db.execute('SELECT count(*) FROM features').fetchone()[0],1)
            self.assertEqual(self.run_scan(server,start=1002,end=1002)['stats']['processed'],1)

    def test_existing_full_and_compact_profiles_cannot_be_mixed(self):
        with MockServer(self.images) as server:
            full=self.run_scan(server,state=self.root/'full',compact=False)
            with self.assertRaisesRegex(ValueError,'新目录'):
                self.run_scan(server,state=self.root/'full')
            self.assertEqual(self.run_scan(server,state=self.root/'full',compact=False)['stats']['resumed'],16)
            self.run_scan(server)
            with self.assertRaisesRegex(ValueError,'不能混用'):self.run_scan(server,compact=False)
            self.assertEqual(self.run_scan(server)['stats']['resumed'],16)

    def test_pruning_overlap_and_target_batch_scope_are_correct(self):
        self.images={1001+i:encode(synthetic_picture(2000+i).resize((100,100))) for i in range(200)}
        with MockServer(self.images) as server:
            first=self.run_scan(server,end=1128,workers=4,download_workers=16)
            self.assertGreater(first['stats']['discarded_unmatched'],0)
            with closing(sqlite3.connect(self.state/'cache.sqlite3')) as db:
                old={row[0] for row in db.execute('SELECT digest FROM raw_images')}
            calls=sum(server.calls.values())
            second=self.run_scan(server,end=1200,workers=4,download_workers=16,top_limit=100)
            self.assertEqual(second['stats']['resumed'],128)
            self.assertEqual(sum(server.calls.values()),calls+72)
            with closing(sqlite3.connect(self.state/'cache.sqlite3')) as db:
                retained={row[0] for row in db.execute('SELECT DISTINCT digest FROM compact_candidates')}
                raw={row[0] for row in db.execute('SELECT digest FROM raw_images')}
                features={row[0] for row in db.execute('SELECT digest FROM features')}
                queries={row[0] for row in db.execute('SELECT query_digest FROM compact_scopes')}
                self.assertEqual(raw,retained)
                self.assertEqual(features,retained|queries)
                self.assertTrue(old-raw,'The fixture must exercise actual review-image eviction')
            full=self.run_scan(server,end=1200,state=self.root/'full',compact=False,workers=4,download_workers=16,top_limit=100)
            self.assertEqual([(r['identifier'],clean_metrics(r['metrics'])) for r in second['results']],
                             [(r['identifier'],clean_metrics(r['metrics'])) for r in full['results']])
            self.target.write_bytes(self.images[1200])
            different=self.run_scan(server,end=1200,workers=4,download_workers=16)
            self.assertEqual(different['stats']['resumed'],0)
            self.assertGreater(different['stats']['requests'],0)
            fresh=SourceConfig(server.url,'new-batch',rate=0,retries=0,keep_alive=True)
            revision=self.run_scan(server,end=1200,config=fresh,workers=4,download_workers=16)
            self.assertEqual(revision['stats']['resumed'],0)
            self.assertEqual(revision['stats']['requests'],200)
            with closing(sqlite3.connect(self.state/'cache.sqlite3')) as db:
                pinned={r[0] for r in db.execute('SELECT DISTINCT digest FROM compact_candidates')}
                self.assertEqual({r[0] for r in db.execute('SELECT digest FROM raw_images')},pinned)
                query_pins={r[0] for r in db.execute('SELECT query_digest FROM compact_scopes')}
                self.assertEqual({r[0] for r in db.execute('SELECT digest FROM features')},pinned|query_pins)

    def test_cli_and_readonly_history_do_not_download_again(self):
        with MockServer(self.images) as server:
            args=[sys.executable,'-m','avatar_compare','--full-speed','--compact','--share-computations',
                '--target',str(self.target),'--start','1001','--end','1016','--endpoint',server.url,
                '--state-dir',str(self.state),'--output',str(self.root/'out')]
            result=subprocess.run(args,capture_output=True,timeout=30)
            self.assertEqual(result.returncode,0,result.stderr)
            calls=sum(server.calls.values())
            history=subprocess.run([sys.executable,'-m','avatar_compare.history','--state-dir',str(self.state),'--json'],capture_output=True,timeout=15)
            self.assertEqual(history.returncode,0,history.stderr)
            payload=json.loads(history.stdout.decode('utf-8'))
            self.assertEqual(payload[0]['jobs'][0]['processed'],16)
            self.assertEqual(sum(server.calls.values()),calls)

    def test_bad_images_and_oversize_are_failures_not_retained_images(self):
        self.images[1002]=b'bad image'
        with MockServer(self.images) as server:
            result=self.run_scan(server,end=1003)
            self.assertEqual(result['stats']['errors'],1)
            invalid=hashlib.sha256(self.images[1002]).hexdigest()
            with closing(sqlite3.connect(self.state/'cache.sqlite3')) as db:
                self.assertIsNone(db.execute('SELECT 1 FROM raw_images WHERE digest=?',(invalid,)).fetchone())
            cfg=SourceConfig(server.url,'small-response',max_bytes=100,rate=0,retries=0,keep_alive=True)
            small=self.run_scan(server,end=1001,config=cfg,buffer_bytes=100)
            self.assertEqual(small['stats']['errors'],1)
            self.assertEqual(inspect(self.state,run_id=small['run_id'])[0]['successful_ranges'],[])

    def test_process_backend_retains_matches_and_progress(self):
        import os
        with MockServer(self.images) as server,patch.dict(os.environ,{'OPENBLAS_NUM_THREADS':'1','MKL_NUM_THREADS':'1','OMP_NUM_THREADS':'1'}):
            result=self.run_scan(server,compute_backend='process')
            self.assertEqual(result['stats']['processed'],16)
            self.assertEqual(result['stats']['errors'],0)
            self.assertGreater(result['stats']['compute_processes'],0)
            self.assertEqual(result['stats']['consistent'],6)
