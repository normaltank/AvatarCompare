from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import numpy as np

from avatar_compare.cache_lock import CacheLease
from avatar_compare.codec import compact_pack
from avatar_compare.engine import Store, pack, unpack, FEATURE_VERSION, STORAGE_VERSION
from avatar_compare.features import extract
from avatar_compare.ledger import export_records
from avatar_compare.mock_service import MockServer, dataset
from avatar_compare.pipeline_io import ByteBudget
from avatar_compare.range_engine import search_range
from avatar_compare.source import SourceConfig, FetchFailure
from avatar_compare.staged import search_staged


def clean_metrics(value):
    if isinstance(value,dict):
        return {k:clean_metrics(v) for k,v in value.items() if not k.endswith('_ms')}
    if isinstance(value,list):
        return [clean_metrics(v) for v in value]
    return value


class StagedTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='avatar-staged-')
        self.root=Path(self.temp.name)
        target,self.images,_=dataset()
        self.target=self.root/'target.png'; self.target.write_bytes(target)
        self.state=self.root/'state'

    def tearDown(self):
        self.temp.cleanup()

    def run_staged(self,server,**kwargs):
        config=kwargs.pop('config',SourceConfig(server.url,'fixture',rate=0,retries=0,keep_alive=True))
        return search_staged(self.target,kwargs.pop('start',1001),kwargs.pop('end',1016),
            kwargs.pop('state',self.state),config,workers=2,download_workers=4,**kwargs)

    def test_cold_resume_export_and_matching_equivalence(self):
        with MockServer(self.images) as server:
            fresh=self.run_staged(server,end=1018,top_limit=18)
            self.assertEqual(fresh['stats']['processed'],18)
            self.assertEqual(fresh['stats']['errors'],2)
            self.assertEqual(fresh['stats']['buffer_reserved_bytes_at_finish'],0)
            calls=sum(server.calls.values())
            resumed=self.run_staged(server,end=1018,top_limit=18)
            self.assertEqual(resumed['stats']['resumed'],18)
            self.assertEqual(sum(server.calls.values()),calls)
            paths=export_records(self.state,fresh['run_id'],self.root/'export',images=True)
            exported=list(paths['images'].glob('*.img'))
            self.assertEqual(len(exported),16)
            for path in exported:
                identifier=int(path.name.split('-')[0])
                self.assertEqual(path.read_bytes(),self.images[identifier])
            self.assertEqual(len(paths['jsonl'].read_text(encoding='utf-8').splitlines()),18)
            config=SourceConfig(server.url,'fixture',rate=0,retries=0,keep_alive=True)
            # New byte cache must also be usable by the legacy file-based route.
            legacy=search_range(self.target,1001,1018,self.state,config,workers=2,stream_results=True,top_limit=18)
            self.assertEqual(sum(server.calls.values()),calls)
            self.assertEqual({r['identifier']:clean_metrics(r['metrics']) for r in fresh['results']},
                             {r['identifier']:clean_metrics(r['metrics']) for r in legacy['results']})
            # Independent cold extraction, not just pair-cache reuse.
            independent=search_range(self.target,1001,1018,self.root/'cold-legacy',config,
                workers=2,stream_results=True,top_limit=18)
            self.assertEqual({r['identifier']:clean_metrics(r['metrics']) for r in fresh['results']},
                             {r['identifier']:clean_metrics(r['metrics']) for r in independent['results']})

    def test_old_cache_and_different_target_need_no_download(self):
        with MockServer(self.images) as server:
            config=SourceConfig(server.url,'fixture',rate=0,retries=0,keep_alive=True)
            search_range(self.target,1001,1016,self.state,config,workers=2)
            calls=sum(server.calls.values())
            first=self.run_staged(server)
            self.assertEqual(first['stats']['download_cache_hits'],16)
            self.assertEqual(sum(server.calls.values()),calls)
            self.target.write_bytes(self.images[1016])
            second=self.run_staged(server)
            self.assertNotEqual(first['run_id'],second['run_id'])
            self.assertEqual(second['stats']['download_cache_hits'],16)
            self.assertEqual(sum(server.calls.values()),calls)

    def test_corrupt_raw_blob_refetched_for_new_target(self):
        with MockServer(self.images) as server:
            self.run_staged(server,end=1002)
            calls=sum(server.calls.values())
            digest=hashlib.sha256(self.images[1002]).hexdigest()
            with closing(sqlite3.connect(self.state/'cache.sqlite3')) as db,db:
                db.execute('UPDATE raw_images SET payload=? WHERE digest=?',(b'corrupt',digest))
            self.target.write_bytes(self.images[1003])
            result=self.run_staged(server,end=1002)
            self.assertEqual(result['stats']['errors'],0)
            self.assertEqual(sum(server.calls.values()),calls+1)
            with closing(sqlite3.connect(self.state/'cache.sqlite3')) as db:
                self.assertEqual(db.execute('SELECT payload FROM raw_images WHERE digest=?',(digest,)).fetchone()[0],self.images[1002])

    def test_stop_billion_lazy_range_and_resume(self):
        with MockServer(self.images,delay=.01) as server:
            stop=threading.Event()
            first=self.run_staged(server,end=1_000_001_000,cancel=stop,
                progress=lambda event:stop.set() if event['processed']>=3 else None)
            self.assertEqual(first['status'],'stopped')
            self.assertEqual(first['stats']['total'],1_000_000_000)
            self.assertLess(first['stats']['processed'],64)
            self.assertLessEqual(first['stats']['buffer_peak_reserved_bytes'],first['stats']['buffer_limit_bytes'])
            stop.set()
            second=self.run_staged(server,end=1_000_001_000,cancel=stop)
            self.assertEqual(second['stats']['resumed'],first['stats']['processed'])
            self.assertEqual(second['stats']['requests'],0)

    def test_stop_on_429_and_explicit_retry(self):
        with MockServer(self.images,faults={1001:[429,200]}) as server:
            first=self.run_staged(server)
            self.assertEqual(first['status'],'stopped')
            second=self.run_staged(server)
            self.assertEqual(second['stats']['requests'],0)
            third=self.run_staged(server,retry_errors=True)
            self.assertEqual(third['status'],'complete')
            self.assertEqual(third['stats']['errors'],0)

    def test_partial_stop_resumes_and_finishes_remaining_items(self):
        self.images.update({number:self.images[1001] for number in range(1017,1201)})
        with MockServer(self.images,delay=.01) as server:
            stop=threading.Event()
            first=self.run_staged(server,end=1200,cancel=stop,
                progress=lambda event:stop.set() if event['processed']>=3 else None)
            self.assertEqual(first['status'],'stopped')
            self.assertLess(first['stats']['processed'],200)
            second=self.run_staged(server,end=1200)
            self.assertEqual(second['status'],'complete')
            self.assertEqual(second['stats']['processed'],200)
            self.assertEqual(second['stats']['resumed'],first['stats']['processed'])
            self.assertEqual(second['stats']['errors'],0)
            self.assertEqual(second['stats']['buffer_reserved_bytes_at_finish'],0)

    def test_bad_image_missing_and_oversize_are_recorded(self):
        self.images[1002]=b'not an image'
        with MockServer(self.images) as server:
            result=self.run_staged(server,end=1017,top_limit=20)
            self.assertEqual(result['stats']['processed'],17)
            self.assertEqual(result['stats']['errors'],2)
            config=SourceConfig(server.url,'small',max_bytes=100,rate=0,retries=0,keep_alive=True)
            result=self.run_staged(server,end=1001,config=config,buffer_bytes=100)
            self.assertEqual(result['stats']['errors'],1)
            self.assertEqual(result['stats']['buffer_peak_reserved_bytes'],0)

    def test_unknown_length_reserves_budget_before_read_and_cancels(self):
        class Response:
            status=200
            headers={}
            def __init__(self,data):self.data=data
            def read1(self,n):
                result,self.data=self.data[:n],self.data[n:]
                return result
        from contextlib import contextmanager
        @contextmanager
        def opened(source,request):
            yield Response(self.images[1001])
        with MockServer(self.images) as server, patch('avatar_compare.source.HttpSource.open',opened):
            config=SourceConfig(server.url,'unknown',max_bytes=100000,rate=0,retries=0,keep_alive=True)
            result=self.run_staged(server,config=config,buffer_bytes=100000)
            self.assertEqual(result['stats']['processed'],16)
            self.assertEqual(result['stats']['errors'],0)
            self.assertEqual(result['stats']['buffer_peak_reserved_bytes'],100000)
        budget=ByteBudget(10); cancel=threading.Event(); lease=budget.acquire(10,cancel)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending=pool.submit(budget.acquire,1,cancel)
            cancel.set()
            with self.assertRaises(FetchFailure):pending.result(timeout=2)
        lease.release();lease.release()
        self.assertEqual(budget.used,0)

    def test_failed_atomic_item_rolls_back_every_component(self):
        # Force the final item insert to fail after raw/features/acquisition writes.
        from avatar_compare.ledger import RangeLedger
        original=RangeLedger.statements
        def failure(ledger,rows):
            return original(ledger,rows)+[('INSERT INTO nonexistent VALUES(1)',())]
        with MockServer(self.images) as server,patch.object(RangeLedger,'statements',failure):
            with self.assertRaises(sqlite3.OperationalError):self.run_staged(server,start=1002,end=1002)
        with closing(sqlite3.connect(self.state/'cache.sqlite3')) as db:
            for table in ['raw_images','acquisitions','pairs','fast_items']:
                self.assertEqual(db.execute(f'SELECT count(*) FROM {table}').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM features').fetchone()[0],1)
        with MockServer(self.images) as server:
            self.assertEqual(self.run_staged(server,start=1002,end=1002)['stats']['processed'],1)

    def test_cache_lease_reentrant_and_other_process_excluded(self):
        with CacheLease(self.state),CacheLease(self.state):
            script='from pathlib import Path; from avatar_compare.cache_lock import CacheLease; CacheLease(Path(__import__("sys").argv[1])).__enter__()'
            blocked=subprocess.run([sys.executable,'-c',script,str(self.state)],capture_output=True,timeout=10)
            self.assertNotEqual(blocked.returncode,0)
            with ThreadPoolExecutor(max_workers=1) as pool:
                with self.assertRaises(ValueError):pool.submit(CacheLease(self.state).__enter__).result()
            with CacheLease(self.root/'other'):pass
        with CacheLease(self.state):pass

    def test_lossless_codec_and_old_feature_payload(self):
        feature=extract(self.target.read_bytes())
        for candidate in [feature,replace(feature,descriptors=feature.descriptors+.125)]:
            restored=unpack(compact_pack(candidate))
            for field in ['gray','points','descriptors']:
                left,right=getattr(candidate,field),getattr(restored,field)
                self.assertEqual(left.dtype,right.dtype)
                self.assertEqual(left.tobytes(),right.tobytes())
            self.assertEqual(candidate.preview,restored.preview)
        store=Store(self.state/'cache.sqlite3')
        try:
            with store.db():
                store.db().execute('INSERT INTO features VALUES(?,?,?,?)',
                    (feature.digest,FEATURE_VERSION,f'{feature.phash:016x}',pack(feature)))
            store.extractor=lambda data:self.fail('old feature should be reused')
            restored,hit=store.get(self.target)
            self.assertEqual(hit,'content')
            self.assertEqual(restored.descriptors.tobytes(),feature.descriptors.tobytes())
            self.assertEqual(store.db().execute('SELECT count(*) FROM features WHERE version=?',(STORAGE_VERSION,)).fetchone()[0],0)
        finally:store.close()

    def test_full_speed_cli_uses_staged_and_keeps_original_export(self):
        with MockServer(self.images) as server:
            completed=subprocess.run([sys.executable,'-m','avatar_compare','--full-speed',
                '--target',str(self.target),'--start','1001','--end','1016','--endpoint',server.url,
                '--state-dir',str(self.state),'--output',str(self.root/'report')],capture_output=True,timeout=30)
            self.assertEqual(completed.returncode,0,completed.stderr)
            text=completed.stdout.decode('utf-8',errors='replace')
            result=json.loads(text[text.index('{'):])
            self.assertEqual(result['stats']['pipeline'],'staged')
            self.assertEqual(result['stats']['workers'],min(8,os.cpu_count() or 1))
            self.assertEqual(result['stats']['errors'],0)
            self.assertTrue(Path(result['report']).is_file())


if __name__=='__main__':unittest.main()
