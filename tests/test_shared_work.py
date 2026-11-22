from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from avatar_compare.engine import Store
from avatar_compare.features import extract
from avatar_compare.mock_service import dataset,MockServer
from avatar_compare.shared_work import SharedComparisons
from avatar_compare.source import SourceConfig
from avatar_compare.staged import search_staged
import test_staged as staged_tests
clean_metrics=staged_tests.clean_metrics


class SharedStageTests(staged_tests.StagedTests):
    def run_staged(self,server,**kwargs):
        kwargs['share_computations']=True
        return super().run_staged(server,**kwargs)

    def test_cli_sharing_is_explicit_and_preserves_every_identifier(self):
        self.images[1002]=self.images[1001]
        with MockServer(self.images) as server:
            command=[sys.executable,'-m','avatar_compare','--full-speed','--share-computations',
                '--target',str(self.target),'--start','1001','--end','1016','--endpoint',server.url,
                '--state-dir',str(self.state),'--output',str(self.root/'report')]
            completed=subprocess.run(command,capture_output=True,timeout=30)
            self.assertEqual(completed.returncode,0,completed.stderr)
            output=completed.stdout.decode('utf-8',errors='replace')
            result=json.loads(output[output.index('{'):])
            self.assertEqual(result['stats']['processed'],16)
            self.assertGreaterEqual(result['stats']['work_hot_hits']+result['stats']['work_inflight_hits'],1)
            self.assertEqual(sum(server.calls.values()),16)
            with closing(sqlite3.connect(self.state/'cache.sqlite3')) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM fast_items').fetchone()[0],16)
            rejected=subprocess.run(command+['--pipeline','legacy'],capture_output=True,timeout=10)
            self.assertEqual(rejected.returncode,2)
            self.assertEqual(sum(server.calls.values()),16)

    def test_process_extraction_can_share_work_and_resume(self):
        self.images.update({number:self.images[1003] for number in range(1017,1065)})
        with MockServer(self.images) as server,patch.dict(os.environ,{'OPENBLAS_NUM_THREADS':'1','MKL_NUM_THREADS':'1','OMP_NUM_THREADS':'1'}):
            config=SourceConfig(server.url,'process-sharing',rate=0,retries=0,keep_alive=True)
            first=search_staged(self.target,1001,1064,self.state,config,workers=2,download_workers=8,
                                share_computations=True,compute_backend='process')
            self.assertEqual(first['stats']['processed'],64)
            self.assertEqual(first['stats']['errors'],0)
            self.assertGreater(first['stats']['compute_processes'],0)
            self.assertLessEqual(first['stats']['work_computations'],16)
            second=search_staged(self.target,1001,1064,self.state,config,workers=2,download_workers=8,
                                 share_computations=True,compute_backend='process')
            self.assertEqual(second['stats']['resumed'],64)
            self.assertEqual(second['stats']['requests'],0)


class WorkSharingTests(unittest.TestCase):
    def test_real_precommit_work_is_once_but_every_caller_gets_writes(self):
        target,images,_=dataset();query=extract(target);calls=0;guard=threading.Lock()
        gate=threading.Event()
        with tempfile.TemporaryDirectory(prefix='avatar-work-') as temporary:
            store=Store(Path(temporary)/'cache.sqlite3')
            def measured(data):
                nonlocal calls
                with guard:calls+=1
                gate.wait(timeout=5)
                return extract(data)
            store.extractor=measured;sharing=SharedComparisons(store.prepare_bytes)
            try:
                with ThreadPoolExecutor(max_workers=8) as pool:
                    tasks=[pool.submit(sharing.prepare,query,images[1003]) for _ in range(8)]
                    try:
                        deadline=time.monotonic()+3
                        while sharing.snapshot()['work_inflight_hits']<7 and time.monotonic()<deadline:time.sleep(.005)
                        self.assertEqual(sharing.snapshot()['work_inflight_hits'],7)
                    finally:gate.set()
                    results=[task.result(timeout=5) for task in tasks]
                self.assertEqual(calls,1)
                self.assertEqual(len(results),8)
                for row,writes in results:
                    self.assertTrue(writes,'Each result must be safe if committed before its producer')
                    self.assertEqual(clean_metrics(row['metrics']),clean_metrics(results[0][0]['metrics']))
                # Commit a waiter first, exactly as out-of-order stage completion can do.
                with store.db():
                    for sql,parameters in results[-1][1]:store.db().execute(sql,parameters)
                sharing.committed(query.digest,hashlib.sha256(images[1003]).hexdigest())
                row,writes=sharing.prepare(query,images[1003])
                self.assertEqual(writes,[])
                self.assertEqual(calls,1)
                results[0][0]['path']='mutated caller'
                self.assertEqual(row['path'],'')
                original=row['metrics']['geometry']['reason'] if 'reason' in row['metrics']['geometry'] else None
                row['metrics']['geometry']['reason']='caller annotation'
                next_row,_=sharing.prepare(query,images[1003])
                self.assertEqual(next_row['metrics']['geometry'].get('reason'),original)
            finally:gate.set();store.close()

    def test_failed_owner_releases_waiters_and_next_attempt_can_retry(self):
        query=extract(dataset()[0]);calls=0;gate=threading.Event();barrier=threading.Barrier(4)
        def failed(query,data):
            nonlocal calls
            calls+=1;gate.wait(timeout=5);raise ValueError('fixture failure')
        sharing=SharedComparisons(failed)
        def work():barrier.wait();return sharing.prepare(query,b'candidate')
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures=[pool.submit(work) for _ in range(4)]
            try:
                deadline=time.monotonic()+3
                while sharing.snapshot()['work_inflight_hits']<3 and time.monotonic()<deadline:time.sleep(.005)
                self.assertEqual(sharing.snapshot()['work_inflight_hits'],3)
            finally:gate.set()
            for future in futures:
                with self.assertRaisesRegex(ValueError,'fixture failure'):future.result(timeout=5)
        self.assertEqual(calls,1)
        with self.assertRaises(ValueError):sharing.prepare(query,b'candidate')
        self.assertEqual(calls,2)
        self.assertEqual(len(sharing.pending),0)

    def test_cache_bounds_and_key_scope(self):
        target,images,_=dataset();query=extract(target);calls=[]
        def compute(query,data):
            calls.append(data)
            return {'metrics':{'exact_file':False},'path':'','digest':hashlib.sha256(data).hexdigest()},[]
        sharing=SharedComparisons(compute,capacity=2,byte_limit=1024)
        for value in [b'a',b'b',b'c',b'c',b'a']:sharing.prepare(query,value)
        self.assertEqual(len(calls),4)
        self.assertLessEqual(sharing.snapshot()['work_peak_entries'],2)
        self.assertLessEqual(sharing.snapshot()['work_peak_payload_bytes'],1024)
        sharing.prepare(extract(images[1003]),b'a')
        self.assertEqual(len(calls),5,'Different target must not reuse comparison')
        no_cache=SharedComparisons(compute,capacity=2,byte_limit=1)
        no_cache.prepare(query,b'a');no_cache.prepare(query,b'a')
        self.assertEqual(no_cache.snapshot()['work_cache_entries'],0)


if __name__=='__main__':unittest.main()
