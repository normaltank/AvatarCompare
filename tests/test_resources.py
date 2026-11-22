from __future__ import annotations

from pathlib import Path
import tempfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import sqlite3
import threading
import os
import json
import subprocess
import sys
import unittest

from avatar_compare.mock_service import MockServer, dataset
from avatar_compare.range_engine import search_range
from avatar_compare.source import SourceConfig
from avatar_compare.writer import SharedWriter
from avatar_compare.ledger import RangeLedger


class AllResourceTests(unittest.TestCase):
    def test_high_compute_and_download_concurrency_preserves_results(self):
        with tempfile.TemporaryDirectory(prefix='avatar-resources-') as temporary:
            root = Path(temporary)
            target, images, _ = dataset()
            query = root/'target.png'
            query.write_bytes(target)
            workers = min(32, max(4, os.cpu_count() or 1))
            with MockServer(images) as server:
                result = search_range(query, 1001, 1016, root/'state',
                    SourceConfig(server.url, 'fixture', rate=0, retries=0, keep_alive=True),
                    workers=workers, download_workers=128, stream_results=True, shared_writer=True)
                self.assertEqual(result['stats']['processed'], 16)
                self.assertEqual(result['stats']['errors'], 0)
                self.assertEqual(result['stats']['workers'], workers)
                self.assertEqual(result['stats']['download_workers'], 128)
                self.assertLessEqual(result['stats']['peak_compute'], workers)
                self.assertGreater(result['stats']['writer_peak_batch'], 1)
                self.assertEqual(next(r for r in result['results'] if r['identifier']=='1001')['metrics']['exact_file'], True)
                resumed = search_range(query, 1001, 1016, root/'state',
                    SourceConfig(server.url, 'fixture', rate=0, retries=0, keep_alive=True),
                    workers=workers, download_workers=128, stream_results=True, shared_writer=True)
                self.assertEqual(resumed['stats']['resumed'], 16)
                self.assertEqual(resumed['stats']['requests'], 0)

    def test_writer_ack_is_durable_and_failed_scope_rolls_back(self):
        with tempfile.TemporaryDirectory(prefix='avatar-writer-') as temporary:
            path = Path(temporary)/'test.db'
            writer = SharedWriter(path)
            try:
                writer.submit(script='CREATE TABLE records(id INTEGER PRIMARY KEY);')
                barrier = threading.Barrier(16)
                def save(number):
                    barrier.wait()
                    writer.submit([('INSERT INTO records VALUES(?)', (number,))])
                    with closing(sqlite3.connect(path)) as reader:
                        return reader.execute('SELECT id FROM records WHERE id=?', (number,)).fetchone()[0]
                with ThreadPoolExecutor(max_workers=16) as pool:
                    self.assertEqual(sorted(pool.map(save, range(16))), list(range(16)))
                with self.assertRaises(sqlite3.OperationalError):
                    writer.submit([('INSERT INTO records VALUES(?)', (17,)), ('INSERT INTO missing VALUES(1)', ())])
                writer.submit([('INSERT INTO records VALUES(?)', (18,))])
                with closing(sqlite3.connect(path)) as reader:
                    self.assertEqual(reader.execute('SELECT COUNT(*) FROM records WHERE id=17').fetchone()[0], 0)
                    self.assertEqual(reader.execute('SELECT COUNT(*) FROM records').fetchone()[0], 17)
                self.assertGreater(writer.peak_batch, 1)
            finally:
                writer.close()

    def test_batch_duplicate_ids_do_not_double_count(self):
        with closing(sqlite3.connect(':memory:')) as db:
            db.row_factory = sqlite3.Row
            ledger = RangeLedger(db, 'duplicates')
            row = {'identifier': '1', 'metrics': None, 'error': 'fixture', 'fetch': {'http_status': 500}}
            self.assertEqual(ledger.save_many([row, row, {**row, 'identifier': '2'}]), [True, False, True])
            self.assertEqual(ledger.totals()['processed'], 2)
            self.assertEqual(ledger.totals()['errors'], 2)

    def test_shared_writer_stop_resume_preserves_all_counts(self):
        with tempfile.TemporaryDirectory(prefix='avatar-resource-resume-') as temporary:
            root = Path(temporary)
            target, images, _ = dataset()
            images.update({number: target for number in range(1017,1065)})
            query = root/'target.png'
            query.write_bytes(target)
            workers = min(8, max(4, os.cpu_count() or 1))
            with MockServer(images) as server:
                config = SourceConfig(server.url, 'resume', rate=0, keep_alive=True)
                stop = threading.Event()
                first = search_range(query, 1001, 1064, root/'state', config, workers=workers, download_workers=32,
                    stream_results=True, shared_writer=True, cancel=stop,
                    progress=lambda event: stop.set() if event['processed'] >= 5 else None)
                self.assertEqual(first['status'], 'stopped')
                second = search_range(query, 1001, 1064, root/'state', config, workers=workers, download_workers=32,
                    stream_results=True, shared_writer=True)
                self.assertEqual(second['stats']['processed'], 64)
                self.assertEqual(second['stats']['resumed'], first['stats']['processed'])
                self.assertEqual(second['stats']['errors'], 0)

    def test_cli_hardware_sized_mode(self):
        with tempfile.TemporaryDirectory(prefix='avatar-resource-cli-') as temporary:
            root = Path(temporary)
            target, images, _ = dataset()
            query = root/'target.png'
            query.write_bytes(target)
            with MockServer(images) as server:
                completed = subprocess.run([sys.executable, '-m', 'avatar_compare', '--max-resources',
                    '--target', str(query), '--start', '1001', '--end', '1016', '--endpoint', server.url,
                    '--state-dir', str(root/'state'), '--output', str(root/'out')], capture_output=True, timeout=30)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                text = completed.stdout.decode('utf-8', errors='replace')
                result = json.loads(text[text.index('{'):])
                cpus = min(os.cpu_count() or 1,256)
                self.assertEqual(result['stats']['workers'], cpus)
                self.assertEqual(result['stats']['download_workers'], min(256,cpus*4))
                self.assertGreater(result['stats']['writer_submissions'], 0)
                self.assertEqual(result['stats']['compute_backend'], 'process')
                self.assertGreater(result['stats']['compute_processes'], 0)


if __name__ == '__main__':
    unittest.main()
