from __future__ import annotations

import csv
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

from avatar_compare.ledger import RangeLedger, export_records
from avatar_compare.mock_service import MockServer, dataset
from avatar_compare.range_engine import search_range
from avatar_compare.report import write_reports
from avatar_compare.source import SourceConfig


class FastRangeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='avatar-fast-')
        self.root = Path(self.temp.name)
        self.target = self.root/'target.png'
        target, self.images, _ = dataset()
        self.target.write_bytes(target)
        self.state = self.root/'state'

    def tearDown(self):
        self.temp.cleanup()

    def run_fast(self, server, start=1001, end=1016, **kwargs):
        config = kwargs.pop('config', SourceConfig(server.url, 'test-v1', rate=0, keep_alive=True, retries=1))
        return search_range(self.target, start, end, self.state, config, workers=2,
                            stream_results=True, top_limit=5, **kwargs)

    def test_all_results_on_disk_top_bound_resume_and_complete_export(self):
        with MockServer(self.images) as server:
            first = self.run_fast(server, end=1018, download_workers=8)
            self.assertEqual(first['stats']['processed'], 18)
            self.assertEqual(first['stats']['errors'], 2)
            self.assertEqual(len(first['results']), 5)
            self.assertLessEqual(first['stats']['peak_in_flight'], 16)
            self.assertLessEqual(first['stats']['peak_compute'], 2)
            self.assertLessEqual(first['stats']['peak_active_requests'], 8)
            calls = sum(server.calls.values())
            second = self.run_fast(server, end=1018, download_workers=8)
            self.assertEqual(second['stats']['resumed'], 18)
            self.assertEqual(second['stats']['errors'], 2)
            self.assertEqual(second['stats']['consistent'], first['stats']['consistent'])
            self.assertEqual(sum(server.calls.values()), calls)
            self.assertEqual({r['identifier'] for r in first['results']}, {r['identifier'] for r in second['results']})
            paths = export_records(self.state, first['run_id'], self.root/'full')
            records = [json.loads(line) for line in paths['jsonl'].read_text(encoding='utf-8').splitlines()]
            self.assertEqual(len(records), 18)
            self.assertEqual(sum(bool(row['error']) for row in records), 2)
            with paths['csv'].open(encoding='utf-8-sig', newline='') as stream:
                self.assertEqual(len(list(csv.reader(stream))), 19)
            reports = write_reports(first, self.root/'report')
            self.assertIn('完整 18 项记录', reports['html'].read_text(encoding='utf-8'))
            self.assertEqual(len(json.loads(reports['json'].read_text(encoding='utf-8'))['results']), 5)

    def test_retries_delete_only_errors_and_update_counters(self):
        with MockServer(self.images, faults={1003: [503, 503, 200]}) as server:
            first = self.run_fast(server, end=1006, download_workers=2)
            self.assertEqual(first['stats']['errors'], 1)
            calls = sum(server.calls.values())
            second = self.run_fast(server, end=1006, download_workers=2, retry_errors=True)
            self.assertEqual(second['stats']['processed'], 6)
            self.assertEqual(second['stats']['resumed'], 5)
            self.assertEqual(second['stats']['errors'], 0)
            self.assertEqual(sum(server.calls.values()), calls+1)

    def test_billion_range_is_lazy_and_stop_resume_is_bounded(self):
        with MockServer(self.images) as server:
            for threshold in [3, 7]:
                stop = threading.Event()
                events = []
                def progress(event):
                    events.append(event)
                    if event['processed'] >= threshold:
                        stop.set()
                result = self.run_fast(server, end=1_000_001_000, download_workers=1,
                                       cancel=stop, progress=progress)
                self.assertEqual(result['stats']['total'], 1_000_000_000)
                self.assertEqual(result['status'], 'stopped')
                self.assertLess(result['stats']['processed'], 12)
                self.assertLessEqual(len(result['results']), 5)
                self.assertLessEqual(result['stats']['peak_in_flight'], 2)
            self.assertGreater(result['stats']['resumed'], 0)
            self.assertLess(sum(server.calls.values()), 14)

    def test_http_keep_alive_reuses_connection(self):
        with MockServer(self.images) as server:
            result = self.run_fast(server, download_workers=1)
            self.assertEqual(result['stats']['errors'], 0)
            self.assertEqual(sum(server.calls.values()), 16)
            self.assertEqual(len(server.client_ports), 1)

    def test_persistent_429_stops_and_explicit_retry_recovers(self):
        with MockServer(self.images, faults={1001: [429]}) as server:
            config = SourceConfig(server.url, 'test-v1', rate=0, retries=0, keep_alive=True)
            first = self.run_fast(server, end=1003, download_workers=1, config=config)
            self.assertEqual(first['status'], 'stopped')
            self.assertLess(first['stats']['processed'], 3)
            server.faults[1001] = [200]
            second = self.run_fast(server, end=1003, download_workers=1, config=config, retry_errors=True)
            self.assertEqual(second['status'], 'complete')
            self.assertEqual(second['stats']['processed'], 3)
            self.assertEqual(second['stats']['errors'], 0)

    def test_gap_paging_does_not_skip_holes(self):
        db = sqlite3.connect(':memory:')
        db.row_factory = sqlite3.Row
        try:
            ledger = RangeLedger(db, 'test')
            for number in [0, 3, 8, 10]:
                ledger.save({'identifier': str(number), 'metrics': None, 'error': 'fixture', 'fetch': {'http_status': 500}})
            self.assertEqual(list(ledger.pending(0, 12, page_size=2)), [1, 2, 4, 5, 6, 7, 9, 11, 12])
            self.assertEqual(ledger.totals()['processed'], 4)
        finally:
            db.close()


if __name__ == '__main__':
    unittest.main()
