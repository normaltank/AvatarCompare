from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

from avatar_compare.mock_service import MockServer, dataset
from avatar_compare.range_engine import search_range, validate_range
from avatar_compare.report import write_reports
from avatar_compare.source import QLOGO_ENDPOINT, SourceConfig, cache_batch


class RangeIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='avatar-range-')
        self.root = Path(self.temp.name)
        self.target = self.root/'目标.png'
        target, self.images, self.positive = dataset()
        self.target.write_bytes(target)
        self.state = self.root/'state'

    def tearDown(self):
        self.temp.cleanup()

    def run_range(self, server, start=1001, end=1006, **kwargs):
        config = kwargs.pop('config', SourceConfig(server.url, 'fixture-v1', rate=100, retries=1))
        return search_range(self.target, start, end, self.state, config, **kwargs)

    def test_full_pipeline_associates_ids_images_evidence_and_errors(self):
        self.images[1018] = b'broken'
        self.images[1019] = self.target.read_bytes()
        with MockServer(self.images, faults={1019: [503, 200], 1020: [503]}) as server:
            events = []
            result = self.run_range(server, end=1020, workers=3, progress=events.append)
            self.assertEqual(result['status'], 'complete')
            self.assertEqual(result['stats']['processed'], 20)
            self.assertEqual(result['stats']['errors'], 3)
            self.assertEqual(server.calls[1019], 2)
            self.assertEqual(server.calls[1020], 2)
            rows = {int(row['identifier']): row for row in result['results']}
            self.assertTrue(rows[1001]['metrics']['exact_file'])
            self.assertEqual(rows[1017]['fetch']['http_status'], 404)
            self.assertIn('UnidentifiedImageError', rows[1018]['error'])
            self.assertEqual(rows[1020]['fetch']['attempts'], 2)
            self.assertLessEqual(result['stats']['peak_in_flight'], 6)
            self.assertLessEqual(result['stats']['peak_active_requests'], 3)
            self.assertEqual(events[-1]['processed'], 20)
            reports = write_reports(result, self.root/'out')
            self.assertEqual(len(reports['errors'].read_text(encoding='utf-8').splitlines()), 3)
            self.assertIn('编号 1001', reports['html'].read_text(encoding='utf-8'))
            self.assertEqual(len(json.loads(reports['json'].read_text(encoding='utf-8'))['results']), 20)

    def test_resume_no_network_and_retry_only_failed_ids(self):
        with MockServer(self.images, faults={1003: [503, 503, 200]}) as server:
            first = self.run_range(server)
            before = sum(server.calls.values())
            second = self.run_range(server)
            self.assertEqual(second['stats']['resumed'], 6)
            self.assertEqual(sum(server.calls.values()), before)
            third = self.run_range(server, retry_errors=True)
            self.assertEqual(third['stats']['errors'], 0)
            self.assertEqual(third['stats']['resumed'], 5)
            self.assertEqual(sum(server.calls.values()), before+1)
            self.assertEqual(first['run_id'], third['run_id'])

    def test_stop_resume_and_revision_isolation(self):
        stop = threading.Event()
        with MockServer(self.images, delay=0.03) as server:
            first = self.run_range(server, end=1016, workers=1, cancel=stop,
                                   progress=lambda event: stop.set() if event['processed'] >= 2 else None)
            self.assertEqual(first['status'], 'stopped')
            self.assertLess(first['stats']['processed'], 16)
            second = self.run_range(server, end=1016)
            self.assertEqual(second['stats']['processed'], 16)
            self.assertEqual(second['stats']['resumed'], first['stats']['processed'])
            third = self.run_range(server, end=1002, config=SourceConfig(server.url, 'v2', rate=100))
            self.assertNotEqual(second['run_id'], third['run_id'])
            self.assertEqual(third['stats']['resumed'], 0)
            self.assertEqual(third['stats']['requests'], 2)

    def test_overlapping_range_and_new_target_reuse_downloads(self):
        with MockServer(self.images) as server:
            self.run_range(server)
            self.target.write_bytes(self.images[1007])
            result = self.run_range(server, start=1004, end=1008)
            self.assertEqual(result['stats']['resumed'], 0)
            self.assertEqual(result['stats']['download_cache_hits'], 3)
            self.assertEqual(result['stats']['requests'], 2)

    def test_missing_or_corrupt_download_refetches(self):
        with MockServer(self.images) as server:
            first = self.run_range(server, end=1001)
            Path(first['results'][0]['path']).write_bytes(b'corrupt')
            second = self.run_range(server, end=1001)
            self.assertEqual(second['stats']['requests'], 1)
            self.assertTrue(second['results'][0]['metrics']['exact_file'])

    def test_missing_images_preserve_the_keepalive_connection(self):
        from avatar_compare.source import HttpSource

        with MockServer(self.images, faults={1018: [410]}) as server:
            source = HttpSource(SourceConfig(server.url, 'fixture-v1', rate=0, retries=0, keep_alive=True))
            try:
                cancel = threading.Event()
                for identifier, status in [(1001, 200), (1017, 404), (1018, 410), (1002, 200)]:
                    image, metadata = source.fetch(identifier, cancel)
                    self.assertEqual(metadata['http_status'], status)
                    self.assertEqual(image, self.images.get(identifier))
                    self.assertEqual(metadata['attempts'], 1)
                self.assertEqual(len(server.client_ports), 1)
            finally:
                source.close()

    def test_ambiguous_or_oversized_missing_responses_do_not_reuse_connection(self):
        from http.client import HTTPMessage
        from types import SimpleNamespace
        from avatar_compare.source import HttpSource

        source = HttpSource(SourceConfig('http://127.0.0.1:1/{id}', 'framing', keep_alive=True, max_bytes=1))
        examples = [
            ([('Content-Length', '+0')], 0),
            ([('Content-Length', '0_0')], 0),
            ([('Content-Length', '0'), ('Transfer-Encoding', ''), ('Transfer-Encoding', 'chunked')], 0),
            ([('Content-Length', '0'), ('Content-Length', '0')], 0),
            ([('Content-Length', '2')], 2),
        ]
        for fields, length in examples:
            with self.subTest(fields=fields):
                headers = HTTPMessage()
                for name, value in fields:
                    headers[name] = value
                response = SimpleNamespace(length=length, will_close=False, headers=headers)
                self.assertFalse(source._drain_missing_body(SimpleNamespace(sock=object()), response))

    def test_no_redirect_and_size_limit(self):
        with MockServer(self.images, faults={1002: [302]}) as server:
            result = self.run_range(server, end=1002, config=SourceConfig(server.url, 'v1', rate=100, max_bytes=100))
            self.assertEqual(result['stats']['errors'], 2)
            self.assertEqual(server.calls[1001], 1)
            rows = {row['identifier']: row for row in result['results']}
            self.assertEqual(rows['1002']['fetch']['http_status'], 302)
            self.assertEqual(rows['1001']['error'], 'response_too_large_or_invalid_length')

    def test_authorization_stops_range_and_token_not_exported(self):
        token = 'test-only-secret'
        with MockServer(self.images, token=token) as server:
            result = self.run_range(server, end=1016, workers=1)
            self.assertEqual(result['status'], 'stopped')
            self.assertLess(result['stats']['processed'], 16)
            os.environ['AVATAR_TEST_TOKEN'] = token
            try:
                authorized = self.run_range(server, end=1002, config=SourceConfig(server.url, 'v1', token_env='AVATAR_TEST_TOKEN', rate=100))
                self.assertEqual(authorized['stats']['errors'], 0)
                self.assertNotIn(token, json.dumps(authorized))
                self.assertNotIn(server.url, json.dumps(authorized))
            finally:
                os.environ.pop('AVATAR_TEST_TOKEN', None)

    def test_paced_requests_avoid_coarse_wakeup_for_short_intervals(self):
        from unittest.mock import patch
        from avatar_compare.source import HttpSource

        source = HttpSource(SourceConfig('http://127.0.0.1:1/{id}', 'clock', rate=100, keep_alive=True))
        cancel = threading.Event()
        now = 1.0

        def sleep(seconds):
            nonlocal now
            now += seconds

        # A coarse event wait overshoots short intervals on Windows.
        with (
            patch('avatar_compare.source.time.monotonic', side_effect=lambda: now),
            patch('avatar_compare.source.time.perf_counter', side_effect=lambda: now),
            patch('avatar_compare.source.time.sleep', side_effect=sleep),
            patch.object(cancel, 'wait', side_effect=lambda delay: sleep(delay + .015625)),
        ):
            initial = source.acquire(cancel)
            second = source.acquire(cancel)
            sleep(.1)
            delayed = source.acquire(cancel)
            following = source.acquire(cancel)
        self.assertAlmostEqual(second - initial, .01)
        self.assertAlmostEqual(following - delayed, .01)

    def test_rate_limit_includes_retries_and_bounds_requests(self):
        from unittest.mock import patch
        from avatar_compare.source import HttpSource
        original=HttpSource.acquire;admissions=[]
        def observed(source,cancel):
            granted=original(source,cancel);admissions.append(granted);return granted
        with MockServer(self.images, faults={1001: [429, 200]}) as server:
            with patch.object(HttpSource,'acquire',observed):
                result = self.run_range(server, end=1003, workers=4,
                                        config=SourceConfig(server.url, 'v1', rate=20, retries=1))
            self.assertEqual(result['stats']['requests'], 4)
            self.assertEqual(sum(server.calls.values()),4)
            stamps=sorted(admissions)
            self.assertEqual(len(stamps),4)
            # Server arrival timestamps include scheduling/network jitter, and
            # Windows GetTickCount64 has 15.625ms resolution. Check the actual
            # limiter's grants, including the retry, rather than arrival jitter.
            self.assertTrue(all(b-a >= .05-1e-9 for a,b in zip(stamps,stamps[1:])),stamps)

    def test_timeout_is_recorded(self):
        with MockServer(self.images, delay=0.1) as server:
            result = self.run_range(server, end=1001, config=SourceConfig(server.url, 'v1', timeout=0.02, retries=0, rate=100))
            self.assertEqual(result['stats']['errors'], 1)
            self.assertEqual(result['results'][0]['error'], 'transport_error')

    def test_single_number_and_invalid_input_before_requests(self):
        with MockServer(self.images) as server:
            result = self.run_range(server, start=1001, end=1001)
            self.assertEqual(result['stats']['total'], 1)
            for start, end in [(2, 1), (-1, 1), (1, 2**63), (True, 2)]:
                with self.assertRaises(ValueError):
                    self.run_range(server, start=start, end=end)
            self.assertEqual(sum(server.calls.values()), 1)
        for endpoint in ['https://example.test/{id}/{other}', 'http://example.test/{id}', 'https://{id}.test/x']:
            with self.assertRaises(ValueError):
                SourceConfig(endpoint, 'v1')

    def test_cli_actual_process_writes_reports_and_rejects_incomplete_input(self):
        with MockServer(self.images) as server:
            command = [sys.executable, '-m', 'avatar_compare', '--target', str(self.target),
                       '--start', '1001', '--end', '1002', '--endpoint', server.url,
                       '--source-revision', 'v1', '--rate', '100', '--state-dir', str(self.state),
                       '--output', str(self.root/'out')]
            completed = subprocess.run(command, capture_output=True, timeout=20)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(len(list((self.root/'out').glob('*.html'))), 1)
            invalid = subprocess.run([sys.executable, '-m', 'avatar_compare', '--start', '2'], capture_output=True, timeout=10)
            self.assertEqual(invalid.returncode, 2)

    def test_automatic_cache_batch_refresh_persists_and_isolates_source(self):
        first = cache_batch(self.state, QLOGO_ENDPOINT)
        self.assertEqual(first, cache_batch(self.state, QLOGO_ENDPOINT))
        refreshed = cache_batch(self.state, QLOGO_ENDPOINT, refresh=True)
        self.assertNotEqual(first, refreshed)
        self.assertEqual(refreshed, cache_batch(self.state, QLOGO_ENDPOINT))
        self.assertNotEqual(refreshed, cache_batch(self.state, 'https://example.test/{id}'))

    def test_cli_minimal_range_options_auto_batch_and_refresh(self):
        with MockServer(self.images) as server:
            command = [sys.executable, '-m', 'avatar_compare', '--target', str(self.target),
                       '--start', '1001', '--end', '1001', '--endpoint', server.url,
                       '--rate', '100', '--state-dir', str(self.state), '--output', str(self.root/'out')]
            for extra in [[], [], ['--refresh-source'], []]:
                result = subprocess.run(command+extra, capture_output=True, timeout=20)
                self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(sum(server.calls.values()), 2)
            self.assertEqual(len(list((self.root/'out').glob('*.json'))), 2)


if __name__ == '__main__':
    unittest.main()
