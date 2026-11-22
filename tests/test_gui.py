"""Exercise real UI HTTP boundaries and the existing engines with local fixtures."""
from __future__ import annotations

from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import HTTPCookieProcessor, ProxyHandler, Request, build_opener

from avatar_compare.gui import LocalServer, existing_url
from avatar_compare.gui_controller import Controller, save_json
from avatar_compare.mock_service import MockServer, dataset
from test_staged import clean_metrics


class GuiTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='avatar-ui-中文 ')
        self.root = Path(self.temporary.name)
        self.opened = []
        self.controller = Controller(self.root, opener=self.opened.append)
        self.target, self.images, _ = dataset()
        self.selected = self.controller.upload(self.target, '目标.png')

    def tearDown(self):
        self.controller.stop()
        if self.controller.worker:
            self.controller.worker.join(30)
        self.temporary.cleanup()

    def wait(self):
        self.controller.worker.join(40)
        self.assertFalse(self.controller.busy(), 'UI task did not finish')
        state = self.controller.snapshot()
        self.assertNotEqual(state['status'], 'error', state.get('message'))
        return state

    def values(self, source, **extra):
        return dict(target_id=self.selected['id'], start='1001', end='1019', endpoint=source.url,
                    revision='gui-test', **extra)

    def test_defaults_enable_full_speed_and_compact_and_preserve_input(self):
        values = self.controller.validate(dict(target_id=self.selected['id'], start='9223372036854775806', end='9223372036854775807'))
        self.assertEqual((values['retention'], values['rate']), ('compact', 0))
        self.assertEqual(values['end'], '9223372036854775807')
        self.assertEqual(Path(values['target']).read_bytes(), self.target)
        self.assertEqual(hashlib.sha256(self.target).hexdigest(), values['target_id'])
        for start, end in [('2', '1'), ('-1', '0'), ('1e3', '2000'), ('0', '9223372036854775807')]:
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                self.controller.validate(dict(target_id=self.selected['id'], start=start, end=end))

    def test_mock_scan_history_zero_request_resume_preview_export(self):
        self.images[1018] = b'not an image'
        self.images[1019] = self.target
        with MockServer(self.images) as source:
            self.controller.start(self.values(source))
            first = self.wait()
            self.assertEqual((first['stats']['processed'], first['stats']['errors']), (19, 2))
            self.assertTrue((self.root / '.state-compact' / 'cache.sqlite3').is_file())
            job = self.controller.history()[0]
            self.assertEqual(job['requested_range'], ['1001', '1019'])
            self.assertEqual(job['failed_identifiers'], ['1017', '1018'])
            self.assertTrue(job['can_resume'])
            self.assertIn(['1001', '1016'], job['successful_ranges'])
            preview = self.controller.preview(first['job_key'], first['results'][0]['digest'])
            self.assertGreater(len(preview), 100)
            with self.assertRaises(ValueError):
                self.controller.preview(first['job_key'], '0' * 64)
            before = sum(source.calls.values())
            params = self.controller.load_job(first['job_key'])
            self.controller.start(params)
            second = self.wait()
            self.assertEqual(second['stats']['resumed'], 19)
            self.assertEqual(sum(source.calls.values()), before)
            self.assertEqual(clean_metrics(first['results']), clean_metrics(second['results']))
            self.controller.export(first['job_key'], images=True)
            exported = self.wait()
            files = exported['export_files']
            self.assertTrue(Path(files['manifest']).is_file())
            self.assertTrue(list(Path(files['images']).glob('*.img')))
            self.controller.open_output(first['job_key'], 'export')
            self.assertEqual(self.opened, [Path(files['manifest']).parent])
            # Repair the mock; retry errors rather than re-fetch successful IDs.
            self.images[1017] = self.images[1018] = self.target
            params['retry_errors'] = True
            self.controller.start(params)
            repaired = self.wait()
            self.assertEqual(repaired['stats']['errors'], 0)
            self.assertEqual(sum(source.calls.values()) - before, 2)

    def test_stop_resume_and_reject_overlapping_operations(self):
        images = {identifier: self.target for identifier in range(1001, 1081)}
        with MockServer(images, delay=0.04) as source:
            values = self.values(source, speed='custom', workers=1, downloads=2)
            values['end'] = '1080'
            self.controller.start(values)
            deadline = time.monotonic() + 10
            while self.controller.snapshot()['stats'].get('processed', 0) < 1 and time.monotonic() < deadline:
                time.sleep(0.02)
            with self.assertRaises(ValueError):
                self.controller.start(values)
            self.controller.stop()
            stopped = self.wait()
            self.assertEqual(stopped['status'], 'stopped')
            self.assertTrue(0 < stopped['stats']['processed'] < 80)
            params = self.controller.load_job(stopped['job_key'])
            self.controller.start(params)
            resumed = self.wait()
            self.assertEqual(resumed['status'], 'complete')
            self.assertEqual(resumed['stats']['processed'], 80)
            self.assertEqual(resumed['stats']['resumed'], stopped['stats']['processed'])

    def test_full_retention_keeps_evidence_identical(self):
        with MockServer(self.images) as source:
            values = self.values(source)
            values['end'] = '1016'
            self.controller.start(values)
            compact = self.wait()
            values['retention'] = 'all'
            self.controller.start(values)
            full = self.wait()
            self.assertEqual([(r['identifier'], r['digest'], clean_metrics(r['metrics'])) for r in compact['results']],
                             [(r['identifier'], r['digest'], clean_metrics(r['metrics'])) for r in full['results']])
            self.assertEqual({row['retention'] for row in self.controller.history()}, {'compact', 'all'})

    def test_all_cpu_option_uses_existing_process_backend_with_compact(self):
        with MockServer(self.images) as source:
            values = self.values(source, speed='maximum')
            values['end'] = '1003'
            self.controller.start(values)
            state = self.wait()
            self.assertEqual(state['stats']['compute_backend'], 'process')
            self.assertGreater(state['stats']['compute_processes'], 0)
            self.assertEqual(state['params']['retention'], 'compact')

    def test_local_mode_excludes_original_target_and_restores_folder(self):
        folder = self.root / '本地图库'
        folder.mkdir()
        original = folder / '原始目标.png'
        original.write_bytes(self.target)
        (folder / '副本.png').write_bytes(self.target)
        with patch.object(self.controller, '_pick', return_value=original):
            selected = self.controller.pick_target()
        with patch.object(self.controller, '_pick', return_value=folder):
            picked = self.controller.pick_folder()
        self.controller.start(dict(target_id=selected['id'], mode='local', folder_id=picked['id']))
        state = self.wait()
        self.assertEqual(state['stats']['total'], 1)
        self.assertEqual(Path(state['results'][0]['path']), (folder / '副本.png').resolve())
        params = self.controller.load_job(state['job_key'])
        self.controller.start(params)
        resumed = self.wait()
        self.assertEqual(resumed['stats']['resumed'], 1)

    def test_unknown_old_source_cannot_be_silently_replaced(self):
        with MockServer(self.images) as source:
            self.controller.start(self.values(source))
            state = self.wait()
        self.controller.jobs = {}
        self.controller.sources = {}
        self.assertFalse(self.controller.history()[0]['can_resume'])
        with self.assertRaises(ValueError):
            self.controller.load_job(state['job_key'])

    def test_configuration_survives_unexpected_failure_after_commit(self):
        from avatar_compare.staged import search_staged
        def fail_after_commit(*args, **kwargs):
            search_staged(*args, **kwargs)
            raise RuntimeError('injected failure after engine commit')
        with MockServer(self.images) as source:
            with self.assertLogs('avatar_compare.gui_controller', level='ERROR'), patch('avatar_compare.staged.search_staged', side_effect=fail_after_commit):
                self.controller.start(self.values(source))
                self.controller.worker.join(30)
            self.assertEqual(self.controller.snapshot()['status'], 'error')
            history = self.controller.history()[0]
            self.assertTrue(history['can_resume'])
            params = self.controller.load_job(history['key'])
            self.assertEqual(params['endpoint'], source.url)
            before = sum(source.calls.values())
            self.controller.start(params)
            resumed = self.wait()
            self.assertEqual(resumed['stats']['resumed'], 19)
            self.assertEqual(sum(source.calls.values()), before)

    def test_loopback_auth_origin_boundaries_and_runtime_reuse(self):
        with LocalServer(self.root, controller=self.controller) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            plain = build_opener(ProxyHandler({}))
            authenticated = build_opener(ProxyHandler({}), HTTPCookieProcessor())
            try:
                with self.assertRaises(HTTPError) as error:
                    plain.open(server.origin + '/api/state')
                self.assertEqual(error.exception.code, 401)
                with authenticated.open(server.launch_url) as response:
                    self.assertEqual(response.status, 200)
                    self.assertIn(b'app.js', response.read())
                with authenticated.open(server.origin + '/api/state') as response:
                    self.assertEqual(json.load(response)['status'], 'idle')
                for origin in [None, 'https://unrelated.example']:
                    request = Request(server.origin + '/api/demo', data=b'{}', headers={'Content-Type': 'application/json', **({'Origin': origin} if origin else {})})
                    with self.assertRaises(HTTPError) as error:
                        authenticated.open(request)
                    self.assertEqual(error.exception.code, 403)
                request = Request(server.origin + '/api/state', headers={'Host': 'unrelated.example'})
                with self.assertRaises(HTTPError) as error:
                    authenticated.open(request)
                self.assertEqual(error.exception.code, 403)
                request = Request(server.origin + '/api/upload?name=sample.png', data=self.target,
                                  headers={'Content-Type': 'application/octet-stream', 'Origin': server.origin})
                with authenticated.open(request) as response:
                    self.assertEqual(json.load(response)['id'], self.selected['id'])
                save_json(self.root / '.ui' / 'runtime.json', {'port': server.server_port, 'token': server.token})
                self.assertEqual(existing_url(self.root), server.launch_url)
                with self.assertRaises(HTTPError) as error:
                    authenticated.open(server.origin + '/../../gui_controller.py')
                self.assertEqual(error.exception.code, 404)
            finally:
                server.shutdown()
                thread.join(3)

    def test_demo_is_separate_from_real_task_libraries(self):
        self.controller.demo()
        state = self.wait()
        self.assertTrue(state['demo'])
        self.assertEqual(state['stats']['processed'], 20)
        self.assertEqual(state['stats']['errors'], 3)
        self.assertEqual(self.controller.history(), [])
        self.assertTrue(self.controller.report_path(state['job_key']).is_file())


if __name__ == '__main__':
    unittest.main()
