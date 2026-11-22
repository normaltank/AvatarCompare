from __future__ import annotations
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest

import numpy as np
from PIL import Image, ImageDraw

from avatar_compare.engine import Store, search
from avatar_compare.features import compare, extract
from avatar_compare.report import safe_cell, write_reports
from avatar_compare.mock_service import encode, synthetic_picture


def picture(seed=3):
    rng = np.random.default_rng(seed)
    image = Image.new('RGB', (320, 320), '#e4e2d8')
    draw = ImageDraw.Draw(image)
    for _ in range(65):
        x, y = rng.integers(0, 290, 2)
        radius = int(rng.integers(4, 20))
        color = tuple(int(v) for v in rng.integers(0, 240, 3))
        draw.ellipse((int(x), int(y), int(x)+radius, int(y)+radius), fill=color)
    return image


def raw(image, fmt='PNG'):
    output = io.BytesIO()
    image.save(output, format=fmt)
    return output.getvalue()


class NumericalAndGeometryTests(unittest.TestCase):
    def test_uniform_gray_does_not_overflow(self):
        feature = extract(raw(Image.new('RGB', (64, 64), (128, 128, 128))))
        self.assertEqual(feature.ahash, 0)
        self.assertEqual(len(feature.descriptors), 0)

    def test_opposite_layout_cannot_be_reported_as_identical(self):
        array = np.full((64, 64, 3), 64, np.uint8)
        array[:, 32:] = 192
        a = extract(raw(Image.fromarray(array)))
        b = extract(raw(Image.fromarray((256-array.astype(np.int16)).astype(np.uint8))))
        result = compare(a, b)
        self.assertEqual(result['ahash_distance'], 64)
        self.assertEqual(result['ahash_agreement'], 0)
        self.assertFalse(result['exact_pixels'])

    def test_same_pixels_different_container(self):
        image = Image.new('RGB', (32, 32), (10, 40, 80))
        left = extract(raw(image))
        right = extract(raw(image, 'BMP'))
        result = compare(left, right)
        self.assertFalse(result['exact_file'])
        self.assertTrue(result['exact_pixels'])
        self.assertAlmostEqual(result['ssim'], 1)

    def test_crop_has_geometric_evidence(self):
        image = picture()
        query = extract(raw(image))
        candidate = extract(raw(image.crop((32, 32, 288, 288))))
        result = compare(query, candidate)
        self.assertTrue(result['geometry']['consistent'], result)
        self.assertGreaterEqual(result['geometry']['inliers'], 8)

    def test_unrelated_layout_has_insufficient_geometric_evidence(self):
        result = compare(extract(raw(picture(3))), extract(raw(picture(998))))
        self.assertFalse(result['geometry']['consistent'])

    def test_repeated_sift_orientations_are_not_independent_locations(self):
        query = extract(encode(synthetic_picture(32)))
        for seed in [32002, 32008]:
            with self.subTest(seed=seed):
                candidate = extract(encode(synthetic_picture(seed)))
                result = compare(query, candidate)
                self.assertFalse(result['geometry']['consistent'], result['geometry'])

    def test_transparent_pixels_are_composited_consistently(self):
        a = extract(raw(Image.new('RGBA', (32, 32), (255, 0, 0, 0))))
        b = extract(raw(Image.new('RGB', (32, 32), 'white')))
        self.assertEqual(a.pixels, b.pixels)

    def test_exif_rotation_matches_oriented_pixels(self):
        image = picture().resize((80, 120))
        metadata = Image.Exif()
        metadata[274] = 6
        stream = io.BytesIO()
        image.save(stream, format='PNG', exif=metadata)
        a = extract(stream.getvalue())
        b = extract(raw(image.transpose(Image.Transpose.ROTATE_270)))
        self.assertEqual(a.pixels, b.pixels)
        self.assertEqual((a.width, a.height), (120, 80))


class PersistentEngineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='avatar-compare-test-')
        self.root = Path(self.temporary.name).resolve()
        self.folder = self.root/'library'
        self.folder.mkdir()
        self.target = self.root/'target.png'
        picture().save(self.target)
        self.state = self.root/'cache'
        for index in range(5):
            picture(index+1).save(self.folder/f'{index}.png')
        (self.folder/'duplicate.png').write_bytes(self.target.read_bytes())

    def tearDown(self):
        self.assertEqual(self.root.parent, Path(tempfile.gettempdir()).resolve())
        self.temporary.cleanup()

    def test_second_run_reuses_only_same_job(self):
        first = search(self.target, self.folder, self.state)
        second = search(self.target, self.folder, self.state)
        self.assertEqual(first['run_id'], second['run_id'])
        self.assertEqual(second['stats']['resumed'], 6)
        self.assertEqual(first['stats']['consistent'], second['stats']['consistent'])

    def test_cancel_persists_then_resumes(self):
        stop = threading.Event()
        def progress(event):
            if event['processed'] >= 2:
                stop.set()
        first = search(self.target, self.folder, self.state, cancel=stop, progress=progress)
        self.assertEqual(first['status'], 'stopped')
        self.assertGreaterEqual(first['stats']['processed'], 2)
        self.assertLess(first['stats']['processed'], 6)
        second = search(self.target, self.folder, self.state)
        self.assertEqual(second['stats']['processed'], 6)
        self.assertEqual(second['stats']['resumed'], first['stats']['processed'])

    def test_new_target_does_not_inherit_previous_results(self):
        first = search(self.target, self.folder, self.state)
        picture(941).save(self.target)
        second = search(self.target, self.folder, self.state)
        self.assertNotEqual(first['run_id'], second['run_id'])
        self.assertEqual(second['stats']['resumed'], 0)

    def test_changed_candidate_invalidates_cache_and_job(self):
        first = search(self.target, self.folder, self.state)
        picture(45).save(self.folder/'0.png')
        second = search(self.target, self.folder, self.state)
        self.assertNotEqual(first['run_id'], second['run_id'])
        self.assertEqual(second['stats']['resumed'], 0)
        self.assertGreaterEqual(second['stats']['feature_misses'], 1)

    def test_bad_image_is_recorded_without_stopping(self):
        (self.folder/'broken.png').write_bytes(b'not a PNG')
        result = search(self.target, self.folder, self.state)
        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['stats']['errors'], 1)
        self.assertEqual(result['stats']['processed'], 7)

    def test_feature_cache_can_be_reused_without_pair_cache(self):
        search(self.target, self.folder, self.state)
        second = search(self.target, self.folder, self.state, resume=False, pair_cache=False)
        self.assertEqual(second['stats']['feature_misses'], 0)
        self.assertEqual(second['stats']['feature_hits'], 6)
        self.assertEqual(second['stats']['resumed'], 0)

    def test_bounded_parallelism_preserves_result_set(self):
        first = search(self.target, self.folder, self.state, workers=1)
        second = search(self.target, self.folder, self.root/'other-cache', workers=2)
        self.assertEqual({row['path'] for row in first['results']}, {row['path'] for row in second['results']})
        self.assertEqual(first['stats']['consistent'], second['stats']['consistent'])

    def test_target_inside_library_is_not_compared_to_itself(self):
        (self.folder/'self.png').write_bytes(self.target.read_bytes())
        result = search(self.folder/'self.png', self.folder, self.state)
        self.assertEqual(result['stats']['total'], 6)

    def test_export_escapes_filenames_and_preserves_metrics(self):
        (self.folder/'a&b.png').write_bytes(self.target.read_bytes())
        result = search(self.target, self.folder, self.state)
        paths = write_reports(result, self.root/'out')
        document = paths['html'].read_text(encoding='utf-8')
        self.assertNotIn('a&b.png', document)
        self.assertIn('a&amp;b.png', document)
        saved = json.loads(paths['json'].read_text(encoding='utf-8'))
        self.assertEqual(saved['run_id'], result['run_id'])
        self.assertTrue(paths['csv'].read_bytes().startswith(b'\xef\xbb\xbf'))

    def test_cache_directory_cannot_hide_entire_library(self):
        with self.assertRaises(ValueError):
            search(self.target, self.folder, self.root)

    def test_corrupt_feature_payload_is_rebuilt(self):
        store = Store(self.state/'cache.sqlite3')
        first, _ = store.get(self.target)
        with store.db():
            store.db().execute('UPDATE features SET payload=?', (b'corrupt',))
        rebuilt, hit = store.get(self.target)
        self.assertEqual(rebuilt.digest, first.digest)
        self.assertEqual(hit, 'miss')
        store.close()


class ExportTests(unittest.TestCase):
    def test_csv_formula_prefix_is_escaped(self):
        self.assertEqual(safe_cell('=1+2'), "'=1+2")
        self.assertEqual(safe_cell('normal.png'), 'normal.png')


if __name__ == '__main__':
    unittest.main()
