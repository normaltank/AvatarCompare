"""Small local transformation experiment, not a population accuracy benchmark."""
from __future__ import annotations
import argparse
import io
import json
from pathlib import Path
import sys
import time

import cv2
import numpy as np
from PIL import Image, ImageEnhance, ImageOps

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from avatar_compare.features import extract, compare

parser = argparse.ArgumentParser()
parser.add_argument('--image', required=True)
parser.add_argument('--output', required=True)
args = parser.parse_args()
out = Path(args.output).resolve()
out.mkdir(parents=True, exist_ok=True)
image = Image.open(args.image).convert('RGB')

def encoded(value, quality=None):
    stream = io.BytesIO()
    value.save(stream, format='JPEG' if quality is not None else 'PNG', **({'quality': quality} if quality is not None else {}))
    return stream.getvalue()

variants = [
    ('exact', encoded(image), True),
    ('resize_100', encoded(image.resize((100, 100))), True),
    ('jpeg_100_q20', encoded(image.resize((100, 100)), 20), True),
    ('jpeg_400_q30', encoded(image, 30), True),
    ('crop_60percent', encoded(image.crop((80, 80, 320, 320)).resize((100, 100))), True),
    ('crop_80percent', encoded(image.crop((40, 40, 360, 360)).resize((100, 100))), True),
    ('brightened', encoded(ImageEnhance.Brightness(image).enhance(1.25)), True),
    ('mirror', encoded(ImageOps.mirror(image)), True),
]
frame = Image.new('RGB', (500, 580), '#eeeeee')
frame.paste(image, (50, 90))
variants.append(('screenshot_frame', encoded(frame), True))
rng = np.random.default_rng(20261105)
for index in range(8):
    grid = rng.integers(32, 230, (8, 8, 3), dtype=np.uint8)
    grid = cv2.resize(grid, (400, 400), interpolation=cv2.INTER_CUBIC)
    variants.append((f'synthetic_distractor_{index}', encoded(Image.fromarray(grid)), False))

cv2.setNumThreads(1)
query = extract(encoded(image))
rows = []
for name, raw, expected_copy in variants:
    features = extract(raw)
    result = compare(query, features)
    result.update(name=name, expected_copy=expected_copy, feature_ms=features.elapsed_ms, keypoints=len(features.points))
    rows.append(result)
    (out / (name + ('.jpg' if name.startswith('jpeg') else '.png'))).write_bytes(raw)

timing = {}
for method in ['light_hash', 'geometry']:
    spans = []
    for repeat in range(3):
        started = time.perf_counter()
        for _, raw, _ in variants:
            features = extract(raw, local_features=(method == 'geometry'))
            if method == 'geometry':
                compare(query, features)
            elif method == 'light_hash':
                _ = (query.phash ^ features.phash).bit_count()
        spans.append((time.perf_counter() - started) * 1000)
    timing[method] = {'median_total_ms': float(np.median(spans)), 'samples_ms': spans, 'note': 'Includes image decoding, preview generation and extraction; light_hash skips SIFT and geometry.'}
cached = [extract(raw) for _, raw, _ in variants]
spans = []
for repeat in range(3):
    started = time.perf_counter()
    for features in cached:
        compare(query, features)
    spans.append((time.perf_counter() - started) * 1000)
timing['cached_geometry'] = {'median_total_ms': float(np.median(spans)), 'note': 'Reuses in-memory features; filesystem/SQLite costs not included.'}
report = {
    'environment': {'numpy': np.__version__, 'opencv': cv2.__version__},
    'query_dimensions': image.size,
    'query_keypoints': len(query.points),
    'metadata': {'info': {str(k):str(v) for k,v in Image.open(args.image).info.items()}, 'exif_present': bool(Image.open(args.image).getexif())},
    'cases': rows,
    'timing': timing,
    'limitations': ['One supplied illustration and generated transformations.', 'Negative controls are synthetic, not same-character different-art hard negatives.', 'No QQ account data, external avatar requests, or identity evaluation.', 'Provisional geometry gate is not calibrated for production.'],
}
(out / 'results.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps(report, ensure_ascii=False, indent=2))
