"""Real loopback HTTP workflow, counterbalanced timing and multi-seed evidence probe."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import platform
import statistics
import sys
import threading
import time
import webbrowser

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import cv2
import numpy as np
import PIL

from avatar_compare.mock_service import MockServer, dataset
from avatar_compare.range_engine import search_range
from avatar_compare.report import write_reports
from avatar_compare.source import SourceConfig


def accepted(row):
    metric = row['metrics']
    return bool(metric and (metric['exact_file'] or metric['exact_pixels'] or metric['geometry']['consistent']))


def run(output: Path, *, full=False):
    output.mkdir(parents=True, exist_ok=True)
    target, images, positive = dataset()
    target_path = output/'target.png'
    target_path.write_bytes(target)
    images.update({1018: b'not an image', 1019: target, 1020: target})
    with MockServer(images, faults={1019: [503, 200], 1020: [503]}, delay=0.04) as server:
        config = SourceConfig(server.url, 'synthetic-v1', name='本机合成验证接口', rate=100, retries=2)
        stop = threading.Event()
        events = []
        def progress(event):
            events.append(event)
            if event['processed'] >= 4:
                stop.set()
        state = output/'workflow-state'
        first = search_range(target_path, 1001, 1020, state, config, cancel=stop, progress=progress)
        complete = search_range(target_path, 1001, 1020, state, config, progress=events.append)
        calls = sum(server.calls.values())
        resumed = search_range(target_path, 1001, 1020, state, config)
        assert first['status'] == 'stopped' and complete['stats']['processed'] == 20
        assert complete['stats']['errors'] == 3 and resumed['stats']['requests'] == 0
        assert calls == sum(server.calls.values())
        complete['dataset_note'] = '全部图片为确定性合成素材。含缺图、坏图、暂时/持续503，用于验证完整流程。'
        reports = write_reports(complete, output, limit=20)
        summary = {'workflow': {'stopped_processed': first['stats']['processed'], 'completed': complete['stats'],
                                'same_run_resume': resumed['stats'], 'http_calls': calls},
                   'report': str(reports['html'])}
        (output/'progress-events.json').write_text(json.dumps(events, ensure_ascii=False, indent=2), encoding='utf-8')

    if full:
        timings = []
        clean_target, clean_images, _ = dataset()
        target_path.write_bytes(clean_target)
        with MockServer(clean_images, delay=0.04) as server:
            config = SourceConfig(server.url, 'timing-v1', rate=100, retries=0)
            for repeat, order in enumerate([(1, 2, 4), (2, 4, 1), (4, 1, 2)]):
                for workers in order:
                    state = output/f'timing-{repeat}-{workers}'
                    cold = search_range(target_path, 1001, 1016, state, config, workers=workers)
                    restored = search_range(target_path, 1001, 1016, state, config, workers=workers)
                    # Different target digest forces a new task while reusing downloads/features.
                    alt_target = output/f'alternative-{repeat}-{workers}.png'
                    alt_target.write_bytes(clean_images[1004])
                    reused = search_range(alt_target, 1001, 1016, state, config, workers=workers)
                    assert cold['stats']['errors'] == reused['stats']['errors'] == 0
                    assert restored['stats']['requests'] == reused['stats']['requests'] == 0
                    timings.append({'workers': workers, 'repeat': repeat, 'cold': cold['stats'],
                                    'resume': restored['stats'], 'cached_download_new_target': reused['stats']})
        summary['timings'] = timings
        summary['median_seconds'] = {str(workers): {stage: statistics.median(row[stage]['elapsed_seconds'] for row in timings if row['workers'] == workers)
                                                   for stage in ['cold', 'resume', 'cached_download_new_target']} for workers in [1, 2, 4]}
        # This is deliberately multiple deterministic source images, all synthetic.
        truth = []
        for seed in list(range(31, 41))+list(range(101, 121)):
            raw, images, positives = dataset(seed)
            query = output/f'accuracy-target-{seed}.png'
            query.write_bytes(raw)
            with MockServer(images) as server:
                result = search_range(query, 1001, 1016, output/f'accuracy-state-{seed}',
                                      SourceConfig(server.url, f'seed-{seed}', rate=100), workers=2)
                assert result['stats']['errors'] == 0
                truth.extend({'seed': seed, 'identifier': row['identifier'], 'positive': int(row['identifier']) in positives,
                              'accepted': accepted(row), 'metrics': row['metrics']} for row in result['results'])
        summary['accuracy'] = {}
        for group, subset in [('regression', [row for row in truth if row['seed'] < 100]),
                              ('holdout', [row for row in truth if row['seed'] >= 100])]:
            tp = sum(row['positive'] and row['accepted'] for row in subset)
            fp = sum(not row['positive'] and row['accepted'] for row in subset)
            fn = sum(row['positive'] and not row['accepted'] for row in subset)
            tn = sum(not row['positive'] and not row['accepted'] for row in subset)
            summary['accuracy'][group] = {'source_images': len({row['seed'] for row in subset}),
                'positive_pairs': tp+fn, 'negative_pairs': fp+tn, 'tp': tp, 'fp': fp, 'fn': fn, 'tn': tn,
                'precision': tp/(tp+fp) if tp+fp else None, 'recall': tp/(tp+fn),
                'scope': 'Synthetic patterns only; no real avatar accuracy or identity validation.'}
        (output/'accuracy-pairs.json').write_text(json.dumps(truth, indent=2), encoding='utf-8')
    summary['environment'] = {'python': platform.python_version(), 'opencv': cv2.__version__,
                              'numpy': np.__version__, 'pillow': PIL.__version__, 'time': time.time(),
                              'mock_delay_seconds': 0.04, 'timing_rate': 100}
    (output/'validation.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=Path('outputs')/('range-probe-'+time.strftime('%Y%m%d-%H%M%S')))
    parser.add_argument('--full', action='store_true', help='Also run 3-repeat timings and 480 synthetic pairs')
    parser.add_argument('--open-report', action='store_true')
    args = parser.parse_args()
    summary = run(args.output.resolve(), full=args.full)
    print(json.dumps({key: value for key, value in summary.items() if key != 'timings'}, ensure_ascii=False, indent=2))
    if args.open_report:
        webbrowser.open(Path(summary['report']).as_uri())
