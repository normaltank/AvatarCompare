"""Compare bounded pipelines over synthetic loopback HTTP, never public IDs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from avatar_compare.mock_service import MockServer, encode, synthetic_picture
from avatar_compare.range_engine import search_range
from avatar_compare.report import write_reports
from avatar_compare.source import SourceConfig


def run(output, count=96):
    output.mkdir(parents=True, exist_ok=True)
    images = {1001+i: encode(synthetic_picture(17 if i == 0 else 2000+i).resize((100, 100))) for i in range(count)}
    target = output/'target.png'
    target.write_bytes(images[1001])
    samples = []
    modes = ['original-2', 'pooled-2', 'full-16']
    with MockServer(images, delay=0.04) as server:
        for repeat in range(3):
            for mode in modes[repeat:]+modes[:repeat]:
                fast = mode != 'original-2'
                config = SourceConfig(server.url, 'benchmark-v1', rate=0 if fast else 100, retries=0, keep_alive=fast)
                state = output/f'state-{repeat}-{mode}'
                result = search_range(target, 1001, 1000+count, state, config, workers=2,
                                      download_workers=16 if mode == 'full-16' else 2,
                                      stream_results=fast, top_limit=20)
                assert result['status'] == 'complete' and result['stats']['errors'] == 0
                assert result['stats']['processed'] == count
                resume = search_range(target, 1001, 1000+count, state, config, workers=2,
                                      download_workers=16 if mode == 'full-16' else 2,
                                      stream_results=fast, top_limit=20)
                assert resume['stats']['requests'] == 0 and resume['stats']['resumed'] == count
                sample = {'mode': mode, 'repeat': repeat, 'stats': result['stats'], 'resume': resume['stats'],
                          'database_bytes': (state/'cache.sqlite3').stat().st_size,
                          'download_bytes': sum(p.stat().st_size for p in (state/'downloads').rglob('*.img'))}
                samples.append(sample)
                print(json.dumps({'mode': mode, 'repeat': repeat, 'seconds': result['stats']['elapsed_seconds']}), flush=True)
                if mode == 'full-16' and repeat == 0:
                    result['dataset_note'] = '全速模式模拟验证：100像素合成头像、96个编号、40ms响应延迟；非公网吞吐。'
                    report = write_reports(result, output)
                    report_path = str(report['html'])
    summary = {'count': count, 'mock_delay_seconds': 0.04, 'samples': samples, 'report': report_path,
               'median_seconds': {mode: statistics.median(s['stats']['elapsed_seconds'] for s in samples if s['mode'] == mode) for mode in modes},
               'scope': 'Synthetic loopback HTTP, 3 counterbalanced runs, fresh app state, no public endpoint load test.'}
    summary['median_items_per_second'] = {mode: count/seconds for mode, seconds in summary['median_seconds'].items()}
    (output/'performance.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({key: value for key, value in summary.items() if key != 'samples'}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=Path('outputs')/('fast-probe-'+time.strftime('%Y%m%d-%H%M%S')))
    args = parser.parse_args()
    run(args.output.resolve())
