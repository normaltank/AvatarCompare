"""Reproducible application-cache and bounded-worker measurements on one local dataset."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from avatar_compare.engine import search

parser = argparse.ArgumentParser()
parser.add_argument('--image', required=True, type=Path)
parser.add_argument('--folder', required=True, type=Path)
parser.add_argument('--output', required=True, type=Path)
parser.add_argument('--repeats', type=int, default=3)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
records = []
orders = []
for repeat in range(args.repeats):
    order = [1, 2, 4]
    shift = repeat % 3
    order = order[shift:] + order[:shift]
    orders.append(order)
    for workers in order:
        state = args.output/f'w{workers}-r{repeat}'
        if (state/'cache.sqlite3').exists():
            raise RuntimeError('Use a new output folder so the empty-cache measurement stays valid.')
        for mode, options in [('empty_application_cache', {}), ('cached_features_only', {'resume':False,'pair_cache':False}), ('same_query_resumed', {})]:
            started = time.perf_counter()
            result = search(args.image, args.folder, state, workers=workers, **options)
            records.append({'workers':workers,'repeat':repeat,'mode':mode,'wall_seconds':time.perf_counter()-started,'stats':result['stats']})
summary = []
for workers in [1,2,4]:
    for mode in ['empty_application_cache','cached_features_only','same_query_resumed']:
        values = [row['wall_seconds'] for row in records if row['workers']==workers and row['mode']==mode]
        summary.append({'workers':workers,'mode':mode,'median_seconds':statistics.median(values),'samples_seconds':values})
document = {'logical_cpu_count':os.cpu_count(),'worker_orders':orders,'records':records,'summary':summary,'scope':['Local 17-image fixture unless caller selected another dataset.','Empty application cache; OS page cache not cleared.','Cached features experiment disables pair cache and result resume.','Same-query resume reuses already persisted results.','Worker order is counterbalanced across repeats; background system activity is uncontrolled.','No model inference or remote-avatar transfer is measured.']}
(args.output/'performance.json').write_text(json.dumps(document,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps({'summary':summary,'path':str(args.output/'performance.json')},ensure_ascii=False,indent=2))
