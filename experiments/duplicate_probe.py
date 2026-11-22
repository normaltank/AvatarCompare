"""Deterministic precommit duplicate-work reproduction without any HTTP requests."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import tempfile
import threading
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import cv2
from avatar_compare.engine import Store
from avatar_compare.features import extract
from avatar_compare.mock_service import dataset


def main():
    cv2.setNumThreads(1)
    target,images,_=dataset();count=8;calls=0;guard=threading.Lock();barrier=threading.Barrier(count)
    with tempfile.TemporaryDirectory(prefix='avatar-duplicate-probe-') as temporary:
        store=Store(Path(temporary)/'cache.sqlite3')
        try:
            query=extract(target)
            def spy(data):
                nonlocal calls
                with guard:calls+=1
                return extract(data)
            store.extractor=spy
            def work(_):
                barrier.wait();return store.prepare_bytes(query,images[1003])
            with ThreadPoolExecutor(max_workers=count) as pool:rows=list(pool.map(work,range(count)))
            print(json.dumps({'simultaneous_same_image':count,'actual_extractions':calls,
                'per_id_results':len(rows),'feature_rows_committed':store.db().execute('SELECT count(*) FROM features').fetchone()[0]}))
            assert calls==8,'Baseline changed: duplicate-work reproduction needs review'
        finally:store.close()


if __name__=='__main__':main()
