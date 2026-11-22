"""Lossless feature encoding probe; compare speed, bytes and restored values."""
from __future__ import annotations
import json
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import numpy as np
from avatar_compare.engine import pack,unpack
from avatar_compare.codec import compact_pack
from avatar_compare.features import extract
from avatar_compare.mock_service import encode,synthetic_picture


if __name__=='__main__':
    features=[extract(encode(synthetic_picture(seed).resize((100,100)))) for seed in range(64)]
    result={}
    for name,function in [('v04',pack),('compact',compact_pack)]:
        samples=[]
        for _ in range(3):
            start=time.perf_counter();payloads=[function(feature) for feature in features]
            samples.append(time.perf_counter()-start)
        for feature,blob in zip(features,payloads):
            restored=unpack(blob)
            for attribute in ['gray','points','descriptors']:
                left,right=getattr(feature,attribute),getattr(restored,attribute)
                assert left.dtype==right.dtype and left.tobytes()==right.tobytes()
            assert feature.preview==restored.preview
        result[name]={'pack_seconds':samples,'median_ms_per_image':statistics.median(samples)/len(features)*1000,
                      'bytes':sum(map(len,payloads)),'images':len(features)}
    path=Path('outputs/transport-diagnosis-20261106/codec-production.json');path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result,indent=2))
