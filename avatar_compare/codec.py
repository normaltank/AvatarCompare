"""Lossless feature payload encoding; storage version is independent of matching."""
from dataclasses import fields
import io
import json
import zipfile

import numpy as np


def compact_pack(features):
    metadata = {field.name:getattr(features,field.name) for field in fields(features)
                if field.name not in {'gray','points','descriptors','preview'}}
    descriptors = features.descriptors
    if not descriptors.size or (np.isfinite(descriptors).all() and descriptors.min()>=0 and descriptors.max()<=255):
        candidate = descriptors.astype(np.uint8)
        restored = candidate.astype(descriptors.dtype)
        if restored.tobytes() == descriptors.tobytes():
            descriptors = candidate
    arrays = {'metadata':np.frombuffer(json.dumps(metadata).encode(),np.uint8),
              'gray':features.gray,'points':features.points,'descriptors':descriptors,
              'preview':np.frombuffer(features.preview,np.uint8)}
    result = io.BytesIO()
    with zipfile.ZipFile(result,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=1) as archive:
        for name,array in arrays.items():
            buffer = io.BytesIO()
            np.save(buffer,array,allow_pickle=False)
            archive.writestr(name+'.npy',buffer.getvalue(),
                compress_type=zipfile.ZIP_STORED if name=='preview' else zipfile.ZIP_DEFLATED)
    return result.getvalue()
