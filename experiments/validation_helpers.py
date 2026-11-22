"""Hash all per-ID matching evidence, excluding diagnostics such as elapsed time."""
import hashlib
import json
import sqlite3


def semantic_hash(state,run_id):
    def clean(value):
        if isinstance(value,dict):return {key:clean(item) for key,item in value.items() if not key.endswith('_ms')}
        if isinstance(value,list):return [clean(item) for item in value]
        return value
    digest=hashlib.sha256()
    with sqlite3.connect(state/'cache.sqlite3') as db:
        for identifier,payload in db.execute('SELECT identifier,payload FROM fast_items WHERE run_id=? ORDER BY identifier',(run_id,)):
            row=json.loads(payload)
            digest.update(json.dumps([identifier,row['digest'],row['error'],clean(row['metrics']),row['fetch']['http_status']],sort_keys=True).encode())
    return digest.hexdigest()
