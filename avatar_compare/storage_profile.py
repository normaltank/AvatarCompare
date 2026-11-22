"""Keep compact scanning isolated from existing complete/local image caches."""
from contextlib import closing
from pathlib import Path
import sqlite3


def table_exists(db,name):
    return db.execute('SELECT 1 FROM sqlite_master WHERE type="table" AND name=?',(name,)).fetchone() is not None


def require_profile(state_dir,*,compact):
    path=Path(state_dir)/'cache.sqlite3'
    if not path.exists():return
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        profile=db.execute('SELECT profile FROM storage_profile WHERE id=1').fetchone() if table_exists(db,'storage_profile') else None
        if profile:
            if profile[0]!='compact-v1':raise ValueError('不支持的缓存保留版本，请使用新目录')
            if (profile[0]=='compact-v1')!=compact:
                raise ValueError('省空间库与完整库不能混用；请使用对应模式或不同的 --state-dir')
            return
        if compact:
            for name in ['runs','features','raw_images','acquisitions','files']:
                if table_exists(db,name) and db.execute(f'SELECT 1 FROM {name} LIMIT 1').fetchone():
                    raise ValueError('省空间模式请使用新目录；不会清理已有完整缓存或用户图片')


def range_specs_schema(db):
    db.executescript('''CREATE TABLE IF NOT EXISTS range_specs(
        run_id TEXT PRIMARY KEY, query_digest TEXT, source TEXT, algorithm TEXT,
        first_id INTEGER, last_id INTEGER, retention TEXT, source_name TEXT, source_revision TEXT);''')


def save_range_spec(db,run_id,query_digest,source,algorithm,start,end,retention,config):
    db.execute('INSERT OR REPLACE INTO range_specs VALUES(?,?,?,?,?,?,?,?,?)',
               (run_id,query_digest,source,algorithm,start,end,retention,config.name,config.revision))
