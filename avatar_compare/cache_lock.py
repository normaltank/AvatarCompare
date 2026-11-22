"""One active owner per cache directory; reentrant within that owner thread."""
from __future__ import annotations

from pathlib import Path
import os
import threading

_guard = threading.Lock()
_owners = {}


class CacheLease:
    def __init__(self, folder):
        self.folder = Path(folder).resolve()
        self.key = os.path.normcase(str(self.folder))
        self.entered = False

    def __enter__(self):
        self.folder.mkdir(parents=True, exist_ok=True)
        identity = (os.getpid(), threading.get_ident())
        with _guard:
            owner = _owners.get(self.key)
            if owner:
                if owner[0] != identity:
                    raise ValueError('同一缓存目录已有活动任务；请等待完成或使用不同的 --state-dir')
                owner[2] += 1
            else:
                stream = (self.folder/'active.lock').open('a+b')
                try:
                    stream.seek(0, 2)
                    if not stream.tell():
                        stream.write(b'0'); stream.flush()
                    stream.seek(0)
                    if os.name == 'nt':
                        import msvcrt
                        msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(stream.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)
                except OSError:
                    stream.close()
                    raise ValueError('同一缓存目录已有活动任务；请等待完成或使用不同的 --state-dir') from None
                _owners[self.key] = [identity, stream, 1]
            self.entered = True
        return self

    def close(self):
        if not self.entered:
            return
        with _guard:
            owner = _owners[self.key]
            owner[2] -= 1
            if not owner[2]:
                stream = owner[1]
                stream.seek(0)
                if os.name == 'nt':
                    import msvcrt
                    msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
                stream.close()
                del _owners[self.key]
            self.entered = False

    def __exit__(self, *args):
        self.close()
