"""Bounded raw-byte ownership and transfer workers independent of compute/write."""
from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import json
import threading

from .source import HttpSource, FetchFailure


class ByteBudget:
    def __init__(self, limit):
        self.limit, self.used, self.peak = limit, 0, 0
        self.condition = threading.Condition()
        self.closed = False
        self.leases = set()

    def acquire(self, size, cancel):
        if not 0 <= size <= self.limit:
            raise ValueError('单个响应预留量超过内存缓冲上限')
        with self.condition:
            while self.used+size > self.limit and not cancel.is_set() and not self.closed:
                self.condition.wait(0.05)
            if cancel.is_set() or self.closed:
                raise FetchFailure('cancelled')
            lease = Lease(self, size)
            self.used += size
            self.peak = max(self.peak, self.used)
            self.leases.add(lease)
            return lease

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()

    def release_all(self):
        for lease in list(self.leases):
            lease.release()


class Lease:
    def __init__(self, budget, size):
        self.budget, self.size, self.released = budget, size, False

    def release(self):
        with self.budget.condition:
            if not self.released:
                self.released = True
                self.budget.used -= self.size
                self.budget.leases.discard(self)
                self.budget.condition.notify_all()


@dataclass
class Packet:
    identifier: int
    data: bytes | None
    fetch: dict
    lease: Lease | None
    cached: bool = False
    acquisition: dict | None = None

    def release(self):
        if self.lease:
            self.lease.release()


def cached_packet(store, source_key, identifier, budget, cancel, max_bytes):
    record = store.db().execute('SELECT payload FROM acquisitions WHERE source=? AND identifier=?', (source_key,str(identifier))).fetchone()
    if not record:
        return None
    acquisition = json.loads(record['payload'])
    if acquisition['error']:
        return Packet(identifier,None,acquisition['fetch'],None,True,acquisition)
    if acquisition.get('storage') == 'sqlite':
        record = store.db().execute('SELECT length(payload) AS size FROM raw_images WHERE digest=?', (acquisition['digest'],)).fetchone()
        size = record['size'] if record else max_bytes+1
    else:
        from pathlib import Path
        try:
            size = Path(acquisition['path']).stat().st_size
        except OSError:
            return None
    if size > max_bytes:
        return None
    lease = budget.acquire(size,cancel)
    try:
        data = store.raw_bytes(acquisition)
        if data is not None:
            return Packet(identifier,data,acquisition['fetch'],lease,True,acquisition)
    except BaseException:
        lease.release()
        raise
    lease.release()
    return None


class BudgetedSource(HttpSource):
    def __init__(self, config, budget, cancel):
        super().__init__(config)
        self.budget, self.cancel = budget, cancel
        self.local = threading.local()

    @contextmanager
    def open(self, request):
        lease = None
        try:
            with super().open(request) as response:
                if response.status == 200:
                    length = response.headers.get('Content-Length')
                    size = (int(length) if length and length.isdecimal() and not response.headers.get('Transfer-Encoding')
                            else self.config.max_bytes)
                    if size <= self.config.max_bytes:
                        lease = self.budget.acquire(size,self.cancel)
                        self.local.lease = lease
                yield response
        except BaseException:
            if lease:
                lease.release()
                self.local.lease = None
            raise


class ThreadTransfers:
    def __init__(self, config, workers, store, budget, cancel):
        self.source = BudgetedSource(config,budget,cancel)
        self.store, self.budget, self.cancel = store,budget,cancel
        self.pool = ThreadPoolExecutor(max_workers=workers,thread_name_prefix='avatar-download')

    def submit(self, identifier):
        return self.pool.submit(self.work,identifier)

    def work(self, identifier):
        try:
            cached = cached_packet(self.store,self.source.key,identifier,self.budget,self.cancel,self.source.config.max_bytes)
        except FetchFailure as error:
            if error.code != 'cancelled':
                raise
            return Packet(identifier,None,self.source.metadata([],__import__('time').perf_counter(),None,'cancelled'),None)
        if cached:
            return cached
        self.source.local.lease = None
        data, metadata = self.source.fetch(identifier,self.cancel)
        lease = self.source.local.lease
        if data is None and lease:
            lease.release(); lease = None
        return Packet(identifier,data,metadata,lease)

    @property
    def requests(self):
        return self.source.requests

    @property
    def peak(self):
        return self.source.peak_active

    def close(self):
        self.pool.shutdown(wait=True,cancel_futures=True)
        self.source.close()
