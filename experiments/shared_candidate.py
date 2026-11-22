"""Bounded candidate comparison sharing; independent of transport and SQL ownership."""
from __future__ import annotations
from collections import OrderedDict
from concurrent.futures import Future
from dataclasses import dataclass
import hashlib
import json
import threading

from avatar_compare.engine import COMPARE_VERSION


@dataclass
class Entry:
    row:dict
    writes:list
    weight:int


def weight(row,writes):
    return len(json.dumps(row).encode())+sum(len(item) if isinstance(item,(bytes,str)) else 8
        for _,parameters in writes for item in parameters)


class SharedComparisons:
    def __init__(self,compute,*,capacity=2048,byte_limit=16*1024*1024):
        if capacity<1 or byte_limit<1:raise ValueError('Sharing cache must have positive bounds')
        self.compute,self.capacity,self.byte_limit=compute,capacity,byte_limit
        self.lock=threading.Lock();self.pending={};self.cache=OrderedDict();self.bytes=0
        self.stats={'work_computations':0,'work_hot_hits':0,'work_inflight_hits':0,'work_evictions':0,
                    'work_peak_entries':0,'work_peak_payload_bytes':0,'work_peak_pending':0}

    def prepare(self,query,data):
        key=(query.digest,hashlib.sha256(data).hexdigest(),COMPARE_VERSION)
        owner=False;shared=False
        with self.lock:
            entry=self.cache.get(key)
            if entry is not None:
                self.cache.move_to_end(key);self.stats['work_hot_hits']+=1;shared=True
            else:
                future=self.pending.get(key)
                if future is None:
                    future=Future();self.pending[key]=future;owner=True
                    self.stats['work_computations']+=1
                    self.stats['work_peak_pending']=max(self.stats['work_peak_pending'],len(self.pending))
                else:self.stats['work_inflight_hits']+=1;shared=True
        if entry is None:
            if owner:
                try:
                    row,writes=self.compute(query,data)
                    entry=Entry(row,writes,weight(row,writes))
                    with self.lock:
                        if entry.weight<=self.byte_limit:
                            self.cache[key]=entry;self.bytes+=entry.weight
                            while len(self.cache)>self.capacity or self.bytes>self.byte_limit:
                                _,removed=self.cache.popitem(last=False);self.bytes-=removed.weight
                                self.stats['work_evictions']+=1
                            self.stats['work_peak_entries']=max(self.stats['work_peak_entries'],len(self.cache))
                            self.stats['work_peak_payload_bytes']=max(self.stats['work_peak_payload_bytes'],self.bytes)
                        self.pending.pop(key)
                    future.set_result(entry)
                except BaseException as error:
                    with self.lock:self.pending.pop(key,None)
                    future.set_exception(error)
                    raise
            else:entry=future.result()
        # Writes are replayed until a successful commit is acknowledged. A shared
        # result can finish before its original producer and must be self-contained.
        with self.lock:row,writes=dict(entry.row),list(entry.writes)
        if shared:
            row.update(feature_cache='shared',pair_cache=True,extraction_ms=0,comparison_ms=0)
        return row,writes

    def committed(self,query_digest,digest):
        key=(query_digest,digest,COMPARE_VERSION)
        with self.lock:
            entry=self.cache.get(key)
            if entry is not None and entry.writes:
                old=entry.weight;entry.writes=[];entry.weight=weight(entry.row,[])
                self.bytes-=old-entry.weight

    def snapshot(self):
        with self.lock:return {**self.stats,'work_cache_entries':len(self.cache),'work_cache_payload_bytes':self.bytes}
