"""Candidate CurlMulti adapter. Experimental until end-to-end/robustness gates pass."""
from __future__ import annotations
from collections import Counter,deque
from concurrent.futures import Future,ThreadPoolExecutor
from dataclasses import dataclass,field
from email.utils import parsedate_to_datetime
import math
import queue
import threading
import time

import pycurl
from avatar_compare import __version__
from avatar_compare.pipeline_io import Packet,cached_packet
from avatar_compare.source import HttpSource,FetchFailure


@dataclass
class Job:
    identifier:int
    future:Future
    started:float=field(default_factory=time.perf_counter)
    events:list=field(default_factory=list)
    ready_at:float=0
    handle:object=None
    headers:dict=field(default_factory=dict)
    status:int|None=None
    chunks:list=field(default_factory=list)
    size:int=0
    lease:object=None
    reserve:int=0
    paused:bool=False
    failure:str|None=None


class NativeTransfers:
    protocol='h2'
    connections=None
    ca_file=None

    def __init__(self,config,workers,store,budget,cancel,*,protocol=None,connections=None,ca_file=None):
        self.protocol=protocol or type(self).protocol
        self.connections=connections or type(self).connections or (workers if self.protocol=='h1' else 8)
        self.ca_file=ca_file or type(self).ca_file
        if self.protocol not in {'h1','h2'}:raise ValueError('unsupported native protocol')
        if self.protocol=='h2' and not pycurl.version_info()[4]&pycurl.VERSION_HTTP2:
            raise ValueError('PycURL build has no HTTP/2')
        self.source=HttpSource(config)
        self.config,self.workers,self.store,self.budget,self.cancel=config,workers,store,budget,cancel
        self.queue=queue.Queue();self.stopping=threading.Event();self.failure=None
        self.versions=Counter();self.connections_opened=0
        self.lookup=ThreadPoolExecutor(max_workers=min(8,workers),thread_name_prefix='avatar-native-cache')
        self.thread=threading.Thread(target=self.run,daemon=True,name='avatar-native-http')
        self.thread.start()

    def submit(self,identifier):
        future=Future()
        self.lookup.submit(self.find,identifier,future)
        return future

    def find(self,identifier,future):
        if not future.set_running_or_notify_cancel():return
        try:
            if self.failure:raise self.failure
            cached=cached_packet(self.store,self.source.key,identifier,self.budget,self.cancel,self.config.max_bytes) if self.store else None
            if cached:future.set_result(cached)
            else:self.queue.put(Job(identifier,future))
        except FetchFailure as error:
            future.set_result(Packet(identifier,None,self.source.metadata([],time.perf_counter(),None,error.code),None))
        except BaseException as error:future.set_exception(error)

    def lease(self,job):
        if job.lease:return True
        with self.budget.condition:
            if self.budget.used+job.reserve>self.budget.limit:return False
            job.lease=self.budget.acquire(job.reserve,self.cancel)
            return True

    def headers(self,job,line):
        try:
            if line.startswith(b'HTTP/'):
                job.status=int(line.split()[1]);job.headers={}
            elif line in {b'\r\n',b'\n'}:
                if job.status is None or job.status<200:return len(line)
                if job.status!=200:
                    job.failure='http_error' if job.status>=300 else 'unexpected_status';return 0
                length=job.headers.get('content-length')
                if length and (not length.isdecimal() or int(length)>self.config.max_bytes):
                    job.failure='response_too_large_or_invalid_length';return 0
                if job.headers.get('content-encoding','identity').lower()!='identity':
                    job.failure='unsupported_content_encoding';return 0
                job.reserve=int(length) if length and not job.headers.get('transfer-encoding') else self.config.max_bytes
            elif b':' in line:
                key,value=line.decode('iso-8859-1').split(':',1);job.headers[key.strip().lower()]=value.strip()
            return len(line)
        except (ValueError,OverflowError):job.failure='response_too_large_or_invalid_length';return 0

    def write(self,job,data):
        if self.cancel.is_set():job.failure='cancelled';return 0
        try:
            if not self.lease(job):job.paused=True;return pycurl.WRITEFUNC_PAUSE
        except FetchFailure:job.failure='cancelled';return 0
        if job.size+len(data)>self.config.max_bytes:
            job.failure='response_too_large';return 0
        job.chunks.append(data);job.size+=len(data)
        return len(data)

    def handle(self,multi,job):
        handle=pycurl.Curl();job.handle=handle;job.headers={};job.status=None
        job.chunks=[];job.size=0;job.reserve=self.config.max_bytes;job.paused=False;job.failure=None
        handle.setopt(pycurl.URL,self.config.endpoint.format(id=job.identifier));handle.setopt(pycurl.PROXY,'')
        if self.ca_file:handle.setopt(pycurl.CAINFO,str(self.ca_file))
        handle.setopt(pycurl.FOLLOWLOCATION,0);handle.setopt(pycurl.NOSIGNAL,1)
        handle.setopt(pycurl.HTTP_VERSION,pycurl.CURL_HTTP_VERSION_1_1 if self.protocol=='h1' else pycurl.CURL_HTTP_VERSION_2TLS)
        handle.setopt(pycurl.PIPEWAIT,1 if self.protocol=='h2' else 0)
        handle.setopt(pycurl.TIMEOUT_MS,max(1,math.ceil(self.config.timeout*1000)))
        handle.setopt(pycurl.CONNECTTIMEOUT_MS,max(1,math.ceil(self.config.timeout*1000)))
        handle.setopt(pycurl.HTTP_CONTENT_DECODING,0)
        headers=['Accept: image/*','Accept-Encoding: identity','User-Agent: AvatarCompare/'+__version__]
        if self.source.token:headers.append('Authorization: Bearer '+self.source.token)
        handle.setopt(pycurl.HTTPHEADER,headers)
        handle.setopt(pycurl.HEADERFUNCTION,lambda line:self.headers(job,line))
        handle.setopt(pycurl.WRITEFUNCTION,lambda data:self.write(job,data))
        multi.add_handle(handle)
        self.source.requests+=1;self.source.active+=1
        self.source.peak_active=max(self.source.peak_active,self.source.active)

    def finish(self,job,error=None):
        status=job.status
        if error is None and job.failure is None:
            length=job.headers.get('content-length')
            if status!=200:job.failure='unexpected_status'
            elif length and job.size!=int(length):job.failure='incomplete_response'
        code=job.failure or ('transport_error' if error is not None else None)
        retry_after=0
        if code:
            if job.lease:job.lease.release();job.lease=None
            job.chunks=[]
            if status in {429,503}:
                try:
                    value=job.headers.get('retry-after','0')
                    try:retry_after=float(value)
                    except ValueError:retry_after=parsedate_to_datetime(value).timestamp()-time.time()
                    if not math.isfinite(retry_after):retry_after=0
                    retry_after=max(0,retry_after)
                except (ValueError,TypeError,OverflowError):retry_after=0
                self.source.next_request=max(self.source.next_request,time.monotonic()+max(.25,retry_after))
                if retry_after>120:self.cancel.set()
            retryable=code in {'transport_error','incomplete_response'} or (code=='http_error' and status in {408,429,500,502,503,504})
            job.events.append({'attempt':len(job.events)+1,'status':status,'code':code,'time':time.time()})
            if retryable and len(job.events)<=self.config.retries and not self.cancel.is_set() and not self.stopping.is_set():
                delay=max(retry_after,min(.25*2**(len(job.events)-1),4))
                job.ready_at=time.monotonic()+delay
                self.source.next_request=max(self.source.next_request,job.ready_at)
                return False
            packet=Packet(job.identifier,None,self.source.metadata(job.events,job.started,status,code),None)
        else:
            job.events.append({'attempt':len(job.events)+1,'status':status,'code':'ok','time':time.time()})
            packet=Packet(job.identifier,b''.join(job.chunks),self.source.metadata(job.events,job.started,status,None),job.lease)
            job.lease=None;job.chunks=[]
        if not job.future.done():job.future.set_result(packet)
        else:packet.release()
        return True

    def run(self):
        active={};waiting=deque();multi=None
        try:
            multi=pycurl.CurlMulti();multi.setopt(pycurl.M_MAX_HOST_CONNECTIONS,self.connections)
            multi.setopt(pycurl.M_MAX_TOTAL_CONNECTIONS,self.connections)
            multi.setopt(pycurl.M_MAX_CONCURRENT_STREAMS,self.workers)
            multi.setopt(pycurl.M_PIPELINING,pycurl.PIPE_MULTIPLEX)
            while True:
                while True:
                    try:waiting.append(self.queue.get_nowait())
                    except queue.Empty:break
                if self.cancel.is_set() or self.stopping.is_set():
                    for handle,job in list(active.items()):
                        multi.remove_handle(handle);handle.close();active.pop(handle);self.source.active-=1
                        job.failure='cancelled';self.finish(job,True)
                    while waiting:
                        job=waiting.popleft();job.failure='cancelled';self.finish(job,True)
                    if self.stopping.is_set():break
                now=time.monotonic()
                for _ in range(len(waiting)):
                    if len(active)>=self.workers or now<self.source.next_request:break
                    job=waiting.popleft()
                    if job.ready_at>now:waiting.append(job);continue
                    self.handle(multi,job);active[job.handle]=job
                    self.source.next_request=now+1/self.config.rate if self.config.rate else now
                    now=time.monotonic()
                for handle,job in list(active.items()):
                    if job.paused and self.lease(job):
                        job.paused=False;handle.pause(pycurl.PAUSE_CONT)
                if active:
                    while multi.perform()[0]==pycurl.E_CALL_MULTI_PERFORM:pass
                    while True:
                        queued,okay,failed=multi.info_read()
                        for handle,number in [(h,None) for h in okay]+[(h,n) for h,n,_ in failed]:
                            job=active.pop(handle);self.source.active-=1
                            self.versions[handle.getinfo(pycurl.INFO_HTTP_VERSION)]+=1
                            self.connections_opened+=handle.getinfo(pycurl.NUM_CONNECTS)
                            multi.remove_handle(handle);handle.close()
                            if not self.finish(job,number):waiting.append(job)
                        if not queued:break
                    if active and multi.select(.01)<0:self.stopping.wait(.001)
                else:self.stopping.wait(.002)
        except BaseException as error:
            self.failure=error
            for job in list(active.values())+list(waiting):
                if job.lease:job.lease.release();job.lease=None
                if not job.future.done():job.future.set_exception(error)
            while True:
                try:job=self.queue.get_nowait()
                except queue.Empty:break
                if not job.future.done():job.future.set_exception(error)
        finally:
            for handle in active:
                try:multi.remove_handle(handle);handle.close()
                except pycurl.error:pass
            if multi:multi.close()

    @property
    def requests(self):return self.source.requests

    @property
    def peak(self):return self.source.peak_active

    def close(self):
        self.lookup.shutdown(wait=True,cancel_futures=True)
        self.stopping.set();self.thread.join(timeout=self.config.timeout+5)
        if self.thread.is_alive():raise TimeoutError('Native reactor did not stop')
        self.source.close()
        if self.failure:raise self.failure
