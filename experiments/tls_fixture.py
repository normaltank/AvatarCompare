"""Loopback TLS H1/H2 fixture; experimental dependencies only, never public data."""
from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timezone, timedelta
import ipaddress
from pathlib import Path
import socket
import ssl
import threading
import time


def certificate(folder):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    certfile,keyfile=folder/'loopback-ca.pem',folder/'loopback-key.pem'
    if certfile.exists() and keyfile.exists():
        return certfile,keyfile
    key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
    name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'AvatarCompare local fixture')])
    now=datetime.now(timezone.utc)
    cert=(x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
          .serial_number(x509.random_serial_number()).not_valid_before(now-timedelta(minutes=5))
          .not_valid_after(now+timedelta(days=3)).add_extension(x509.BasicConstraints(ca=True,path_length=0),critical=True)
          .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost'),x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),critical=False)
          .sign(key,hashes.SHA256()))
    certfile.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    keyfile.write_bytes(key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))
    return certfile,keyfile


class TLSFixture:
    def __init__(self,images,folder,*,delay=.04,faults=None,omit_length=False,token=None,retry_after='0.02'):
        self.images,self.delay,self.faults=images,delay,faults or {}
        self.omit_length,self.token,self.retry_after=omit_length,token,retry_after
        self.certfile,self.keyfile=certificate(folder)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0));self.port=sock.getsockname()[1]
        self.url=f'https://127.0.0.1:{self.port}/avatars/{{id}}'
        self.calls=Counter();self.versions=Counter();self.clients=set()
        self.active=self.peak=0
        self.stop=threading.Event();self.error=None
        self.thread=threading.Thread(target=self.run,daemon=True,name='avatar-tls-fixture')

    async def app(self,scope,receive,send):
        if scope['type']=='lifespan':
            while True:
                message=await receive()
                if message['type']=='lifespan.startup':await send({'type':'lifespan.startup.complete'})
                if message['type']=='lifespan.shutdown':
                    await send({'type':'lifespan.shutdown.complete'});return
        if scope['type']!='http':return
        try:identifier=int(scope['path'].removeprefix('/avatars/'))
        except ValueError:identifier=-1
        self.calls[identifier]+=1;attempt=self.calls[identifier]
        self.versions[scope['http_version']]+=1;self.clients.add(tuple(scope['client']))
        self.active+=1;self.peak=max(self.peak,self.active)
        try:
            await asyncio.sleep(self.delay)
            faults=self.faults.get(identifier,[])
            status=faults[min(attempt-1,len(faults)-1)] if faults else (200 if identifier in self.images else 404)
            if self.token and dict(scope['headers']).get(b'authorization')!=('Bearer '+self.token).encode():status=403
            data=self.images.get(identifier,b'') if status==200 else b'fixture error'
            headers=[(b'content-type',b'image/png')]
            if not self.omit_length:headers.append((b'content-length',str(len(data)).encode()))
            if status==429:headers.append((b'retry-after',self.retry_after.encode()))
            if status in {301,302,307,308}:headers.append((b'location',b'/avatars/1001'))
            await send({'type':'http.response.start','status':status,'headers':headers})
            await send({'type':'http.response.body','body':data})
        finally:self.active-=1

    def run(self):
        from hypercorn.asyncio import serve
        from hypercorn.config import Config
        config=Config();config.bind=[f'127.0.0.1:{self.port}']
        config.certfile=str(self.certfile);config.keyfile=str(self.keyfile)
        config.alpn_protocols=['h2','http/1.1'];config.h2_max_concurrent_streams=256
        config.accesslog=None;config.errorlog=None;config.keep_alive_timeout=5
        config.backlog=1024;config.graceful_timeout=1;config.shutdown_timeout=2
        # Separate throughput from connection-recycling fault tests (>1000 H2 requests).
        config.keep_alive_max_requests=1_000_000
        async def shutdown():
            while not self.stop.is_set():await asyncio.sleep(.02)
        async def main():
            # Windows TLS transports can wait 30s for a peer's close_notify.
            # Bound the fixture shutdown independently of client measurements.
            serving=asyncio.create_task(serve(self.app,config,shutdown_trigger=shutdown))
            while not self.stop.is_set() and not serving.done():await asyncio.sleep(.02)
            try:await asyncio.wait_for(serving,timeout=3)
            except asyncio.TimeoutError:
                serving.cancel()
                await asyncio.gather(serving,return_exceptions=True)
        try:
            # A dedicated selector loop avoids Windows Proactor close/reset races.
            with asyncio.Runner(loop_factory=asyncio.SelectorEventLoop) as runner:runner.run(main())
        except BaseException as error:self.error=error

    def __enter__(self):
        self.thread.start();deadline=time.monotonic()+10
        context=ssl.create_default_context(cafile=str(self.certfile))
        while time.monotonic()<deadline:
            if self.error:raise self.error
            try:
                with socket.create_connection(('127.0.0.1',self.port),timeout=.3) as sock:
                    with context.wrap_socket(sock,server_hostname='127.0.0.1'):return self
            except (OSError,ssl.SSLError):time.sleep(.02)
        self.stop.set();self.thread.join(timeout=5)
        raise TimeoutError('TLS fixture did not start')

    def __exit__(self,*args):
        self.stop.set();self.thread.join(timeout=15)
        if self.thread.is_alive():raise TimeoutError('TLS fixture did not stop')
        if self.error:raise self.error
