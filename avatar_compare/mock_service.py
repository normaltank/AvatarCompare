"""Loopback-only synthetic image service for unattended end-to-end validation."""
from __future__ import annotations

from collections import Counter
from contextlib import AbstractContextManager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import random
import threading
import time

from PIL import Image, ImageDraw


def encode(image, fmt='PNG', **kwargs):
    buffer = io.BytesIO()
    image.save(buffer, format=fmt, **kwargs)
    return buffer.getvalue()


def synthetic_picture(seed):
    rng = random.Random(seed)
    image = Image.new('RGB', (320, 320), '#e4e2d8')
    draw = ImageDraw.Draw(image)
    for _ in range(90):
        x, y = rng.randrange(290), rng.randrange(290)
        size = rng.randrange(4, 24)
        color = tuple(rng.randrange(240) for _ in range(3))
        draw.ellipse((x, y, x+size, y+size), fill=color)
    return image


def dataset(seed=17):
    image = synthetic_picture(seed)
    screenshot = Image.new('RGB', (380, 420), '#485454')
    screenshot.paste(image, (30, 50))
    positive = [encode(image), encode(image, 'BMP'), encode(image.resize((100, 100)), 'JPEG', quality=30),
                encode(image.crop((32, 32, 288, 288))), encode(screenshot),
                encode(image.resize((160, 160)), 'JPEG', quality=75)]
    images = {1001+i: data for i, data in enumerate(positive)}
    for index in range(10):
        images[1007+index] = encode(synthetic_picture(seed*1000+index))
    return encode(image), images, set(range(1001, 1007))


class MockServer(AbstractContextManager):
    def __init__(self, images=None, *, faults=None, delay=0.0, token=None, port=0):
        self.images = images if images is not None else dataset()[1]
        self.faults = faults or {}
        self.delay = delay
        self.token = token
        self.calls = Counter()
        self.timestamps = []
        self.client_ports = set()
        self.active = self.peak = 0
        self.lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            disable_nagle_algorithm = True

            def handle(self):
                try:
                    super().handle()
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    pass
            def log_message(self, *args):
                pass

            def do_GET(self):
                try:
                    identifier = int(self.path.removeprefix('/avatars/'))
                except ValueError:
                    self.send_error(404)
                    return
                with owner.lock:
                    owner.calls[identifier] += 1
                    attempt = owner.calls[identifier]
                    owner.timestamps.append(time.monotonic())
                    owner.client_ports.add(self.client_address[1])
                    owner.active += 1
                    owner.peak = max(owner.peak, owner.active)
                try:
                    time.sleep(owner.delay)
                    statuses = owner.faults.get(identifier, [])
                    status = statuses[min(attempt-1, len(statuses)-1)] if statuses else 200
                    if owner.token and self.headers.get('Authorization') != 'Bearer '+owner.token:
                        status = 403
                    if identifier not in owner.images and status == 200:
                        status = 404
                    self.send_response(status)
                    if status in {301, 302, 307, 308}:
                        self.send_header('Location', '/avatars/1001')
                    if status == 429:
                        self.send_header('Retry-After', '0.01')
                    data = owner.images.get(identifier, b'') if status == 200 else b'error'
                    self.send_header('Content-Type', 'image/png' if status == 200 else 'text/plain')
                    self.send_header('Content-Length', str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    pass
                finally:
                    with owner.lock:
                        owner.active -= 1

        class Server(ThreadingHTTPServer):
            request_queue_size = 512

        self.server = Server(('127.0.0.1', port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.url = f'http://127.0.0.1:{self.server.server_port}/avatars/{{id}}'

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='仅本机的合成头像测试接口')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--fixture-dir', type=Path, default=Path('outputs/mock-fixtures'))
    args = parser.parse_args()
    target, images, positive = dataset()
    args.fixture_dir.mkdir(parents=True, exist_ok=True)
    (args.fixture_dir/'target.png').write_bytes(target)
    manifest = {'start': 1001, 'end': 1020, 'positive_ids': sorted(positive), 'missing': [1017],
                'invalid_image': [1018], 'transient_503': [1019], 'persistent_503': [1020]}
    (args.fixture_dir/'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    images.update({1018: b'invalid image', 1019: target, 1020: target})
    with MockServer(images, faults={1019: [503, 200], 1020: [503]}, delay=0.04, port=args.port) as server:
        print('合成测试接口：'+server.url, flush=True)
        print('目标：'+str((args.fixture_dir/'target.png').resolve()), flush=True)
        print('编号1001–1020；Ctrl+C停止。', flush=True)
        try:
            while True:
                time.sleep(0.5)
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    main()
