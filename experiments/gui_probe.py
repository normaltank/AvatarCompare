"""Isolated GUI + delayed mock for repeatable browser workflow checks. No public HTTP."""
from pathlib import Path
import argparse
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from avatar_compare.gui import LocalServer
from avatar_compare.gui_controller import save_json
from avatar_compare.mock_service import MockServer, dataset


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    target, fixture, _ = dataset()
    images = {identifier: fixture[1001 + (identifier - 1001) % 16] for identifier in range(1001, 1121)}
    images.pop(1017)
    images[1018] = b'invalid image'
    (output / 'target.png').write_bytes(target)
    with MockServer(images, delay=0.15) as source, LocalServer(output / 'app') as server:
        save_json(output / 'probe-private.json', {'ui_port': server.server_port, 'ui_token': server.token,
                                                 'endpoint': source.url, 'target': str(output / 'target.png')})
        print('Isolated browser fixture ready', flush=True)
        try:
            server.serve_forever(poll_interval=0.1)
        finally:
            server.controller.stop()
            if server.controller.worker:
                server.controller.worker.join()
            save_json(output / 'audit.json', {'requests': sum(source.calls.values()),
                                            'counts': dict(source.calls), 'peak': source.peak,
                                            'status': server.controller.snapshot()['status']})


if __name__ == '__main__':
    main()
