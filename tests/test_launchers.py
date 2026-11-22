"""Exercise shipped CMD bytes through real cmd.exe without UI or network."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


APP = Path(__file__).resolve().parents[1]
LAUNCHERS = {'启动.cmd': ['-m', 'avatar_compare.gui', '--launch']}


@unittest.skipUnless(sys.platform == 'win32', 'Windows cmd.exe regression')
class NativeLauncherTests(unittest.TestCase):
    def run_launcher(self, name, *, installed):
        with tempfile.TemporaryDirectory(prefix='avatar-launch-中文 ') as temporary:
            root = Path(temporary)/'项目 with spaces'
            root.mkdir()
            shutil.copyfile(APP/name, root/name)
            result = root/'invocation.json'
            bootstrap = root/'installed.txt'
            capture = "import json, os, sys\nfrom pathlib import Path\nPath(os.environ['AVATAR_LAUNCH_TEST_RESULT']).write_text(json.dumps({'argv':sys.argv,'cwd':str(Path.cwd())}), encoding='utf-8')\n"
            package = root/'avatar_compare'
            package.mkdir()
            (package/'__init__.py').write_text('', encoding='utf-8')
            (package/'__main__.py').write_text(capture, encoding='utf-8')
            (package/'history.py').write_text(capture,encoding='utf-8')
            (package/'gui.py').write_text(capture,encoding='utf-8')
            experiments = root/'experiments'
            experiments.mkdir()
            (experiments/'range_probe.py').write_text(capture, encoding='utf-8')
            (experiments/'limit_suite.py').write_text(capture, encoding='utf-8')
            config = 'home = '+str(Path(sys._base_executable).parent)+'\ninclude-system-site-packages = false\n'
            if installed:
                scripts = root/'.venv'/'Scripts'
                scripts.mkdir(parents=True)
                shutil.copyfile(sys.executable, scripts/'python.exe')
                (root/'.venv'/'pyvenv.cfg').write_text(config, encoding='utf-8')
            # Isolate bootstrap too: never install dependencies or contact a network.
            quote = lambda value: "'"+str(value).replace("'", "''")+"'"
            installer = ["$ErrorActionPreference = 'Stop'",
                         "$scripts = Join-Path $PSScriptRoot '.venv\\Scripts'",
                         'New-Item -ItemType Directory -Force -Path $scripts | Out-Null',
                         f"Copy-Item -LiteralPath {quote(sys.executable)} -Destination (Join-Path $scripts 'python.exe')",
                         f"[IO.File]::WriteAllText((Join-Path $PSScriptRoot '.venv\\pyvenv.cfg'), {quote(config)})",
                         f"[IO.File]::WriteAllText({quote(bootstrap)}, 'ok')"]
            (root/'安装依赖.ps1').write_text('\n'.join(installer), encoding='utf-8-sig')
            env = os.environ.copy()
            env['AVATAR_LAUNCH_TEST_RESULT'] = str(result)
            env['AVATAR_LAUNCH_TEST_SCRIPT'] = str(root/name)
            command = '"'+env.get('COMSPEC', 'cmd.exe')+'" /d /c call "%AVATAR_LAUNCH_TEST_SCRIPT%"'
            completed = subprocess.run(command,
                                       cwd=temporary, env=env, input=b'\r\n', capture_output=True, timeout=15)
            output = (completed.stdout+completed.stderr).decode('utf-8', errors='replace')
            self.assertEqual(completed.returncode, 0, output)
            self.assertTrue(result.is_file(), output)
            data = json.loads(result.read_text(encoding='utf-8'))
            self.assertEqual(Path(data['cwd']), root)
            self.assertEqual(data['argv'][1:], LAUNCHERS[name][2:] if LAUNCHERS[name][0] == '-m' else LAUNCHERS[name][1:])
            self.assertEqual(bootstrap.is_file(), not installed)
            self.assertNotIn('not recognized', output)

    def test_native_launchers_with_existing_environment(self):
        self.assertEqual({path.name for path in APP.glob('*.cmd')}, set(LAUNCHERS))
        for name in LAUNCHERS:
            with self.subTest(launcher=name):
                self.run_launcher(name, installed=True)

    def test_native_launchers_bootstrap_missing_environment(self):
        for name in LAUNCHERS:
            with self.subTest(launcher=name):
                self.run_launcher(name, installed=False)


if __name__ == '__main__':
    unittest.main()
