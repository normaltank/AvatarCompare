"""Build a source-only release and enforce native Windows launcher line endings."""
from pathlib import Path
import sys
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from avatar_compare import __version__


def build():
    app = Path(__file__).resolve().parents[1]
    files = [path for name in ['avatar_compare', 'tests', 'experiments'] for path in (app/name).glob('*.py')]
    files += list((app/'docs').glob('*.md'))
    files += [path for path in (app/'avatar_compare'/'web').iterdir() if path.suffix in {'.html', '.css', '.js'}]
    files += [app/name for name in [
        'README.md', 'CHANGELOG.md', 'CONTRIBUTING.md', 'DESIGN.md', 'LICENSE',
        'pyproject.toml', 'requirements.txt', '安装依赖.ps1', '.gitattributes', '.gitignore',
    ]]
    files += list(app.glob('*.cmd'))
    archive = app/'release'/f'AvatarCompare_range_{__version__}.zip'
    archive.parent.mkdir(exist_ok=True)
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as output:
        for path in sorted(files):
            data = path.read_bytes()
            if path.suffix == '.cmd':
                data = data.replace(b'\r\n', b'\n').replace(b'\n', b'\r\n')
            output.writestr('AvatarCompare/'+path.relative_to(app).as_posix(), data)
    with zipfile.ZipFile(archive) as output:
        assert output.testzip() is None
        for name in output.namelist():
            assert not any(part in name for part in ['.state/', '.state-compact/', '.ui/', '.experiments/', 'outputs/', '.venv/', '__pycache__/'])
            if name.endswith('.cmd'):
                assert b'\n' not in output.read(name).replace(b'\r\n', b'')
    return archive


if __name__ == '__main__':
    print(build())
