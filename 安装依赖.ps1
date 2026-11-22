$ErrorActionPreference = 'Stop'
$appRoot = $PSScriptRoot
Set-Location -LiteralPath $appRoot
$venvPython = Join-Path $appRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $venvPython)) {
    python -c "import sys; assert (3,10) <= sys.version_info[:2] < (3,14), 'Please use Python 3.10-3.13; tested with 3.12.'"
    if ($LASTEXITCODE -ne 0) { throw '需要 Python 3.10–3.13；本机已验证 Python 3.12。' }
    python -m venv (Join-Path $appRoot '.venv')
    if ($LASTEXITCODE -ne 0) { throw '无法创建本地运行环境。' }
}
& $venvPython -m pip install --index-url https://pypi.org/simple --disable-pip-version-check --no-input -r (Join-Path $appRoot 'requirements.txt')
if ($LASTEXITCODE -ne 0) { throw '依赖安装失败，请查看上方信息。' }
& $venvPython -m pip check
if ($LASTEXITCODE -ne 0) { throw '依赖检查未通过。' }
Write-Output '本地环境已准备好，可以运行“启动.cmd”。'
