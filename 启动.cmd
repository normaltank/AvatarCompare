@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\安装依赖.ps1"
  if errorlevel 1 (
    pause
    exit /b 1
  )
)
".venv\Scripts\python.exe" -m avatar_compare.gui --launch
if errorlevel 1 pause
