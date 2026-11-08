@echo off
title build txid-collector
cd /d "%~dp0"
if not exist ".venv-build\Scripts\python.exe" (
  echo Creating build venv...
  python -m venv .venv-build || goto :err
)
echo Installing build dependencies...
.venv-build\Scripts\python.exe -m pip install --disable-pip-version-check -q --upgrade ^
  pip pyinstaller requests pyarrow rich psutil || goto :err
echo Building...
.venv-build\Scripts\python.exe -m PyInstaller --noconfirm --clean --onefile --console ^
  --name txid-collector --paths src --hidden-import psutil --exclude-module tkinter ^
  src\A_launcher.py || goto :err
echo.
echo Built: dist\txid-collector.exe
pause
exit /b 0
:err
echo BUILD FAILED
pause
exit /b 1
