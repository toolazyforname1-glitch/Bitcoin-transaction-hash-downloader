@echo off
title Bitcoin txid collector
chcp 65001 >nul
cd /d "%~dp0"
python "src\A_launcher.py"
echo.
pause
