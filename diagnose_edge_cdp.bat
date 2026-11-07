@echo off
setlocal EnableExtensions
chcp 65001 >nul
cd /d "%~dp0"
title AutoBot - Check Edge CDP
if exist ".venv312\Scripts\python.exe" (
  ".venv312\Scripts\python.exe" "diagnose_edge_cdp.py"
  goto :DONE
)
if exist "venv\Scripts\python.exe" (
  "venv\Scripts\python.exe" "diagnose_edge_cdp.py"
  goto :DONE
)
py -3 "diagnose_edge_cdp.py"
:DONE
echo.
echo Press any key to close.
pause >nul
endlocal
