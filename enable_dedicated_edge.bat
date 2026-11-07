@echo off
setlocal EnableExtensions
chcp 65001 >nul
cd /d "%~dp0"
title AutoBot - Switch to Dedicated Edge Profile
if exist ".venv312\Scripts\python.exe" (
  ".venv312\Scripts\python.exe" "switch_edge_profile.py"
  goto :DONE
)
if exist "venv\Scripts\python.exe" (
  "venv\Scripts\python.exe" "switch_edge_profile.py"
  goto :DONE
)
py -3 "switch_edge_profile.py"
:DONE
echo.
echo Press any key to close.
pause >nul
endlocal
