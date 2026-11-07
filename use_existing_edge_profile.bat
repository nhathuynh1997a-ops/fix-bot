@echo off
setlocal EnableExtensions
chcp 65001 >nul
cd /d "%~dp0"
title AutoBot - Su dung Edge profile dang mo

echo ================================================
echo  AUTOBOT - KET NOI PROFILE EDGE HIEN TAI
echo  Khong xoa cookies, khong tao profile moi
echo ================================================
echo.
if exist ".venv312\Scripts\python.exe" (
  ".venv312\Scripts\python.exe" "use_existing_edge_profile.py" %*
  goto :CHECK
)
if exist "venv\Scripts\python.exe" (
  "venv\Scripts\python.exe" "use_existing_edge_profile.py" %*
  goto :CHECK
)
py -3 "use_existing_edge_profile.py" %*
:CHECK
if errorlevel 1 (
    echo [LOI] Chua chuyen duoc profile. Khong can xoa hay khoi tao Edge.
) else (
    echo.
    echo [TIEP] Trong dung profile Edge muon dung, mo:
    echo        edge://inspect/#remote-debugging
    echo        Bat Allow remote debugging, giu Edge mo.
    echo        Sau do chay diagnose_edge_cdp.bat va run.bat
)
echo.
pause
endlocal
