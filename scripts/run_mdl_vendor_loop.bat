@echo off
REM =====================================================================
REM BrokerOps - MDL Vendor Dispatcher + Reply Sweep Runner
REM
REM Invoked every 5 minutes by the Windows Scheduled Task
REM   BrokerOps-MDL-Vendor-Dispatcher
REM
REM Runs a single dispatcher cycle followed by a single reply sweep.
REM Appends timestamped stdout/stderr to scripts\logs\mdl_vendor_loop.log.
REM Exits with the dispatcher's exit code (reply sweep errors are logged
REM but do not fail the task -- the dispatcher is the critical path).
REM
REM To pause:  schtasks /change /tn "BrokerOps-MDL-Vendor-Dispatcher" /disable
REM To resume: schtasks /change /tn "BrokerOps-MDL-Vendor-Dispatcher" /enable
REM See docs\mdl_vendor_cron_wiring.md for full operational notes.
REM =====================================================================

cd /d C:\Users\Owner\brokerops-ai
set PYTHONPATH=.
set PYTHON_EXE=C:\Python314\python.exe
set LOG_FILE=C:\Users\Owner\brokerops-ai\scripts\logs\mdl_vendor_loop.log

REM Ensure logs dir exists (idempotent).
if not exist "C:\Users\Owner\brokerops-ai\scripts\logs" mkdir "C:\Users\Owner\brokerops-ai\scripts\logs"

REM Timestamp (YYYY-MM-DD HH:MM:SS) via PowerShell for portability.
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-ddTHH:mm:ss"') do set TS=%%i

echo. >> "%LOG_FILE%"
echo [%TS%] ===== MDL vendor loop tick ===== >> "%LOG_FILE%"

echo [%TS%] dispatcher --once >> "%LOG_FILE%"
"%PYTHON_EXE%" scripts\dispatch_mdl_vendor_outreach.py --once >> "%LOG_FILE%" 2>&1
set DISPATCH_RC=%ERRORLEVEL%
echo [%TS%] dispatcher exit=%DISPATCH_RC% >> "%LOG_FILE%"

echo [%TS%] reply sweep --once >> "%LOG_FILE%"
"%PYTHON_EXE%" scripts\process_mdl_vendor_replies.py --once >> "%LOG_FILE%" 2>&1
set REPLY_RC=%ERRORLEVEL%
echo [%TS%] reply sweep exit=%REPLY_RC% >> "%LOG_FILE%"

echo [%TS%] ===== tick complete (dispatch=%DISPATCH_RC% reply=%REPLY_RC%) ===== >> "%LOG_FILE%"

exit /b %DISPATCH_RC%
