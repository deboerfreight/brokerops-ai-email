@echo off
REM =====================================================================
REM BrokerOps - Carrier Vetting Daily Sweep Runner
REM
REM Invoked once per day at 04:00 by the Windows Scheduled Task
REM   BrokerOps-Vetting-Daily-Sweep
REM
REM Re-vets every row in the Carrier Database and the Carrier Quarantine
REM tabs against the canonical hard-reject rules. Releases any rows that
REM now pass back to the main tab. Does NOT re-fetch FMCSA by default
REM (use --refetch flag for that — slow, only after rule changes).
REM
REM Logs to: scripts\logs\vetting_sweep.log (append mode)
REM
REM To pause:  schtasks /change /tn "BrokerOps-Vetting-Daily-Sweep" /disable
REM To resume: schtasks /change /tn "BrokerOps-Vetting-Daily-Sweep" /enable
REM See docs\vetting_pipeline.md for full operational notes.
REM =====================================================================

cd /d C:\Users\Owner\brokerops-ai
set PYTHONPATH=.
set PYTHON_EXE=C:\Python314\python.exe
set LOG_FILE=C:\Users\Owner\brokerops-ai\scripts\logs\vetting_sweep.log

if not exist "C:\Users\Owner\brokerops-ai\scripts\logs" mkdir "C:\Users\Owner\brokerops-ai\scripts\logs"

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-ddTHH:mm:ss"') do set TS=%%i

echo. >> "%LOG_FILE%"
echo [%TS%] ===== vetting sweep tick ===== >> "%LOG_FILE%"

"%PYTHON_EXE%" scripts\run_vetting_sweep.py --all >> "%LOG_FILE%" 2>&1
set RC=%ERRORLEVEL%

echo [%TS%] ===== tick complete (rc=%RC%) ===== >> "%LOG_FILE%"

exit /b %RC%
