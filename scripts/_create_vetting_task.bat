@echo off
REM One-shot helper to register the BrokerOps-Vetting-Daily-Sweep task.
REM Run once from cmd or by double-clicking. Safe to re-run (uses /f).

schtasks /create ^
  /tn "BrokerOps-Vetting-Daily-Sweep" ^
  /tr "C:\Users\Owner\brokerops-ai\scripts\run_vetting_sweep.bat" ^
  /sc DAILY ^
  /st 04:00 ^
  /it ^
  /f

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Task created. Verify with:
    echo   schtasks /query /tn "BrokerOps-Vetting-Daily-Sweep" /fo LIST /v
)

exit /b %ERRORLEVEL%
