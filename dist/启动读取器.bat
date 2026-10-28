@echo off
rem Zebra FXR90 MQTT tag reader launcher (ASCII only)
cd /d "%~dp0"
if exist "FXR90Reader.exe" (
  start "" "FXR90Reader.exe" %*
  exit /b 0
)
if exist ".venv\Scripts\pythonw.exe" (
  start "" ".venv\Scripts\pythonw.exe" "fxr90_reader.py" %*
  exit /b 0
)
echo FXR90Reader.exe not found. Build it first (see README.md).
pause
