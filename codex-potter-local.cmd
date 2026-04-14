@echo off
setlocal

cd /d "%~dp0"
if errorlevel 1 exit /b 1

echo [CodexPotter] Building local binary...
"C:\Users\GaoqQang\.cargo\bin\cargo.exe" build -p codex-potter-cli
if errorlevel 1 exit /b %ERRORLEVEL%

"target\debug\codex-potter.exe" %*
exit /b %ERRORLEVEL%
