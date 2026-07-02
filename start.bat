@echo off
cd /d "%~dp0backend"
echo ================================
echo   ?A?? - ??
echo ================================
echo.
echo [1] ??????...
python seed_watchlist.py
echo.
echo [2] ???????????????...
python server.py sync
echo.
echo [3] ????...
start http://localhost:8080
python server.py
pause
