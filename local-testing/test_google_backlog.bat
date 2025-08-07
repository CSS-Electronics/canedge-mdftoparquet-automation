@echo off
python %~dp0run_test.py --cloud Google --input-bucket canedge-playground-bucket --backlog
echo.
echo Press any key to exit...
pause > nul