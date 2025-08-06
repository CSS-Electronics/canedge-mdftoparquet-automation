@echo off
python %~dp0run_test.py --cloud Azure --input-bucket canedge-playground-bucket --aggregate
echo.
echo Press any key to exit...
pause > nul
