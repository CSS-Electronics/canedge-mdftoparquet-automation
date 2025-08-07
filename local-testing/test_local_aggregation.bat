@echo off
python %~dp0run_test.py --cloud Local --input-bucket local-input-bucket --aggregate
echo.
echo Press any key to exit...
pause > nul
