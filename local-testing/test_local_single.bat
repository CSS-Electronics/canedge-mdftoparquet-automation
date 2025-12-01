@echo off
REM Single object test for Local
python %~dp0run_test.py --cloud Local --object-path "2F6913DB/00000086/00000003-62977DFB.MF4" --input-bucket "%~dp0local-input-bucket"
echo.
echo Press any key to exit...
pause > nul
