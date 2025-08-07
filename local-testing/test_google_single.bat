@echo off
python %~dp0run_test.py --cloud Google --object-path 2F6913DB/00000086/00000003-62977DFB.MF4 --input-bucket canedge-playground-bucket
echo.
echo Press any key to exit...
pause > nul