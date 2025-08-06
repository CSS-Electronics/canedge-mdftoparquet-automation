@echo off
python %~dp0run_test.py --cloud Amazon --input-bucket s3-test-lambda-v210 --backlog
echo.
echo Press any key to exit...
pause > nul