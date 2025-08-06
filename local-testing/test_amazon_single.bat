@echo off
REM Single object test for Amazon S3
python %~dp0run_test.py --cloud Amazon --object-path 2F6913DB/00000086/00000003-62977DFB.MF4 --input-bucket s3-test-lambda-v210
echo.
echo Press any key to exit...
pause > nul
