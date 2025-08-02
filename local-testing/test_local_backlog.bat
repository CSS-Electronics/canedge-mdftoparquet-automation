@echo off
:: Test script for local backlog processing
echo Testing Local Backlog Processing

:: Change to script directory
cd %~dp0

:: Set up Python path
set PYTHONPATH=%~dp0..;%PYTHONPATH%

:: Run the test script with Local cloud option
python run_test.py --cloud Local --input-bucket "%~dp0local-input-bucket" --backlog

echo Test complete
pause
