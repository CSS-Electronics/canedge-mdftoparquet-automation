@echo off
setlocal
REM End-to-end local test of the optional routing.json feature (two devices -> two output buckets).
REM Runs from this folder so the emulated "buckets" (folders) are created here as siblings of
REM local-input-bucket. Temporarily drops routing.json into the input bucket, then removes it so
REM the other local tests are unaffected.
cd /d "%~dp0"

echo Placing routing.json into local-input-bucket (temporary)...
copy /Y "routing-local-example.json" "local-input-bucket\routing.json" >nul

echo.
echo === Single-file: device 2F6913DB  (expect -^> local-input-bucket-custa-parquet) ===
python run_test.py --cloud Local --input-bucket "local-input-bucket" --object-path "2F6913DB/00000086/00000003-62977DFB.MF4"

echo.
echo === Single-file: device 7512BE4D  (expect -^> local-input-bucket-custb-parquet) ===
python run_test.py --cloud Local --input-bucket "local-input-bucket" --object-path "7512BE4D/00000427/00000001.MF4"

echo.
echo === Backlog: both devices via backlog.json (routing applies through the backlog path too) ===
python run_test.py --cloud Local --input-bucket "local-input-bucket" --backlog

echo.
echo Removing temporary routing.json...
del "local-input-bucket\routing.json" >nul 2>&1

echo.
echo Verify the routed output folders (each device under its own bucket):
echo   local-testing\local-input-bucket-custa-parquet\2F6913DB\...
echo   local-testing\local-input-bucket-custb-parquet\7512BE4D\...
echo (An unmapped device, or data before its from_date, would instead land in the catch-all
echo  local-testing\local-input-bucket-parquet\ )
endlocal
