@echo off
setlocal enabledelayedexpansion

echo ================================================
echo    CANEDGE MDF TO PARQUET DEPLOYMENT CREATOR    
echo ================================================
echo Starting comprehensive deployment preparation...
echo.

set "ERROR_OCCURRED=0"
set "ROOT_DIR=%~dp0"
set "RELEASE_DIR=%ROOT_DIR%release"

:: Create release directory if it doesn't exist
if not exist "%RELEASE_DIR%" mkdir "%RELEASE_DIR%"
echo Release files will be created in: %RELEASE_DIR%
echo.

:: =================================================
:: Step 1: MDF to Parquet deployment
:: =================================================
echo Step 1/3: Creating MDF to Parquet deployments
echo ------------------------------------------------
cd "%ROOT_DIR%mdftoparquet"
call prepare_mdftoparquet_zip_files.bat
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to create MDF to Parquet deployments
    echo Stopping deployment process due to errors.
    set "ERROR_OCCURRED=1"
    exit /b 1
) else (
    echo [SUCCESS] MDF to Parquet deployments created successfully
)
echo.

:: =================================================
:: Step 2: Backlog processing deployment
:: =================================================
echo Step 2/3: Creating Backlog Processing deployments
echo ------------------------------------------------
cd "%ROOT_DIR%mdftoparquet-backlog"
call prepare_backlog_deployments.bat
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to create Backlog Processing deployments
    echo Stopping deployment process due to errors.
    set "ERROR_OCCURRED=1"
    exit /b 1
) else (
    echo [SUCCESS] Backlog Processing deployments created successfully
)
echo.

:: =================================================
:: Step 3: Aggregation deployment
:: =================================================
echo Step 3/3: Creating Aggregation deployments
echo ------------------------------------------------
cd "%ROOT_DIR%aggregation"
call prepare_aggregation_deployments.bat
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to create Aggregation deployments
    echo Stopping deployment process due to errors.
    set "ERROR_OCCURRED=1"
    exit /b 1
) else (
    echo [SUCCESS] Aggregation deployments created successfully
)
echo.

:: =================================================
:: Summary
:: =================================================
cd "%ROOT_DIR%"
echo ================================================
echo                DEPLOYMENT SUMMARY               
echo ================================================

if %ERROR_OCCURRED% EQU 0 (
    echo All deployments were created successfully!
    echo Release files are available in: %RELEASE_DIR%
) else (
    echo WARNING: One or more deployments had errors.
    echo Please check the output above for details.
)
echo.

:: List all created files in the release directory
echo Files in release directory:
dir /b "%RELEASE_DIR%"

echo.
echo Press any key to exit...
pause > nul
