@echo off
setlocal enabledelayedexpansion

:: Enable error handling
set "ERROR_OCCURRED=0"

echo Starting MDF to Parquet zip file preparation...
echo.

:: Check if 7-Zip is installed
set "SEVENZIP_PATH=C:\Program Files\7-Zip\7z.exe"
if not exist "%SEVENZIP_PATH%" (
    echo 7-Zip not found, attempting to install via winget...
    winget install 7zip.7zip --accept-source-agreements --accept-package-agreements
    
    :: Check if installation was successful
    if not exist "%SEVENZIP_PATH%" (
        echo Failed to install 7-Zip. Please install it manually.
        echo You can download it from: https://www.7-zip.org/download.html
        exit /b 1
    ) else (
        echo 7-Zip installed successfully.
    )
) else (
    echo 7-Zip found at: %SEVENZIP_PATH%
)
echo.

:: Set output directory for zip files
set "REPO_ROOT=.."
set "OUTPUT_DIR=%REPO_ROOT%\release"
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"

:: Read versions from the root config file
set "VERSION_FILE=%REPO_ROOT%\versions.cfg"
if not exist "%VERSION_FILE%" (
    echo Error: %VERSION_FILE% not found.
    exit /b 1
)

:: Initialize versions
set "AMAZON_VERSION="
set "GOOGLE_VERSION="
set "AZURE_VERSION="

:: Parse the versions.cfg file
for /f "tokens=1,2 delims==" %%a in (%VERSION_FILE%) do (
    if "%%a"=="amazon" set "AMAZON_VERSION=%%b"
    if "%%a"=="google" set "GOOGLE_VERSION=%%b"
    if "%%a"=="azure" set "AZURE_VERSION=%%b"
)

echo Detected versions:
echo Amazon: %AMAZON_VERSION%
echo Google: %GOOGLE_VERSION%
echo Azure:  %AZURE_VERSION%
echo.

:: Create a temporary directory for building the zip files
set "TEMP_DIR=temp_build"
if exist "%TEMP_DIR%" rd /s /q "%TEMP_DIR%"
mkdir "%TEMP_DIR%"

:: ==========================================
:: Build Amazon Lambda zip
:: ==========================================
echo Building Amazon Lambda zip file...
set "AMAZON_BUILD_DIR=%TEMP_DIR%\amazon"
mkdir "%AMAZON_BUILD_DIR%"
mkdir "%AMAZON_BUILD_DIR%\modules"

:: Check required files for Amazon Lambda
echo Checking required files for Amazon Lambda...

:: Check for lambda_function.py
if not exist lambda_function.py (
    echo ERROR: Required file lambda_function.py not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Check for process_backlog_amazon.py
if not exist "%REPO_ROOT%\mdftoparquet-backlog\process_backlog_amazon.py" (
    echo ERROR: Required file process_backlog_amazon.py not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Check for process_aggregation_amazon.py
if not exist "%REPO_ROOT%\aggregation\process_aggregation_amazon.py" (
    echo ERROR: Required file process_aggregation_amazon.py not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Check for decoder executable
if not exist "%REPO_ROOT%\mdf2parquet_decode" (
    echo ERROR: Required file mdf2parquet_decode not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Copy files for Amazon Lambda after all checks have passed
echo Copying Amazon Lambda files...
copy lambda_function.py "%AMAZON_BUILD_DIR%\"
:: Add the process_backlog_amazon.py script from mdftoparquet-backlog folder
copy "%REPO_ROOT%\mdftoparquet-backlog\process_backlog_amazon.py" "%AMAZON_BUILD_DIR%\"
:: Add the process_aggregation_amazon.py script from aggregation folder
copy "%REPO_ROOT%\aggregation\process_aggregation_amazon.py" "%AMAZON_BUILD_DIR%\"
:: Use the Linux version of the decoder for cloud environments
copy "%REPO_ROOT%\mdf2parquet_decode" "%AMAZON_BUILD_DIR%\"
:: Copy modules excluding __pycache__ folders
for /D %%d in ("%REPO_ROOT%\modules\*") do (
    set "folder=%%~nxd"
    if /I not "!folder!"=="__pycache__" (
        xcopy /E /I /Y "%REPO_ROOT%\modules\!folder!" "%AMAZON_BUILD_DIR%\modules\!folder!"
    )
)
xcopy /Y "%REPO_ROOT%\modules\*.py" "%AMAZON_BUILD_DIR%\modules\"

:: Create the zip file
set "AMAZON_ZIP_NAME=mdf-to-parquet-amazon-function-v%AMAZON_VERSION%.zip"
cd "%AMAZON_BUILD_DIR%"
"%SEVENZIP_PATH%" a -tzip -mx=5 "..\..\%OUTPUT_DIR%\%AMAZON_ZIP_NAME%" *
cd ..\..\
echo Amazon Lambda zip created: %OUTPUT_DIR%\%AMAZON_ZIP_NAME%
echo.

:: ==========================================
:: Build Google Cloud Function zip
:: ==========================================
echo Building Google Cloud Function zip file...
set "GOOGLE_BUILD_DIR=%TEMP_DIR%\google"
mkdir "%GOOGLE_BUILD_DIR%"
mkdir "%GOOGLE_BUILD_DIR%\modules"

:: Check required files for Google Cloud Function
echo Checking required files for Google Cloud Function...

:: Check for main.py
if not exist main.py (
    echo ERROR: Required file main.py not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Check for decoder executable
if not exist "%REPO_ROOT%\mdf2parquet_decode" (
    echo ERROR: Required file mdf2parquet_decode not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Check for Google requirements file
if not exist google-function-root\requirements.txt (
    echo ERROR: Required file google-function-root\requirements.txt not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Copy files for Google Cloud Function after all checks have passed
echo Copying Google Cloud Function files...
copy main.py "%GOOGLE_BUILD_DIR%\"
:: Use the Linux version of the decoder for cloud environments
copy "%REPO_ROOT%\mdf2parquet_decode" "%GOOGLE_BUILD_DIR%\"
copy google-function-root\requirements.txt "%GOOGLE_BUILD_DIR%\"
:: Copy modules excluding __pycache__ folders
for /D %%d in ("%REPO_ROOT%\modules\*") do (
    set "folder=%%~nxd"
    if /I not "!folder!"=="__pycache__" (
        xcopy /E /I /Y "%REPO_ROOT%\modules\!folder!" "%GOOGLE_BUILD_DIR%\modules\!folder!"
    )
)
xcopy /Y "%REPO_ROOT%\modules\*.py" "%GOOGLE_BUILD_DIR%\modules\"

:: Create the zip file
set "GOOGLE_ZIP_NAME=mdf-to-parquet-google-function-v%GOOGLE_VERSION%.zip"
cd "%GOOGLE_BUILD_DIR%"
"%SEVENZIP_PATH%" a -tzip -mx=5 "..\..\%OUTPUT_DIR%\%GOOGLE_ZIP_NAME%" *
cd ..\..\
echo Google Cloud Function zip created: %OUTPUT_DIR%\%GOOGLE_ZIP_NAME%
echo.

:: ==========================================
:: Build Azure Function zip
:: ==========================================
echo Building Azure Function zip file...
set "AZURE_BUILD_DIR=%TEMP_DIR%\azure"
mkdir "%AZURE_BUILD_DIR%"
mkdir "%AZURE_BUILD_DIR%\modules"

:: Return to the mdftoparquet directory to ensure proper paths
cd %~dp0

:: Copy files for Azure Function
echo Checking required files for Azure function...

:: Check for function_app.py
if not exist function_app.py (
    echo ERROR: Required file function_app.py not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Check for decoder executable
if not exist "%REPO_ROOT%\mdf2parquet_decode" (
    echo ERROR: Required file mdf2parquet_decode not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Check for azure function configuration directory
if not exist "%~dp0azure-function-root" (
    echo ERROR: Required directory azure-function-root not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Check for required files in azure-function-root
if not exist "%~dp0azure-function-root\host.json" (
    echo ERROR: Required file azure-function-root\host.json not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Copy files for Azure Function after all checks have passed
echo Copying Azure function files...
copy function_app.py "%AZURE_BUILD_DIR%\"
:: Use the Linux version of the decoder for cloud environments
copy "%REPO_ROOT%\mdf2parquet_decode" "%AZURE_BUILD_DIR%\"

:: Copy Azure function root files
echo Copying Azure function root files from azure-function-root...
xcopy /E /I /Y "%~dp0azure-function-root\*" "%AZURE_BUILD_DIR%\"
:: Copy modules excluding __pycache__ folders
for /D %%d in ("%REPO_ROOT%\modules\*") do (
    set "folder=%%~nxd"
    if /I not "!folder!"=="__pycache__" (
        xcopy /E /I /Y "%REPO_ROOT%\modules\!folder!" "%AZURE_BUILD_DIR%\modules\!folder!"
    )
)
xcopy /Y "%REPO_ROOT%\modules\*.py" "%AZURE_BUILD_DIR%\modules\"

:: Create the zip file
set "AZURE_ZIP_NAME=mdf-to-parquet-azure-function-v%AZURE_VERSION%.zip"
echo Creating Azure Function zip file...
cd "%AZURE_BUILD_DIR%"
"%SEVENZIP_PATH%" a -tzip -mx=5 "..\..\%OUTPUT_DIR%\%AZURE_ZIP_NAME%" *
cd "%~dp0"
echo Azure Function zip created: %OUTPUT_DIR%\%AZURE_ZIP_NAME%
echo.

:: Clean up temporary directory
echo Cleaning up temporary files...
rd /s /q "%TEMP_DIR%"

echo All zip files created successfully in the "%OUTPUT_DIR%" directory.
echo.
