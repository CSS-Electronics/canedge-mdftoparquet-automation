@echo off
setlocal enabledelayedexpansion

:: Enable error handling
set "ERROR_OCCURRED=0"

echo Starting aggregation deployment preparation...
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
set "LOCAL_VERSION=1.0.0"

:: Parse the backlog versions config file
for /f "tokens=1,2 delims==" %%a in (%VERSION_FILE%) do (
    if "%%a"=="amazon" set "AMAZON_VERSION=%%b"
    if "%%a"=="google" set "GOOGLE_VERSION=%%b"
    if "%%a"=="azure" set "AZURE_VERSION=%%b"
    if "%%a"=="local" set "LOCAL_VERSION=%%b"
)

echo Detected versions:
echo Amazon: %AMAZON_VERSION%
echo Google: %GOOGLE_VERSION%
echo Azure:  %AZURE_VERSION%
echo Local:  %LOCAL_VERSION%
echo.

:: Create a temporary directory for building the zip files
set "TEMP_DIR=temp_build"
if exist "%TEMP_DIR%" rd /s /q "%TEMP_DIR%"
mkdir "%TEMP_DIR%"

:: ==========================================
:: Create versioned Amazon Aggregation entry script
:: ==========================================
echo Creating versioned Amazon Aggregation entry script...

:: Create versioned entry script in output directory
set "AMAZON_ENTRY_SCRIPT=process_aggregation_amazon_entry-v%AMAZON_VERSION%.py"
copy process_aggregation_amazon_entry.py "%OUTPUT_DIR%\%AMAZON_ENTRY_SCRIPT%"
echo Amazon Aggregation entry script created: %OUTPUT_DIR%\%AMAZON_ENTRY_SCRIPT%
echo.

:: ==========================================
:: Build Google Aggregation zip
:: ==========================================
echo Building Google Aggregation zip file...
set "GOOGLE_BUILD_DIR=%TEMP_DIR%\google-aggregation"
mkdir "%GOOGLE_BUILD_DIR%"
mkdir "%GOOGLE_BUILD_DIR%\modules"

:: Copy files for Google Aggregation
copy process_aggregation_google.py "%GOOGLE_BUILD_DIR%\"

:: Copy modules
xcopy /E /I /Y "%REPO_ROOT%\modules" "%GOOGLE_BUILD_DIR%\modules"

:: Create zip archive
set "GOOGLE_ZIP=process-aggregation-google-v%GOOGLE_VERSION%.zip"
cd "%TEMP_DIR%"
"%SEVENZIP_PATH%" a -r "%REPO_ROOT%\release\%GOOGLE_ZIP%" "google-aggregation\*"
cd "%~dp0"

echo Google Aggregation zip created: %OUTPUT_DIR%\%GOOGLE_ZIP%
echo.

:: ==========================================
:: Create container deployment
:: ==========================================
echo Creating container deployment files...

:: Create directory for container files
set "CONTAINER_DIR=%OUTPUT_DIR%\aggregation-processor-container"
if exist "%CONTAINER_DIR%" rd /s /q "%CONTAINER_DIR%"
mkdir "%CONTAINER_DIR%"

:: Check required files for container deployment
echo Checking required files for aggregation container...

:: Check for Dockerfile
if not exist "container-root\Dockerfile" (
    echo ERROR: Required file container-root\Dockerfile not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Check for requirements file
if not exist "container-root\requirements.txt" (
    echo ERROR: Required file container-root\requirements.txt not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Check for container script
if not exist "process_aggregation_container.py" (
    echo ERROR: Required file process_aggregation_container.py not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Copy files for container deployment after all checks passed
echo Copying container deployment files...
copy container-root\Dockerfile "%CONTAINER_DIR%\"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to copy Dockerfile!
    set ERROR_OCCURRED=1
    exit /b 1
)

copy container-root\requirements.txt "%CONTAINER_DIR%\"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to copy requirements.txt!
    set ERROR_OCCURRED=1
    exit /b 1
)

copy process_aggregation_container.py "%CONTAINER_DIR%\"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to copy process_aggregation_container.py!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Create modules directory
mkdir "%CONTAINER_DIR%\modules"

:: Copy modules excluding __pycache__ folders
echo Copying modules excluding __pycache__ folders...
for /D %%d in ("%REPO_ROOT%\modules\*") do (
    set "folder=%%~nxd"
    if /I not "!folder!"=="__pycache__" (
        xcopy /E /I /Y "%REPO_ROOT%\modules\!folder!" "%CONTAINER_DIR%\modules\!folder!"
    )
)

:: Copy module root Python files
xcopy /Y "%REPO_ROOT%\modules\*.py" "%CONTAINER_DIR%\modules\"

echo Container deployment files created in: %CONTAINER_DIR%
echo.

:: Clean up temporary directory
echo Cleaning up temporary files...
rd /s /q "%TEMP_DIR%"

echo All aggregation deployment files created successfully in the "%OUTPUT_DIR%" directory.
echo.
