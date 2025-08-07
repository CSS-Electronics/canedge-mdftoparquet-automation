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

:: Check for required files
echo Checking required files for Google Aggregation...

:: Check for process_aggregation_google.py
if not exist process_aggregation_google.py (
    echo ERROR: Required file process_aggregation_google.py not found!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Copy files for Google Aggregation after all checks have passed
echo Copying Google Aggregation files...
:: Rename process_aggregation_google.py to main.py for Google Cloud Function requirements
copy process_aggregation_google.py "%GOOGLE_BUILD_DIR%\main.py"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to copy process_aggregation_google.py!
    set ERROR_OCCURRED=1
    exit /b 1
)

:: Copy requirements.txt if it exists
if exist "google-function-root\requirements.txt" (
    copy "google-function-root\requirements.txt" "%GOOGLE_BUILD_DIR%\"
    if %ERRORLEVEL% NEQ 0 (
        echo ERROR: Failed to copy requirements.txt!
        set ERROR_OCCURRED=1
        exit /b 1
    )
)

:: Copy modules excluding __pycache__ folders
echo Copying modules to Google Aggregation build directory (excluding __pycache__)...

:: Copy individual module files
for %%f in ("%REPO_ROOT%\modules\*.py") do (
    copy "%%f" "%GOOGLE_BUILD_DIR%\modules\"
    if %ERRORLEVEL% NEQ 0 (
        echo ERROR: Failed to copy module file %%~nxf!
        set ERROR_OCCURRED=1
        exit /b 1
    )
)

:: Copy module subdirectories excluding __pycache__
for /D %%d in ("%REPO_ROOT%\modules\*") do (
    set "folder=%%~nxd"
    if /I not "!folder!"=="__pycache__" (
        if exist "%%d" (
            mkdir "%GOOGLE_BUILD_DIR%\modules\!folder!" 2>nul
            xcopy /E /I /Y "%%d" "%GOOGLE_BUILD_DIR%\modules\!folder!"
            if %ERRORLEVEL% NEQ 0 (
                echo ERROR: Failed to copy module directory !folder!!
                set ERROR_OCCURRED=1
                exit /b 1
            )
        )
    ) else (
        echo Skipping __pycache__ folder
    )
)

:: Create zip archive with explicit full paths to avoid any path issues
set "GOOGLE_ZIP=aggregation-processor-google-v%GOOGLE_VERSION%.zip"

:: Get absolute paths
pushd "%TEMP_DIR%"
set "TEMP_DIR_ABS=%CD%"
popd

pushd "%REPO_ROOT%\release"
set "RELEASE_DIR_ABS=%CD%"
popd

:: Delete the file if it already exists
if exist "%RELEASE_DIR_ABS%\%GOOGLE_ZIP%" (
    echo Removing existing Google Aggregation zip file...
    del /F "%RELEASE_DIR_ABS%\%GOOGLE_ZIP%"
    if %ERRORLEVEL% NEQ 0 (
        echo WARNING: Failed to delete existing zip file, continuing...
    )
)

:: Move to temp directory and create zip file with absolute paths
cd "%TEMP_DIR_ABS%"

echo Creating Google Aggregation zip file: %GOOGLE_ZIP%
echo Current directory: %CD%
echo Target zip file: %RELEASE_DIR_ABS%\%GOOGLE_ZIP%

"%SEVENZIP_PATH%" a -tzip "%RELEASE_DIR_ABS%\%GOOGLE_ZIP%" ".\google-aggregation\*" -r
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Failed to create Google Aggregation zip file!
    set ERROR_OCCURRED=1
    cd "%~dp0"
    exit /b 1
)

:: Verify the file was created with dir command to show output
dir "%RELEASE_DIR_ABS%\%GOOGLE_ZIP%" 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Google Aggregation zip file was not created or cannot be accessed!
    set ERROR_OCCURRED=1
    cd "%~dp0"
    exit /b 1
)

:: Return to original directory
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
