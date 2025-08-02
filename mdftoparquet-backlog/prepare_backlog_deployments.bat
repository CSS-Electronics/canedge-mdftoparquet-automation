@echo off
setlocal enabledelayedexpansion

echo Starting backlog processing deployment preparation...
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

:: Read versions from the config file
set "VERSION_FILE=backlog-versions.cfg"
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
:: Create versioned Amazon Backlog Processing entry script
:: ==========================================
echo Creating versioned Amazon Backlog Processing entry script...

:: Create versioned entry script in output directory
set "AMAZON_ENTRY_SCRIPT=process_backlog_amazon_entry-v%AMAZON_VERSION%.py"
copy process_backlog_amazon_entry.py "%OUTPUT_DIR%\%AMAZON_ENTRY_SCRIPT%"
echo Amazon Backlog Processing entry script created: %OUTPUT_DIR%\%AMAZON_ENTRY_SCRIPT%
echo.

:: ==========================================
:: Build Google Backlog Processing zip
:: ==========================================
echo Building Google Backlog Processing zip file...
set "GOOGLE_BUILD_DIR=%TEMP_DIR%\google-backlog"
mkdir "%GOOGLE_BUILD_DIR%"
mkdir "%GOOGLE_BUILD_DIR%\modules"

:: Copy files for Google Backlog Processing
:: Rename process_backlog_google.py to main.py for Google Cloud Function requirements
copy process_backlog_google.py "%GOOGLE_BUILD_DIR%\main.py"
copy "%REPO_ROOT%\mdf2parquet_decode" "%GOOGLE_BUILD_DIR%\"

:: Copy Google function root content if it exists
if exist "%REPO_ROOT%\mdftoparquet\google-function-root" (
    for /f %%f in ('dir /b "%REPO_ROOT%\mdftoparquet\google-function-root"') do (
        if "%%f" NEQ ".gitignore" (
            if exist "%REPO_ROOT%\mdftoparquet\google-function-root\%%f" (
                if exist "%REPO_ROOT%\mdftoparquet\google-function-root\%%f\" (
                    xcopy /E /I /Y "%REPO_ROOT%\mdftoparquet\google-function-root\%%f" "%GOOGLE_BUILD_DIR%\%%f"
                ) else (
                    copy "%REPO_ROOT%\mdftoparquet\google-function-root\%%f" "%GOOGLE_BUILD_DIR%\"
                )
            )
        )
    )
)

:: Copy modules excluding __pycache__ folders
for /D %%d in ("%REPO_ROOT%\modules\*") do (
    set "folder=%%~nxd"
    if /I not "!folder!"=="__pycache__" (
        xcopy /E /I /Y "%REPO_ROOT%\modules\!folder!" "%GOOGLE_BUILD_DIR%\modules\!folder!"
    )
)
xcopy /Y "%REPO_ROOT%\modules\*.py" "%GOOGLE_BUILD_DIR%\modules\"

:: Create the zip file
set "GOOGLE_ZIP_NAME=backlog-processor-google-v%GOOGLE_VERSION%.zip"
cd "%GOOGLE_BUILD_DIR%"
"%SEVENZIP_PATH%" a -tzip -mx=5 "..\..\%OUTPUT_DIR%\%GOOGLE_ZIP_NAME%" *
cd ..\..\
echo Google Backlog Processing zip created: %OUTPUT_DIR%\%GOOGLE_ZIP_NAME%
echo.

:: ==========================================
:: Build Azure Backlog Processing container directory
:: ==========================================
echo Building Azure Backlog Processing container directory...
set "AZURE_CONTAINER_DIR=%OUTPUT_DIR%\backlog-processor-azure-container"

:: Create the container directory structure
if exist "%AZURE_CONTAINER_DIR%" rd /s /q "%AZURE_CONTAINER_DIR%"
mkdir "%AZURE_CONTAINER_DIR%"
mkdir "%AZURE_CONTAINER_DIR%\modules"

:: Copy azure-container-root contents to the container directory
if exist "azure-container-root" (
    xcopy /E /I /Y "azure-container-root\*" "%AZURE_CONTAINER_DIR%\"
)

:: Copy files for Azure Backlog Processing
copy process_backlog_azure.py "%AZURE_CONTAINER_DIR%\"
copy "%REPO_ROOT%\mdf2parquet_decode" "%AZURE_CONTAINER_DIR%\"

:: Copy modules excluding __pycache__ folders
for /D %%d in ("%REPO_ROOT%\modules\*") do (
    set "folder=%%~nxd"
    if /I not "!folder!"=="__pycache__" (
        xcopy /E /I /Y "%REPO_ROOT%\modules\!folder!" "%AZURE_CONTAINER_DIR%\modules\!folder!"
    )
)
xcopy /Y "%REPO_ROOT%\modules\*.py" "%AZURE_CONTAINER_DIR%\modules\"

echo Azure Backlog Processing container directory created: %AZURE_CONTAINER_DIR%
echo.

:: ==========================================
:: Build Local Backlog Processing zip
:: ==========================================
echo Building Local Backlog Processing zip file...
set "LOCAL_BUILD_DIR=%TEMP_DIR%\local-backlog"
mkdir "%LOCAL_BUILD_DIR%"
mkdir "%LOCAL_BUILD_DIR%\modules"

:: Copy files for Local Backlog Processing
copy process_backlog_local.py "%LOCAL_BUILD_DIR%\"
copy "%REPO_ROOT%\mdf2parquet_decode" "%LOCAL_BUILD_DIR%\"
:: Copy Windows decoder executable if it exists
if exist "%REPO_ROOT%\local-testing\mdf4decoder.exe" (
    copy "%REPO_ROOT%\local-testing\mdf4decoder.exe" "%LOCAL_BUILD_DIR%\"
)

:: Copy modules excluding __pycache__ folders
for /D %%d in ("%REPO_ROOT%\modules\*") do (
    set "folder=%%~nxd"
    if /I not "!folder!"=="__pycache__" (
        xcopy /E /I /Y "%REPO_ROOT%\modules\!folder!" "%LOCAL_BUILD_DIR%\modules\!folder!"
    )
)
xcopy /Y "%REPO_ROOT%\modules\*.py" "%LOCAL_BUILD_DIR%\modules\"

:: Create the zip file
set "LOCAL_ZIP_NAME=backlog-processor-local-v%LOCAL_VERSION%.zip"
cd "%LOCAL_BUILD_DIR%"
"%SEVENZIP_PATH%" a -tzip -mx=5 "..\..\%OUTPUT_DIR%\%LOCAL_ZIP_NAME%" *
cd ..\..\
echo Local Backlog Processing zip created: %OUTPUT_DIR%\%LOCAL_ZIP_NAME%
echo.

:: Clean up temporary directory
echo Cleaning up temporary files...
rd /s /q "%TEMP_DIR%"

echo All backlog processing zip files created successfully in the "%OUTPUT_DIR%" directory.
echo.
