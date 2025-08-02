@echo off
setlocal enabledelayedexpansion

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

:: Read versions from the config file
set "VERSION_FILE=mdftoparquet-versions.cfg"
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

:: Copy files for Amazon Lambda
copy lambda_function.py "%AMAZON_BUILD_DIR%\"
:: Add the process_backlog_amazon.py script from mdftoparquet-backlog folder
copy "%REPO_ROOT%\mdftoparquet-backlog\process_backlog_amazon.py" "%AMAZON_BUILD_DIR%\"
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

:: Copy files for Google Cloud Function
copy main.py "%GOOGLE_BUILD_DIR%\"
:: Use the Linux version of the decoder for cloud environments
copy "%REPO_ROOT%\mdf2parquet_decode" "%GOOGLE_BUILD_DIR%\"
if exist google-function-root\requirements.txt copy google-function-root\requirements.txt "%GOOGLE_BUILD_DIR%\"
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

:: Copy files for Azure Function
copy function_app.py "%AZURE_BUILD_DIR%\"
:: Use the Linux version of the decoder for cloud environments
copy "%REPO_ROOT%\mdf2parquet_decode" "%AZURE_BUILD_DIR%\"

:: Copy Azure function root files
if exist azure-function-root (xcopy /E /I /Y "azure-function-root\*" "%AZURE_BUILD_DIR%\")
:: Make sure we have these files from somewhere
copy host.json "%AZURE_BUILD_DIR%\" 2>NUL
copy local.settings.json "%AZURE_BUILD_DIR%\" 2>NUL
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
cd "%AZURE_BUILD_DIR%"
"%SEVENZIP_PATH%" a -tzip -mx=5 "..\..\%OUTPUT_DIR%\%AZURE_ZIP_NAME%" *
cd ..\..\
echo Azure Function zip created: %OUTPUT_DIR%\%AZURE_ZIP_NAME%
echo.

:: Clean up temporary directory
echo Cleaning up temporary files...
rd /s /q "%TEMP_DIR%"

echo All zip files created successfully in the "%OUTPUT_DIR%" directory.
echo.
