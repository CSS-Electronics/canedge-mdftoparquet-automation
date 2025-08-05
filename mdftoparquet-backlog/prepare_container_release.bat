@echo off
echo Preparing Multi-Cloud Container Release Files
echo ---------------------------------------------

:: Create release directory if it doesn't exist
if not exist "..\release\backlog-processor-container" mkdir "..\release\backlog-processor-container"

:: Copy requirements files
echo Copying requirements files...
copy "container-root\requirements_azure.txt" "..\release\backlog-processor-container\"
copy "container-root\requirements_amazon.txt" "..\release\backlog-processor-container\"

:: Copy decoder executable
echo Copying decoder executable...
copy "..\mdf2parquet_decode" "..\release\backlog-processor-container\"

:: Copy script files
echo Copying main script...
copy "process_backlog_container.py" "..\release\backlog-processor-container\"

:: Copy modules directory recursively
echo Copying modules directory...
robocopy "..\modules" "..\release\backlog-processor-container\modules" /E /NFL /NDL /NJH /NJS /nc /ns /np

:: Done
echo.
echo Release files prepared successfully in ..\release\backlog-processor-container\
