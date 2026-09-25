@echo off
:: ============================================================
:: CryptoETL Pipeline Runner — used by Windows Task Scheduler
:: ============================================================
echo [%date% %time%] Starting Crypto ETL Pipeline...

:: Change to project root (edit this path if you move the project)
cd /d D:\4th_year\project\crypto-pipeline

:: Activate virtual environment
call venv\Scripts\activate.bat

:: Run the pipeline — exits with code 0 (success) or 1 (failure)
python run_etl.py

:: Capture and log exit code
set EXIT_CODE=%ERRORLEVEL%
echo [%date% %time%] Pipeline finished with exit code: %EXIT_CODE%

:: Exit with same code so Task Scheduler detects failures
exit /b %EXIT_CODE%