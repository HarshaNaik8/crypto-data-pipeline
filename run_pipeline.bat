@echo off
echo Running ETL pipeline...
cd /d D:\4th_year\project\crypto-pipeline
call venv\Scripts\activate
python run_etl.py
pause