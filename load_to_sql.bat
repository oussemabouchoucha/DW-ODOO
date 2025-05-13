@echo off
echo Loading data to SQL Server...
python etl_process/load_to_sql.py
echo Process completed.
pause
