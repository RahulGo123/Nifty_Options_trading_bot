@echo off
echo Waking up the Nifty 50 Execution Engine...

:: Switch to the correct drive and directory
J:
cd J:\trading_bot

:: Activate the virtual environment (Assuming it is named 'venv')
:: If yours is named 'env', change 'venv' to 'env' below.
call venv\Scripts\activate

:: Refresh Dhan API token before starting the bot
echo Refreshing Dhan API token...
python auth\refresh_token.py
if %ERRORLEVEL% neq 0 (
    echo ERROR: Token refresh failed. Aborting bot launch.
    pause
    exit /b 1
)
echo Token refreshed successfully. Waiting 3 seconds...
timeout /t 3 /nobreak >nul

:: Execute the orchestrator
python main.py

:: Keep the window open if the bot crashes
pause