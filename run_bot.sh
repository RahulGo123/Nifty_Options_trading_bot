#!/bin/bash
echo "Waking up the Nifty 50 Execution Engine..." 

# 1. Navigate to the project directory (The Linux 'cd')
cd /home/goyallavanya431/Nifty_Options_trading_bot

# 2. Activate the virtual environment 
source venv/bin/activate

# 3. Refresh Dhan API token
echo "Refreshing Dhan API token..."
python auth/refresh_token.py

# Check if the refresh succeeded (Linux equivalent of %ERRORLEVEL%)
if [ $? -ne 0 ]; then
    echo "ERROR: Token refresh failed. Aborting bot launch."
    exit 1
fi

echo "Token refreshed successfully."
echo "Waiting 3 seconds..." 
sleep 3

# 4. Execute the orchestrator in Unbuffered mode
# We add -u so you can see the logs live in journalctl
python -u main.py
