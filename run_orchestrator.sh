#!/bin/bash
# One-click launcher for GymTendency Orchestrator

# Navigate to the project directory
cd "/home/alex-shanov/OneDrive/AnalyticsProjects/GymTendency" || exit

# Activate virtual environment
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d ".venv" ]; then
    source .venv/bin/activate
else
    echo "Error: Virtual environment (venv or .venv) not found."
    read -p "Press enter to exit..."
    exit 1
fi

# Run the orchestrator
echo "Starting GymTendency Orchestrator..."
python3 orchestrator.py

# Keep terminal open
echo ""
echo "Process finished."
read -p "Press enter to close this terminal..."
