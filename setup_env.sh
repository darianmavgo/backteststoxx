#!/bin/bash

# This script sets up the Python environment for the Gmail API project.

# Exit immediately if a command exits with a non-zero status.
set -e

PYTHON_VENV="venv"

echo "--- Setting up Python virtual environment ---"

# Check if python3 is available
if ! command -v python3 &> /dev/null
then
    echo "ERROR: python3 could not be found. Please install Python 3."
    exit 1
fi

# Create a virtual environment
if [ ! -d "$PYTHON_VENV" ]; then
    echo "Creating virtual environment in '$PYTHON_VENV'..."
    python3 -m venv $PYTHON_VENV
else
    echo "Virtual environment '$PYTHON_VENV' already exists."
fi

# Activate the virtual environment and install packages
source $PYTHON_VENV/bin/activate
echo "--- Installing required Python packages ---"
pip install --upgrade pip
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib

echo -e "\n✅ Environment setup complete!"
echo "To activate the virtual environment in your shell, run:"
echo "source $PYTHON_VENV/bin/activate"