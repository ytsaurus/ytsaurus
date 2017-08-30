#!/bin/bash

# Path to file requirements.txt
TM_REQUIREMENTS=$1
TM_VENV_PATH=$2

echo >&2 "Preparing virtual environment..."
virtualenv "$TM_VENV_PATH" -v
source "$TM_VENV_PATH/bin/activate"
echo >&2 "Installing packages..."
cat "$TM_REQUIREMENTS" | grep -v yandex-yt | xargs -n 1 pip install -v
deactivate
