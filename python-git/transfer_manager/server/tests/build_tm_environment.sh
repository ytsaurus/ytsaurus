#!/bin/bash

# Path to file requirements.txt
TM_REQUIREMENTS=$1

TM_VENV_PATH="tmvenv_tmp"
YT_PATH="//home/files/test_data/transfer_manager/tmvenv.tar"
export YT_PROXY=locke

virtualenv "$TM_VENV_PATH"
source "$TM_VENV_PATH/bin/activate"
cat "$TM_REQUIREMENTS" | grep yandex-yt -v | xargs -n 1 pip install
deactivate

cd "$TM_VENV_PATH"
tar -cf tmvenv.tar *

cat tmvenv.tar | yt write-file "$YT_PATH"
cd ..
rm -rf "$TM_VENV_PATH"
