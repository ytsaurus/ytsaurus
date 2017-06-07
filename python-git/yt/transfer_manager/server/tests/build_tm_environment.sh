#!/bin/bash

# Path to file requirements.txt
TM_REQUIREMENTS=$1

TM_VENV_PATH="tmvenv_tmp"

SCRIPT_ABSOLUTE_FILENAME=$(readlink -e "$0")
SCRIPT_DIRECTORY=$(dirname "$SCRIPT_ABSOLUTE_FILENAME")

DISTRIB_CODENAME=$(lsb_release -a -s | gawk NR==4)
CACHE_PATH=$(cat "$SCRIPT_DIRECTORY/cache_path")
YT_PATH="$CACHE_PATH/$DISTRIB_CODENAME-tmvenv.tar"
export YT_PROXY=locke

bash "$SCRIPT_DIRECTORY/prepare_tm_environment.sh" "$TM_REQUIREMENTS" "$TM_VENV_PATH"

cd "$TM_VENV_PATH"
tar -cf tmvenv.tar *

cat tmvenv.tar | yt write-file "$YT_PATH"
cd ..
rm -rf "$TM_VENV_PATH"
