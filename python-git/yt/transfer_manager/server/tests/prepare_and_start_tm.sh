#!/bin/bash

TM_SERVER_BIN=$1
TM_CONFIG=$2
TM_VENV_PATH=$3

_term() {
    kill -TERM "$PID"
    exit
}
trap _term SIGTERM

virtualenv $TM_VENV_PATH
source "$TM_VENV_PATH/bin/activate"

$TM_SERVER_BIN --config "$TM_CONFIG" & PID=$!
wait $PID
deactivate
