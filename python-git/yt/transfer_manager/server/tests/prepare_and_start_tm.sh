#!/bin/bash

TM_REQUIREMENTS=$1
TM_SERVER_BIN=$2
TM_CONFIG=$3
TM_VENV_PATH=$4

_term() {
    kill -TERM "$PID"
    rm -rf tmenv
    exit
}
trap _term SIGTERM

if [ ! -d $TM_VENV_PATH ]; then
    virtualenv "$TM_VENV_PATH"
    source "$TM_VENV_PATH/bin/activate"
    cat "$TM_REQUIREMENTS" | grep yandex-yt -v | xargs -n 1 pip install
else
    source "$TM_VENV_PATH/bin/activate"
fi

$TM_SERVER_BIN --config "$TM_CONFIG" & PID=$!
wait $PID
deactivate
