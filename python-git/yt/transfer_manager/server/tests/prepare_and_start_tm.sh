#!/bin/bash

TM_REQUIREMENTS=$1
TM_SERVER_BIN=$2
TM_CONFIG=$3

_term() {
    kill -TERM "$PID"
    rm -rf tmenv
    exit
}
trap _term SIGTERM

virtualenv tmenv
source tmenv/bin/activate
cat $TM_REQUIREMENTS | grep yandex-yt -v | xargs -n 1 pip install

$TM_SERVER_BIN --config $TM_CONFIG & PID=$!
wait $PID
deactivate
