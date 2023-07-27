#!/usr/bin/env bash
set -eu

##########################

# As in the value of "event_log_file" in config.yson.
export SCHEDULER_EVENT_LOG_FILE="scheduler_event_log.txt"

export YT_USER="$(whoami)"

export LOCAL_JUPYTER_PORT=8888
# Just make sure that this tcp port is not in use on your build host.
export BUILD_HOST_JUPYTER_PORT=28883

##########################

export PATH="$(pwd):$PATH"
export PATH="$(realpath ../scripts):$PATH"
export PYTHONPATH="$(realpath ../scripts):$PATH"

##########################
