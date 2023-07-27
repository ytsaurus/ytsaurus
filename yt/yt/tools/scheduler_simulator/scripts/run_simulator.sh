#!/usr/bin/env bash
set -eu

allow_debug_mode_opt="--allow-debug-mode"

if [[ "2" != "$#" && ( "3" != "$#" || "$3" != "$allow_debug_mode_opt" ) ]]; then
    echo "Usage: run_simulator OPERATIONS_LOG CONFIG [--allow-debug-mode]" >&2
    exit 1
fi

maybe_allow_debug_mode=""
if [[ "3" == "$#" ]]; then
    echo "Debug mode has been explicitly allowed for the simulator" >&2
    maybe_allow_debug_mode="$allow_debug_mode_opt"
fi

echo "Running simulator for $1 with config $2" >&2

cat "$1" | scheduler_simulator "$2" $maybe_allow_debug_mode
