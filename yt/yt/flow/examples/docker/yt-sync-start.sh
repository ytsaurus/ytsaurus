#!/bin/sh
set -e

if [ -z "$TEST_CLUSTER" ]; then
    echo "YT_CLUSTER must be set" >&2
    exit 1
fi

FLOW_USER=${YT_FLOW_USER:-${USER:-$(whoami)}}
export TEST_YT_PATH=${TEST_YT_PATH:-//tmp/$FLOW_USER/flow/noop}

exec /usr/bin/yt_sync --stage test --scenario ensure --parallel-factor 0 --commit
