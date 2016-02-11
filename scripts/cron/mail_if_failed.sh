#!/bin/bash

set -o pipefail

EMAIL=$1 && shift

OUTPUT=$(mktemp -u --tmpdir=/tmp)

COMMAND="$@"
sh -c "$COMMAND" |& tee "$OUTPUT"
EXIT_CODE=$?

if [ "$EXIT_CODE" != "0" ]; then
    TITLE="Regular process failed"
    if [ -n "$YT_PROXY" ]; then
        TITLE="$TITLE on $YT_PROXY"
    fi
    echo -e "'$COMMAND' failed with code ${EXIT_CODE}.\n\n$(tail -n 20 $OUTPUT)\n\nignat@yandex-team.ru" | /opt/cron/tools/hide_tokens.py | mail -s "$TITLE" "$EMAIL"
fi

rm "$OUTPUT"
