#!/bin/bash

set -o pipefail

EMAIL=$1 && shift

OUTPUT=$(mktemp -u --tmpdir=/tmp)

COMMAND="$@"
sh -c "$COMMAND" |& tee "$OUTPUT"
EXIT_CODE=$?

if [ "$EXIT_CODE" != "0" ]; then
    if [ -n "$TITLE" ]; then
        TITLE="Regular process failed: $TITLE"
    else
        TITLE="Regular process failed."
    fi
    echo -e "'$COMMAND' failed with code ${EXIT_CODE}.\n\n$(cat $OUTPUT)\n\nignat@yandex-team.ru" | /opt/cron/tools/hide_tokens.py | mail -s "$TITLE" "$EMAIL"
fi

rm "$OUTPUT"
