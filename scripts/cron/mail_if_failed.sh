#!/bin/bash

set -o pipefail

EMAIL=$1 && shift

OUTPUT=$(mktemp -u --tmpdir=/tmp)

COMMAND="$@"
sh -c "$COMMAND" |& tee "$OUTPUT"
EXIT_CODE=$?

if [ "$EXIT_CODE" != "0" ]; then
    echo -e "'$COMMAND' failed with code ${EXIT_CODE}.\n\n$(tail -n 20 $OUTPUT)\n\nignat@yandex-team.ru" | /opt/cron/tools/hide_tokens.py | mail -s "Regular process failed." $EMAIL
fi

rm "$OUTPUT"
