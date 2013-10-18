#!/bin/sh

EMAIL=$1 && shift

COMMAND="$@"
sh -c "$COMMAND"
EXIT_CODE=$?

if [ "$EXIT_CODE" != "0" ]; then
    echo -e "'$COMMAND' failed with code ${EXIT_CODE}.\n\nignat@yandex-team.ru" | /opt/cron/tools/hide_tokens.py | mail -s "Regular process failed" $EMAIL
fi
