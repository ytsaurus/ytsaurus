#!/bin/sh

EMAIL=$1 && shift

COMMAND="$@"
sh -c "$COMMAND"
EXIT_CODE=$?

if [ "$EXIT_CODE" != "0" ]; then
    echo -e "'$COMMAND' failed with code ${EXIT_CODE}.\n\nignat@yandex-team.ru" | mail -s "Regular process failed" $EMAIL
fi
