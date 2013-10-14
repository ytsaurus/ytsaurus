#!/bin/sh

EMAIL=$1 && shift

sh -c "$@"
EXIT_CODE=$?

if [ "$EXIT_CODE" != "0" ]; then
    echo -e "'$@' failed with code ${EXIT_CODE}.\n\nIgnat\nignat@yandex-team.ru" | mail -s "Regular process failed" $EMAIL
fi
