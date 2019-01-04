#!/bin/bash -eux

yt2 remove //tmp/fennel_test --force --proxy hahn
echo -e "key=1\nkey=2\nkey=3\nkey=4\nkey=5" | yt2 write //tmp/fennel_test --format dsv --proxy hahn

random_suffix="$(cat /dev/urandom | tr -cd 'a-f0-9' | head -c 4)"

bin/fennel.py push-to-logbroker \
    --table-path //tmp/fennel_test \
    --yt-proxy hahn \
    --yt-config "{token=$(cat ~/.yt/token)}" \
    --logbroker-url logbroker.yandex.net \
    --logbroker-port 8999 \
    --logbroker-source-id "test_fennel_$random_suffix" \
    --logbroker-log-type "test_fennel" \
    --session-count 2 \
    --range-row-count 2

processed_row_count="$(yt2 get //tmp/fennel_test/@processed_row_count --proxy hahn)"
if [ "5" != "$processed_row_count" ]; then
    die "/@processed_row_count mismatch (received: ${processed_row_count}, expected: 5)"
fi

# TODO(ignat): read from logbroker and check data
