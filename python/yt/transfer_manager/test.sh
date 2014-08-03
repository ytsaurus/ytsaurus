#!/bin/bash -eu

die() {
    echo $@
    exit 1
}

log() {
    echo "$@" >&2
}

check() {
    local first="$(echo -e "$1")"
    local second="$(echo -e "$2")"
    [ "${first}" = "${second}" ] || die "Test fail $1 does not equal $2"
}

get_task() {
    local id="$1"
    curl -X GET -s -k -L "http://localhost:5010/tasks/$id/" \
         -H "Content-Type: application/json" \
         -H "Authorization: OAuth $YT_TOKEN"
}

run_task() {
    local body="$1"
    log "Running task $body"
    curl -X POST -s -k -L "http://localhost:5010/tasks/" \
         -H "Content-Type: application/json" \
         -H "Authorization: OAuth $YT_TOKEN" \
         -d "$1"
}

abort_task() {
    local id="$1"
    log "Aborting task $id"
    curl -X POST -s -k -L "http://localhost:5010/tasks/$id/abort/" \
         -H "Content-Type: application/json" \
         -H "Authorization: OAuth $YT_TOKEN"
}

restart_task() {
    local id="$1"
    log "Restarting task $id"
    curl -X POST -s -k -L "http://localhost:5010/tasks/$id/restart/" \
         -H "Content-Type: application/json" \
         -H "Authorization: OAuth $YT_TOKEN"
}

wait_task() {
    local id="$1"
    log "Waiting task $id"
    while true; do
        description=$(get_task $id)
        state=$(echo "$description" | jq '.state')
        if [ "$state" = '"failed"' ] || [ "$state" = '"aborted"' ]; then
            die "Task $id $state"
        fi
        if [ "$state" = '"completed"' ]; then
            break
        fi
        echo "STATE: $state"
        sleep 1.0
    done
}

# Different transfers
echo -e "a\tb" | yt2 write //tmp/test_table --format yamr --proxy kant.yt.yandex.net

id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "kant", "destination_table": "tmp/test_table", "destination_cluster": "cedar"}')
wait_task $id

id=$(run_task '{"source_table": "tmp/test_table", "source_cluster": "cedar", "destination_table": "//tmp/test_table", "destination_cluster": "plato"}')
wait_task $id

id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "plato", "destination_table": "//tmp/test_table_from_plato", "destination_cluster": "kant"}')
wait_task $id

check \
    "$(yt2 read //tmp/test_table --proxy kant.yt.yandex.net --format yamr)" \
    "$(yt2 read //tmp/test_table_from_plato --proxy kant.yt.yandex.net --format yamr)"

# Abort, restart
yt2 remove //tmp/test_table_from_plato --proxy kant.yt.yandex.net --force
id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "plato", "destination_table": "//tmp/test_table_from_plato", "destination_cluster": "kant"}')
abort_task $id
restart_task $id
wait_task $id

check \
    "$(yt2 read //tmp/test_table --proxy kant.yt.yandex.net --format yamr)" \
    "$(yt2 read //tmp/test_table_from_plato --proxy kant.yt.yandex.net --format yamr)"
