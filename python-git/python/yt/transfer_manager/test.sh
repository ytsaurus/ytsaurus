#!/bin/bash -eu

PORT=5010

set +u
if [ -z "$YT_TOKEN" ]; then
    export YT_TOKEN=$(cat ~/.yt/token)
fi
set -u

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

request() {
    local method="$1" && shift
    local path="$1" && shift
    curl -X "$method" -sS -k -L -f "http://localhost:${PORT}/${path}" \
         -H "Content-Type: application/json" \
         -H "Authorization: OAuth $YT_TOKEN" \
         "$@"
}

get_task() {
    local id="$1"
    request "GET" "tasks/$id/"
}

run_task() {
    local body="$1"
    log "Running task $body"
    request "POST" "tasks/" -d "$1"
}

abort_task() {
    local id="$1"
    log "Aborting task $id"
    request "POST" "tasks/$id/abort/"
}

restart_task() {
    local id="$1"
    log "Restarting task $id"
    request "POST" "tasks/$id/restart/"
}

wait_task() {
    local id="$1"
    log "Waiting task $id"
    while true; do
        description=$(get_task $id)
        state=$(echo "$description" | jq '.state')
        echo "STATE: $state"
        if [ "$state" = '"failed"' ] || [ "$state" = '"aborted"' ]; then
            die "Task $id $state"
        fi
        if [ "$state" = '"completed"' ]; then
            break
        fi
        sleep 1.0
    done
}

# Different transfers
echo -e "a\tb" | yt2 write //tmp/test_table --format yamr --proxy smith.yt.yandex.net

echo "Importing from Smith to Cedar"
id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "smith", "destination_table": "tmp/yt/test_table", "destination_cluster": "cedar"}')
wait_task $id

echo "Importing from Cedar to Redwood"
id=$(run_task '{"source_table": "tmp/yt/test_table", "source_cluster": "cedar", "destination_table": "tmp/yt/test_table", "destination_cluster": "redwood", "mr_user": "userdata"}')
wait_task $id

echo "Importing from Redwood to Plato"
id=$(run_task '{"source_table": "tmp/yt/test_table", "source_cluster": "redwood", "destination_table": "//tmp/test_table", "destination_cluster": "plato", "mr_user": "userdata"}')
wait_task $id

echo "Importing from Plato to Smith"
id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "plato", "destination_table": "//tmp/test_table_from_plato", "destination_cluster": "smith"}')
wait_task $id

check \
    "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)" \
    "$(yt2 read //tmp/test_table_from_plato --proxy smith.yt.yandex.net --format yamr)"

echo "Importing from Plato to Quine"
id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "plato", "destination_table": "//tmp/test_table_from_plato", "destination_cluster": "quine"}')
wait_task $id

check \
    "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)" \
    "$(yt2 read //tmp/test_table_from_plato --proxy smith.yt.yandex.net --format yamr)"

# Abort, restart
yt2 remove //tmp/test_table_from_plato --proxy smith.yt.yandex.net --force
echo "Importing from Plato to Smith"
id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "plato", "destination_table": "//tmp/test_table_from_plato", "destination_cluster": "smith"}')
echo "Aborting, than restarting task"
abort_task $id
restart_task $id
wait_task $id

check \
    "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)" \
    "$(yt2 read //tmp/test_table_from_plato --proxy smith.yt.yandex.net --format yamr)"
