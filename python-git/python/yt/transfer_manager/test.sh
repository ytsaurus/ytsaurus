#!/bin/bash -eu

PORT=6000

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
echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/test_table --format yamr --proxy smith.yt.yandex.net
yt2 sort --src //tmp/test_table --dst //tmp/test_table --sort-by key --sort-by subkey --proxy smith.yt.yandex.net

test_copy_from_smith_to_cedar() {
    echo "Importing from Smith to Cedar"
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "smith", "destination_table": "tmp/yt/test_table", "destination_cluster": "cedar"}')
    wait_task $id
}

test_copy_from_cedar_to_redwood() {
    echo "Importing from Cedar to Redwood"
    id=$(run_task '{"source_table": "tmp/yt/test_table", "source_cluster": "cedar", "destination_table": "tmp/yt/test_table", "destination_cluster": "redwood", "mr_user": "userdata"}')
    wait_task $id
}

test_copy_from_redwood_to_plato() {
    echo "Importing from Redwood to Plato"
    id=$(run_task '{"source_table": "tmp/yt/test_table", "source_cluster": "redwood", "destination_table": "//tmp/test_table", "destination_cluster": "plato", "mr_user": "userdata", "pool": "ignat"}')
    wait_task $id
    check "true" "$(yt2 exists //tmp/test_table/@sorted --proxy plato.yt.yandex.net)"
}

test_copy_from_plato_to_smith() {
    echo "Importing from Plato to Smith"
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "plato", "destination_table": "//tmp/test_table_from_plato", "destination_cluster": "smith", "pool": "ignat"}')
    wait_task $id

    check \
        "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)" \
        "$(yt2 read //tmp/test_table_from_plato --proxy smith.yt.yandex.net --format yamr)"

    check "true" "$(yt2 exists //tmp/test_table_from_plato/@sorted --proxy smith.yt.yandex.net)"
}

test_copy_from_plato_to_quine() {
    echo "Importing from Plato to Quine"
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "plato", "destination_table": "//tmp/test_table_from_plato", "destination_cluster": "quine", "pool": "ignat"}')
    wait_task $id

    check \
        "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)" \
        "$(yt2 read //tmp/test_table_from_plato --proxy smith.yt.yandex.net --format yamr)"

    check "true" "$(yt2 exists //tmp/test_table_from_plato/@sorted --proxy quine.yt.yandex.net)"
}

test_copy_from_cedar_to_plato() {
    echo "Importing from Cedar to Plato"
    # mr_user: asaitgalin because this user has zero quota
    id=$(run_task '{"source_table": "tmp/yt/test_table", "source_cluster": "cedar", "destination_table": "//tmp/test_table_from_cedar", "destination_cluster": "plato", "mr_user": "asaitgalin", "pool": "ignat"}')
    wait_task $id

    check \
        "$(yt2 read //tmp/test_table_from_cedar --proxy plato.yt.yandex.net --format yamr)" \
        "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)"
}

test_abort_restart_task() {
    yt2 remove //tmp/test_table_from_plato --proxy smith.yt.yandex.net --force
    echo "Importing from Plato to Smith"
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "plato", "destination_table": "//tmp/test_table_from_plato", "destination_cluster": "smith", "pool": "ignat"}')
    echo "Aborting, than restarting task"
    abort_task $id
    restart_task $id
    wait_task $id

    check \
        "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)" \
        "$(yt2 read //tmp/test_table_from_plato --proxy smith.yt.yandex.net --format yamr)"
}

test_copy_table_attributes() {
    echo "Importing from Smith to Plato (attributes copying test)"

    set_attribute() {
        yt2 set //tmp/test_table/@$1 "$2" --proxy smith.yt.yandex.net
    }

    set_attribute "test_key" "test_value"
    set_attribute "erasure_codec" "lrc_12_2_2"
    set_attribute "compression_codec" "gzip_best_compression"

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "smith", "destination_table": "//tmp/test_table_from_smith", "destination_cluster": "plato", "pool": "ignat"}')
    wait_task $id

    check_attribute() {
        check \
            "$(yt2 get //tmp/test_table_from_smith/@$1 --proxy plato.yt.yandex.net)" \
            "$(yt2 get //tmp/test_table/@$1 --proxy smith.yt.yandex.net)"
    }

    for attribute in "test_key" "erasure_codec" "compression_codec"; do
        check_attribute $attribute
    done

    yt2 remove //tmp/test_table_from_smith --proxy plato.yt.yandex.net --force

    unset -f set_attribute
    unset -f check_attribute
}

test_copy_to_yamr_table_with_spaces_in_name() {
    echo "Importing from Smith to Cedar (test spaces in destination table name)"
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "smith", "destination_table": "tmp/yt/test table", "destination_cluster": "cedar"}')
    wait_task $id
}

test_copy_from_smith_to_cedar
test_copy_from_cedar_to_redwood
test_copy_from_redwood_to_plato
test_copy_from_plato_to_smith
test_copy_from_plato_to_quine
test_copy_from_cedar_to_plato
test_abort_restart_task
test_copy_table_attributes
test_copy_to_yamr_table_with_spaces_in_name
