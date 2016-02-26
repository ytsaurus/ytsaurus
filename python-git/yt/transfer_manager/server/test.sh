#!/bin/bash -eu

TM_PORT=6000

die() {
    echo $@
    exit 1
}

set +u
if [ -z "$YT_TOKEN" ]; then
    export YT_TOKEN=$(cat ~/.yt/token)
fi

if [ -z "$1" ]; then
    die "Path to TM config is not specified"
fi
set -u

TM_CONFIG=$1

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
    curl -X "$method" -sS -k -L "http://localhost:${TM_PORT}/${path}" \
         -H "Content-Type: application/json" \
         -H "Authorization: OAuth $YT_TOKEN" \
         "$@"
}

get_task() {
    local id="$1"
    request "GET" "tasks/$id/" -f
}

get_task_state() {
    local id="$1"
    description=$(get_task $id)
    state=$(echo "$description" | jq '.state')
    echo "$state"
}

run_task() {
    local body="$1"
    log "Running task $body"
    request "POST" "tasks/" -d "$1" -f
}

abort_task() {
    local id="$1"
    log "Aborting task $id"
    request "POST" "tasks/$id/abort/" -f
}

restart_task() {
    local id="$1"
    log "Restarting task $id"
    request "POST" "tasks/$id/restart/" -f
}

wait_task() {
    local id="$1"
    log "Waiting task $id"
    while true; do
        state=$(get_task_state $id)
        echo "STATE: $state"
        if [ "$state" = '"failed"' ] || [ "$state" = '"aborted"' ]; then
            die "Task $id $state"
        fi
        if [ "$state" = '"completed"' ] || [ "$state" = '"skipped"' ]; then
            break
        fi
        sleep 1.0
    done
}

test_copy_empty_table() {
    echo "Importing empty table from Plato to Smith"
    yt2 create table //tmp/empty_table --proxy plato --ignore-existing
    yt2 set "//tmp/empty_table/@test_attr" 10 --proxy plato
    id=$(run_task '{"source_table": "//tmp/empty_table", "source_cluster": "plato", "destination_table": "//tmp/empty_table", "destination_cluster": "smith", "pool": "ignat"}')
    wait_task $id

    check "true" "$(yt2 exists //tmp/empty_table --proxy smith)"
    check "10" "$(yt2 get //tmp/empty_table/@test_attr --proxy smith)"

    id=$(run_task '{"source_table": "//tmp/empty_table", "source_cluster": "plato", "destination_table": "tmp/empty_table", "destination_cluster": "sakura"}')
    wait_task $id
}

test_copy_from_smith_to_sakura() {
    echo "Importing from Smith to Sakura"
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "smith", "destination_table": "tmp/yt/test_table", "destination_cluster": "sakura"}')
    wait_task $id
}

test_copy_from_smith_to_cedar() {
    echo "Importing from Smith to Sakura"
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "smith", "destination_table": "tmp/yt/test_table", "destination_cluster": "cedar", "mr_user": "userdata"}')
    wait_task $id
}

test_copy_from_sakura_to_cedar() {
    echo "Importing from Sakura to Cedar"
    id=$(run_task '{"source_table": "tmp/yt/test_table", "source_cluster": "sakura", "destination_table": "tmp/yt/test_table", "destination_cluster": "cedar", "mr_user": "userdata"}')
    wait_task $id
}

test_copy_from_cedar_to_plato() {
    echo "Importing from Cedar to Plato"
    id=$(run_task '{"source_table": "tmp/yt/test_table", "source_cluster": "cedar", "destination_table": "//tmp/test_table", "destination_cluster": "plato", "mr_user": "userdata", "pool": "ignat"}')
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
        "$(yt2 read //tmp/test_table_from_plato --proxy quine.yt.yandex.net --format yamr)"

    check "true" "$(yt2 exists //tmp/test_table_from_plato/@sorted --proxy quine.yt.yandex.net)"
}

test_copy_from_sakura_to_plato() {
    echo "Importing from Sakura to Plato"
    # mr_user: asaitgalin because this user has zero quota
    id=$(run_task '{"source_table": "tmp/yt/test_table", "source_cluster": "sakura", "destination_table": "//tmp/test_table_from_sakura", "destination_cluster": "plato", "mr_user": "asaitgalin", "pool": "ignat"}')
    wait_task $id

    check \
        "$(yt2 read //tmp/test_table_from_sakura --proxy plato.yt.yandex.net --format yamr)" \
        "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)"
}

test_various_transfers() {
    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/test_table --format yamr --proxy smith.yt.yandex.net
    yt2 sort --src //tmp/test_table --dst //tmp/test_table --sort-by key --sort-by subkey --proxy smith.yt.yandex.net

    test_copy_from_smith_to_sakura
    test_copy_from_sakura_to_cedar
    test_copy_from_cedar_to_plato
    test_copy_from_plato_to_smith
    test_copy_from_plato_to_quine
    test_copy_from_sakura_to_plato
}

test_abort_restart_task() {
    echo "Test abort and restart"

    yt2 remove --force //tmp/test_table_from_plato --proxy smith
    yt2 remove --force //tmp/test_table --proxy plato
    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/test_table --format yamr --proxy plato
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "plato", "destination_table": "//tmp/test_table_from_plato", "destination_cluster": "smith", "pool": "ignat"}')
    echo "Aborting, than restarting task"
    abort_task $id
    restart_task $id
    wait_task $id

    check \
        "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)" \
        "$(yt2 read //tmp/test_table_from_plato --proxy smith.yt.yandex.net --format yamr)"
}

test_lease() {
    echo "Test task lease"

    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/test_table --format yamr --proxy plato
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "plato", "destination_table": "//tmp/test_table_from_plato", "destination_cluster": "smith", "pool": "ignat", "lease_timeout": 2}')
    sleep 10.0

    check '"aborted"' "$(get_task_state $id)"
}

test_copy_table_range() {
    echo "Test copy table with specified range"

    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/test_table --format yamr --proxy smith.yt.yandex.net

    id=$(run_task '{"source_table": "//tmp/test_table[#1:#2]", "source_cluster": "smith", "destination_table": "//tmp/test_table_from_smith", "destination_cluster": "plato", "pool" : "ignat", "copy_method": "proxy"}')
    wait_task $id

    check \
        "c\td\n" \
        "$(yt2 read //tmp/test_table_from_smith --proxy plato.yt.yandex.net --format yamr)"
}

test_copy_table_attributes() {
    echo "Importing from Smith to Plato (attributes copying test)"

    set_attribute() {
        yt2 set //tmp/test_table/@$1 "$2" --proxy smith.yt.yandex.net
    }

    set_attribute "test_key" "test_value"
    set_attribute "erasure_codec" "lrc_12_2_2"
    set_attribute "compression_codec" "zlib9"

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
    echo "Importing from Smith to Sakura (test spaces in destination table name)"
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "smith", "destination_table": "tmp/yt/test table", "destination_cluster": "sakura"}')
    wait_task $id
}

test_recursive_path_creation() {
    echo "Test recursive path creation at destination"

    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/test_table --format yamr --proxy plato.yt.yandex.net

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "plato", "destination_table": "//tmp/test/table/from/plato", "destination_cluster": "quine", "pool": "ignat"}')
    wait_task $id

    check \
        "$(yt2 read //tmp/test_table --proxy plato.yt.yandex.net --format yamr)" \
        "$(yt2 read //tmp/test/table/from/plato --proxy quine.yt.yandex.net --format yamr)"
}

strip_quotes() {
    local str="$1"
    str="${str%\"}"
    str="${str#\"}"
    echo "$str"
}

test_passing_custom_spec() {
    echo "Test passing spec to Transfer Manager tasks and passing queue name"

    yt2 remove //tmp/test_table --force --proxy quine
    yt2 set //tmp/test_table/@erasure_codec lrc_12_2_2 --proxy plato

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "plato", "destination_table": "//tmp/test_table", "destination_cluster": "quine", "copy_spec": {"type": "copy"}, "postprocess_spec": {"type": "postprocess"}, "copy_method": "proxy", "queue_name": "ignat"}')
    wait_task $id

    local task_descr=$(get_task $id)
    op1=$(strip_quotes $(echo $task_descr | jq '.progress.operations' | jq '.[0].id'))
    op2=$(strip_quotes $(echo $task_descr | jq '.progress.operations' | jq '.[1].id'))
    check "$(yt2 get //sys/operations/$op1/@spec/type --proxy quine)" '"copy"'
    check "$(yt2 get //sys/operations/$op2/@spec/type --proxy quine)" '"postprocess"'
}

test_clusters_configuration_reloading() {
    echo "Test clusters configuration reloading"

    # Making config backup
    temp_filename=$(mktemp)
    cp $TM_CONFIG $temp_filename
    echo "Made config backup: $temp_filename"

    local config=$(cat $TM_CONFIG)
    local config_reload_timeout=$(echo $config | jq ".clusters_config_reload_timeout")
    local sleeping_time=$(($config_reload_timeout + 3))
    echo $config | jq ".availability_graph.cedar = []" > $TM_CONFIG
    echo "Sleeping for $sleeping_time seconds to ensure that config is reloaded" && sleep $sleeping_time

    local task_descr='{"source_table": "tmp/yt/test_table", "source_cluster": "cedar", "destination_table": "//tmp/test_table", "destination_cluster": "plato", "mr_user": "userdata", "pool": "ignat"}'
    local content=$(request "POST" "tasks/" -d "$task_descr")
    check_result=$(echo $content | jq ".inner_errors[0].message" | grep "not available")
    check "$?" "0"

    cp $temp_filename $TM_CONFIG
}

test_types_preserving_during_copy() {
    echo "Test that different column types are copied correctly"

    export YT_VERSION="v3"

    yt2 remove --force //tmp/test_table --proxy quine
    echo '{"a": true, "b": 0, "c": "str"}' | yt2 write //tmp/test_table --proxy quine --format json
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table", "destination_cluster": "plato", "copy_method": "proxy"}')
    wait_task $id

    check "$(yt2 read //tmp/test_table --proxy quine --format json)" "$(yt2 read //tmp/test_table --proxy plato --format json)"

    unset YT_VERSION
}

test_skip_if_destination_exists() {
    echo "Test skipped state of task"

    echo 'a=b' | yt2 write //tmp/test_table --proxy quine --format dsv
    echo 'c=d' | yt2 write //tmp/test_table --proxy plato --format dsv

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table", "destination_cluster": "plato", "skip_if_destination_exists": true}')
    wait_task $id

    check "c=d" "$(yt2 read //tmp/test_table --proxy plato --format dsv)"
}

test_mutating_requests_retries() {
    echo "Test mutating requests retries"

    sleep_time=$(($(jq .mutating_requests_cache_expiration_timeout $TM_CONFIG) / 1000 + 3))
    echo "Waiting for $sleep_time seconds to ensure that TM will process retry requests"
    sleep $sleep_time

    local task='{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table", "destination_cluster": "plato"}'
    echo "Adding task with mutation id"
    task_id=$(request "POST" "tasks/" -d "$task" --header 'X-TM-Parameters:{"mutation_id": "tm_test_mutation_id_123"}')
    sleep 3.0

    echo "Adding task with the same mutation id and retry"
    retry_task_id=$(request "POST" "tasks/" -d "$task" --header 'X-TM-Parameters:{"mutation_id": "tm_test_mutation_id_123", "retry": "true"}')
    echo "Checking that these tasks have the same id"
    check "$task_id" "$retry_task_id"

    echo "Checking that request with the same mutation id but without retry flag will fail"
    retry_task_id2=$(request "POST" "tasks/" -d "$task" --header 'X-TM-Parameters:{"mutation_id": "tm_test_mutation_id_123"}')
    grep_result=$(echo $retry_task_id2 | grep "is not marked as")
    check "$?" "0"

    wait_task "$task_id"

    echo "Ok"
}

test_destination_codecs() {
    echo "Test destination codecs"

    echo 'a=b' | yt2 write //tmp/test_table --proxy quine --format dsv

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table", "destination_cluster": "plato", "destination_compression_codec": "zlib6"}')
    wait_task $id

    check "a=b" "$(yt2 read //tmp/test_table --proxy plato --format dsv)"
    check '"zlib6"' "$(yt2 get //tmp/test_table/@compression_codec --proxy plato)"
}

# Different transfers
test_copy_empty_table
test_various_transfers
test_lease
test_abort_restart_task
test_copy_table_range
test_copy_table_attributes
test_copy_to_yamr_table_with_spaces_in_name
test_recursive_path_creation
test_passing_custom_spec
test_clusters_configuration_reloading
test_types_preserving_during_copy
test_skip_if_destination_exists
test_mutating_requests_retries
test_destination_codecs
