#!/bin/bash -eu

TM_PORT=6000
USER="$(whoami)"

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
    local uuid="$(cat /proc/sys/kernel/random/uuid)"
    curl -X "$method" -sS -k -L "http://localhost:${TM_PORT}/${path}" \
         -H "Content-Type: application/json" \
         -H "Authorization: OAuth $YT_TOKEN" \
         -H 'X-TM-Parameters: {"mutation_id": "'"$uuid"'"}' \
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

run_task_that_should_fail() {
    local body="$1"
    log "Running task that should fail ($body)"
    http_code=$(request "POST" "tasks/" -d "$1" --write-out %{http_code} --output /dev/stderr)
    check "$http_code" "404"
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
    echo "Importing empty table from Banach to Smith"
    yt2 create table //tmp/empty_table --proxy banach --ignore-existing
    yt2 set "//tmp/empty_table/@test_attr" 10 --proxy banach
    id=$(run_task '{"source_table": "//tmp/empty_table", "source_cluster": "banach", "destination_table": "//tmp/empty_table", "destination_cluster": "smith", "pool": "ignat"}')
    wait_task $id

    check "true" "$(yt2 exists //tmp/empty_table --proxy smith)"
    check "10" "$(yt2 get //tmp/empty_table/@test_attr --proxy smith)"

    id=$(run_task '{"source_table": "//tmp/empty_table", "source_cluster": "banach", "destination_table": "tmp/empty_table", "destination_cluster": "sakura"}')
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

test_copy_from_cedar_to_banach() {
    echo "Importing from Cedar to Banach"
    id=$(run_task '{"source_table": "tmp/yt/test_table", "source_cluster": "cedar", "destination_table": "//tmp/test_table", "destination_cluster": "banach", "mr_user": "userdata", "pool": "ignat"}')
    wait_task $id
    check "true" "$(yt2 exists //tmp/test_table/@sorted --proxy banach.yt.yandex.net)"
}

test_copy_from_banach_to_smith() {
    echo "Importing from Banach to Smith"
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "banach", "destination_table": "//tmp/test_table_from_banach", "destination_cluster": "smith", "pool": "ignat"}')
    wait_task $id

    check \
        "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)" \
        "$(yt2 read //tmp/test_table_from_banach --proxy smith.yt.yandex.net --format yamr)"

    check "true" "$(yt2 exists //tmp/test_table_from_banach/@sorted --proxy smith.yt.yandex.net)"
}

test_copy_from_banach_to_quine() {
    echo "Importing from Banach to Quine"
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "banach", "destination_table": "//tmp/test_table_from_banach", "destination_cluster": "quine", "pool": "ignat"}')
    wait_task $id

    check \
        "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)" \
        "$(yt2 read //tmp/test_table_from_banach --proxy quine.yt.yandex.net --format yamr)"

    check "true" "$(yt2 exists //tmp/test_table_from_banach/@sorted --proxy quine.yt.yandex.net)"
}

test_copy_from_sakura_to_banach() {
    echo "Importing from Sakura to Banach"
    # mr_user: asaitgalin because this user has zero quota
    id=$(run_task '{"source_table": "tmp/yt/test_table", "source_cluster": "sakura", "destination_table": "//tmp/test_table_from_sakura", "destination_cluster": "banach", "mr_user": "asaitgalin", "pool": "ignat"}')
    wait_task $id

    check \
        "$(yt2 read //tmp/test_table_from_sakura --proxy banach.yt.yandex.net --format yamr)" \
        "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)"
}

test_various_transfers() {
    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/test_table --format yamr --proxy smith.yt.yandex.net
    yt2 sort --src //tmp/test_table --dst //tmp/test_table --sort-by key --sort-by subkey --proxy smith.yt.yandex.net

    test_copy_from_smith_to_sakura
    test_copy_from_sakura_to_cedar
    test_copy_from_cedar_to_banach
    test_copy_from_banach_to_smith
    test_copy_from_banach_to_quine
    test_copy_from_sakura_to_banach
}

test_incorrect_copy_to_yamr() {
    yt2 remove //tmp/test_table --force --proxy banach

    # Incorrect column names.
    echo -e "a=b" | yt2 write //tmp/test_table --format dsv --proxy banach
    run_task_that_should_fail '{"source_table": "//tmp/test_table", "source_cluster": "banach", "destination_table": "tmp/test_table_from_banach", "destination_cluster": "sakura"}'

    # Incorrect type of values.
    echo -e '{"key": "a", "value": 10}' | yt2 write //tmp/test_table --format json --proxy banach
    run_task_that_should_fail '{"source_table": "//tmp/test_table", "source_cluster": "banach", "destination_table": "tmp/test_table_from_banach", "destination_cluster": "sakura"}'
}

test_abort_restart_task() {
    echo "Test abort and restart"

    yt2 remove --force //tmp/test_table_from_banach --proxy smith
    yt2 remove --force //tmp/test_table --proxy banach
    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/test_table --format yamr --proxy banach
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "banach", "destination_table": "//tmp/test_table_from_banach", "destination_cluster": "smith", "pool": "ignat"}')
    echo "Aborting, than restarting task"
    abort_task $id
    restart_task $id
    wait_task $id

    check \
        "$(yt2 read //tmp/test_table --proxy smith.yt.yandex.net --format yamr)" \
        "$(yt2 read //tmp/test_table_from_banach --proxy smith.yt.yandex.net --format yamr)"
}

test_lease() {
    echo "Test task lease"

    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/test_table --format yamr --proxy banach
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "banach", "destination_table": "//tmp/test_table_from_banach", "destination_cluster": "smith", "pool": "ignat", "lease_timeout": 2}')
    sleep 10.0

    check '"aborted"' "$(get_task_state $id)"
}

test_copy_table_range() {
    echo "Test copy table with specified range"

    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/test_table --format yamr --proxy smith.yt.yandex.net

    id=$(run_task '{"source_table": "//tmp/test_table[#1:#2]", "source_cluster": "smith", "destination_table": "//tmp/test_table_from_smith", "destination_cluster": "banach", "pool" : "ignat", "copy_method": "proxy"}')
    wait_task $id

    check \
        "c\td\n" \
        "$(yt2 read //tmp/test_table_from_smith --proxy banach.yt.yandex.net --format yamr)"
}

test_copy_table_range_with_codec() {
    echo "Test copy table with specified range and codecs"

    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/test_table --format yamr --proxy smith.yt.yandex.net

    id=$(run_task '{"source_table": "//tmp/test_table[#1:#2]", "source_cluster": "smith", "destination_table": "//tmp/test_table_from_smith", "destination_cluster": "banach", "pool" : "ignat", "copy_method": "proxy", "destination_compression_codec": "zlib9"}')
    wait_task $id

    check \
        "c\td\n" \
        "$(yt2 read //tmp/test_table_from_smith --proxy banach.yt.yandex.net --format yamr)"

    # Remote copy do not support ranges.
    #id=$(run_task '{"source_table": "//tmp/test_table[#1:#2]", "source_cluster": "smith", "destination_table": "//tmp/test_table_from_smith", "destination_cluster": "banach", "pool" : "ignat", "destination_compression_codec": "zlib9"}')
    #wait_task $id

    #check \
    #    "c\td\n" \
    #    "$(yt2 read //tmp/test_table_from_smith --proxy banach.yt.yandex.net --format yamr)"
}

test_copy_table_attributes() {
    echo "Importing from Smith to Banach (attributes copying test)"

    set_attribute() {
        yt2 set //tmp/test_table/@$1 "$2" --proxy smith.yt.yandex.net
    }

    set_attribute "test_key" "test_value"
    set_attribute "erasure_codec" "lrc_12_2_2"
    set_attribute "compression_codec" "zlib9"

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "smith", "destination_table": "//tmp/test_table_from_smith", "destination_cluster": "banach", "pool": "ignat"}')
    wait_task $id

    check_attribute() {
        check \
            "$(yt2 get //tmp/test_table_from_smith/@$1 --proxy banach.yt.yandex.net)" \
            "$(yt2 get //tmp/test_table/@$1 --proxy smith.yt.yandex.net)"
    }

    for attribute in "test_key" "erasure_codec" "compression_codec"; do
        check_attribute $attribute
    done

    yt2 remove //tmp/test_table_from_smith --proxy banach.yt.yandex.net --force

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

    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/test_table --format yamr --proxy banach.yt.yandex.net

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "banach", "destination_table": "//tmp/test/table/from/banach", "destination_cluster": "quine", "pool": "ignat"}')
    wait_task $id

    check \
        "$(yt2 read //tmp/test_table --proxy banach.yt.yandex.net --format yamr)" \
        "$(yt2 read //tmp/test/table/from/banach --proxy quine.yt.yandex.net --format yamr)"
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
    yt2 set //tmp/test_table/@erasure_codec lrc_12_2_2 --proxy banach

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "banach", "destination_table": "//tmp/test_table", "destination_cluster": "quine", "copy_spec": {"type": "copy", "pool": "copy"}, "postprocess_spec": {"type": "postprocess"}, "copy_method": "proxy", "queue_name": "ignat"}')
    wait_task $id

    local task_descr=$(get_task $id)
    op1=$(strip_quotes $(echo $task_descr | jq '.progress.operations' | jq '.[0].id'))
    op2=$(strip_quotes $(echo $task_descr | jq '.progress.operations' | jq '.[1].id'))
    check "$(yt2 get //sys/operations/$op1/@spec/type --proxy quine)" '"copy"'
    check "$(yt2 get //sys/operations/$op1/@spec/pool --proxy quine)" '"copy"'
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
    local sleeping_time=$(($config_reload_timeout / 1000 + 3))
    echo $config | jq ".availability_graph.cedar = []" > $TM_CONFIG
    echo "Sleeping for $sleeping_time seconds to ensure that config is reloaded" && sleep $sleeping_time

    local task_descr='{"source_table": "tmp/yt/test_table", "source_cluster": "cedar", "destination_table": "//tmp/test_table", "destination_cluster": "banach", "mr_user": "userdata", "pool": "ignat"}'
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
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table", "destination_cluster": "banach", "copy_method": "proxy"}')
    wait_task $id

    check "$(yt2 read //tmp/test_table --proxy quine --format json)" "$(yt2 read //tmp/test_table --proxy banach --format json)"

    unset YT_VERSION
}

test_skip_if_destination_exists() {
    echo "Test skipped state of task"

    echo 'a=b' | yt2 write //tmp/test_table --proxy quine --format dsv
    echo 'c=d' | yt2 write //tmp/test_table --proxy banach --format dsv

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table", "destination_cluster": "banach", "skip_if_destination_exists": true}')
    wait_task $id

    check "c=d" "$(yt2 read //tmp/test_table --proxy banach --format dsv)"
}

test_mutating_requests_retries() {
    echo "Test mutating requests retries"

    sleep_time=$(($(jq .mutating_requests_cache_expiration_timeout $TM_CONFIG) / 1000 + 3))
    echo "Waiting for $sleep_time seconds to ensure that TM will process retry requests"
    sleep $sleep_time

    local task='{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table", "destination_cluster": "banach"}'
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

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table", "destination_cluster": "banach", "destination_compression_codec": "zlib6"}')
    wait_task $id

    check "a=b" "$(yt2 read //tmp/test_table --proxy banach --format dsv)"
    check '"zlib6"' "$(yt2 get //tmp/test_table/@compression_codec --proxy banach)"
}

test_source_codecs() {
    echo "Test source codecs"

    yt2 remove //tmp/test_table --proxy quine --force
    yt2 create table //tmp/test_table --proxy quine --attributes '{compression_codec=zlib6}'
    echo 'a=b' | yt2 write //tmp/test_table --proxy quine --format dsv

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table", "destination_cluster": "banach"}')
    wait_task $id

    check "a=b" "$(yt2 read //tmp/test_table --proxy banach --format dsv)"
    check '"zlib6"' "$(yt2 get //tmp/test_table/@compression_codec --proxy banach)"
}

test_intermediate_format() {
    echo "Test intermediate format"

    echo 'a=b' | yt2 write //tmp/test_table --proxy quine --format dsv

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table", "destination_cluster": "banach", "copy_method": "proxy", "intermediate_format": "<boolean_as_string=false>yson"}')
    wait_task $id

    check "a=b" "$(yt2 read //tmp/test_table --proxy banach --format dsv)"
}

test_delete_tasks() {
    echo "Test delete multiple tasks"

    echo 'a=b' | yt2 write //tmp/test_table --proxy quine --format dsv

    t1=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table1", "destination_cluster": "banach"}')
    t2=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table2", "destination_cluster": "banach"}')

    http_code=$(request "DELETE" "tasks/" -d "[\"$t1\", \"$t2\"]" --silent --write-out %{http_code} --output /dev/null)
    check "$http_code" "404"

    wait_task $t1
    wait_task $t2

    http_code=$(request "DELETE" "tasks/" -d "[\"$t1\", \"$t2\"]" --silent --write-out %{http_code} --output /dev/null)
    check "$http_code" "200"

    echo "Ok"
}

test_copy_inefficiently_stored_table()
{
    echo "Test copy inefficiently stored table"

    gen() {
        for i in {1..101}; do
            echo "a=b"
        done
    }

    yt2 remove --force '//tmp/test_table' --proxy banach
    gen | yt2 write '<append=true>//tmp/test_table' --proxy banach --format dsv --config '{write_retries={chunk_size=1}}'

    check "$(yt2 get //tmp/test_table/@chunk_count --proxy banach)" "101"
    check "$(yt2 get //tmp/test_table/@row_count --proxy banach)" "101"

    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "banach", "destination_table": "//tmp/new_test_table", "destination_cluster": "smith", "pool": "ignat"}')
    wait_task $id

    check "$(yt2 get //tmp/new_test_table/@row_count --proxy smith)" "101"
}

test_copy_with_annotated_json() {
    echo "Test copy with annotated JSON"

    echo -ne '{"x"=12u}\n' | yt2 write //tmp/test_table --proxy quine --format yson
    id=$(run_task '{"source_table": "//tmp/test_table", "source_cluster": "quine", "destination_table": "//tmp/test_table1", "destination_cluster": "banach", "copy_method": "proxy"}')
    wait_task $id

    check "$(yt2 read //tmp/test_table1 --format '<format=text>yson' --proxy banach)" '{"x"=12u;};'
}

test_kiwi_copy() {
    echo "Test kiwi copy"
    id=$(run_task '{"source_table": "//home/ignat/tm_kiwi_test_table", "source_cluster": "freud", "destination_cluster": "kiwi_apteryx", "kiwi_user": "flux"}')
    wait_task $id
}

_ensure_operation_started() {
    local task_id="$1" && shift
    local op_id=""
    while true; do
        log "Waiting for started task operation..."
        op_id=$(get_task $task_id | jq ".progress.operations[0].id" | tr -d "[:space:]" | tr -d "\"")
        if [[ "${op_id}" != "null" ]]; then
            log "Operation $op_id started"
            echo "$op_id"
            break
        fi
        sleep 1.0
    done
}

_ensure_transfer_manager_is_running() {
    while true; do
        log "Waiting for Transfer Manager to become ready..."
        # XXX(asaitgalin): Not using "request" here, because request can fail with "Connection refused"
        set +e
        resp=$(curl "http://localhost:$TM_PORT/ping/" --silent --write-out %{http_code} --output /dev/null)
        set -e

        if [[ $resp -eq 200 ]]; then
            break
        fi
        sleep 1.0
    done
}

test_abort_operations_on_startup() {
    echo "Test abort operations on startup"

    echo 'a=b\n' | yt2 write //tmp/test_table --proxy quine --format dsv

    task_description=$(cat <<EOF
{
    "source_table": "//tmp/test_table",
    "source_cluster": "quine",
    "destination_table": "//tmp/test_table1",
    "destination_cluster": "banach",
    "copy_method": "proxy",
    "destination_erasure_codec": "lrc_12_2_2"
}
EOF
)
    task_id=$(run_task "$task_description")
    echo "Started task $task_id"

    op=$(_ensure_operation_started $task_id)

    echo "Operations started, killing Transfer Manager..."
    for pid in $(pgrep -f transfer-manager-server -u $USER); do
        # XXX(asaitgalin): -9 to ensure tasks are not aborted during process termination
        kill -9 $pid || true
    done

    echo "Aborting lock transaction and starting TM..."

    local path=$(cat "$TM_CONFIG" | jq .path | tr -d "\"")
    local proxy=$(cat "$TM_CONFIG" | jq .yt_backend_options.proxy | tr -d "\"")

    yt2 abort-tx $(yt2 get $path/lock/@locks/0/transaction_id --proxy $proxy | tr -d "\"")
    transfer-manager-server --config $(realpath $TM_CONFIG) &

    _ensure_transfer_manager_is_running

    check "$(yt2 get //sys/operations/$op/@state --proxy banach | tr -d "\"")" "aborted"
    wait_task $task_id

    echo "Ok"
}

# Different transfers
test_copy_empty_table
test_various_transfers
test_incorrect_copy_to_yamr
test_lease
test_abort_restart_task
test_copy_table_range
test_copy_table_range_with_codec
test_copy_table_attributes
test_copy_to_yamr_table_with_spaces_in_name
test_recursive_path_creation
test_passing_custom_spec
test_clusters_configuration_reloading
test_types_preserving_during_copy
test_skip_if_destination_exists
test_mutating_requests_retries
test_destination_codecs
test_source_codecs
test_intermediate_format
test_delete_tasks
test_copy_inefficiently_stored_table
test_copy_with_annotated_json
test_kiwi_copy
test_abort_operations_on_startup
