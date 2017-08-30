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
FORCE_COPY_WITH_OPERATION=false

cleanup() {
    # TODO(ignat): how automatically determine clusters
    for cluster in "freud" "hahn" "banach"; do
        yt2 remove //tmp/tm --recursive --force --proxy $cluster
        yt2 create map_node //tmp/tm --proxy $cluster
    done;
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
    if $FORCE_COPY_WITH_OPERATION; then
        log "Running task with set force_copy_with_operation flag"
        request "POST" "tasks/" -d "$(echo $body | jq '.force_copy_with_operation=true')" -f
    else
        request "POST" "tasks/" -d "$1" -f
    fi
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
    local prev_state=""
    local skipped="0"
    local print_state
    log "Waiting task $id"
    while true; do
        state=$(get_task_state $id)
        if [ "$state" = "$prev_state" ]; then
            skipped=$(($skipped + 1))
            print_state="0"
        fi
        if [ "$state" != "$prev_state" ] || [ "$skipped" = "10" ]; then
            print_state="1"
            skipped="0"
        fi
        if [ "$print_state" = "1" ]; then
            echo "STATE: $state"
        fi
        if [ "$state" = '"failed"' ] || [ "$state" = '"aborted"' ]; then
            die "Task $id $state"
        fi
        if [ "$state" = '"completed"' ] || [ "$state" = '"skipped"' ]; then
            break
        fi
        prev_state="$state"
        sleep 1.0
    done
}

wait_task_to_fail() {
    local id="$1"
    log "Waiting task $id (should fail)"
    while true; do
        state=$(get_task_state $id)
        echo "STATE: $state"
        if [ "$state" = '"failed"' ]; then
            echo "Task $id failed as expected"
            break
        fi
        if [ "$state" = '"completed"' ] || [ "$state" = '"skipped"' ] || [ "$state" = '"aborted"' ]; then
            die "Task $id $state, but expected to fail"
        fi
        sleep 1.0
    done
}

test_copy_empty_table() {
    echo "Importing empty table from Banach to Freud"
    yt2 create table //tmp/tm/empty_table --proxy banach --ignore-existing
    yt2 set "//tmp/tm/empty_table/@test_attr" 10 --proxy banach
    id=$(run_task '{"source_table": "//tmp/tm/empty_table", "source_cluster": "banach", "destination_table": "//tmp/tm/empty_table", "destination_cluster": "freud", "pool": "ignat"}')
    wait_task $id

    check "true" "$(yt2 exists //tmp/tm/empty_table --proxy freud)"
    check "10" "$(yt2 get //tmp/tm/empty_table/@test_attr --proxy freud)"
}

test_copy_empty_file() {
    echo "Importing empty file from Banach to Freud"
    yt2 create file //tmp/tm/empty_file --proxy banach --ignore-existing
    yt2 set "//tmp/tm/empty_file/@test_attr" 10 --proxy banach
    id=$(run_task '{"source_table": "//tmp/tm/empty_file", "source_cluster": "banach", "destination_table": "//tmp/tm/empty_file", "destination_cluster": "freud", "pool": "ignat"}')
    wait_task $id

    check "true" "$(yt2 exists //tmp/tm/empty_file --proxy freud)"
    check "10" "$(yt2 get //tmp/tm/empty_file/@test_attr --proxy freud)"
}

test_copy_from_freud_to_banach() {
    echo "Importing from Freud to Banach"
    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table", "destination_cluster": "banach"}')
    wait_task $id
}


test_copy_from_banach_to_freud() {
    echo "Importing from Banach to Freud"
    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "banach", "destination_table": "//tmp/tm/test_table_from_banach", "destination_cluster": "freud", "pool": "ignat"}')
    wait_task $id

    check \
        "$(yt2 read //tmp/tm/test_table --proxy banach --format yamr)" \
        "$(yt2 read //tmp/tm/test_table_from_banach --proxy freud --format yamr)"

    check "true" "$(yt2 exists //tmp/tm/test_table_from_banach/@sorted --proxy freud)"
}

test_various_transfers() {
    echo "Test various transfers"

    yt2 remove //tmp/tm/test_table --proxy freud --force
    yt2 remove //tmp/tm/test_table --proxy freud --force
    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/tm/test_table --format yamr --proxy freud
    yt2 sort --src //tmp/tm/test_table --dst //tmp/tm/test_table --sort-by key --sort-by subkey --proxy freud

    test_copy_from_freud_to_banach
    test_copy_from_banach_to_freud
}

test_copy_file() {
    echo "Importing from Freud to Banach (file copying test)"

    echo -e "TestFile" | yt2 write-file //tmp/tm/test_file --proxy freud

    id=$(run_task '{"source_table": "//tmp/tm/test_file", "source_cluster": "freud",
                    "destination_table": "//tmp/tm/test_file", "destination_cluster": "banach", "pool": "ignat"}')
    wait_task $id

    check "true" "$(yt2 exists //tmp/tm/test_file --proxy banach)"

    check \
        "$(yt2 read-file //tmp/tm/test_file --proxy freud)" \
        "$(yt2 read-file //tmp/tm/test_file --proxy banach)"
}

test_copy_file_attributes() {
    echo "Importing from Freud to Banach (file attributes copying test)"

    echo -e "Test file" | yt2 write-file //tmp/tm/test_file --proxy freud

    set_attribute() {
        yt2 set //tmp/tm/test_file/@$1 "$2" --proxy freud
    }

    set_attribute "test_key" "test_value"
    set_attribute "erasure_codec" "lrc_12_2_2"
    set_attribute "compression_codec" "zlib_9"
    set_attribute "expiration_time" "2100000000000"

    id=$(run_task '{"source_table": "//tmp/tm/test_file", "source_cluster": "freud", "destination_table": "//tmp/tm/test_file", "destination_cluster": "banach", "pool": "ignat", "additional_attributes": ["expiration_time"]}')
    wait_task $id

    check_attribute() {
        check \
            "$(yt2 get //tmp/tm/test_file/@$1 --proxy banach)" \
            "$(yt2 get //tmp/tm/test_file/@$1 --proxy freud)"
    }

    for attribute in "test_key" "erasure_codec" "compression_codec" "expiration_time"; do
        check_attribute $attribute
    done

    yt2 remove //tmp/tm/test_file --proxy banach --force

    unset -f set_attribute
    unset -f check_attribute
}

test_destination_file_codecs() {
    echo "Test destination file codecs"

    echo "Test Content" | yt2 write-file //tmp/tm/test_file --proxy freud

    id=$(run_task '{"source_table": "//tmp/tm/test_file", "source_cluster": "freud", "destination_table": "//tmp/tm/test_file", "destination_cluster": "banach", "destination_compression_codec": "zlib_6"}')
    wait_task $id

    check "Test Content" "$(yt2 read-file //tmp/tm/test_file --proxy banach)"
    check '"zlib_6"' "$(yt2 get //tmp/tm/test_file/@compression_codec --proxy banach)"
}

test_source_file_codecs() {
    echo "Test source file codecs"

    yt2 remove //tmp/tm/test_file --proxy freud --force
    yt2 create file //tmp/tm/test_file --proxy freud --attributes '{compression_codec=zlib_6}'
    echo "test" | yt2 write-file //tmp/tm/test_file --proxy freud

    id=$(run_task '{"source_table": "//tmp/tm/test_file", "source_cluster": "freud", "destination_table": "//tmp/tm/test_file", "destination_cluster": "banach"}')
    wait_task $id

    check "test" "$(yt2 read-file //tmp/tm/test_file --proxy banach)"
    check '"zlib_6"' "$(yt2 get //tmp/tm/test_file/@compression_codec --proxy banach)"
}

test_abort_restart_task() {
    echo "Test abort and restart"

    yt2 remove --force //tmp/tm/test_table_from_banach --proxy freud
    yt2 remove --force //tmp/tm/test_table --proxy banach
    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/tm/test_table --format yamr --proxy banach
    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "banach", "destination_table": "//tmp/tm/test_table_from_banach", "destination_cluster": "freud", "pool": "ignat"}')
    echo "Aborting, than restarting task"
    abort_task $id
    restart_task $id
    wait_task $id

    check \
        "$(yt2 read //tmp/tm/test_table --proxy banach --format yson)" \
        "$(yt2 read //tmp/tm/test_table_from_banach --proxy freud --format yson)"
}

test_lease() {
    echo "Test task lease"

    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/tm/test_table --format yamr --proxy banach
    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "banach", "destination_table": "//tmp/tm/test_table_from_banach", "destination_cluster": "freud", "pool": "ignat", "lease_timeout": 2}')
    sleep 10.0

    check '"aborted"' "$(get_task_state $id)"
}

test_copy_table_range() {
    echo "Test copy table with specified range"

    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/tm/test_table --format yamr --proxy freud

    id=$(run_task '{"source_table": "//tmp/tm/test_table[#1:#2]", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table_from_freud", "destination_cluster": "banach", "pool" : "ignat", "copy_method": "proxy"}')
    wait_task $id

    check \
        "c\td\n" \
        "$(yt2 read //tmp/tm/test_table_from_freud --proxy banach --format yamr)"
}

test_copy_table_range_with_codec() {
    echo "Test copy table with specified range and codecs"

    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/tm/test_table --format yamr --proxy freud

    id=$(run_task '{"source_table": "//tmp/tm/test_table[#1:#2]", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table_from_freud", "destination_cluster": "banach", "pool" : "ignat", "copy_method": "proxy", "destination_compression_codec": "zlib_9"}')
    wait_task $id

    check \
        "c\td\n" \
        "$(yt2 read //tmp/tm/test_table_from_freud --proxy banach --format yamr)"

    # Remote copy do not support ranges.
    #id=$(run_task '{"source_table": "//tmp/tm/test_table[#1:#2]", "source_cluster": "hume", "destination_table": "//tmp/tm/test_table_from_hume", "destination_cluster": "banach", "pool" : "ignat", "destination_compression_codec": "zlib_9"}')
    #wait_task $id

    #check \
    #    "c\td\n" \
    #    "$(yt2 read //tmp/tm/test_table_from_hume --proxy banach --format yamr)"
}

test_copy_table_attributes() {
    echo "Importing from Freud to Banach (attributes copying test)"

    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/tm/test_table --format yamr --proxy freud

    set_attribute() {
        yt2 set //tmp/tm/test_table/@$1 "$2" --proxy freud
    }

    set_attribute "test_key" "test_value"
    set_attribute "erasure_codec" "lrc_12_2_2"
    set_attribute "compression_codec" "zlib_9"
    set_attribute "expiration_time" "2100000000000"

    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table_from_freud", "destination_cluster": "banach", "pool": "ignat", "additional_attributes": ["expiration_time"]}')
    wait_task $id

    check_attribute() {
        check \
            "$(yt2 get //tmp/tm/test_table_from_freud/@$1 --proxy banach)" \
            "$(yt2 get //tmp/tm/test_table/@$1 --proxy freud)"
    }

    for attribute in "test_key" "erasure_codec" "compression_codec" "expiration_time"; do
        check_attribute $attribute
    done

    yt2 remove //tmp/tm/test_table_from_freud --proxy banach --force

    unset -f set_attribute
    unset -f check_attribute
}


test_recursive_path_creation() {
    echo "Test recursive path creation at destination"

    echo -e "a\tb\nc\td\ne\tf" | yt2 write //tmp/tm/test_table --format yamr --proxy banach

    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "banach", "destination_table": "//tmp/tm/test/table/from/banach", "destination_cluster": "freud", "pool": "ignat"}')
    wait_task $id

    check \
        "$(yt2 read //tmp/tm/test_table --proxy banach --format yamr)" \
        "$(yt2 read //tmp/tm/test/table/from/banach --proxy freud --format yamr)"
}

strip_quotes() {
    local str="$1"
    str="${str%\"}"
    str="${str#\"}"
    echo "$str"
}

test_passing_custom_spec() {
    echo "Test passing spec to Transfer Manager tasks and passing queue name"

    yt2 remove //tmp/tm/test_table --force --proxy freud
    yt2 set //tmp/tm/test_table/@erasure_codec lrc_12_2_2 --proxy banach

    task_description=$(cat <<EOF
{
    "source_table": "//tmp/tm/test_table",
    "source_cluster": "banach",
    "destination_table": "//tmp/tm/test_table",
    "destination_cluster": "freud",
    "copy_method": "proxy",
    "copy_spec": {"type": "copy", "pool": "copy"},
    "postprocess_spec": {"type": "postprocess"},
    "queue_name": "ignat",
    "force_copy_with_operation": true
}
EOF
)
    id=$(run_task "$task_description")
    wait_task $id

    local task_descr=$(get_task $id)
    op1=$(strip_quotes $(echo $task_descr | jq '.progress.operations' | jq '.[0].id'))
    op2=$(strip_quotes $(echo $task_descr | jq '.progress.operations' | jq '.[1].id'))
    check "$(yt2 get //sys/operations/$op1/@spec/type --proxy freud)" '"copy"'
    check "$(yt2 get //sys/operations/$op1/@spec/pool --proxy freud)" '"copy"'
    check "$(yt2 get //sys/operations/$op2/@spec/type --proxy freud)" '"postprocess"'
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
    echo $config | jq ".availability_graph.freud = []" > $TM_CONFIG
    echo "Sleeping for $sleeping_time seconds to ensure that config is reloaded" && sleep $sleeping_time

    local task_descr='{"source_table": "tmp/yt/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table", "destination_cluster": "banach", "pool": "ignat"}'
    local content=$(request "POST" "tasks/" -d "$task_descr")
    check_result=$(echo $content | jq ".inner_errors[0].message" | grep "not available")
    check "$?" "0"

    cp $temp_filename $TM_CONFIG
}

test_types_preserving_during_copy() {
    echo "Test that different column types are copied correctly"

    export YT_VERSION="v3"

    yt2 remove --force //tmp/tm/test_table --proxy freud
    echo '{"a": true, "b": 0, "c": "str"}' | yt2 write //tmp/tm/test_table --proxy freud --format json
    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table", "destination_cluster": "banach", "copy_method": "proxy"}')
    wait_task $id

    check "$(yt2 read //tmp/tm/test_table --proxy freud --format json)" "$(yt2 read //tmp/tm/test_table --proxy banach --format json)"

    unset YT_VERSION
}

test_skip_if_destination_exists() {
    echo "Test skipped state of task"

    echo 'a=b' | yt2 write //tmp/tm/test_table --proxy freud --format dsv
    echo 'c=d' | yt2 write //tmp/tm/test_table --proxy banach --format dsv

    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table", "destination_cluster": "banach", "skip_if_destination_exists": true}')
    wait_task $id

    check "c=d" "$(yt2 read //tmp/tm/test_table --proxy banach --format dsv)"
}

test_mutating_requests_retries() {
    echo "Test mutating requests retries"

    sleep_time=$(($(jq .mutating_requests_cache_expiration_timeout $TM_CONFIG) / 1000 + 3))
    echo "Waiting for $sleep_time seconds to ensure that TM will process retry requests"
    sleep $sleep_time

    local task='{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table", "destination_cluster": "banach"}'
    echo "Adding task with mutation id"
    task_id=$(request "POST" "tasks/" -d "$task" --header 'X-TM-Parameters:{"mutation_id": "test_mutation_id_123"}')
    sleep 3.0

    echo "Adding task with the same mutation id and retry"
    retry_task_id=$(request "POST" "tasks/" -d "$task" --header 'X-TM-Parameters:{"mutation_id": "test_mutation_id_123", "retry": "true"}')
    echo "Checking that these tasks have the same id"
    check "$task_id" "$retry_task_id"

    echo "Checking that request with the same mutation id but without retry flag will fail"
    retry_task_id2=$(request "POST" "tasks/" -d "$task" --header 'X-TM-Parameters:{"mutation_id": "test_mutation_id_123"}')
    grep_result=$(echo $retry_task_id2 | grep "is not marked as")
    check "$?" "0"

    wait_task "$task_id"

    echo "Ok"
}

test_destination_codecs() {
    echo "Test destination codecs"

    echo 'a=b' | yt2 write //tmp/tm/test_table --proxy freud --format dsv

    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table", "destination_cluster": "banach", "destination_compression_codec": "zlib_6"}')
    wait_task $id

    check "a=b" "$(yt2 read //tmp/tm/test_table --proxy banach --format dsv)"
    check '"zlib_6"' "$(yt2 get //tmp/tm/test_table/@compression_codec --proxy banach)"
}

test_source_codecs() {
    echo "Test source codecs"

    yt2 remove //tmp/tm/test_table --proxy freud --force
    yt2 create table //tmp/tm/test_table --proxy freud --attributes '{compression_codec=zlib_6}'
    echo 'a=b' | yt2 write //tmp/tm/test_table --proxy freud --format dsv

    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table", "destination_cluster": "banach"}')
    wait_task $id

    check "a=b" "$(yt2 read //tmp/tm/test_table --proxy banach --format dsv)"
    check '"zlib_6"' "$(yt2 get //tmp/tm/test_table/@compression_codec --proxy banach)"
}

test_intermediate_format() {
    echo "Test intermediate format"

    echo 'a=b' | yt2 write //tmp/tm/test_table --proxy freud --format dsv

    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table", "destination_cluster": "banach", "copy_method": "proxy", "intermediate_format": "<boolean_as_string=false>yson"}')
    wait_task $id

    check "a=b" "$(yt2 read //tmp/tm/test_table --proxy banach --format dsv)"
}

test_delete_tasks() {
    echo "Test delete multiple tasks"

    echo 'a=b' | yt2 write //tmp/tm/test_table --proxy freud --format dsv

    t1=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table1", "destination_cluster": "banach"}')
    t2=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table2", "destination_cluster": "banach"}')

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

    yt2 remove --force '//tmp/tm/test_table' --proxy banach
    gen | yt2 write '<append=true>//tmp/tm/test_table' --proxy banach --format dsv --config '{write_retries={chunk_size=1}}'

    check "$(yt2 get //tmp/tm/test_table/@chunk_count --proxy banach)" "101"
    check "$(yt2 get //tmp/tm/test_table/@row_count --proxy banach)" "101"

    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "banach", "destination_table": "//tmp/tm/new_test_table", "destination_cluster": "freud", "pool": "ignat"}')
    wait_task $id

    check "$(yt2 get //tmp/tm/new_test_table/@row_count --proxy freud)" "101"
}

test_copy_with_annotated_json() {
    echo "Test copy with annotated JSON"

    echo -ne '{"x"=12u}\n' | yt2 write //tmp/tm/test_table --proxy freud --format yson
    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table1", "destination_cluster": "banach", "copy_method": "proxy"}')
    wait_task $id

    check "$(yt2 read //tmp/tm/test_table1 --format '<format=text>yson' --proxy banach)" '{"x"=12u;};'
}

test_kiwi_copy() {
    echo "Test kiwi copy"

    id=$(run_task '{"source_table": "//home/asaitgalin/tm_kiwi_test_table", "source_cluster": "banach", "destination_cluster": "kiwi_apteryx", "kiwi_user": "flux", "copy_spec": {"pool": "ignat", "max_failed_job_count": 1, "scheduling_tag": "fol"}}')
    wait_task $id

    id=$(run_task '{"source_table": "//home/asaitgalin/tm_kiwi_test_table", "source_cluster": "banach", "destination_cluster": "kiwi_apteryx", "kiwi_user": "flux", "table_for_errors": "//tmp/tm/table_for_errors", "copy_spec": {"pool": "ignat", "max_failed_job_count": 1, "scheduling_tag": "fol"}}')
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

    echo 'a=b\n' | yt2 write //tmp/tm/test_table --proxy freud --format dsv

    task_description=$(cat <<EOF
{
    "source_table": "//tmp/tm/test_table",
    "source_cluster": "freud",
    "destination_table": "//tmp/tm/test_table1",
    "destination_cluster": "banach",
    "copy_method": "proxy",
    "destination_erasure_codec": "lrc_12_2_2",
    "force_copy_with_operation": true
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

    yt2 abort-tx $(yt2 get $path/lock/@locks/0/transaction_id --proxy $proxy | tr -d "\"") --proxy $proxy
    bin/transfer-manager-server/transfer-manager-server --config $(realpath $TM_CONFIG) &

    _ensure_transfer_manager_is_running

    check "$(yt2 get //sys/operations/$op/@state --proxy banach | tr -d "\"")" "aborted"
    wait_task $task_id

    echo "Ok"
}

test_schema_copy()
{
    echo "Test schema copy"

    yt2 remove //tmp/tm/test_table --proxy banach --force

    yt2 remove //tmp/tm/test_table --proxy freud --force
    yt2 create table //tmp/tm/test_table --proxy freud --attributes '{schema=[{type=string;name=a}]}'
    echo 'a=1' | yt2 write //tmp/tm/test_table --proxy freud --format dsv

    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table", "destination_cluster": "banach", "copy_method": "proxy"}')
    wait_task $id

    check "a=1" "$(yt2 read //tmp/tm/test_table --proxy banach --format dsv)"
    check '"string"' "$(yt2 get //tmp/tm/test_table/@schema/0/type --proxy banach)"
    check '%true' "$(yt2 get //tmp/tm/test_table/@schema/@strict --proxy banach)"
    check '"strong"' "$(yt2 get //tmp/tm/test_table/@schema_mode --proxy banach)"

    yt2 remove //tmp/tm/test_table --proxy banach --force

    yt2 remove //tmp/tm/test_table --proxy freud --force
    yt2 create table //tmp/tm/test_table --proxy freud --attributes '{schema=[{type=string;name=b}]}'
    echo "b=2" | yt2 write //tmp/tm/test_table --proxy freud --format dsv

    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table", "destination_cluster": "banach", "schema_inference_mode": "from_input", "copy_method": "proxy"}')
    wait_task $id

    check "b=2" "$(yt2 read //tmp/tm/test_table --proxy banach --format dsv)"
    check '"string"' "$(yt2 get //tmp/tm/test_table/@schema/0/type --proxy banach)"
    check '%true' "$(yt2 get //tmp/tm/test_table/@schema/@strict --proxy banach)"
    check '"strong"' "$(yt2 get //tmp/tm/test_table/@schema_mode --proxy banach)"

    yt2 remove //tmp/tm/test_table --proxy banach --force

    yt2 remove //tmp/tm/test_table --proxy freud --force
    echo -ne "a=2\na=1\n" | yt2 write //tmp/tm/test_table --proxy freud --format dsv
    yt2 sort --src //tmp/tm/test_table --dst //tmp/tm/test_table --sort-by a --proxy freud

    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table", "destination_cluster": "banach", "schema_inference_mode": "from_input", "copy_method": "proxy"}')
    wait_task $id

    check '"weak"' "$(yt2 get //tmp/tm/test_table/@schema_mode --proxy banach)"
    check 'a=1\na=2' "$(yt2 read //tmp/tm/test_table --proxy banach --format dsv)"
    check '"ascending"' "$(yt2 get //tmp/tm/test_table/@schema/0/sort_order --proxy banach)"

    yt2 remove //tmp/tm/test_table --proxy freud --force
    yt2 remove //tmp/tm/test_table --proxy banach --force

    yt2 create table //tmp/tm/test_table --proxy banach --attributes '{schema=[{type=string;name=k}]}'
    echo "k=1" | yt2 write //tmp/tm/test_table --proxy banach --format dsv

    echo -ne "a=1\n" | yt2 write //tmp/tm/test_table --proxy freud --format dsv
    yt2 sort --src //tmp/tm/test_table --dst //tmp/tm/test_table --sort-by a --proxy freud

    id=$(run_task '{"source_table": "//tmp/tm/test_table", "source_cluster": "freud", "destination_table": "//tmp/tm/test_table", "destination_cluster": "banach", "schema_inference_mode": "from_input", "copy_method": "proxy"}')
    wait_task_to_fail $id
    yt2 remove //tmp/tm/test_table --proxy banach --force
}

test_pattern_matching() {
    echo "Importing from Banach to Freud (pattern matching test)"
    yt2 remove //tmp/tm/yt_test --force --proxy banach --recursive

    yt2 create table //tmp/tm/yt_test/table1 --proxy banach --ignore-existing --recursive
    yt2 create table //tmp/tm/yt_test/table2 --proxy banach --ignore-existing --recursive
    yt2 create file //tmp/tm/yt_test/file1 --proxy banach --ignore-existing --recursive

    res=$(request "POST" "match/" -d '{"source_pattern": "//tmp/tm/yt_test/{*}", "source_cluster": "banach", "destination_pattern": "//tmp/tm/{*}"}' -f)
    res=$(echo $res | jq sort)
    check \
        '[ [ "//tmp/tm/yt_test/table1", "//tmp/tm/table1" ], [ "//tmp/tm/yt_test/table2", "//tmp/tm/table2" ] ]' \
        "$(echo $res)"

    res=$(request "POST" "match/" -d '{"source_pattern": "//tmp/tm/yt_test/{*}", "source_cluster": "banach", "destination_pattern": "//tmp/tm/{*}", "include_files": "true"}' -f)
    res=$(echo $res | jq sort)
    check \
        '[ [ "//tmp/tm/yt_test/file1", "//tmp/tm/file1" ], [ "//tmp/tm/yt_test/table1", "//tmp/tm/table1" ], [ "//tmp/tm/yt_test/table2", "//tmp/tm/table2" ] ]' \
        "$(echo $res)"
}

cleanup

# Different transfers
test_copy_empty_table
test_copy_empty_file

for flag in true false; do
    FORCE_COPY_WITH_OPERATION=$flag
    test_copy_file
    test_copy_file_attributes
    test_destination_file_codecs
    test_source_file_codecs
    test_various_transfers
    test_copy_table_range
    test_copy_table_range_with_codec
    test_copy_table_attributes
    test_types_preserving_during_copy
    test_destination_codecs
    test_source_codecs
    test_copy_inefficiently_stored_table
    test_intermediate_format
    test_schema_copy
done
FORCE_COPY_WITH_OPERATION=false

test_pattern_matching
test_lease
test_abort_restart_task
test_recursive_path_creation
test_passing_custom_spec
test_clusters_configuration_reloading
test_skip_if_destination_exists
test_mutating_requests_retries
test_delete_tasks
test_copy_with_annotated_json
test_kiwi_copy

# Test it manually since it requires killing TM instance.
#test_abort_operations_on_startup
