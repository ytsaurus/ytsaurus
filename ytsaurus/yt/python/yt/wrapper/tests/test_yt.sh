#!/bin/bash -eux

PIDS=""
YT="$PYTHON_BINARY $YT_CLI_PATH"

add_pid_to_kill() {
    PIDS="$PIDS $1"
}

set_up() {
    $YT create map_node //home/wrapper_test --ignore-existing
}

tear_down() {
    $YT remove //home/wrapper_test --force --recursive

    for pid in $PIDS; do
        if ps ax | awk '{print $1}' | grep $pid; then
            set +e
            kill $pid
            set -e
        fi
    done

    rm -f script.sh
}

die() {
    tear_down
    echo "$@" && exit 1
}

check() {
    local first="$(echo -e "$1")"
    local second="$(echo -e "$2")"
    [ "${first}" = "${second}" ] || die "Test fail $1 does not equal $2"
}

check_failed() {
    set +e
    eval $1
    if [ "$?" = "0" ]; then
        die "Command \"$@\" should fail"
    fi
    set -e
}

run_test() {
    set_up
    eval $1
    tear_down
}

# Directory creation, list, get and set commands
test_cypress_commands()
{
    check "" "$($YT list //home/wrapper_test)"
    check "" "$($YT find //home/wrapper_test --name "xxx")"

    $YT set //home/wrapper_test/folder "{}"
    check "" "$($YT list //home/wrapper_test/folder)"
    check "folder" "$($YT list //home/wrapper_test)"
    check "folder" "$($YT list //home/wrapper_test --read-from cache)"
    check '["folder"]' "$($YT list //home/wrapper_test --format json)"
    check "{\"folder\"={};}" "$($YT get //home/wrapper_test --format "<format=text>yson")"
    check "" "$($YT find //home/wrapper_test --name "xxx")"
    check "//home/wrapper_test/folder" "$($YT find //home/wrapper_test --name "folder")"
    check "//home/wrapper_test/folder" "$($YT find //home/wrapper_test --name "folder" --read-from cache)"
    check "//home/wrapper_test/folder" "$(YT_PREFIX="//home/" $YT find wrapper_test --name "folder")"

    $YT set //home/wrapper_test/folder/@attr '<a=b>c'
    check  '<"a"="b";>"c"' "$($YT get //home/wrapper_test/folder/@attr --format '<format=text>yson')"

    $YT set //home/wrapper_test/folder/@attr '{"attr": 10}' --format json
    check '{"attr":10}' $($YT get //home/wrapper_test/folder/@attr --format json)

    $YT set //home/wrapper_test/other_folder/my_dir '{}' --recursive --force
    check 'true' "$($YT exists //home/wrapper_test/other_folder/my_dir)"

    $YT create file //home/wrapper_test/file_with_attrs --attributes "{testattr=1;other=2}" --ignore-existing
    check "//home/wrapper_test/file_with_attrs" "$($YT find //home/wrapper_test --attribute-filter "testattr=1")"
    check "" "$($YT find //home/wrapper_test --attribute-filter "attr=1")"
}

test_create_account()
{
    $YT create-account parent
    check 'true' "$($YT exists //sys/account_tree/parent)"
    $YT create-account parent -i

    check '0' "$($YT get //sys/account_tree/parent/@resource_limits/node_count)"
    $YT set //sys/accounts/parent/@resource_limits/node_count 10

    $YT create-account --parent-name parent --name child --resource-limits {node_count=10} --allow-children-limit-overcommit
    check 'true' "$($YT exists //sys/account_tree/parent/child)"
    check '10' "$($YT get //sys/account_tree/parent/child/@resource_limits/node_count)"
    check '%true' "$($YT get //sys/account_tree/parent/child/@allow_children_limit_overcommit)"
}

test_create_pool()
{
    $YT create-pool test
    check 'true' "$($YT exists //sys/pool_trees/default/test)"
    $YT create-pool test -i

    $YT create scheduler_pool_tree --attributes {name=yggdrasil}

    $YT create-pool parent yggdrasil --resource-limits '{cpu=10; memory=1000000000}' --min-share-resources {cpu=5}
    check 'true' "$($YT exists //sys/pool_trees/yggdrasil/parent)"
    check '10' "$($YT get //sys/pool_trees/yggdrasil/parent/@resource_limits/cpu)"
    check '1000000000' "$($YT get //sys/pool_trees/yggdrasil/parent/@resource_limits/memory)"
    check '5' "$($YT get //sys/pool_trees/yggdrasil/parent/@min_share_resources/cpu)"

    $YT create-pool --parent-name parent --pool-tree yggdrasil --name fair-share-child --weight 3.14 \
    --create-ephemeral-subpools --max-operation-count 10 --max-running-operation-count 5 --attributes {attr=value}
    check 'true' "$($YT exists //sys/pool_trees/yggdrasil/parent/fair-share-child)"
    check '3.14' "$($YT get //sys/pool_trees/yggdrasil/parent/fair-share-child/@weight)"
    check '10' "$($YT get //sys/pool_trees/yggdrasil/parent/fair-share-child/@max_operation_count)"
    check '5' "$($YT get //sys/pool_trees/yggdrasil/parent/fair-share-child/@max_running_operation_count)"
    check '"value"' "$($YT get //sys/pool_trees/yggdrasil/parent/fair-share-child/@attr)"
    check '%true' "$($YT get //sys/pool_trees/yggdrasil/parent/fair-share-child/@create_ephemeral_subpools)"

    $YT create-pool fifo-child --parent-name parent --pool-tree yggdrasil --mode fifo --fifo-sort-parameters [pending_job_count] \
    --ephemeral-subpool-config {max_operation_count=10} --forbid-immediate-operations
    check '"fifo"' "$($YT get //sys/pool_trees/yggdrasil/parent/fifo-child/@mode)"
    check '"pending_job_count"' "$($YT get //sys/pool_trees/yggdrasil/parent/fifo-child/@fifo_sort_parameters/0)"
    check 'false' "$($YT exists //sys/pool_trees/yggdrasil/parent/fifo-child/@fifo_sort_parameters/1)"
    check '%true' "$($YT get //sys/pool_trees/yggdrasil/parent/fifo-child/@forbid_immediate_operations)"
    check '10' "$($YT get //sys/pool_trees/yggdrasil/parent/fifo-child/@ephemeral_subpool_config/max_operation_count)"

    $YT remove //sys/pool_trees/yggdrasil --force --recursive
}

test_list_long_format()
{
    $YT list -l "//home"

    # list with symlinks
    $YT create table "//home/wrapper_test/folder_with_symlinks/test_table" --recursive
    $YT link "//home/wrapper_test/folder_with_symlinks/test_table" "//home/wrapper_test/folder_with_symlinks/valid_link"
    $YT create table "//home/wrapper_test/table_to_delete"
    $YT link "//home/wrapper_test/table_to_delete" "//home/wrapper_test/folder_with_symlinks/invalid_link"
    $YT remove "//home/wrapper_test/table_to_delete"
    $YT list -l "//home/wrapper_test/folder_with_symlinks"
}

test_concatenate()
{
    echo "Hello" | $YT write-file //home/wrapper_test/file_a
    echo "World" | $YT write-file //home/wrapper_test/file_b

    $YT concatenate --src //home/wrapper_test/file_a --src //home/wrapper_test/file_b --dst //home/wrapper_test/output_file

    check "$(echo -e "Hello\nWorld")" "$($YT read-file //home/wrapper_test/output_file)"
}

# read and write table
test_table_commands()
{
    $YT create table //home/wrapper_test/test_table
    check "" "$($YT read //home/wrapper_test/test_table --format dsv)"

    echo -e "value=y\nvalue=x\n" | $YT write //home/wrapper_test/test_table --format dsv
    check "$(echo -e "value=y\nvalue=x\n")" "$($YT read //home/wrapper_test/test_table --format dsv)"
}

# download and upload file, use it in map operation
test_file_commands()
{
    echo "grep x" >script
    chmod +x script

    cat script | $YT upload //home/wrapper_test/script --executable

    check "grep x" "$($YT download //home/wrapper_test/script)"

    echo -e "value=y\nvalue=x\n" | $YT write //home/wrapper_test/input_table --format dsv

    $YT map "./script" --src //home/wrapper_test/input_table --dst //home/wrapper_test/output_table \
        --file //home/wrapper_test/script --format dsv
    check "value=x\n" "$($YT read //home/wrapper_test/output_table --format dsv)"

    $YT map "./script" --src //home/wrapper_test/input_table --dst //home/wrapper_test/output_table \
        --local-file script --format dsv
    check "value=x\n" "$($YT read //home/wrapper_test/output_table --format dsv)"

    rm -f script
}

test_copy_move_link()
{
    $YT create table //home/wrapper_test/table
    check "table" "$($YT list //home/wrapper_test)"

    $YT copy //home/wrapper_test/table //home/wrapper_test/other_table
    check $'other_table\ntable' "$($YT list //home/wrapper_test | sort)"

    $YT remove //home/wrapper_test/table
    check "other_table" "$($YT list //home/wrapper_test)"

    $YT move //home/wrapper_test/other_table //home/wrapper_test/table
    check "table" "$($YT list //home/wrapper_test)"

    $YT link //home/wrapper_test/table //home/wrapper_test/other_table
    check $'other_table\ntable' "$($YT list //home/wrapper_test | sort)"

    $YT remove //home/wrapper_test/table
    check_failed "$YT read //home/wrapper_test/other_table --format dsv"
    $YT remove //home/wrapper_test/other_table

    $YT create account --attributes '{name=test}'
    $YT set //sys/accounts/test/@resource_limits/master_memory/total 1000000
    $YT set //sys/accounts/test/@resource_limits/master_memory/chunk_host 1000000
    $YT set //sys/accounts/test/@resource_limits/node_count 1000
    $YT set //sys/accounts/test/@resource_limits/chunk_count 100000
    $YT set //sys/accounts/test/@resource_limits/disk_space_per_medium/default 1000000000
    $YT create table //home/wrapper_test/table --attributes '{account=test}'

    $YT copy //home/wrapper_test/table //home/wrapper_test/other_table
    check '"sys"' "$($YT get //home/wrapper_test/other_table/@account)"
    $YT remove //home/wrapper_test/other_table

    $YT copy //home/wrapper_test/table //home/wrapper_test/other_table --preserve-account
    check '"test"' "$($YT get //home/wrapper_test/other_table/@account)"
    $YT remove //home/wrapper_test/other_table

    $YT move //home/wrapper_test/table //home/wrapper_test/other_table --preserve-account
    check '"test"' "$($YT get //home/wrapper_test/other_table/@account)"
    $YT remove //home/wrapper_test/other_table

    $YT create table //home/wrapper_test/table --attributes '{account=test}'
    $YT move //home/wrapper_test/table //home/wrapper_test/other_table
    check '"sys"' "$($YT get //home/wrapper_test/other_table/@account)"
    $YT remove //home/wrapper_test/other_table

    $YT create table //home/wrapper_test/table --attributes '{expiration_time="2050-01-01T12:00:00.000000Z"}'
    $YT move //home/wrapper_test/table //home/wrapper_test/other_table --preserve-expiration-time
    check 'true' "$($YT exists //home/wrapper_test/other_table/@expiration_time)"
    $YT remove //home/wrapper_test/other_table

    $YT create table //home/wrapper_test/table --attributes '{expiration_time="2050-01-01T12:00:00.000000Z"}'
    $YT copy //home/wrapper_test/table //home/wrapper_test/other_table
    check 'false' "$($YT exists //home/wrapper_test/other_table/@expiration_time)"
}

test_merge_erase()
{
    for i in {1..3}; do
        echo -e "value=${i}" | $YT write "//home/wrapper_test/table${i}" --format dsv
    done
    $YT merge --src //home/wrapper_test/table{1..3} --dst "//home/wrapper_test/merge"
    check "3" "$($YT get //home/wrapper_test/merge/@row_count)"

    $YT erase '//home/wrapper_test/merge[#1:#2]'
    check "2" "$($YT get //home/wrapper_test/merge/@row_count)"

    $YT merge --src "//home/wrapper_test/merge" --src "//home/wrapper_test/merge" --dst "//home/wrapper_test/merge"
    check "4" "$($YT get //home/wrapper_test/merge/@row_count)"
}

test_map_reduce()
{
    export YT_TABULAR_DATA_FORMAT="dsv"
    echo -e "value=1\nvalue=2" | $YT write //home/wrapper_test/input_table
    check "2" "$($YT get //home/wrapper_test/input_table/@row_count)"

    $YT map-reduce --mapper cat --reducer "grep 2" --src //home/wrapper_test/input_table --dst //home/wrapper_test/input_table --reduce-by value
    check "1" "$($YT get //home/wrapper_test/input_table/@row_count)"
    unset YT_TABULAR_DATA_FORMAT
}

test_users()
{
    $YT create user --attribute '{name=test_user}'
    $YT create group --attribute '{name=test_group}'

    check "[]" "$($YT get //sys/groups/test_group/@members --format '<format=text>yson')"

    $YT add-member test_user test_group
    check  '["test_user";]' "$($YT get //sys/groups/test_group/@members --format '<format=text>yson')"

    $YT set "//home/wrapper_test/@acl/end" "{action=allow;subjects=[test_group];permissions=[write]}"
    $YT check-permission test_user write "//home/wrapper_test" | grep allow

    $YT remove-member test_user test_group
    check "[]" "$($YT get //sys/groups/test_group/@members --format '<format=text>yson')"

    $YT remove //sys/users/test_user
}

#TODO(ignat): move this test to python
test_concurrent_upload_in_operation()
{
    echo "cat" > script.sh
    chmod +x script.sh

    echo "x=y" | $YT write //home/wrapper_test/table --format dsv

    $YT map "cat" --src "//home/wrapper_test/table" --dst "//home/wrapper_test/out1" --format dsv --local-file script.sh &
    add_pid_to_kill "$!"
    $YT map "cat" --src "//home/wrapper_test/table" --dst "//home/wrapper_test/out2" --format dsv --local-file script.sh &
    add_pid_to_kill "$!"

    ok=0
    for i in {1..10}; do
        check=1
        for out_index in {1..2}; do
            if [ $($YT exists "//home/wrapper_test/out${out_index}") = "false" ]; then
                check=0
                break
            fi
            content=$($YT read "//home/wrapper_test/out${out_index}" --format dsv)
            if [ "$content" != "x=y" ]; then
                check=0
                break
            fi
        done

        if [ "$check" = "0" ]; then
            sleep 2
        else
            ok=1
            break
        fi
    done
    check "$ok" "1"
}

test_sorted_by()
{
    echo "x=y" | $YT write "<sorted-by=[x]>//home/wrapper_test/table" --format dsv
    echo "x=z" | $YT write "<sorted_by=[x]>//home/wrapper_test/table" --format dsv
    check "$($YT get //home/wrapper_test/table/@sorted)" "$TRUE"
}

test_transactions()
{
    local tx=$($YT start-tx)
    $YT abort-tx "$tx"

    tx=$($YT start-tx)
    $YT commit-tx "$tx"
}

test_hybrid_arguments()
{
    $YT create table //home/wrapper_test/hybrid_test

    $YT copy //home/wrapper_test/hybrid_test --destination-path //home/wrapper_test/hybrid_copy
    check "$($YT exists --path //home/wrapper_test/hybrid_copy)" "true"

    $YT copy --destination-path //home/wrapper_test/hybrid_copy2 --source-path //home/wrapper_test/hybrid_copy
    check "$($YT exists --path //home/wrapper_test/hybrid_copy2)" "true"

    $YT move //home/wrapper_test/hybrid_test --destination-path //home/wrapper_test/hybrid_moved
    check "$($YT exists //home/wrapper_test/hybrid_moved)" "true"

    $YT move --destination-path //home/wrapper_test/hybrid_test --source-path //home/wrapper_test/hybrid_moved
    check "$($YT exists //home/wrapper_test/hybrid_test)" "true"

    $YT link --link-path //home/wrapper_test/hybrid_link --target-path //home/wrapper_test/hybrid_test

    $YT remove --path //home/wrapper_test/hybrid_test
    check_failed "$YT read //home/wrapper_test/hybrid_link --format dsv"

    $YT create map_node //home/wrapper_test/test_dir
    $YT create --type map_node --path //home/wrapper_test/test_dir2
    check "$($YT list --path //home/wrapper_test/test_dir)" ""
    check "$($YT find --path //home/wrapper_test/test_dir --type file)" ""

    echo -ne "a\tb\n" | $YT write --table //home/wrapper_test/yamr_table --format "yamr"
    check "$($YT read --table //home/wrapper_test/yamr_table --format yamr)" "a\tb\n"

    echo -ne "abcdef" | $YT write-file --destination //home/wrapper_test/test_file
    check "$($YT read-file --path //home/wrapper_test/test_file)" "abcdef"

    TX=$($YT start-tx --timeout 10000)
    $YT lock --path //home/wrapper_test/test_file --tx $TX
    check_failed "$YT remove --path //home/wrapper_test/test_file"
    $YT ping-tx --transaction $TX
    $YT abort-tx --transaction $TX

    $YT check-permission root write //home/wrapper_test
    $YT check-permission --user root --permission write --path //home/wrapper_test

    $YT set --path //home/wrapper_test/value --value "def"
    check "$($YT get --path //home/wrapper_test/value)" "\"def\""

    $YT set //home/wrapper_test/value "abc"
    check "$($YT get --path //home/wrapper_test/value)" "\"abc\""

    echo -ne "with_pipe" | $YT set //home/wrapper_test/value
    check "$($YT get --path //home/wrapper_test/value)" "\"with_pipe\""
}

test_async_operations() {
    export YT_TABULAR_DATA_FORMAT="dsv"
    echo -e "x=1\n" | $YT write //home/wrapper_test/input_table
    map_op=$($YT map "tr 1 2" --src //home/wrapper_test/input_table --dst //home/wrapper_test/map_output --async)

    sort_op=$($YT sort --src //home/wrapper_test/input_table --dst //home/wrapper_test/sort_output --sort-by "x" --async)
    $YT track-op $sort_op

    reduce_op=$($YT reduce "cat" \
                --src //home/wrapper_test/sort_output \
                --dst //home/wrapper_test/reduce_output \
                --sort-by "x" \
                --reduce-by "x" \
                --async)
    $YT track-op $reduce_op

    op=$($YT map-reduce \
         --mapper "cat" \
         --reducer "cat" \
         --src //home/wrapper_test/sort_output \
         --dst //home/wrapper_test/map_reduce_output \
         --reduce-by "x" \
         --async)
    $YT track-op $op

    $YT track-op $map_op
    check "x=2\n" "$($YT read //home/wrapper_test/map_output)"

    unset YT_TABULAR_DATA_FORMAT
}

test_json_structured_format() {
    export YT_STRUCTURED_DATA_FORMAT="json"

    $YT set //home/wrapper_test/folder "{}"
    check "$(echo -e "{\n    \"folder\": {\n\n    }\n}\n\n")" "$($YT get //home/wrapper_test)"

    $YT set //home/wrapper_test/folder/@attr '{"test": "value"}'
    check  "$(echo -e "{\n    \"test\": \"value\"\n}\n\n")" "$($YT get //home/wrapper_test/folder/@attr)"

    unset YT_STRUCTURED_FORMAT
}

test_transform()
{
    export YT_TABULAR_DATA_FORMAT="dsv"
    echo -e "k=v\n" | $YT write //home/wrapper_test/table_to_transform
    $YT transform //home/wrapper_test/table_to_transform

    $YT transform //home/wrapper_test/table_to_transform --compression-codec zlib_6
    check '"zlib_6"' "$($YT get //home/wrapper_test/table_to_transform/@compression_codec)"

    $YT transform //home/wrapper_test/table_to_transform --compression-codec zlib_6 --check-codecs

    $YT transform //home/wrapper_test/table_to_transform //home/wrapper_test/other_table --compression-codec zlib_6
    check '"zlib_6"' "$($YT get //home/wrapper_test/other_table/@compression_codec)"

    $YT transform //home/wrapper_test/table_to_transform //home/wrapper_test/other_table --optimize-for scan
    check '"scan"' "$($YT get //home/wrapper_test/other_table/@optimize_for)"

    $YT create table //home/wrapper_test/empty_table_to_transform
    $YT transform //home/wrapper_test/empty_table_to_transform --compression-codec brotli_8 --optimize-for scan
    check '"scan"' "$($YT get //home/wrapper_test/empty_table_to_transform/@optimize_for)"
    check '"brotli_8"' "$($YT get //home/wrapper_test/empty_table_to_transform/@compression_codec)"

    unset YT_TABULAR_DATA_FORMAT
}

test_create_temp_table()
{
    local table="$($YT create-temp-table)"
    check "$($YT exists "$table")" "true"

    local table="$($YT create-temp-table --attributes '{test_attribute=a}')"
    check "$($YT get "$table/@test_attribute")" '"a"'

    $YT create map_node //home/wrapper_test/temp_tables
    local table="$($YT create-temp-table --path //home/wrapper_test/temp_tables --name-prefix check)"
    if [[ ! $table =~ //home/wrapper_test/temp_tables/check* ]]; then
        die "test_create_temp_table: table has invalid full path"
    fi

    local table="$($YT create-temp-table --expiration-timeout 1000)"
    sleep 2
    check "$($YT exists "$table")" "false"
}

test_dynamic_table_commands()
{
    local tablet_cell="$($YT create tablet_cell --attributes "{size=1}")"

    local schema="[{name=x; type=string; sort_order=ascending};{name=y; type=int64}]"
    local table="//home/wrapper_test/dyn_table"
    $YT create table "$table" --attributes "{schema=$schema; dynamic=%true}"

    while true; do
        if [ "$($YT get //sys/tablet_cells/${tablet_cell}/@health)" = '"good"' ]; then
            break
        fi
        sleep 0.1
    done

    $YT mount-table "$table" --sync

    echo -ne "{x=a; y=1};{x=b;y=2}" | $YT insert-rows "$table" --format "<format=text>yson"
    echo -ne "{x=a}" | $YT delete-rows "$table" --format "<format=text>yson"

    check '{"x"="b"}' "$($YT select-rows "x FROM [$table]" --format "<format=text>yson" | tr -d ";\n")"

    $YT select-rows "x FROM [$table]" --print-statistics --format "<format=text>yson"

    $YT unmount-table "$table" --sync
}

test_atomicity_argument()
{
    local tablet_cell="$($YT create tablet_cell --attributes "{size=1}")"

    local schema="[{name=x; type=string; sort_order=ascending};{name=y; type=int64}]"
    local table="//home/wrapper_test/dyn_table"
    $YT create table "$table" --attributes "{schema=$schema; dynamic=%true; atomicity=none}"

    while true; do
        if [ "$($YT get //sys/tablet_cells/${tablet_cell}/@health)" = '"good"' ]; then
            break
        fi
        sleep 0.1
    done

    $YT mount-table "$table" --sync

    echo -ne "{x=a; y=1};{x=b;y=2}" | $YT insert-rows "$table" --format "<format=text>yson" --atomicity "none"
    echo -ne "{x=a}" | $YT delete-rows "$table" --format "<format=text>yson" --atomicity "none"

    $YT unmount-table "$table" --sync
}

test_sandbox_file_name_specification()
{
    local table="//home/wrapper_test/table"
    echo -ne "a=b\n" | $YT write "$table" --format "dsv"

    echo "content" >script

    $YT map "ls some_file >/dev/null && cat" \
        --src "$table" \
        --dst "$table" \
        --local-file "<file_name=some_file>script" \
        --format dsv
}

test_execute()
{
    local table_path="//home/wrapper_test/test_table"
    $YT execute create '{type=table;path="'"$table_path"'";output_format=yson}'
    check 'true' $($YT execute exists '{path="'"$table_path"'";output_format=json}' | python -c "import json, sys; json.dump(json.load(sys.stdin)['value'], sys.stdout)")
    $YT execute remove '{path="'"$table_path"'"}'
    check 'false' $($YT execute exists '{path="'"$table_path"'";output_format=json}' | python -c "import json, sys; json.dump(json.load(sys.stdin)['value'], sys.stdout)")
}

test_brotli_write()
{
    local table_path="//home/wrapper_test/test_table"
    echo -ne "x=1\nx=2\nx=3\nx=4\n" | $YT write "$table_path" --format dsv --config '{proxy={content_encoding=br}}'
    check "x=1\nx=2\nx=3\nx=4\n" "$($YT read "$table_path" --format dsv | sort)"
}

test_vanilla_operations()
{
    $YT vanilla --tasks '{sample={command="echo AAA >&2";job_count=1}}'
    op_id=$($YT vanilla --tasks '{sample={command="echo AAA >&2";job_count=1}}' --async)
    $YT get-operation "$op_id"
}

test_ping_ancestor_transactions_in_operations()
{
    TX=$($YT start-tx --timeout 5000)
    check_failed "$YT vanilla --tasks '"'{sample={command="sleep 6";job_count=1}}'"' --tx $TX"

    TX=$($YT start-tx --timeout 10000)
    $YT vanilla --tasks '{sample={command="sleep 12";job_count=1}}' --tx $TX --ping-ancestor-txs
}

test_operation_and_job_commands()
{
    export YT_TABULAR_DATA_FORMAT="dsv"
    echo -e "x=1\n" | $YT write //home/wrapper_test/input_table
    map_op=$($YT map 'echo "Well hello there" >&2 && tr 1 2' --src //home/wrapper_test/input_table --dst //home/wrapper_test/map_output --async)
    $YT track-op $map_op

    get_operation_res="$($YT get-operation $map_op --attribute state --attribute authenticated_user --format json)"
    state=$(echo "$get_operation_res" | $PYTHON_BINARY -c 'import sys, json; sys.stdout.write(json.load(sys.stdin)["state"])')
    user=$(echo "$get_operation_res" | $PYTHON_BINARY -c 'import sys, json; sys.stdout.write(json.load(sys.stdin)["authenticated_user"])')
    check "$state" "completed"
    check "$user" "root"
    echo "$get_operation_res" | $PYTHON_BINARY -c 'import sys, json; assert len(json.load(sys.stdin)) == 2'

    list_operations_res="$($YT list-operations --format json)"
    echo "$list_operations_res" | \
        $PYTHON_BINARY -c 'import sys, json; assert '"\"$map_op\""' in [d["id"] for d in json.load(sys.stdin)["operations"]]'

    list_operations_res_ts="$($YT list-operations --to-time 1 --format json)"
    echo "$list_operations_res_ts" | \
        $PYTHON_BINARY -c 'import sys, json; assert '"\"$map_op\""' not in [d["id"] for d in json.load(sys.stdin)["operations"]]'

    list_jobs_res="$($YT list-jobs $map_op --format json)"
    job_id="$(echo "$list_jobs_res" | $PYTHON_BINARY -c 'import sys, json; sys.stdout.write(json.load(sys.stdin)["jobs"][0]["id"])')"

    success="0"
    set +e
    for i in {1..10}; do
        get_job_res="$($YT get-job $job_id $map_op --format json)"
        if [ $? = "0" ]; then
            job_state=$(echo "$get_job_res" | $PYTHON_BINARY -c 'import sys, json; sys.stdout.write(json.load(sys.stdin)["state"])')
            check "$job_state" "completed"
            success="1"
            break
        fi
        sleep 1
    done
    set -e

    check $success "1"
    unset YT_TABULAR_DATA_FORMAT
}

test_check_permissions()
{
    $YT create user --attribute '{name=test_user}'

    local table="//home/wrapper_test/table"
    $YT create table "$table" --attributes '{schema=[{name=a;type=string;};{name=b;type=string;}];acl=[{action=allow;subjects=[test_user];permissions=[read]}]}'
    echo -ne "a=10\tb=20\n" | $YT write-table "$table" --format "dsv"

    $YT check-permission test_user read "//home/wrapper_test/table" | grep allow
    $YT check-permission test_user read "//home/wrapper_test/table" --columns '[a]' | grep allow
}

test_transfer_account_resources()
{
    $YT create account --attributes '{name=a1;resource_limits={node_count=10}}'
    $YT create account --attributes '{name=a2;resource_limits={node_count=12}}'

    $YT transfer-account-resources a1 a2 --resource-delta '{node_count=3}'
    check "7" "$($YT get //sys/accounts/a1/@resource_limits/node_count)"
    check "15" "$($YT get //sys/accounts/a2/@resource_limits/node_count)"

    $YT transfer-account-resources --src a1 --dst a2 --resource-delta '{node_count=3}'
    check "4" "$($YT get //sys/accounts/a1/@resource_limits/node_count)"
    check "18" "$($YT get //sys/accounts/a2/@resource_limits/node_count)"

    $YT transfer-account-resources --destination-account a1 --source-account a2 --resource-delta '{node_count=6}'
    check "10" "$($YT get //sys/accounts/a1/@resource_limits/node_count)"
    check "12" "$($YT get //sys/accounts/a2/@resource_limits/node_count)"
}

test_transfer_pool_resources()
{
    $YT create scheduler_pool --attributes '{name=from;pool_tree=default;strong_guarantee_resources={cpu=10};integral_guarantees={resource_flow={cpu=20};burst_guarantee_resources={cpu=30}};max_running_operation_count=40;max_operation_count=50}'
    $YT create scheduler_pool --attributes '{name=to;pool_tree=default;strong_guarantee_resources={cpu=10};integral_guarantees={resource_flow={cpu=20};burst_guarantee_resources={cpu=30}};max_running_operation_count=40;max_operation_count=50}'

    $YT transfer-pool-resources --src from --dst to --pool-tree default --resource-delta '{strong_guarantee_resources={cpu=4};resource_flow={cpu=8};burst_guarantee_resources={cpu=12};max_running_operation_count=16;max_operation_count=20}'

    check "6.0" "$($YT get //sys/pool_trees/default/from/@strong_guarantee_resources/cpu)"
    check "12.0" "$($YT get //sys/pool_trees/default/from/@integral_guarantees/resource_flow/cpu)"
    check "18.0" "$($YT get //sys/pool_trees/default/from/@integral_guarantees/burst_guarantee_resources/cpu)"
    check "24" "$($YT get //sys/pool_trees/default/from/@max_running_operation_count)"
    check "30" "$($YT get //sys/pool_trees/default/from/@max_operation_count)"

    check "14.0" "$($YT get //sys/pool_trees/default/to/@strong_guarantee_resources/cpu)"
    check "28.0" "$($YT get //sys/pool_trees/default/to/@integral_guarantees/resource_flow/cpu)"
    check "42.0" "$($YT get //sys/pool_trees/default/to/@integral_guarantees/burst_guarantee_resources/cpu)"
    check "56" "$($YT get //sys/pool_trees/default/to/@max_running_operation_count)"
    check "70" "$($YT get //sys/pool_trees/default/to/@max_operation_count)"
}

test_generate_timestamp()
{
    ts=$($YT generate-timestamp)
    echo $ts | grep '^[0-9]\+$' -q || die "generated timestamp should be integer, got '$ts'"
    [ $ts -gt 0 ] || die "generated timestamp should be positive, got '$ts'"
}

test_sort_order()
{
    local in_table="//home/wrapper_test/in_table_to_sort"
    local out_table="//home/wrapper_test/out_table_to_sort"

    $YT create table "$in_table" --attributes '{schema=[{name=a;type=int64;};{name=b;type=int64;};{name=c;type=int64;}];}'
    {
        echo -ne "a=2\tb=1\tc=3\n"
        echo -ne "a=1\tb=2\tc=2\n"
        echo -ne "a=2\tb=2\tc=4\n"
        echo -ne "a=1\tb=1\tc=1\n"
    } | $YT write-table "$in_table" --format "<enable_string_to_all_conversion=%true>dsv"

    $YT sort --src "$in_table" --dst "$out_table" --sort-by c
    check "c=1\nc=2\nc=3\nc=4\n" "$($YT read-table "$out_table" --format dsv | cut -f 1)"

    $YT sort --src "$in_table" --dst "$out_table" --sort-by "{name=c; sort_order=descending;}"
    check "c=4\nc=3\nc=2\nc=1\n" "$($YT read-table "$out_table" --format dsv | cut -f 1)"

    $YT sort --src "$in_table" --dst "$out_table" --sort-by "{name=a; sort_order=descending;}" --sort-by b
    check "c=3\nc=4\nc=1\nc=2\n" "$($YT read-table "$out_table" --format dsv | cut -f 3)"

    $YT sort --src "$in_table" --dst "$out_table" --sort-by b --sort-by a
    check "c=1\nc=3\nc=2\nc=4\n" "$($YT read-table "$out_table" --format dsv | cut -f 3)"

    $YT sort --src "$in_table" --dst "$out_table" --sort-by b --sort-by "{name=a; sort_order=descending;}"
    check "c=3\nc=1\nc=4\nc=2\n" "$($YT read-table "$out_table" --format dsv | cut -f 3)"
}

test_dirtable()
{
    SOURCE_DIR="tmp_source_files"
    DOWNLOAD_DIR="tmp_download_files"
    # patch for py2 yt/python/yt/wrapper/framing.py assert isinstance(size, int)
    YT_CONFIG_PATCHES=$(echo $YT_CONFIG_PATCHES | sed 's/"read_buffer_size"=[0-9]*;//')

    rm -rf ${SOURCE_DIR}
    rm -rf ${DOWNLOAD_DIR}
    mkdir ${SOURCE_DIR}
    
    # cat /dev/random | fold -w 1024 | head -512000 > ${SOURCE_DIR}/random_file1
    python -c "open('${SOURCE_DIR}/random_file1','wb').truncate(1024*1024*10)"
    mkdir ${SOURCE_DIR}/data
    # cat /dev/random | fold -w 1024 | head -512000 > ${SOURCE_DIR}/data/random_file2
    python -c "open('${SOURCE_DIR}/data/random_file2','wb').truncate(1024*1024*20)"
    $YT dirtable upload --directory ${SOURCE_DIR} --yt-table //home/wrapper_test/dirtable
    mkdir ${DOWNLOAD_DIR}
    $YT dirtable download --directory ${DOWNLOAD_DIR} --yt-table //home/wrapper_test/dirtable
    check "" "$(diff -qr ${SOURCE_DIR} ${DOWNLOAD_DIR})"
    rm -rf ${DOWNLOAD_DIR}/*

    # cat /dev/random | fold -w 1024 | head -512000 > ${SOURCE_DIR}/i_am_just_appended_random_file
    python -c "open('${SOURCE_DIR}/i_am_just_appended_random_file','wb').truncate(1024*1024*30)"
    $YT dirtable append-single-file --yt-name i_am_just_appended_random_file --fs-path ${SOURCE_DIR}/i_am_just_appended_random_file --yt-table //home/wrapper_test/dirtable
    $YT dirtable download --directory ${DOWNLOAD_DIR} --yt-table //home/wrapper_test/dirtable
    check "" "$(diff -qr ${SOURCE_DIR} ${DOWNLOAD_DIR})"

    rm -rf ${SOURCE_DIR}
    rm -rf ${DOWNLOAD_DIR}
}

tear_down
run_test test_cypress_commands
run_test test_list_long_format
run_test test_concatenate
run_test test_table_commands
run_test test_file_commands
run_test test_copy_move_link
run_test test_merge_erase
run_test test_map_reduce
run_test test_create_account
run_test test_create_pool
run_test test_users
run_test test_concurrent_upload_in_operation
run_test test_sorted_by
run_test test_transactions
run_test test_hybrid_arguments
run_test test_async_operations
run_test test_json_structured_format
run_test test_transform
run_test test_create_temp_table
run_test test_dynamic_table_commands
run_test test_atomicity_argument
run_test test_sandbox_file_name_specification
run_test test_execute
run_test test_brotli_write
run_test test_vanilla_operations
run_test test_ping_ancestor_transactions_in_operations
run_test test_operation_and_job_commands
run_test test_check_permissions
run_test test_transfer_account_resources
run_test test_transfer_pool_resources
run_test test_sort_order
run_test test_dirtable
