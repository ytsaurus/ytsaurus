#!/bin/bash -eux

cd $(dirname "${BASH_SOURCE[0]}")

set_up() {
    ./yt create map_node //home/wrapper_test --ignore-existing
}

tear_down() {
    ./yt remove //home/wrapper_test --force --recursive

    for pid in `jobs -p`; do
        if ps ax | awk '{print $1}' | grep $pid; then
            kill $pid
        fi
    done

    rm -f script.sh
}

die() {
    tear_down
    echo "$@" && exit 1
}

check() {
    local first="`echo -e "$1"`"
    local second="`echo -e "$2"`"
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
test_tree_commands()
{
    check "" "`yt list //home/wrapper_test`"
    check "" "`yt find //home/wrapper_test --name "xxx"`"

    ./yt set //home/wrapper_test/folder {}
    check "" "`yt list //home/wrapper_test/folder`"
    check "folder" "`yt list //home/wrapper_test`"
    check "{\"folder\"={}}" "`yt get //home/wrapper_test --format "<format=text>yson"`"
    check "" "`yt find //home/wrapper_test --name "xxx"`"
    check "//home/wrapper_test/folder" "`yt find //home/wrapper_test --name "folder"`"

    ./yt set //home/wrapper_test/folder/@attr '<a=b>c'
    check '<"a"="b">"c"' `./yt get //home/wrapper_test/folder/@attr --format '<format=text>yson'`
}

# download and upload file, use it in map operation
test_file_commands()
{
    echo "grep x" >script
    chmod +x script

    cat script | ./yt upload //home/wrapper_test/script --executable

    check "grep x" "`yt download //home/wrapper_test/script`"

    echo -e "value=y\nvalue=x\n" | ./yt write //home/wrapper_test/input_table --format dsv

    ./yt map "./script" --src //home/wrapper_test/input_table --dst //home/wrapper_test/output_table \
        --file //home/wrapper_test/script --format dsv
    check "value=x\n" "`yt read //home/wrapper_test/output_table --format dsv`"

    ./yt map "./script" --src //home/wrapper_test/input_table --dst //home/wrapper_test/output_table \
        --local-file ./script --format dsv
    check "value=x\n" "`yt read //home/wrapper_test/output_table --format dsv`"

    rm -f script
}

test_copy_move_link()
{
    ./yt create table //home/wrapper_test/table
    check "table" "`yt list //home/wrapper_test`"

    ./yt copy //home/wrapper_test/table //home/wrapper_test/other_table
    check $'other_table\ntable' "`yt list //home/wrapper_test | sort`"

    ./yt remove //home/wrapper_test/table
    check "other_table" "`yt list //home/wrapper_test`"

    ./yt move //home/wrapper_test/other_table //home/wrapper_test/table
    check "table" "`yt list //home/wrapper_test`"

    ./yt link //home/wrapper_test/table //home/wrapper_test/other_table
    check $'other_table\ntable' "`yt list //home/wrapper_test | sort`"

    ./yt remove //home/wrapper_test/table
    check_failed "yt read //home/wrapper_test/other_table --format dsv"
}

test_merge_erase()
{
    for i in {1..3}; do
        echo -e "value=${i}\n" | ./yt write "//home/wrapper_test/table${i}"
    done
    ./yt merge --src "//home/wrapper_test/table1" --src "//home/wrapper_test/table3" --dst "//home/wrapper_test/merge"
}

test_map_reduce()
{
    export YT_TABULAR_DATA_FORMAT="dsv"
    ./yt write //home/wrapper_test/input_table < <(echo -e "value=1\nvalue=2")
    check "2" `./yt get //home/wrapper_test/input_table/@row_count`

    ./yt map-reduce --mapper cat --reducer "grep 2" --src //home/wrapper_test/input_table --dst //home/wrapper_test/input_table --reduce-by value
    check "1" `./yt get //home/wrapper_test/input_table/@row_count`
}

test_users()
{
    ./yt create user --attribute '{name=test_user}'
    ./yt create group --attribute '{name=test_group}'

    check "[]" `./yt get //sys/groups/test_group/@members --format '<format=text>yson'`

    ./yt add-member test_user test_group
    check '["test_user"]' `./yt get //sys/groups/test_group/@members --format '<format=text>yson'`

    ./yt set "//home/wrapper_test/@acl/end" "{action=allow;subjects=[test_group];permissions=[write]}"
    ./yt check-permission test_user write "//home/wrapper_test" | grep allow

    ./yt remove-member test_user test_group
    check "[]" `./yt get //sys/groups/test_group/@members --format '<format=text>yson'`

    ./yt remove //sys/users/test_user
}

#TODO(ignat): move this test to python
test_concurrent_upload_in_operation()
{
    echo "cat" > script.sh
    chmod +x script.sh

    echo "x=y" | ./yt write //home/wrapper_test/table --format dsv

    ./yt map "cat" --src "//home/wrapper_test/table" --dst "//home/wrapper_test/out1" --format dsv --local-file script.sh &
    ./yt map "cat" --src "//home/wrapper_test/table" --dst "//home/wrapper_test/out2" --format dsv --local-file script.sh &

    ok=0
    for i in {1..10}; do
        check=1
        for out_index in {1..2}; do
            if [ $(./yt exists "//home/wrapper_test/out${out_index}") = "false" ]; then
                check=0
                break
            fi
            content=$(./yt read "//home/wrapper_test/out${out_index}" --format dsv)
            if [ "$content" != "x=y" ]; then
                check=0
                break
            fi
        done

        if [ "$check" = "0" ]; then
            sleep 1
        else
            ok=1
        fi
    done
    check "$ok" "1"
}

test_sorted_by()
{
    echo "x=y" | ./yt write "<sorted-by=[x]>//home/wrapper_test/table" --format dsv
    echo "x=z" | ./yt write "<sorted_by=[x]>//home/wrapper_test/table" --format dsv
    check "$(./yt get //home/wrapper_test/table/@sorted)" "$TRUE"
}

test_transactions()
{
    local tx=$(./yt start-tx)
    ./yt abort-tx "$tx"

    tx=$(./yt start-tx)
    ./yt commit-tx "$tx"
}

tear_down
run_test test_tree_commands
run_test test_file_commands
run_test test_copy_move_link
run_test test_map_reduce
run_test test_users
run_test test_concurrent_upload_in_operation
run_test test_sorted_by
run_test test_transactions
