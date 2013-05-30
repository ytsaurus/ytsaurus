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

run_test test_tree_commands
run_test test_file_commands
run_test test_copy_move_link
run_test test_map_reduce
