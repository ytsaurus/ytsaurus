#!/bin/bash -eux

export YT_PREFIX="//home/wrapper_tests/"

current_process_pid=$$

set +u
if [ -z "$ENABLE_SCHEMA" ]; then
    ENABLE_SCHEMA=""
else
    export YT_CONFIG_PATCHES="{yamr_mode={create_schema_on_tables=%true}};$YT_CONFIG_PATCHES"
fi
set -u

MAPREDUCE_YT="$PYTHON_BINARY $MAPREDUCE_YT_CLI_PATH"
YT="$PYTHON_BINARY $YT_CLI_PATH"

timeout() {
    local time_to_sleep=$1 && shift
    $@ &
    pid=$!
    set +x
    for i in $(seq 1 $((10 * $time_to_sleep))); do
        if ! ps -p $pid &>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    if ps -p $pid &>/dev/null; then
        kill $pid
        return 1
    fi
    return 0
    set -x
}

prepare_table_files() {
    set +x

    echo -e "4\t5\t6\n1\t2\t3" > table_file
    TABLE_SIZE=2

    rm -f big_file
    for i in {1..10}; do
        for j in {1..10}; do
            echo -e "$i\tA\t$j" >> big_file
        done
        echo -e "$i\tX\tX" >> big_file
    done
    BIG_TABLE_SIZE=100

    set -x
}

cleanup() {
    for pid in `pstree -p $$ | grep -o '([0-9]\+)' | grep -o '[0-9]\+'`; do
        if [ "$pid" = "$current_process_pid" ]; then
            continue
        fi
        if ps ax | awk '{print $1}' | grep $pid; then
            # We use "|| true" to prevent failure in case when the process
            # terminates before we kill it.
            kill -2 $pid || true
            sleep 0.1
            kill $pid || true
        fi
    done
    rm -f table_file big_file
}

die() {
    cleanup
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

test_base_functionality()
{
    $MAPREDUCE_YT -list
    $MAPREDUCE_YT -list -prefix //unexisting/path
    $MAPREDUCE_YT -drop "ignat/temp"
    $MAPREDUCE_YT -write "ignat/temp" <table_file
    $MAPREDUCE_YT -move -src "ignat/temp" -dst "ignat/other_table"
    check "4\t5\t6\n1\t2\t3\n" "`$MAPREDUCE_YT -read "ignat/other_table"`"
    $MAPREDUCE_YT -copy -src "ignat/other_table" -dst "ignat/temp"
    check "4\t5\t6\n1\t2\t3\n" "`$MAPREDUCE_YT -read "ignat/other_table"`"
    $MAPREDUCE_YT -copy -src "ignat/other_table" -dst "ignat/other_table"
    check "4\t5\t6\n1\t2\t3\n" "`$MAPREDUCE_YT -read "ignat/other_table"`"
    $MAPREDUCE_YT -drop "ignat/temp"
    $MAPREDUCE_YT -sort  -src "ignat/other_table" -dst "ignat/other_table"
    check "1\t2\t3\n4\t5\t6\n" "`$MAPREDUCE_YT -read "ignat/other_table"`"
    $MAPREDUCE_YT -sort "ignat/other_table"
    check "1\t2\t3\n4\t5\t6\n" "`$MAPREDUCE_YT -read "ignat/other_table"`"
    check "4\t5\t6\n" "`$MAPREDUCE_YT -read "ignat/other_table" -lowerkey 3`"
    $MAPREDUCE_YT -map "cat" -src "ignat/other_table" -dst "ignat/mapped" -ytspec '{"job_count": 10}'
    check 2 `$MAPREDUCE_YT -read "ignat/mapped" | wc -l`
    $MAPREDUCE_YT -map "cat" -src "ignat/other_table" -src "ignat/mapped" \
        -dstappend "ignat/temp"
    $MAPREDUCE_YT -orderedmap "cat" -src "ignat/other_table" -src "ignat/mapped" \
        -dst "ignat/temp" -append
    check 8 `$MAPREDUCE_YT -read "ignat/temp" | wc -l`

    $MAPREDUCE_YT -reduce "cat" -src "ignat/other_table" -dst "ignat/temp"
    check 2 `$MAPREDUCE_YT -read "ignat/temp" | wc -l`

    MR_TABLE_PREFIX="ignat/" $MAPREDUCE_YT -reduce "cat" -src "other_table" -dst "temp2"
    check 2 `$MAPREDUCE_YT -read "ignat/temp2" | wc -l`

    $MAPREDUCE_YT -hash-reduce "cat" -src "ignat/other_table" -dst "ignat/temp"
    check 2 `$MAPREDUCE_YT -read "ignat/temp" | wc -l`
}

test_copy_move()
{

    $MAPREDUCE_YT -write "ignat/table" <table_file
    check "4\t5\t6\n1\t2\t3\n" "`$MAPREDUCE_YT -read "ignat/table"`"
    $MAPREDUCE_YT -copy -src "ignat/unexisting_table" -dstappend "ignat/table"
    check "4\t5\t6\n1\t2\t3\n" "`$MAPREDUCE_YT -read "ignat/table"`"
    $MAPREDUCE_YT -move -src "ignat/unexisting_table" -dstappend "ignat/table"
    check "4\t5\t6\n1\t2\t3\n" "`$MAPREDUCE_YT -read "ignat/table"`"
    $MAPREDUCE_YT -copy -src "ignat/unexisting_table" -dst "ignat/table"
    check "" "`$MAPREDUCE_YT -read "ignat/table"`"
}

test_list()
{
    $MAPREDUCE_YT -write "ignat/test_dir/table1" <table_file
    $MAPREDUCE_YT -write "ignat/test_dir/table2" <table_file
    $YT create table "ignat/test_dir/table3"

    export YT_IGNORE_EMPTY_TABLES_IN_MAPREDUCE_LIST=1
    export YT_USE_YAMR_STYLE_PREFIX=1

    check "ignat/test_dir/table1\nignat/test_dir/table2\n" "`$MAPREDUCE_YT -list -prefix "ignat/test_dir"`"
    check "ignat/test_dir/table1\nignat/test_dir/table2\n" "`$MAPREDUCE_YT -list -prefix "ignat/test_dir/"`"
    check "ignat/test_dir/table1\nignat/test_dir/table2\n" "`$MAPREDUCE_YT -list -prefix "ignat/test_dir/tab"`"
    check "ignat/test_dir/table1\n" "`$MAPREDUCE_YT -list -prefix "ignat/test_dir/table1"`"
    check "ignat/test_dir/table1\n" "`$MAPREDUCE_YT -list -exact "ignat/test_dir/table1"`"
    check "test_dir/table1\n" "`MR_TABLE_PREFIX='ignat/' $MAPREDUCE_YT -list -exact "test_dir/table1"`"
    check "" "`$MAPREDUCE_YT -list -exact "ignat/test_dir/table"`"
    check "" "`$MAPREDUCE_YT -list -exact "ignat/test_dir"`"
    check_failed '$MAPREDUCE_YT -list -exact "ignat/test_dir/" -prefix "ignat"'

    check "ignat/test_dir/table1\n" "`$MAPREDUCE_YT -list -prefix "ignat/test_dir/table" -jsonoutput | python2 -c "import json, sys; print json.load(sys.stdin)[0]['name']"`"
    check "ignat/test_dir/table2\n" "`$MAPREDUCE_YT -list -prefix "ignat/test_dir/table" -jsonoutput | python2 -c "import json, sys; print json.load(sys.stdin)[1]['name']"`"
    check "[]\n" "`$MAPREDUCE_YT -list -exact "ignat/test_dir/table" -jsonoutput`"

    unset YT_IGNORE_EMPTY_TABLES_IN_MAPREDUCE_LIST

    check "ignat/test_dir/table1\nignat/test_dir/table2\nignat/test_dir/table3\n" "`$MAPREDUCE_YT -list -prefix "ignat/test_dir"`"

    unset YT_USE_YAMR_STYLE_PREFIX

    check "table1\ntable2\ntable3\n" "`$MAPREDUCE_YT -list -prefix "${YT_PREFIX}ignat/test_dir/"`"
}

test_codec()
{
    $MAPREDUCE_YT -write "ignat/temp" <table_file
    # Checking that we appending with new codec is disabled.
    #check_failed '$MAPREDUCE_YT -write "ignat/temp" -codec "none" <table_file'

    # We cannot write to existing table with replication factor
    $MAPREDUCE_YT -drop "ignat/temp"
    $MAPREDUCE_YT -write "ignat/temp" -codec "zlib9" -replicationfactor 5 <table_file
    check 5 "`$MAPREDUCE_YT -get "ignat/temp/@replication_factor"`"
}

test_many_output_tables()
{
    # Test many output tables
    echo -e "#!/usr/bin/env python
import os
import sys

if __name__ == '__main__':
    for line in sys.stdin:
        pass

    for descr in [3, 4, 5]:
        os.write(descr, '{0}\\\t{0}\\\t{0}\\\n'.format(descr))
    " >many_output_mapreduce.py
    chmod +x many_output_mapreduce.py

    $MAPREDUCE_YT -map "./many_output_mapreduce.py" -src "ignat/temp" -dst "ignat/out1" -dst "ignat/out2" -dst "ignat/out3" -file "many_output_mapreduce.py"
    for (( i=1 ; i <= 3 ; i++ )); do
        check 1 "`$MAPREDUCE_YT -read "ignat/out$i" | wc -l`"
    done

    rm -f "many_output_mapreduce.py"
}

test_chunksize()
{
    $MAPREDUCE_YT -write "ignat/temp" -chunksize 1 <table_file
    $MAPREDUCE_YT -get "ignat/temp/@"
    check 2 "`$MAPREDUCE_YT -get "ignat/temp/@chunk_count"`"
}

test_mapreduce()
{
    $MAPREDUCE_YT -subkey -write "ignat/temp" <big_file
    $MAPREDUCE_YT -subkey -src "ignat/temp" -dst "ignat/reduced" \
        -map 'grep "A"' \
        -reduce 'awk '"'"'{a[$1]+=$3} END {for (i in a) {print i"\t\t"a[i]}}'"'"
    check 10 "`$MAPREDUCE_YT -subkey -read "ignat/reduced" | wc -l`"
}

test_input_output_format()
{
    $MAPREDUCE_YT -subkey -write "ignat/temp" <table_file

    echo -e "#!/usr/bin/env python
import sys

if __name__ == '__main__':
    for line in sys.stdin:
        pass

    for i in [0, 1]:
        sys.stdout.write('k={0}\\\ts={0}\\\tv={0}\\\n'.format(i))
    " >reformat.py

    $MAPREDUCE_YT -subkey -outputformat "dsv" -map "python reformat.py" -file "reformat.py" -src "ignat/temp" -dst "ignat/reformatted"
    for i in {0..1}; do
        for f in k s v; do
            $MAPREDUCE_YT -dsv -read "ignat/reformatted" | grep "$f=$i"
        done
    done

    rm reformat.py

    echo "{k=1;v=2}" | $MAPREDUCE_YT -format yson -write "ignat/table"
    # TODO: We need to check equality in order independent manner
    check "k=1\tv=2" "`$MAPREDUCE_YT -format dsv -read "ignat/table"`"
}

test_transactions()
{
    $MAPREDUCE_YT -subkey -write "ignat/temp" <table_file
    TX=`$MAPREDUCE_YT -starttx`
    $MAPREDUCE_YT -renewtx "$TX"
    $MAPREDUCE_YT -subkey -write "ignat/temp" -append -tx "$TX" < table_file
    $MAPREDUCE_YT -set "ignat/temp/@my_attr"  -value 10 -tx "$TX"

    check_failed '$MAPREDUCE_YT -get "ignat/temp/@my_attr"'
    check 2 "`$MAPREDUCE_YT -read "ignat/temp" | wc -l`"

    $MAPREDUCE_YT -committx "$TX"

    check 10 "`$MAPREDUCE_YT -get "ignat/temp/@my_attr"`"
    check 4 "`$MAPREDUCE_YT -read "ignat/temp" | wc -l`"
}

test_range_map()
{
    $MAPREDUCE_YT -subkey -write "ignat/temp" <table_file
    $MAPREDUCE_YT -map 'awk '"'"'{sum+=$1+$2} END {print "\t"sum}'"'" -src "ignat/temp{key,value}" -dst "ignat/sum"
    check "\t14" "`$MAPREDUCE_YT -read "ignat/sum"`"
}

test_uploaded_files()
{
    $MAPREDUCE_YT -subkey -write "ignat/temp" <table_file

    echo -e "#!/usr/bin/env python
import sys

if __name__ == '__main__':
    for line in sys.stdin:
        pass

    for i in [0, 1, 2, 3, 4]:
        sys.stdout.write('{0}\\\t{1}\\\t{2}\\\n'.format(i, i * i, i * i * i))
    " >my_mapper.py
    chmod +x my_mapper.py

    $MAPREDUCE_YT -drop ignat/dir/mapper.py
    $MAPREDUCE_YT -listfiles >&2
    initial_number_of_files="`$MAPREDUCE_YT -listfiles | wc -l`"

    $MAPREDUCE_YT -upload ignat/dir/mapper.py -executable < my_mapper.py
    cat my_mapper.py | $MAPREDUCE_YT -upload ignat/dir/mapper.py -executable
    $MAPREDUCE_YT -download ignat/dir/mapper.py > my_mapper_copy.py
    diff my_mapper.py my_mapper_copy.py

    check $((1 + ${initial_number_of_files})) "`$MAPREDUCE_YT -listfiles | wc -l`"

    $MAPREDUCE_YT -subkey -map "./mapper.py" -ytfile "ignat/dir/mapper.py" -src "ignat/temp" -dst "ignat/mapped"
    check 5 "`$MAPREDUCE_YT -subkey -read "ignat/mapped" | wc -l`"

    rm -f my_mapper.py my_mapper_copy.py
}

test_ignore_positional_arguments()
{
    $MAPREDUCE_YT -list "" "123" >/dev/null
}

test_stderr()
{
    $MAPREDUCE_YT -subkey -write "ignat/temp" <table_file
    check_failed "$MAPREDUCE_YT -subkey -map 'cat &>2 && exit(1)' -src 'ignat/temp' -dst 'ignat/tmp' 2>/dev/null"
}

test_spec()
{
    $MAPREDUCE_YT -subkey -write "ignat/input" <table_file
    $MAPREDUCE_YT -map 'cat >/dev/null; echo -e "${YT_OPERATION_ID}\t"' \
        -ytspec '{"opt1": 10, "opt2": {"$attributes": {"my_attr": "ignat"}, "$value": 0.5}}' \
        -src "ignat/input" \
        -dst "ignat/output"

    op_id="`$MAPREDUCE_YT -read "ignat/output" | tr -d '[[:space:]]'`"
    op_dir=${op_id:$((${#op_id}-2)):2}
    op_path="//sys/operations/${op_dir}/${op_id}"
    check "10" "`$MAPREDUCE_YT -get "${op_path}/@spec/opt1"`"
    check "0.5" "`$MAPREDUCE_YT -get "${op_path}/@spec/opt2" | python2 -c 'import sys, json; print json.loads(sys.stdin.read())["$value"]'`"

    YT_SPEC='{"opt3": "hello", "opt4": {"$attributes": {}, "$value": null}}' $MAPREDUCE_YT \
        -map 'cat >/dev/null; echo -e "${YT_OPERATION_ID}\t"' \
        -src "ignat/input" \
        -dst "ignat/output"

    op_id="`$MAPREDUCE_YT -read "ignat/output" | tr -d '[[:space:]]'`"
    op_dir=${op_id:$((${#op_id}-2)):2}
    op_path="//sys/operations/${op_dir}/${op_id}"
    check '"hello"' "`$MAPREDUCE_YT -get "${op_path}/@spec/opt3"`"
    check 'null' "`$MAPREDUCE_YT -get "${op_path}/@spec/opt4"`"

    YT_USE_YAMR_DEFAULTS=1 $MAPREDUCE_YT \
        -map 'cat >/dev/null; echo -e "${YT_OPERATION_ID}\t"' \
        -ytspec '{"mapper": {"memory_limit": 1234567890}}' \
        -src "ignat/input" \
        -dst "ignat/output"

    op_id="`$MAPREDUCE_YT -read "ignat/output" | tr -d '[[:space:]]'`"
    op_dir=${op_id:$((${#op_id}-2)):2}
    op_path="//sys/operations/${op_dir}/${op_id}"
    check '1234567890' "`$MAPREDUCE_YT -get "${op_path}/@spec/mapper/memory_limit"`"
}

test_smart_format()
{
    format='{
    "$value":"yamred_dsv",
    "$attributes":{
        "key_column_names":["x","y"],
        "subkey_column_names":["subkey"],
        "has_subkey":"true"
    }
}'
    same_format_with_other_repr='{
    "$value":"yamred_dsv",
    "$attributes":{
        "key_column_names":["x","y"],
        "subkey_column_names":["subkey"],
        "has_subkey":"true"
    }
}'
    export YT_SMART_FORMAT=1
    $MAPREDUCE_YT -createtable "ignat/smart_x"
    $MAPREDUCE_YT -createtable "ignat/smart_z"
    $MAPREDUCE_YT -createtable "ignat/smart_w"

    $MAPREDUCE_YT -set "ignat/smart_x/@_format" -value "$format"
    $MAPREDUCE_YT -set "ignat/smart_z/@_format" -value "$format"
    $MAPREDUCE_YT -set "ignat/smart_w/@_format" -value "$same_format_with_other_repr"

    # test read/write
    echo -e "1 2\t\tz=10" | $MAPREDUCE_YT -subkey -write "ignat/smart_x"
    echo -e "3 4\t\tz=20" | $MAPREDUCE_YT -subkey -write "ignat/smart_w"
    check "1 2\tz=10" "`$MAPREDUCE_YT -read "ignat/smart_x"`"
    check "`printf "\x00\x00\x00\x031 2\x00\x00\x00\x04z=10"`" "`$MAPREDUCE_YT -lenval -read "ignat/smart_x"`"
    check "1 2\t\tz=10" "`$MAPREDUCE_YT -subkey -read "ignat/smart_x"`"
    # test columns
    ranged_table='ignat/smart_x{x,z}'
    # disable brace expansion
    set +B
    check_failed "$MAPREDUCE_YT -read ${ranged_table}"
    set -B
    check "x=1\tz=10" "`$MAPREDUCE_YT -read ${ranged_table} -dsv`"
    $MAPREDUCE_YT -map cat -src ignat/smart_x -src ignat/smart_w -dst ignat/output
    $MAPREDUCE_YT -read ignat/output -dsv
    $MAPREDUCE_YT -sort -src ignat/output -dst ignat/output
    check "key=1 2\nkey=3 4" "`$MAPREDUCE_YT -read ignat/output\{key\} -dsv`"

    unset YT_SMART_FORMAT
    # write in yamr
    echo -e "1 2\t\tz=10" | $MAPREDUCE_YT -subkey -write "ignat/smart_y"
    # convert to yamred_dsv
    $MAPREDUCE_YT -smartformat -map "cat" -src "ignat/smart_y" -src "fake" -dst "ignat/smart_x"
    check "1 2\tz=10" "`$MAPREDUCE_YT -smartformat -read "ignat/smart_x"`"

    check_failed '$MAPREDUCE_YT -smartformat -map "cat" -src "ignat/smart_y" -src "fake" -dst "ignat/smart_x" -dst "ignat/some_table"'
    $MAPREDUCE_YT -smartformat -map "cat" -src "ignat/smart_y" -src "fake" -dst "ignat/smart_x" -dst "ignat/smart_z" -outputformat "yamr"
    check "1 2\tz=10" "`$MAPREDUCE_YT -read "ignat/smart_x"`"
    check_failed '$MAPREDUCE_YT -smartformat -read "ignat/smart_x"'

    export YT_SMART_FORMAT=1
    echo -e "1 2\t\tz=10" | $MAPREDUCE_YT -subkey -write "ignat/smart_x"
    $MAPREDUCE_YT -copy -src "ignat/smart_x" -dst "ignat/smart_z"

    $MAPREDUCE_YT -map "cat" -src "ignat/smart_x" -src "ignat/smart_z" -dst "ignat/smart_z"
    check "1 2\tz=10\n1 2\tz=10" "`$MAPREDUCE_YT -read "ignat/smart_z"`"

    $MAPREDUCE_YT -map "cat" -reduce "cat" -src "ignat/smart_x" -dst "ignat/smart_y"
    check "1 2\tz=10" "`$MAPREDUCE_YT -read "ignat/smart_y"`"

    echo -e "1 1\t" | $MAPREDUCE_YT -write "ignat/smart_x" -append
    $MAPREDUCE_YT -sort -src "ignat/smart_x" -dst "ignat/smart_x"
    check "1 1\t\n1 2\tz=10" "`$MAPREDUCE_YT -read "ignat/smart_x"`"

    # TODO(ignat): improve this test to check that reduce is made by proper columns
    echo -e "1 2\t\tz=1" | $MAPREDUCE_YT -write "ignat/smart_x" -append

    $MAPREDUCE_YT -read "ignat/smart_x" -dsv
    $MAPREDUCE_YT -read "ignat/smart_x"

    $MAPREDUCE_YT -reduce "tr '=' ' ' | awk '{sum+=\$4} END {print sum \"\t\"}'" -src "ignat/smart_x" -dst "ignat/output" -jobcount 2
    check "11\t" "`$MAPREDUCE_YT -read "ignat/output"`"
}

test_drop()
{
    $MAPREDUCE_YT -subkey -write "ignat/xxx/yyy/zzz" <table_file
    $MAPREDUCE_YT -drop "ignat/xxx/yyy/zzz"
    check_failed '$MAPREDUCE_YT -get "ignat/xxx"'
}

test_create_table()
{
    $MAPREDUCE_YT -createtable "ignat/empty_table"
    $MAPREDUCE_YT -set "ignat/empty_table/@xxx" -value '"my_value"'
    echo -e "x\ty" | $MAPREDUCE_YT -write "ignat/empty_table" -append
    check '"my_value"' "`$MAPREDUCE_YT -get "ignat/empty_table/@xxx"`"
}

test_do_not_delete_empty_table()
{
    export YT_DELETE_EMPTY_TABLES=0
    $MAPREDUCE_YT -drop "ignat/empty_table"
    $MAPREDUCE_YT -createtable "ignat/empty_table"
    $MAPREDUCE_YT -write "ignat/empty_table" </dev/null
    check "0" "`$MAPREDUCE_YT -get "ignat/empty_table/@row_count"`"
    unset YT_DELETE_EMPTY_TABLES
}

test_sortby_reduceby()
{
    # It calculates sum of c2 grouped by c2
    echo -e "#!/usr/bin/env python
from __future__ import print_function
import sys
from itertools import groupby, starmap

def parse(line):
    d = dict(x.split('=') for x in line.split())
    return (d['c3'], d['c2'])

try:
    xrange
except NameError:  # Python 3
    xrange = range

def aggregate(key, recs):
    recs = list(recs)
    for i in xrange(len(recs) - 1):
        assert recs[i][1] <= recs[i + 1][1]
    return key, sum(map(lambda x: int(x[1]), recs))

if __name__ == '__main__':
    recs = map(parse, sys.stdin.readlines())
    for key, num in starmap(aggregate, groupby(recs, lambda rec: rec[0])):
        print('c3=%s	c2=%d' % (key, num))
    " >my_reducer.py
    chmod +x my_reducer.py

    echo -e "#!/usr/bin/env python
from __future__ import print_function
import sys

if __name__ == '__main__':
    for line in sys.stdin:
        print('\t'.join(k + '=' + v for k, v in sorted(x.split('=') for x in line.split())))
    " >order.py
    chmod +x order.py

    echo -e "c1=1\tc2=2\tc3=z\n"\
            "c1=1\tc2=3\tc3=x\n"\
            "c1=2\tc2=2\tc3=x" | $MAPREDUCE_YT -dsv -write "ignat/test_table"
    $MAPREDUCE_YT -sort -src "ignat/test_table" -dst "ignat/sorted_table" -sortby "c3" -sortby "c2"
    $MAPREDUCE_YT -reduce "./my_reducer.py" -src "ignat/sorted_table{c3,c2}" -dst "ignat/reduced_table" -reduceby "c3" -file "my_reducer.py" -dsv

    check "`echo -e "c3=x\tc2=5\nc3=z\tc2=2\n" | ./order.py`" "`$MAPREDUCE_YT -read "ignat/reduced_table" -dsv | ./order.py`"

    rm -f my_reducer.py order.py

    echo -e "a=1\tb=2\na=1\tb=1" | $MAPREDUCE_YT -dsv -write "<sorted_by=[a]>ignat/test_table"

    $MAPREDUCE_YT -reduce "cat" -src "ignat/test_table" -dst "ignat/reduced_table" -reduceby "a" -dsv
    check "`echo -e "a=1\tb=2\na=1\tb=1"`" "`$MAPREDUCE_YT -read "ignat/reduced_table" -dsv`"

    $MAPREDUCE_YT -reduce "cat" -src "ignat/test_table" -dst "ignat/reduced_table" -reduceby "a" -sortby "a" -sortby "b" -dsv
    check "`echo -e "a=1\tb=1\na=1\tb=2"`" "`$MAPREDUCE_YT -read "ignat/reduced_table" -dsv`"

    $MAPREDUCE_YT -reduce "cat" -src "ignat/test_table" -dst "ignat/reduced_table" -reduceby "b" -sortby "b" -dsv
    check "`echo -e "b=1\ta=1\nb=2\ta=1"`" "`$MAPREDUCE_YT -read "ignat/reduced_table" -dsv`"
}

test_empty_destination()
{
    $MAPREDUCE_YT -write "ignat/empty_table" </dev/null
}

test_dsv_reduce()
{
    echo -e "x=10\nx=0" | $MAPREDUCE_YT -dsv -write "ignat/empty_table"
    $MAPREDUCE_YT -dsv -reduce "cat" -reduceby "x" -src "ignat/empty_table" -dst "ignat/empty_table"
}

test_slow_write()
{
    gen_data()
    {
        sleep $1
        echo -e "a\tb"
    }
    $MAPREDUCE_YT -drop "ignat/some_table"
    gen_data 1 | $MAPREDUCE_YT -write "ignat/some_table" -timeout 2000
    check "`echo -e "a\tb"`" "`$MAPREDUCE_YT -read "ignat/some_table"`"
    gen_data 5 | $MAPREDUCE_YT -write "ignat/some_table" -timeout 1000
    check "`echo -e "a\tb"`" "`$MAPREDUCE_YT -read "ignat/some_table"`"
}

test_many_dst_write()
{
    echo -e "a\tb\n1\nc\td\n0\ne\tf" | $MAPREDUCE_YT -write -dst "ignat/A" -dst "ignat/B"
    check "`echo -e "a\tb\ne\tf"`" "`$MAPREDUCE_YT -read "ignat/A"`"
    check "`echo -e "c\td"`" "`$MAPREDUCE_YT -read "ignat/B"`"
}

test_dstsorted()
{
    echo -e "x\t10\ny\t15" | $MAPREDUCE_YT -writesorted ignat/some_table
    check "$TRUE" "`$MAPREDUCE_YT -get ignat/some_table/@sorted`"

    $MAPREDUCE_YT -reduce "grep x" -src ignat/some_table -dstsorted ignat/some_table
    check "$TRUE" "`$MAPREDUCE_YT -get ignat/some_table/@sorted`"

    echo -e "x\t10\ny\t15" | $MAPREDUCE_YT -write ignat/some_table
    $MAPREDUCE_YT -reduce "grep x" -src ignat/some_table -dstsorted ignat/some_table
    check "$TRUE" "`$MAPREDUCE_YT -get ignat/some_table/@sorted`"

    check_failed "$MAPREDUCE_YT -map cat -src ignat/some_table -dstsorted ignat/some_table -format dsv"
}

test_custom_fs_rs()
{
    echo -e "x y z" | $MAPREDUCE_YT -fs " " -write ignat/some_table
    check "`echo -e "x\ty z"`" "`$MAPREDUCE_YT -read ignat/some_table`"
}

test_write_with_tx()
{
    gen_data()
    {
        sleep $1
        echo -e "a\tb"
    }
    $MAPREDUCE_YT -drop "ignat/some_table"

    TX=`$MAPREDUCE_YT -starttx`
    gen_data 2 | $MAPREDUCE_YT -write "ignat/some_table" -tx $TX &

    sleep 1
    check "0" "`$MAPREDUCE_YT -read "ignat/some_table" | wc -l`"

    sleep 2
    check "0" "`$MAPREDUCE_YT -read "ignat/some_table" | wc -l`"

    wait

    $MAPREDUCE_YT -committx $TX
    check "1" "`$MAPREDUCE_YT -read "ignat/some_table" | wc -l`"
}

test_table_file()
{
    echo "x=0" | $MAPREDUCE_YT -dsv -write "ignat/input"
    echo "field=10" | $MAPREDUCE_YT -dsv -write "ignat/dictionary"
    $MAPREDUCE_YT -map "cat >/dev/null; cat dictionary" -dsv -src "ignat/input" -dst "ignat/output" -ytfile "<format=dsv>ignat/dictionary"
    check "field=10" "`$MAPREDUCE_YT -dsv -read "ignat/output"`"
}

test_unexisting_input_tables()
{
    echo "x=0" | $MAPREDUCE_YT -dsv -write "ignat/output"
    $MAPREDUCE_YT -map "cat" -src "ignat/unexisting1" -src "ignat/unexisting2" -dst "ignat/output"
    check "" "`$MAPREDUCE_YT -read ignat/output`"
}

test_copy_files()
{
    echo -e "MY CONTENT" >test_file
    cat test_file | $MAPREDUCE_YT -upload "ignat/my_file"
    $MAPREDUCE_YT -copy -src "ignat/my_file" -dst "ignat/other_file"
    check "MY CONTENT" "`$MAPREDUCE_YT -download ignat/other_file`"
    rm test_file
}

test_write_lenval()
{
    echo -n -e "\\x01\\x00\\x00\\x00a\\x01\\x00\\x00\\x00b" | $MAPREDUCE_YT -lenval -write "ignat/lenval_table"
    check "a\tb" "`$MAPREDUCE_YT -read "ignat/lenval_table"`"
}

test_force_drop()
{
    gen_data()
    {
        stdbuf -o 0 echo -e "a\tb"
        sleep 4
        echo -e "x\ty"
    }

    $MAPREDUCE_YT -drop "ignat/some_table" -force
    $MAPREDUCE_YT -createtable "ignat/some_table"

    gen_data | $MAPREDUCE_YT -append -write "ignat/some_table" &
    bg_pid=$!

    sleep 2

    $MAPREDUCE_YT -drop "ignat/some_table" -force

    check "" "`$MAPREDUCE_YT -read ignat/some_table`"

    kill $bg_pid || true
}

test_parallel_dstappend()
{
    echo -e "x\t10" | $MAPREDUCE_YT -write ignat/table

    run_op()
    {
        timeout 25 $MAPREDUCE_YT -map "cat" -src ignat/table -dstappend ignat/output_table
        if [ "$?" = 0 ]; then
            echo "xxx" >> sync_file
        fi
    }

    touch sync_file
    $MAPREDUCE_YT -createtable ignat/output_table
    run_op &
    run_op &

    local ok=0
    for i in {1..26}; do
        lines=`cat sync_file | wc -l`
        if [ "$lines" = "2" ]; then
            rm -f sync_file
            check "2" "`$MAPREDUCE_YT -read "ignat/output_table" | wc -l`"
            ok=1
            break
        fi
        sleep 1
    done

    if [ "$ok" = 0 ]; then
        rm -f sync_file
        die "Two simple operations do not finish correctly in 30 seconds"
    fi
}

test_many_to_many_copy_move()
{
    $MAPREDUCE_YT -write "ignat/in1" <table_file
    $MAPREDUCE_YT -write "ignat/in2" <table_file
    $MAPREDUCE_YT -move -src "ignat/in1" -dst "ignat/out1" -src "ignat/in2" -dst "ignat/out2"

    check "" "`$MAPREDUCE_YT -read "ignat/in1"`"
    check "" "`$MAPREDUCE_YT -read "ignat/in2"`"
    check "4\t5\t6\n1\t2\t3\n" "`$MAPREDUCE_YT -read "ignat/out1"`"
    check "4\t5\t6\n1\t2\t3\n" "`$MAPREDUCE_YT -read "ignat/out2"`"

    $MAPREDUCE_YT -copy -src "ignat/out1" -dst "ignat/in1" -src "ignat/out2" -dst "ignat/in2"
    check "4\t5\t6\n1\t2\t3\n" "`$MAPREDUCE_YT -read "ignat/in1"`"
    check "4\t5\t6\n1\t2\t3\n" "`$MAPREDUCE_YT -read "ignat/in2"`"
}

test_missing_prefix()
{
    local prefix="$YT_PREFIX"
    unset YT_PREFIX

    $MAPREDUCE_YT -get "tmp/@key"

    export YT_PREFIX="$prefix"
}

test_table_record_index()
{
    echo -e "a\t1" | $MAPREDUCE_YT -writesorted ignat/tableA
    echo -e "b\t2" | $MAPREDUCE_YT -writesorted ignat/tableB

    tempfile=$(mktemp /tmp/test_mapreduce_binary.XXXXXX)
    chmod 777 "$tempfile"

    $MAPREDUCE_YT -reduce "cat > $tempfile" \
                -src ignat/tableA \
                -src ignat/tableB \
                -dst ignat/dst \
                -tablerecordindex

    check "0\n0\na\t1\n1\n0\nb\t2" "$(cat $tempfile)"
    rm -rf $tempfile
}

test_opts()
{
    $MAPREDUCE_YT -write "tmp/input" <table_file
    $MAPREDUCE_YT -map "cat" -src "tmp/input" -dst "tmp/output"
    check "1" "$($MAPREDUCE_YT -get tmp/output/@chunk_count)"
    $MAPREDUCE_YT -map "cat" -src "tmp/input" -dst "tmp/output" -opt "jobcount=2"
    check "2" "$($MAPREDUCE_YT -get tmp/output/@chunk_count)"
    $MAPREDUCE_YT -map "cat" -src "tmp/input" -dst "tmp/output" -opt "cpu.intensive.mode=1"
    check "2" "$($MAPREDUCE_YT -get tmp/output/@chunk_count)"
    MR_OPT="cpu.intensive.mode=1" $MAPREDUCE_YT -map "cat" -src "tmp/input" -dst "tmp/output"
    check "2" "$($MAPREDUCE_YT -get tmp/output/@chunk_count)"
    MR_OPT="cpu.intensive.mode=1" $MAPREDUCE_YT -map "cat" -src "tmp/input" -dst "tmp/output" -opt "cpu.intensive.mode=0"
    check "1" "$($MAPREDUCE_YT -get tmp/output/@chunk_count)"
}

test_defrag()
{
    echo -e "a\t1" | $MAPREDUCE_YT -writesorted ignat/input
    echo -e "b\t2" | $MAPREDUCE_YT -append -writesorted ignat/input

    for defrag in "" "full"; do
        $MAPREDUCE_YT -defrag $defrag -src ignat/input -dst ignat/output
        check "`echo -e "a\t1\nb\t2"`" "`$MAPREDUCE_YT -read ignat/output`"
        check "$TRUE" "`$MAPREDUCE_YT -get ignat/output/@sorted`"
        check "1" "`$MAPREDUCE_YT -get ignat/output/@chunk_count`"
    done
}

test_archive_and_transform()
{
    tempfile=$(mktemp /tmp/test_mapreduce_binary_config.XXXXXX)
    echo "{transform_options={desired_chunk_size=10000000}}" >$tempfile
    export YT_CONFIG_PATH="$tempfile"

    echo -e "a\t1\nb\t2" | $MAPREDUCE_YT -write ignat/input

    $MAPREDUCE_YT -archive ignat/input -erasurecodec none
    check '"zlib_9"' "`$MAPREDUCE_YT -get ignat/input/@compression_codec`"
    check '"none"' "`$MAPREDUCE_YT -get ignat/input/@erasure_codec`"

    $MAPREDUCE_YT -unarchive ignat/input
    # Check nothing

    $MAPREDUCE_YT -transform -src ignat/input -dst ignat/output -codec "zlib_6"
    check '"zlib_6"' "`$MAPREDUCE_YT -get ignat/output/@compression_codec`"
    check '"none"' "`$MAPREDUCE_YT -get ignat/output/@erasure_codec`"

    rm -rf "$tempfile"
    unset YT_CONFIG_PATH
}

test_create_tables_under_transaction_option()
{
    gen_data()
    {
        stdbuf -o 0 echo -e "a\tb"
        sleep 10
        echo -e "x\ty"
    }

    run()
    {
        table="${1}"
        $MAPREDUCE_YT -drop "$table" -force
        check "false" "`$MAPREDUCE_YT -exists "$table"`"
        gen_data | $MAPREDUCE_YT -append -write "$table"
    }

    run "ignat/xxx_some_table" &
    sleep 5
    check "true" "`$MAPREDUCE_YT -exists "ignat/xxx_some_table"`"
    sleep 7

    YT_CREATE_TABLES_UNDER_TRANSACTION=1 run "ignat/xxx_some_table2" &
    sleep 5
    check "false" "`$MAPREDUCE_YT -exists "ignat/xxx_some_table2"`"
    sleep 7
}

prepare_table_files
test_base_functionality
test_copy_move
test_list
test_codec
test_many_output_tables
test_chunksize
test_mapreduce
test_transactions
test_range_map
test_uploaded_files
test_ignore_positional_arguments
test_stderr
test_spec
test_drop
test_create_table
test_do_not_delete_empty_table
test_empty_destination
test_slow_write
test_many_dst_write
test_dstsorted
test_custom_fs_rs
test_write_with_tx
test_copy_files
test_write_lenval
test_force_drop
test_parallel_dstappend
test_many_to_many_copy_move
test_missing_prefix
test_table_record_index
test_opts
test_archive_and_transform
test_defrag
test_create_tables_under_transaction_option

if [ -z "$ENABLE_SCHEMA" ]; then
    test_sortby_reduceby
    test_input_output_format
    test_smart_format
    test_dsv_reduce
    test_table_file
    test_unexisting_input_tables
fi

cleanup
