#!/bin/bash -eux

cd $(dirname "${BASH_SOURCE[0]}")

set +x
echo -e "4\t5\t6\n1\t2\t3" > table_file

rm -f big_file
for i in {1..10}; do
    for j in {1..10}; do
        echo -e "$i\tA\t$j" >> big_file
    done
    echo -e "$i\tX\tX" >> big_file
done
set -x

die()
{
    exit 1
}

test_base_functionality()
{
    ./mapreduce -list
    ./mapreduce -drop "ignat/temp"
    ./mapreduce -write "ignat/temp" <table_file
    ./mapreduce -copy -src "ignat/temp" -dst "ignat/other_table"
    ./mapreduce -read "ignat/other_table"
    ./mapreduce -drop "ignat/temp"
    ./mapreduce -sort  -src "ignat/other_table" -dst "ignat/other_table"
    ./mapreduce -read "ignat/other_table"
    ./mapreduce -read "ignat/other_table" -lowerkey 3
    ./mapreduce -map "cat" -src "ignat/other_table" -dst "ignat/mapped" -ytspec '{"job_count": 10}'
    ./mapreduce -read "ignat/mapped"
    for (( i=1 ; i <= 2 ; i++ )); do
        ./mapreduce -map "cat" -src "ignat/other_table" -src "ignat/mapped" \
            -dstappend "ignat/temp"
    done
    ./mapreduce -read "ignat/temp" | wc -l
}

test_codec()
{
    ./mapreduce -write "ignat/temp" -codec "none" <table_file
    ./mapreduce -write "ignat/temp" -codec "gzip_best_compression" -replication_factor 5 <table_file
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

    for descr in range(3, 6):
        os.write(descr, '{0}\\\t{0}\\\t{0}\\\n'.format(descr))
    " >many_output_mapreduce.py
    chmod +x many_output_mapreduce.py

    ./mapreduce -map "./many_output_mapreduce.py" -src "ignat/temp" -dst "ignat/out1" -dst "ignat/out2" -dst "ignat/out3" -file "many_output_mapreduce.py"
    for (( i=1 ; i <= 3 ; i++ )); do
        ./mapreduce -read "ignat/out$i" | wc -l
    done

    rm -f "many_output_mapreduce.py"
}

test_chunksize()
{
    ./mapreduce -write "ignat/temp" -chunksize 1 <table_file
}

test_mapreduce()
{
    ./mapreduce -subkey -write "ignat/temp" <big_file
    ./mapreduce -subkey -src "ignat/temp" -dst "ignat/reduced" \
        -map 'grep "A"' \
        -reduce 'awk '"'"'{a[$1]+=$3} END {for (i in a) {print i"\t\t"a[i]}}'"'"
    ./mapreduce -subkey -read "ignat/reduced" | wc -l
}

test_input_output_format()
{
    ./mapreduce -subkey -write "ignat/temp" <table_file

    echo -e "#!/usr/bin/env python
import sys

if __name__ == '__main__':
    for line in sys.stdin:
        pass

    for i in range(5):
        sys.stdout.write('k={0}\\\ts={0}\\\tv={0}\\\n'.format(i))
    " >reformat.py

    ./mapreduce -subkey -outputformat "dsv" -map "python reformat.py" -file "reformat.py" -src "ignat/temp" -dst "ignat/reformatted"
    ./mapreduce -dsv -read "ignat/reformatted"

    rm reformat.py
}

test_transactions()
{
    ./mapreduce -subkey -write "ignat/temp" <table_file
    TX=`./mapreduce -start_tx`
    ./mapreduce -subkey -write "ignat/temp" -append -tx "$TX" < table_file
    ./mapreduce -set "ignat/temp/@my_attr"  -value 10 -tx "$TX"

    ./mapreduce -get "ignat/temp/@my_attr"
    ./mapreduce -read "ignat/temp" | wc -l

    ./mapreduce -commit_tx "$TX"

    ./mapreduce -get "ignat/temp/@my_attr"
    ./mapreduce -read "ignat/temp" | wc -l
}

test_range_map()
{
    ./mapreduce -subkey -write "ignat/temp" <table_file
    ./mapreduce -subkey -map 'awk '"'"'{sum+=$1+$2} END {print "\t\t"sum}'"'" -src "ignat/temp{key,subkey}" -dst "ignat/sum"
    ./mapreduce -read "ignat/sum"
}

test_uploaded_files()
{
    ./mapreduce -subkey -write "ignat/temp" <table_file

    echo -e "#!/usr/bin/env python
import sys

if __name__ == '__main__':
    for line in sys.stdin:
        pass

    for i in range(5):
        sys.stdout.write('{0}\\\t{1}\\\t{2}\\\n'.format(i, i * i, i * i * i))
    " >my_mapper.py
    chmod +x my_mapper.py
    ./mapreduce -upload ignat/mapper.py -executable < my_mapper.py
    cat my_mapper.py | ./mapreduce -upload ignat/mapper.py -executable
    ./mapreduce -download ignat/mapper.py > my_mapper_copy.py
    diff my_mapper.py my_mapper_copy.py

    ./mapreduce -listfiles

    ./mapreduce -subkey -map "./mapper.py" -ytfile "ignat/mapper.py" -src "ignat/temp" -dst "ignat/mapped"
    ./mapreduce -subkey -read "ignat/mapped" | wc -l

    rm -f mapper.py
}

test_ignore_positional_arguments()
{
    ./mapreduce -list "" "123" >/dev/null
}

test_stderr()
{
    ./mapreduce -subkey -write "ignat/temp" <table_file
    ./mapreduce -subkey -map "cat &>2 && exit(1)" -src "ignat/temp" -dst "ignat/tmp" 2>/dev/null && die || true
}

test_smart_format()
{
    echo -e "key=1\tvalue=2" | ./mapreduce -smart_format -dsv -write "ignat/smart_x"
    ./mapreduce -smart_format -map "cat" -src "ignat/smart_x" -dst "ignat/smart_y"
}

test_drop()
{
    ./mapreduce -subkey -write "ignat/xxx/yyy/zzz" <table_file
    ./mapreduce -drop "ignat/xxx/yyy/zzz"
    ./mapreduce -get "ignat/xxx"
}

test_create_table()
{
    ./mapreduce -createtable "ignat/empty_table"
    ./mapreduce -set "ignat/empty_table/@xxx" -value "my_value"
    echo -e "x\ty" | ./mapreduce -write "ignat/empty_table" -append
    ./mapreduce -get "ignat/empty_table/@xxx"
}

test_base_functionality
test_codec
test_many_output_tables
test_chunksize
test_mapreduce
test_input_output_format
test_transactions
test_range_map
test_uploaded_files
test_ignore_positional_arguments
test_stderr
test_smart_format
test_drop
test_create_table

rm -f table_file big_file
