#!/bin/sh -eux

echo -e "4\t5\t6\n1\t2\t3" > table_file

rm -f big_file
for i in {1..10}; do
    for j in {1..10}; do
        echo -e "$i\tA\t$j" >> big_file
    done
    echo -e "$i\tX\tX" >> big_file
done

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
    ./mapreduce -map "cat" -src "ignat/other_table" -dst "ignat/mapped" -ytspec "{'job_count': 10}"
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
    ./mapreduce -write "ignat/temp" -codec "gzip_best_compression" <table_file
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


test_base_functionality
test_codec
test_many_output_tables
test_chunksize
test_mapreduce

rm -f table_file big_file


