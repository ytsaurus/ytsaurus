#!/bin/sh -eux

echo -e "4\t5\t6\n1\t2\t3\n" > table_file
./mapreduce -list
./mapreduce -drop "ignat/temp"
./mapreduce -write "ignat/temp" <table_file
./mapreduce -copy -src "ignat/temp" -dst "ignat/other_table"
./mapreduce -read "ignat/other_table"
./mapreduce -drop "ignat/temp"
./mapreduce -sort  -src "ignat/other_table"
./mapreduce -read "ignat/other_table"
./mapreduce -read "ignat/other_table" -lowerkey 3
./mapreduce -map "cat" -src "ignat/other_table" -dst "ignat/mapped"
./mapreduce -read "ignat/mapped"
for (( i=1 ; i <= 2 ; i++ )); do
    ./mapreduce -map "cat" -src "ignat/other_table" -src "ignat/mapped" \
        -dstappend "ignat/temp"
done
./mapreduce -read "ignat/temp" | wc -l

./mapreduce -write "ignat/temp" -chunksize 1 <table_file

./mapreduce -map "tests/many_output.py" -src "ignat/temp" -dst "ignat/out1" -dst "ignat/out2" -dst "ignat/out3"
for (( i=1 ; i <= 3 ; i++ )); do
    ./mapreduce -read "ignat/out$i" | wc -l
done
