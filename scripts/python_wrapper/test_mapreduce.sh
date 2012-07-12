#!/bin/sh -eux

echo -e "4\t5\t6\n1\t2\t3\n" > table_file
./mapreduce -list
./mapreduce -drop "//home/ignat/temp"
./mapreduce -write "//home/ignat/temp" <table_file 
./mapreduce -copy -src "//home/ignat/temp" -dst "//home/ignat/other_table"
./mapreduce -read "//home/ignat/other_table"
./mapreduce -drop "//home/ignat/temp"
./mapreduce -sort  -src "//home/ignat/other_table"
./mapreduce -read "//home/ignat/other_table"
./mapreduce -read "//home/ignat/other_table" -lowerkey 3
./mapreduce -map "cat" -src "//home/ignat/other_table" -dst "//home/ignat/mapped"
./mapreduce -read "//home/ignat/mapped"
for (( i=1 ; i <= 2 ; i++ )); do
    ./mapreduce -map "cat" -src "//home/ignat/other_table" -src "//home/ignat/mapped" \
        -dstappend "//home/ignat/temp"
done
./mapreduce -read "//home/ignat/temp" | wc -l 

./mapreduce -write "//home/ignat/temp" -chunksize 1 <table_file 
