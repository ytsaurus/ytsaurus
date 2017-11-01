#!/bin/bash

time $MAPREDUCE -map ./map.pl -src 'access-log/"2012-07-04"' -dst usr/proskurnev/tmp/st1 -file statistic/map.pl
time $MAPREDUCE -reduce ./reduce.pl -src usr/proskurnev/tmp/st1 -dst usr/proskurnev/tmp/st2 -file statistic/reduce.pl
time $MAPREDUCE -reduce ./finalize.pl -src usr/proskurnev/tmp/st2 -dst usr/proskurnev/result -file statistic/finalize.pl
time $MAPREDUCE -sort -src usr/proskurnev/result -dst usr/proskurnev/result
time $MAPREDUCE -read usr/proskurnev/result > res

