#!/bin/bash

rm data

#for i in {401..431}; do
#    echo $i
#    for suffix in "" ".1"; do
#        scp n01-0${i}g.yt.yandex.net:/yt/disk1/atop/atop.log${suffix} .
#        atop -P CPU -r atop.log${suffix} >>data
#    done
#    #cat data${i} | ./analyzer.py ${i}
#done
cat data | awk '{i += 1;if (i%2==0){ sum=$9+$10+$11+$12+$13+$14+$15; print $3, 24 * (sum - $12) / sum}}' | python draw.py all

