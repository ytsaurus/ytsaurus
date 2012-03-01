#!/bin/bash

for i in `seq 1 3`; do
    timeout 20s rsync run_gdb.sh meta01-00"$i"g:~/
done

for i in `seq 400 600`; do
    timeout 20s rsync run_gdb.sh n01-0"$i"g:~/ &
done

wait