#!/bin/bash

for i in `seq 1 3`; do
    rsync run_gdb.sh meta01-00$ig:~/

for i in `seq 400 600`; do
    rsync run_gdb.sh n01-$ig:~/ &
