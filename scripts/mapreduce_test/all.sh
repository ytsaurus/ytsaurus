#!/bin/sh -eu

for (( iter = 1 ; iter <= 20; iter++ ))
do
    echo "Iteration $iter"
    time ./prepare.sh
    time ./sort.sh
done

