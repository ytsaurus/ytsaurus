#!/bin/sh -eu

./mapreduce -server $SERVER -merge -file run.sh -file gen_terasort -src "$1" -src "$2" -dst "$3" -subkey 
