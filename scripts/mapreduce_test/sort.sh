#!/bin/sh -eu

# export server variable
source ./server.sh

./mapreduce -server $SERVER -src speed_test/output -dst speed_test/output -subkey -sort

