#!/bin/sh -eu

echo "./mapreduce -server "$SERVER" $PARAMS -src "$OUTPUT" -dst "$OUTPUT" -subkey -sort"
./mapreduce -server "$SERVER" $PARAMS -src "$OUTPUT" -dst "$OUTPUT" -subkey -sort

