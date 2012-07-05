#!/bin/sh -eu

./mapreduce -server $SERVER -src "$OUTPUT" -dst "$OUTPUT" -subkey -sort

