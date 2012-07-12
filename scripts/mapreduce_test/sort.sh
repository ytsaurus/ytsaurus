#!/bin/sh -eu

$MAPREDUCE -server "$SERVER" $PARAMS -src "$OUTPUT" -dst "$OUTPUT" -subkey -sort

