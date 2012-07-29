#!/bin/sh -eu

if [ "$SYSTEM" = "mapreduce" ]; then
    $MAPREDUCE -server "$SERVER" $PARAMS -src "$OUTPUT" -dst "$OUTPUT" -sort
elif [ "$SYSTEM" = "yt" ]; then
    echo -e "
import config
import yt
config.DEFAULT_PROXY='$SERVER'
config.DEFAULT_FORMAT=yt.DsvFormat()

yt.sort_table('//home/ignat/' + '$OUTPUT', columns=['k'])
" >sort.py
    python sort.py
fi
