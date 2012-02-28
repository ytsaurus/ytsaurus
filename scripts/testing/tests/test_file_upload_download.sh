#!/bin/bash
#% NUM_MASTERS = 3
#% NUM_HOLDERS = 5
#% SETUP_TIMEOUT = 10

rm -f downloaded_file.txt

dd if=/dev/urandom of=some_file.txt bs=1000 count=1000 2>/dev/null

echo '{do=upload; path= "/file"; stream = "< some_file.txt" }' |ytdriver

echo '{do=get; path = "/file@size"}' |ytdriver
echo '{do=get; path = "/sys/chunks/@size"}' |ytdriver

echo '{do=download; path= "/file"; stream = "> downloaded_file.txt" }' |ytdriver

diff= `diff some_file.txt downloaded_file.txt`
echo "Diff: $diff"

echo '{do=remove; path = "/file"}' |ytdriver
echo '{do=get; path = "/sys/chunks/@size"}' |ytdriver
