#!/bin/bash
#% NUM_MASTERS = 3
#% NUM_HOLDERS = 5

rm -f downloaded_file.txt

dd if=/dev/urandom of=some_file.txt bs=1000 count=1000 2>/dev/null

cat some_file.txt | yt upload //file

yt get //file@size
yt get //sys/chunks@count

yt download //file > downloaded_file.txt

diff= `diff some_file.txt downloaded_file.txt`
echo "Diff: $diff"

yt remove /file
yt get //sys/chunks@count
