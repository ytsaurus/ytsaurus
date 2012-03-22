#!/bin/bash 
#% NUM_MASTERS = 3
#% NUM_HOLDERS = 5

yt create /table table

echo 'read empty table'
yt read /table
yt get /table
yt get /table@row_count

echo 'write one value'
yt write /table [{b="hello"}]
yt get /table@row_count
yt read /table

echo 'write many values'
yt write /table '[{b="2";a="1"};{x="10";y="20";a="30"}]'
yt get /table@row_count
yt read /table

#TODO(panin): add more checks of read methods

