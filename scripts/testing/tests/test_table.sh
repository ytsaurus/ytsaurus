#!/bin/bash
#% NUM_MASTERS = 1
#% NUM_HOLDERS = 5
#% SETUP_TIMEOUT = 10

echo '{do=create; path="/table"; type=table}' | ytdriver

echo '{do=get; path = "/table@row_count"}' |ytdriver

# write one value
echo '{do=write; path = "/table"; value=[{b="hello"}]}' | ytdriver

# write many values
echo '{do=write; path = "/table"; value=[{b=2;a=1};{x=10;y=20;a=30}]}' | ytdriver

#write from stream
echo '{row = some}' > table.txt
echo '{do=write; path = "/table"; stream="<table.txt"}' |ytdriver

echo '{do=get; path = "/table@row_count"}' |ytdriver

