#!/bin/bash -e
# Should be run inside build directory

# unittester-query has some unresolved problems with compiling bc functions.
# unittester-containers requires porto to be installed.
for unittester_binary in $(find . -name "unittester-*" -type f | grep -v "unittester-query\|unittester-containers"); do
    echo "Running ${unittester_binary}"
    ${unittester_binary}
done
