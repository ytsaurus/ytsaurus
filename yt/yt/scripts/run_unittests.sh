#!/bin/bash -e
# Should be run inside build directory

# unittester-containers requires porto to be installed.
for unittester_binary in $(find . -name "unittester-*" -type f | grep -v "unittester-containers"); do
    echo "Running ${unittester_binary}"
    ${unittester_binary}
done

for unittester_binary in $(find . -name "*-ut" -type f); do
    echo "Running ${unittester_binary}"
    ${unittester_binary}
done
