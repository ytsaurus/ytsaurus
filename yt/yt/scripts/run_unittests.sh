#!/bin/bash -e
# Should be run inside build directory

# unittester-containers requires porto to be installed.
for unittester_binary in $(find . -name "unittester-*" -type f); do
    if [[ ${unittester_binary} =~ "unittester-containers" ]]; then
        continue
    fi
    echo "Running ${unittester_binary}"
    unittester_name="$(basename ${unittester_binary})"
    ${unittester_binary} --gtest_output="xml:junit-${unittester_name}.xml"
done

for unittester_binary in $(find . -name "*-ut" -type f); do
    if [[ ${unittester_binary} =~ "library-cpp-logger-global" ]]; then
        continue
    elif [[ ${unittester_binary} =~ "yt-cpp-mapreduce" ]]; then
        continue
    fi
    echo "Running ${unittester_binary}"
    ${unittester_binary}
done
