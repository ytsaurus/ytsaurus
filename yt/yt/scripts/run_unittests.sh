#!/bin/bash -ex
# Should be run inside build directory

function retry {
    local retries=3
    local count=0
    until "$@"; do
        exit_code=$?
        count=$(($count + 1))
        if [ $count -lt $retries ]; then
            echo "Attempt $count failed with exit code $exit_code. Retrying..."
        else
            echo "Attempt $count failed with exit code $exit_code. No more retries."
            return $exit_code
        fi
    done
    return 0
}

# unittester-containers requires porto to be installed.
for unittester_binary in $(find . -name "unittester-*" -type f); do
    if [[ ${unittester_binary} =~ "unittester-containers" ]]; then
        continue
    fi
    echo "Running ${unittester_binary}"
    unittester_name="$(basename ${unittester_binary})"
    retry ${unittester_binary} --gtest_output="xml:junit-${unittester_name}.xml"
done

for unittester_binary in $(find . -name "*-ut" -type f); do
    if [[ ${unittester_binary} =~ "library-cpp-logger-global" ]]; then
        continue
    elif [[ ${unittester_binary} =~ "yt-cpp-mapreduce" ]]; then
        continue
    fi
    echo "Running ${unittester_binary}"
    retry ${unittester_binary}
done
