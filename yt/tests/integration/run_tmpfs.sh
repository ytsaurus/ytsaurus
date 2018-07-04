#!/bin/bash

if ! mountpoint -q tests.sandbox ; then
    if ! [ -e tests.sandbox ]; then
        echo "!!! creating tests.sandbox"
        mkdir tests.sandbox
    fi
    echo "!!! mounting tests.sandbox as tmpfs"
    sudo mount -t tmpfs -o size=4g tmpfs tests.sandbox
else
    # do nothing
    echo "!!! tests.sandbox already mounted as tmpfs"
fi

ulimit -c unlimited
py.test -sv --ignore tests.sandbox "$@"
exit_code=$?

echo "==========================================================="
cores=`find tests.sandbox/ -name "core*" -printf "%C+ %p\n" | sort -r`
if [[ "$cores" != "" ]]; then
    echo "Core dumps in tests.sandbox (sorted by creation time)"
    echo "$cores"
fi

exit $exit_code
