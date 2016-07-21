#!/bin/bash

if ! mount | grep `pwd`/tests.sandbox; then
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

py.test -sv --ignore tests.sandbox "$@"

