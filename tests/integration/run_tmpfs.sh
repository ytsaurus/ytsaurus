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

    most_recent=`echo $cores | head -n1 | cut -d' ' -f2`
    executable=`file $most_recent | sed "s/.*from '\([^ ]*\).*/\1/g"`
    echo Most recent core dump was likely produced by $executable
    if [[ -n "$SYMLINK_CORE_TO_ROOT_DIRECTORY" ]]; then
        ln -sf $most_recent core
        echo Linked most recent core dump to ./core
    fi
fi

exit $exit_code
