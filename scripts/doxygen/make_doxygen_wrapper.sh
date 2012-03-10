#!/bin/bash
source $(dirname ${BASH_SOURCE[0]})/common.sh
set +x
set +e

if mkdir ${TARGET}/pending &> /dev/null; then
    trap 'rm -rf ${TARGET}/pending' 0
    echo $$ > ${TARGET}/pending/pid

    # Okay, we are the only process running.
    (cd ${SOURCE} && git rev-parse -q HEAD) > ${TARGET}/pending/rev

    old_rev=$(cat ${TARGET}/current/rev 2>/dev/null)
    new_rev=$(cat ${TARGET}/pending/rev 2>/dev/null)

    if [[ "$old_rev" = "$new_rev" ]]; then
        exit
    fi

    ${SOURCE}/scripts/doxygen/make_doxygen.sh \
         > ${TARGET}/pending/run_stdout.txt \
        2> ${TARGET}/pending/run_stderr.txt

    if [[ $? == 0 ]]; then
        rm -rf ${TARGET}/current
        mv ${TARGET}/pending ${TARGET}/current
    fi

else
    # Nope, there are other processes.
    PID=$(cat ${TARGET}/pending/pid)
    if [[ $? != 0 ]]; then
        exit
    fi


    if ! kill -0 $PID &> /dev/null; then
        # Remove the stale lock of a nonexistent process and then restart.
        rm -rf ${TARGET}/pending
        exec "$0" "$@"
    else
        # We are locked; do nothing.
        exit
    fi
fi

