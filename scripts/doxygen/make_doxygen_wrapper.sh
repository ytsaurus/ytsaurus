#!/bin/bash
source $(dirname ${BASH_SOURCE[0]})/common.sh
set +x

rm -rf ${TARGET}/pending
mkdir -p ${TARGET}/pending

set +e

${SOURCE}/scripts/doxygen/make_doxygen.sh \
     > ${TARGET}/pending/run_stdout.txt \
    2> ${TARGET}/pending/run_stderr.txt

rc=$?

if [[ $rc == 0 ]]; then
    rm -rf ${TARGET}/current
    mv ${TARGET}/pending ${TARGET}/current
fi

