#!/bin/bash
source $(dirname ${BASH_SOURCE[0]})/common.sh
set +x

rm -rf ${SOURCE}/doxygen/output
mkdir -p ${SOURCE}/doxygen/output

set +e

${SOURCE}/doxygen/make_doxygen.sh \
     > ${SOURCE}/doxygen/output/run_stdout.txt \
    2> ${SOURCE}/doxygen/output/run_stderr.txt

rc=$?

if [[ $rc == 0 ]]; then
    rm -rf ${SOURCE}/doxygen/stable-output
    mv ${SOURCE}/doxygen/output ${SOURCE}/doxygen/stable-output
fi

