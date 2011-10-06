#!/bin/bash
source $(dirname ${BASH_SOURCE[0]})/common.sh
set +x

TIMEOUT=5

while [[ 1 == 1 ]]; do
    echo "$(date) :: Starting lighttpd..."
    sudo ${CELLAR}/sbin/lighttpd -t -f ${SOURCE}/doxygen/lighttpd.conf
    sudo ${CELLAR}/sbin/lighttpd -D -f ${SOURCE}/doxygen/lighttpd.conf
    echo "$(date) :: lighttpd terminated; respawning in ${TIMEOUT} seconds..."

    sleep ${TIMEOUT}
done
