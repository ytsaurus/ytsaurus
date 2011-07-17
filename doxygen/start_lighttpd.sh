#!/bin/bash
source $(dirname ${BASH_SOURCE[0]})/common.sh
set +x

${CELLAR}/sbin/lighttpd -t -f ${SOURCE}/doxygen/lighttpd.conf
${CELLAR}/sbin/lighttpd -D -f ${SOURCE}/doxygen/lighttpd.conf
