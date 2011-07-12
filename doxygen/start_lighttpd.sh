#!/bin/bash
source /home/sandello/source/doxygen/common.sh

${CELLAR}/sbin/lighttpd -t -f ${SOURCE}/doxygen/lighttpd.conf
${CELLAR}/sbin/lighttpd -D -f ${SOURCE}/doxygen/lighttpd.conf
