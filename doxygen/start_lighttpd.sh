#!/bin/bash

set -e
set -x

CELLAR=/home/sandello/Cellar/stage
SOURCE=/home/sandello/source/junk/monster/yt

${CELLAR}/sbin/lighttpd -t -f ${SOURCE}/doxygen/lighttpd.conf
${CELLAR}/sbin/lighttpd -D -f ${SOURCE}/doxygen/lighttpd.conf
