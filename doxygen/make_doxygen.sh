#!/bin/bash
source /home/sandello/source/doxygen/common.sh
set -x

LAST_CHANGED_REV=$(svn info ${SOURCE} | grep 'Last Changed Rev' | cut -d : -f 2- | cut -c 2-)
LAST_CHANGED_DATE=$(svn info ${SOURCE} | grep 'Last Changed Date' | cut -d : -f 2- | cut -c 2-)
LAST_GENERATED_DATE=$(date +"%F %T %z (%a, %d %b %Y)")

svn update $SOURCE

cat ${SOURCE}/doxygen/yt.cfg.template \
    | sed "s!%%LAST_CHANGED_REV%%!${LAST_CHANGED_REV}!" \
    | sed "s!%%LAST_CHANGED_DATE%%!${LAST_CHANGED_DATE}!" \
    | sed "s!%%LAST_GENERATED_DATE%%!${LAST_GENERATED_DATE}!" \
    | sed "s!%%STRIP_FROM_PATH%%!${SOURCE}!" \
    | sed "s!%%OUTPUT_DIRECTORY%%!${SOURCE}/doxygen/output/!" \
    | sed "s!%%INPUT%%!${SOURCE}/yt/ytlib/!" \
    > ${SOURCE}/doxygen/output/yt.cfg

${CELLAR}/bin/doxygen ${SOURCE}/doxygen/output/yt.cfg
