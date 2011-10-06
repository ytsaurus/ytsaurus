#!/bin/bash
source $(dirname ${BASH_SOURCE[0]})/common.sh
set -x

CURRENT_BRANCH=$( ( cd ${SOURCE} && git branch --no-color --no-abbrev -v ) | awk '{print $2}' )
CURRENT_COMMIT=$( ( cd ${SOURCE} && git branch --no-color --no-abbrev -v ) | awk '{print $3}' )
GENERATED_AT=$(date +"%F %T %z (%a, %d %b %Y)")

svn update $SOURCE

cat ${SOURCE}/doxygen/yt.cfg.template \
    | sed "s!%%CURRENT_BRANCH%%!${CURRENT_BRANCH}!" \
    | sed "s!%%CURRENT_COMMIT%%!${CURRENT_COMMIT}!" \
    | sed "s!%%GENERATED_AT%%!${GENERATED_AT}!" \
    | sed "s!%%STRIP_FROM_PATH%%!${SOURCE}!" \
    | sed "s!%%OUTPUT_DIRECTORY%%!${TARGET}/pending/!" \
    | sed "s!%%INPUT%%!${SOURCE}/yt/ytlib/!" \
    > ${TARGET}/pending/yt.cfg

${CELLAR}/bin/doxygen ${TARGET}/pending/yt.cfg

