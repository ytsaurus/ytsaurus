#!/bin/bash
set -ex

case $(hostname -f) in
    "build01-02g.yt.yandex.net")
        export LD_LIBRARY_PATH=/usr/lib/llvm-3.4/lib
        DOXYGEN=/home/sandello/Cellar/bin/doxygen
        BUILD=/home/sandello/yt/build
        SOURCE=/home/sandello/yt/source
        TARGET=/home/sandello/yt/doxygen
        ;;
    *)
        echo "Unknown machine; please, include your configuration into common.sh."
        exit 1
esac

[[ ! -d ${SOURCE} ]] && echo "Source directory does not exist" && exit 1
[[ ! -d ${TARGET} ]] && echo "Target directory does not exist" && exit 1

GENERATE_COMMIT=$( (cd ${SOURCE} && git rev-parse HEAD) )
GENERATE_TIME=$(date +"%F %T %z (%a, %d %b %Y)")

cat ${SOURCE}/scripts/doxygen/Doxyfile.template \
    | sed "s!%%GENERATE_COMMIT%%!${GENERATE_COMMIT}!" \
    | sed "s!%%GENERATE_TIME%%!${GENERATE_TIME}!" \
    | sed "s!%%OUTPUT_DIRECTORY%%!${TARGET}!" \
    | sed "s!%%STRIP_FROM_PATH%%!${SOURCE}!" \
    | sed "s!%%INCLUDE_PATH%%!!" \
    | sed "s!%%CLANG_OPTIONS%%!-x c++ -std=c++11!" \
    | sed "s!%%INPUT%%!${SOURCE}/yt/!" \
    > ${TARGET}/Doxyfile

(cd ${TARGET} && ${DOXYGEN} ${TARGET}/Doxyfile)

