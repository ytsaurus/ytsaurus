#!/bin/bash

set -o pipefail
set -e

sourcedir=$1
targetdir=$2

if [ -n $YTFACE ]; then
    . ./ytface.sh
fi

MRSERVER=${MRSERVER:-redwood.yandex.ru}

# get lists of tables at redwood:$sourcedir/ and yt:$targetdir/
function mr_list_tables {
    # mapreduce table lists returns already sorted (?)
    ./mapreduce -server ${MRSERVER}:8013 -list -prefix $1 
}
function mr_table_size {
    curl -s "http://${MRSERVER}:13013/json?info=table&table=$1"|grep size|grep -o -e '[0-9]\+'
}
function mr_table_rowcount {
    curl -s "http://${MRSERVER}:13013/json?info=table&table=$1"|grep recordCount|grep -o -e '[0-9]\+'
}

SRCLIST=$(mr_list_tables $sourcedir)

for MR_TABLE in $SRCLIST;
do
    SRCSIZE=$(mr_table_rowcount ${MR_TABLE})
    YT_TABLE="$targetdir/${MR_TABLE}"
    if _path_exists ${YT_TABLE} "0-0-0-0";  then
        DSTSIZE=$(table_rowcount ${YT_TABLE})
    else
        DSTSIZE="-1"
    fi

    if [ "${SRCSIZE}" != "${DSTSIZE}" ]; then
        echo ${MR_TABLE}
    fi
done

