#!/bin/bash

set -e

while getopts "nA:B:al" opt; do
    case $opt in
        n)
            DRYRUN_SWITCH=echo
        ;;
        a)
            DIFF="-c clone"
        ;;
        l)
            LOGGING="enabled"
        ;;
        \?)
            exit 1
        ;;
    esac
done

shift $((OPTIND-1))

if [ ! $# -eq 2 ]; then
    echo "$0: 2 positional parameters expected, got $#" >&2
    exit 2
fi

sourcedir=$1
targetdir=$2

trap 'kill 0' SIGINT SIGTERM EXIT

logfile="log.${sourcedir//\//+}.$(date +%Y%m%dT%H%M%S)"
tablefile="tables.${sourcedir//\//+}.$(date +%Y%m%dT%H%M%S)"
LOGGING=${LOGGING:+">> $logfile 2>&1"}

./mapreduce -list -server redwood:8013 -prefix $sourcedir > $tablefile
    
for from in `cat $tablefile`; do
    to=$targetdir/$from
    echo "$from $to" | xargs -n2 -I% $DRYRUN_SWITCH \
        sh -c "./copy-redwood-to-yt.sh -f gzip_best_compression % $LOGGING"
done

