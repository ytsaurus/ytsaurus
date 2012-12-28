#!/bin/bash

set -o pipefail
set -e

comm_opts=-23

while getopts "A:B:c:psu" opt; do
    case $opt in
        A)
            after=$OPTARG
        ;;
        B)
            before=$OPTARG
        ;;
        c)
            case $OPTARG in
                left) comm_opts=-23 ;;  # uniq in left
                right) comm_opts=-13 ;; # uniq in right
                both) comm_opts=-12 ;;  # exists in both
                clone) comm_opts=-2 ;;  # all left, ignore what's in right
                *)
                   echo "Illegal value of -c option: must be one of [left, right, both]"
                   exit 1
                ;;
            esac
        ;;
        p)
            print_table_pairs=1
        ;;
        s)
            check_rowcounts=1
        ;;
        u)
            unite_diffs=1
            check_rowcounts=1
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

# get first level tables only
pattern='^[^\/]*$'

if [ -n $YTFACE ]; then
    . ./ytface.sh
fi

MRSERVER=${MRSERVER:-redwood.yandex.ru}

# get lists of tables at redwood:$sourcedir/ and yt:$targetdir/
function mr_list_tables {
    # mapreduce table lists returns already sorted (?)
    ./mapreduce -server ${MRSERVER}:8013 -list -prefix $1 | sed -e 's#^'$1/'##'
}
function mr_table_size {
    curl -s "http://${MRSERVER}:13013/json?info=table&table=$1"|grep size|grep -o -e '[0-9]\+'
}
function mr_table_rowcount {
    curl -s "http://${MRSERVER}:13013/json?info=table&table=$1"|grep recordCount|grep -o -e '[0-9]\+'
}

SRCLIST=$(mr_list_tables $sourcedir | grep $pattern)
DSTLIST=$(list_tables $targetdir)

#echo "$SRCLIST"
#echo
#echo "$DSTLIST"

# if -A and -B limits were specified then clamp those lists
function date_after {
    awk -v date_after=$1 '{ if ($0 >= date_after) print $0 }'
}
function date_before {
    awk -v date_before=$1 '{ if ($0 <= date_before) print $0 }'
}

if [ ! -z "$after" ]; then
    SRCLIST=$(echo "$SRCLIST" | date_after $after)
    DSTLIST=$(echo "$DSTLIST" | date_after $after)
fi
if [ ! -z "$before" ]; then
    SRCLIST=$(echo "$SRCLIST" | date_before $before)
    DSTLIST=$(echo "$DSTLIST" | date_before $before)
fi

#echo "$SRCLIST"
#echo
#echo "$DSTLIST"

# then find and print two lists difference
function compare {
    comm $1 <(echo "${SRCLIST}") <(echo "${DSTLIST}") | sed -e 's/^[ \t]*//'
}

DIFF=$(compare $comm_opts)

if [ -v check_rowcounts ]; then
    x=$(compare -12)
    SRCSIZES=$(for i in $x; do echo -e "$i\t$(mr_table_rowcount $sourcedir/$i)"; done &)
    DSTSIZES=$(for i in $x; do echo -e "$i\t$(table_rowcount $targetdir/$i)"; done &)
    set +e
    COUNTDIFF=$(diff --changed-group-format='%>' --unchanged-group-format='' <(echo "$SRCSIZES") <(echo "$DSTSIZES") | awk '{print $1}')
    set -e
    if [ -v unite_diffs ]; then
        DIFF=$((echo "${DIFF}"; echo "${COUNTDIFF}") | sort)
    else
        DIFF="$COUNTDIFF"
    fi
fi

if [ -n "$DIFF" ]; then 
    if [ -z $print_table_pairs ]; then
        echo "$DIFF"
    else
        echo "$DIFF" | awk -v sourcedir=$sourcedir -v targetdir=$targetdir '{ print sourcedir "/" $0, targetdir "/" $0}'
    fi
fi
# vim: expandtab sw=4 ts=4 sts=4
