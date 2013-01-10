
#!/bin/bash

YTFACE=${YTFACE:-"auto"}

if [ "$YTFACE" == "auto" ]; then
    if which curl >/dev/null; then
        YTFACE=curl
    elif [ -x ./mapreduce-yt ]; then
        YTFACE=mapreduce-yt
        MRYT=./mapreduce-yt
    elif which mapreduce-yt; then
        YTFACE=mapreduce-yt
        MRYT=mapreduce-yt
    else
        echo "This interface needs curl or mapreduce-yt, but neither was found" >&2
        unset YTFACE
        exit 1
    fi
fi

if [ "$YTFACE" == "curl" ]; then
    # curl based implementation

    YTPROXY=${YTPROXY:-proxy.yt.yandex.net}
    APIBASE=http://$YTPROXY/api

    function _curl {
        local x=$(curl -s --write-out '\n%{http_code}' "$@")
        #echo "$x" >&2
        local status=$(echo "$x" | tail -n1)
        local body=$(echo "$x" | head -n-1)

        if [ $status -ne 200 ] && [ $status -ne 202 ]; then
            echo "Error status: $status" >&2
            [[ -n "$body" ]] && echo "$body" >&2
            return 1
        fi
        [[ -n "$body" ]] && echo "$body"
        return 0
    }

    function start_tx {
        # start_tx
        # start_tx {timeout}
        # start_tx {tx}
        # start_tx {timeout} {tx}
        if [ "$#" == "1" ] && [ "${1/*-*/X}" == "X" ]; then
            local tx=$1
        else
            local timeout=$1
            local tx=$2
        fi
        local timeout=${timeout:-120000}
        _curl -X POST -H 'X-YT-Output-Format: "dsv"' "$APIBASE/start_tx?timeout=$timeout""${tx:+&transaction_id=$tx}"
    }
    function commit_tx {
        _curl -X POST "$APIBASE/commit_tx?transaction_id=$1"
    }
    function abort_tx {
        _curl -X POST "$APIBASE/abort_tx?transaction_id=$1"
    }
    function renew_tx {
        _curl -X POST "$APIBASE/renew_tx?transaction_id=$1"
    }

    function table_attr {
        _curl "$APIBASE/get?path=$1/@$2""${3:+&transaction_id=$3}" | sed 's/"//g'
    }
    function table_rowcount {
        table_attr $1 row_count $2
    }

    function list_tables {
        # 1) not recursive and 2) returns nodes of all types, not only tables
        # yt's list command returns unsorted result
        _curl -H 'X-YT-Output-Format: "dsv"' "$APIBASE/list?path=$1" | sort
    }
    function _path_exists {
        local R=$(_curl -H 'X-YT-Output-Format: "dsv"' "$APIBASE/exists?path=$1""${2:+&transaction_id=$2}")
        if [ "$R" == "true" ]; then
            return 0;
        else
            return 1;
        fi
    }
    function _is_table {
        local R=$(_curl -H 'X-YT-Output-Format: "dsv"' "$APIBASE/get?path=$1/@type""${2:+&transaction_id=$2}")
        if [ "$R" == "table" ]; then
            return 0;
        else
            return 1;
        fi
    }

    function _create_dir {
        if ! _path_exists $1 $2; then
            _create_dir $(dirname $1) $2
            #FIXME: check that return string is actually a node id
            _curl -X POST "$APIBASE/create?type=map_node&path=$1""${2:+&transaction_id=$2}" >/dev/null 
        fi
    }
    function create_table {
        if ! _path_exists $1 $2; then
            local dir=$(dirname $1)
            if ! _path_exists $dir $2; then
                _create_dir $dir $2
            fi
            _curl -X POST "$APIBASE/create?type=table&path=$1""${2:+&transaction_id=$2}" >/dev/null
        fi
    }
    function drop_table {
        if _path_exists $1 $2; then
            if _is_table $1 $2; then
                _curl -X POST "$APIBASE/remove?path=$1""${2:+&transaction_id=$2}"
            else
                return 1;
            fi
        fi
    }
    function move_path {
        _curl -X POST "$APIBASE/move?source_path=$1&destination_path=$2""${3:+&transaction_id=$3}"
    }

    function lock {
        _curl -X POST "$APIBASE/lock?path=$1&mode=${3:-exclusive}""${2:+&transaction_id=$2}"
    }
    function _get {
        _curl -H 'X-YT-Output-Format: "dsv"' "$APIBASE/get?path=$1""${2:+&transaction_id=$2}"
    }
    function _set {
        _curl -X PUT --data-binary @- "$APIBASE/set?path=$1""${2:+&transaction_id=$2}"
    }

    function _write {
        # _write {target} [{-curl-option}]...
        # _write {target} {tx} [{-curl-option}]...
        # _write {target} {tx} {codec} [{-curl-option}]...
        for ((i=1; i<=$#; i++)); do
            if [[ ${!i:0:1} == "-" ]]; then
                shift $(($i - 1))
                break
            fi
            args[$i]=${!i}
        done
        local target=${args[1]}
        local tx=${args[2]}
        local codec=${args[3]}
        _curl -X PUT -T- "$APIBASE/write?path=$target&ping_ancestor_transactions=true""${tx:+&transaction_id=$tx}""${codec:+&table_writer[codec]=$codec}" \
            -H 'Transfer-Encoding: chunked' \
            --globoff \
            "$@"
    }
    function write_lenval {
        _write "$@" -H 'Content-Type: application/x-yamr-subkey-lenval'
    }
    function write_lenval_gzip {
        _write "$@" -H 'Content-Type: application/x-yamr-subkey-lenval' -H 'Content-Encoding: gzip'
    }
    function write_delim {
        _write "$@" -H 'Content-Type: application/x-yamr-subkey-delimited'
    }
    function write_delim_gzip {
        _write "$@" -H 'Content-Type: application/x-yamr-subkey-delimited' -H 'Content-Encoding: gzip'
    }

    function _sort_table_nb {
        local input=$1
        local output=$2
        local tx=$3
        local codec=$4

        # optional codec
        if [ "${codec:+X}" == "X" ]; then
            local codec_spec=$(cat <<END
"sort_job_io": {
      "table_writer": {
        "codec": "$codec"
      }
    },
    "merge_job_io": {
      "table_writer": {
        "codec": "$codec"
      }
    },
END
            )
        fi

        # support for inplace sorting
        if [ "$input" == "$output" ]; then
            local overwrite="true"
        else
            local overwrite="false"
        fi

        # operation spec construction
        local SPEC=$(cat <<END
{
  "spec": {
    $codec_spec
    "input_table_paths": [ "$input" ],
    "output_table_path": { "\$value": "$output",
                           "\$attributes": { "overwrite": "$overwrite" }
                         },
    "sort_by": [ "key", "subkey" ]
  }
}
END
        )

        #echo $SPEC >&2

        echo "$SPEC" | _curl -X POST "$APIBASE/sort""${tx:+?transaction_id=$tx}" \
            -H 'X-YT-Output-Format: "dsv"' \
            -H 'Content-Type: application/json' \
            --data-binary @-
    }
    function _wait_op_end {
        #XXX: 2012-12-03: now value of attribute 'state' could be an empty string sometimes
        local status="preparing";
        while [ "$status" == "running" ] || [ "$status" == "preparing" ] || [ "$status" == "" ]; do
            sleep 1
            status=$(_curl -H 'X-YT-Output-Format: "dsv"' "$APIBASE/get?path=//sys/operations/$1/@state")
            #[[ "$status" == "running" ]] && _curl --globoff -H 'X-YT-Output-Format: "dsv"' "$APIBASE/get?path=//sys/operations/$1/@progress/jobs"
        done
        while [ "$status" != "completed" ] && [ "$status" != "aborted" ] && [ "$status" != "failed" ]; do
            sleep 1
            status=$(_curl -H 'X-YT-Output-Format: "dsv"' "$APIBASE/get?path=//sys/operations/$1/@state")
        done
        if [ "$status" != "completed" ]; then
            local msg=$(_curl "$APIBASE/get?path=//sys/operations/$1/@result" | grep -o '"message":[^,]*"' | sed 's/^.*"message":[^"]*//' | sed 's/^"\|"$//g') 
            echo "ERROR: operation $status: $msg"
            false
        fi
    }
    function sort_table {
        # blocks till operation end
        local source=$1
        local target=$2
        local tx=$3
        local codec=$4

        local id=$(_sort_table_nb $source $target $tx $codec)
        if [[ $id != *-*-*-* ]]; then
            return 1;
        fi
        _wait_op_end $id
    }

elif [ "$YTFACE" == "mapreduce-yt" ]; then
    # mapreduce-yt based implementation

    function start_tx {
        $MRYT -starttx -timeout=120000
    }
    function commit_tx {
        $MRYT -committx $1
    }
    function abort_tx {
        $MRYT -aborttx $1
    }
    function renew_tx {
        $MRYT -renewtx $1
    }
    function list_tables {
        # 1) recursive, 2) returns only table nodes
        # mapreduce-yt -list command returns sorted result
        $MRYT -list -prefix $(dirname ${1}/droptail)/
    }
    function create_table {
        $MRYT -createtable "$1" ${2:+-tx $2}
    }
    function drop_table {
        $MRYT -drop "$1" ${2:+-tx $2}
    }
    function sort_table {
        $MRYT -sort -src $1 -dst $2 ${3:+-tx $3} ${4:+-codec $4}
    }

    function table_attr {
        $MRYT -get "$1/@$2" -outputformat dsv ${3:+-tx $3}
    }
    function table_rowcount {
        table_attr $1 row_count $2
    }

fi
