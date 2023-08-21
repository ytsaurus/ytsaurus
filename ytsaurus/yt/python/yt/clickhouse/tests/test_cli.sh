#!/bin/bash -eux

PIDS=""
CLI="$CMD"

die() {
    echo "$@" && exit 1
}

check() {
    local first="$(echo -e "$1")"
    local second="$(echo -e "$2")"
    [ "${first}" = "${second}" ] || die "Test fail $1 does not equal $2"
}

check_failed() {
    set +e
    eval $1
    if [ "$?" = "0" ]; then
        die "Command \"$@\" should fail"
    fi
    set -e
}

test_simple() {
    check "42" "$($CLI execute 'select 42')"
    check "$(echo -ne '1\tfoo\n2\tbar\n')" "$($CLI execute 'select * from `//tmp/t`')"
}

test_simple
