#!/bin/bash

TEMPORARY=

Cleanup() {
    rm -f $TEMPORARY
}

trap Cleanup INT TERM EXIT

if [[ "$(uname)" == "Darwin" ]]; then
    TEMPORARY=$(mktemp -t x)
else
    TEMPORARY=$(mktemp -p .)
fi

if [[ "$@" != "apply" ]]; then
    echo "*** Supply an 'apply' as a parameter to update Subversion property."
    python svn-externals-regenerate.py
else
    python svn-externals-regenerate.py > $TEMPORARY
    svn propset svn:externals . -F $TEMPORARY
fi

