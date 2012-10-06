#!/bin/bash

hr1="##########"
hr2="$hr1$hr1"
hr4="$hr2$hr2"
hr8="$hr4$hr4"

function run_jshint()
{
    echo
    echo $hr8
    echo "## jshint: $1"
    echo $hr8

    jshint --config lint.json "$1"
}

run_jshint ./bin/yt_http_proxy

find ./lib/ -type f -name '*.js' -and -not -name 'test_*' -print0 | while IFS= read -r -d $'\0' file
do
    run_jshint $file
done

