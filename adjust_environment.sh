#!/bin/bash

if [[ x$0 == *environment* ]]; then
    echo "Don't forget to use source, Luke:"
    echo source $0
else
    PWD="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export PYTHONPATH="${PWD}/python:${PYTHONPATH}"
fi
