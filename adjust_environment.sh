#!/bin/bash

if [[ x$0 == *environment* ]]; then
    echo "Don't forget to use source, Luke:"
    echo source $0
else
    export PYTHONPATH="`pwd`/python:$PYTHONPATH"
fi
