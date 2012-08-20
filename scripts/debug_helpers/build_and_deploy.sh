#!/bin/sh -exu

build()
{
    cd ~yt/build
    make -j16
}

deploy()
{
    cd ~yt/src/scripts/config
    PYTHONPATH=~yt/src/python ./example_remote.py
    cd control
    ./stop.sh
    ./prepare.sh
    ./run.sh
}

build
deploy
