#!/bin/bash -ex

cd yt/wrapper && make && cd -

./arcadia-sync.sh yandex-yt-python contrib/python/yt_trunk $@
