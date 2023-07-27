#!/bin/bash

TARGET=history
tar pcf - $TARGET | pv -s $(du -sb $TARGET | awk '{print $1}')  | gzip > ${TARGET}.tar.gz

FILE=${TARGET}.tar.gz

export YT_PROXY=plato.yt.yandex.net
yt create -i map_node //home/acid/analysis
cat $FILE | yt upload //home/acid/analysis/$FILE
