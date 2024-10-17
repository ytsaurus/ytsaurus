#!/usr/bin/env bash

yt_local start --proxy-port 80 \
--local-cypress-dir /var/lib/yt/local-cypress \
--fqdn localhost \
--ytserver-all-path /usr/bin/ytserver-all \
--sync $@
