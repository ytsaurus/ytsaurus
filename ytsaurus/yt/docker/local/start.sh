#!/usr/bin/env bash

yt_local start --proxy-port 80 \
--master-config /etc/yt/server.yson \
--node-config /etc/yt/server.yson \
--scheduler-config /etc/yt/server.yson \
--controller-agent-config /etc/yt/controller-agent.yson \
--rpc-proxy-config /etc/yt/server.yson \
--local-cypress-dir /var/lib/yt/local-cypress \
--fqdn localhost \
--ytserver-all-path /usr/bin/ytserver-all \
--sync $@
