#!/bin/sh
set -e

# Keep runtime config preparation out of docker-compose.yml. Compose is only
# responsible for wiring services; this script validates required environment,
# expands placeholders in the mounted YSON config, and then execs the Flow
# binary as PID 1 under tini.

if [ -z "$YT_CLUSTER" ]; then
    echo "YT_CLUSTER must be set" >&2
    exit 1
fi

if [ -z "$CONFIG_SRC" ]; then
    echo "CONFIG_SRC must be set" >&2
    exit 1
fi

FLOW_USER=${YT_FLOW_USER:-${USER:-$(whoami)}}
FLOW_PATH=${YT_FLOW_PATH:-//tmp/$FLOW_USER/flow/noop}
PIPELINE_PATH=$FLOW_PATH/pipeline

# Flow registers controller/worker addresses in YT. With host networking those
# addresses must be reachable from YT proxies, so each service needs a public
# host IPv6 address to publish in its Flow config.
#
# If YT_FLOW_PUBLIC_ADDRESS is not provided, the Python snippet below prepares
# it like this:
#   1. Parse YT_CLUSTER and extract the host part.
#   2. Resolve that host to an IPv6 address.
#   3. Open an IPv6 UDP socket to that remote address. No packet is sent, but
#      the kernel still selects the outgoing route and source address.
#   4. Read the selected local source address with getsockname(); that address
#      is written to address_resolver.localhost_name_override.
#
# Examples:
#   YT_CLUSTER=<cluster>
#       Use <cluster> as the Flow cluster name. With no extra DNS rules set
#       it to the cluster's HTTP proxy address: <host>, <host>:<port>, or a
#       full http://<host>:<port> URL.
#   YT_FLOW_PUBLIC_ADDRESS=2a02:6b8:... docker compose up
#       Skip detection and publish this address in the Flow config.
flow_public_address_override=""
if grep -q '%YT_FLOW_PUBLIC_ADDRESS_OVERRIDE%' "$CONFIG_SRC"; then
    if [ -z "$YT_FLOW_PUBLIC_ADDRESS" ]; then
    YT_FLOW_PUBLIC_ADDRESS=$(python3 - "$YT_CLUSTER" <<'PY'
import socket
import sys
from urllib.parse import urlsplit

cluster = sys.argv[1]
parsed = urlsplit(cluster if "://" in cluster else "//" + cluster)
if not parsed.hostname:
    raise SystemExit("Failed to parse host from YT_CLUSTER={!r}".format(cluster))

host = parsed.hostname
lookup_hosts = [host]
if "." not in host and ":" not in host:
    lookup_hosts.append(host + ".yt.yandex.net")

for lookup_host in lookup_hosts:
    try:
        for family, _, _, _, sockaddr in socket.getaddrinfo(lookup_host, None, socket.AF_INET6):
            if family == socket.AF_INET6:
                cluster_addr = sockaddr[0]
                probe = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
                try:
                    probe.connect((cluster_addr, 1))
                    print(probe.getsockname()[0])
                finally:
                    probe.close()
                raise SystemExit(0)
    except socket.gaierror:
        pass

raise SystemExit("Failed to resolve {} to IPv6".format(host))
PY
    )

        if [ -z "$YT_FLOW_PUBLIC_ADDRESS" ]; then
            echo "Failed to auto-detect YT_FLOW_PUBLIC_ADDRESS for $YT_CLUSTER" >&2
            exit 1
        fi

        echo "Auto-detected YT_FLOW_PUBLIC_ADDRESS=$YT_FLOW_PUBLIC_ADDRESS" >&2
    fi

    flow_public_address_override="localhost_name_override = \"$YT_FLOW_PUBLIC_ADDRESS\";"
fi

sed \
    -e "s|%YT_CLUSTER%|$YT_CLUSTER|g" \
    -e "s|%YT_PIPELINE_PATH%|$PIPELINE_PATH|g" \
    -e "s|%YT_FLOW_RPC_PORT%|${YT_FLOW_RPC_PORT:-9002}|g" \
    -e "s|%YT_FLOW_MONITORING_PORT%|${YT_FLOW_MONITORING_PORT:-10002}|g" \
    -e "s|%YT_FLOW_PUBLIC_ADDRESS_OVERRIDE%|$flow_public_address_override|g" \
    "$CONFIG_SRC" > /tmp/config.yson

exec /usr/bin/noop_pipeline --config /tmp/config.yson
