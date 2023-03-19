#!/usr/bin/env bash

local_cypress_dir=$1

mkdir -p /etc/yt
mkdir -p $local_cypress_dir

cat >/etc/yt/server.yson <<EOF
{
  address_resolver = {
    enable_ipv4 = true;
    enable_ipv6 = false;
  }
}
EOF
cat >/etc/yt/controller-agent.yson <<EOF
{
  address_resolver = {
    enable_ipv4 = true;
    enable_ipv6 = false;
  }
}
EOF

