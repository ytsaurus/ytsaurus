#!/bin/bash -eux

YT_PROTOS="
    yt/core/misc/proto/error.proto
    yt/core/misc/proto/guid.proto
    yt/core/misc/proto/protobuf_helpers.proto

    yt/core/ytree/proto/attributes.proto
    yt/core/ytree/proto/ypath.proto

    yt/core/rpc/proto/rpc.proto
    yt/core/rpc/grpc/proto/grpc.proto

    yt/client/hive/timestamp_map.proto
    yt/client/hive/cell_directory.proto
    yt/client/rpc_proxy/proto/api_service.proto
    yt/client/rpc_proxy/proto/discovery_service.proto

    yt/client/node_tracker_client/proto/node.proto
"

PROTO_PATHS=""

if [ -d "../.git" ]; then
    YT_PREFIX=""
else
    YT_PREFIX="/yt/19_3"
fi

for path in $YT_PROTOS; do
    PROTO_PATHS="$PROTO_PATHS\nyt_proto/$path"
    path_dir="$(dirname $path)"
    mkdir -p "ya_proto/yt_proto/$path_dir"
    python ../replace-imports.py <"..$YT_PREFIX/$path" >"ya_proto/yt_proto/$path"
done

echo -e "SET(PROTO_SRCS${PROTO_PATHS}\n)" >ya_proto/ya.make.inc
