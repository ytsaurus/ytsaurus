#!/bin/bash -eux

YT_PROTOS="
    yt/core/misc/proto/error.proto
    yt/core/misc/proto/guid.proto
    yt/core/misc/proto/protobuf_helpers.proto
    
    yt/core/ytree/proto/attributes.proto
    yt/core/ytree/proto/ypath.proto
    
    yt/core/rpc/proto/rpc.proto
    yt/core/rpc/grpc/proto/grpc.proto
"

YP_PROTOS="
    yp/client/api/proto/cluster_api.proto
    yp/client/api/proto/pod_agent.proto
    yp/client/api/proto/data_model.proto
    yp/client/api/proto/discovery_service.proto
    yp/client/api/proto/object_service.proto
"

PROTO_PATHS=""

for path in $YT_PROTOS; do
    PROTO_PATHS="$PROTO_PATHS\nproto/$path"
    path_dir="$(dirname $path)"
    mkdir -p "ya_proto/yt_proto/$path_dir"
    python replace_imports.py <"../../yt/19_3/$path" >"ya_proto/yt_proto/$path"
done

for path in $YP_PROTOS; do
    PROTO_PATHS="$PROTO_PATHS\nproto/$path"
    path_dir="$(dirname $path)"
    mkdir -p "ya_proto/yp_proto/$path_dir"
    python replace_imports.py <"../../$path" >"ya_proto/yp_proto/$path"
done

echo -e "SET(PROTO_SRCS${PROTO_PATHS}\n)" >ya_proto/ya.make.inc
