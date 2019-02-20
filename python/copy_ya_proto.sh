#!/bin/bash -eux

YP_PROTOS="
    yp/client/api/proto/conditions.proto
    yp/client/api/proto/cluster_api.proto
    yp/client/api/proto/pod_agent.proto
    yp/client/api/proto/data_model.proto
    yp/client/api/proto/dynamic_resource.proto
    yp/client/api/proto/object_type.proto
    yp/client/api/proto/discovery_service.proto
    yp/client/api/proto/object_service.proto
    yp/client/api/proto/replica_set.proto
    yp/client/api/proto/resource_cache.proto
    yp/client/api/proto/secrets.proto
"

PROTO_PATHS=""

if [ -d "../../.git" ]; then
    YT_PREFIX=""
else
    YT_PREFIX="/yt/19_3"
fi

for path in $YP_PROTOS; do
    PROTO_PATHS="$PROTO_PATHS\nyp_proto/$path"
    path_dir="$(dirname $path)"
    mkdir -p "ya_proto/yp_proto/$path_dir"
    python ../../replace-imports.py <"../../$path" >"ya_proto/yp_proto/$path"
done

echo -e "SET(PROTO_SRCS${PROTO_PATHS}\n)" >ya_proto/ya.make.inc
