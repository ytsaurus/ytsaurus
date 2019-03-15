#!/bin/bash -eux

YP_PROTOS="
    yp/client/api/proto/autogen.proto
    yp/client/api/proto/conditions.proto
    yp/client/api/proto/cluster_api.proto
    yp/client/api/proto/pod_agent.proto
    yp/client/api/proto/data_model.proto
    yp/client/api/proto/enums.proto
    yp/client/api/proto/dynamic_resource.proto
    yp/client/api/proto/discovery_service.proto
    yp/client/api/proto/object_service.proto
    yp/client/api/proto/replica_set.proto
    yp/client/api/proto/multi_cluster_replica_set.proto
    yp/client/api/proto/resource_cache.proto
    yp/client/api/proto/secrets.proto
    yp/client/api/proto/dynamic_attributes.proto
"

PROTO_PATHS=""

if [ ! -d "../../.git" ]; then
    echo "Must be called from git repository" >&2
    exit 1
fi

for path in $YP_PROTOS; do
    PROTO_PATHS="$PROTO_PATHS\nyp_proto/$path"
    path_dir="$(dirname $path)"
    mkdir -p "ya_proto/yp_proto/$path_dir"
    python ../../replace-imports.py <"../../$path" >"ya_proto/yp_proto/$path"
done

echo -e "SET(PROTO_SRCS${PROTO_PATHS}\n)" >ya_proto/ya.make.inc
