LIBRARY()

SRCS(
    config.cpp
    control_plane_storage_counters.cpp
    in_memory_control_plane_storage.cpp
    probes.cpp
    request_validators.cpp
    util.cpp
    validators.cpp
    ydb_control_plane_storage.cpp
    ydb_control_plane_storage_bindings.cpp
    ydb_control_plane_storage_compute_database.cpp
    ydb_control_plane_storage_connections.cpp
    ydb_control_plane_storage_queries.cpp
    ydb_control_plane_storage_quotas.cpp
)

PEERDIR(
    library/cpp/lwtrace
    library/cpp/protobuf/interop
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/common
    contrib/ydb/core/fq/libs/config
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/control_plane_storage/events
    contrib/ydb/core/fq/libs/control_plane_storage/internal
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/core/fq/libs/db_schema
    contrib/ydb/core/fq/libs/graph_params/proto
    contrib/ydb/core/fq/libs/quota_manager/events
    contrib/ydb/core/fq/libs/shared_resources
    contrib/ydb/core/fq/libs/ydb
    contrib/ydb/core/mon
    contrib/ydb/library/security
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/library/db_pool
    contrib/ydb/library/yql/providers/s3/path_generator
    contrib/ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
    internal
    proto
)
