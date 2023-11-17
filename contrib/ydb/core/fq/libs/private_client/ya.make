LIBRARY()

SRCS(
    internal_service.cpp
    loopback_service.cpp
    private_client.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/protobuf/json
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/core/fq/libs/grpc
    contrib/ydb/core/fq/libs/shared_resources
    contrib/ydb/core/protos
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()
