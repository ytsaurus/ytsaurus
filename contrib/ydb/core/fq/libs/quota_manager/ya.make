LIBRARY()

SRCS(
    quota_manager.cpp
    quota_proxy.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/protobuf/json
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/core/fq/libs/quota_manager/events
    contrib/ydb/core/fq/libs/shared_resources
    contrib/ydb/core/protos
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
    proto
    ut_helpers
)
