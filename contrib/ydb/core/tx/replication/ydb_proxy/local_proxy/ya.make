LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/writer
    contrib/ydb/core/protos
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/topic
)

SRCS(
    local_partition_actor.cpp
    local_partition_committer.cpp
    local_partition_reader.cpp
    local_proxy.cpp
)

YQL_LAST_ABI_VERSION()

END()
