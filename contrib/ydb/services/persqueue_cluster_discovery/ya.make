LIBRARY()

SRCS(
    cluster_discovery_service.cpp
    cluster_discovery_worker.cpp
    counters.cpp
    grpc_service.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/client/server
    contrib/ydb/core/grpc_services
    contrib/ydb/core/mind/address_classification
    contrib/ydb/core/mon
    contrib/ydb/core/persqueue
    contrib/ydb/core/protos
    contrib/ydb/core/util
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
    contrib/ydb/services/persqueue_cluster_discovery/cluster_ordering
)

END()

RECURSE(
    cluster_ordering
)

RECURSE_FOR_TESTS(
    ut
)
