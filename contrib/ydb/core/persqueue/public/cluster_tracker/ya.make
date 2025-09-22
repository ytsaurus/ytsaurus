LIBRARY()

SRCS(
    cluster_tracker.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/public
    contrib/ydb/core/grpc_services/cancelation
)

END()
