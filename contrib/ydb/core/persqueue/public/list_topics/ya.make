LIBRARY()

SRCS(
    list_all_topics_actor.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/public
    contrib/ydb/core/grpc_services/cancelation
    contrib/ydb/core/tx/scheme_cache
)

END()

RECURSE_FOR_TESTS(
    ut
)
