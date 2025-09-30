LIBRARY()

SRCS(
    dread_cache_service/caching_service.cpp
    pq.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/public/cluster_tracker
    contrib/ydb/core/persqueue/public/fetcher
    contrib/ydb/core/persqueue/public/list_topics
    contrib/ydb/core/persqueue/pqrb
    contrib/ydb/core/persqueue/pqtablet
    contrib/ydb/core/persqueue/writer
)

END()

RECURSE(
    common
    events
    pqrb
    pqtablet
    public
    writer
)

RECURSE_FOR_TESTS(
    ut
    dread_cache_service/ut
    ut/slow
    ut/ut_with_sdk
)
