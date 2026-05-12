LIBRARY()

SRCS(
    pq_l2_cache.cpp
)



PEERDIR(
    contrib/ydb/core/keyvalue
    contrib/ydb/core/persqueue/pqtablet/blob
    contrib/ydb/core/persqueue/events
)

END()

RECURSE_FOR_TESTS(
    ut
)

