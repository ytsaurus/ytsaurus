LIBRARY()

SRCS(
    pq_l2_cache.cpp
)



PEERDIR(
    contrib/ydb/core/keyvalue
    contrib/ydb/core/persqueue/pqtablet/blob
)

END()

RECURSE_FOR_TESTS(
    ut
)

