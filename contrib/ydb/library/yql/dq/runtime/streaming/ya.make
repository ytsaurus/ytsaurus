LIBRARY()

SRCS(
    dq_compute_actor_watermarks.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
