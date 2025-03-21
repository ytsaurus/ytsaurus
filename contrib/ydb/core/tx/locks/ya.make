LIBRARY()

SRCS(
    locks.cpp
    locks_db.cpp
    time_counters.cpp
    range_treap.cpp
)


PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tablet_flat
)

YQL_LAST_ABI_VERSION()

END()


RECURSE_FOR_TESTS(
    ut_range_treap
)
