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
    contrib/ydb/library/range_treap
)

YQL_LAST_ABI_VERSION()

END()
