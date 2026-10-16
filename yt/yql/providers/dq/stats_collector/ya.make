YQL_LIBRARY()

SRCS(
    pool_stats_collector.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/helpers
    library/cpp/monlib/dynamic_counters
)

END()
