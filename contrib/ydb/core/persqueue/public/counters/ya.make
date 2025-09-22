LIBRARY()

SRCS(
    percentile_counter.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    contrib/ydb/library/persqueue/topic_parser
)

END()
