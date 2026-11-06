GTEST()

SRCS(
    dynamic_counters_ut.cpp
)

PEERDIR(
    yt/yql/providers/dq/metrics
    yt/yt/library/profiling/solomon
)

END()
