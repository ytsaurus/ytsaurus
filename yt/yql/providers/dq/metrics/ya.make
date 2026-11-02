LIBRARY()

SRCS(
    dynamic_counters.cpp
    metrics_printer.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    library/cpp/monlib/encode
    yql/essentials/providers/common/metrics
    yt/yt/library/profiling
)

END()

RECURSE_FOR_TESTS(
    ut
)
