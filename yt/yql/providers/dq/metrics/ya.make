LIBRARY()

SRCS(
    metrics_printer.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    library/cpp/monlib/encode
    yql/essentials/providers/common/metrics
)

END()
