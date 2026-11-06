LIBRARY()

SRCS(
    metrics_pusher.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    contrib/ydb/library/yql/utils/actors
    library/cpp/monlib/encode
    yql/essentials/providers/common/metrics
    yql/essentials/utils/log
    yql/tools/yqlworker/solomon_agent # TODO: split to client and config, move config to library
)

END()
