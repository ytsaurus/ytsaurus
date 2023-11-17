LIBRARY()

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/providers/common/metrics
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/api/protos
)

YQL_LAST_ABI_VERSION()

SET(
    SOURCE
    events.cpp
    worker_info.cpp
    counters.cpp
)

SRCS(
    ${SOURCE}
)

END()
