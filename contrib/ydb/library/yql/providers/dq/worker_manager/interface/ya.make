LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    yql/essentials/utils/log
    contrib/ydb/library/yql/dq/common
    yql/essentials/providers/common/metrics
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
