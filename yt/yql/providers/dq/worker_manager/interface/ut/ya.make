UNITTEST()

SRCS(
    events_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    yt/yql/providers/dq/common/attrs
    yt/yql/providers/dq/worker_manager/interface
)

END()
