YQL_LIBRARY()

SRCS(
    test_coordination_helper.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    yt/yql/providers/dq/actors/events
    yt/yql/providers/dq/global_worker_manager
)

END()
