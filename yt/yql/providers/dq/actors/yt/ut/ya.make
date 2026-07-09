UNITTEST_FOR(yt/yql/providers/dq/actors/yt)

NO_BUILD_IF(OS_WINDOWS)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/yql/providers/dq/actors/events
    contrib/ydb/library/yql/providers/dq/common
    yt/cpp/mapreduce/interface
    yt/yql/providers/dq/global_worker_manager
    yql/essentials/utils/log
    yql/essentials/utils/log/proto
)

SRCS(
    node_id_allocator_ut.cpp
    yt_resource_manager_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
