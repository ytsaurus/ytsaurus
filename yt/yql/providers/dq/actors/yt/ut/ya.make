UNITTEST_FOR(yt/yql/providers/dq/actors/yt)

NO_BUILD_IF(OS_WINDOWS)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/yql/providers/dq/actors/events
    contrib/ydb/library/yql/providers/dq/common
    library/cpp/testing/gmock_in_unittest
    yql/essentials/utils/log
    yql/essentials/utils/log/proto
    yt/cpp/mapreduce/interface
    yt/yql/providers/dq/global_worker_manager
    yt/yt/client/unittests/mock
)

SRCS(
    lock_ut.cpp
    node_id_allocator_ut.cpp
    nodeid_cleaner_ut.cpp
    yt_resource_manager_ut.cpp
    yt_wrapper_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
