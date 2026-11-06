UNITTEST_FOR(yt/yql/providers/dq/global_worker_manager)

NO_BUILD_IF(OS_WINDOWS)

SRCS(
    global_worker_manager_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/testlib
    library/cpp/svnversion
    yql/essentials/providers/common/metrics
    yql/essentials/public/udf/service/stub
    yql/essentials/utils/log
    yt/yql/providers/dq/testlib
)

YQL_LAST_ABI_VERSION()

END()
