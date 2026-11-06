UNITTEST_FOR(yql/tools/yqlworker/core)

IF (OS_LINUX OR OS_DARWIN)

SRCS(
    inspector_status_pusher_ut.cpp
    task_subscriptions_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/tools/yqlworker/executor/common/tasks
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
)

ENDIF()

END()
