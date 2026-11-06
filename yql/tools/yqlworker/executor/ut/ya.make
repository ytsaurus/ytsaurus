UNITTEST_FOR(yql/tools/yqlworker/executor)

IF (OS_LINUX OR OS_DARWIN)
    SRCS(
        task_executor_ut.cpp
    )
ENDIF()

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/tools/yqlworker/executor/common/tasks
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
    yql/tools/yqlworker/interface/proto
)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

END()
