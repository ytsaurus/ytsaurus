UNITTEST_FOR(yql/tools/yqlworker/executor/common)

SRCS(
    runnable_tasks_ut.cpp
    task_progress_merger_ut.cpp
)

PEERDIR(
    library/cpp/yson/node
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg
    yql/tools/yqlworker/executor/common/tasks
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
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
