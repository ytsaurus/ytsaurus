UNITTEST()

NO_BUILD_IF(OS_WINDOWS)

PEERDIR(
    contrib/ydb/library/yql/dq/runtime
    contrib/ydb/library/yql/providers/dq/api/protos
    library/cpp/testing/common
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/minikql/invoke_builtins
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
    yt/yql/providers/dq/runtime
    yt/yql/providers/dq/task_runner
)

DEPENDS(
    yt/yql/providers/dq/task_runner/ut/execution/child
)

SRCS(
    pipe_execution_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    child
)
