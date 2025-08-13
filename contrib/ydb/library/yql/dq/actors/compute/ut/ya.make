UNITTEST_FOR(contrib/ydb/library/yql/dq/actors/compute)

IF (NOT OS_WINDOWS)
SRCS(
    dq_async_compute_actor_ut.cpp
)
ELSE()
# TTestActorRuntimeBase(..., true) seems broken on windows
ENDIF()

SRCS(
    dq_compute_actor_ut.cpp
    dq_compute_actor_async_input_helper_ut.cpp
    dq_compute_issues_buffer_ut.cpp
    dq_source_watermark_tracker_ut.cpp
    mock_lookup_factory.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/actors/wilson
    contrib/ydb/library/services
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/library/yql/dq/actors/compute/ut/proto
    contrib/ydb/library/yql/dq/actors/input_transforms
    contrib/ydb/library/yql/dq/actors/task_runner
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/tasks
    contrib/ydb/library/yql/dq/transform
    contrib/ydb/library/yql/providers/dq/task_runner
    contrib/ydb/library/yql/public/ydb_issue
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/computation
    yql/essentials/minikql/invoke_builtins
    yql/essentials/providers/common/comp_nodes
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
   proto
)
