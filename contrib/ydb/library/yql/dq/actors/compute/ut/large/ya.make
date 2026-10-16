UNITTEST_FOR(contrib/ydb/library/yql/dq/actors/compute)

IF (NOT OS_WINDOWS)
SRCS(
    ../dq_async_compute_actor_ut.cpp
    ../dq_sync_compute_actor_ut.cpp
)
ELSE()
# TTestActorRuntimeBase(..., true) seems broken on windows
ENDIF()

SRCS(
    ../mock_lookup_factory.cpp
)
ENV(TESTS_LARGE=1)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/actors/wilson
    contrib/ydb/library/services
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/library/yql/dq/actors/compute/ut/proto
    contrib/ydb/library/yql/dq/actors/input_transforms
    contrib/ydb/library/yql/dq/actors/task_runner
    contrib/ydb/library/yql/dq/tasks
    contrib/ydb/library/yql/dq/transform
    contrib/ydb/library/yql/providers/dq/task_runner
    contrib/ydb/library/yql/public/ydb_issue
    yql/essentials/minikql/computation
    yql/essentials/providers/common/comp_nodes
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    yql/essentials/utils/backtrace
)

CHECK_DEPENDENT_DIRS(ALLOW_ONLY PEERDIRS
    build
    certs
    contrib/libs
    contrib/proto
    contrib/restricted
    library
    tools
    util
    contrib/ydb/core/quoter/public
    contrib/ydb/library
    contrib/ydb/public
    yql/essentials
)

YQL_LAST_ABI_VERSION()

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
FORK_SUBTESTS()

END()
