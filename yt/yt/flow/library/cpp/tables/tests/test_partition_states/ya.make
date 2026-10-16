GTEST(flow-cpp-integration-test-partition-states)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    test_partition_states.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/client/cache
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/tables
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

REQUIREMENTS(ram:16)

END()
