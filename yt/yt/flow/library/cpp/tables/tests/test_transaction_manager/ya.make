GTEST(flow-cpp-integration-test-transaction-manager)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    test_transaction_manager.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/client/cache
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/tables
    yt/yt/flow/library/cpp/misc
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

REQUIREMENTS(ram:16)

END()
