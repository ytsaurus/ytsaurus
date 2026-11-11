GTEST(flow-cpp-integration-test-timer-pages)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    test_timer_pages.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/client/cache
    yt/yt/client/unittests/mock
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/native_client
    yt/yt/flow/library/cpp/pipeline_tables
    yt/yt/flow/library/cpp/tables
    yt/yt/library/query/engine
    yt/yt/ytlib
)

TAG(ya:huge_logs)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

REQUIREMENTS(ram:16)

END()
