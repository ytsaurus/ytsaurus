GTEST(flow-cpp-integration-test-partition-store)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

TIMEOUT(60)

FORK_SUBTESTS()

SRCS(
    test_paged_timer_store.cpp
    test_timer_store.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/cpp/test_base
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/native_client
    yt/yt/flow/library/cpp/pipeline_tables
)

TAG(ya:huge_logs)

# remove after YT-19477
IF (OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

IF (YT_TEAMCITY)
    TAG(ya:yt ya:fat)

    YT_SPEC(yt/yt/tests/integration/spec_teamcity.yson)

    SIZE(LARGE)
ELSE()
    SIZE(MEDIUM)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/yt_env.inc)

REQUIREMENTS(ram:16)

END()
