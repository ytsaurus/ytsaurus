GTEST(flow-cpp-integration-test-compact-output-store)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

TIMEOUT(60)

SRCS(
    test_compact_output_store.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/client/cache
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/misc
    yt/yt/flow/library/cpp/tables
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

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/recipes/local_yt.inc)

REQUIREMENTS(ram:16)

END()
