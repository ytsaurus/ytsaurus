GTEST(flow-cpp-multiplexer-test-query)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    test_query.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/cpp/test_base
    yt/yt/library/query/engine
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/multiplexer
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
