GTEST(cpp-integration-test-chaos-lease)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    test_chaos_lease.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/cpp/test_base
    yt/yt/ytlib
    yt/yt/core/test_framework
    yt/yt/client
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/recipe/recipe.inc)

TAG(
    ya:yt
    ya:fat
    ya:huge_logs
    ya:large_tests_on_single_slots
)

SIZE(LARGE)

YT_SPEC(yt/yt/tests/integration/spec.yson)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:20)
ELSE()
    REQUIREMENTS(ram:10)
ENDIF()

END()
