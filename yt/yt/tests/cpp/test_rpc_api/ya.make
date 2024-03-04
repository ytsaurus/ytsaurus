GTEST(cpp-integration-test-rpc-api)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    test_rpc_api.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/cpp/common_tests.make.inc)

EXPLICIT_DATA()
DATA(arcadia/yt/yt/tests/cpp/test_rpc_api/config.yson)

IF(NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/cpp/test_base
    yt/yt/ytlib
    yt/yt/core/test_framework
    yt/yt/library/named_value
    contrib/libs/apache/arrow
)

ENV(USE_YT_RECIPE_BUILT_FROM_SOURCE=yes)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/recipe/recipe.inc)

TAG(ya:yt ya:fat ya:huge_logs)

SIZE(LARGE)

YT_SPEC(yt/yt/tests/integration/spec.yson)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:20)
ENDIF()

END()
