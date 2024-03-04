GTEST(cpp-integration-test-native-api)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    test_native_api.cpp
)
INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/cpp/common_tests.make.inc)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/query/engine
    yt/yt/tests/cpp/test_base
    yt/yt/ytlib
    yt/yt/core/test_framework
    yt/yt/library/named_value
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/recipe/recipe.inc)

TAG(ya:yt ya:fat ya:huge_logs)

SIZE(LARGE)

YT_SPEC(yt/yt/tests/integration/spec.yson)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:20)
ELSE()
    REQUIREMENTS(ram:10)
ENDIF()

END()
