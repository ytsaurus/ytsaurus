GTEST(cpp-integration-test-s3-data)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    test_s3_data.cpp
)
INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/cpp/common_tests.make.inc)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/local_s3_recipe/recipe.inc)

PEERDIR(
    yt/yt/tests/cpp/test_base
    yt/yt/ytlib
    yt/yt/core/test_framework
    yt/yt/library/named_value
    yt/yt/server/lib/signature
    yt/yt/server/lib/s3
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
