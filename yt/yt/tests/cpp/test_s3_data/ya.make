GTEST(cpp-integration-test-s3-data)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
)
INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/cpp/common_tests.make.inc)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/local_s3_recipe/recipe.inc)

PEERDIR(
    yt/yt/tests/cpp/test_base
    yt/yt/client
    yt/yt/ytlib
    yt/yt/library/query/engine
    yt/yt/core/test_framework
    yt/yt/library/named_value
    yt/yt/server/lib/signature/components
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

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/integration/yt_spec.inc)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:20)
ELSE()
    REQUIREMENTS(ram:10)
ENDIF()

END()
