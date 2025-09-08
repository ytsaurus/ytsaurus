GTEST(cpp-integration-test-chunk-scraper)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    test_chunk_scraper.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/cpp/test_base
    yt/yt/ytlib
    yt/yt/core/test_framework
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
