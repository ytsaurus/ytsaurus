UNITTEST_WITH_CUSTOM_ENTRY_POINT(yt-lambda-test)

ENV(
    YT_TESTS_USE_CORE_HTTP_CLIENT="yes"
)

SRCS(
    ../ut/field_copier_ut.cpp
    ../ut/lambda_test.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    yt/cpp/mapreduce/library/lambda
    yt/cpp/mapreduce/tests/yt_unittest_lib
    yt/cpp/mapreduce/library/lambda/ut/proto
)

SIZE(MEDIUM)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)
ENDIF()

END()
