UNITTEST_WITH_CUSTOM_ENTRY_POINT(yt-lambda-test)

SRCS(
    field_copier_ut.cpp
    lambda_test.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    yt/cpp/mapreduce/library/lambda
    yt/cpp/mapreduce/tests/yt_unittest_lib
    yt/cpp/mapreduce/tests/gtest_main
    yt/cpp/mapreduce/library/lambda/ut/proto
)

SIZE(MEDIUM)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)
ENDIF()

END()
