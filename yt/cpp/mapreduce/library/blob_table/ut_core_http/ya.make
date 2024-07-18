GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ENV(
    YT_TESTS_USE_CORE_HTTP_CLIENT="yes"
)

SRCS(
    ../ut/blob_table_ut.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    yt/cpp/mapreduce/library/blob_table
    yt/cpp/mapreduce/tests/yt_unittest_lib
    yt/cpp/mapreduce/tests/gtest_main
)

SIZE(MEDIUM)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)
ENDIF()

END()
