UNITTEST_WITH_CUSTOM_ENTRY_POINT()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

IF (OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

ENV(
    YT_TESTS_USE_CORE_HTTP_CLIENT="yes"
)

SRCS(
    ../ut/parallel_reader_ut.cpp
    ../ut/parallel_writer_ut.cpp
    ../ut/parallel_file_writer_ut.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    yt/cpp/mapreduce/library/parallel_io
    yt/cpp/mapreduce/library/parallel_io/ut/proto
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/util
    yt/cpp/mapreduce/tests/yt_unittest_lib
    yt/cpp/mapreduce/tests/gtest_main
    yt/cpp/mapreduce/tests/native/proto_lib
    yt/cpp/mapreduce/tests/lib
)

SIZE(MEDIUM)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)
ENDIF()

END()
