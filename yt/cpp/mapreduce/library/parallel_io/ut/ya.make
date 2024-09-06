UNITTEST_WITH_CUSTOM_ENTRY_POINT()

IF (OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

SRCS(
    parallel_reader_ut.cpp
    parallel_writer_ut.cpp
    parallel_file_writer_ut.cpp
    parallel_file_reader_ut.cpp
    resource_limiter_ut.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/library/mock_client
    yt/cpp/mapreduce/library/parallel_io
    yt/cpp/mapreduce/library/parallel_io/ut/proto
    yt/cpp/mapreduce/tests/yt_unittest_lib
    yt/cpp/mapreduce/tests/gtest_main
    yt/cpp/mapreduce/tests/native/proto_lib
    yt/cpp/mapreduce/tests/lib
    yt/cpp/mapreduce/util
)

SIZE(MEDIUM)

REQUIREMENTS(
    cpu:4
)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

END()
