YT_UNITTEST(mapreduce-yt-library-parallel-io-test)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

OWNER(
    g:yt
    g:yt-cpp
)

SRCS(
    parallel_reader_ut.cpp
    test_message.proto
    parallel_writer_ut.cpp
    data.proto
    parallel_file_writer_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    yt/cpp/mapreduce/library/parallel_io
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/util
    yt/cpp/mapreduce/tests_core_http/yt_unittest_lib
    yt/cpp/mapreduce/tests_core_http/yt_unittest_main
    yt/cpp/mapreduce/tests_core_http/native/proto_lib
    yt/cpp/mapreduce/tests_core_http/lib
)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)

END()
