LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/cpp/mapreduce/interface
    library/cpp/yt/memory
)

SRCS(
    parallel_reader.cpp
    parallel_writer.cpp
    parallel_file_reader.cpp
    parallel_file_writer.cpp
    resource_limiter.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_core_http
)
