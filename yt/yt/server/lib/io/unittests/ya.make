GTEST(unittester-server-lib-io)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    erasure_ut.cpp
    gentle_loader_ut.cpp
    chunk_file_writer_ut.cpp
    io_engine_ut.cpp
    io_tracker_ut.cpp
    read_request_combiner_ut.cpp
    io_request_slicer_ut.cpp
    io_workload_model_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build
    yt/yt/server/lib/io
)

FORK_TESTS()

SPLIT_FACTOR(5)

SIZE(MEDIUM)

END()
