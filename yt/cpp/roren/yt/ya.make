LIBRARY()

SRCS(
    base_state.cpp
    connectors.cpp
    dependency_runner.cpp
    dot.cpp
    jobs.cpp
    kv_state.cpp
    level_runner.cpp
    operations.cpp
    profile_state.cpp
    tables.cpp
    transforms.cpp
    visitors.cpp
    yt.cpp
    yt_io_private.cpp
    yt_graph.cpp
    yt_graph_v2.cpp
    yt_proto_io.cpp
    yt_write.cpp
)

PEERDIR(
    library/cpp/threading/future
    library/cpp/threading/future/subscription
    yt/cpp/roren/interface
    yt/cpp/roren/yt/proto
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/util
)

END()

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        ut
        test_medium
    )
ENDIF()
