LIBRARY()

SRCS(
    operation_runner.cpp
    jobs.cpp
    yt.cpp
    base_state.cpp
    kv_state.cpp
    profile_state.cpp
    yt_io_private.cpp
    yt_graph.cpp
    yt_graph_v2.cpp
)

PEERDIR(
    library/cpp/threading/future
    library/cpp/threading/future/subscription
    yt/cpp/roren/interface
    yt/cpp/roren/yt/proto
    yt/cpp/mapreduce/client
)

END()

IF (NOT OPENSOURCE)
    RECURSE_FOR_TESTS(
        ut
        test_medium
    )
ENDIF()
