LIBRARY()

SRCS(
    jobs.cpp
    yt.cpp
    yt_io_private.cpp
    yt_graph.cpp
    yt_graph_v2.cpp
)

PEERDIR(
    yt/cpp/roren/interface
    yt/cpp/roren/yt/proto
    yt/cpp/mapreduce/client
)

END()

IF (NOT OPENSOURCE) 
    RECURSE_FOR_TESTS(
        ut
        test-medium
    )
ENDIF()
