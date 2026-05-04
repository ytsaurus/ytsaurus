LIBRARY()

SRCS(
    yql_yt_vanilla_peer_tracker.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/http/simple
    yt/cpp/mapreduce/client
)

END()
