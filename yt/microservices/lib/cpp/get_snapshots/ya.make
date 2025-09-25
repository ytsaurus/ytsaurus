LIBRARY()

SRCS(
    get_snapshots.cpp
)

PEERDIR(
    yt/cpp/mapreduce/tests/yt_initialize_hook
    yt/yt/client/hedging
)

END()
