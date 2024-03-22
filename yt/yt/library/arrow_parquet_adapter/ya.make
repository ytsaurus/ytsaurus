LIBRARY()

SRCS(
    arrow.cpp
)

PEERDIR(
    yt/cpp/mapreduce/client
    library/cpp/yt/assert

    contrib/libs/apache/arrow
)

END()
