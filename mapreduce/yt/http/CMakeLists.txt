LIBRARY()

OWNER(monster)

SRCS(
    error.cpp
    http.cpp
    requests.cpp
    transaction.cpp
)

PEERDIR(
    util
    library/http/io
    mapreduce/yt/interface
    mapreduce/yt/common
)

END()
