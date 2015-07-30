LIBRARY()

OWNER(monster)

SRCS(
    client.cpp
    init.cpp
    operation.cpp
)

PEERDIR(
    mapreduce/yt/interface
    mapreduce/yt/common
    mapreduce/yt/io
    library/digest/md5
)

END()
