LIBRARY()

OWNER(
    ermolovd
    monster
)

SRCS(
    client.cpp
    init.cpp
    mock_client.cpp
    operation.cpp
)

PEERDIR(
    library/digest/md5
    library/yson
    mapreduce/yt/common
    mapreduce/yt/interface
    mapreduce/yt/io
)

END()
