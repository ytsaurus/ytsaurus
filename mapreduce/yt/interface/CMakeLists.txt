LIBRARY()

OWNER(
    ermolovd
    monster
)

SRCS(
    common.h
    node.cpp
    cypress.h
    io.h
    operation.h
    client.h
)

PEERDIR(
    contrib/libs/protobuf
    mapreduce/yt/interface/protos
)

END()
