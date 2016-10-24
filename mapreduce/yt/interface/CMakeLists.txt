LIBRARY()

OWNER(
    ermolovd
    monster
)

SRCS(
    common.h
    node.h
    cypress.h
    io.h
    operation.h
    client.h
)

PEERDIR(
    util
    contrib/libs/protobuf
    mapreduce/yt/interface/protos
)

END()
