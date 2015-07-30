LIBRARY()

OWNER(monster)

SRCS(
    common.h
    node.h
    cypress.h
    io.h
    operation.h
    client.h
    extension.proto
)

PEERDIR(
    util
    contrib/libs/protobuf
)

END()
