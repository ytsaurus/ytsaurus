LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    bootstrap.cpp
    config.cpp
    connection.cpp
    server.cpp
)

PEERDIR(
    yt/yt/core
)

END()
