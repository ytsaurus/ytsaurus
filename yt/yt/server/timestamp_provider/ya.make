LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    bootstrap.cpp
    config.cpp
    program.cpp
)

PEERDIR(
    library/cpp/getopt

    yt/yt/ytlib

    yt/yt/library/program
    yt/yt/library/server_program

    yt/yt/server/lib
    yt/yt/server/lib/transaction_server
)

END()
