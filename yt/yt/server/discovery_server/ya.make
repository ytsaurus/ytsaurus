LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    bootstrap.cpp
    config.cpp
    program.cpp
)

PEERDIR(
    yt/yt/server/lib
    yt/yt/server/lib/discovery_server

    yt/yt/ytlib

    yt/yt/library/orchid
    yt/yt/library/server_program

    library/cpp/getopt
    library/cpp/yt/phdr_cache
)

END()
