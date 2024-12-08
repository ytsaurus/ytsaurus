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
    library/cpp/yt/phdr_cache
    yt/yt/library/server_program
    yt/yt/ytlib
    yt/yt/server/lib
    yt/yt/server/lib/discovery_server
)

END()
