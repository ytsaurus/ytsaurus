LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    bootstrap.cpp
    config.cpp
    dynamic_config_manager.cpp
    program.cpp
)

PEERDIR(
    library/cpp/getopt

    yt/yt/library/dynamic_config
    yt/yt/library/orchid
    yt/yt/library/server_program

    yt/yt/ytlib

    yt/yt/server/lib/chaos_cache
    yt/yt/server/lib/chaos_node
    yt/yt/server/lib/cypress_registrar
)

END()
