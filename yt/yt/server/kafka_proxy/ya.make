LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

GENERATE_YT_RECORD(
    records/kafka_message.yaml
)

SRCS(
    bootstrap.cpp
    config.cpp
    connection.cpp
    dynamic_config_manager.cpp
    program.cpp
    server.cpp
)

PEERDIR(
    yt/yt/server/lib/cypress_registrar
    yt/yt/server/lib/misc

    yt/yt/ytlib

    yt/yt/library/dynamic_config

    library/cpp/getopt
    library/cpp/yt/phdr_cache
)

END()
