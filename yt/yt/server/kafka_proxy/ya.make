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
    group_coordinator.cpp
    helpers.cpp
    program.cpp
    server.cpp
)

PEERDIR(
    yt/yt/server/lib/cypress_registrar
    yt/yt/server/lib/misc

    yt/yt/ytlib

    yt/yt/library/dynamic_config
    yt/yt/library/program
    yt/yt/library/server_program

    library/cpp/getopt
    library/cpp/iterator
)

END()
