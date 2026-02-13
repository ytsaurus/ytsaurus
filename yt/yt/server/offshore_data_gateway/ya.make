LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    bootstrap.cpp
    config.cpp
    dynamic_config_manager.cpp
    offshore_data_gateway_service.cpp
    program.cpp
)

PEERDIR(
    library/cpp/yt/phdr_cache

    library/cpp/getopt

    yt/yt/library/dynamic_config
    yt/yt/library/program

    yt/yt/client

    yt/yt/ytlib

    yt/yt/server/lib/admin
    yt/yt/server/lib/misc
    yt/yt/server/lib/cypress_registrar
    yt/yt/server/lib/s3
)

END()
