LIBRARY()

SRCS(
    inspector_client.cpp
    inspector_client_msgbus.cpp
    worker_server.cpp
    worker_server_msgbus.cpp
)

PEERDIR(
    library/cpp/deprecated/enum_codegen
    library/cpp/logger/global
    library/cpp/messagebus
    library/cpp/messagebus/protobuf
    yql/tools/yqlworker/config
    yql/tools/yqlworker/misc
    yql/tools/yqlworker/proto
)

END()

RECURSE_FOR_TESTS(
    ut
)
