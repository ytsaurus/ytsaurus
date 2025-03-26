LIBRARY()

SRCS(
    msgbus_client.cpp
    msgbus_client.h
    msgbus_client_config.h
    grpc_client.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/messagebus
    contrib/ydb/public/lib/base
)

END()
