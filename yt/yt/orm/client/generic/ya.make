LIBRARY()

SRCS(
    client.cpp
    generic_orm_client.cpp
    generic_orm_service_proxy.cpp
)

PEERDIR(
    yt/yt/orm/client/native
    yt/yt/orm/client/objects

    yt/yt/client

    yt/yt/core
    yt/yt/core/rpc/grpc

    yt/yt_proto/yt/client
    yt/yt_proto/yt/orm/client/proto

    library/cpp/protobuf/interop
    library/cpp/resource
    library/cpp/retry
)

END()
