LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    attribute.cpp
    channel_factory.cpp
    client.cpp
    client_impl.cpp
    config.cpp
    connection.cpp
    connection_impl.cpp
    helpers.cpp
    object_service_proxy.cpp
    payload.cpp
    public.cpp
    request.cpp
    response.cpp
    semaphore_guard.cpp
    semaphore_guard_options.cpp
)

PEERDIR(
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

RESOURCE(
    certs/yandex_internal.pem yandex_internal.pem
)

END()

RECURSE_FOR_TESTS(unittests)
