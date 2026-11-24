LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

PEERDIR(
    contrib/libs/fmt
)

IF (EXPORT_CMAKE)
    PEERDIR(
        contrib/libs/grpc
        contrib/libs/jwt-cpp
        contrib/libs/protobuf
        library/cpp/cgiparam
        library/cpp/colorizer
        library/cpp/containers/stack_vector
        library/cpp/logger
        library/cpp/monlib/metrics
        library/cpp/openssl/holders
        library/cpp/resource
        library/cpp/streams/zstd
        library/cpp/string_utils/quote
        library/cpp/string_utils/url
        library/cpp/threading/future
        library/cpp/threading/future/subscription
    )
    IF (OS_WINDOWS)
        PEERDIR(
            yt/yt/flow/extensions/ydb/sdk/uri
        )
    ELSE()
        PEERDIR(
            library/cpp/uri
        )
        IF (OS_LINUX)
            PEERDIR(
                contrib/libs/libidn/static
            )
        ENDIF()
    ENDIF()
    PEERDIR(
        library/cpp/yt/threading
        tools/enum_parser/enum_serialization_runtime
    )
    PEERDIR(
        library/cpp/monlib/encode/json
        library/cpp/monlib/encode/prometheus
        library/cpp/monlib/encode/spack
    )
    IF (OPENSOURCE_PROJECT != "yt-cpp-sdk")
        CPP_PROTO_PLUGIN0(
            grpc_cpp
            contrib/tools/protoc/plugins/grpc_cpp
        )
    ENDIF()
ELSE()
    PEERDIR(
        contrib/libs/ydb-cpp-sdk/src/client/driver
        contrib/libs/ydb-cpp-sdk/src/client/federated_topic
        contrib/libs/ydb-cpp-sdk/src/client/topic
        contrib/libs/ydb-cpp-sdk/src/client/types/credentials/login
        contrib/ydb/public/api/grpc
    )
ENDIF()

END()
