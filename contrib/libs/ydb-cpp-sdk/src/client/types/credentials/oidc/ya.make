LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    client_provider.cpp
    credentials.cpp
    device_provider.cpp
    private.cpp
    protocol.cpp
    provider_base.cpp
    static_provider.cpp
)

PEERDIR(
    library/cpp/cgiparam
    library/cpp/http/simple
    library/cpp/json
    library/cpp/openssl/crypto
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    library/cpp/threading/cancellation
    library/cpp/uri
    contrib/libs/ydb-cpp-sdk/src/client/types
    contrib/libs/ydb-cpp-sdk/src/client/types/credentials
)

END()
