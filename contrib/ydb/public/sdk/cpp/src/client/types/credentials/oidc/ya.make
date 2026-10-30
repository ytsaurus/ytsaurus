LIBRARY()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common_arcadia.inc)

SRCS(
    credentials.cpp
    private.cpp
    provider_base.cpp
    static_provider.cpp
    client_provider.cpp
    device_provider.cpp
    protocol.cpp
)

PEERDIR(
    library/cpp/cgiparam
    library/cpp/http/simple
    library/cpp/json
    library/cpp/openssl/crypto
    library/cpp/string_utils/quote
    library/cpp/string_utils/base64
    library/cpp/threading/cancellation
    library/cpp/uri
    contrib/ydb/public/sdk/cpp/src/client/types/core_facility
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

END()
