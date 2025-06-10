LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    client.cpp
    config.cpp
    credential_provider.cpp
    crypto_helpers.cpp
    http.cpp
)

PEERDIR(
    yt/yt/library/tvm/service
    yt/yt/core
    yt/yt/core/http

    library/cpp/string_utils/base64
    library/cpp/xml/document
)

END()

RECURSE_FOR_TESTS(
    unittests
)
