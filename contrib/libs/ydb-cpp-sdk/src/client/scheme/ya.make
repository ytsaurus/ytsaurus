LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    out.cpp
    scheme.cpp
)

GENERATE_ENUM_SERIALIZATION(contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/scheme/scheme.h)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/make_request
    contrib/libs/ydb-cpp-sdk/src/client/impl/internal/scheme_helpers
    contrib/libs/ydb-cpp-sdk/src/client/common_client/impl
    contrib/libs/ydb-cpp-sdk/src/client/driver
)

END()
