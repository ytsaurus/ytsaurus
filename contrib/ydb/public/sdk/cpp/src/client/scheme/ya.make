LIBRARY()

SRCS(
    out.cpp
    scheme.cpp
)

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request
    contrib/ydb/public/sdk/cpp/src/client/common_client/impl
    contrib/ydb/public/sdk/cpp/src/client/driver
)

END()
