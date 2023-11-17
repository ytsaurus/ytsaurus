LIBRARY()

SRCS(
    scheme.cpp
)

GENERATE_ENUM_SERIALIZATION(scheme.h)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    contrib/ydb/public/sdk/cpp/client/ydb_common_client/impl
    contrib/ydb/public/sdk/cpp/client/ydb_driver
)

END()
