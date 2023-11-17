LIBRARY()

SRCS(
    import.cpp
)

GENERATE_ENUM_SERIALIZATION(import.h)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_common_client/impl
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_proto
    contrib/ydb/public/sdk/cpp/client/ydb_types/operation
)

END()
