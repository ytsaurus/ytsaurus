LIBRARY()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common_arcadia.inc)

SRCS(
    virtual_timestamp.cpp
    ydb.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/public/sdk/cpp/src/library/issue
)

GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/s3_settings.h)
GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h)
GENERATE_ENUM_SERIALIZATION(contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/ydb.h)

END()
