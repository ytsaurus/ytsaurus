LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    virtual_timestamp.cpp
    ydb.cpp
    core_facility/simple_core_facility.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/libs/ydb-cpp-sdk/src/library/grpc/client
    contrib/libs/ydb-cpp-sdk/src/library/issue
)

GENERATE_ENUM_SERIALIZATION(contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/types/s3_settings.h)
GENERATE_ENUM_SERIALIZATION(contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/types/status_codes.h)
GENERATE_ENUM_SERIALIZATION(contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/types/ydb.h)

END()
