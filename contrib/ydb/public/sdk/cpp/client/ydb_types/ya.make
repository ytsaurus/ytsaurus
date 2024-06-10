LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/library/grpc/client
    contrib/ydb/library/yql/public/issue
)

GENERATE_ENUM_SERIALIZATION(s3_settings.h)
GENERATE_ENUM_SERIALIZATION(status_codes.h)

END()
