LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/grpc/client
    contrib/ydb/library/yql/public/issue
)

GENERATE_ENUM_SERIALIZATION(s3_settings.h)
GENERATE_ENUM_SERIALIZATION(status_codes.h)

END()
