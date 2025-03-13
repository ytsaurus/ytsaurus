LIBRARY()

SRCS(
    value.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/string_utils/base64
    contrib/ydb/core/protos
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/public/lib/scheme_types
    contrib/ydb/public/sdk/cpp/src/client/value
)

END()
