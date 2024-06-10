LIBRARY()

SRCS(
    operation.cpp
    out.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/threading/future
    contrib/ydb/public/lib/operation_id
    contrib/ydb/public/sdk/cpp/client/ydb_types
)

END()
