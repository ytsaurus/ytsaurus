LIBRARY()

SRCS(
    cms.cpp
)

PEERDIR(
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/common_client/impl
    contrib/ydb/public/sdk/cpp/src/client/driver
)

END()
