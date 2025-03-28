LIBRARY()

SRCS(
    solomon_accessor_client.cpp
    solomon_client_utils.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/solomon/proto
    contrib/ydb/library/yql/providers/solomon/solomon_accessor/grpc
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    yql/essentials/utils
)

END()
