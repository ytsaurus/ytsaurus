LIBRARY()

SRCS(
    cancelation.cpp
)

PEERDIR(
    contrib/ydb/core/grpc_services/cancelation/protos
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/protos
)

END()
