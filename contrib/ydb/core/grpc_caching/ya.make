LIBRARY()

SRCS(
    cached_grpc_request_actor.cpp
)

PEERDIR(
    contrib/ydb/library/actors/protos
    contrib/ydb/core/base
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
)

END()
