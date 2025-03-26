LIBRARY()

SRCS(
    local_rpc.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/base
)

YQL_LAST_ABI_VERSION()

END()
