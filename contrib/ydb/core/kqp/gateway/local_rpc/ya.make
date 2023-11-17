LIBRARY()

SRCS(
    helper.cpp
)

PEERDIR(
    contrib/ydb/core/grpc_services/local_rpc
    contrib/ydb/core/kqp/provider
    contrib/ydb/library/ydb_issue
)

YQL_LAST_ABI_VERSION()

END()
