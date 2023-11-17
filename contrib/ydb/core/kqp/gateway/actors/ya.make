LIBRARY()

SRCS(
    scheme.cpp
)

PEERDIR(
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/provider
    contrib/ydb/library/yql/providers/common/gateway
    contrib/ydb/core/tx/schemeshard
    library/cpp/actors/core
)

YQL_LAST_ABI_VERSION()

END()
