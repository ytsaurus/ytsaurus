LIBRARY()

SRCS(
    scheme.cpp
    analyze_actor.cpp
)

PEERDIR(
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/core/kqp/common
    contrib/ydb/core/kqp/provider
    yql/essentials/providers/common/gateway
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/library/actors/core
    contrib/ydb/library/services
)

YQL_LAST_ABI_VERSION()

END()
