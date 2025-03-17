LIBRARY()

SRCS(
    manager.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/kqp/gateway/actors
    contrib/ydb/core/kqp/provider
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/metadata/manager
)

YQL_LAST_ABI_VERSION()

END()
